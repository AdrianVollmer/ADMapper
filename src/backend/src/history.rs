//! Query history service - application-level query history management.
//!
//! This module provides query history storage independent of the graph database backend.
//! For SQLite-based backends (CrustDB), history is persisted to the same database file.
//! For remote backends (FalkorDB, Neo4j), history is stored in memory.

use crate::db::types::{NewQueryHistoryEntry, QueryHistoryRow};
use parking_lot::Mutex;
use rusqlite::params;
use std::path::Path;
use thiserror::Error;

/// Maximum number of entries to keep in memory for in-memory storage.
const MAX_IN_MEMORY_ENTRIES: usize = 1000;

/// Error type for history operations.
#[derive(Error, Debug)]
pub enum HistoryError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("History entry not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, HistoryError>;

/// Query history service that manages history storage.
pub struct QueryHistoryService {
    storage: Box<dyn HistoryStorage>,
}

impl QueryHistoryService {
    /// Create a new history service with SQLite storage.
    /// Uses the same database file as the CrustDB backend.
    pub fn new_sqlite(db_path: &Path) -> Result<Self> {
        let storage = SqliteHistoryStorage::new(db_path)?;
        Ok(Self {
            storage: Box::new(storage),
        })
    }

    /// Create a new history service with in-memory storage.
    /// Used for remote backends like FalkorDB and Neo4j.
    pub fn new_in_memory() -> Self {
        Self {
            storage: Box::new(InMemoryHistoryStorage::new()),
        }
    }

    /// Add a new query history entry.
    pub fn add(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        self.storage.add(entry)
    }

    /// Update the status of an existing query.
    pub fn update_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        self.storage
            .update_status(id, status, duration_ms, result_count, error)
    }

    /// Get query history with pagination.
    /// Returns (entries, total_count).
    pub fn get(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)> {
        self.storage.get(limit, offset)
    }

    /// Delete a single query history entry.
    pub fn delete(&self, id: &str) -> Result<()> {
        self.storage.delete(id)
    }

    /// Clear all query history.
    pub fn clear(&self) -> Result<()> {
        self.storage.clear()
    }
}

/// Trait for query history storage backends.
trait HistoryStorage: Send + Sync {
    fn add(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()>;
    fn update_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()>;
    fn get(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)>;
    fn delete(&self, id: &str) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

// ============================================================================
// SQLite Storage
// ============================================================================

/// SQLite-based history storage for CrustDB backend.
struct SqliteHistoryStorage {
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteHistoryStorage {
    fn new(db_path: &Path) -> Result<Self> {
        let conn = rusqlite::Connection::open(db_path)?;

        // Enable WAL mode for better concurrency
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;

        // Create the query_history table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS query_history (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                query TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                result_count INTEGER,
                status TEXT NOT NULL DEFAULT 'completed',
                started_at INTEGER NOT NULL DEFAULT 0,
                duration_ms INTEGER,
                error TEXT,
                background INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )?;

        // Create index for efficient ordering
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_query_history_started_at ON query_history(started_at DESC)",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl Drop for SqliteHistoryStorage {
    fn drop(&mut self) {
        // Checkpoint WAL so -wal files don't survive after shutdown.
        let conn = self.conn.lock();
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    }
}

impl HistoryStorage for SqliteHistoryStorage {
    fn add(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO query_history (id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                entry.id,
                entry.name,
                entry.query,
                entry.timestamp,
                entry.result_count,
                entry.status,
                entry.started_at,
                entry.duration_ms,
                entry.error,
                entry.background as i32,
            ],
        )?;
        Ok(())
    }

    fn update_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock();
        let rows_affected = conn.execute(
            "UPDATE query_history SET status = ?1, duration_ms = ?2, result_count = ?3, error = ?4 WHERE id = ?5",
            params![status, duration_ms, result_count, error, id],
        )?;
        if rows_affected == 0 {
            return Err(HistoryError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn get(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)> {
        let conn = self.conn.lock();

        // Get total count
        let total: usize = conn.query_row(
            "SELECT COUNT(*) FROM query_history",
            [],
            |row: &rusqlite::Row| row.get(0),
        )?;

        // Get paginated results
        let mut stmt = conn.prepare(
            "SELECT id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background
             FROM query_history
             ORDER BY started_at DESC
             LIMIT ?1 OFFSET ?2",
        )?;

        let rows = stmt.query_map(params![limit, offset], |row: &rusqlite::Row| {
            Ok(QueryHistoryRow {
                id: row.get(0)?,
                name: row.get(1)?,
                query: row.get(2)?,
                timestamp: row.get(3)?,
                result_count: row.get(4)?,
                status: row.get(5)?,
                started_at: row.get(6)?,
                duration_ms: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                error: row.get(8)?,
                background: row.get::<_, i32>(9)? != 0,
            })
        })?;

        let entries: std::result::Result<Vec<_>, _> = rows.collect();
        Ok((entries?, total))
    }

    fn delete(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock();
        let rows_affected = conn.execute("DELETE FROM query_history WHERE id = ?1", [id])?;
        if rows_affected == 0 {
            return Err(HistoryError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM query_history", [])?;
        Ok(())
    }
}

// ============================================================================
// In-Memory Storage
// ============================================================================

/// In-memory history storage for remote backends (FalkorDB, Neo4j).
struct InMemoryHistoryStorage {
    entries: Mutex<Vec<QueryHistoryRow>>,
}

impl InMemoryHistoryStorage {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl HistoryStorage for InMemoryHistoryStorage {
    fn add(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        let mut entries = self.entries.lock();

        // Convert borrowed entry to owned
        let row = QueryHistoryRow {
            id: entry.id.to_string(),
            name: entry.name.to_string(),
            query: entry.query.to_string(),
            timestamp: entry.timestamp,
            result_count: entry.result_count,
            status: entry.status.to_string(),
            started_at: entry.started_at,
            duration_ms: entry.duration_ms,
            error: entry.error.map(String::from),
            background: entry.background,
        };

        // Insert at the beginning (most recent first)
        entries.insert(0, row);

        // Trim to max size
        if entries.len() > MAX_IN_MEMORY_ENTRIES {
            entries.truncate(MAX_IN_MEMORY_ENTRIES);
        }

        Ok(())
    }

    fn update_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let mut entries = self.entries.lock();
        let entry = entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| HistoryError::NotFound(id.to_string()))?;

        entry.status = status.to_string();
        entry.duration_ms = duration_ms;
        entry.result_count = result_count;
        entry.error = error.map(String::from);

        Ok(())
    }

    fn get(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)> {
        let entries = self.entries.lock();
        let total = entries.len();

        let result: Vec<_> = entries.iter().skip(offset).take(limit).cloned().collect();

        Ok((result, total))
    }

    fn delete(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.lock();
        let len_before = entries.len();
        entries.retain(|e| e.id != id);
        if entries.len() == len_before {
            return Err(HistoryError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let mut entries = self.entries.lock();
        entries.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_entry(id: &str) -> NewQueryHistoryEntry<'_> {
        NewQueryHistoryEntry {
            id,
            name: "Test Query",
            query: "MATCH (n) RETURN n",
            timestamp: 1234567890,
            result_count: Some(10),
            status: "completed",
            started_at: 1234567890,
            duration_ms: Some(100),
            error: None,
            background: false,
        }
    }

    #[test]
    fn test_in_memory_add_and_get() {
        let service = QueryHistoryService::new_in_memory();

        service.add(create_test_entry("1")).unwrap();
        service.add(create_test_entry("2")).unwrap();

        let (entries, total) = service.get(10, 0).unwrap();
        assert_eq!(total, 2);
        assert_eq!(entries.len(), 2);
        // Most recent first
        assert_eq!(entries[0].id, "2");
        assert_eq!(entries[1].id, "1");
    }

    #[test]
    fn test_in_memory_update_status() {
        let service = QueryHistoryService::new_in_memory();
        service.add(create_test_entry("1")).unwrap();

        service
            .update_status("1", "failed", Some(200), None, Some("Error message"))
            .unwrap();

        let (entries, _) = service.get(10, 0).unwrap();
        assert_eq!(entries[0].status, "failed");
        assert_eq!(entries[0].duration_ms, Some(200));
        assert_eq!(entries[0].error.as_deref(), Some("Error message"));
    }

    #[test]
    fn test_in_memory_delete() {
        let service = QueryHistoryService::new_in_memory();
        service.add(create_test_entry("1")).unwrap();
        service.add(create_test_entry("2")).unwrap();

        service.delete("1").unwrap();

        let (entries, total) = service.get(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(entries[0].id, "2");
    }

    #[test]
    fn test_in_memory_clear() {
        let service = QueryHistoryService::new_in_memory();
        service.add(create_test_entry("1")).unwrap();
        service.add(create_test_entry("2")).unwrap();

        service.clear().unwrap();

        let (entries, total) = service.get(10, 0).unwrap();
        assert_eq!(total, 0);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_in_memory_max_entries() {
        let service = QueryHistoryService::new_in_memory();

        for i in 0..1100 {
            service.add(create_test_entry(&i.to_string())).unwrap();
        }

        let (_, total) = service.get(10, 0).unwrap();
        assert_eq!(total, MAX_IN_MEMORY_ENTRIES);
    }

    #[test]
    fn test_sqlite_add_and_get() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let service = QueryHistoryService::new_sqlite(&db_path).unwrap();

        service.add(create_test_entry("1")).unwrap();
        service.add(create_test_entry("2")).unwrap();

        let (entries, total) = service.get(10, 0).unwrap();
        assert_eq!(total, 2);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_sqlite_update_status() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let service = QueryHistoryService::new_sqlite(&db_path).unwrap();

        service.add(create_test_entry("1")).unwrap();

        service
            .update_status("1", "failed", Some(200), None, Some("Error message"))
            .unwrap();

        let (entries, _) = service.get(10, 0).unwrap();
        assert_eq!(entries[0].status, "failed");
        assert_eq!(entries[0].duration_ms, Some(200));
        assert_eq!(entries[0].error.as_deref(), Some("Error message"));
    }

    #[test]
    fn test_sqlite_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create and add entry
        {
            let service = QueryHistoryService::new_sqlite(&db_path).unwrap();
            service.add(create_test_entry("1")).unwrap();
        }

        // Reopen and verify
        {
            let service = QueryHistoryService::new_sqlite(&db_path).unwrap();
            let (entries, total) = service.get(10, 0).unwrap();
            assert_eq!(total, 1);
            assert_eq!(entries[0].id, "1");
        }
    }

    #[test]
    fn test_sqlite_wal_cleanup_on_drop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_wal.db");
        let wal_path = dir.path().join("test_wal.db-wal");

        {
            let service = QueryHistoryService::new_sqlite(&db_path).unwrap();
            service.add(create_test_entry("1")).unwrap();
        }

        // After drop, WAL file should be cleaned up via checkpoint
        assert!(
            !wal_path.exists(),
            "WAL file should be cleaned up after SqliteHistoryStorage::drop()"
        );

        // Data should survive
        let service = QueryHistoryService::new_sqlite(&db_path).unwrap();
        let (entries, total) = service.get(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(entries[0].id, "1");
    }
}
