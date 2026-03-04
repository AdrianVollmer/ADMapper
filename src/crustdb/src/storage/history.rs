//! Query history management.

use crate::error::Result;
use crate::{NewQueryHistoryEntry, QueryHistoryRow};
use rusqlite::params;

use super::SqliteStorage;

impl SqliteStorage {
    /// Add a query to the history.
    pub fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO query_history
             (id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                entry.id,
                entry.name,
                entry.query,
                entry.timestamp,
                entry.result_count,
                entry.status,
                entry.started_at,
                entry.duration_ms.map(|d| d as i64),
                entry.error,
                entry.background
            ],
        )?;
        Ok(())
    }

    /// Update the status of a query in history.
    pub fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE query_history
             SET status = ?2, duration_ms = ?3, result_count = ?4, error = ?5
             WHERE id = ?1",
            params![
                id,
                status,
                duration_ms.map(|d| d as i64),
                result_count,
                error
            ],
        )?;
        Ok(())
    }

    /// Get query history with pagination.
    /// Returns (rows, total_count).
    pub fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        // Get total count
        let total: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM query_history", [], |row| row.get(0))?;

        // Get paginated results
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background
             FROM query_history
             ORDER BY timestamp DESC
             LIMIT ?1 OFFSET ?2",
        )?;

        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(QueryHistoryRow {
                id: row.get(0)?,
                name: row.get(1)?,
                query: row.get(2)?,
                timestamp: row.get(3)?,
                result_count: row.get(4)?,
                status: row.get(5)?,
                started_at: row.get(6)?,
                duration_ms: row.get::<_, Option<i64>>(7)?.map(|d| d as u64),
                error: row.get(8)?,
                background: row.get::<_, i64>(9).map(|v| v != 0).unwrap_or(false),
            })
        })?;

        let history: Vec<QueryHistoryRow> = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((history, total))
    }

    /// Delete a query from history.
    pub fn delete_query_history(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM query_history WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Clear all query history.
    pub fn clear_query_history(&self) -> Result<()> {
        self.conn.execute("DELETE FROM query_history", [])?;
        Ok(())
    }
}
