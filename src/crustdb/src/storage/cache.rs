//! Query cache management.

use crate::error::Result;

use super::{CacheStats, SqliteStorage};

impl SqliteStorage {
    /// Get a cached query result by hash.
    pub fn get_cached_result(&self, query_hash: &str) -> Result<Option<Vec<u8>>> {
        use rusqlite::{params, OptionalExtension};

        let result: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT result FROM query_cache WHERE query_hash = ?1",
                params![query_hash],
                |row| row.get(0),
            )
            .optional()?;
        Ok(result)
    }

    /// Store a query result in the cache.
    pub fn cache_result(&self, query_hash: &str, ast_json: &str, result: &[u8]) -> Result<()> {
        use rusqlite::params;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT OR REPLACE INTO query_cache (query_hash, query_ast, result, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![query_hash, ast_json, result, now],
        )?;
        Ok(())
    }

    /// Clear the query cache manually.
    pub fn clear_query_cache(&self) -> Result<()> {
        self.conn.execute("DELETE FROM query_cache", [])?;
        Ok(())
    }

    /// Get cache statistics (entry count, total size).
    pub fn cache_stats(&self) -> Result<CacheStats> {
        let entry_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM query_cache", [], |row| row.get(0))?;

        let total_size_bytes: usize = self.conn.query_row(
            "SELECT COALESCE(SUM(LENGTH(result)), 0) FROM query_cache",
            [],
            |row| row.get(0),
        )?;

        Ok(CacheStats {
            entry_count,
            total_size_bytes,
        })
    }
}
