use super::{NewQueryHistoryEntry, QueryHistoryRow};
use crate::error::{Error, Result};

impl super::Database {
    // ========================================================================
    // Query History Methods
    // ========================================================================

    /// Add a query to the history.
    pub fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.add_query_history(entry)
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
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.update_query_status(id, status, duration_ms, result_count, error)
    }

    /// Get query history with pagination.
    /// Returns (rows, total_count).
    pub fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.get_query_history(limit, offset)
    }

    /// Delete a query from history.
    pub fn delete_query_history(&self, id: &str) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.delete_query_history(id)
    }

    /// Clear all query history.
    pub fn clear_query_history(&self) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.clear_query_history()
    }
}
