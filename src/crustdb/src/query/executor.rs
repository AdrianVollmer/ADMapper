//! Query executor - runs query plans against the storage backend.

use crate::error::Result;
use crate::storage::SqliteStorage;
use super::{QueryResult, QueryStats};
use super::planner::QueryPlan;

/// Execute a query plan against the storage.
pub fn execute(plan: &QueryPlan, storage: &SqliteStorage) -> Result<QueryResult> {
    // TODO: Implement query executor
    let _ = (plan, storage);
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        stats: QueryStats::default(),
    })
}
