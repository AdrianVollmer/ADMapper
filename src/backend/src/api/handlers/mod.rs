//! API route handlers.
//!
//! Split into submodules by domain area. All public handler functions are
//! re-exported so callers can continue using `handlers::function_name`.

mod database;
mod graph;
mod history;
mod import;
mod insights;
mod mutation;
mod nodes;
mod paths;
mod query;
mod settings;

pub use database::*;
pub use graph::*;
pub use history::*;
pub use import::*;
pub use insights::*;
pub use mutation::*;
pub use nodes::*;
pub use paths::*;
pub use query::*;
pub use settings::*;

use crate::api::types::ApiError;
use crate::db::DatabaseBackend;
use crate::history::QueryHistoryService;
use std::sync::Arc;

// ============================================================================
// Shared Helpers
// ============================================================================

/// Run a blocking database operation in a spawn_blocking task.
///
/// This helper reduces boilerplate for the common pattern of running
/// synchronous database operations in an async context.
pub(crate) async fn run_db<T, F>(db: Arc<dyn DatabaseBackend>, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&dyn DatabaseBackend) -> crate::db::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(db.as_ref()))
        .await
        .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
        .map_err(Into::into)
}

/// Run a blocking history operation in a spawn_blocking task.
pub(crate) async fn run_history<T, F>(
    history: Arc<QueryHistoryService>,
    f: F,
) -> Result<T, ApiError>
where
    F: FnOnce(&QueryHistoryService) -> crate::history::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(history.as_ref()))
        .await
        .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
        .map_err(|e| ApiError::Internal(e.to_string()))
}
