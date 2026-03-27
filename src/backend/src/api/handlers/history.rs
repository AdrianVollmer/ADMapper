//! Query history endpoints.

use super::run_history;
use crate::api::core;
use crate::api::types::{
    AddHistoryRequest, ApiError, HistoryParams, QueryHistoryEntry, QueryHistoryResponse,
    QueryStatus,
};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use tracing::{debug, info, instrument};

/// Get query history with pagination.
#[instrument(skip(state))]
pub async fn get_query_history(
    State(state): State<AppState>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<QueryHistoryResponse>, ApiError> {
    let history_service = state.require_history()?;
    let page = params.page.max(1);
    let per_page = params.per_page.clamp(1, 100);
    let offset = (page - 1) * per_page;

    let (history, total): (Vec<_>, usize) =
        run_history(history_service, move |h| h.get(per_page, offset)).await?;

    let entries: Vec<QueryHistoryEntry> = history
        .into_iter()
        .map(|row| {
            let status = match row.status.as_str() {
                "running" => QueryStatus::Running,
                "completed" => QueryStatus::Completed,
                "failed" => QueryStatus::Failed,
                "aborted" => QueryStatus::Aborted,
                _ => QueryStatus::Completed, // Default fallback
            };
            QueryHistoryEntry {
                id: row.id,
                name: row.name,
                query: row.query,
                timestamp: row.timestamp,
                result_count: row.result_count,
                status,
                started_at: row.started_at,
                duration_ms: row.duration_ms,
                error: row.error,
                background: row.background,
            }
        })
        .collect();

    debug!(
        total = total,
        page = page,
        per_page = per_page,
        "Query history retrieved"
    );
    Ok(Json(QueryHistoryResponse {
        entries,
        total,
        page,
        per_page,
    }))
}

/// Add a query to history.
#[instrument(skip(state, body))]
pub async fn add_query_history(
    State(state): State<AppState>,
    Json(body): Json<AddHistoryRequest>,
) -> Result<Json<QueryHistoryEntry>, ApiError> {
    let history_service = state.require_history()?;

    let entry = tokio::task::spawn_blocking(move || {
        core::add_query_history(history_service.as_ref(), body)
    })
    .await
    .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
    .map_err(ApiError::Internal)?;

    info!(id = %entry.id, name = %entry.name, "Query added to history");
    Ok(Json(entry))
}

/// Delete a query from history.
#[instrument(skip(state))]
pub async fn delete_query_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let history_service = state.require_history()?;
    let id_clone = id.clone();
    run_history(history_service, move |h| h.delete(&id_clone)).await?;
    info!(id = %id, "Query deleted from history");
    Ok(StatusCode::NO_CONTENT)
}

/// Clear all query history.
#[instrument(skip(state))]
pub async fn clear_query_history(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let history_service = state.require_history()?;
    run_history(history_service, |h| h.clear()).await?;
    info!("Query history cleared");
    Ok(StatusCode::NO_CONTENT)
}
