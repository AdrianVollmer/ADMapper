//! Graph insights, analytics, and tier analysis endpoints.

use super::run_db;
use crate::api::core;
use crate::api::types::ApiError;
use crate::db::DbError;
use crate::state::AppState;
use axum::{extract::State, response::Json};
use tracing::{debug, info, instrument};

/// Get security insights from the graph.
#[instrument(skip(state))]
pub async fn graph_insights(
    State(state): State<AppState>,
) -> Result<Json<crate::db::SecurityInsights>, ApiError> {
    let db = state.require_db()?;
    let insights = run_db(db, |db| db.get_security_insights()).await?;

    info!(
        effective_das = insights.effective_da_count,
        real_das = insights.real_da_count,
        total_users = insights.total_users,
        "Security insights computed"
    );
    Ok(Json(insights))
}

/// Get all distinct relationship types in the database.
#[instrument(skip(state))]
pub async fn graph_relationship_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types: Vec<String> = run_db(db, |db| db.get_relationship_types()).await?;
    debug!(count = types.len(), "Relationship types retrieved");
    Ok(Json(types))
}

/// Get all distinct node types in the database.
#[instrument(skip(state))]
pub async fn graph_node_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types: Vec<String> = run_db(db, |db| db.get_node_types()).await?;
    debug!(count = types.len(), "Node types retrieved");
    Ok(Json(types))
}

/// Get choke points in the graph using relationship betweenness centrality.
///
/// Returns the top relationships through which the most shortest paths pass.
/// These are critical "choke point" relationships whose removal would disrupt many attack paths.
///
/// Results are cached at the database level and automatically invalidated when data changes.
#[instrument(skip(state))]
pub async fn graph_choke_points(
    State(state): State<AppState>,
) -> Result<Json<crate::db::ChokePointsResponse>, ApiError> {
    let db = state.require_db()?;
    // Return top 50 choke points
    let result = run_db(db, |db| {
        core::graph_choke_points(db, 50).map_err(DbError::Database)
    })
    .await?;
    info!(
        count = result.choke_points.len(),
        total_edges = result.total_edges,
        "Choke points retrieved"
    );
    Ok(Json(result))
}
