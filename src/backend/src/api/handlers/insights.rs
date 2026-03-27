//! Graph insights, analytics, and tier analysis endpoints.

use super::run_db;
use crate::api::core;
use crate::api::types::{ApiError, ComputeEffectiveTiersResponse, TierViolationsResponse};
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
pub async fn graph_edge_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types: Vec<String> = run_db(db, |db| db.get_edge_types()).await?;
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

/// Compute tier violations: direct relationships crossing tier zone boundaries.
///
/// Analyze tier violations.
///
/// Uses stored `effective_tier` property if available (set by compute-effective-tiers).
/// Falls back to on-the-fly reverse BFS computation if effective_tier is not yet computed.
///
/// A violation is an edge from a node in zone N to a node in zone M where N > M
/// (lower-privilege zone reaching higher-privilege zone).
#[instrument(skip(state))]
pub async fn tier_violations(
    State(state): State<AppState>,
) -> Result<Json<TierViolationsResponse>, ApiError> {
    let db = state.require_db()?;

    let result = run_db(db, |db| {
        core::tier_violations(db).map_err(crate::db::DbError::Database)
    })
    .await?;

    info!(
        violations = result.violations.iter().map(|v| v.count).sum::<usize>(),
        "Tier violations computed"
    );

    Ok(Json(result))
}

/// Compute effective tiers for all nodes using multi-source reverse BFS.
///
/// For each tier level (0, 1, 2), finds all nodes that can transitively reach
/// a node of that tier. Each node's effective tier is the minimum tier it can reach.
/// Results are stored as the `effective_tier` property on each node.
pub async fn compute_effective_tiers(
    State(state): State<AppState>,
) -> Result<Json<ComputeEffectiveTiersResponse>, ApiError> {
    let db = state.require_db()?;

    let result = run_db(db, |db| {
        core::compute_effective_tiers(db).map_err(crate::db::DbError::Database)
    })
    .await?;

    info!(
        computed = result.computed,
        violations = result.violations,
        "Effective tiers computed"
    );

    Ok(Json(result))
}
