//! Node retrieval, search, status, and ownership endpoints.

use super::run_db;
use crate::api::core;
use crate::api::types::{
    ApiError, BatchSetTierRequest, BatchSetTierResponse, NodeCounts, NodeStatus, SearchParams,
};
use crate::db::{DbEdge, DbNode};
use crate::graph::{FullGraph, GraphEdge, GraphNode};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use tracing::{debug, info, instrument};

/// Get all graph nodes.
pub async fn graph_nodes(State(state): State<AppState>) -> Result<Json<Vec<DbNode>>, ApiError> {
    let db = state.require_db()?;
    let nodes: Vec<DbNode> = run_db(db, |db| db.get_all_nodes()).await?;
    Ok(Json(nodes))
}

/// Get all graph relationships.
pub async fn graph_edges(State(state): State<AppState>) -> Result<Json<Vec<GraphEdge>>, ApiError> {
    let db = state.require_db()?;
    let relationships: Vec<DbEdge> = run_db(db, |db| db.get_all_edges()).await?;
    let result: Vec<GraphEdge> = relationships.into_iter().map(GraphEdge::from).collect();
    Ok(Json(result))
}

/// Get full graph (nodes and relationships).
pub async fn graph_all(State(state): State<AppState>) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    let (nodes, relationships): (Vec<DbNode>, Vec<DbEdge>) = run_db(db, |db| {
        let nodes = db.get_all_nodes()?;
        let relationships = db.get_all_edges()?;
        Ok((nodes, relationships))
    })
    .await?;

    let result = FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        relationships: relationships.into_iter().map(GraphEdge::from).collect(),
    };

    Ok(Json(result))
}

/// Search nodes by name (for autocomplete).
///
/// Results are ordered by label priority (Domain, User, Group, Computer,
/// then remaining labels alphabetically), with names sorted within each group.
#[instrument(skip(state))]
pub async fn graph_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<DbNode>>, ApiError> {
    if params.q.len() < 2 {
        return Ok(Json(Vec::new()));
    }

    let db = state.require_db()?;

    let query = params.q.clone();
    let limit = params.limit;
    let nodes: Vec<DbNode> = run_db(db, move |db| {
        core::graph_search(db, &query, Some(limit)).map_err(crate::db::DbError::Database)
    })
    .await?;

    debug!(query = %params.q, results = nodes.len(), "Search complete");
    Ok(Json(nodes))
}

/// Get a single node by ID with full properties.
///
/// This endpoint is used to fetch node properties on-demand when clicking
/// on a node in the graph visualization.
#[instrument(skip(state))]
pub async fn node_get(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<DbNode>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Fetching node properties");

    let node_id_clone = node_id.clone();
    let nodes = run_db(db, move |db| db.get_nodes_by_ids(&[node_id_clone])).await?;

    nodes
        .into_iter()
        .next()
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {node_id}")))
}

/// Get connection counts for a node.
/// Returns counts for incoming, outgoing, admin permissions, memberOf, and members.
#[instrument(skip(state))]
pub async fn node_counts(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeCounts>, ApiError> {
    let db = state.require_db()?;
    let node_id_clone = node_id.clone();
    let (incoming, outgoing, admin_to, member_of, members) = run_db(db, move |db| {
        db.get_node_relationship_counts(&node_id_clone)
    })
    .await?;

    debug!(
        node_id = %node_id,
        incoming = incoming,
        outgoing = outgoing,
        admin_to = admin_to,
        member_of = member_of,
        members = members,
        "Node counts retrieved"
    );

    Ok(Json(NodeCounts {
        incoming,
        outgoing,
        admin_to,
        member_of,
        members,
    }))
}

/// Get connections for a node in a specific direction.
/// Returns the full graph (nodes and relationships) for rendering.
#[instrument(skip(state))]
pub async fn node_connections(
    State(state): State<AppState>,
    Path((node_id, direction)): Path<(String, String)>,
) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, direction = %direction, "Loading node connections");

    let node_id_clone = node_id.clone();
    let direction_clone = direction.clone();
    let (nodes, relationships): (Vec<DbNode>, Vec<DbEdge>) = run_db(db, move |db| {
        db.get_node_connections(&node_id_clone, &direction_clone)
    })
    .await?;

    Ok(Json(FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        relationships: relationships.into_iter().map(GraphEdge::from).collect(),
    }))
}

/// Get security status for a node.
///
/// Delegates to `core::node_status_full` which is the single canonical
/// implementation shared by both the HTTP (axum) and Tauri desktop paths.
#[instrument(skip(state))]
pub async fn node_status(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeStatus>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Checking node security status");

    let core_status =
        tokio::task::spawn_blocking(move || core::node_status_full(db.as_ref(), &node_id))
            .await
            .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
            .map_err(ApiError::Internal)?;

    Ok(Json(NodeStatus {
        owned: core_status.owned,
        is_disabled: core_status.is_disabled,
        is_enterprise_admin: core_status.is_enterprise_admin,
        is_domain_admin: core_status.is_domain_admin,
        tier: core_status.tier,
        has_path_to_high_tier: core_status.has_path_to_high_tier,
        path_length: core_status.path_length,
    }))
}

/// Request body for setting owned status.
#[derive(Debug, serde::Deserialize)]
pub struct SetOwnedRequest {
    pub owned: bool,
}

/// Toggle the owned status of a node.
#[instrument(skip(state))]
pub async fn node_set_owned(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(body): Json<SetOwnedRequest>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;

    // Escape the node_id for use in Cypher query
    let escaped_id = node_id.replace('\'', "\\'");
    let query = format!(
        "MATCH (n {{objectid: '{}'}}) SET n.owned = {}",
        escaped_id, body.owned
    );

    run_db(db, move |db| db.run_custom_query(&query)).await?;

    info!(node_id = %node_id, owned = %body.owned, "Set node owned status");
    Ok(StatusCode::NO_CONTENT)
}

/// Batch-set the tier property on nodes matching the given filters.
#[instrument(skip(state, body))]
pub async fn batch_set_tier(
    State(state): State<AppState>,
    Json(body): Json<BatchSetTierRequest>,
) -> Result<Json<BatchSetTierResponse>, ApiError> {
    let db = state.require_db()?;

    let result = run_db(db, move |db| {
        core::batch_set_tier(db, body).map_err(crate::db::DbError::Database)
    })
    .await?;

    info!(updated = result.updated, "Batch set tier");
    Ok(Json(result))
}
