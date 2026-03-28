//! Path-finding endpoints.

use super::run_db;
use crate::api::types::{
    ApiError, PathParams, PathResponse, PathStep, PathsToDaEntry, PathsToDaParams,
    PathsToDaResponse,
};
use crate::db::{DbError, DbNode, NewQueryHistoryEntry};
use crate::graph::{FullGraph, GraphEdge, GraphNode};
use crate::state::AppState;
use axum::{
    extract::{Query, State},
    response::Json,
};
use serde_json::Value as JsonValue;
use tracing::{debug, info, instrument, warn};

/// Find shortest path between two nodes.
/// Accepts either object IDs or labels as identifiers.
#[instrument(skip(state))]
pub async fn graph_path(
    State(state): State<AppState>,
    Query(params): Query<PathParams>,
) -> Result<Json<PathResponse>, ApiError> {
    let db = state.require_db()?;

    // Generate query ID and track in history
    let query_id = uuid::Uuid::new_v4().to_string();
    let started_at = std::time::Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // First resolve identifiers to get the actual object IDs for the Cypher query
    let from_param = params.from.clone();
    let to_param = params.to.clone();
    let db_for_resolve = db.clone();
    let (from_id, to_id) = run_db(db_for_resolve, move |db| {
        let from_id = match db.resolve_node_identifier(&from_param)? {
            Some(id) => id,
            None => return Err(DbError::NotFound(format!("Node not found: {}", from_param))),
        };
        let to_id = match db.resolve_node_identifier(&to_param)? {
            Some(id) => id,
            None => return Err(DbError::NotFound(format!("Node not found: {}", to_param))),
        };
        Ok((from_id, to_id))
    })
    .await?;

    // Generate proper Cypher query for history (can be re-run from query history)
    let escaped_from = from_id.replace('\'', "\\'");
    let escaped_to = to_id.replace('\'', "\\'");
    let query_name = format!("Path: {} -> {}", params.from, params.to);
    let query_text = format!(
        "MATCH p = shortestPath((a)-[*1..]->(b)) WHERE a.objectid = '{}' AND b.objectid = '{}' RETURN p",
        escaped_from, escaped_to
    );

    // Add to history with "running" status
    if let Some(history) = state.history() {
        if let Err(e) = history.add(NewQueryHistoryEntry {
            id: &query_id,
            name: &query_name,
            query: &query_text,
            timestamp: started_at_unix,
            result_count: None,
            status: "running",
            started_at: started_at_unix,
            duration_ms: None,
            error: None,
            background: false,
        }) {
            warn!(error = %e, "Failed to add path query to history");
        }
    }

    state.start_sync_query();

    let from_id_for_closure = from_id.clone();
    let to_id_for_closure = to_id.clone();
    let db_for_query = db.clone();
    let result = run_db(db_for_query, move |db| {
        let path_result = db.shortest_path(&from_id_for_closure, &to_id_for_closure)?;

        match path_result {
            None => {
                debug!(from = %from_id_for_closure, to = %to_id_for_closure, "No path found");
                Ok(PathResponse {
                    found: false,
                    path: Vec::new(),
                    graph: FullGraph {
                        nodes: Vec::new(),
                        relationships: Vec::new(),
                    },
                })
            }
            Some(path) => {
                // Get node IDs from path
                let node_ids: Vec<String> = path.iter().map(|(id, _)| id.clone()).collect();

                // Get full node data
                let nodes = db.get_nodes_by_ids(&node_ids)?;

                // Build node lookup
                let node_map: std::collections::HashMap<String, DbNode> = nodes
                    .into_iter()
                    .map(|node| (node.id.clone(), node))
                    .collect();

                // Build path steps
                let path_steps: Vec<PathStep> = path
                    .iter()
                    .map(|(id, rel_type)| {
                        let node = node_map.get(id).cloned().unwrap_or_else(|| DbNode {
                            id: id.clone(),
                            name: id.clone(),
                            label: "Unknown".to_string(),
                            properties: JsonValue::Null,
                        });
                        PathStep {
                            node,
                            rel_type: rel_type.clone(),
                        }
                    })
                    .collect();

                // Get relationships between path nodes
                let relationships = db.get_edges_between(&node_ids)?;

                let graph = FullGraph {
                    nodes: path_steps
                        .iter()
                        .map(|s| GraphNode::from(s.node.clone()))
                        .collect(),
                    relationships: relationships.into_iter().map(GraphEdge::from).collect(),
                };

                debug!(
                    from = %from_id_for_closure,
                    to = %to_id_for_closure,
                    path_len = path_steps.len(),
                    "Path found"
                );

                Ok(PathResponse {
                    found: true,
                    path: path_steps,
                    graph,
                })
            }
        }
    })
    .await;

    state.end_sync_query();

    let duration_ms = started_at.elapsed().as_millis() as u64;

    if let Some(history) = state.history() {
        match &result {
            Ok(response) => {
                let result_count = if response.found {
                    Some(response.path.len() as i64)
                } else {
                    Some(0)
                };
                if let Err(e) = history.update_status(
                    &query_id,
                    "completed",
                    Some(duration_ms),
                    result_count,
                    None,
                ) {
                    warn!(error = %e, "Failed to update path query history");
                }
            }
            Err(e) => {
                let error_str = e.to_string();
                if let Err(e2) = history.update_status(
                    &query_id,
                    "failed",
                    Some(duration_ms),
                    None,
                    Some(&error_str),
                ) {
                    warn!(error = %e2, "Failed to update path query history");
                }
            }
        }
    }

    Ok(Json(result?))
}

/// Find all users with paths to Domain Admins.
#[instrument(skip(state))]
pub async fn paths_to_domain_admins(
    State(state): State<AppState>,
    Query(params): Query<PathsToDaParams>,
) -> Result<Json<PathsToDaResponse>, ApiError> {
    // Parse excluded relationship types
    let exclude_types: Vec<String> = if params.exclude.is_empty() {
        Vec::new()
    } else {
        params
            .exclude
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    debug!(exclude = ?exclude_types, "Finding paths to Domain Admins");

    let db = state.require_db()?;
    let results: Vec<(String, String, String, usize)> =
        run_db(db, move |db| db.find_paths_to_domain_admins(&exclude_types)).await?;

    let entries: Vec<PathsToDaEntry> = results
        .into_iter()
        .map(|(id, label, name, hops)| PathsToDaEntry {
            id,
            label,
            name,
            hops,
        })
        .collect();

    let count = entries.len();
    info!(count = count, "Found users with paths to Domain Admins");

    Ok(Json(PathsToDaResponse { count, entries }))
}
