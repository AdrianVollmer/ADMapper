//! Path finding operations.

use crate::db::{DatabaseBackend, DbNode};
use crate::graph::{FullGraph, GraphEdge, GraphNode};
use serde_json::Value as JsonValue;

use super::{PathResponse, PathStep, PathsToDaEntry, PathsToDaResponse};

/// Find shortest path between two nodes.
pub fn graph_path(db: &dyn DatabaseBackend, from: &str, to: &str) -> Result<PathResponse, String> {
    // Resolve identifiers
    let from_id = db
        .resolve_node_identifier(from)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Node not found: {from}"))?;

    let to_id = db
        .resolve_node_identifier(to)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Node not found: {to}"))?;

    let path_result = db
        .shortest_path(&from_id, &to_id)
        .map_err(|e| e.to_string())?;

    match path_result {
        None => Ok(PathResponse {
            found: false,
            path: Vec::new(),
            graph: FullGraph {
                nodes: Vec::new(),
                relationships: Vec::new(),
            },
        }),
        Some(path) => {
            let node_ids: Vec<String> = path.iter().map(|(id, _)| id.clone()).collect();
            let nodes = db.get_nodes_by_ids(&node_ids).map_err(|e| e.to_string())?;

            let node_map: std::collections::HashMap<String, DbNode> = nodes
                .into_iter()
                .map(|node| (node.id.clone(), node))
                .collect();

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

            let relationships = db.get_edges_between(&node_ids).map_err(|e| e.to_string())?;

            let graph = FullGraph {
                nodes: path_steps
                    .iter()
                    .map(|s| GraphNode::from(s.node.clone()))
                    .collect(),
                relationships: relationships.into_iter().map(GraphEdge::from).collect(),
            };

            Ok(PathResponse {
                found: true,
                path: path_steps,
                graph,
            })
        }
    }
}

/// Find paths to domain admins.
pub fn paths_to_domain_admins(
    db: &dyn DatabaseBackend,
    exclude_types: &[String],
) -> Result<PathsToDaResponse, String> {
    let results = db
        .find_paths_to_domain_admins(exclude_types)
        .map_err(|e| e.to_string())?;

    let entries: Vec<PathsToDaEntry> = results
        .into_iter()
        .map(|(id, label, name, hops)| PathsToDaEntry {
            id,
            label,
            name,
            hops,
        })
        .collect();

    Ok(PathsToDaResponse {
        count: entries.len(),
        entries,
    })
}

/// Helper: Check if there's a path matching a WHERE condition.
/// Returns Some(hops) if path found, None otherwise.
pub(crate) fn check_path_to_condition(
    db: &dyn DatabaseBackend,
    node_id: &str,
    condition: &str,
) -> Result<Option<usize>, String> {
    let escaped_id = node_id.replace('\'', "\\'");
    // Use shortestPath with explicit a <> b guard to avoid Neo4j's
    // "start and end nodes are the same" error.
    let query_text = format!(
        "MATCH p = shortestPath((a)-[*1..20]->(b)) WHERE a.objectid = '{}' AND ({}) AND a <> b RETURN length(p) AS hops",
        escaped_id, condition
    );

    let result = db
        .run_custom_query(&query_text)
        .map_err(|e| e.to_string())?;
    if let Some(rows) = result.get("rows").and_then(|v| v.as_array()) {
        if let Some(first_row) = rows.first().and_then(|r| r.as_array()) {
            if let Some(hops) = first_row.first().and_then(|h| h.as_i64()) {
                return Ok(Some(hops as usize));
            }
        }
    }
    Ok(None)
}
