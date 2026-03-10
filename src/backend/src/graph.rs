//! Graph types and extraction functions.

use crate::api::types::ApiError;
use crate::db::{DatabaseBackend, DbEdge, DbNode};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

// ============================================================================
// Graph Types
// ============================================================================

/// Graph node response format for visualization.
///
/// This is a minimal subset of `DbNode` used for graph rendering, excluding
/// the heavy `properties` field which can contain large BloodHound data.
/// Properties can be fetched on-demand when a user clicks on a node.
#[derive(Debug, Clone, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String,
}

impl From<DbNode> for GraphNode {
    fn from(node: DbNode) -> Self {
        GraphNode {
            id: node.id,
            name: node.name,
            node_type: node.label,
        }
    }
}

/// Graph relationship response format.
///
/// This is a subset of `DbEdge` used for API responses, excluding
/// internal fields like `properties`, `source_type`, and `target_type`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    #[serde(rename = "type")]
    pub rel_type: String,
}

impl From<DbEdge> for GraphEdge {
    fn from(relationship: DbEdge) -> Self {
        GraphEdge {
            source: relationship.source,
            target: relationship.target,
            rel_type: relationship.rel_type,
        }
    }
}

/// Full graph response for visualization.
///
/// Uses `GraphNode` instead of `DbNode` to avoid sending heavy properties
/// that aren't needed for graph rendering.
#[derive(Debug, Clone, Serialize)]
pub struct FullGraph {
    pub nodes: Vec<GraphNode>,
    pub relationships: Vec<GraphEdge>,
}

impl FullGraph {
    /// Build a subgraph containing only the specified nodes and relationships between them.
    pub fn from_node_ids(
        db: &Arc<dyn DatabaseBackend>,
        node_ids: &[String],
    ) -> Result<Self, ApiError> {
        if node_ids.is_empty() {
            return Ok(FullGraph {
                nodes: Vec::new(),
                relationships: Vec::new(),
            });
        }

        let nodes = db.get_nodes_by_ids(node_ids)?;
        let relationships = db.get_edges_between(node_ids)?;

        Ok(FullGraph {
            nodes: nodes.into_iter().map(GraphNode::from).collect(),
            relationships: relationships.into_iter().map(GraphEdge::from).collect(),
        })
    }
}

// ============================================================================
// Graph Extraction
// ============================================================================

/// Extract a graph from query results.
///
/// This function looks for node and relationship objects in the query results and
/// extracts them into a graph structure. It handles:
/// - Direct node/relationship objects (with `_type: "node"` or `_type: "relationship"`)
/// - Object IDs in properties (looks up nodes from the database)
pub fn extract_graph_from_results(
    results: &JsonValue,
    db: &Arc<dyn DatabaseBackend>,
) -> Result<Option<FullGraph>, ApiError> {
    let rows = match results.get("rows").and_then(|r| r.as_array()) {
        Some(rows) if !rows.is_empty() => rows,
        _ => return Ok(None),
    };

    let mut nodes: Vec<GraphNode> = Vec::new();
    let mut raw_edges: Vec<JsonValue> = Vec::new();
    let mut node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Map internal database IDs to objectids for relationship resolution
    let mut id_to_objectid: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();

    // Scan all values in all rows for node/relationship objects
    for row in rows {
        let values: Vec<&JsonValue> = if let Some(arr) = row.as_array() {
            arr.iter().collect()
        } else if let Some(obj) = row.as_object() {
            obj.values().collect()
        } else {
            continue;
        };

        for value in values {
            // Check if this is a node object
            if value.get("_type").and_then(|t| t.as_str()) == Some("node") {
                if let Some(node) = extract_node_from_json(value) {
                    // Build ID mapping for relationship resolution
                    if let Some(internal_id) = value.get("id").and_then(|v| v.as_i64()) {
                        id_to_objectid.insert(internal_id, node.id.clone());
                    }
                    if node_ids.insert(node.id.clone()) {
                        nodes.push(node);
                    }
                }
            }
            // Check if this is an relationship object - store for later processing
            else if value.get("_type").and_then(|t| t.as_str()) == Some("relationship") {
                raw_edges.push(value.clone());
            }
            // Check if this is a path object - extract nodes and relationships from it
            else if value.get("_type").and_then(|t| t.as_str()) == Some("path") {
                // Extract nodes from path
                if let Some(path_nodes) = value.get("nodes").and_then(|n| n.as_array()) {
                    for path_node in path_nodes {
                        if let Some(node) = extract_node_from_json(path_node) {
                            // Build ID mapping for relationship resolution
                            if let Some(internal_id) = path_node.get("id").and_then(|v| v.as_i64())
                            {
                                id_to_objectid.insert(internal_id, node.id.clone());
                            }
                            if node_ids.insert(node.id.clone()) {
                                nodes.push(node);
                            }
                        }
                    }
                }
                // Extract relationships from path
                if let Some(path_edges) = value.get("relationships").and_then(|e| e.as_array()) {
                    for path_edge in path_edges {
                        raw_edges.push(path_edge.clone());
                    }
                }
            }
            // Try to extract objectid from string values
            else if let Some(id) = value.as_str() {
                if !id.is_empty() {
                    node_ids.insert(id.to_string());
                }
            }
        }
    }

    // Process relationships, mapping internal IDs to objectids and deduplicating
    // Multiple paths can share the same relationship, so we need to deduplicate
    let relationships: Vec<GraphEdge> = raw_edges
        .iter()
        .filter_map(|value| extract_edge_from_json(value, &id_to_objectid))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // If we found direct node/relationship objects, use those
    if !nodes.is_empty() || !relationships.is_empty() {
        // If we have relationships but missing some nodes, fetch them
        let edge_node_ids: std::collections::HashSet<String> = relationships
            .iter()
            .flat_map(|e| vec![e.source.clone(), e.target.clone()])
            .collect();

        let missing_ids: Vec<String> = edge_node_ids.difference(&node_ids).cloned().collect();

        if !missing_ids.is_empty() {
            let additional_nodes = db.get_nodes_by_ids(&missing_ids)?;
            for node in additional_nodes {
                if node_ids.insert(node.id.clone()) {
                    nodes.push(GraphNode::from(node));
                }
            }
        }

        return Ok(Some(FullGraph {
            nodes,
            relationships,
        }));
    }

    // Fall back to looking up nodes by collected IDs
    let ids: Vec<String> = node_ids.into_iter().collect();
    if ids.is_empty() {
        return Ok(None);
    }

    FullGraph::from_node_ids(db, &ids).map(Some)
}

/// Extract a GraphNode from a JSON node object.
///
/// Only extracts the minimal fields needed for graph visualization (id, name, type).
/// Full properties are not included to keep response sizes small.
fn extract_node_from_json(value: &JsonValue) -> Option<GraphNode> {
    let objectid = value
        .get("objectid")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            // Try getting from properties
            value
                .get("properties")
                .and_then(|p| p.get("objectid"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .or_else(|| {
            value
                .get("id")
                .and_then(|v| v.as_i64())
                .map(|id| id.to_string())
        })?;

    let labels = value
        .get("labels")
        .and_then(|l| l.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let node_type_from_labels = labels.first().cloned();
    let node_type_from_props = value
        .get("properties")
        .and_then(|p| p.get("node_type"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let node_type = node_type_from_props
        .or(node_type_from_labels)
        .unwrap_or_else(|| "Unknown".to_string());

    // Try "name" first (standard property), fall back to "label" (BloodHound style)
    let name = value
        .get("properties")
        .and_then(|p| p.get("name").or_else(|| p.get("label")))
        .and_then(|l| l.as_str())
        .map(String::from)
        .unwrap_or_else(|| objectid.clone());

    Some(GraphNode {
        id: objectid,
        name,
        node_type,
    })
}

/// Extract a GraphEdge from a JSON relationship object.
///
/// Uses the id_map to convert internal database IDs to objectids.
fn extract_edge_from_json(
    value: &JsonValue,
    id_map: &std::collections::HashMap<i64, String>,
) -> Option<GraphEdge> {
    // Try to get source as string first, then as i64 and map it
    let source = value.get("source").and_then(|v| {
        v.as_str().map(String::from).or_else(|| {
            v.as_i64()
                .and_then(|id| id_map.get(&id).cloned().or_else(|| Some(id.to_string())))
        })
    })?;

    let target = value.get("target").and_then(|v| {
        v.as_str().map(String::from).or_else(|| {
            v.as_i64()
                .and_then(|id| id_map.get(&id).cloned().or_else(|| Some(id.to_string())))
        })
    })?;

    let rel_type = value
        .get("rel_type")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "RELATED".to_string());

    Some(GraphEdge {
        source,
        target,
        rel_type,
    })
}
