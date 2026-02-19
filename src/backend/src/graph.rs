//! Graph types and extraction functions.

use crate::api::types::ApiError;
use crate::db::{DatabaseBackend, DbEdge, DbNode};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

// ============================================================================
// Graph Types
// ============================================================================

/// Graph node response format.
#[derive(Debug, Clone, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub properties: JsonValue,
}

/// Graph edge response format.
#[derive(Debug, Clone, Serialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    #[serde(rename = "type")]
    pub edge_type: String,
}

impl From<DbNode> for GraphNode {
    fn from(node: DbNode) -> Self {
        GraphNode {
            id: node.id,
            label: node.label,
            node_type: node.node_type,
            properties: node.properties,
        }
    }
}

impl From<DbEdge> for GraphEdge {
    fn from(edge: DbEdge) -> Self {
        GraphEdge {
            source: edge.source,
            target: edge.target,
            edge_type: edge.edge_type,
        }
    }
}

/// Full graph response.
#[derive(Debug, Clone, Serialize)]
pub struct FullGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

impl FullGraph {
    /// Build a subgraph containing only the specified nodes and edges between them.
    pub fn from_node_ids(
        db: &Arc<dyn DatabaseBackend>,
        node_ids: &[String],
    ) -> Result<Self, ApiError> {
        if node_ids.is_empty() {
            return Ok(FullGraph {
                nodes: Vec::new(),
                edges: Vec::new(),
            });
        }

        let nodes = db.get_nodes_by_ids(node_ids)?;
        let edges = db.get_edges_between(node_ids)?;

        Ok(FullGraph {
            nodes: nodes.into_iter().map(GraphNode::from).collect(),
            edges: edges.into_iter().map(GraphEdge::from).collect(),
        })
    }
}

// ============================================================================
// Graph Extraction
// ============================================================================

/// Extract a graph from query results.
///
/// This function looks for node and edge objects in the query results and
/// extracts them into a graph structure. It handles:
/// - Direct node/edge objects (with `_type: "node"` or `_type: "edge"`)
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
    // Map internal database IDs to object_ids for edge resolution
    let mut id_to_object_id: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();

    // Scan all values in all rows for node/edge objects
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
                    // Build ID mapping for edge resolution
                    if let Some(internal_id) = value.get("id").and_then(|v| v.as_i64()) {
                        id_to_object_id.insert(internal_id, node.id.clone());
                    }
                    if node_ids.insert(node.id.clone()) {
                        nodes.push(node);
                    }
                }
            }
            // Check if this is an edge object - store for later processing
            else if value.get("_type").and_then(|t| t.as_str()) == Some("edge") {
                raw_edges.push(value.clone());
            }
            // Check if this is a path object - extract nodes and edges from it
            else if value.get("_type").and_then(|t| t.as_str()) == Some("path") {
                // Extract nodes from path
                if let Some(path_nodes) = value.get("nodes").and_then(|n| n.as_array()) {
                    for path_node in path_nodes {
                        if let Some(node) = extract_node_from_json(path_node) {
                            // Build ID mapping for edge resolution
                            if let Some(internal_id) = path_node.get("id").and_then(|v| v.as_i64())
                            {
                                id_to_object_id.insert(internal_id, node.id.clone());
                            }
                            if node_ids.insert(node.id.clone()) {
                                nodes.push(node);
                            }
                        }
                    }
                }
                // Extract edges from path
                if let Some(path_edges) = value.get("edges").and_then(|e| e.as_array()) {
                    for path_edge in path_edges {
                        raw_edges.push(path_edge.clone());
                    }
                }
            }
            // Try to extract object_id from string values
            else if let Some(id) = value.as_str() {
                if !id.is_empty() {
                    node_ids.insert(id.to_string());
                }
            }
        }
    }

    // Process edges, mapping internal IDs to object_ids
    let edges: Vec<GraphEdge> = raw_edges
        .iter()
        .filter_map(|value| extract_edge_from_json(value, &id_to_object_id))
        .collect();

    // If we found direct node/edge objects, use those
    if !nodes.is_empty() || !edges.is_empty() {
        // If we have edges but missing some nodes, fetch them
        let edge_node_ids: std::collections::HashSet<String> = edges
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

        return Ok(Some(FullGraph { nodes, edges }));
    }

    // Fall back to looking up nodes by collected IDs
    let ids: Vec<String> = node_ids.into_iter().collect();
    if ids.is_empty() {
        return Ok(None);
    }

    FullGraph::from_node_ids(db, &ids).map(Some)
}

/// Extract a GraphNode from a JSON node object.
fn extract_node_from_json(value: &JsonValue) -> Option<GraphNode> {
    let object_id = value
        .get("object_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            // Try getting from properties
            value
                .get("properties")
                .and_then(|p| p.get("object_id"))
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

    let label = value
        .get("properties")
        .and_then(|p| p.get("label"))
        .and_then(|l| l.as_str())
        .map(String::from)
        .unwrap_or_else(|| object_id.clone());

    // Extract properties - handle nested JSON string from CrustDB storage
    let properties = extract_nested_properties(value);

    Some(GraphNode {
        id: object_id,
        label,
        node_type,
        properties,
    })
}

/// Extract a GraphEdge from a JSON edge object.
///
/// Uses the id_map to convert internal database IDs to object_ids.
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

    let edge_type = value
        .get("edge_type")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "RELATED".to_string());

    Some(GraphEdge {
        source,
        target,
        edge_type,
    })
}

/// Extract nested properties from a node JSON object.
///
/// CrustDB stores original BloodHound properties as a JSON string in the
/// `properties.properties` field. This function parses that nested JSON
/// and flattens it into the top-level properties object.
fn extract_nested_properties(value: &JsonValue) -> JsonValue {
    let props = match value.get("properties") {
        Some(p) => p,
        None => return JsonValue::Object(serde_json::Map::new()),
    };

    // Check if there's a nested "properties" field that's a JSON string
    if let Some(nested_str) = props.get("properties").and_then(|p| p.as_str()) {
        // Try to parse the nested JSON string
        if let Ok(JsonValue::Object(mut nested_props)) =
            serde_json::from_str::<JsonValue>(nested_str)
        {
            // Merge with top-level properties, preferring nested values
            // but keeping object_id, label, node_type from top level
            if let Some(object_id) = props.get("object_id") {
                nested_props.insert("object_id".to_string(), object_id.clone());
            }
            if let Some(label) = props.get("label") {
                nested_props.insert("label".to_string(), label.clone());
            }
            if let Some(node_type) = props.get("node_type") {
                nested_props.insert("node_type".to_string(), node_type.clone());
            }
            return JsonValue::Object(nested_props);
        }
    }

    // No nested properties or parsing failed - return as-is but remove the
    // "properties" key if it's a string (to avoid showing raw JSON)
    if let JsonValue::Object(mut obj) = props.clone() {
        if obj
            .get("properties")
            .map(|p| p.is_string())
            .unwrap_or(false)
        {
            obj.remove("properties");
        }
        return JsonValue::Object(obj);
    }

    props.clone()
}
