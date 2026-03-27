//! Node and relationship mutation operations.

use crate::db::{DatabaseBackend, DbEdge, DbNode};
use crate::graph::GraphEdge;
use serde_json::Value as JsonValue;

/// Add a node.
pub fn add_node(
    db: &dyn DatabaseBackend,
    id: String,
    name: String,
    label: String,
    properties: JsonValue,
) -> Result<DbNode, String> {
    if id.is_empty() {
        return Err("Node ID is required".to_string());
    }
    if name.is_empty() {
        return Err("Node name is required".to_string());
    }
    if label.is_empty() {
        return Err("Node label is required".to_string());
    }

    let node = DbNode {
        id: id.clone(),
        name: name.clone(),
        label: label.clone(),
        properties: if properties.is_null() {
            serde_json::json!({})
        } else {
            properties
        },
    };

    db.insert_node(node).map_err(|e| e.to_string())?;

    Ok(DbNode {
        id,
        name,
        label,
        properties: serde_json::json!({}),
    })
}

/// Add a relationship.
pub fn add_edge(
    db: &dyn DatabaseBackend,
    source: String,
    target: String,
    rel_type: String,
    properties: JsonValue,
) -> Result<GraphEdge, String> {
    if source.is_empty() {
        return Err("Source node ID is required".to_string());
    }
    if target.is_empty() {
        return Err("Target node ID is required".to_string());
    }
    if rel_type.is_empty() {
        return Err("Relationship type is required".to_string());
    }

    let relationship = DbEdge {
        source: source.clone(),
        target: target.clone(),
        rel_type: rel_type.clone(),
        properties: if properties.is_null() {
            serde_json::json!({})
        } else {
            properties
        },
        ..Default::default()
    };

    db.insert_edge(relationship).map_err(|e| e.to_string())?;

    Ok(GraphEdge {
        source,
        target,
        rel_type,
    })
}

/// Update a node's properties.
pub fn update_node(
    db: &dyn DatabaseBackend,
    node_id: &str,
    name: Option<String>,
    label: Option<String>,
    properties: JsonValue,
) -> Result<(), String> {
    if node_id.is_empty() {
        return Err("Node ID is required".to_string());
    }

    let escaped_id = node_id.replace('\'', "\\'");

    // Build SET clauses for name/label changes
    let mut set_parts = Vec::new();
    if let Some(ref name) = name {
        let escaped = name.replace('\'', "\\'");
        set_parts.push(format!("n.name = '{}'", escaped));
    }
    if let Some(ref label) = label {
        // Label changes require removing old label and adding new one
        // For now, we just update the label property stored on the node
        let escaped = label.replace('\'', "\\'");
        set_parts.push(format!("n.label = '{}'", escaped));
    }

    // Set individual properties from the JSON object
    if let Some(obj) = properties.as_object() {
        for (key, value) in obj {
            // Sanitize key name (alphanumeric + underscore only)
            let safe_key: String = key
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if safe_key.is_empty() {
                continue;
            }
            match value {
                serde_json::Value::String(s) => {
                    let escaped = s.replace('\'', "\\'");
                    set_parts.push(format!("n.{} = '{}'", safe_key, escaped));
                }
                serde_json::Value::Number(n) => {
                    set_parts.push(format!("n.{} = {}", safe_key, n));
                }
                serde_json::Value::Bool(b) => {
                    set_parts.push(format!("n.{} = {}", safe_key, b));
                }
                serde_json::Value::Null => {
                    set_parts.push(format!("n.{} = null", safe_key));
                }
                _ => {
                    // Arrays/objects: store as JSON string
                    let escaped = value.to_string().replace('\'', "\\'");
                    set_parts.push(format!("n.{} = '{}'", safe_key, escaped));
                }
            }
        }
    }

    if set_parts.is_empty() {
        return Ok(()); // Nothing to update
    }

    let query = format!(
        "MATCH (n) WHERE n.objectid = '{}' OR n.name = '{}' SET {}",
        escaped_id,
        escaped_id,
        set_parts.join(", ")
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

/// Update an edge's properties.
pub fn update_edge(
    db: &dyn DatabaseBackend,
    source: &str,
    target: &str,
    rel_type: &str,
    properties: JsonValue,
) -> Result<(), String> {
    if source.is_empty() {
        return Err("Source node ID is required".to_string());
    }
    if target.is_empty() {
        return Err("Target node ID is required".to_string());
    }
    if rel_type.is_empty() {
        return Err("Relationship type is required".to_string());
    }

    let escaped_source = source.replace('\'', "\\'");
    let escaped_target = target.replace('\'', "\\'");
    let safe_edge_type: String = rel_type
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    // Build SET clauses from properties
    let mut set_parts = Vec::new();
    if let Some(obj) = properties.as_object() {
        for (key, value) in obj {
            let safe_key: String = key
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if safe_key.is_empty() {
                continue;
            }
            match value {
                serde_json::Value::String(s) => {
                    let escaped = s.replace('\'', "\\'");
                    set_parts.push(format!("r.{} = '{}'", safe_key, escaped));
                }
                serde_json::Value::Number(n) => {
                    set_parts.push(format!("r.{} = {}", safe_key, n));
                }
                serde_json::Value::Bool(b) => {
                    set_parts.push(format!("r.{} = {}", safe_key, b));
                }
                serde_json::Value::Null => {
                    set_parts.push(format!("r.{} = null", safe_key));
                }
                _ => {
                    let escaped = value.to_string().replace('\'', "\\'");
                    set_parts.push(format!("r.{} = '{}'", safe_key, escaped));
                }
            }
        }
    }

    if set_parts.is_empty() {
        return Ok(()); // Nothing to update
    }

    let query = format!(
        "MATCH (a)-[r:{}]->(b) WHERE (a.objectid = '{}' OR a.name = '{}') AND (b.objectid = '{}' OR b.name = '{}') SET {}",
        safe_edge_type, escaped_source, escaped_source, escaped_target, escaped_target,
        set_parts.join(", ")
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

/// Delete a node from the graph.
pub fn delete_node(db: &dyn DatabaseBackend, node_id: &str) -> Result<(), String> {
    // Escape single quotes in the ID to prevent injection
    let escaped_id = node_id.replace('\'', "\\'");
    // Use DETACH DELETE to also remove connected relationships
    let query = format!(
        "MATCH (n) WHERE n.objectid = '{}' OR n.name = '{}' DETACH DELETE n",
        escaped_id, escaped_id
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

/// Delete an edge from the graph.
pub fn delete_edge(
    db: &dyn DatabaseBackend,
    source: &str,
    target: &str,
    rel_type: &str,
) -> Result<(), String> {
    // Escape single quotes to prevent injection
    let escaped_source = source.replace('\'', "\\'");
    let escaped_target = target.replace('\'', "\\'");
    // Relationship type should be alphanumeric (relationship name)
    let safe_edge_type: String = rel_type
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let query = format!(
        "MATCH (a)-[r:{}]->(b) WHERE (a.objectid = '{}' OR a.name = '{}') AND (b.objectid = '{}' OR b.name = '{}') DELETE r",
        safe_edge_type, escaped_source, escaped_source, escaped_target, escaped_target
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

/// Get choke points in the graph using relationship betweenness centrality.
pub fn graph_choke_points(
    db: &dyn DatabaseBackend,
    limit: usize,
) -> Result<crate::db::ChokePointsResponse, String> {
    db.get_choke_points(limit).map_err(|e| e.to_string())
}
