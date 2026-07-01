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
        id,
        name,
        label,
        properties: if properties.is_null() {
            serde_json::json!({})
        } else {
            properties
        },
    };

    db.insert_node(node.clone()).map_err(|e| e.to_string())?;

    Ok(node)
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

    // Add default exploit_likelihood if not present
    let properties = if properties.is_null() {
        let likelihood = crate::exploit_likelihood::default_for(&rel_type);
        serde_json::json!({"exploit_likelihood": likelihood})
    } else if let Some(obj) = properties.as_object() {
        if !obj.contains_key("exploit_likelihood") {
            let mut new_obj = obj.clone();
            let likelihood = crate::exploit_likelihood::default_for(&rel_type);
            new_obj.insert(
                "exploit_likelihood".to_string(),
                serde_json::json!(likelihood),
            );
            serde_json::Value::Object(new_obj)
        } else {
            properties
        }
    } else {
        properties
    };

    let exploit_likelihood = properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64());

    let relationship = DbEdge {
        source: source.clone(),
        target: target.clone(),
        rel_type: rel_type.clone(),
        properties,
        ..Default::default()
    };

    db.insert_edge(relationship).map_err(|e| e.to_string())?;

    Ok(GraphEdge {
        source,
        target,
        rel_type,
        exploit_likelihood,
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
    let safe_rel_type: String = rel_type
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
        safe_rel_type, escaped_source, escaped_source, escaped_target, escaped_target,
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
    let safe_rel_type: String = rel_type
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let query = format!(
        "MATCH (a)-[r:{}]->(b) WHERE (a.objectid = '{}' OR a.name = '{}') AND (b.objectid = '{}' OR b.name = '{}') DELETE r",
        safe_rel_type, escaped_source, escaped_source, escaped_target, escaped_target
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

/// Batch edit nodes by name.
///
/// Resolves each name to a node (case-insensitive match on `name`), then
/// applies the requested action. All operations run as individual Cypher
/// queries within a single blocking task to avoid N round-trips.
pub fn batch_edit_nodes(
    db: &dyn DatabaseBackend,
    request: crate::api::types::BatchEditNodesRequest,
) -> Result<crate::api::types::BatchEditNodesResponse, String> {
    use crate::api::types::{BatchEditAction, BatchEditNodeResult};

    let mut updated = 0usize;
    let mut failed = 0usize;
    let mut results = Vec::with_capacity(request.names.len());

    for name in &request.names {
        let escaped = name.replace('\'', "\\'");

        // Resolve name to objectid
        let resolve_query = format!(
            "MATCH (n) WHERE toLower(n.name) = toLower('{}') RETURN n.objectid AS oid LIMIT 1",
            escaped
        );

        let resolve_result = match db.run_custom_query(&resolve_query) {
            Ok(v) => v,
            Err(e) => {
                failed += 1;
                results.push(BatchEditNodeResult {
                    name: name.clone(),
                    success: false,
                    node_id: None,
                    error: Some(e.to_string()),
                });
                continue;
            }
        };

        // Extract objectid from query result
        let node_id = resolve_result
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("oid"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let Some(ref oid) = node_id else {
            failed += 1;
            results.push(BatchEditNodeResult {
                name: name.clone(),
                success: false,
                node_id: None,
                error: Some("Node not found".to_string()),
            });
            continue;
        };

        let escaped_oid = oid.replace('\'', "\\'");

        let action_query = match request.action {
            BatchEditAction::MarkOwned => {
                format!("MATCH (n {{objectid: '{}'}}) SET n.owned = true", escaped_oid)
            }
            BatchEditAction::MarkNotOwned => {
                format!("MATCH (n {{objectid: '{}'}}) SET n.owned = false", escaped_oid)
            }
            BatchEditAction::SetEnabled => {
                format!("MATCH (n {{objectid: '{}'}}) SET n.enabled = true", escaped_oid)
            }
            BatchEditAction::SetDisabled => {
                format!("MATCH (n {{objectid: '{}'}}) SET n.enabled = false", escaped_oid)
            }
            BatchEditAction::Delete => {
                format!("MATCH (n {{objectid: '{}'}}) DETACH DELETE n", escaped_oid)
            }
        };

        match db.run_custom_query(&action_query) {
            Ok(_) => {
                updated += 1;
                results.push(BatchEditNodeResult {
                    name: name.clone(),
                    success: true,
                    node_id: node_id.clone(),
                    error: None,
                });
            }
            Err(e) => {
                failed += 1;
                results.push(BatchEditNodeResult {
                    name: name.clone(),
                    success: false,
                    node_id: node_id.clone(),
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Ok(crate::api::types::BatchEditNodesResponse {
        updated,
        failed,
        results,
    })
}

/// Get choke points in the graph using relationship betweenness centrality.
pub fn graph_choke_points(
    db: &dyn DatabaseBackend,
    limit: usize,
) -> Result<crate::db::ChokePointsResponse, String> {
    db.get_choke_points(limit).map_err(|e| e.to_string())
}
