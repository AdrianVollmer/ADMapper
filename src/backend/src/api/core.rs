//! Core API logic shared between Axum handlers and Tauri commands.
//!
//! This module contains the business logic that can be called from either
//! HTTP handlers (headless mode) or Tauri commands (desktop mode).

// Allow dead code when desktop feature is not enabled, since this module
// is primarily used by tauri_commands which requires the desktop feature.
#![allow(dead_code)]

use crate::api::types::{
    BatchSetTierRequest, BatchSetTierResponse, ComputeEffectiveTiersResponse, GenerateSize,
    TierViolationCategory, TierViolationEdge, TierViolationsResponse,
};
use crate::db::{DatabaseBackend, DbEdge, DbNode, QueryLanguage};
use crate::graph::{extract_graph_from_results, FullGraph, GraphEdge, GraphNode};
use crate::history::QueryHistoryService;
use crate::import::{BloodHoundImporter, ImportProgress};
use crate::settings::{self, Settings};
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

// ============================================================================
// Shared Types
// ============================================================================

/// Database connection status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStatus {
    pub connected: bool,
    pub database_type: Option<String>,
}

/// Supported database info.
#[derive(Debug, Clone, Serialize)]
pub struct SupportedDatabase {
    pub id: &'static str,
    pub name: &'static str,
    pub connection_type: &'static str,
}

/// Graph statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStats {
    pub nodes: usize,
    pub relationships: usize,
}

/// Node connection counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCounts {
    pub incoming: usize,
    pub outgoing: usize,
    pub admin_to: usize,
    pub member_of: usize,
    pub members: usize,
}

/// Node security status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    pub owned: bool,
    pub is_disabled: bool,
    pub is_enterprise_admin: bool,
    pub is_domain_admin: bool,
    /// Tier level (0 = most critical, 3 = default)
    pub tier: i64,
    pub has_path_to_high_tier: bool,
    pub path_length: Option<usize>,
}

/// Path step in shortest path results.
#[derive(Debug, Clone, Serialize)]
pub struct PathStep {
    pub node: DbNode,
    pub rel_type: Option<String>,
}

/// Path finding response.
#[derive(Debug, Clone, Serialize)]
pub struct PathResponse {
    pub found: bool,
    pub path: Vec<PathStep>,
    pub graph: FullGraph,
}

/// Entry in paths to DA results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsToDaEntry {
    pub id: String,
    pub label: String,
    pub name: String,
    pub hops: usize,
}

/// Paths to Domain Admins response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsToDaResponse {
    pub count: usize,
    pub entries: Vec<PathsToDaEntry>,
}

/// Generate data response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateResponse {
    pub nodes: usize,
    pub relationships: usize,
}

/// Query history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHistoryEntry {
    pub id: String,
    pub name: String,
    pub query: String,
    pub timestamp: i64,
    pub result_count: Option<i64>,
    pub status: String,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    pub background: bool,
}

/// Query history response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHistoryResponse {
    pub entries: Vec<QueryHistoryEntry>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Browse entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
}

/// Browse response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseResponse {
    pub current: String,
    pub parent: Option<String>,
    pub entries: Vec<BrowseEntry>,
}

/// Query result for custom queries.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub results: Option<JsonValue>,
    pub graph: Option<FullGraph>,
    pub result_count: Option<i64>,
    pub duration_ms: u64,
}

// ============================================================================
// Database Connection
// ============================================================================

/// Get current database connection status.
pub fn database_status(state: &AppState) -> DatabaseStatus {
    DatabaseStatus {
        connected: state.is_connected(),
        database_type: state.database_type().map(|t| t.name().to_string()),
    }
}

/// Get list of supported database types.
#[allow(unused_mut, clippy::vec_init_then_push)]
pub fn database_supported() -> Vec<SupportedDatabase> {
    let mut supported = Vec::new();

    #[cfg(feature = "crustdb")]
    supported.push(SupportedDatabase {
        id: "crustdb",
        name: "CrustDB",
        connection_type: "file",
    });

    #[cfg(feature = "neo4j")]
    supported.push(SupportedDatabase {
        id: "neo4j",
        name: "Neo4j",
        connection_type: "network",
    });

    #[cfg(feature = "falkordb")]
    supported.push(SupportedDatabase {
        id: "falkordb",
        name: "FalkorDB",
        connection_type: "network",
    });

    supported
}

/// Connect to a database.
pub fn database_connect(state: &AppState, url: &str) -> Result<DatabaseStatus, String> {
    state.connect(url).map_err(|e| e.to_string())?;
    Ok(DatabaseStatus {
        connected: true,
        database_type: state.database_type().map(|t| t.name().to_string()),
    })
}

/// Disconnect from the database.
pub fn database_disconnect(state: &AppState) {
    state.disconnect();
}

// ============================================================================
// Graph Statistics
// ============================================================================

/// Get basic graph statistics.
pub fn graph_stats(db: &dyn DatabaseBackend) -> Result<GraphStats, String> {
    let (nodes, relationships) = db.get_stats().map_err(|e| e.to_string())?;
    Ok(GraphStats {
        nodes,
        relationships,
    })
}

/// Get detailed graph statistics.
pub fn graph_detailed_stats(db: &dyn DatabaseBackend) -> Result<crate::db::DetailedStats, String> {
    db.get_detailed_stats().map_err(|e| e.to_string())
}

/// Clear all graph data.
pub fn graph_clear(db: &dyn DatabaseBackend) -> Result<(), String> {
    db.clear().map_err(|e| e.to_string())
}

/// Clear disabled objects.
pub fn graph_clear_disabled(db: &dyn DatabaseBackend) -> Result<(), String> {
    db.run_custom_query("MATCH (n {enabled: false}) DETACH DELETE n")
        .map_err(|e| e.to_string())?;
    Ok(())
}

// ============================================================================
// Graph Data
// ============================================================================

/// Get all nodes.
pub fn graph_nodes(db: &dyn DatabaseBackend) -> Result<Vec<DbNode>, String> {
    db.get_all_nodes().map_err(|e| e.to_string())
}

/// Get all relationships.
pub fn graph_edges(db: &dyn DatabaseBackend) -> Result<Vec<GraphEdge>, String> {
    let relationships = db.get_all_edges().map_err(|e| e.to_string())?;
    Ok(relationships.into_iter().map(GraphEdge::from).collect())
}

/// Get full graph.
pub fn graph_all(db: &dyn DatabaseBackend) -> Result<FullGraph, String> {
    let nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let relationships = db.get_all_edges().map_err(|e| e.to_string())?;
    Ok(FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        relationships: relationships.into_iter().map(GraphEdge::from).collect(),
    })
}

/// Search nodes.
pub fn graph_search(
    db: &dyn DatabaseBackend,
    query: &str,
    limit: Option<usize>,
) -> Result<Vec<DbNode>, String> {
    if query.len() < 2 {
        return Ok(Vec::new());
    }
    db.search_nodes(query, limit.unwrap_or(50))
        .map_err(|e| e.to_string())
}

// ============================================================================
// Node Operations
// ============================================================================

/// Get a node by ID.
pub fn node_get(db: &dyn DatabaseBackend, node_id: &str) -> Result<DbNode, String> {
    let nodes = db
        .get_nodes_by_ids(&[node_id.to_string()])
        .map_err(|e| e.to_string())?;
    nodes
        .into_iter()
        .next()
        .ok_or_else(|| format!("Node not found: {node_id}"))
}

/// Get node connection counts.
pub fn node_counts(db: &dyn DatabaseBackend, node_id: &str) -> Result<NodeCounts, String> {
    let (incoming, outgoing, admin_to, member_of, members) = db
        .get_node_relationship_counts(node_id)
        .map_err(|e| e.to_string())?;
    Ok(NodeCounts {
        incoming,
        outgoing,
        admin_to,
        member_of,
        members,
    })
}

/// Get node connections in a direction.
pub fn node_connections(
    db: &dyn DatabaseBackend,
    node_id: &str,
    direction: &str,
) -> Result<FullGraph, String> {
    let (nodes, relationships) = db
        .get_node_connections(node_id, direction)
        .map_err(|e| e.to_string())?;
    Ok(FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        relationships: relationships.into_iter().map(GraphEdge::from).collect(),
    })
}

/// Get node security status with full path-finding checks.
pub fn node_status_full(db: &dyn DatabaseBackend, node_id: &str) -> Result<NodeStatus, String> {
    let nodes = db
        .get_nodes_by_ids(std::slice::from_ref(&node_id.to_string()))
        .map_err(|e| e.to_string())?;
    let node = nodes.first();

    // Get node label to check if we should do expensive membership/path checks
    let node_label = node.map(|n| n.label.to_lowercase()).unwrap_or_default();

    // Check owned status
    let owned = node
        .and_then(|n| {
            let props = &n.properties;
            props.get("owned").or(props.get("Owned")).and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_i64().map(|i| i == 1))
                    .or_else(|| v.as_str().map(|s| s == "true"))
            })
        })
        .unwrap_or(false);

    // Check if disabled (enabled=false means disabled)
    let is_disabled = node
        .and_then(|n| {
            let props = &n.properties;
            props.get("enabled").or(props.get("Enabled")).and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_i64().map(|i| i == 1))
                    .or_else(|| v.as_str().map(|s| s == "true"))
            })
        })
        .map(|enabled| !enabled) // disabled = NOT enabled
        .unwrap_or(false); // if no enabled property, assume not disabled

    // Only run expensive membership/path checks for users, computers, and groups
    let dominated_types = ["user", "computer", "group"];
    if !dominated_types.contains(&node_label.as_str()) {
        return Ok(NodeStatus {
            owned,
            is_disabled: false,
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: 3,
            has_path_to_high_tier: false,
            path_length: None,
        });
    }

    // Check if the node itself IS an Enterprise Admins or Domain Admins group
    // (by its own objectid), or if it's a MEMBER OF one.
    let is_enterprise_admin = node_id.ends_with("-519")
        || db
            .find_membership_by_sid_suffix(node_id, "-519")
            .map_err(|e| e.to_string())?
            .is_some();

    if is_enterprise_admin {
        return Ok(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: true,
            is_domain_admin: false,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        });
    }

    let is_domain_admin = node_id.ends_with("-512")
        || db
            .find_membership_by_sid_suffix(node_id, "-512")
            .map_err(|e| e.to_string())?
            .is_some();

    if is_domain_admin {
        return Ok(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: true,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        });
    }

    // Check if the node has a tier property set
    let node_tier = node
        .and_then(|n| n.properties.get("tier").and_then(|v| v.as_i64()))
        .unwrap_or(3);

    // Other tier-0 RIDs (excluding -512 DA and -519 EA which are checked above)
    const OTHER_TIER_ZERO_RIDS: &[&str] = &["-518", "-516", "-498", "-544", "-548", "-549", "-551"];

    let mut is_tier_zero = node_tier == 0;
    if !is_tier_zero {
        // Check if the node itself IS a tier-0 group (by its own objectid)
        is_tier_zero = OTHER_TIER_ZERO_RIDS
            .iter()
            .any(|rid| node_id.ends_with(rid));
    }
    if !is_tier_zero {
        for rid in OTHER_TIER_ZERO_RIDS {
            if db
                .find_membership_by_sid_suffix(node_id, rid)
                .map_err(|e| e.to_string())?
                .is_some()
            {
                is_tier_zero = true;
                break;
            }
        }
    }

    if is_tier_zero {
        return Ok(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        });
    }

    // Check path to any tier-0 target using the tier property
    // (set at import time for all privileged groups and domains)
    if let Some(hops) = check_path_to_condition(db, node_id, "b.tier = 0")? {
        return Ok(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: node_tier,
            has_path_to_high_tier: true,
            path_length: Some(hops),
        });
    }

    // No tier-0 status or paths found
    Ok(NodeStatus {
        owned,
        is_disabled,
        is_enterprise_admin: false,
        is_domain_admin: false,
        tier: node_tier,
        has_path_to_high_tier: false,
        path_length: None,
    })
}

/// Helper: Check if there's a path matching a WHERE condition.
/// Returns Some(hops) if path found, None otherwise.
fn check_path_to_condition(
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

/// Set node owned status.
pub fn node_set_owned(db: &dyn DatabaseBackend, node_id: &str, owned: bool) -> Result<(), String> {
    let escaped_id = node_id.replace('\'', "\\'");
    let query = format!(
        "MATCH (n {{objectid: '{}'}}) SET n.owned = {}",
        escaped_id, owned
    );
    db.run_custom_query(&query).map_err(|e| e.to_string())?;
    Ok(())
}

// ============================================================================
// Path Finding
// ============================================================================

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

// ============================================================================
// Insights
// ============================================================================

/// Get security insights.
pub fn graph_insights(db: &dyn DatabaseBackend) -> Result<crate::db::SecurityInsights, String> {
    db.get_security_insights().map_err(|e| e.to_string())
}

/// Get relationship types.
pub fn graph_edge_types(db: &dyn DatabaseBackend) -> Result<Vec<String>, String> {
    db.get_edge_types().map_err(|e| e.to_string())
}

/// Get node types.
pub fn graph_node_types(db: &dyn DatabaseBackend) -> Result<Vec<String>, String> {
    db.get_node_types().map_err(|e| e.to_string())
}

// ============================================================================
// Node/Relationship Mutation
// ============================================================================

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

/// Add an relationship.
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

// ============================================================================
// Tier Management
// ============================================================================

/// Reverse BFS from `root_id`, following edges of `edge_type` in reverse
/// (target -> source), returning all reached node IDs (excluding root).
fn expand_transitive(
    edges: &[DbEdge],
    root_id: &str,
    edge_type: &str,
) -> std::collections::HashSet<String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges {
        if edge.rel_type.eq_ignore_ascii_case(edge_type) {
            reverse_adj
                .entry(edge.target.as_str())
                .or_default()
                .push(edge.source.as_str());
        }
    }

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    visited.insert(root_id.to_string());
    queue.push_back(root_id.to_string());

    while let Some(current) = queue.pop_front() {
        if let Some(predecessors) = reverse_adj.get(current.as_str()) {
            for &pred in predecessors {
                if visited.insert(pred.to_string()) {
                    queue.push_back(pred.to_string());
                }
            }
        }
    }

    visited.remove(root_id);
    visited
}

/// Batch-set the tier property on nodes matching the given filters.
pub fn batch_set_tier(
    db: &dyn DatabaseBackend,
    request: BatchSetTierRequest,
) -> Result<BatchSetTierResponse, String> {
    use std::collections::HashSet;

    if !(0..=3).contains(&request.tier) {
        return Err("Tier must be between 0 and 3".into());
    }

    let all_nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let regex = request
        .name_regex
        .as_deref()
        .filter(|r| !r.is_empty())
        .map(regex::Regex::new)
        .transpose()
        .map_err(|e| format!("Invalid regex: {e}"))?;

    let mut matching_ids: HashSet<String> = all_nodes
        .iter()
        .filter(|n| {
            if let Some(ref nt) = request.node_type {
                if !n.label.eq_ignore_ascii_case(nt) {
                    return false;
                }
            }
            if let Some(ref re) = regex {
                if !re.is_match(&n.name) {
                    return false;
                }
            }
            true
        })
        .map(|n| n.id.clone())
        .collect();

    let needs_expansion = request.group_id.is_some() || request.ou_id.is_some();
    let edges = if needs_expansion {
        db.get_all_edges().map_err(|e| e.to_string())?
    } else {
        Vec::new()
    };

    if let Some(ref gid) = request.group_id {
        let members = expand_transitive(&edges, gid, "MemberOf");
        if request.node_type.is_some() || regex.is_some() {
            matching_ids = matching_ids.intersection(&members).cloned().collect();
        } else {
            matching_ids = members;
        }
    }

    if let Some(ref oid) = request.ou_id {
        let contained = expand_transitive(&edges, oid, "Contains");
        if request.node_type.is_some() || regex.is_some() || request.group_id.is_some() {
            matching_ids = matching_ids.intersection(&contained).cloned().collect();
        } else {
            matching_ids = contained;
        }
    }

    if let Some(ref ids) = request.node_ids {
        for id in ids {
            matching_ids.insert(id.clone());
        }
    }

    let final_ids: Vec<String> = matching_ids.into_iter().collect();
    let count = final_ids.len();

    for chunk in final_ids.chunks(500) {
        let ids_list: Vec<String> = chunk
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "\\'")))
            .collect();
        let query = format!(
            "MATCH (n) WHERE n.objectid IN [{}] SET n.tier = {}",
            ids_list.join(", "),
            request.tier
        );
        db.run_custom_query(&query).map_err(|e| e.to_string())?;
    }

    Ok(BatchSetTierResponse { updated: count })
}

/// Compute tier violations: edges crossing tier boundaries.
pub fn tier_violations(db: &dyn DatabaseBackend) -> Result<TierViolationsResponse, String> {
    use std::collections::HashMap;

    let nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let edges = db.get_all_edges().map_err(|e| e.to_string())?;
    let total_nodes = nodes.len();
    let total_edges = edges.len();

    let tier_map: HashMap<&str, i64> = nodes
        .iter()
        .map(|n| {
            let tier = n
                .properties
                .get("tier")
                .and_then(|v| v.as_i64())
                .unwrap_or(3);
            (n.id.as_str(), tier)
        })
        .collect();

    let max_edges = 500;
    let mut violations = Vec::new();

    for (src_label, tgt_label) in [(1i64, 0i64), (2, 1), (3, 2)] {
        let mut count = 0usize;
        let mut sample_edges = Vec::new();

        for edge in &edges {
            let src_tier = *tier_map.get(edge.source.as_str()).unwrap_or(&3);
            let tgt_tier = *tier_map.get(edge.target.as_str()).unwrap_or(&3);
            if src_tier >= src_label && tgt_tier == tgt_label {
                count += 1;
                if sample_edges.len() < max_edges {
                    sample_edges.push(TierViolationEdge {
                        source_id: edge.source.clone(),
                        target_id: edge.target.clone(),
                        rel_type: edge.rel_type.clone(),
                    });
                }
            }
        }

        violations.push(TierViolationCategory {
            source_zone: src_label,
            target_zone: tgt_label,
            count,
            edges: sample_edges,
        });
    }

    Ok(TierViolationsResponse {
        violations,
        total_nodes,
        total_edges,
    })
}

/// Compute effective tiers for all nodes using multi-source reverse BFS.
pub fn compute_effective_tiers(
    db: &dyn DatabaseBackend,
) -> Result<ComputeEffectiveTiersResponse, String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let edges = db.get_all_edges().map_err(|e| e.to_string())?;

    let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &edges {
        reverse_adj
            .entry(edge.target.as_str())
            .or_default()
            .push(edge.source.as_str());
    }

    let tier_map: HashMap<&str, i64> = nodes
        .iter()
        .map(|n| {
            let tier = n
                .properties
                .get("tier")
                .and_then(|v| v.as_i64())
                .unwrap_or(3);
            (n.id.as_str(), tier)
        })
        .collect();

    let mut effective_tier: HashMap<&str, i64> =
        nodes.iter().map(|n| (n.id.as_str(), 3i64)).collect();

    for target_tier in [0i64, 1, 2] {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for node in &nodes {
            if *tier_map.get(node.id.as_str()).unwrap_or(&3) == target_tier
                && visited.insert(node.id.as_str())
            {
                queue.push_back(node.id.as_str());
            }
        }

        while let Some(current) = queue.pop_front() {
            let eff = effective_tier.entry(current).or_insert(3);
            if target_tier < *eff {
                *eff = target_tier;
            }

            if let Some(predecessors) = reverse_adj.get(current) {
                for &pred in predecessors {
                    if visited.insert(pred) {
                        queue.push_back(pred);
                    }
                }
            }
        }
    }

    let node_tiers: Vec<(String, i64)> = effective_tier
        .iter()
        .map(|(id, tier)| (id.to_string(), *tier))
        .collect();

    for chunk in node_tiers.chunks(500) {
        let mut by_tier: HashMap<i64, Vec<String>> = HashMap::new();
        for (id, tier) in chunk {
            by_tier.entry(*tier).or_default().push(id.clone());
        }

        for (tier_val, ids) in &by_tier {
            let ids_list: Vec<String> = ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "\\'")))
                .collect();
            let query = format!(
                "MATCH (n) WHERE n.objectid IN [{}] SET n.effective_tier = {}",
                ids_list.join(", "),
                tier_val
            );
            db.run_custom_query(&query).map_err(|e| e.to_string())?;
        }
    }

    let computed = nodes.len();
    let violations = nodes
        .iter()
        .filter(|n| {
            let assigned = *tier_map.get(n.id.as_str()).unwrap_or(&3);
            let effective = *effective_tier.get(n.id.as_str()).unwrap_or(&3);
            effective < assigned
        })
        .count();

    Ok(ComputeEffectiveTiersResponse {
        computed,
        violations,
    })
}

// ============================================================================
// Query Execution (Synchronous - for simple queries)
// ============================================================================

/// Execute a query synchronously.
pub fn execute_query(
    db: Arc<dyn DatabaseBackend>,
    query: &str,
    language: Option<&str>,
    extract_graph: bool,
) -> Result<QueryResult, String> {
    let started_at = std::time::Instant::now();

    let result = if let Some(lang_str) = language {
        lang_str
            .parse::<QueryLanguage>()
            .map_err(|e| e.to_string())
            .and_then(|lang| {
                db.run_query_with_language(query, lang)
                    .map_err(|e| e.to_string())
            })
    } else {
        db.run_custom_query(query).map_err(|e| e.to_string())
    };

    let duration_ms = started_at.elapsed().as_millis() as u64;

    match result {
        Ok(results) => {
            let result_count = results
                .get("rows")
                .and_then(|r| r.as_array())
                .map(|arr| arr.len() as i64);

            let graph = if extract_graph {
                extract_graph_from_results(&results, &db).ok().flatten()
            } else {
                None
            };

            Ok(QueryResult {
                results: if graph.is_some() { None } else { Some(results) },
                graph,
                result_count,
                duration_ms,
            })
        }
        Err(e) => Err(e),
    }
}

// ============================================================================
// Query History
// ============================================================================

/// Get query history.
pub fn get_query_history(
    history: &QueryHistoryService,
    page: usize,
    per_page: usize,
) -> Result<QueryHistoryResponse, String> {
    let page = page.max(1);
    let per_page = per_page.clamp(1, 100);
    let offset = (page - 1) * per_page;

    let (history_rows, total) = history.get(per_page, offset).map_err(|e| e.to_string())?;

    let entries: Vec<QueryHistoryEntry> = history_rows
        .into_iter()
        .map(|row| QueryHistoryEntry {
            id: row.id,
            name: row.name,
            query: row.query,
            timestamp: row.timestamp,
            result_count: row.result_count,
            status: row.status,
            started_at: row.started_at,
            duration_ms: row.duration_ms,
            error: row.error,
            background: row.background,
        })
        .collect();

    Ok(QueryHistoryResponse {
        entries,
        total,
        page,
        per_page,
    })
}

/// Delete query history entry.
pub fn delete_query_history(history: &QueryHistoryService, id: &str) -> Result<(), String> {
    history.delete(id).map_err(|e| e.to_string())
}

/// Clear all query history.
pub fn clear_query_history(history: &QueryHistoryService) -> Result<(), String> {
    history.clear().map_err(|e| e.to_string())
}

/// Add a query to history.
pub fn add_query_history(
    history: &QueryHistoryService,
    body: crate::api::types::AddHistoryRequest,
) -> Result<crate::api::types::QueryHistoryEntry, String> {
    use crate::api::types::{QueryHistoryEntry, QueryStatus};
    use crate::db::NewQueryHistoryEntry;

    let id = uuid::Uuid::new_v4().to_string();
    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let status_str = body.status.as_deref().unwrap_or("completed");
    let status = match status_str {
        "running" => QueryStatus::Running,
        "failed" => QueryStatus::Failed,
        "aborted" => QueryStatus::Aborted,
        _ => QueryStatus::Completed,
    };

    history
        .add(NewQueryHistoryEntry {
            id: &id,
            name: &body.name,
            query: &body.query,
            timestamp: started_at,
            result_count: body.result_count,
            status: status_str,
            started_at,
            duration_ms: body.duration_ms,
            error: body.error.as_deref(),
            background: body.background,
        })
        .map_err(|e| e.to_string())?;

    Ok(QueryHistoryEntry {
        id,
        name: body.name,
        query: body.query,
        timestamp: started_at,
        result_count: body.result_count,
        status,
        started_at,
        duration_ms: body.duration_ms,
        error: body.error,
        background: body.background,
    })
}

// ============================================================================
// Settings
// ============================================================================

/// Get settings.
pub fn get_settings() -> Settings {
    settings::load()
}

/// Update settings.
pub fn update_settings(new_settings: Settings) -> Result<Settings, String> {
    if new_settings.theme != "dark" && new_settings.theme != "light" {
        return Err(format!(
            "Invalid theme: {}. Must be 'dark' or 'light'",
            new_settings.theme
        ));
    }

    let valid_layouts = ["force", "hierarchical", "grid", "circular"];
    if !valid_layouts.contains(&new_settings.default_graph_layout.as_str()) {
        return Err(format!(
            "Invalid layout: {}. Must be one of: {}",
            new_settings.default_graph_layout,
            valid_layouts.join(", ")
        ));
    }

    settings::save(&new_settings).map_err(|e| format!("Failed to save settings: {e}"))?;
    Ok(new_settings)
}

// ============================================================================
// File Browser
// ============================================================================

/// Browse directory.
pub fn browse_directory(path: Option<&str>) -> Result<BrowseResponse, String> {
    use std::path::PathBuf;

    let path = match path {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => dirs::home_dir().unwrap_or_else(|| PathBuf::from("/")),
    };

    if !path.exists() {
        return Err(format!("Path does not exist: {}", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("Path is not a directory: {}", path.display()));
    }

    let canonical = path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve path: {e}"))?;

    let parent = canonical.parent().map(|p| p.to_string_lossy().to_string());

    let mut entries = Vec::new();
    let read_dir =
        std::fs::read_dir(&canonical).map_err(|e| format!("Failed to read directory: {e}"))?;

    for entry in read_dir.flatten() {
        let entry_path = entry.path();
        let is_dir = entry_path.is_dir();
        let name = entry.file_name().to_string_lossy().to_string();

        if name.starts_with('.') {
            continue;
        }

        entries.push(BrowseEntry {
            name,
            path: entry_path.to_string_lossy().to_string(),
            is_dir,
        });
    }

    entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });

    Ok(BrowseResponse {
        current: canonical.to_string_lossy().to_string(),
        parent,
        entries,
    })
}

// ============================================================================
// Data Generation
// ============================================================================

/// Generate sample data.
pub fn generate_data(
    db: Arc<dyn DatabaseBackend>,
    size: GenerateSize,
) -> Result<GenerateResponse, String> {
    // Check if empty
    let (node_count, edge_count) = db.get_stats().map_err(|e| e.to_string())?;
    if node_count > 0 || edge_count > 0 {
        return Err("Database must be empty to generate sample data".to_string());
    }

    // Generate
    let (nodes, relationships) = crate::generate::Generator::generate(size);
    let node_count = nodes.len();
    let edge_count = relationships.len();

    // Insert
    db.insert_nodes(&nodes).map_err(|e| e.to_string())?;
    db.insert_edges(&relationships).map_err(|e| e.to_string())?;

    Ok(GenerateResponse {
        nodes: node_count,
        relationships: edge_count,
    })
}

// ============================================================================
// Import Functions
// ============================================================================

/// Import response for path-based import.
#[derive(Debug, Clone, Serialize)]
pub struct ImportResponse {
    pub job_id: String,
    pub status: String,
}

/// Import BloodHound data from file paths.
/// This is used by the Tauri command for desktop imports where files are selected
/// via native file dialog rather than uploaded via HTTP.
pub fn import_from_paths(
    state: &AppState,
    paths: Vec<String>,
    progress_callback: impl Fn(&ImportProgress) + Send + 'static,
) -> Result<String, String> {
    if paths.is_empty() {
        return Err("No files selected".to_string());
    }

    let db = state.require_db().map_err(|e| e.to_string())?;
    let job_id = uuid::Uuid::new_v4().to_string();

    info!(job_id = %job_id, file_count = paths.len(), "Starting import from paths");

    // Create a broadcast channel for progress (unused but required by importer)
    let (tx, _) = broadcast::channel::<ImportProgress>(100);

    let mut importer = BloodHoundImporter::new(db, tx);

    for path_str in &paths {
        let path = Path::new(path_str);
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        info!(filename = %filename, path = %path_str, "Importing file");

        let result = if path_str.ends_with(".zip") {
            match std::fs::File::open(path) {
                Ok(file) => importer.import_zip(file, &job_id),
                Err(e) => {
                    error!(error = %e, path = %path_str, "Failed to open file");
                    Err(format!("Failed to open file: {e}"))
                }
            }
        } else if path_str.ends_with(".json") {
            importer.import_json_file(path, &job_id)
        } else {
            warn!(filename = %filename, "Unsupported file type");
            Err(format!("Unsupported file type: {filename}"))
        };

        match &result {
            Ok(progress) => {
                info!(
                    filename = %filename,
                    nodes = progress.nodes_imported,
                    relationships = progress.edges_imported,
                    "File imported successfully"
                );
                progress_callback(progress);
            }
            Err(e) => {
                error!(filename = %filename, error = %e, "Import failed");
                // Create error progress and notify
                let mut error_progress = ImportProgress::new(job_id.clone());
                error_progress.fail(e.clone());
                progress_callback(&error_progress);
                return Err(e.clone());
            }
        }
    }

    Ok(job_id)
}
