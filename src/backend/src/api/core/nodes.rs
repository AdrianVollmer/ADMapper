//! Node and relationship basic operations.

use crate::db::{DatabaseBackend, DbNode};
use crate::graph::{FullGraph, GraphEdge, GraphNode};

use super::{check_path_to_condition, NodeCounts, NodeStatus};

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
    const OTHER_TIER_ZERO_RIDS: &[&str] = &["-518", "-516", "-498", "-S-1-5-9", "-544", "-548", "-549", "-551"];

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
