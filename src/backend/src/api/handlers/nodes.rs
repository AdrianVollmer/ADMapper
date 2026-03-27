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

use super::paths::check_path_to_condition;

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

/// Search nodes by label (for autocomplete).
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
    let nodes: Vec<DbNode> = run_db(db, move |db| db.search_nodes(&query, limit)).await?;

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
/// Checks in order (aborting early when a condition is met):
/// 1. Independent properties: owned, disabled
/// 2. Membership in Enterprise Admins (-519)
/// 3. Membership in Domain Admins (-512)
/// 4. Membership in other tier-0 groups
/// 5. Path to Enterprise Admins
/// 6. Path to Domain Admins
/// 7. Path to other tier-0 groups
#[instrument(skip(state))]
pub async fn node_status(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeStatus>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Checking node security status");

    // Well-known tier-0 SID suffixes (excluding -512 DA and -519 EA which are checked separately):
    //   -518: Schema Admins
    //   -516: Domain Controllers
    //   -498: Enterprise Read-Only Domain Controllers
    //   -S-1-5-9: Enterprise Domain Controllers
    //   -544: Administrators (local)
    //   -548: Account Operators
    //   -549: Server Operators
    //   -551: Backup Operators
    const OTHER_TIER_ZERO_RIDS: &[&str] = &[
        "-518", "-516", "-498", "-S-1-5-9", "-544", "-548", "-549", "-551",
    ];

    // === Step 1: Get node type and independent properties (owned, disabled) ===
    let node_id_clone = node_id.clone();
    let db_for_props = db.clone();
    let (node_label, owned, is_disabled) = run_db(db_for_props, move |db| {
        let nodes = db.get_nodes_by_ids(std::slice::from_ref(&node_id_clone))?;
        let node = nodes.first();

        let label = node.map(|n| n.label.to_lowercase()).unwrap_or_default();

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
        // Only applicable to users, computers, groups
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
            .unwrap_or(false);

        Ok((label, owned, is_disabled))
    })
    .await?;

    // Only run expensive membership/path checks for users, computers, and groups
    let dominated_types = ["user", "computer", "group"];
    if !dominated_types.contains(&node_label.as_str()) {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled: false, // Not applicable to domains, OUs, etc.
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: 3,
            has_path_to_high_tier: false,
            path_length: None,
        }));
    }

    // === Step 2: Check membership in Enterprise Admins (-519) ===
    let node_id_clone = node_id.clone();
    let db_for_ea = db.clone();
    let is_enterprise_admin = run_db(db_for_ea, move |db| {
        Ok(db
            .find_membership_by_sid_suffix(&node_id_clone, "-519")?
            .is_some())
    })
    .await?;

    if is_enterprise_admin {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: true,
            is_domain_admin: false,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        }));
    }

    // === Step 3: Check membership in Domain Admins (-512) ===
    let node_id_clone = node_id.clone();
    let db_for_da = db.clone();
    let is_domain_admin = run_db(db_for_da, move |db| {
        Ok(db
            .find_membership_by_sid_suffix(&node_id_clone, "-512")?
            .is_some())
    })
    .await?;

    if is_domain_admin {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: true,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        }));
    }

    // === Step 4: Check tier property and membership in other tier-0 groups ===
    let node_id_clone = node_id.clone();
    let db_for_tier = db.clone();
    let (is_tier_zero, node_tier) = run_db(db_for_tier, move |db| {
        // Check tier property first
        let nodes = db.get_nodes_by_ids(std::slice::from_ref(&node_id_clone))?;
        let tier = nodes
            .first()
            .and_then(|n| n.properties.get("tier").and_then(|v| v.as_i64()))
            .unwrap_or(3);

        if tier == 0 {
            return Ok((true, tier));
        }

        // Check membership in other tier-0 groups
        for rid in OTHER_TIER_ZERO_RIDS {
            if db
                .find_membership_by_sid_suffix(&node_id_clone, rid)?
                .is_some()
            {
                return Ok((true, tier));
            }
        }
        Ok((false, tier))
    })
    .await?;

    if is_tier_zero {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: 0,
            has_path_to_high_tier: false,
            path_length: None,
        }));
    }

    // === Step 5: Check path to any tier-0 target ===
    // Uses tier property set at import time for all privileged groups and domains
    let path_to_tier0 =
        check_path_to_condition(&state, &db, &node_id, "b.tier = 0", "tier-0").await?;
    if let Some(hops) = path_to_tier0 {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            tier: node_tier,
            has_path_to_high_tier: true,
            path_length: Some(hops),
        }));
    }

    // No tier-0 status or paths found
    Ok(Json(NodeStatus {
        owned,
        is_disabled,
        is_enterprise_admin: false,
        is_domain_admin: false,
        tier: node_tier,
        has_path_to_high_tier: false,
        path_length: None,
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
