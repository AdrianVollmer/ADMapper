//! Core API logic shared between Axum handlers and Tauri commands.
//!
//! This module contains the business logic that can be called from either
//! HTTP handlers (headless mode) or Tauri commands (desktop mode).

// Allow dead code when desktop feature is not enabled, since this module
// is primarily used by tauri_commands which requires the desktop feature.
#![allow(dead_code)]

mod database;
pub mod exploit_likelihood;
pub mod mutation;
mod nodes;
pub(crate) mod paths;
mod query;
mod tiers;

use crate::graph::FullGraph;
use serde::Serialize;
use serde_json::Value as JsonValue;

// ============================================================================
// Re-exported types from api::types (canonical definitions live there)
// ============================================================================

pub use crate::api::types::{
    BrowseEntry, BrowseResponse, DatabaseStatus, GenerateResponse, NodeCounts, NodeStatus,
    PathResponse, PathStep, PathsToDaEntry, PathsToDaResponse, QueryHistoryEntry,
    QueryHistoryResponse, SupportedDatabase,
};

// ============================================================================
// Types unique to core (not duplicated in api::types)
// ============================================================================

/// Graph statistics.
#[derive(Debug, Clone, Serialize)]
pub struct GraphStats {
    pub nodes: usize,
    pub relationships: usize,
}

/// Query result for custom queries.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub results: Option<JsonValue>,
    pub graph: Option<FullGraph>,
    pub result_count: Option<i64>,
    pub duration_ms: u64,
}

/// Import response for path-based import.
#[derive(Debug, Clone, Serialize)]
pub struct ImportResponse {
    pub job_id: String,
    pub status: String,
}

// ============================================================================
// Re-exports: all public functions from submodules.
// Some are used only by tauri_commands (desktop feature) or handlers, so not
// all are referenced within this crate in every feature combination.
// ============================================================================

#[allow(unused_imports)]
pub use database::{
    database_connect, database_disconnect, database_status, database_supported, generate_data,
    graph_clear, graph_clear_disabled, graph_detailed_stats, graph_stats,
};

#[allow(unused_imports)]
pub use nodes::{
    graph_all, graph_edges, graph_insights, graph_node_types, graph_nodes,
    graph_relationship_types, graph_search, node_connections, node_counts, node_get,
    node_set_owned, node_status_full,
};

#[allow(unused_imports)]
pub use paths::{graph_path, paths_to_domain_admins};
// Used by nodes.rs within the crate
use paths::check_path_to_condition;

#[allow(unused_imports)]
pub use mutation::{
    add_edge, add_node, delete_edge, delete_node, graph_choke_points, update_edge, update_node,
};

#[allow(unused_imports)]
pub use tiers::{batch_set_tier, compute_effective_tiers, tier_violations};

#[allow(unused_imports)]
pub use query::{
    add_query_history, browse_directory, clear_query_history, delete_query_history, execute_query,
    get_query_history, get_settings, import_from_paths, update_settings,
};
