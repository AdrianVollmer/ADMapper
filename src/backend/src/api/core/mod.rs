//! Core API logic shared between Axum handlers and Tauri commands.
//!
//! This module contains the business logic that can be called from either
//! HTTP handlers (headless mode) or Tauri commands (desktop mode).

// Allow dead code when desktop feature is not enabled, since this module
// is primarily used by tauri_commands which requires the desktop feature.
#![allow(dead_code)]

mod database;
mod mutation;
mod nodes;
mod paths;
mod query;
mod tiers;

use crate::db::DbNode;
use crate::graph::FullGraph;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
    graph_all, graph_edge_types, graph_edges, graph_insights, graph_node_types, graph_nodes,
    graph_search, node_connections, node_counts, node_get, node_set_owned, node_status_full,
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
