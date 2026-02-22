//! Tauri command handlers for desktop IPC.
//!
//! These commands provide the same functionality as the HTTP API handlers,
//! but communicate via Tauri's IPC mechanism instead of HTTP.

use crate::api::core;
use crate::api::types::GenerateSize;
use crate::db::DbNode;
use crate::graph::{FullGraph, GraphEdge};
use crate::settings::Settings;
use crate::state::AppState;
use serde_json::Value as JsonValue;
use tauri::State;
use tracing::{debug, info};

// ============================================================================
// App Info Commands
// ============================================================================

/// Get app version.
#[tauri::command]
pub fn app_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

// ============================================================================
// Database Connection Commands
// ============================================================================

/// Get database connection status.
#[tauri::command]
pub fn database_status(state: State<'_, AppState>) -> core::DatabaseStatus {
    core::database_status(&state)
}

/// Get supported database types.
#[tauri::command]
pub fn database_supported() -> Vec<core::SupportedDatabase> {
    core::database_supported()
}

/// Connect to a database.
#[tauri::command]
pub fn database_connect(
    state: State<'_, AppState>,
    url: String,
) -> Result<core::DatabaseStatus, String> {
    info!(url = %url, "Connecting to database (IPC)");
    core::database_connect(&state, &url)
}

/// Disconnect from the database.
#[tauri::command]
pub fn database_disconnect(state: State<'_, AppState>) {
    info!("Disconnecting from database (IPC)");
    core::database_disconnect(&state);
}

// ============================================================================
// Graph Statistics Commands
// ============================================================================

/// Get graph statistics.
#[tauri::command]
pub fn graph_stats(state: State<'_, AppState>) -> Result<core::GraphStats, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_stats(db.as_ref())
}

/// Get detailed graph statistics.
#[tauri::command]
pub fn graph_detailed_stats(
    state: State<'_, AppState>,
) -> Result<crate::db::DetailedStats, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_detailed_stats(db.as_ref())
}

/// Clear all graph data.
#[tauri::command]
pub fn graph_clear(state: State<'_, AppState>) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!("Clearing database (IPC)");
    core::graph_clear(db.as_ref())
}

/// Clear disabled objects.
#[tauri::command]
pub fn graph_clear_disabled(state: State<'_, AppState>) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!("Clearing disabled objects (IPC)");
    core::graph_clear_disabled(db.as_ref())
}

// ============================================================================
// Graph Data Commands
// ============================================================================

/// Get all nodes.
#[tauri::command]
pub fn graph_nodes(state: State<'_, AppState>) -> Result<Vec<DbNode>, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_nodes(db.as_ref())
}

/// Get all edges.
#[tauri::command]
pub fn graph_edges(state: State<'_, AppState>) -> Result<Vec<GraphEdge>, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_edges(db.as_ref())
}

/// Get full graph.
#[tauri::command]
pub fn graph_all(state: State<'_, AppState>) -> Result<FullGraph, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_all(db.as_ref())
}

/// Search nodes.
#[tauri::command]
pub fn graph_search(
    state: State<'_, AppState>,
    q: String,
    limit: Option<usize>,
) -> Result<Vec<DbNode>, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!(query = %q, limit = ?limit, "Searching nodes (IPC)");
    core::graph_search(db.as_ref(), &q, limit)
}

// ============================================================================
// Node Commands
// ============================================================================

/// Get a node by ID.
#[tauri::command]
pub fn node_get(state: State<'_, AppState>, id: String) -> Result<DbNode, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::node_get(db.as_ref(), &id)
}

/// Get node connection counts.
#[tauri::command]
pub fn node_counts(state: State<'_, AppState>, id: String) -> Result<core::NodeCounts, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::node_counts(db.as_ref(), &id)
}

/// Get node connections.
#[tauri::command]
pub fn node_connections(
    state: State<'_, AppState>,
    id: String,
    direction: String,
) -> Result<FullGraph, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!(node_id = %id, direction = %direction, "Loading node connections (IPC)");
    core::node_connections(db.as_ref(), &id, &direction)
}

/// Get node security status.
#[tauri::command]
pub fn node_status(state: State<'_, AppState>, id: String) -> Result<core::NodeStatus, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!(node_id = %id, "Checking node status (IPC)");
    core::node_status_quick(db.as_ref(), &id)
}

/// Set node owned status.
#[tauri::command]
pub fn node_set_owned(state: State<'_, AppState>, id: String, owned: bool) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(node_id = %id, owned = %owned, "Setting node owned status (IPC)");
    core::node_set_owned(db.as_ref(), &id, owned)
}

// ============================================================================
// Path Finding Commands
// ============================================================================

/// Find shortest path.
#[tauri::command]
pub fn graph_path(
    state: State<'_, AppState>,
    from: String,
    to: String,
) -> Result<core::PathResponse, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!(from = %from, to = %to, "Finding path (IPC)");
    core::graph_path(db.as_ref(), &from, &to)
}

/// Find paths to domain admins.
#[tauri::command]
pub fn paths_to_domain_admins(
    state: State<'_, AppState>,
    exclude: Option<String>,
) -> Result<core::PathsToDaResponse, String> {
    let db = state.db().ok_or("Not connected to database")?;

    let exclude_types: Vec<String> = exclude
        .map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    debug!(exclude = ?exclude_types, "Finding paths to DA (IPC)");
    core::paths_to_domain_admins(db.as_ref(), &exclude_types)
}

// ============================================================================
// Insights Commands
// ============================================================================

/// Get security insights.
#[tauri::command]
pub fn graph_insights(state: State<'_, AppState>) -> Result<crate::db::SecurityInsights, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_insights(db.as_ref())
}

/// Get edge types.
#[tauri::command]
pub fn graph_edge_types(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_edge_types(db.as_ref())
}

/// Get node types.
#[tauri::command]
pub fn graph_node_types(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::graph_node_types(db.as_ref())
}

// ============================================================================
// Node/Edge Mutation Commands
// ============================================================================

/// Add a node.
#[tauri::command]
pub fn add_node(
    state: State<'_, AppState>,
    id: String,
    name: String,
    label: String,
    properties: Option<JsonValue>,
) -> Result<DbNode, String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(id = %id, name = %name, label = %label, "Adding node (IPC)");
    core::add_node(
        db.as_ref(),
        id,
        name,
        label,
        properties.unwrap_or(JsonValue::Null),
    )
}

/// Add an edge.
#[tauri::command]
pub fn add_edge(
    state: State<'_, AppState>,
    source: String,
    target: String,
    edge_type: String,
    properties: Option<JsonValue>,
) -> Result<GraphEdge, String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(source = %source, target = %target, edge_type = %edge_type, "Adding edge (IPC)");
    core::add_edge(
        db.as_ref(),
        source,
        target,
        edge_type,
        properties.unwrap_or(JsonValue::Null),
    )
}

// ============================================================================
// Query Commands
// ============================================================================

/// Execute a query synchronously.
/// For long-running queries, consider using the async HTTP endpoint with SSE.
#[tauri::command]
pub fn graph_query(
    state: State<'_, AppState>,
    query: String,
    language: Option<String>,
    extract_graph: Option<bool>,
) -> Result<core::QueryResult, String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(query = %query, "Executing query (IPC)");
    core::execute_query(
        db,
        &query,
        language.as_deref(),
        extract_graph.unwrap_or(true),
    )
}

// ============================================================================
// Query History Commands
// ============================================================================

/// Get query history.
#[tauri::command]
pub fn get_query_history(
    state: State<'_, AppState>,
    page: Option<usize>,
    per_page: Option<usize>,
) -> Result<core::QueryHistoryResponse, String> {
    let db = state.db().ok_or("Not connected to database")?;
    core::get_query_history(db.as_ref(), page.unwrap_or(1), per_page.unwrap_or(20))
}

/// Delete query history entry.
#[tauri::command]
pub fn delete_query_history(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(id = %id, "Deleting query history (IPC)");
    core::delete_query_history(db.as_ref(), &id)
}

/// Clear query history.
#[tauri::command]
pub fn clear_query_history(state: State<'_, AppState>) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!("Clearing query history (IPC)");
    core::clear_query_history(db.as_ref())
}

// ============================================================================
// Settings Commands
// ============================================================================

/// Get settings.
#[tauri::command]
pub fn get_settings() -> Settings {
    core::get_settings()
}

/// Update settings.
#[tauri::command]
pub fn update_settings(settings: Settings) -> Result<Settings, String> {
    info!(theme = %settings.theme, layout = %settings.default_graph_layout, "Updating settings (IPC)");
    core::update_settings(settings)
}

// ============================================================================
// File Browser Commands
// ============================================================================

/// Browse directory.
#[tauri::command]
pub fn browse_directory(path: Option<String>) -> Result<core::BrowseResponse, String> {
    core::browse_directory(path.as_deref())
}

// ============================================================================
// Data Generation Commands
// ============================================================================

/// Generate sample data.
#[tauri::command]
pub fn generate_data(
    state: State<'_, AppState>,
    size: String,
) -> Result<core::GenerateResponse, String> {
    let db = state.db().ok_or("Not connected to database")?;

    let size = match size.as_str() {
        "small" => GenerateSize::Small,
        "medium" => GenerateSize::Medium,
        "large" => GenerateSize::Large,
        _ => {
            return Err(format!(
                "Invalid size: {}. Must be small, medium, or large",
                size
            ))
        }
    };

    info!(size = ?size, "Generating sample data (IPC)");
    core::generate_data(db, size)
}

// ============================================================================
// Health Check
// ============================================================================

/// Health check.
#[tauri::command]
pub fn health_check() -> JsonValue {
    serde_json::json!({"status": "ok"})
}

// ============================================================================
// Import Commands
// ============================================================================

/// Import BloodHound data from file paths.
/// Used with native file dialog selection.
#[tauri::command]
pub fn import_from_paths(
    state: State<'_, AppState>,
    paths: Vec<String>,
) -> Result<core::ImportResponse, String> {
    info!(file_count = paths.len(), "Importing from paths (IPC)");

    let state_clone = state.inner().clone();
    let job_id = core::import_from_paths(&state, paths, move |progress| {
        state_clone.emit_import_progress(&progress.job_id, progress);
    })?;

    Ok(core::ImportResponse {
        job_id,
        status: "completed".to_string(),
    })
}
