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

/// Get all relationships.
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
pub async fn node_counts(
    state: State<'_, AppState>,
    id: String,
) -> Result<core::NodeCounts, String> {
    let db = state.db().ok_or("Not connected to database")?;
    tokio::task::spawn_blocking(move || core::node_counts(db.as_ref(), &id))
        .await
        .map_err(|e| format!("Task join error: {e}"))?
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

/// Get node security status (includes path finding).
#[tauri::command]
pub async fn node_status(
    state: State<'_, AppState>,
    id: String,
) -> Result<core::NodeStatus, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!(node_id = %id, "Checking node status (IPC)");
    tokio::task::spawn_blocking(move || core::node_status_full(db.as_ref(), &id))
        .await
        .map_err(|e| format!("Task join error: {e}"))?
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

/// Get relationship types.
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
// Node/Relationship Mutation Commands
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

/// Add an relationship.
#[tauri::command]
pub fn add_edge(
    state: State<'_, AppState>,
    source: String,
    target: String,
    rel_type: String,
    properties: Option<JsonValue>,
) -> Result<GraphEdge, String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(source = %source, target = %target, rel_type = %rel_type, "Adding relationship (IPC)");
    core::add_edge(
        db.as_ref(),
        source,
        target,
        rel_type,
        properties.unwrap_or(JsonValue::Null),
    )
}

/// Delete a node from the graph.
#[tauri::command]
pub fn delete_node(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(id = %id, "Deleting node (IPC)");
    core::delete_node(db.as_ref(), &id)
}

/// Delete an edge from the graph.
#[tauri::command]
pub fn delete_edge(
    state: State<'_, AppState>,
    source: String,
    target: String,
    rel_type: String,
) -> Result<(), String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!(source = %source, target = %target, rel_type = %rel_type, "Deleting edge (IPC)");
    core::delete_edge(db.as_ref(), &source, &target, &rel_type)
}

/// Get choke points in the graph.
#[tauri::command]
pub fn graph_choke_points(
    state: State<'_, AppState>,
) -> Result<crate::db::ChokePointsResponse, String> {
    let db = state.db().ok_or("Not connected to database")?;
    debug!("Getting choke points (IPC)");
    core::graph_choke_points(db.as_ref(), 50)
}

// ============================================================================
// Query Commands
// ============================================================================

/// Execute a query synchronously via Tauri IPC.
///
/// Unlike the HTTP handler which supports async mode with SSE progress events,
/// Tauri commands execute the query inline and return results directly. This
/// avoids a race condition where the query completes and emits a Tauri event
/// before the frontend has registered its event listener (late subscriber
/// problem). Since Tauri commands run on a thread pool, blocking is fine.
#[tauri::command]
pub fn graph_query(
    state: State<'_, AppState>,
    query: String,
    language: Option<String>,
    extract_graph: Option<bool>,
    background: Option<bool>,
) -> Result<crate::api::types::QueryStartResponse, String> {
    use crate::db::NewQueryHistoryEntry;

    let db = state.db().ok_or("Not connected to database")?;
    let history = state.history();
    info!(query = %query, "Executing query (IPC)");

    let query_id = uuid::Uuid::new_v4().to_string();
    let extract_graph = extract_graph.unwrap_or(true);
    let background = background.unwrap_or(false);

    let started_at = std::time::Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Add query to history with "running" status
    if let Some(ref h) = history {
        if let Err(e) = h.add(NewQueryHistoryEntry {
            id: &query_id,
            name: &query,
            query: &query,
            timestamp: started_at_unix,
            result_count: None,
            status: "running",
            started_at: started_at_unix,
            duration_ms: None,
            error: None,
            background,
        }) {
            debug!(error = %e, "Failed to add query to history");
        }
    }

    // Execute query synchronously
    let result = core::execute_query(db, &query, language.as_deref(), extract_graph);

    let duration_ms = started_at.elapsed().as_millis() as u64;

    match result {
        Ok(query_result) => {
            // Update history with completed status
            if let Some(ref h) = history {
                if let Err(e) = h.update_status(
                    &query_id,
                    "completed",
                    Some(duration_ms),
                    query_result.result_count,
                    None,
                ) {
                    debug!(error = %e, "Failed to update query history status");
                }
            }

            Ok(crate::api::types::QueryStartResponse::Sync {
                query_id,
                duration_ms,
                result_count: query_result.result_count,
                results: query_result.results,
                graph: query_result.graph,
            })
        }
        Err(e) => {
            // Update history with failed status
            if let Some(ref h) = history {
                if let Err(e2) =
                    h.update_status(&query_id, "failed", Some(duration_ms), None, Some(&e))
                {
                    debug!(error = %e2, "Failed to update query history status");
                }
            }

            Err(e)
        }
    }
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
    let history = state.history().ok_or("Not connected to database")?;
    core::get_query_history(history.as_ref(), page.unwrap_or(1), per_page.unwrap_or(20))
}

/// Delete query history entry.
#[tauri::command]
pub fn delete_query_history(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let history = state.history().ok_or("Not connected to database")?;
    info!(id = %id, "Deleting query history (IPC)");
    core::delete_query_history(history.as_ref(), &id)
}

/// Clear query history.
#[tauri::command]
pub fn clear_query_history(state: State<'_, AppState>) -> Result<(), String> {
    let history = state.history().ok_or("Not connected to database")?;
    info!("Clearing query history (IPC)");
    core::clear_query_history(history.as_ref())
}

// ============================================================================
// Cache Commands
// ============================================================================

/// Cache statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    pub supported: bool,
    pub entry_count: Option<usize>,
    pub size_bytes: Option<usize>,
}

/// Get cache statistics.
#[tauri::command]
pub fn get_cache_stats(state: State<'_, AppState>) -> Result<CacheStats, String> {
    let db = state.db().ok_or("Not connected to database")?;
    let stats = db.get_cache_stats().map_err(|e| e.to_string())?;
    match stats {
        Some((entry_count, size_bytes)) => Ok(CacheStats {
            supported: true,
            entry_count: Some(entry_count),
            size_bytes: Some(size_bytes),
        }),
        None => Ok(CacheStats {
            supported: false,
            entry_count: None,
            size_bytes: None,
        }),
    }
}

/// Clear query cache.
#[tauri::command]
pub fn clear_cache(state: State<'_, AppState>) -> Result<bool, String> {
    let db = state.db().ok_or("Not connected to database")?;
    info!("Clearing query cache (IPC)");
    db.clear_cache().map_err(|e| e.to_string())
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
// Query Activity
// ============================================================================

/// Get current query activity (number of active queries).
/// Used by the frontend to get initial state since Tauri events are push-only.
#[tauri::command]
pub fn get_query_activity(state: State<'_, AppState>) -> JsonValue {
    let active = state.active_query_count();
    serde_json::json!({"active": active})
}

// ============================================================================
// Import Commands
// ============================================================================

/// Import BloodHound data from file paths.
/// Used with native file dialog selection.
/// Returns immediately with job_id; progress is emitted via Tauri events.
#[tauri::command]
pub fn import_from_paths(
    state: State<'_, AppState>,
    paths: Vec<String>,
) -> Result<core::ImportResponse, String> {
    use crate::import::{ImportProgress, ImportStatus};

    if paths.is_empty() {
        return Err("No files selected".to_string());
    }

    info!(
        file_count = paths.len(),
        "Starting async import from paths (IPC)"
    );

    // Generate job ID and return immediately
    let job_id = uuid::Uuid::new_v4().to_string();
    let job_id_clone = job_id.clone();

    // Clone state for background thread
    let state_clone = state.inner().clone();
    let db = state.db().ok_or("Not connected to database")?.clone();

    // Emit initial progress
    let initial_progress = ImportProgress {
        job_id: job_id.clone(),
        status: ImportStatus::Running,
        total_files: paths.len(),
        files_processed: 0,
        current_file: None,
        nodes_imported: 0,
        edges_imported: 0,
        bytes_processed: 0,
        bytes_total: 0,
        error: None,
    };
    state_clone.emit_import_progress(&job_id, &initial_progress);

    // Run import in background thread
    std::thread::spawn(move || {
        use crate::import::BloodHoundImporter;
        use std::path::{Path, PathBuf};
        use tokio::sync::broadcast;

        let (tx, mut rx) = broadcast::channel::<ImportProgress>(100);
        let mut importer = BloodHoundImporter::new(db, tx);

        // Spawn a thread to forward broadcast messages to Tauri events
        let state_for_events = state_clone.clone();
        let job_id_for_events = job_id_clone.clone();
        std::thread::spawn(move || {
            while let Ok(progress) = rx.blocking_recv() {
                state_for_events.emit_import_progress(&job_id_for_events, &progress);
            }
        });

        let mut total_nodes = 0usize;
        let mut total_edges = 0usize;

        // Separate ZIP files and JSON files
        let (zip_paths, json_paths): (Vec<_>, Vec<_>) =
            paths.iter().partition(|p| p.ends_with(".zip"));

        // Process ZIP files one at a time (they handle multiple files internally)
        for path_str in &zip_paths {
            let path = Path::new(path_str);
            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            // Emit progress for current file
            let progress = ImportProgress {
                job_id: job_id_clone.clone(),
                status: ImportStatus::Running,
                total_files: paths.len(),
                files_processed: 0,
                current_file: Some(filename.clone()),
                nodes_imported: total_nodes,
                edges_imported: total_edges,
                bytes_processed: 0,
                bytes_total: 0,
                error: None,
            };
            state_clone.emit_import_progress(&job_id_clone, &progress);

            let result = match std::fs::File::open(path) {
                Ok(file) => importer.import_zip(file, &job_id_clone),
                Err(e) => Err(format!("Failed to open file: {e}")),
            };

            match result {
                Ok(file_progress) => {
                    total_nodes = file_progress.nodes_imported;
                    total_edges = file_progress.edges_imported;
                    // Emit progress from importer
                    state_clone.emit_import_progress(&job_id_clone, &file_progress);
                }
                Err(e) => {
                    let error_progress = ImportProgress {
                        job_id: job_id_clone.clone(),
                        status: ImportStatus::Failed,
                        total_files: paths.len(),
                        files_processed: 0,
                        current_file: Some(filename),
                        nodes_imported: total_nodes,
                        edges_imported: total_edges,
                        bytes_processed: 0,
                        bytes_total: 0,
                        error: Some(e),
                    };
                    state_clone.emit_import_progress(&job_id_clone, &error_progress);
                    return;
                }
            }
        }

        // Process all JSON files together with unified progress tracking
        if !json_paths.is_empty() {
            let json_files: Vec<(String, PathBuf)> = json_paths
                .iter()
                .filter(|p| p.ends_with(".json"))
                .map(|p| {
                    let path = Path::new(p);
                    let filename = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    (filename, PathBuf::from(p))
                })
                .collect();

            if !json_files.is_empty() {
                let result = importer.import_json_files(&json_files, &job_id_clone);

                match result {
                    Ok(file_progress) => {
                        total_nodes = file_progress.nodes_imported;
                        total_edges = file_progress.edges_imported;
                        // The importer already emits progress, but we need to forward to Tauri
                        state_clone.emit_import_progress(&job_id_clone, &file_progress);
                    }
                    Err(e) => {
                        let error_progress = ImportProgress {
                            job_id: job_id_clone.clone(),
                            status: ImportStatus::Failed,
                            total_files: json_files.len(),
                            files_processed: 0,
                            current_file: None,
                            nodes_imported: total_nodes,
                            edges_imported: total_edges,
                            bytes_processed: 0,
                            bytes_total: 0,
                            error: Some(e),
                        };
                        state_clone.emit_import_progress(&job_id_clone, &error_progress);
                        return;
                    }
                }
            }
        }

        // Emit completion
        let final_progress = ImportProgress {
            job_id: job_id_clone.clone(),
            status: ImportStatus::Completed,
            total_files: paths.len(),
            files_processed: paths.len(),
            current_file: None,
            nodes_imported: total_nodes,
            edges_imported: total_edges,
            bytes_processed: 0,
            bytes_total: 0,
            error: None,
        };
        state_clone.emit_import_progress(&job_id_clone, &final_progress);
    });

    // Return immediately with job_id - import runs in background
    Ok(core::ImportResponse {
        job_id,
        status: "running".to_string(),
    })
}

// ============================================================================
// File Operations
// ============================================================================

/// Write bytes to a file.
/// Used for exporting graphs to user-selected paths.
#[tauri::command]
pub fn write_file(path: String, contents: Vec<u8>) -> Result<(), String> {
    use std::io::Write;

    info!(path = %path, size = contents.len(), "Writing file (IPC)");

    let mut file =
        std::fs::File::create(&path).map_err(|e| format!("Failed to create file: {}", e))?;

    file.write_all(&contents)
        .map_err(|e| format!("Failed to write file: {}", e))?;

    Ok(())
}
