//! Query execution, history, settings, file browser, and import operations.

use crate::db::{DatabaseBackend, QueryLanguage};
use crate::graph::extract_graph_from_results;
use crate::history::QueryHistoryService;
use crate::import::{BloodHoundImporter, ImportProgress};
use crate::settings::{self, Settings};
use crate::state::AppState;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::api::types::QueryStatus;
use super::{BrowseEntry, BrowseResponse, QueryHistoryEntry, QueryHistoryResponse, QueryResult};

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
        .map(|row| {
            let status = match row.status.as_str() {
                "running" => QueryStatus::Running,
                "failed" => QueryStatus::Failed,
                "aborted" => QueryStatus::Aborted,
                _ => QueryStatus::Completed,
            };
            QueryHistoryEntry {
                id: row.id,
                name: row.name,
                query: row.query,
                timestamp: row.timestamp,
                result_count: row.result_count,
                status,
                started_at: row.started_at,
                duration_ms: row.duration_ms,
                error: row.error,
                background: row.background,
            }
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
) -> Result<QueryHistoryEntry, String> {
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
