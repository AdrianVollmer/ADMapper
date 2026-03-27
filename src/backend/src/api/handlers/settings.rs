//! Cache, settings, and file browser endpoints.

use super::run_db;
use crate::api::types::{ApiError, BrowseEntry, BrowseParams, BrowseResponse};
use crate::settings::{self, Settings};
use crate::state::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
};
use tracing::{debug, info, instrument};

// ============================================================================
// Cache Endpoints
// ============================================================================

/// Cache statistics response.
#[derive(Debug, serde::Serialize)]
pub struct CacheStats {
    /// Whether the connected database supports caching.
    pub supported: bool,
    /// Number of cached entries (if supported).
    pub entry_count: Option<usize>,
    /// Total size of cached data in bytes (if supported).
    pub size_bytes: Option<usize>,
}

/// Get cache statistics.
#[instrument(skip(state))]
pub async fn get_cache_stats(State(state): State<AppState>) -> Result<Json<CacheStats>, ApiError> {
    let db = state.require_db()?;
    let stats = run_db(db, |db| db.get_cache_stats()).await?;

    match stats {
        Some((entry_count, size_bytes)) => Ok(Json(CacheStats {
            supported: true,
            entry_count: Some(entry_count),
            size_bytes: Some(size_bytes),
        })),
        None => Ok(Json(CacheStats {
            supported: false,
            entry_count: None,
            size_bytes: None,
        })),
    }
}

/// Clear query cache.
#[instrument(skip(state))]
pub async fn clear_cache(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let cleared = run_db(db, |db| db.clear_cache()).await?;
    if cleared {
        info!("Query cache cleared");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::BadRequest(
            "This database backend does not support caching".to_string(),
        ))
    }
}

// ============================================================================
// Settings Endpoints
// ============================================================================

/// Get current application settings.
pub async fn get_settings() -> Json<Settings> {
    let settings = settings::load();
    debug!(theme = %settings.theme, layout = %settings.default_graph_layout, "Settings loaded");
    Json(settings)
}

/// Update application settings.
#[instrument(skip(body))]
pub async fn update_settings(Json(body): Json<Settings>) -> Result<Json<Settings>, ApiError> {
    // Validate theme
    if body.theme != "dark" && body.theme != "light" {
        return Err(ApiError::BadRequest(format!(
            "Invalid theme: {}. Must be 'dark' or 'light'",
            body.theme
        )));
    }

    // Validate layout
    let valid_layouts = ["force", "hierarchical", "grid", "circular"];
    if !valid_layouts.contains(&body.default_graph_layout.as_str()) {
        return Err(ApiError::BadRequest(format!(
            "Invalid layout: {}. Must be one of: {}",
            body.default_graph_layout,
            valid_layouts.join(", ")
        )));
    }

    settings::save(&body)
        .map_err(|e| ApiError::Internal(format!("Failed to save settings: {e}")))?;

    info!(theme = %body.theme, layout = %body.default_graph_layout, "Settings updated");
    Ok(Json(body))
}

// ============================================================================
// File Browser
// ============================================================================

/// Browse directories on the server filesystem.
#[instrument(skip_all)]
pub async fn browse_directory(
    Query(params): Query<BrowseParams>,
) -> Result<Json<BrowseResponse>, ApiError> {
    use std::path::PathBuf;

    // Determine starting path
    let path = match &params.path {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => dirs::home_dir().unwrap_or_else(|| PathBuf::from("/")),
    };

    // Ensure path exists and is a directory
    if !path.exists() {
        return Err(ApiError::NotFound(format!(
            "Path does not exist: {}",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(ApiError::BadRequest(format!(
            "Path is not a directory: {}",
            path.display()
        )));
    }

    // Get canonical path
    let canonical = path
        .canonicalize()
        .map_err(|e| ApiError::Internal(format!("Failed to resolve path: {e}")))?;

    // Get parent directory
    let parent = canonical.parent().map(|p| p.to_string_lossy().to_string());

    // Read directory entries
    let mut entries = Vec::new();
    let read_dir = std::fs::read_dir(&canonical)
        .map_err(|e| ApiError::Internal(format!("Failed to read directory: {e}")))?;

    for entry in read_dir.flatten() {
        let entry_path = entry.path();
        let is_dir = entry_path.is_dir();
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip hidden files (starting with .)
        if name.starts_with('.') {
            continue;
        }

        entries.push(BrowseEntry {
            name,
            path: entry_path.to_string_lossy().to_string(),
            is_dir,
        });
    }

    // Sort: directories first, then alphabetically
    entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });

    Ok(Json(BrowseResponse {
        current: canonical.to_string_lossy().to_string(),
        parent,
        entries,
    }))
}
