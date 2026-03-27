//! Cache, settings, and file browser endpoints.

use super::run_db;
use crate::api::core;
use crate::api::types::{ApiError, BrowseParams, BrowseResponse};
use crate::settings::Settings;
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
    let settings = core::get_settings();
    debug!(theme = %settings.theme, layout = %settings.default_graph_layout, "Settings loaded");
    Json(settings)
}

/// Update application settings.
#[instrument(skip(body))]
pub async fn update_settings(Json(body): Json<Settings>) -> Result<Json<Settings>, ApiError> {
    let saved = core::update_settings(body).map_err(|e| {
        if e.starts_with("Failed to save") {
            ApiError::Internal(e)
        } else {
            ApiError::BadRequest(e)
        }
    })?;
    info!(theme = %saved.theme, layout = %saved.default_graph_layout, "Settings updated");
    Ok(Json(saved))
}

// ============================================================================
// File Browser
// ============================================================================

/// Browse directories on the server filesystem.
#[instrument(skip_all)]
pub async fn browse_directory(
    Query(params): Query<BrowseParams>,
) -> Result<Json<BrowseResponse>, ApiError> {
    let response = core::browse_directory(params.path.as_deref()).map_err(|e| {
        if e.starts_with("Path does not exist") {
            ApiError::NotFound(e)
        } else if e.starts_with("Path is not a directory") {
            ApiError::BadRequest(e)
        } else {
            ApiError::Internal(e)
        }
    })?;
    Ok(Json(response))
}
