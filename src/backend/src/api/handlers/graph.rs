//! Graph statistics and data management endpoints.

use super::run_db;
use crate::api::types::{ApiError, GenerateRequest, GenerateResponse};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::Json};
use serde_json::json;
use tracing::{debug, info, instrument};

/// Get graph statistics.
#[instrument(skip(state))]
pub async fn graph_stats(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = state.require_db()?;
    let (node_count, edge_count) = run_db(db, |db| db.get_stats()).await?;

    debug!(
        nodes = node_count,
        relationships = edge_count,
        "Graph stats retrieved"
    );
    Ok(Json(json!({
        "nodes": node_count,
        "relationships": edge_count
    })))
}

/// Get detailed graph statistics including counts by type.
#[instrument(skip(state))]
pub async fn graph_detailed_stats(
    State(state): State<AppState>,
) -> Result<Json<crate::db::DetailedStats>, ApiError> {
    let db = state.require_db()?;
    let stats = run_db(db, |db| db.get_detailed_stats()).await?;

    debug!(
        nodes = stats.total_nodes,
        relationships = stats.total_edges,
        users = stats.users,
        computers = stats.computers,
        "Detailed stats retrieved"
    );
    Ok(Json(stats))
}

/// Clear all graph data from the database.
#[instrument(skip(state))]
pub async fn graph_clear(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    run_db(db, |db| db.clear()).await?;
    info!("Database cleared");
    Ok(StatusCode::NO_CONTENT)
}

/// Clear all disabled objects (nodes with enabled=false) from the database.
#[instrument(skip(state))]
pub async fn graph_clear_disabled(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    run_db(db, |db| {
        // Execute Cypher query to delete disabled nodes and their relationships
        db.run_custom_query("MATCH (n {enabled: false}) DETACH DELETE n")
    })
    .await?;

    info!("Cleared disabled objects from database");
    Ok(StatusCode::NO_CONTENT)
}

/// Generate sample Active Directory data.
/// Only works if the database is empty.
#[instrument(skip(state))]
pub async fn generate_data(
    State(state): State<AppState>,
    Json(body): Json<GenerateRequest>,
) -> Result<Json<GenerateResponse>, ApiError> {
    let db = state.require_db()?;

    // Check if database is empty
    let (node_count, edge_count) = run_db(db.clone(), |db| db.get_stats()).await?;

    if node_count > 0 || edge_count > 0 {
        return Err(ApiError::BadRequest(
            "Database must be empty to generate sample data".to_string(),
        ));
    }

    info!(size = ?body.size, "Generating sample data");

    // Generate data
    let size = body.size;
    let (nodes, relationships) =
        tokio::task::spawn_blocking(move || crate::generate::Generator::generate(size))
            .await
            .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?;

    let node_count = nodes.len();
    let edge_count = relationships.len();

    info!(
        nodes = node_count,
        relationships = edge_count,
        "Generated sample data, inserting..."
    );

    // Insert nodes
    run_db(db.clone(), move |db| db.insert_nodes(&nodes)).await?;

    // Insert relationships
    run_db(db, move |db| db.insert_edges(&relationships)).await?;

    info!(
        nodes = node_count,
        relationships = edge_count,
        "Sample data generation complete"
    );

    Ok(Json(GenerateResponse {
        nodes: node_count,
        relationships: edge_count,
    }))
}
