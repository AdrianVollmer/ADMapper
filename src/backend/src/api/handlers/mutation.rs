//! Node and relationship CRUD mutation endpoints.

use super::run_db;
use crate::api::core;
use crate::api::types::{
    AddEdgeRequest, AddNodeRequest, ApiError, UpdateEdgeRequest, UpdateNodeRequest,
};
use crate::db::{DbEdge, DbError, DbNode};
use crate::graph::GraphEdge;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use tracing::{info, instrument};

/// Add a new node to the graph.
#[instrument(skip(state, body))]
pub async fn add_node(
    State(state): State<AppState>,
    Json(body): Json<AddNodeRequest>,
) -> Result<Json<DbNode>, ApiError> {
    // Validate inputs
    if body.id.is_empty() {
        return Err(ApiError::BadRequest("Node ID is required".to_string()));
    }
    if body.name.is_empty() {
        return Err(ApiError::BadRequest("Node name is required".to_string()));
    }
    if body.label.is_empty() {
        return Err(ApiError::BadRequest("Node label is required".to_string()));
    }

    let node = DbNode {
        id: body.id.clone(),
        name: body.name.clone(),
        label: body.label.clone(),
        properties: if body.properties.is_null() {
            serde_json::json!({})
        } else {
            body.properties
        },
    };

    let db = state.require_db()?;
    run_db(db, move |db| db.insert_node(node)).await?;
    info!(id = %body.id, name = %body.name, label = %body.label, "Node added");

    Ok(Json(DbNode {
        id: body.id,
        name: body.name,
        label: body.label,
        properties: serde_json::json!({}),
    }))
}

/// Add a new relationship to the graph.
#[instrument(skip(state, body))]
pub async fn add_edge(
    State(state): State<AppState>,
    Json(body): Json<AddEdgeRequest>,
) -> Result<Json<GraphEdge>, ApiError> {
    // Validate inputs
    if body.source.is_empty() {
        return Err(ApiError::BadRequest(
            "Source node ID is required".to_string(),
        ));
    }
    if body.target.is_empty() {
        return Err(ApiError::BadRequest(
            "Target node ID is required".to_string(),
        ));
    }
    if body.rel_type.is_empty() {
        return Err(ApiError::BadRequest(
            "Relationship type is required".to_string(),
        ));
    }

    let relationship = DbEdge {
        source: body.source.clone(),
        target: body.target.clone(),
        rel_type: body.rel_type.clone(),
        properties: if body.properties.is_null() {
            serde_json::json!({})
        } else {
            body.properties
        },
        ..Default::default()
    };

    let db = state.require_db()?;
    run_db(db, move |db| db.insert_edge(relationship)).await?;
    info!(
        source = %body.source,
        target = %body.target,
        rel_type = %body.rel_type,
        "Relationship added"
    );

    Ok(Json(GraphEdge {
        source: body.source,
        target: body.target,
        rel_type: body.rel_type,
    }))
}

/// Update a node's properties.
#[instrument(skip(state, body))]
pub async fn update_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateNodeRequest>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let id_for_log = id.clone();
    let name = body.name;
    let label = body.label;
    let properties = body.properties;
    run_db(db, move |db| {
        core::update_node(db, &id, name, label, properties).map_err(DbError::Database)
    })
    .await?;
    info!(id = %id_for_log, "Node updated");
    Ok(StatusCode::NO_CONTENT)
}

/// Update an edge's properties.
#[instrument(skip(state, body))]
pub async fn update_edge(
    State(state): State<AppState>,
    Path((source, target, rel_type)): Path<(String, String, String)>,
    Json(body): Json<UpdateEdgeRequest>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let source_for_log = source.clone();
    let target_for_log = target.clone();
    let rel_type_for_log = rel_type.clone();
    let properties = body.properties;
    run_db(db, move |db| {
        core::update_edge(db, &source, &target, &rel_type, properties).map_err(DbError::Database)
    })
    .await?;
    info!(source = %source_for_log, target = %target_for_log, rel_type = %rel_type_for_log, "Relationship updated");
    Ok(StatusCode::NO_CONTENT)
}

/// Delete a node from the graph.
#[instrument(skip(state))]
pub async fn delete_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let id_for_log = id.clone();
    run_db(db, move |db| {
        core::delete_node(db, &id).map_err(DbError::Database)
    })
    .await?;
    info!(id = %id_for_log, "Node deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Delete an relationship from the graph.
#[instrument(skip(state))]
pub async fn delete_edge(
    State(state): State<AppState>,
    Path((source, target, rel_type)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let source_for_log = source.clone();
    let target_for_log = target.clone();
    let rel_type_for_log = rel_type.clone();
    run_db(db, move |db| {
        core::delete_edge(db, &source, &target, &rel_type).map_err(DbError::Database)
    })
    .await?;
    info!(source = %source_for_log, target = %target_for_log, rel_type = %rel_type_for_log, "Relationship deleted");
    Ok(StatusCode::NO_CONTENT)
}
