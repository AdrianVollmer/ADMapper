//! Database connection endpoints.

use crate::api::types::{ApiError, ConnectRequest, DatabaseStatus, SupportedDatabase};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::Json};
use tracing::info;

/// Health check endpoint.
pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Get current database connection status.
pub async fn database_status(State(state): State<AppState>) -> Json<DatabaseStatus> {
    let connected = state.is_connected();
    let database_type = state.database_type().map(|t| t.name().to_string());
    Json(DatabaseStatus {
        connected,
        database_type,
    })
}

/// Get list of supported database types based on compiled features.
#[allow(unused_mut, clippy::vec_init_then_push)]
pub async fn database_supported() -> Json<Vec<SupportedDatabase>> {
    let mut supported = Vec::new();

    #[cfg(feature = "crustdb")]
    supported.push(SupportedDatabase {
        id: "crustdb",
        name: "CrustDB",
        connection_type: "file",
    });

    #[cfg(feature = "neo4j")]
    supported.push(SupportedDatabase {
        id: "neo4j",
        name: "Neo4j",
        connection_type: "network",
    });

    #[cfg(feature = "falkordb")]
    supported.push(SupportedDatabase {
        id: "falkordb",
        name: "FalkorDB",
        connection_type: "network",
    });

    Json(supported)
}

/// Connect to a database.
pub async fn database_connect(
    State(state): State<AppState>,
    Json(body): Json<ConnectRequest>,
) -> Result<Json<DatabaseStatus>, ApiError> {
    info!(url = %body.url, "Connecting to database");

    state.connect(&body.url).map_err(ApiError::BadRequest)?;

    let database_type = state.database_type().map(|t| t.name().to_string());
    Ok(Json(DatabaseStatus {
        connected: true,
        database_type,
    }))
}

/// Disconnect from the current database.
pub async fn database_disconnect(State(state): State<AppState>) -> StatusCode {
    state.disconnect();
    StatusCode::NO_CONTENT
}
