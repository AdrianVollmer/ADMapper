//! Database connection and graph statistics operations.

use crate::api::types::GenerateSize;
use crate::db::DatabaseBackend;
use crate::state::AppState;
use std::sync::Arc;
use tracing::info;

use super::{DatabaseStatus, GenerateResponse, GraphStats, SupportedDatabase};

/// Get current database connection status.
pub fn database_status(state: &AppState) -> DatabaseStatus {
    DatabaseStatus {
        connected: state.is_connected(),
        database_type: state.database_type().map(|t| t.name().to_string()),
    }
}

/// Get list of supported database types.
#[allow(unused_mut, clippy::vec_init_then_push)]
pub fn database_supported() -> Vec<SupportedDatabase> {
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

    supported
}

/// Connect to a database.
pub fn database_connect(state: &AppState, url: &str) -> Result<DatabaseStatus, String> {
    state.connect(url).map_err(|e| e.to_string())?;
    Ok(DatabaseStatus {
        connected: true,
        database_type: state.database_type().map(|t| t.name().to_string()),
    })
}

/// Disconnect from the database.
pub fn database_disconnect(state: &AppState) {
    state.disconnect();
}

/// Get basic graph statistics.
pub fn graph_stats(db: &dyn DatabaseBackend) -> Result<GraphStats, String> {
    let (nodes, relationships) = db.get_stats().map_err(|e| e.to_string())?;
    Ok(GraphStats {
        nodes,
        relationships,
    })
}

/// Get detailed graph statistics.
pub fn graph_detailed_stats(db: &dyn DatabaseBackend) -> Result<crate::db::DetailedStats, String> {
    db.get_detailed_stats().map_err(|e| e.to_string())
}

/// Clear all graph data.
pub fn graph_clear(db: &dyn DatabaseBackend) -> Result<(), String> {
    db.clear().map_err(|e| e.to_string())
}

/// Clear disabled objects.
pub fn graph_clear_disabled(db: &dyn DatabaseBackend) -> Result<(), String> {
    db.run_custom_query("MATCH (n {enabled: false}) DETACH DELETE n")
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Generate sample data.
pub fn generate_data(
    db: Arc<dyn DatabaseBackend>,
    size: GenerateSize,
) -> Result<GenerateResponse, String> {
    // Check if empty
    let (node_count, edge_count) = db.get_stats().map_err(|e| e.to_string())?;
    if node_count > 0 || edge_count > 0 {
        return Err("Database must be empty to generate sample data".to_string());
    }

    // Generate
    let (nodes, relationships) = crate::generate::Generator::generate(size);
    let node_count = nodes.len();
    let edge_count = relationships.len();

    // Insert
    db.insert_nodes(&nodes).map_err(|e| e.to_string())?;
    db.insert_edges(&relationships).map_err(|e| e.to_string())?;

    info!(
        nodes = node_count,
        relationships = edge_count,
        "Generated sample data"
    );

    Ok(GenerateResponse {
        nodes: node_count,
        relationships: edge_count,
    })
}
