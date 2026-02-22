//! ADMapper application.
//!
//! Can run as either a Tauri desktop app or a standalone web service.

mod api;
mod db;
mod generate;
mod graph;
mod import;
mod settings;
mod state;

#[cfg(feature = "desktop")]
mod tauri_commands;

use api::handlers;
use axum::{routing::get, routing::post, routing::put, Router};
use std::net::SocketAddr;
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};
use tracing::{error, info};

#[cfg(feature = "desktop")]
#[cfg(debug_assertions)]
use tauri::Manager;

// Re-export public types
pub use api::types::ApiError;
pub use db::{DatabaseBackend, DatabaseType, DbEdge, DbNode};
pub use graph::{FullGraph, GraphEdge, GraphNode};
pub use state::AppState;

// Re-export database implementations for testing
#[cfg(feature = "crustdb")]
pub use db::CrustDatabase;

/// Run as Tauri desktop application.
#[cfg(feature = "desktop")]
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run_desktop(database_url: Option<&str>) {
    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_ansi(true)
        .init();

    // Create shared state
    let state = AppState::new_disconnected();

    // Connect to initial database if provided
    if let Some(url) = database_url {
        info!(url = %url, "Connecting to database from CLI argument");
        match state.connect(url) {
            Ok(db_type) => {
                if let Some(db) = state.db() {
                    let (nodes, edges) = db.get_stats().unwrap_or((0, 0));
                    info!(database = %db_type.name(), nodes = nodes, edges = edges, "Database connected");
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to database");
            }
        }
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(state)
        .setup(|_app| {
            #[cfg(debug_assertions)]
            {
                let window = _app.get_webview_window("main").unwrap();
                window.open_devtools();
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            // Database
            tauri_commands::database_status,
            tauri_commands::database_supported,
            tauri_commands::database_connect,
            tauri_commands::database_disconnect,
            // Graph stats
            tauri_commands::graph_stats,
            tauri_commands::graph_detailed_stats,
            tauri_commands::graph_clear,
            tauri_commands::graph_clear_disabled,
            // Graph data
            tauri_commands::graph_nodes,
            tauri_commands::graph_edges,
            tauri_commands::graph_all,
            tauri_commands::graph_search,
            // Node operations
            tauri_commands::node_get,
            tauri_commands::node_counts,
            tauri_commands::node_connections,
            tauri_commands::node_status,
            tauri_commands::node_set_owned,
            // Path finding
            tauri_commands::graph_path,
            tauri_commands::paths_to_domain_admins,
            // Insights
            tauri_commands::graph_insights,
            tauri_commands::graph_edge_types,
            tauri_commands::graph_node_types,
            // Mutations
            tauri_commands::add_node,
            tauri_commands::add_edge,
            // Query
            tauri_commands::graph_query,
            // Query history
            tauri_commands::get_query_history,
            tauri_commands::delete_query_history,
            tauri_commands::clear_query_history,
            // Settings
            tauri_commands::get_settings,
            tauri_commands::update_settings,
            // File browser
            tauri_commands::browse_directory,
            // Data generation
            tauri_commands::generate_data,
            // Health
            tauri_commands::health_check,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(not(feature = "desktop"))]
pub fn run_desktop(_database_url: Option<&str>) {
    eprintln!("Error: Desktop mode not available. Build with --features desktop");
    eprintln!("Or use --headless to run as web server.");
    std::process::exit(1);
}

/// Create the API router with the given state.
///
/// This is useful for integration tests that want to test the actual
/// application handlers without starting a full server.
pub fn create_api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(handlers::health_check))
        // Database connection management
        .route("/api/database/status", get(handlers::database_status))
        .route("/api/database/supported", get(handlers::database_supported))
        .route("/api/database/connect", post(handlers::database_connect))
        .route(
            "/api/database/disconnect",
            post(handlers::database_disconnect),
        )
        // Import
        .route("/api/import", post(handlers::import_bloodhound))
        .route(
            "/api/import/progress/:job_id",
            get(handlers::import_progress),
        )
        // Graph operations
        .route("/api/graph/stats", get(handlers::graph_stats))
        .route(
            "/api/graph/detailed-stats",
            get(handlers::graph_detailed_stats),
        )
        .route("/api/graph/clear", post(handlers::graph_clear))
        .route(
            "/api/graph/clear-disabled",
            post(handlers::graph_clear_disabled),
        )
        .route("/api/graph/nodes", get(handlers::graph_nodes))
        .route("/api/graph/edges", get(handlers::graph_edges))
        .route("/api/graph/all", get(handlers::graph_all))
        .route("/api/graph/search", get(handlers::graph_search))
        .route("/api/graph/node/:id", get(handlers::node_get))
        .route("/api/graph/node/:id/counts", get(handlers::node_counts))
        .route(
            "/api/graph/node/:id/connections/:direction",
            get(handlers::node_connections),
        )
        .route("/api/graph/node/:id/status", get(handlers::node_status))
        .route("/api/graph/node/:id/owned", post(handlers::node_set_owned))
        .route("/api/graph/path", get(handlers::graph_path))
        .route(
            "/api/graph/paths-to-da",
            get(handlers::paths_to_domain_admins),
        )
        .route("/api/graph/edge-types", get(handlers::graph_edge_types))
        .route("/api/graph/node-types", get(handlers::graph_node_types))
        .route("/api/graph/node", post(handlers::add_node))
        .route("/api/graph/edge", post(handlers::add_edge))
        .route("/api/graph/insights", get(handlers::graph_insights))
        .route("/api/graph/generate", post(handlers::generate_data))
        .route("/api/graph/query", post(handlers::graph_query))
        // Query progress and abort
        .route("/api/query/progress/:id", get(handlers::query_progress))
        .route("/api/query/abort/:id", post(handlers::query_abort))
        .route("/api/query/activity", get(handlers::query_activity))
        // Query history
        .route("/api/query-history", get(handlers::get_query_history))
        .route("/api/query-history", post(handlers::add_query_history))
        .route(
            "/api/query-history/:id",
            axum::routing::delete(handlers::delete_query_history),
        )
        .route(
            "/api/query-history/clear",
            post(handlers::clear_query_history),
        )
        // Settings
        .route("/api/settings", get(handlers::get_settings))
        .route("/api/settings", put(handlers::update_settings))
        // File browser
        .route("/api/browse", get(handlers::browse_directory))
        .with_state(state)
}

/// Run as standalone web service.
#[tokio::main]
pub async fn run_service(bind: &str, port: u16, database_url: Option<&str>) {
    // Initialize tracing with colors
    // RUST_LOG env var controls log level (e.g., RUST_LOG=debug or RUST_LOG=admapper=debug)
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_ansi(true)
        .init();

    let addr: SocketAddr = format!("{}:{}", bind, port)
        .parse()
        .expect("Invalid bind address");

    // Initialize state (possibly with initial database connection)
    let state = if let Some(url) = database_url {
        info!(url = %url, "Connecting to database from URL");
        let state = AppState::new_disconnected();
        match state.connect(url) {
            Ok(db_type) => {
                let db = state.db().unwrap();
                let (nodes, edges) = db.get_stats().unwrap_or((0, 0));
                info!(database = %db_type.name(), nodes = nodes, edges = edges, "Database connected");
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to database");
            }
        }
        state
    } else if let Ok(db_path) = std::env::var("ADMAPPER_DB_PATH") {
        // Legacy: support ADMAPPER_DB_PATH environment variable
        let url = format!("kuzu://{}", db_path);
        info!(path = %db_path, "Opening KuzuDB database from environment");
        let state = AppState::new_disconnected();
        match state.connect(&url) {
            Ok(_) => {
                let db = state.db().unwrap();
                let (nodes, edges) = db.get_stats().unwrap_or((0, 0));
                info!(nodes = nodes, edges = edges, "Database loaded");
            }
            Err(e) => {
                error!(error = %e, "Failed to open database");
            }
        }
        state
    } else {
        info!("Starting without database connection");
        AppState::new_disconnected()
    };

    // Start background cleanup task for completed queries
    state.spawn_query_cleanup_task();

    // Serve static files from the build directory
    let static_files = ServeDir::new("build").append_index_html_on_directories(true);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = create_api_router(state)
        .fallback_service(static_files)
        .layer(cors);

    println!("ADMapper running at http://{}", addr);
    println!("Press Ctrl+C to stop");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
