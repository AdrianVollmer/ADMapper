//! ADMapper application.
//!
//! Can run as either a Tauri desktop app or a standalone web service.

mod db;
mod import;

use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        Json,
    },
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use db::GraphDatabase;
use import::{BloodHoundImporter, ImportProgress};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use std::convert::Infallible;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};

#[cfg(feature = "desktop")]
#[cfg(debug_assertions)]
use tauri::Manager;

/// Application state shared across requests.
#[derive(Clone)]
pub struct AppState {
    db: GraphDatabase,
    /// Active import jobs and their progress channels.
    import_jobs: Arc<DashMap<String, broadcast::Sender<ImportProgress>>>,
}

impl AppState {
    pub fn new(db: GraphDatabase) -> Self {
        Self {
            db,
            import_jobs: Arc::new(DashMap::new()),
        }
    }
}

/// Run as Tauri desktop application.
#[cfg(feature = "desktop")]
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run_desktop() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|_app| {
            #[cfg(debug_assertions)]
            {
                let window = _app.get_webview_window("main").unwrap();
                window.open_devtools();
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(not(feature = "desktop"))]
pub fn run_desktop() {
    eprintln!("Error: Desktop mode not available. Build with --features desktop");
    eprintln!("Or use --headless to run as web server.");
    std::process::exit(1);
}

/// Run as standalone web service.
#[tokio::main]
pub async fn run_service(bind: &str, port: u16) {
    let addr: SocketAddr = format!("{}:{}", bind, port)
        .parse()
        .expect("Invalid bind address");

    // Initialize database
    let db_path = std::env::var("ADMAPPER_DB_PATH").unwrap_or_else(|_| "admapper.db".to_string());
    let db = GraphDatabase::new(&db_path).expect("Failed to open database");
    let state = AppState::new(db);

    // Serve static files from the build directory
    let static_files = ServeDir::new("build").append_index_html_on_directories(true);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/import", post(import_bloodhound))
        .route("/api/import/progress/{job_id}", get(import_progress))
        .route("/api/graph/stats", get(graph_stats))
        .route("/api/graph/nodes", get(graph_nodes))
        .route("/api/graph/edges", get(graph_edges))
        .route("/api/graph/all", get(graph_all))
        .with_state(state)
        .fallback_service(static_files)
        .layer(cors);

    println!("ADMapper running at http://{}", addr);
    println!("Press Ctrl+C to stop");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> &'static str {
    "ok"
}

/// Handle BloodHound data import via multipart upload.
async fn import_bloodhound(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<JsonValue>, (StatusCode, String)> {
    let job_id = uuid::Uuid::new_v4().to_string();

    // Create broadcast channel for progress updates
    let (tx, _) = broadcast::channel::<ImportProgress>(100);
    state.import_jobs.insert(job_id.clone(), tx.clone());

    // Collect uploaded files
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Multipart error: {e}")))?
    {
        let filename = field.file_name().unwrap_or("unknown").to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Read error: {e}")))?
            .to_vec();
        files.push((filename, data));
    }

    if files.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No files uploaded".to_string()));
    }

    let db = state.db.clone();
    let job_id_clone = job_id.clone();
    let import_jobs = state.import_jobs.clone();

    // Spawn import task
    tokio::task::spawn_blocking(move || {
        let mut importer = BloodHoundImporter::new(db, tx);

        for (filename, data) in files {
            let result = if filename.ends_with(".zip") {
                let cursor = Cursor::new(data);
                importer.import_zip(cursor, &job_id_clone)
            } else if filename.ends_with(".json") {
                // For JSON, write to temp file and import
                let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
                let temp_path = temp_dir.path().join(&filename);
                std::fs::write(&temp_path, &data).expect("Failed to write temp file");
                importer.import_json_file(&temp_path, &job_id_clone)
            } else {
                Err(format!("Unsupported file type: {filename}"))
            };

            if let Err(e) = result {
                tracing::error!("Import failed for {}: {}", filename, e);
            }
        }

        // Clean up job after a delay
        std::thread::sleep(std::time::Duration::from_secs(60));
        import_jobs.remove(&job_id_clone);
    });

    Ok(Json(json!({
        "job_id": job_id,
        "status": "started"
    })))
}

/// SSE endpoint for import progress updates.
async fn import_progress(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)>
{
    let tx = state
        .import_jobs
        .get(&job_id)
        .map(|r| r.value().clone())
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    let rx = tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|progress| {
            let data = serde_json::to_string(&progress).unwrap_or_default();
            Ok(Event::default().data(data))
        })
    });

    Ok(Sse::new(stream))
}

/// Get graph statistics.
async fn graph_stats(State(state): State<AppState>) -> Result<Json<JsonValue>, (StatusCode, String)> {
    let (node_count, edge_count) = state
        .db
        .get_stats()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({
        "nodes": node_count,
        "edges": edge_count
    })))
}

/// Graph node response format.
#[derive(Serialize)]
struct GraphNode {
    id: String,
    label: String,
    #[serde(rename = "type")]
    node_type: String,
    properties: JsonValue,
}

/// Graph edge response format.
#[derive(Serialize)]
struct GraphEdge {
    source: String,
    target: String,
    #[serde(rename = "type")]
    edge_type: String,
}

/// Get all graph nodes.
async fn graph_nodes(State(state): State<AppState>) -> Result<Json<Vec<GraphNode>>, (StatusCode, String)> {
    let nodes = state
        .db
        .get_all_nodes()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let result: Vec<GraphNode> = nodes
        .into_iter()
        .map(|(id, label, node_type, properties)| GraphNode {
            id,
            label,
            node_type,
            properties,
        })
        .collect();

    Ok(Json(result))
}

/// Get all graph edges.
async fn graph_edges(State(state): State<AppState>) -> Result<Json<Vec<GraphEdge>>, (StatusCode, String)> {
    let edges = state
        .db
        .get_all_edges()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let result: Vec<GraphEdge> = edges
        .into_iter()
        .map(|(source, target, edge_type, _)| GraphEdge {
            source,
            target,
            edge_type,
        })
        .collect();

    Ok(Json(result))
}

/// Full graph response.
#[derive(Serialize)]
struct FullGraph {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

/// Get full graph (nodes and edges).
async fn graph_all(State(state): State<AppState>) -> Result<Json<FullGraph>, (StatusCode, String)> {
    let nodes = state
        .db
        .get_all_nodes()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let edges = state
        .db
        .get_all_edges()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let result = FullGraph {
        nodes: nodes
            .into_iter()
            .map(|(id, label, node_type, properties)| GraphNode {
                id,
                label,
                node_type,
                properties,
            })
            .collect(),
        edges: edges
            .into_iter()
            .map(|(source, target, edge_type, _)| GraphEdge {
                source,
                target,
                edge_type,
            })
            .collect(),
    };

    Ok(Json(result))
}
