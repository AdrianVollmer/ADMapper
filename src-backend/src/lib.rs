//! ADMapper application.
//!
//! Can run as either a Tauri desktop app or a standalone web service.

mod db;
mod import;

use axum::{
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Json, Response,
    },
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use db::DbError;
use import::{BloodHoundImporter, ImportProgress};
use parking_lot::RwLock;
use serde::Deserialize;
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
use tracing::{debug, error, info, instrument, warn};

#[cfg(feature = "desktop")]
#[cfg(debug_assertions)]
use tauri::Manager;

// ============================================================================
// API Error Type
// ============================================================================

/// API error type with automatic response conversion.
#[derive(Debug)]
pub enum ApiError {
    /// Database operation failed
    Database(DbError),
    /// Invalid request from client
    BadRequest(String),
    /// Requested resource not found
    NotFound(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Database(e) => write!(f, "Database error: {e}"),
            ApiError::BadRequest(msg) => write!(f, "Bad request: {msg}"),
            ApiError::NotFound(msg) => write!(f, "Not found: {msg}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<DbError> for ApiError {
    fn from(e: DbError) -> Self {
        error!(error = %e, "Database error");
        ApiError::Database(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Database(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
        };

        (status, message).into_response()
    }
}

// ============================================================================
// Application State
// ============================================================================

/// Import job state: channel for live updates + optional final state for late subscribers.
pub struct ImportJob {
    pub channel: broadcast::Sender<ImportProgress>,
    pub final_state: RwLock<Option<ImportProgress>>,
}

/// Application state shared across requests.
#[derive(Clone)]
pub struct AppState {
    db: GraphDatabase,
    /// Active import jobs and their progress channels.
    import_jobs: Arc<DashMap<String, Arc<ImportJob>>>,
}

impl AppState {
    pub fn new(db: GraphDatabase) -> Self {
        Self {
            db,
            import_jobs: Arc::new(DashMap::new()),
        }
    }

    /// Get a reference to the database (for testing).
    pub fn db(&self) -> &GraphDatabase {
        &self.db
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

// Re-export database types for tests
pub use db::{DbEdge, DbNode, GraphDatabase};

/// Create the API router with the given state.
///
/// This is useful for integration tests that want to test the actual
/// application handlers without starting a full server.
pub fn create_api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/import", post(import_bloodhound))
        .route("/api/import/progress/:job_id", get(import_progress))
        .route("/api/graph/stats", get(graph_stats))
        .route("/api/graph/detailed-stats", get(graph_detailed_stats))
        .route("/api/graph/clear", post(graph_clear))
        .route("/api/graph/nodes", get(graph_nodes))
        .route("/api/graph/edges", get(graph_edges))
        .route("/api/graph/all", get(graph_all))
        .route("/api/graph/search", get(graph_search))
        .route("/api/graph/path", get(graph_path))
        .route("/api/graph/paths-to-da", get(paths_to_domain_admins))
        .route("/api/graph/edge-types", get(graph_edge_types))
        .route("/api/graph/node-types", get(graph_node_types))
        .route("/api/graph/node", post(add_node))
        .route("/api/graph/edge", post(add_edge))
        .route("/api/graph/insights", get(graph_insights))
        .route("/api/graph/query", post(graph_query))
        .route("/api/query-history", get(get_query_history))
        .route("/api/query-history", post(add_query_history))
        .route(
            "/api/query-history/:id",
            axum::routing::delete(delete_query_history),
        )
        .route("/api/query-history/clear", post(clear_query_history))
        .with_state(state)
}

/// Run as standalone web service.
#[tokio::main]
pub async fn run_service(bind: &str, port: u16) {
    // Initialize tracing with colors
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_target(true)
        .with_ansi(true)
        .init();

    let addr: SocketAddr = format!("{}:{}", bind, port)
        .parse()
        .expect("Invalid bind address");

    // Initialize database (KuzuDB uses a directory)
    let db_path = std::env::var("ADMAPPER_DB_PATH").unwrap_or_else(|_| "admapper_kuzu".to_string());
    info!(path = %db_path, "Opening KuzuDB database");
    let db = GraphDatabase::new(&db_path).expect("Failed to open database");
    let (nodes, edges) = db.get_stats().unwrap_or((0, 0));
    info!(nodes = nodes, edges = edges, "Database loaded");
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
        .route("/api/import/progress/:job_id", get(import_progress))
        .route("/api/graph/stats", get(graph_stats))
        .route("/api/graph/detailed-stats", get(graph_detailed_stats))
        .route("/api/graph/clear", post(graph_clear))
        .route("/api/graph/nodes", get(graph_nodes))
        .route("/api/graph/edges", get(graph_edges))
        .route("/api/graph/all", get(graph_all))
        .route("/api/graph/search", get(graph_search))
        .route("/api/graph/path", get(graph_path))
        .route("/api/graph/paths-to-da", get(paths_to_domain_admins))
        .route("/api/graph/edge-types", get(graph_edge_types))
        .route("/api/graph/node-types", get(graph_node_types))
        .route("/api/graph/node", post(add_node))
        .route("/api/graph/edge", post(add_edge))
        .route("/api/graph/insights", get(graph_insights))
        .route("/api/graph/query", post(graph_query))
        .route("/api/query-history", get(get_query_history))
        .route("/api/query-history", post(add_query_history))
        .route(
            "/api/query-history/:id",
            axum::routing::delete(delete_query_history),
        )
        .route("/api/query-history/clear", post(clear_query_history))
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
#[instrument(skip(state, multipart))]
async fn import_bloodhound(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<JsonValue>, ApiError> {
    let job_id = uuid::Uuid::new_v4().to_string();
    info!(job_id = %job_id, "Starting import job");

    // Create broadcast channel for progress updates
    let (tx, _) = broadcast::channel::<ImportProgress>(100);
    let job = Arc::new(ImportJob {
        channel: tx.clone(),
        final_state: RwLock::new(None),
    });
    state.import_jobs.insert(job_id.clone(), job.clone());

    // Collect uploaded files
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        error!(error = %e, "Multipart read error");
        ApiError::BadRequest(format!("Multipart error: {e}"))
    })? {
        let filename = field.file_name().unwrap_or("unknown").to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| {
                error!(error = %e, filename = %filename, "Failed to read file data");
                ApiError::BadRequest(format!("Read error: {e}"))
            })?
            .to_vec();
        debug!(filename = %filename, size = data.len(), "Received file");
        files.push((filename, data));
    }

    if files.is_empty() {
        warn!("Import request with no files");
        return Err(ApiError::BadRequest("No files uploaded".to_string()));
    }

    info!(file_count = files.len(), "Processing uploaded files");

    let db = state.db.clone();
    let job_id_clone = job_id.clone();
    let import_jobs = state.import_jobs.clone();
    let job_for_task = job.clone();

    // Spawn import task
    tokio::task::spawn_blocking(move || {
        let mut importer = BloodHoundImporter::new(db, tx);
        let mut final_progress: Option<ImportProgress> = None;

        for (filename, data) in files {
            info!(filename = %filename, size = data.len(), "Importing file");
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
                warn!(filename = %filename, "Unsupported file type");
                Err(format!("Unsupported file type: {filename}"))
            };

            match &result {
                Ok(progress) => {
                    info!(
                        filename = %filename,
                        nodes = progress.nodes_imported,
                        edges = progress.edges_imported,
                        "File imported successfully"
                    );
                    final_progress = Some(progress.clone());
                }
                Err(e) => {
                    error!(filename = %filename, error = %e, "Import failed");
                }
            }
        }

        // Store final state for late subscribers
        if let Some(progress) = final_progress {
            *job_for_task.final_state.write() = Some(progress);
        }

        debug!(job_id = %job_id_clone, "Import job complete, cleanup scheduled");
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
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    use futures::future::Either;

    let job = state
        .import_jobs
        .get(&job_id)
        .map(|r| r.value().clone())
        .ok_or_else(|| ApiError::NotFound("Job not found".to_string()))?;

    // Check if import already completed (late subscriber)
    let final_state = job.final_state.read().clone();
    if let Some(progress) = final_state {
        debug!(job_id = %job_id, "Sending cached final state to late subscriber");
        let data = serde_json::to_string(&progress).unwrap_or_default();
        let stream = Either::Left(tokio_stream::once(Ok(Event::default().data(data))));
        return Ok(Sse::new(stream));
    }

    // Subscribe to live updates
    let rx = job.channel.subscribe();
    let stream = Either::Right(BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|progress| {
            let data = serde_json::to_string(&progress).unwrap_or_default();
            Ok(Event::default().data(data))
        })
    }));

    Ok(Sse::new(stream))
}

/// Get graph statistics.
#[instrument(skip(state))]
async fn graph_stats(State(state): State<AppState>) -> Result<Json<JsonValue>, ApiError> {
    let (node_count, edge_count) = state.db.get_stats()?;

    debug!(
        nodes = node_count,
        edges = edge_count,
        "Graph stats retrieved"
    );
    Ok(Json(json!({
        "nodes": node_count,
        "edges": edge_count
    })))
}

/// Get detailed graph statistics including counts by type.
#[instrument(skip(state))]
async fn graph_detailed_stats(
    State(state): State<AppState>,
) -> Result<Json<db::DetailedStats>, ApiError> {
    let stats = state.db.get_detailed_stats()?;
    debug!(
        nodes = stats.total_nodes,
        edges = stats.total_edges,
        users = stats.users,
        computers = stats.computers,
        "Detailed stats retrieved"
    );
    Ok(Json(stats))
}

/// Clear all graph data from the database.
#[instrument(skip(state))]
async fn graph_clear(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state.db.clear()?;
    info!("Database cleared");
    Ok(StatusCode::NO_CONTENT)
}

/// Graph node response format.
#[derive(Serialize, Clone)]
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

impl From<DbNode> for GraphNode {
    fn from(node: DbNode) -> Self {
        GraphNode {
            id: node.id,
            label: node.label,
            node_type: node.node_type,
            properties: node.properties,
        }
    }
}

impl From<DbEdge> for GraphEdge {
    fn from(edge: DbEdge) -> Self {
        GraphEdge {
            source: edge.source,
            target: edge.target,
            edge_type: edge.edge_type,
        }
    }
}

/// Get all graph nodes.
async fn graph_nodes(State(state): State<AppState>) -> Result<Json<Vec<GraphNode>>, ApiError> {
    let nodes = state.db.get_all_nodes()?;
    let result: Vec<GraphNode> = nodes.into_iter().map(GraphNode::from).collect();
    Ok(Json(result))
}

/// Get all graph edges.
async fn graph_edges(State(state): State<AppState>) -> Result<Json<Vec<GraphEdge>>, ApiError> {
    let edges = state.db.get_all_edges()?;
    let result: Vec<GraphEdge> = edges.into_iter().map(GraphEdge::from).collect();
    Ok(Json(result))
}

/// Full graph response.
#[derive(Serialize)]
struct FullGraph {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

impl FullGraph {
    /// Build a subgraph containing only the specified nodes and edges between them.
    fn from_node_ids(db: &GraphDatabase, node_ids: &[String]) -> Result<Self, ApiError> {
        if node_ids.is_empty() {
            return Ok(FullGraph {
                nodes: Vec::new(),
                edges: Vec::new(),
            });
        }

        let nodes = db.get_nodes_by_ids(node_ids)?;
        let edges = db.get_edges_between(node_ids)?;

        Ok(FullGraph {
            nodes: nodes.into_iter().map(GraphNode::from).collect(),
            edges: edges.into_iter().map(GraphEdge::from).collect(),
        })
    }
}

/// Get full graph (nodes and edges).
async fn graph_all(State(state): State<AppState>) -> Result<Json<FullGraph>, ApiError> {
    let nodes = state.db.get_all_nodes()?;
    let edges = state.db.get_all_edges()?;

    let result = FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        edges: edges.into_iter().map(GraphEdge::from).collect(),
    };

    Ok(Json(result))
}

/// Search query parameters.
#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

/// Search nodes by label (for autocomplete).
#[instrument(skip(state))]
async fn graph_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<GraphNode>>, ApiError> {
    if params.q.len() < 2 {
        return Ok(Json(Vec::new()));
    }

    let nodes = state.db.search_nodes(&params.q, params.limit)?;
    let result: Vec<GraphNode> = nodes.into_iter().map(GraphNode::from).collect();

    debug!(query = %params.q, results = result.len(), "Search complete");
    Ok(Json(result))
}

/// Path query parameters.
#[derive(Debug, Deserialize)]
struct PathParams {
    from: String,
    to: String,
}

/// Path step in the response.
#[derive(Serialize)]
struct PathStep {
    node: GraphNode,
    #[serde(skip_serializing_if = "Option::is_none")]
    edge_type: Option<String>,
}

/// Path response with full graph for rendering.
#[derive(Serialize)]
struct PathResponse {
    found: bool,
    path: Vec<PathStep>,
    graph: FullGraph,
}

/// Find shortest path between two nodes.
/// Accepts either object IDs or labels as identifiers.
#[instrument(skip(state))]
async fn graph_path(
    State(state): State<AppState>,
    Query(params): Query<PathParams>,
) -> Result<Json<PathResponse>, ApiError> {
    // Resolve identifiers to object IDs (supports both IDs and labels)
    let from_id = state
        .db
        .resolve_node_identifier(&params.from)?
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {}", params.from)))?;

    let to_id = state
        .db
        .resolve_node_identifier(&params.to)?
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {}", params.to)))?;

    let path_result = state.db.shortest_path(&from_id, &to_id)?;

    match path_result {
        None => {
            debug!(from = %from_id, to = %to_id, "No path found");
            Ok(Json(PathResponse {
                found: false,
                path: Vec::new(),
                graph: FullGraph {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                },
            }))
        }
        Some(path) => {
            // Get node IDs from path
            let node_ids: Vec<String> = path.iter().map(|(id, _)| id.clone()).collect();

            // Get full node data
            let nodes = state.db.get_nodes_by_ids(&node_ids)?;

            // Build node lookup
            let node_map: std::collections::HashMap<String, DbNode> = nodes
                .into_iter()
                .map(|node| (node.id.clone(), node))
                .collect();

            // Build path steps
            let path_steps: Vec<PathStep> = path
                .iter()
                .map(|(id, edge_type)| {
                    let node = node_map.get(id).cloned().unwrap_or_else(|| DbNode {
                        id: id.clone(),
                        label: id.clone(),
                        node_type: "Unknown".to_string(),
                        properties: JsonValue::Null,
                    });
                    PathStep {
                        node: GraphNode::from(node),
                        edge_type: edge_type.clone(),
                    }
                })
                .collect();

            // Get edges between path nodes
            let edges = state.db.get_edges_between(&node_ids)?;

            let graph = FullGraph {
                nodes: path_steps.iter().map(|s| s.node.clone()).collect(),
                edges: edges.into_iter().map(GraphEdge::from).collect(),
            };

            debug!(
                from = %params.from,
                to = %params.to,
                path_len = path_steps.len(),
                "Path found"
            );

            Ok(Json(PathResponse {
                found: true,
                path: path_steps,
                graph,
            }))
        }
    }
}

/// Query parameters for paths to Domain Admins.
#[derive(Debug, Deserialize)]
struct PathsToDaParams {
    /// Comma-separated list of edge types to exclude
    #[serde(default)]
    exclude: String,
}

/// Response item for paths to Domain Admins query.
#[derive(Serialize)]
struct PathsToDaEntry {
    id: String,
    #[serde(rename = "type")]
    node_type: String,
    label: String,
    hops: usize,
}

/// Response for paths to Domain Admins query.
#[derive(Serialize)]
struct PathsToDaResponse {
    count: usize,
    entries: Vec<PathsToDaEntry>,
}

/// Find all users with paths to Domain Admins.
#[instrument(skip(state))]
async fn paths_to_domain_admins(
    State(state): State<AppState>,
    Query(params): Query<PathsToDaParams>,
) -> Result<Json<PathsToDaResponse>, ApiError> {
    // Parse excluded edge types
    let exclude_types: Vec<String> = if params.exclude.is_empty() {
        Vec::new()
    } else {
        params
            .exclude
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    debug!(exclude = ?exclude_types, "Finding paths to Domain Admins");

    let results = state.db.find_paths_to_domain_admins(&exclude_types)?;

    let entries: Vec<PathsToDaEntry> = results
        .into_iter()
        .map(|(id, node_type, label, hops)| PathsToDaEntry {
            id,
            node_type,
            label,
            hops,
        })
        .collect();

    let count = entries.len();
    info!(count = count, "Found users with paths to Domain Admins");

    Ok(Json(PathsToDaResponse { count, entries }))
}

/// Get security insights from the graph.
#[instrument(skip(state))]
async fn graph_insights(
    State(state): State<AppState>,
) -> Result<Json<db::SecurityInsights>, ApiError> {
    let insights = state.db.get_security_insights()?;
    info!(
        effective_das = insights.effective_da_count,
        real_das = insights.real_da_count,
        total_users = insights.total_users,
        "Security insights computed"
    );
    Ok(Json(insights))
}

/// Get all distinct edge types in the database.
#[instrument(skip(state))]
async fn graph_edge_types(State(state): State<AppState>) -> Result<Json<Vec<String>>, ApiError> {
    let types = state.db.get_edge_types()?;
    debug!(count = types.len(), "Edge types retrieved");
    Ok(Json(types))
}

/// Get all distinct node types in the database.
#[instrument(skip(state))]
async fn graph_node_types(State(state): State<AppState>) -> Result<Json<Vec<String>>, ApiError> {
    let types = state.db.get_node_types()?;
    debug!(count = types.len(), "Node types retrieved");
    Ok(Json(types))
}

/// Request body for adding a node.
#[derive(Deserialize)]
struct AddNodeRequest {
    id: String,
    label: String,
    node_type: String,
    #[serde(default)]
    properties: JsonValue,
}

/// Add a new node to the graph.
#[instrument(skip(state, body))]
async fn add_node(
    State(state): State<AppState>,
    Json(body): Json<AddNodeRequest>,
) -> Result<Json<GraphNode>, ApiError> {
    // Validate inputs
    if body.id.is_empty() {
        return Err(ApiError::BadRequest("Node ID is required".to_string()));
    }
    if body.label.is_empty() {
        return Err(ApiError::BadRequest("Node label is required".to_string()));
    }
    if body.node_type.is_empty() {
        return Err(ApiError::BadRequest("Node type is required".to_string()));
    }

    let node = DbNode {
        id: body.id.clone(),
        label: body.label.clone(),
        node_type: body.node_type.clone(),
        properties: if body.properties.is_null() {
            serde_json::json!({})
        } else {
            body.properties
        },
    };

    state.db.insert_node(node)?;

    info!(id = %body.id, label = %body.label, node_type = %body.node_type, "Node added");

    Ok(Json(GraphNode {
        id: body.id,
        label: body.label,
        node_type: body.node_type,
        properties: serde_json::json!({}),
    }))
}

/// Request body for adding an edge.
#[derive(Deserialize)]
struct AddEdgeRequest {
    source: String,
    target: String,
    edge_type: String,
    #[serde(default)]
    properties: JsonValue,
}

/// Add a new edge to the graph.
#[instrument(skip(state, body))]
async fn add_edge(
    State(state): State<AppState>,
    Json(body): Json<AddEdgeRequest>,
) -> Result<Json<GraphEdge>, ApiError> {
    // Validate inputs
    if body.source.is_empty() {
        return Err(ApiError::BadRequest("Source node ID is required".to_string()));
    }
    if body.target.is_empty() {
        return Err(ApiError::BadRequest("Target node ID is required".to_string()));
    }
    if body.edge_type.is_empty() {
        return Err(ApiError::BadRequest("Edge type is required".to_string()));
    }

    let edge = DbEdge {
        source: body.source.clone(),
        target: body.target.clone(),
        edge_type: body.edge_type.clone(),
        properties: if body.properties.is_null() {
            serde_json::json!({})
        } else {
            body.properties
        },
    };

    state.db.insert_edge(edge)?;

    info!(
        source = %body.source,
        target = %body.target,
        edge_type = %body.edge_type,
        "Edge added"
    );

    Ok(Json(GraphEdge {
        source: body.source,
        target: body.target,
        edge_type: body.edge_type,
    }))
}

/// Custom query request body.
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    /// If true, try to extract a graph from the query results
    #[serde(default)]
    extract_graph: bool,
}

/// Query response.
#[derive(Serialize)]
struct QueryResponse {
    /// Raw query results
    results: JsonValue,
    /// Extracted graph (if extract_graph was true)
    #[serde(skip_serializing_if = "Option::is_none")]
    graph: Option<FullGraph>,
}

/// Execute a custom CozoDB query.
#[instrument(skip(state, body))]
async fn graph_query(
    State(state): State<AppState>,
    Json(body): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    info!(query = %body.query, "Executing custom query");

    let results = state
        .db
        .run_custom_query(&body.query)
        .map_err(|e| ApiError::BadRequest(format!("Query error: {e}")))?;

    let graph = if body.extract_graph {
        // Try to extract node IDs from the first column of results
        let node_ids: Vec<String> = results
            .get("rows")
            .and_then(|r| r.as_array())
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if node_ids.is_empty() {
            None
        } else {
            Some(FullGraph::from_node_ids(&state.db, &node_ids)?)
        }
    } else {
        None
    };

    Ok(Json(QueryResponse { results, graph }))
}

// ============================================================================
// Query History Endpoints
// ============================================================================

/// Query history entry.
#[derive(Serialize)]
struct QueryHistoryEntry {
    id: String,
    name: String,
    query: String,
    timestamp: i64,
    result_count: Option<i64>,
}

/// Query history response with pagination.
#[derive(Serialize)]
struct QueryHistoryResponse {
    entries: Vec<QueryHistoryEntry>,
    total: usize,
    page: usize,
    per_page: usize,
}

/// Query history pagination params.
#[derive(Debug, Deserialize)]
struct HistoryParams {
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_per_page")]
    per_page: usize,
}

fn default_page() -> usize {
    1
}

fn default_per_page() -> usize {
    20
}

/// Get query history with pagination.
#[instrument(skip(state))]
async fn get_query_history(
    State(state): State<AppState>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<QueryHistoryResponse>, ApiError> {
    let page = params.page.max(1);
    let per_page = params.per_page.clamp(1, 100);
    let offset = (page - 1) * per_page;

    let (history, total) = state.db.get_query_history(per_page, offset)?;

    let entries: Vec<QueryHistoryEntry> = history
        .into_iter()
        .map(
            |(id, name, query, timestamp, result_count)| QueryHistoryEntry {
                id,
                name,
                query,
                timestamp,
                result_count,
            },
        )
        .collect();

    debug!(
        total = total,
        page = page,
        per_page = per_page,
        "Query history retrieved"
    );
    Ok(Json(QueryHistoryResponse {
        entries,
        total,
        page,
        per_page,
    }))
}

/// Add query history request.
#[derive(Debug, Deserialize)]
struct AddHistoryRequest {
    name: String,
    query: String,
    result_count: Option<i64>,
}

/// Add a query to history.
#[instrument(skip(state, body))]
async fn add_query_history(
    State(state): State<AppState>,
    Json(body): Json<AddHistoryRequest>,
) -> Result<Json<QueryHistoryEntry>, ApiError> {
    let id = uuid::Uuid::new_v4().to_string();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    state
        .db
        .add_query_history(&id, &body.name, &body.query, timestamp, body.result_count)?;

    info!(id = %id, name = %body.name, "Query added to history");
    Ok(Json(QueryHistoryEntry {
        id,
        name: body.name,
        query: body.query,
        timestamp,
        result_count: body.result_count,
    }))
}

/// Delete a query from history.
#[instrument(skip(state))]
async fn delete_query_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.db.delete_query_history(&id)?;
    info!(id = %id, "Query deleted from history");
    Ok(StatusCode::NO_CONTENT)
}

/// Clear all query history.
#[instrument(skip(state))]
async fn clear_query_history(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state.db.clear_query_history()?;
    info!("Query history cleared");
    Ok(StatusCode::NO_CONTENT)
}
