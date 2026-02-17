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
#[cfg(feature = "cozo")]
use db::CozoDatabase;
#[cfg(feature = "crustdb")]
use db::CrustDatabase;
#[cfg(feature = "falkordb")]
use db::FalkorDbDatabase;
#[cfg(feature = "kuzu")]
use db::KuzuDatabase;
#[cfg(feature = "neo4j")]
use db::Neo4jDatabase;
use db::{DatabaseBackend, DatabaseType, DatabaseUrl, DbError, QueryLanguage};
use import::{BloodHoundImporter, ImportProgress};
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use serde_json::{json, Value as JsonValue};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
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
    /// Not connected to a database
    NotConnected,
    /// Internal server error
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Database(e) => write!(f, "Database error: {e}"),
            ApiError::BadRequest(msg) => write!(f, "Bad request: {msg}"),
            ApiError::NotFound(msg) => write!(f, "Not found: {msg}"),
            ApiError::NotConnected => write!(f, "Not connected to a database"),
            ApiError::Internal(msg) => write!(f, "Internal error: {msg}"),
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
            ApiError::NotConnected => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Not connected to a database".to_string(),
            ),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        (status, message).into_response()
    }
}

// ============================================================================
// Query Tracking Types
// ============================================================================

/// Status of a running or completed query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Running,
    Completed,
    Failed,
    Aborted,
}

impl std::fmt::Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStatus::Running => write!(f, "running"),
            QueryStatus::Completed => write!(f, "completed"),
            QueryStatus::Failed => write!(f, "failed"),
            QueryStatus::Aborted => write!(f, "aborted"),
        }
    }
}

/// Progress update for a running query.
#[derive(Debug, Clone, Serialize)]
pub struct QueryProgress {
    pub query_id: String,
    pub status: QueryStatus,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub result_count: Option<i64>,
    pub error: Option<String>,
    /// Query results (only populated when status is Completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<JsonValue>,
    /// Extracted graph (only populated when status is Completed and extract_graph was true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<FullGraph>,
}

/// State for a running query.
pub struct RunningQuery {
    pub query_id: String,
    pub query: String,
    pub started_at: std::time::Instant,
    pub started_at_unix: i64,
    pub cancel_token: CancellationToken,
    pub progress_tx: broadcast::Sender<QueryProgress>,
    pub final_state: RwLock<Option<QueryProgress>>,
}

// ============================================================================
// Application State
// ============================================================================

/// Import job state: channel for live updates + optional final state for late subscribers.
pub struct ImportJob {
    pub channel: broadcast::Sender<ImportProgress>,
    pub final_state: RwLock<Option<ImportProgress>>,
}

/// Database connection state.
struct DatabaseConnection {
    backend: Arc<dyn DatabaseBackend>,
    db_type: DatabaseType,
}

/// Application state shared across requests.
#[derive(Clone)]
pub struct AppState {
    /// Current database connection (if any).
    connection: Arc<RwLock<Option<DatabaseConnection>>>,
    /// Active import jobs and their progress channels.
    import_jobs: Arc<DashMap<String, Arc<ImportJob>>>,
    /// Active running queries for tracking and cancellation.
    running_queries: Arc<DashMap<String, Arc<RunningQuery>>>,
}

impl AppState {
    /// Create a new AppState without a database connection.
    pub fn new_disconnected() -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            import_jobs: Arc::new(DashMap::new()),
            running_queries: Arc::new(DashMap::new()),
        }
    }

    /// Create a new AppState with an initial database connection.
    pub fn new_connected(backend: Arc<dyn DatabaseBackend>, db_type: DatabaseType) -> Self {
        Self {
            connection: Arc::new(RwLock::new(Some(DatabaseConnection { backend, db_type }))),
            import_jobs: Arc::new(DashMap::new()),
            running_queries: Arc::new(DashMap::new()),
        }
    }

    /// Check if connected to a database.
    pub fn is_connected(&self) -> bool {
        self.connection.read().is_some()
    }

    /// Get the current database type if connected.
    pub fn database_type(&self) -> Option<DatabaseType> {
        self.connection.read().as_ref().map(|c| c.db_type)
    }

    /// Get a reference to the database backend if connected.
    pub fn db(&self) -> Option<Arc<dyn DatabaseBackend>> {
        self.connection.read().as_ref().map(|c| c.backend.clone())
    }

    /// Connect to a database using a URL.
    pub fn connect(&self, url: &str) -> Result<DatabaseType, String> {
        let parsed = DatabaseUrl::parse(url).map_err(|e| e.to_string())?;

        let backend: Arc<dyn DatabaseBackend> = match parsed.db_type {
            #[cfg(feature = "kuzu")]
            DatabaseType::KuzuDB => {
                let path = parsed.path.ok_or("Missing path for KuzuDB")?;
                let db = KuzuDatabase::new(&path).map_err(|e| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "kuzu"))]
            DatabaseType::KuzuDB => {
                return Err("KuzuDB support not compiled in.".to_string());
            }
            #[cfg(feature = "cozo")]
            DatabaseType::CozoDB => {
                let path = parsed.path.ok_or("Missing path for CozoDB")?;
                let db = CozoDatabase::new(&path).map_err(|e| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "cozo"))]
            DatabaseType::CozoDB => {
                return Err("CozoDB support not compiled in.".to_string());
            }
            #[cfg(feature = "neo4j")]
            DatabaseType::Neo4j => {
                let host = parsed.host.ok_or("Missing host for Neo4j")?;
                let port = parsed.port.unwrap_or(7687);
                let db = Neo4jDatabase::new(
                    &host,
                    port,
                    parsed.username,
                    parsed.password,
                    parsed.database,
                )
                .map_err(|e| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "neo4j"))]
            DatabaseType::Neo4j => {
                return Err("Neo4j support not compiled in.".to_string());
            }
            #[cfg(feature = "falkordb")]
            DatabaseType::FalkorDB => {
                let host = parsed.host.ok_or("Missing host for FalkorDB")?;
                let port = parsed.port.unwrap_or(6379);
                let db = FalkorDbDatabase::new(&host, port, parsed.username, parsed.password)
                    .map_err(|e| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "falkordb"))]
            DatabaseType::FalkorDB => {
                return Err("FalkorDB support not compiled in.".to_string());
            }
            #[cfg(feature = "crustdb")]
            DatabaseType::CrustDB => {
                let path = parsed.path.ok_or("Missing path for CrustDB")?;
                let db = CrustDatabase::new(&path).map_err(|e: db::DbError| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "crustdb"))]
            DatabaseType::CrustDB => {
                return Err(
                    "CrustDB support not compiled in. See Cargo.toml for instructions.".to_string(),
                );
            }
        };

        let db_type = parsed.db_type;
        *self.connection.write() = Some(DatabaseConnection { backend, db_type });
        info!(database_type = %db_type.name(), "Connected to database");
        Ok(db_type)
    }

    /// Disconnect from the current database.
    pub fn disconnect(&self) {
        *self.connection.write() = None;
        info!("Disconnected from database");
    }

    /// Get the database, returning an error if not connected.
    fn require_db(&self) -> Result<Arc<dyn DatabaseBackend>, ApiError> {
        self.db().ok_or_else(|| ApiError::NotConnected)
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
pub use db::{DbEdge, DbNode};

/// Create the API router with the given state.
///
/// This is useful for integration tests that want to test the actual
/// application handlers without starting a full server.
pub fn create_api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health_check))
        // Database connection management
        .route("/api/database/status", get(database_status))
        .route("/api/database/connect", post(database_connect))
        .route("/api/database/disconnect", post(database_disconnect))
        // Import
        .route("/api/import", post(import_bloodhound))
        .route("/api/import/progress/:job_id", get(import_progress))
        // Graph operations
        .route("/api/graph/stats", get(graph_stats))
        .route("/api/graph/detailed-stats", get(graph_detailed_stats))
        .route("/api/graph/clear", post(graph_clear))
        .route("/api/graph/nodes", get(graph_nodes))
        .route("/api/graph/edges", get(graph_edges))
        .route("/api/graph/all", get(graph_all))
        .route("/api/graph/search", get(graph_search))
        .route("/api/graph/node/:id/counts", get(node_counts))
        .route(
            "/api/graph/node/:id/connections/:direction",
            get(node_connections),
        )
        .route("/api/graph/node/:id/status", get(node_status))
        .route("/api/graph/path", get(graph_path))
        .route("/api/graph/paths-to-da", get(paths_to_domain_admins))
        .route("/api/graph/edge-types", get(graph_edge_types))
        .route("/api/graph/node-types", get(graph_node_types))
        .route("/api/graph/node", post(add_node))
        .route("/api/graph/edge", post(add_edge))
        .route("/api/graph/insights", get(graph_insights))
        .route("/api/graph/query", post(graph_query))
        // Query progress and abort
        .route("/api/query/progress/:id", get(query_progress))
        .route("/api/query/abort/:id", post(query_abort))
        // Query history
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

async fn health_check() -> Json<JsonValue> {
    Json(json!({"status": "ok"}))
}

// ============================================================================
// Database Connection Endpoints
// ============================================================================

/// Database status response.
#[derive(Serialize)]
struct DatabaseStatus {
    connected: bool,
    database_type: Option<String>,
}

/// Get current database connection status.
async fn database_status(State(state): State<AppState>) -> Json<DatabaseStatus> {
    let connected = state.is_connected();
    let database_type = state.database_type().map(|t| t.name().to_string());
    Json(DatabaseStatus {
        connected,
        database_type,
    })
}

/// Database connect request.
#[derive(Deserialize)]
struct ConnectRequest {
    url: String,
}

/// Connect to a database.
async fn database_connect(
    State(state): State<AppState>,
    Json(body): Json<ConnectRequest>,
) -> Result<Json<DatabaseStatus>, ApiError> {
    info!(url = %body.url, "Connecting to database");

    state
        .connect(&body.url)
        .map_err(|e| ApiError::BadRequest(e))?;

    let database_type = state.database_type().map(|t| t.name().to_string());
    Ok(Json(DatabaseStatus {
        connected: true,
        database_type,
    }))
}

/// Disconnect from the current database.
async fn database_disconnect(State(state): State<AppState>) -> StatusCode {
    state.disconnect();
    StatusCode::NO_CONTENT
}

// ============================================================================
// Import Endpoints
// ============================================================================

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

    // Stream uploaded files to temp files to avoid holding large files in memory
    let mut files: Vec<(String, std::path::PathBuf)> = Vec::new();

    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        error!(error = %e, "Multipart read error");
        ApiError::BadRequest(format!("Multipart error: {e}"))
    })? {
        let filename = field.file_name().unwrap_or("unknown").to_string();

        // Create temp file path with unique ID
        let temp_path = std::env::temp_dir().join(format!(
            "admapper-upload-{}-{}",
            uuid::Uuid::new_v4(),
            filename.replace(std::path::MAIN_SEPARATOR, "_")
        ));

        // Create and write to temp file
        let mut async_file = tokio::fs::File::create(&temp_path).await.map_err(|e| {
            error!(error = %e, path = %temp_path.display(), "Failed to create temp file");
            ApiError::Internal(format!("Temp file error: {e}"))
        })?;
        let mut total_bytes = 0usize;

        // Stream chunks to temp file
        while let Some(chunk) = field.chunk().await.map_err(|e| {
            error!(error = %e, filename = %filename, "Failed to read chunk");
            ApiError::BadRequest(format!("Read error: {e}"))
        })? {
            total_bytes += chunk.len();
            async_file.write_all(&chunk).await.map_err(|e| {
                error!(error = %e, filename = %filename, "Failed to write chunk");
                ApiError::Internal(format!("Write error: {e}"))
            })?;
        }

        async_file.flush().await.map_err(|e| {
            error!(error = %e, filename = %filename, "Failed to flush temp file");
            ApiError::Internal(format!("Flush error: {e}"))
        })?;

        debug!(filename = %filename, size = total_bytes, path = %temp_path.display(), "Received file");
        files.push((filename, temp_path));
    }

    if files.is_empty() {
        warn!("Import request with no files");
        return Err(ApiError::BadRequest("No files uploaded".to_string()));
    }

    info!(file_count = files.len(), "Processing uploaded files");

    let db = state.require_db()?;
    let job_id_clone = job_id.clone();
    let import_jobs = state.import_jobs.clone();
    let job_for_task = job.clone();

    // Spawn import task
    tokio::task::spawn_blocking(move || {
        let mut importer = BloodHoundImporter::new(db, tx);
        let mut final_progress: Option<ImportProgress> = None;

        for (filename, temp_path) in &files {
            info!(filename = %filename, path = %temp_path.display(), "Importing file");
            let result = if filename.ends_with(".zip") {
                // Open temp file for reading
                match std::fs::File::open(temp_path) {
                    Ok(file) => importer.import_zip(file, &job_id_clone),
                    Err(e) => {
                        error!(error = %e, path = %temp_path.display(), "Failed to open temp file");
                        Err(format!("Failed to open temp file: {e}"))
                    }
                }
            } else if filename.ends_with(".json") {
                importer.import_json_file(temp_path, &job_id_clone)
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

        // Clean up temp files
        for (filename, temp_path) in files {
            if let Err(e) = std::fs::remove_file(&temp_path) {
                debug!(filename = %filename, error = %e, "Failed to remove temp file (may already be cleaned up)");
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
    let db = state.require_db()?;
    let (node_count, edge_count) = db.get_stats()?;

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
    let db = state.require_db()?;
    let stats = db.get_detailed_stats()?;
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
    let db = state.require_db()?;
    db.clear()?;
    info!("Database cleared");
    Ok(StatusCode::NO_CONTENT)
}

/// Graph node response format.
#[derive(Debug, Clone, Serialize)]
struct GraphNode {
    id: String,
    label: String,
    #[serde(rename = "type")]
    node_type: String,
    properties: JsonValue,
}

/// Graph edge response format.
#[derive(Debug, Clone, Serialize)]
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
    let db = state.require_db()?;
    let nodes = db.get_all_nodes()?;
    let result: Vec<GraphNode> = nodes.into_iter().map(GraphNode::from).collect();
    Ok(Json(result))
}

/// Get all graph edges.
async fn graph_edges(State(state): State<AppState>) -> Result<Json<Vec<GraphEdge>>, ApiError> {
    let db = state.require_db()?;
    let edges = db.get_all_edges()?;
    let result: Vec<GraphEdge> = edges.into_iter().map(GraphEdge::from).collect();
    Ok(Json(result))
}

/// Full graph response.
#[derive(Debug, Clone, Serialize)]
pub struct FullGraph {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

impl FullGraph {
    /// Build a subgraph containing only the specified nodes and edges between them.
    fn from_node_ids(db: &Arc<dyn DatabaseBackend>, node_ids: &[String]) -> Result<Self, ApiError> {
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

/// Extract a graph from query results.
///
/// This function looks for node and edge objects in the query results and
/// extracts them into a graph structure. It handles:
/// - Direct node/edge objects (with `_type: "node"` or `_type: "edge"`)
/// - Object IDs in properties (looks up nodes from the database)
fn extract_graph_from_results(
    results: &JsonValue,
    db: &Arc<dyn DatabaseBackend>,
) -> Result<Option<FullGraph>, ApiError> {
    let rows = match results.get("rows").and_then(|r| r.as_array()) {
        Some(rows) if !rows.is_empty() => rows,
        _ => return Ok(None),
    };

    let mut nodes: Vec<GraphNode> = Vec::new();
    let mut raw_edges: Vec<JsonValue> = Vec::new();
    let mut node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Map internal database IDs to object_ids for edge resolution
    let mut id_to_object_id: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();

    // Scan all values in all rows for node/edge objects
    for row in rows {
        let values: Vec<&JsonValue> = if let Some(arr) = row.as_array() {
            arr.iter().collect()
        } else if let Some(obj) = row.as_object() {
            obj.values().collect()
        } else {
            continue;
        };

        for value in values {
            // Check if this is a node object
            if value.get("_type").and_then(|t| t.as_str()) == Some("node") {
                if let Some(node) = extract_node_from_json(value) {
                    // Build ID mapping for edge resolution
                    if let Some(internal_id) = value.get("id").and_then(|v| v.as_i64()) {
                        id_to_object_id.insert(internal_id, node.id.clone());
                    }
                    if node_ids.insert(node.id.clone()) {
                        nodes.push(node);
                    }
                }
            }
            // Check if this is an edge object - store for later processing
            else if value.get("_type").and_then(|t| t.as_str()) == Some("edge") {
                raw_edges.push(value.clone());
            }
            // Try to extract object_id from string values
            else if let Some(id) = value.as_str() {
                if !id.is_empty() {
                    node_ids.insert(id.to_string());
                }
            }
        }
    }

    // Process edges, mapping internal IDs to object_ids
    let edges: Vec<GraphEdge> = raw_edges
        .iter()
        .filter_map(|value| extract_edge_from_json(value, &id_to_object_id))
        .collect();

    // If we found direct node/edge objects, use those
    if !nodes.is_empty() || !edges.is_empty() {
        // If we have edges but missing some nodes, fetch them
        let edge_node_ids: std::collections::HashSet<String> = edges
            .iter()
            .flat_map(|e| vec![e.source.clone(), e.target.clone()])
            .collect();

        let missing_ids: Vec<String> = edge_node_ids.difference(&node_ids).cloned().collect();

        if !missing_ids.is_empty() {
            let additional_nodes = db.get_nodes_by_ids(&missing_ids)?;
            for node in additional_nodes {
                if node_ids.insert(node.id.clone()) {
                    nodes.push(GraphNode::from(node));
                }
            }
        }

        return Ok(Some(FullGraph { nodes, edges }));
    }

    // Fall back to looking up nodes by collected IDs
    let ids: Vec<String> = node_ids.into_iter().collect();
    if ids.is_empty() {
        return Ok(None);
    }

    FullGraph::from_node_ids(db, &ids).map(Some)
}

/// Extract a GraphNode from a JSON node object.
fn extract_node_from_json(value: &JsonValue) -> Option<GraphNode> {
    let object_id = value
        .get("object_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            // Try getting from properties
            value
                .get("properties")
                .and_then(|p| p.get("object_id"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .or_else(|| {
            value
                .get("id")
                .and_then(|v| v.as_i64())
                .map(|id| id.to_string())
        })?;

    let labels = value
        .get("labels")
        .and_then(|l| l.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let node_type_from_labels = labels.first().cloned();
    let node_type_from_props = value
        .get("properties")
        .and_then(|p| p.get("node_type"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let node_type = node_type_from_props
        .or(node_type_from_labels)
        .unwrap_or_else(|| "Unknown".to_string());

    let label = value
        .get("properties")
        .and_then(|p| p.get("label"))
        .and_then(|l| l.as_str())
        .map(String::from)
        .unwrap_or_else(|| object_id.clone());

    // Extract properties - handle nested JSON string from CrustDB storage
    let properties = extract_nested_properties(value);

    Some(GraphNode {
        id: object_id,
        label,
        node_type,
        properties,
    })
}

/// Extract a GraphEdge from a JSON edge object.
///
/// Uses the id_map to convert internal database IDs to object_ids.
fn extract_edge_from_json(
    value: &JsonValue,
    id_map: &std::collections::HashMap<i64, String>,
) -> Option<GraphEdge> {
    // Try to get source as string first, then as i64 and map it
    let source = value.get("source").and_then(|v| {
        v.as_str().map(String::from).or_else(|| {
            v.as_i64()
                .and_then(|id| id_map.get(&id).cloned().or_else(|| Some(id.to_string())))
        })
    })?;

    let target = value.get("target").and_then(|v| {
        v.as_str().map(String::from).or_else(|| {
            v.as_i64()
                .and_then(|id| id_map.get(&id).cloned().or_else(|| Some(id.to_string())))
        })
    })?;

    let edge_type = value
        .get("edge_type")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "RELATED".to_string());

    Some(GraphEdge {
        source,
        target,
        edge_type,
    })
}

/// Extract nested properties from a node JSON object.
///
/// CrustDB stores original BloodHound properties as a JSON string in the
/// `properties.properties` field. This function parses that nested JSON
/// and flattens it into the top-level properties object.
fn extract_nested_properties(value: &JsonValue) -> JsonValue {
    let props = match value.get("properties") {
        Some(p) => p,
        None => return JsonValue::Object(serde_json::Map::new()),
    };

    // Check if there's a nested "properties" field that's a JSON string
    if let Some(nested_str) = props.get("properties").and_then(|p| p.as_str()) {
        // Try to parse the nested JSON string
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(nested_str) {
            if let JsonValue::Object(mut nested_props) = parsed {
                // Merge with top-level properties, preferring nested values
                // but keeping object_id, label, node_type from top level
                if let Some(object_id) = props.get("object_id") {
                    nested_props.insert("object_id".to_string(), object_id.clone());
                }
                if let Some(label) = props.get("label") {
                    nested_props.insert("label".to_string(), label.clone());
                }
                if let Some(node_type) = props.get("node_type") {
                    nested_props.insert("node_type".to_string(), node_type.clone());
                }
                return JsonValue::Object(nested_props);
            }
        }
    }

    // No nested properties or parsing failed - return as-is but remove the
    // "properties" key if it's a string (to avoid showing raw JSON)
    if let JsonValue::Object(mut obj) = props.clone() {
        if obj
            .get("properties")
            .map(|p| p.is_string())
            .unwrap_or(false)
        {
            obj.remove("properties");
        }
        return JsonValue::Object(obj);
    }

    props.clone()
}

/// Get full graph (nodes and edges).
async fn graph_all(State(state): State<AppState>) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    let nodes = db.get_all_nodes()?;
    let edges = db.get_all_edges()?;

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

    let db = state.require_db()?;
    let nodes = db.search_nodes(&params.q, params.limit)?;
    let result: Vec<GraphNode> = nodes.into_iter().map(GraphNode::from).collect();

    debug!(query = %params.q, results = result.len(), "Search complete");
    Ok(Json(result))
}

/// Node connection counts response.
#[derive(Serialize)]
struct NodeCounts {
    incoming: usize,
    outgoing: usize,
    #[serde(rename = "adminTo")]
    admin_to: usize,
    #[serde(rename = "memberOf")]
    member_of: usize,
    members: usize,
}

/// Get connection counts for a node.
/// Returns counts for incoming, outgoing, admin permissions, memberOf, and members.
#[instrument(skip(state))]
async fn node_counts(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeCounts>, ApiError> {
    let db = state.require_db()?;

    // Get all edges to count connections
    // This is not the most efficient approach, but works across all backends
    let all_edges = db.get_all_edges()?;

    let mut incoming = 0;
    let mut outgoing = 0;
    let mut admin_to = 0;
    let mut member_of = 0;
    let mut members = 0;

    // Admin-related edge types
    let admin_types: std::collections::HashSet<&str> = [
        "AdminTo",
        "GenericAll",
        "GenericWrite",
        "Owns",
        "WriteDacl",
        "WriteOwner",
        "AllExtendedRights",
        "ForceChangePassword",
        "AddMember",
    ]
    .into_iter()
    .collect();

    for edge in &all_edges {
        if edge.target == node_id {
            incoming += 1;
            if edge.edge_type == "MemberOf" {
                members += 1;
            }
        }
        if edge.source == node_id {
            outgoing += 1;
            if edge.edge_type == "MemberOf" {
                member_of += 1;
            }
            if admin_types.contains(edge.edge_type.as_str()) {
                admin_to += 1;
            }
        }
    }

    debug!(
        node_id = %node_id,
        incoming = incoming,
        outgoing = outgoing,
        admin_to = admin_to,
        member_of = member_of,
        members = members,
        "Node counts retrieved"
    );

    Ok(Json(NodeCounts {
        incoming,
        outgoing,
        admin_to,
        member_of,
        members,
    }))
}

/// Get connections for a node in a specific direction.
/// Returns the full graph (nodes and edges) for rendering.
#[instrument(skip(state))]
async fn node_connections(
    State(state): State<AppState>,
    Path((node_id, direction)): Path<(String, String)>,
) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, direction = %direction, "Loading node connections");

    let (nodes, edges) = db.get_node_connections(&node_id, &direction)?;

    Ok(Json(FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        edges: edges.into_iter().map(GraphEdge::from).collect(),
    }))
}

/// Node security status response.
#[derive(Serialize)]
struct NodeStatus {
    /// Is the node owned by the attacker
    owned: bool,
    /// Is the node a member of Enterprise Admins (SID -519)
    #[serde(rename = "isEnterpriseAdmin")]
    is_enterprise_admin: bool,
    /// Is the node a member of Domain Admins (SID -512)
    #[serde(rename = "isDomainAdmin")]
    is_domain_admin: bool,
    /// Is the node marked as high value or in a high-value group
    #[serde(rename = "isHighValue")]
    is_high_value: bool,
    /// Does the node have a path to a high-value target (if not already high value)
    #[serde(rename = "hasPathToHighValue")]
    has_path_to_high_value: bool,
    /// Number of hops to the nearest high-value target (if hasPathToHighValue)
    #[serde(rename = "pathLength", skip_serializing_if = "Option::is_none")]
    path_length: Option<usize>,
}

/// Get security status for a node.
/// Checks group memberships and paths to high-value targets.
#[instrument(skip(state))]
async fn node_status(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeStatus>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Checking node security status");

    // Get the node to check its properties
    let nodes = db.get_nodes_by_ids(&[node_id.clone()])?;
    let node = nodes.first();

    // Check owned status from properties
    let owned = node
        .and_then(|n| {
            let props = &n.properties;
            props.get("owned").or(props.get("Owned")).and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_i64().map(|i| i == 1))
                    .or_else(|| v.as_str().map(|s| s == "true"))
            })
        })
        .unwrap_or(false);

    // Check high value from properties
    let high_value_prop = node
        .and_then(|n| {
            let props = &n.properties;
            props
                .get("highvalue")
                .or(props.get("HighValue"))
                .or(props.get("highValue"))
                .and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_i64().map(|i| i == 1))
                        .or_else(|| v.as_str().map(|s| s == "true"))
                })
        })
        .unwrap_or(false);

    // Check group memberships using graph traversal
    let is_enterprise_admin = db
        .find_membership_by_sid_suffix(&node_id, "-519")?
        .is_some();
    let is_domain_admin = db
        .find_membership_by_sid_suffix(&node_id, "-512")?
        .is_some();

    // High-value RIDs to check membership for
    let high_value_rids = ["-512", "-519", "-518", "-516", "-498", "-544"];
    let is_high_value_member = high_value_rids.iter().any(|rid| {
        db.find_membership_by_sid_suffix(&node_id, rid)
            .unwrap_or(None)
            .is_some()
    });

    let is_high_value = high_value_prop || is_high_value_member;

    // Check for paths to high-value targets (only if not already high value)
    let (has_path_to_high_value, path_length) =
        if is_enterprise_admin || is_domain_admin || is_high_value {
            (false, None)
        } else {
            // Use the existing paths-to-DA logic to check for attack paths
            let paths = db.find_paths_to_domain_admins(&[])?;
            let path_info = paths.iter().find(|(id, _, _, _)| id == &node_id);
            match path_info {
                Some((_, _, _, hops)) => (true, Some(*hops)),
                None => (false, None),
            }
        };

    Ok(Json(NodeStatus {
        owned,
        is_enterprise_admin,
        is_domain_admin,
        is_high_value,
        has_path_to_high_value,
        path_length,
    }))
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
    let db = state.require_db()?;

    // Resolve identifiers to object IDs (supports both IDs and labels)
    let from_id = db
        .resolve_node_identifier(&params.from)?
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {}", params.from)))?;

    let to_id = db
        .resolve_node_identifier(&params.to)?
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {}", params.to)))?;

    let path_result = db.shortest_path(&from_id, &to_id)?;

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
            let nodes = db.get_nodes_by_ids(&node_ids)?;

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
            let edges = db.get_edges_between(&node_ids)?;

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

    let db = state.require_db()?;
    let results = db.find_paths_to_domain_admins(&exclude_types)?;

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
    let db = state.require_db()?;
    let insights = db.get_security_insights()?;
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
    let db = state.require_db()?;
    let types = db.get_edge_types()?;
    debug!(count = types.len(), "Edge types retrieved");
    Ok(Json(types))
}

/// Get all distinct node types in the database.
#[instrument(skip(state))]
async fn graph_node_types(State(state): State<AppState>) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types = db.get_node_types()?;
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

    let db = state.require_db()?;
    db.insert_node(node)?;

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
        return Err(ApiError::BadRequest(
            "Source node ID is required".to_string(),
        ));
    }
    if body.target.is_empty() {
        return Err(ApiError::BadRequest(
            "Target node ID is required".to_string(),
        ));
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

    let db = state.require_db()?;
    db.insert_edge(edge)?;

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
    /// Query language (optional, defaults to backend's default)
    #[serde(default)]
    language: Option<String>,
}

/// Response when starting an async query.
#[derive(Serialize)]
struct QueryStartResponse {
    query_id: String,
}

/// Execute a custom query asynchronously.
/// Returns immediately with a query_id. Subscribe to /api/query/progress/:id for updates.
#[instrument(skip(state, body))]
async fn graph_query(
    State(state): State<AppState>,
    Json(body): Json<QueryRequest>,
) -> Result<Json<QueryStartResponse>, ApiError> {
    let db = state.require_db()?;
    info!(query = %body.query, "Starting async query");

    // Generate query ID and setup tracking
    let query_id = uuid::Uuid::new_v4().to_string();
    let cancel_token = CancellationToken::new();
    let (progress_tx, _) = broadcast::channel::<QueryProgress>(16);

    let started_at = std::time::Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let running_query = Arc::new(RunningQuery {
        query_id: query_id.clone(),
        query: body.query.clone(),
        started_at,
        started_at_unix,
        cancel_token: cancel_token.clone(),
        progress_tx: progress_tx.clone(),
        final_state: RwLock::new(None),
    });

    state
        .running_queries
        .insert(query_id.clone(), running_query.clone());

    // Broadcast initial "running" status
    let initial_progress = QueryProgress {
        query_id: query_id.clone(),
        status: QueryStatus::Running,
        started_at: started_at_unix,
        duration_ms: None,
        result_count: None,
        error: None,
        results: None,
        graph: None,
    };
    let _ = progress_tx.send(initial_progress);

    // Spawn the query execution
    let query_id_clone = query_id.clone();
    let query_text = body.query.clone();
    let extract_graph = body.extract_graph;
    let language = body.language.clone();
    let running_queries = state.running_queries.clone();
    let running_query_for_task = running_query.clone();

    tokio::task::spawn_blocking(move || {
        // Check if cancelled before starting
        if cancel_token.is_cancelled() {
            let progress = QueryProgress {
                query_id: query_id_clone.clone(),
                status: QueryStatus::Aborted,
                started_at: started_at_unix,
                duration_ms: Some(started_at.elapsed().as_millis() as u64),
                result_count: None,
                error: None,
                results: None,
                graph: None,
            };
            let _ = progress_tx.send(progress.clone());
            *running_query_for_task.final_state.write() = Some(progress);
            return;
        }

        // Execute the query
        let result = if let Some(lang_str) = &language {
            QueryLanguage::from_str(lang_str)
                .map(|lang| db.run_query_with_language(&query_text, lang))
                .unwrap_or_else(|| Err(DbError::Database(format!("Unknown language: {}", lang_str))))
        } else {
            db.run_custom_query(&query_text)
        };

        // Check if cancelled after query completion
        if cancel_token.is_cancelled() {
            let progress = QueryProgress {
                query_id: query_id_clone.clone(),
                status: QueryStatus::Aborted,
                started_at: started_at_unix,
                duration_ms: Some(started_at.elapsed().as_millis() as u64),
                result_count: None,
                error: None,
                results: None,
                graph: None,
            };
            let _ = progress_tx.send(progress.clone());
            *running_query_for_task.final_state.write() = Some(progress);
            return;
        }

        let duration_ms = started_at.elapsed().as_millis() as u64;

        let progress = match result {
            Ok(results) => {
                // Count results
                let result_count = results
                    .get("rows")
                    .and_then(|r| r.as_array())
                    .map(|arr| arr.len() as i64);

                // Try to extract graph if requested
                let graph = if extract_graph {
                    extract_graph_from_results(&results, &db).ok().flatten()
                } else {
                    None
                };

                debug!(
                    query_id = %query_id_clone,
                    duration_ms = duration_ms,
                    result_count = ?result_count,
                    has_graph = graph.is_some(),
                    "Query completed"
                );

                QueryProgress {
                    query_id: query_id_clone.clone(),
                    status: QueryStatus::Completed,
                    started_at: started_at_unix,
                    duration_ms: Some(duration_ms),
                    result_count,
                    error: None,
                    results: Some(results),
                    graph,
                }
            }
            Err(e) => {
                error!(query_id = %query_id_clone, error = %e, "Query failed");
                QueryProgress {
                    query_id: query_id_clone.clone(),
                    status: QueryStatus::Failed,
                    started_at: started_at_unix,
                    duration_ms: Some(duration_ms),
                    result_count: None,
                    error: Some(e.to_string()),
                    results: None,
                    graph: None,
                }
            }
        };

        // Broadcast final status
        let _ = progress_tx.send(progress.clone());
        *running_query_for_task.final_state.write() = Some(progress);

        // Clean up after a delay
        std::thread::sleep(std::time::Duration::from_secs(60));
        running_queries.remove(&query_id_clone);
    });

    Ok(Json(QueryStartResponse { query_id }))
}

/// SSE endpoint for query progress updates.
async fn query_progress(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    use futures::future::Either;

    let query = state
        .running_queries
        .get(&query_id)
        .map(|r| r.value().clone())
        .ok_or_else(|| ApiError::NotFound("Query not found".to_string()))?;

    // Check if query already completed (late subscriber)
    let final_state = query.final_state.read().clone();
    if let Some(progress) = final_state {
        debug!(query_id = %query_id, "Sending cached final state to late subscriber");
        let data = serde_json::to_string(&progress).unwrap_or_default();
        let stream = Either::Left(tokio_stream::once(Ok(Event::default().data(data))));
        return Ok(Sse::new(stream));
    }

    // Subscribe to live updates
    let rx = query.progress_tx.subscribe();
    let stream = Either::Right(BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|progress| {
            let data = serde_json::to_string(&progress).unwrap_or_default();
            Ok(Event::default().data(data))
        })
    }));

    Ok(Sse::new(stream))
}

/// Abort a running query.
#[instrument(skip(state))]
async fn query_abort(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let query = state
        .running_queries
        .get(&query_id)
        .map(|r| r.value().clone())
        .ok_or_else(|| ApiError::NotFound("Query not found".to_string()))?;

    info!(query_id = %query_id, "Aborting query");

    // Cancel the token - this will signal the query to abort
    query.cancel_token.cancel();

    // Broadcast aborted status
    let progress = QueryProgress {
        query_id: query_id.clone(),
        status: QueryStatus::Aborted,
        started_at: query.started_at_unix,
        duration_ms: Some(query.started_at.elapsed().as_millis() as u64),
        result_count: None,
        error: None,
        results: None,
        graph: None,
    };
    let _ = query.progress_tx.send(progress.clone());
    *query.final_state.write() = Some(progress);

    Ok(StatusCode::NO_CONTENT)
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
    status: QueryStatus,
    started_at: i64,
    duration_ms: Option<u64>,
    error: Option<String>,
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
    let db = state.require_db()?;
    let page = params.page.max(1);
    let per_page = params.per_page.clamp(1, 100);
    let offset = (page - 1) * per_page;

    let (history, total) = db.get_query_history(per_page, offset)?;

    let entries: Vec<QueryHistoryEntry> = history
        .into_iter()
        .map(
            |(id, name, query, timestamp, result_count, status, started_at, duration_ms, error)| {
                let status = match status.as_str() {
                    "running" => QueryStatus::Running,
                    "completed" => QueryStatus::Completed,
                    "failed" => QueryStatus::Failed,
                    "aborted" => QueryStatus::Aborted,
                    _ => QueryStatus::Completed, // Default fallback
                };
                QueryHistoryEntry {
                    id,
                    name,
                    query,
                    timestamp,
                    result_count,
                    status,
                    started_at,
                    duration_ms,
                    error,
                }
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
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

/// Add a query to history.
#[instrument(skip(state, body))]
async fn add_query_history(
    State(state): State<AppState>,
    Json(body): Json<AddHistoryRequest>,
) -> Result<Json<QueryHistoryEntry>, ApiError> {
    let db = state.require_db()?;
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

    db.add_query_history(
        &id,
        &body.name,
        &body.query,
        started_at, // timestamp
        body.result_count,
        status_str,
        started_at,
        body.duration_ms,
        body.error.as_deref(),
    )?;

    info!(id = %id, name = %body.name, "Query added to history");
    Ok(Json(QueryHistoryEntry {
        id,
        name: body.name,
        query: body.query,
        timestamp: started_at,
        result_count: body.result_count,
        status,
        started_at,
        duration_ms: body.duration_ms,
        error: body.error,
    }))
}

/// Delete a query from history.
#[instrument(skip(state))]
async fn delete_query_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    db.delete_query_history(&id)?;
    info!(id = %id, "Query deleted from history");
    Ok(StatusCode::NO_CONTENT)
}

/// Clear all query history.
#[instrument(skip(state))]
async fn clear_query_history(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    db.clear_query_history()?;
    info!("Query history cleared");
    Ok(StatusCode::NO_CONTENT)
}
