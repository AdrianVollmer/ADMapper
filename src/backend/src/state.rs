//! Application state management.

use crate::api::types::{QueryActivity, QueryProgress};
use crate::db::{DatabaseBackend, DatabaseType, DatabaseUrl};
use crate::history::QueryHistoryService;
use crate::import::ImportProgress;
#[cfg(feature = "crustdb")]
use crate::settings;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[cfg(feature = "desktop")]
use serde::Serialize;
#[cfg(feature = "desktop")]
use tauri::{AppHandle, Emitter};

#[cfg(feature = "crustdb")]
use crate::db::CrustDatabase;
#[cfg(feature = "falkordb")]
use crate::db::FalkorDbDatabase;
#[cfg(feature = "neo4j")]
use crate::db::Neo4jDatabase;

// ============================================================================
// Running Query State
// ============================================================================

/// State for a running query.
pub struct RunningQuery {
    pub query_id: String,
    pub query: String,
    pub started_at: std::time::Instant,
    pub started_at_unix: i64,
    pub cancel_token: CancellationToken,
    pub progress_tx: broadcast::Sender<QueryProgress>,
    pub final_state: RwLock<Option<QueryProgress>>,
    /// When the query completed (for TTL-based cleanup).
    pub completed_at: RwLock<Option<std::time::Instant>>,
}

// ============================================================================
// Import Job State
// ============================================================================

/// Import job state: channel for live updates + optional final state for late subscribers.
pub struct ImportJob {
    pub channel: broadcast::Sender<ImportProgress>,
    pub final_state: RwLock<Option<ImportProgress>>,
}

// ============================================================================
// Database Connection State
// ============================================================================

/// Database connection state.
struct DatabaseConnection {
    backend: Arc<dyn DatabaseBackend>,
    db_type: DatabaseType,
    /// Database path (for file-based backends like CrustDB).
    #[allow(dead_code)]
    db_path: Option<PathBuf>,
}

// ============================================================================
// Application State
// ============================================================================

/// Application state shared across requests.
#[derive(Clone)]
pub struct AppState {
    /// Current database connection (if any).
    connection: Arc<RwLock<Option<DatabaseConnection>>>,
    /// Query history service (created on connect, persists across queries).
    history_service: Arc<RwLock<Option<Arc<QueryHistoryService>>>>,
    /// Active import jobs and their progress channels.
    pub import_jobs: Arc<DashMap<String, Arc<ImportJob>>>,
    /// Active running queries for tracking and cancellation.
    pub running_queries: Arc<DashMap<String, Arc<RunningQuery>>>,
    /// Counter for synchronous queries (path finding, connections, etc.)
    sync_query_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Broadcast channel for query activity updates.
    pub query_activity_tx: broadcast::Sender<QueryActivity>,
    /// Tauri app handle for emitting events (desktop mode only).
    #[cfg(feature = "desktop")]
    app_handle: Arc<RwLock<Option<AppHandle>>>,
}

impl AppState {
    /// Create a new AppState without a database connection.
    pub fn new_disconnected() -> Self {
        let (query_activity_tx, _) = broadcast::channel(16);
        Self {
            connection: Arc::new(RwLock::new(None)),
            history_service: Arc::new(RwLock::new(None)),
            import_jobs: Arc::new(DashMap::new()),
            running_queries: Arc::new(DashMap::new()),
            sync_query_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            query_activity_tx,
            #[cfg(feature = "desktop")]
            app_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new AppState with an initial database connection.
    pub fn new_connected(
        backend: Arc<dyn DatabaseBackend>,
        db_type: DatabaseType,
        db_path: Option<PathBuf>,
    ) -> Self {
        let (query_activity_tx, _) = broadcast::channel(16);

        // Create history service based on database type
        let history_service = Self::create_history_service(db_type, db_path.as_deref());

        Self {
            connection: Arc::new(RwLock::new(Some(DatabaseConnection {
                backend,
                db_type,
                db_path,
            }))),
            history_service: Arc::new(RwLock::new(Some(Arc::new(history_service)))),
            import_jobs: Arc::new(DashMap::new()),
            running_queries: Arc::new(DashMap::new()),
            sync_query_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            query_activity_tx,
            #[cfg(feature = "desktop")]
            app_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a history service based on database type.
    fn create_history_service(
        db_type: DatabaseType,
        db_path: Option<&std::path::Path>,
    ) -> QueryHistoryService {
        match db_type {
            // File-based backends: use SQLite storage in the same directory
            DatabaseType::CrustDB => {
                if let Some(path) = db_path {
                    let history_path = path.to_path_buf();
                    match QueryHistoryService::new_sqlite(&history_path) {
                        Ok(service) => return service,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to create SQLite history storage: {}. Falling back to in-memory.",
                                e
                            );
                        }
                    }
                }
                QueryHistoryService::new_in_memory()
            }
            // Remote backends: use in-memory storage
            DatabaseType::Neo4j | DatabaseType::FalkorDB => QueryHistoryService::new_in_memory(),
        }
    }

    /// Set the Tauri app handle for event emission (desktop mode only).
    #[cfg(feature = "desktop")]
    pub fn set_app_handle(&self, handle: AppHandle) {
        *self.app_handle.write() = Some(handle);
    }

    /// Emit a Tauri event (desktop mode only).
    /// In headless mode, this is a no-op.
    #[cfg(feature = "desktop")]
    pub fn emit_event<T: Serialize + Clone>(&self, event: &str, payload: T) {
        if let Some(handle) = self.app_handle.read().as_ref() {
            if let Err(e) = handle.emit(event, payload) {
                debug!(event = %event, error = %e, "Failed to emit Tauri event");
            }
        }
    }

    /// Emit import progress event (works in both desktop and headless modes).
    pub fn emit_import_progress(&self, job_id: &str, progress: &ImportProgress) {
        #[cfg(feature = "desktop")]
        {
            #[derive(Serialize, Clone)]
            struct ImportProgressEvent<'a> {
                job_id: &'a str,
                #[serde(flatten)]
                progress: ImportProgress,
            }
            self.emit_event(
                "import-progress",
                ImportProgressEvent {
                    job_id,
                    progress: progress.clone(),
                },
            );
        }
        #[cfg(not(feature = "desktop"))]
        {
            let _ = (job_id, progress); // Suppress unused warnings
        }
    }

    /// Emit query progress event (works in both desktop and headless modes).
    pub fn emit_query_progress(&self, progress: &QueryProgress) {
        #[cfg(feature = "desktop")]
        self.emit_event("query-progress", progress.clone());
        #[cfg(not(feature = "desktop"))]
        let _ = progress;
    }

    /// Emit query activity event (works in both desktop and headless modes).
    pub fn emit_query_activity(&self, activity: &QueryActivity) {
        #[cfg(feature = "desktop")]
        self.emit_event("query-activity", activity.clone());
        #[cfg(not(feature = "desktop"))]
        let _ = activity;
    }

    /// Get total active query count (async + sync).
    /// Only counts queries that haven't completed yet.
    pub fn active_query_count(&self) -> usize {
        let async_count = self
            .running_queries
            .iter()
            .filter(|entry| entry.value().completed_at.read().is_none())
            .count();
        async_count
            + self
                .sync_query_count
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Broadcast query activity update (via both HTTP SSE and Tauri events).
    pub fn broadcast_query_activity(&self) {
        let active = self.active_query_count();
        let activity = QueryActivity { active };
        let _ = self.query_activity_tx.send(activity.clone());
        self.emit_query_activity(&activity);
    }

    /// Increment sync query count and broadcast.
    pub fn start_sync_query(&self) {
        self.sync_query_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.broadcast_query_activity();
    }

    /// Decrement sync query count and broadcast.
    pub fn end_sync_query(&self) {
        self.sync_query_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.broadcast_query_activity();
    }

    /// Spawn a background task to clean up completed queries after TTL expires.
    /// Queries that have been completed for more than 2 minutes are removed.
    pub fn spawn_query_cleanup_task(&self) {
        let running_queries = self.running_queries.clone();
        const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
        const QUERY_TTL: std::time::Duration = std::time::Duration::from_secs(120);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(CLEANUP_INTERVAL).await;

                let now = std::time::Instant::now();
                let mut to_remove = Vec::new();

                // Find queries that have been completed for longer than TTL
                for entry in running_queries.iter() {
                    if let Some(completed_at) = *entry.value().completed_at.read() {
                        if now.duration_since(completed_at) > QUERY_TTL {
                            to_remove.push(entry.key().clone());
                        }
                    }
                }

                // Remove expired queries
                for query_id in to_remove {
                    running_queries.remove(&query_id);
                    debug!(query_id = %query_id, "Cleaned up expired query");
                }
            }
        });
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
    #[allow(unused_variables, unreachable_code)]
    pub fn connect(&self, url: &str) -> Result<DatabaseType, String> {
        let parsed = DatabaseUrl::parse(url).map_err(|e| e.to_string())?;

        // Track the database path for file-based backends
        #[allow(unused_mut)]
        let mut db_path: Option<PathBuf> = None;

        let backend: Arc<dyn DatabaseBackend> = match parsed.db_type {
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
                    parsed.use_ssl,
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
                db_path = Some(PathBuf::from(&path));
                let app_settings = settings::load();
                let db = CrustDatabase::new(&path, app_settings.query_caching)
                    .map_err(|e: crate::db::DbError| e.to_string())?;
                Arc::new(db)
            }
            #[cfg(not(feature = "crustdb"))]
            DatabaseType::CrustDB => {
                return Err(
                    "CrustDB support not compiled in. See Cargo.toml for instructions.".to_string(),
                );
            }
        };

        // Verify the connection is working before storing it
        backend.ping().map_err(|e| e.to_string())?;

        let db_type = parsed.db_type;

        // Create history service based on backend type
        let history_service = Self::create_history_service(db_type, db_path.as_deref());

        *self.connection.write() = Some(DatabaseConnection {
            backend,
            db_type,
            db_path,
        });
        *self.history_service.write() = Some(Arc::new(history_service));

        info!(database_type = %db_type.name(), "Connected to database");
        Ok(db_type)
    }

    /// Disconnect from the current database.
    pub fn disconnect(&self) {
        *self.connection.write() = None;
        *self.history_service.write() = None;
        info!("Disconnected from database");
    }

    /// Get a reference to the history service if connected.
    pub fn history(&self) -> Option<Arc<QueryHistoryService>> {
        self.history_service.read().clone()
    }

    /// Get the history service, returning an error if not connected.
    pub fn require_history(&self) -> Result<Arc<QueryHistoryService>, crate::api::types::ApiError> {
        self.history()
            .ok_or(crate::api::types::ApiError::NotConnected)
    }

    /// Get the database, returning an error if not connected.
    pub fn require_db(&self) -> Result<Arc<dyn DatabaseBackend>, crate::api::types::ApiError> {
        self.db().ok_or(crate::api::types::ApiError::NotConnected)
    }
}
