//! API route handlers.

use crate::api::types::{
    AddEdgeRequest, AddHistoryRequest, AddNodeRequest, ApiError, BrowseEntry, BrowseParams,
    BrowseResponse, ConnectRequest, DatabaseStatus, GenerateRequest, GenerateResponse,
    HistoryParams, NodeCounts, NodeStatus, PathParams, PathResponse, PathStep, PathsToDaEntry,
    PathsToDaParams, PathsToDaResponse, QueryActivity, QueryHistoryEntry, QueryHistoryResponse,
    QueryProgress, QueryRequest, QueryStartResponse, QueryStatus, SearchParams, SupportedDatabase,
};
use crate::db::{
    DatabaseBackend, DbEdge, DbError, DbNode, NewQueryHistoryEntry, QueryLanguage,
    Result as DbResult,
};
use crate::graph::{extract_graph_from_results, FullGraph, GraphEdge, GraphNode};
use crate::import::{BloodHoundImporter, ImportProgress};
use crate::settings::{self, Settings};
use crate::state::{AppState, ImportJob, RunningQuery};
use axum::{
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        Json,
    },
};
use parking_lot::RwLock;
use serde_json::{json, Value as JsonValue};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

// ============================================================================
// Database Helper
// ============================================================================

/// Run a blocking database operation in a spawn_blocking task.
///
/// This helper reduces boilerplate for the common pattern of running
/// synchronous database operations in an async context.
async fn run_db<T, F>(db: Arc<dyn DatabaseBackend>, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&dyn DatabaseBackend) -> DbResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(db.as_ref()))
        .await
        .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
        .map_err(Into::into)
}

// ============================================================================
// Health Check
// ============================================================================

pub async fn health_check() -> Json<JsonValue> {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

// ============================================================================
// Database Connection Endpoints
// ============================================================================

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

    #[cfg(feature = "kuzu")]
    supported.push(SupportedDatabase {
        id: "kuzu",
        name: "KuzuDB",
        connection_type: "file",
    });

    #[cfg(feature = "cozo")]
    supported.push(SupportedDatabase {
        id: "cozo",
        name: "CozoDB",
        connection_type: "file",
    });

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

// ============================================================================
// Import Endpoints
// ============================================================================

/// Handle BloodHound data import via multipart upload.
#[instrument(skip(state, multipart))]
pub async fn import_bloodhound(
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
        let msg = format!("{e}");
        let hint = if msg.contains("boundary") || msg.contains("content-type") {
            "Ensure Content-Type header is 'multipart/form-data' with a valid boundary"
        } else if msg.contains("size") || msg.contains("limit") || msg.contains("too large") {
            "File size exceeds the upload limit (500MB)"
        } else {
            "Check that files are sent as multipart/form-data with field name 'files'"
        };
        ApiError::BadRequest(format!("Failed to parse upload: {e}. Hint: {hint}"))
    })? {
        let filename = field.file_name().unwrap_or("unknown").to_string();

        // Create temp file path with unique ID
        // Use only UUID + sanitized extension to prevent path traversal attacks
        let extension = std::path::Path::new(&filename)
            .extension()
            .and_then(|ext| ext.to_str())
            .filter(|ext| ext.chars().all(|c| c.is_ascii_alphanumeric()))
            .map(|ext| format!(".{}", ext))
            .unwrap_or_default();
        let temp_path = std::env::temp_dir().join(format!(
            "admapper-upload-{}{}",
            uuid::Uuid::new_v4(),
            extension
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
            ApiError::BadRequest(format!("Failed to read file '{}': {}", filename, e))
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
    let job_id_for_events = job_id.clone();
    let import_jobs = state.import_jobs.clone();
    let job_for_task = job.clone();
    let state_for_task = state.clone();

    // Spawn import task
    tokio::task::spawn_blocking(move || {
        let mut importer = BloodHoundImporter::new(db, tx);

        // Separate files by type
        let (zip_files, json_files): (Vec<_>, Vec<_>) = files
            .iter()
            .partition(|(filename, _)| filename.ends_with(".zip"));

        // Process ZIP files one at a time (they handle multiple files internally)
        for (filename, temp_path) in &zip_files {
            info!(filename = %filename, path = %temp_path.display(), "Importing ZIP file");
            let result = match std::fs::File::open(temp_path) {
                Ok(file) => importer.import_zip(file, &job_id_clone),
                Err(e) => {
                    error!(error = %e, path = %temp_path.display(), "Failed to open temp file");
                    Err(format!("Failed to open temp file: {e}"))
                }
            };

            match &result {
                Ok(progress) => {
                    info!(
                        filename = %filename,
                        nodes = progress.nodes_imported,
                        edges = progress.edges_imported,
                        "ZIP imported successfully"
                    );
                    *job_for_task.final_state.write() = Some(progress.clone());
                    state_for_task.emit_import_progress(&job_id_for_events, progress);
                }
                Err(e) => {
                    error!(filename = %filename, error = %e, "ZIP import failed");
                }
            }
        }

        // Process all JSON files together with unified progress tracking
        if !json_files.is_empty() {
            // Filter to only .json files (skip unsupported types)
            let valid_json_files: Vec<(String, &std::path::PathBuf)> = json_files
                .iter()
                .filter(|(filename, _)| filename.ends_with(".json"))
                .map(|(filename, path)| (filename.clone(), path))
                .collect();

            if !valid_json_files.is_empty() {
                info!(file_count = valid_json_files.len(), "Importing JSON files");
                let result = importer.import_json_files(&valid_json_files, &job_id_clone);

                match &result {
                    Ok(progress) => {
                        info!(
                            nodes = progress.nodes_imported,
                            edges = progress.edges_imported,
                            "JSON files imported successfully"
                        );
                        *job_for_task.final_state.write() = Some(progress.clone());
                        state_for_task.emit_import_progress(&job_id_for_events, progress);
                    }
                    Err(e) => {
                        error!(error = %e, "JSON import failed");
                    }
                }
            }
        }

        // Clean up temp files
        for (filename, temp_path) in files {
            if let Err(e) = std::fs::remove_file(&temp_path) {
                debug!(filename = %filename, error = %e, "Failed to remove temp file (may already be cleaned up)");
            }
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
pub async fn import_progress(
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

// ============================================================================
// Graph Statistics Endpoints
// ============================================================================

/// Get graph statistics.
#[instrument(skip(state))]
pub async fn graph_stats(State(state): State<AppState>) -> Result<Json<JsonValue>, ApiError> {
    let db = state.require_db()?;
    let (node_count, edge_count) = run_db(db, |db| db.get_stats()).await?;

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
pub async fn graph_detailed_stats(
    State(state): State<AppState>,
) -> Result<Json<crate::db::DetailedStats>, ApiError> {
    let db = state.require_db()?;
    let stats = run_db(db, |db| db.get_detailed_stats()).await?;

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
        // Execute Cypher query to delete disabled nodes and their edges
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
    let (nodes, edges) =
        tokio::task::spawn_blocking(move || crate::generate::Generator::generate(size))
            .await
            .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?;

    let node_count = nodes.len();
    let edge_count = edges.len();

    info!(
        nodes = node_count,
        edges = edge_count,
        "Generated sample data, inserting..."
    );

    // Insert nodes
    run_db(db.clone(), move |db| db.insert_nodes(&nodes)).await?;

    // Insert edges
    run_db(db, move |db| db.insert_edges(&edges)).await?;

    info!(
        nodes = node_count,
        edges = edge_count,
        "Sample data generation complete"
    );

    Ok(Json(GenerateResponse {
        nodes: node_count,
        edges: edge_count,
    }))
}

// ============================================================================
// Graph Node/Edge Endpoints
// ============================================================================

/// Get all graph nodes.
pub async fn graph_nodes(State(state): State<AppState>) -> Result<Json<Vec<DbNode>>, ApiError> {
    let db = state.require_db()?;
    let nodes: Vec<DbNode> = run_db(db, |db| db.get_all_nodes()).await?;
    Ok(Json(nodes))
}

/// Get all graph edges.
pub async fn graph_edges(State(state): State<AppState>) -> Result<Json<Vec<GraphEdge>>, ApiError> {
    let db = state.require_db()?;
    let edges: Vec<DbEdge> = run_db(db, |db| db.get_all_edges()).await?;
    let result: Vec<GraphEdge> = edges.into_iter().map(GraphEdge::from).collect();
    Ok(Json(result))
}

/// Get full graph (nodes and edges).
pub async fn graph_all(State(state): State<AppState>) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    let (nodes, edges): (Vec<DbNode>, Vec<DbEdge>) = run_db(db, |db| {
        let nodes = db.get_all_nodes()?;
        let edges = db.get_all_edges()?;
        Ok((nodes, edges))
    })
    .await?;

    let result = FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        edges: edges.into_iter().map(GraphEdge::from).collect(),
    };

    Ok(Json(result))
}

/// Search nodes by label (for autocomplete).
#[instrument(skip(state))]
pub async fn graph_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<DbNode>>, ApiError> {
    if params.q.len() < 2 {
        return Ok(Json(Vec::new()));
    }

    let db = state.require_db()?;

    let query = params.q.clone();
    let limit = params.limit;
    let nodes: Vec<DbNode> = run_db(db, move |db| db.search_nodes(&query, limit)).await?;

    debug!(query = %params.q, results = nodes.len(), "Search complete");
    Ok(Json(nodes))
}

/// Get a single node by ID with full properties.
///
/// This endpoint is used to fetch node properties on-demand when clicking
/// on a node in the graph visualization.
#[instrument(skip(state))]
pub async fn node_get(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<DbNode>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Fetching node properties");

    let node_id_clone = node_id.clone();
    let nodes = run_db(db, move |db| db.get_nodes_by_ids(&[node_id_clone])).await?;

    nodes
        .into_iter()
        .next()
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("Node not found: {node_id}")))
}

/// Get connection counts for a node.
/// Returns counts for incoming, outgoing, admin permissions, memberOf, and members.
#[instrument(skip(state))]
pub async fn node_counts(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeCounts>, ApiError> {
    let db = state.require_db()?;
    let node_id_clone = node_id.clone();
    let (incoming, outgoing, admin_to, member_of, members) =
        run_db(db, move |db| db.get_node_edge_counts(&node_id_clone)).await?;

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
pub async fn node_connections(
    State(state): State<AppState>,
    Path((node_id, direction)): Path<(String, String)>,
) -> Result<Json<FullGraph>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, direction = %direction, "Loading node connections");

    let node_id_clone = node_id.clone();
    let direction_clone = direction.clone();
    let (nodes, edges): (Vec<DbNode>, Vec<DbEdge>) = run_db(db, move |db| {
        db.get_node_connections(&node_id_clone, &direction_clone)
    })
    .await?;

    Ok(Json(FullGraph {
        nodes: nodes.into_iter().map(GraphNode::from).collect(),
        edges: edges.into_iter().map(GraphEdge::from).collect(),
    }))
}

/// Get security status for a node.
///
/// Checks in order (aborting early when a condition is met):
/// 1. Independent properties: owned, disabled
/// 2. Membership in Enterprise Admins (-519)
/// 3. Membership in Domain Admins (-512)
/// 4. Membership in other high-value groups
/// 5. Path to Enterprise Admins
/// 6. Path to Domain Admins
/// 7. Path to other high-value groups
#[instrument(skip(state))]
pub async fn node_status(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeStatus>, ApiError> {
    let db = state.require_db()?;
    info!(node_id = %node_id, "Checking node security status");

    // Well-known high-value RIDs (excluding -512 DA and -519 EA which are checked separately):
    //   -518: Schema Admins
    //   -516: Domain Controllers
    //   -498: Enterprise Read-Only Domain Controllers
    //   -544: Administrators (local)
    //   -548: Account Operators
    //   -549: Server Operators
    //   -551: Backup Operators
    const OTHER_HIGH_VALUE_RIDS: &[&str] =
        &["-518", "-516", "-498", "-544", "-548", "-549", "-551"];

    // === Step 1: Get node type and independent properties (owned, disabled) ===
    let node_id_clone = node_id.clone();
    let db_for_props = db.clone();
    let (node_label, owned, is_disabled) = run_db(db_for_props, move |db| {
        let nodes = db.get_nodes_by_ids(std::slice::from_ref(&node_id_clone))?;
        let node = nodes.first();

        let label = node.map(|n| n.label.to_lowercase()).unwrap_or_default();

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

        // Check if disabled (enabled=false means disabled)
        // Only applicable to users, computers, groups
        let is_disabled = node
            .and_then(|n| {
                let props = &n.properties;
                props.get("enabled").or(props.get("Enabled")).and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_i64().map(|i| i == 1))
                        .or_else(|| v.as_str().map(|s| s == "true"))
                })
            })
            .map(|enabled| !enabled) // disabled = NOT enabled
            .unwrap_or(false);

        Ok((label, owned, is_disabled))
    })
    .await?;

    // Only run expensive membership/path checks for users, computers, and groups
    let dominated_types = ["user", "computer", "group"];
    if !dominated_types.contains(&node_label.as_str()) {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled: false, // Not applicable to domains, OUs, etc.
            is_enterprise_admin: false,
            is_domain_admin: false,
            is_high_value: false,
            has_path_to_high_value: false,
            path_length: None,
        }));
    }

    // === Step 2: Check membership in Enterprise Admins (-519) ===
    let node_id_clone = node_id.clone();
    let db_for_ea = db.clone();
    let is_enterprise_admin = run_db(db_for_ea, move |db| {
        Ok(db
            .find_membership_by_sid_suffix(&node_id_clone, "-519")?
            .is_some())
    })
    .await?;

    if is_enterprise_admin {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: true,
            is_domain_admin: false,
            is_high_value: true,
            has_path_to_high_value: false,
            path_length: None,
        }));
    }

    // === Step 3: Check membership in Domain Admins (-512) ===
    let node_id_clone = node_id.clone();
    let db_for_da = db.clone();
    let is_domain_admin = run_db(db_for_da, move |db| {
        Ok(db
            .find_membership_by_sid_suffix(&node_id_clone, "-512")?
            .is_some())
    })
    .await?;

    if is_domain_admin {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: true,
            is_high_value: true,
            has_path_to_high_value: false,
            path_length: None,
        }));
    }

    // === Step 4: Check membership in other high-value groups ===
    let node_id_clone = node_id.clone();
    let db_for_hv = db.clone();
    let is_high_value = run_db(db_for_hv, move |db| {
        // Check highvalue property first
        let nodes = db.get_nodes_by_ids(std::slice::from_ref(&node_id_clone))?;
        if let Some(node) = nodes.first() {
            let props = &node.properties;
            let high_value_prop = props
                .get("highvalue")
                .or(props.get("HighValue"))
                .or(props.get("highValue"))
                .and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_i64().map(|i| i == 1))
                        .or_else(|| v.as_str().map(|s| s == "true"))
                })
                .unwrap_or(false);
            if high_value_prop {
                return Ok(true);
            }
        }

        // Check membership in other high-value groups
        for rid in OTHER_HIGH_VALUE_RIDS {
            if db
                .find_membership_by_sid_suffix(&node_id_clone, rid)?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    })
    .await?;

    if is_high_value {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            is_high_value: true,
            has_path_to_high_value: false,
            path_length: None,
        }));
    }

    // === Step 5: Check path to Enterprise Admins (-519) ===
    let path_to_ea = check_path_to_rid(&state, &db, &node_id, "-519", "Enterprise Admins").await?;
    if let Some(hops) = path_to_ea {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            is_high_value: false,
            has_path_to_high_value: true,
            path_length: Some(hops),
        }));
    }

    // === Step 6: Check path to Domain Admins (-512) ===
    let path_to_da = check_path_to_rid(&state, &db, &node_id, "-512", "Domain Admins").await?;
    if let Some(hops) = path_to_da {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            is_high_value: false,
            has_path_to_high_value: true,
            path_length: Some(hops),
        }));
    }

    // === Step 7: Check path to other high-value groups ===
    let rid_conditions: Vec<String> = OTHER_HIGH_VALUE_RIDS
        .iter()
        .map(|rid| format!("b.object_id ENDS WITH '{}'", rid))
        .collect();
    let path_to_hv = check_path_to_condition(
        &state,
        &db,
        &node_id,
        &rid_conditions.join(" OR "),
        "high-value",
    )
    .await?;
    if let Some(hops) = path_to_hv {
        return Ok(Json(NodeStatus {
            owned,
            is_disabled,
            is_enterprise_admin: false,
            is_domain_admin: false,
            is_high_value: false,
            has_path_to_high_value: true,
            path_length: Some(hops),
        }));
    }

    // No high-value status or paths found
    Ok(Json(NodeStatus {
        owned,
        is_disabled,
        is_enterprise_admin: false,
        is_domain_admin: false,
        is_high_value: false,
        has_path_to_high_value: false,
        path_length: None,
    }))
}

/// Helper: Check if there's a path to a group with given RID suffix.
/// Returns Some(hops) if path found, None otherwise.
async fn check_path_to_rid(
    state: &AppState,
    db: &Arc<dyn DatabaseBackend>,
    node_id: &str,
    rid: &str,
    target_name: &str,
) -> Result<Option<usize>, ApiError> {
    let condition = format!("b.object_id ENDS WITH '{}'", rid);
    check_path_to_condition(state, db, node_id, &condition, target_name).await
}

/// Helper: Check if there's a path matching a WHERE condition.
/// Returns Some(hops) if path found, None otherwise.
async fn check_path_to_condition(
    state: &AppState,
    db: &Arc<dyn DatabaseBackend>,
    node_id: &str,
    condition: &str,
    target_name: &str,
) -> Result<Option<usize>, ApiError> {
    let query_id = uuid::Uuid::new_v4().to_string();
    let started_at = std::time::Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let escaped_id = node_id.replace('\'', "\\'");
    let query_name = format!("Path to {}: {}", target_name, node_id);
    let query_text = format!(
        "MATCH p = (a)-[]-+(b) WHERE a.object_id = '{}' AND ({}) RETURN length(p) AS hops LIMIT 1",
        escaped_id, condition
    );

    // Add to history with "running" status (background query)
    if let Err(e) = db.add_query_history(NewQueryHistoryEntry {
        id: &query_id,
        name: &query_name,
        query: &query_text,
        timestamp: started_at_unix,
        result_count: None,
        status: "running",
        started_at: started_at_unix,
        duration_ms: None,
        error: None,
        background: true,
    }) {
        warn!(error = %e, "Failed to add path check to history");
    }

    state.start_sync_query();

    let query_text_clone = query_text.clone();
    let db_clone = db.clone();
    let result: Result<Option<usize>, ApiError> = run_db(db_clone, move |db| {
        let result = db.run_custom_query(&query_text_clone)?;
        if let Some(rows) = result.get("rows").and_then(|v| v.as_array()) {
            if let Some(first_row) = rows.first().and_then(|r| r.as_array()) {
                if let Some(hops) = first_row.first().and_then(|h| h.as_i64()) {
                    return Ok(Some(hops as usize));
                }
            }
        }
        Ok(None)
    })
    .await;

    state.end_sync_query();

    let duration_ms = started_at.elapsed().as_millis() as u64;

    match &result {
        Ok(path_len) => {
            let result_count = path_len.map(|l| l as i64).or(Some(0));
            if let Err(e) = db.update_query_status(
                &query_id,
                "completed",
                Some(duration_ms),
                result_count,
                None,
            ) {
                warn!(error = %e, "Failed to update path check history");
            }
        }
        Err(e) => {
            let error_str = e.to_string();
            if let Err(e2) = db.update_query_status(
                &query_id,
                "failed",
                Some(duration_ms),
                None,
                Some(&error_str),
            ) {
                warn!(error = %e2, "Failed to update path check history");
            }
        }
    }

    result
}

/// Request body for setting owned status.
#[derive(Debug, serde::Deserialize)]
pub struct SetOwnedRequest {
    pub owned: bool,
}

/// Toggle the owned status of a node.
#[instrument(skip(state))]
pub async fn node_set_owned(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(body): Json<SetOwnedRequest>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;

    // Escape the node_id for use in Cypher query
    let escaped_id = node_id.replace('\'', "\\'");
    let query = format!(
        "MATCH (n {{object_id: '{}'}}) SET n.owned = {}",
        escaped_id, body.owned
    );

    run_db(db, move |db| db.run_custom_query(&query)).await?;

    info!(node_id = %node_id, owned = %body.owned, "Set node owned status");
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Path Finding Endpoints
// ============================================================================

/// Find shortest path between two nodes.
/// Accepts either object IDs or labels as identifiers.
#[instrument(skip(state))]
pub async fn graph_path(
    State(state): State<AppState>,
    Query(params): Query<PathParams>,
) -> Result<Json<PathResponse>, ApiError> {
    let db = state.require_db()?;

    // Generate query ID and track in history
    let query_id = uuid::Uuid::new_v4().to_string();
    let started_at = std::time::Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // First resolve identifiers to get the actual object IDs for the Cypher query
    let from_param = params.from.clone();
    let to_param = params.to.clone();
    let db_for_resolve = db.clone();
    let (from_id, to_id) = run_db(db_for_resolve, move |db| {
        let from_id = match db.resolve_node_identifier(&from_param)? {
            Some(id) => id,
            None => return Err(DbError::Database(format!("Node not found: {}", from_param))),
        };
        let to_id = match db.resolve_node_identifier(&to_param)? {
            Some(id) => id,
            None => return Err(DbError::Database(format!("Node not found: {}", to_param))),
        };
        Ok((from_id, to_id))
    })
    .await?;

    // Generate proper Cypher query for history (can be re-run from query history)
    let escaped_from = from_id.replace('\'', "\\'");
    let escaped_to = to_id.replace('\'', "\\'");
    let query_name = format!("Path: {} → {}", params.from, params.to);
    let query_text = format!(
        "MATCH p = SHORTEST 1 (a)-[]-+(b) WHERE a.object_id = '{}' AND b.object_id = '{}' RETURN p",
        escaped_from, escaped_to
    );

    // Add to history with "running" status
    if let Err(e) = db.add_query_history(NewQueryHistoryEntry {
        id: &query_id,
        name: &query_name,
        query: &query_text,
        timestamp: started_at_unix,
        result_count: None,
        status: "running",
        started_at: started_at_unix,
        duration_ms: None,
        error: None,
        background: false,
    }) {
        warn!(error = %e, "Failed to add path query to history");
    }

    state.start_sync_query();

    let from_id_for_closure = from_id.clone();
    let to_id_for_closure = to_id.clone();
    let db_for_query = db.clone();
    let result = run_db(db_for_query, move |db| {
        let path_result = db.shortest_path(&from_id_for_closure, &to_id_for_closure)?;

        match path_result {
            None => {
                debug!(from = %from_id_for_closure, to = %to_id_for_closure, "No path found");
                Ok(PathResponse {
                    found: false,
                    path: Vec::new(),
                    graph: FullGraph {
                        nodes: Vec::new(),
                        edges: Vec::new(),
                    },
                })
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
                            name: id.clone(),
                            label: "Unknown".to_string(),
                            properties: JsonValue::Null,
                        });
                        PathStep {
                            node,
                            edge_type: edge_type.clone(),
                        }
                    })
                    .collect();

                // Get edges between path nodes
                let edges = db.get_edges_between(&node_ids)?;

                let graph = FullGraph {
                    nodes: path_steps
                        .iter()
                        .map(|s| GraphNode::from(s.node.clone()))
                        .collect(),
                    edges: edges.into_iter().map(GraphEdge::from).collect(),
                };

                debug!(
                    from = %from_id_for_closure,
                    to = %to_id_for_closure,
                    path_len = path_steps.len(),
                    "Path found"
                );

                Ok(PathResponse {
                    found: true,
                    path: path_steps,
                    graph,
                })
            }
        }
    })
    .await;

    state.end_sync_query();

    let duration_ms = started_at.elapsed().as_millis() as u64;

    match &result {
        Ok(response) => {
            let result_count = if response.found {
                Some(response.path.len() as i64)
            } else {
                Some(0)
            };
            if let Err(e) = db.update_query_status(
                &query_id,
                "completed",
                Some(duration_ms),
                result_count,
                None,
            ) {
                warn!(error = %e, "Failed to update path query history");
            }
        }
        Err(e) => {
            let error_str = e.to_string();
            if let Err(e2) = db.update_query_status(
                &query_id,
                "failed",
                Some(duration_ms),
                None,
                Some(&error_str),
            ) {
                warn!(error = %e2, "Failed to update path query history");
            }
        }
    }

    Ok(Json(result?))
}

/// Find all users with paths to Domain Admins.
#[instrument(skip(state))]
pub async fn paths_to_domain_admins(
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
    let results: Vec<(String, String, String, usize)> =
        run_db(db, move |db| db.find_paths_to_domain_admins(&exclude_types)).await?;

    let entries: Vec<PathsToDaEntry> = results
        .into_iter()
        .map(|(id, label, name, hops)| PathsToDaEntry {
            id,
            label,
            name,
            hops,
        })
        .collect();

    let count = entries.len();
    info!(count = count, "Found users with paths to Domain Admins");

    Ok(Json(PathsToDaResponse { count, entries }))
}

// ============================================================================
// Graph Insights Endpoints
// ============================================================================

/// Get security insights from the graph.
#[instrument(skip(state))]
pub async fn graph_insights(
    State(state): State<AppState>,
) -> Result<Json<crate::db::SecurityInsights>, ApiError> {
    let db = state.require_db()?;
    let insights = run_db(db, |db| db.get_security_insights()).await?;

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
pub async fn graph_edge_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types: Vec<String> = run_db(db, |db| db.get_edge_types()).await?;
    debug!(count = types.len(), "Edge types retrieved");
    Ok(Json(types))
}

/// Get all distinct node types in the database.
#[instrument(skip(state))]
pub async fn graph_node_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let db = state.require_db()?;
    let types: Vec<String> = run_db(db, |db| db.get_node_types()).await?;
    debug!(count = types.len(), "Node types retrieved");
    Ok(Json(types))
}

/// Get choke points in the graph using edge betweenness centrality.
///
/// Returns the top edges through which the most shortest paths pass.
/// These are critical "choke point" edges whose removal would disrupt many attack paths.
///
/// Results are cached at the database level and automatically invalidated when data changes.
#[instrument(skip(state))]
pub async fn graph_choke_points(
    State(state): State<AppState>,
) -> Result<Json<crate::db::ChokePointsResponse>, ApiError> {
    let db = state.require_db()?;
    // Return top 10 choke points by default
    // CrustDB caches the result and auto-invalidates on data changes
    let result = run_db(db, |db| db.get_choke_points(10)).await?;
    info!(
        count = result.choke_points.len(),
        total_edges = result.total_edges,
        "Choke points retrieved"
    );
    Ok(Json(result))
}

// ============================================================================
// Node/Edge Mutation Endpoints
// ============================================================================

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

/// Add a new edge to the graph.
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
        ..Default::default()
    };

    let db = state.require_db()?;
    run_db(db, move |db| db.insert_edge(edge)).await?;
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

/// Delete a node from the graph.
#[instrument(skip(state))]
pub async fn delete_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    // Escape single quotes in the ID to prevent injection
    let escaped_id = id.replace('\'', "\\'");
    run_db(db, move |db| {
        // Use DETACH DELETE to also remove connected edges
        let query = format!(
            "MATCH (n) WHERE n.objectid = '{}' OR n.name = '{}' DETACH DELETE n",
            escaped_id, escaped_id
        );
        db.run_custom_query(&query)
    })
    .await?;
    info!(id = %id, "Node deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Delete an edge from the graph.
#[instrument(skip(state))]
pub async fn delete_edge(
    State(state): State<AppState>,
    Path((source, target, edge_type)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    // Escape single quotes to prevent injection
    let escaped_source = source.replace('\'', "\\'");
    let escaped_target = target.replace('\'', "\\'");
    // Edge type should be alphanumeric (relationship name)
    let safe_edge_type = edge_type
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();
    run_db(db, move |db| {
        let query = format!(
            "MATCH (a)-[r:{}]->(b) WHERE (a.objectid = '{}' OR a.name = '{}') AND (b.objectid = '{}' OR b.name = '{}') DELETE r",
            safe_edge_type, escaped_source, escaped_source, escaped_target, escaped_target
        );
        db.run_custom_query(&query)
    })
    .await?;
    info!(source = %source, target = %target, edge_type = %edge_type, "Edge deleted");
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Query Execution Endpoints
// ============================================================================

/// Execute a custom query.
///
/// For fast queries (<50ms), returns results inline (sync mode).
/// For slower queries, returns immediately with a query_id (async mode).
/// Subscribe to /api/query/progress/:id for async updates.
#[instrument(skip(state, body))]
pub async fn graph_query(
    State(state): State<AppState>,
    Json(body): Json<QueryRequest>,
) -> Result<Json<QueryStartResponse>, ApiError> {
    let db = state.require_db()?;
    info!(query = %body.query, "Starting query");

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
        completed_at: RwLock::new(None),
    });

    state
        .running_queries
        .insert(query_id.clone(), running_query.clone());

    // Broadcast query activity update (new query started)
    state.broadcast_query_activity();

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
    let _ = progress_tx.send(initial_progress.clone());
    state.emit_query_progress(&initial_progress);

    // Add query to history with "running" status
    let timestamp = started_at_unix;
    if let Err(e) = db.add_query_history(NewQueryHistoryEntry {
        id: &query_id,
        name: &body.query, // Use query text as name
        query: &body.query,
        timestamp,
        result_count: None,
        status: "running",
        started_at: started_at_unix,
        duration_ms: None,
        error: None,
        background: body.background,
    }) {
        warn!(error = %e, "Failed to add query to history");
    }

    // Spawn the query execution
    let query_id_clone = query_id.clone();
    let query_text = body.query.clone();
    let extract_graph = body.extract_graph;
    let language = body.language.clone();
    let running_query_for_task = running_query.clone();
    let state_for_task = state.clone();

    tokio::task::spawn_blocking(move || {
        // Helper to update query status in history
        let update_history = |status: &str,
                              duration_ms: Option<u64>,
                              result_count: Option<i64>,
                              error: Option<&str>| {
            if let Err(e) =
                db.update_query_status(&query_id_clone, status, duration_ms, result_count, error)
            {
                warn!(error = %e, "Failed to update query history status");
            }
        };

        // Check if cancelled before starting
        if cancel_token.is_cancelled() {
            let duration_ms = started_at.elapsed().as_millis() as u64;
            update_history("aborted", Some(duration_ms), None, None);
            let progress = QueryProgress {
                query_id: query_id_clone.clone(),
                status: QueryStatus::Aborted,
                started_at: started_at_unix,
                duration_ms: Some(duration_ms),
                result_count: None,
                error: None,
                results: None,
                graph: None,
            };
            let _ = progress_tx.send(progress.clone());
            state_for_task.emit_query_progress(&progress);
            *running_query_for_task.final_state.write() = Some(progress);
            *running_query_for_task.completed_at.write() = Some(std::time::Instant::now());
            state_for_task.broadcast_query_activity();
            return;
        }

        // Execute the query
        let result = if let Some(lang_str) = &language {
            lang_str
                .parse::<QueryLanguage>()
                .map_err(DbError::Database)
                .and_then(|lang| db.run_query_with_language(&query_text, lang))
        } else {
            db.run_custom_query(&query_text)
        };

        // Check if cancelled after query completion
        if cancel_token.is_cancelled() {
            let duration_ms = started_at.elapsed().as_millis() as u64;
            update_history("aborted", Some(duration_ms), None, None);
            let progress = QueryProgress {
                query_id: query_id_clone.clone(),
                status: QueryStatus::Aborted,
                started_at: started_at_unix,
                duration_ms: Some(duration_ms),
                result_count: None,
                error: None,
                results: None,
                graph: None,
            };
            let _ = progress_tx.send(progress.clone());
            state_for_task.emit_query_progress(&progress);
            *running_query_for_task.final_state.write() = Some(progress);
            *running_query_for_task.completed_at.write() = Some(std::time::Instant::now());
            state_for_task.broadcast_query_activity();
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

                // Update history with completed status
                update_history("completed", Some(duration_ms), result_count, None);

                QueryProgress {
                    query_id: query_id_clone.clone(),
                    status: QueryStatus::Completed,
                    started_at: started_at_unix,
                    duration_ms: Some(duration_ms),
                    result_count,
                    error: None,
                    // Only send raw results if we don't have an extracted graph.
                    // The frontend only uses the graph for visualization, so sending
                    // both is wasteful and can overwhelm the browser with large payloads.
                    results: if graph.is_some() { None } else { Some(results) },
                    graph,
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!(query_id = %query_id_clone, error = %error_msg, "Query failed");

                // Update history with failed status
                update_history("failed", Some(duration_ms), None, Some(&error_msg));

                QueryProgress {
                    query_id: query_id_clone.clone(),
                    status: QueryStatus::Failed,
                    started_at: started_at_unix,
                    duration_ms: Some(duration_ms),
                    result_count: None,
                    error: Some(error_msg),
                    results: None,
                    graph: None,
                }
            }
        };

        // Broadcast final status
        let _ = progress_tx.send(progress.clone());
        state_for_task.emit_query_progress(&progress);
        *running_query_for_task.final_state.write() = Some(progress);

        // Mark the query as completed with a timestamp for TTL-based cleanup.
        // The query stays in running_queries so late subscribers can get the result.
        // A background task will clean up queries that have been completed for >2 minutes.
        *running_query_for_task.completed_at.write() = Some(std::time::Instant::now());

        // Broadcast activity update (query no longer "running" for UI purposes)
        state_for_task.broadcast_query_activity();
    });

    // Wait briefly for fast queries to complete (50ms)
    // This allows simple queries like COUNT to return inline without SSE overhead
    const SYNC_TIMEOUT_MS: u64 = 50;
    tokio::time::sleep(std::time::Duration::from_millis(SYNC_TIMEOUT_MS)).await;

    // Check if query completed within the timeout
    if let Some(progress) = running_query.final_state.read().clone() {
        match progress.status {
            QueryStatus::Completed => {
                debug!(query_id = %query_id, "Query completed fast - returning inline results");
                return Ok(Json(QueryStartResponse::Sync {
                    query_id,
                    duration_ms: progress.duration_ms.unwrap_or(0),
                    result_count: progress.result_count,
                    results: progress.results,
                    graph: progress.graph,
                }));
            }
            QueryStatus::Failed => {
                // Return error inline for fast failures
                return Err(ApiError::BadRequest(
                    progress.error.unwrap_or_else(|| "Query failed".to_string()),
                ));
            }
            _ => {
                // Aborted or unexpected status - fall through to async
            }
        }
    }

    // Query still running - return async mode
    debug!(query_id = %query_id, "Query still running - returning async mode");
    Ok(Json(QueryStartResponse::Async { query_id }))
}

/// SSE endpoint for query progress updates.
pub async fn query_progress(
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
pub async fn query_abort(
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

    let duration_ms = query.started_at.elapsed().as_millis() as u64;

    // Update query history with aborted status
    if let Some(db) = state.db() {
        if let Err(e) = db.update_query_status(&query_id, "aborted", Some(duration_ms), None, None)
        {
            warn!(error = %e, "Failed to update query history status on abort");
        }
    }

    // Broadcast aborted status
    let progress = QueryProgress {
        query_id: query_id.clone(),
        status: QueryStatus::Aborted,
        started_at: query.started_at_unix,
        duration_ms: Some(duration_ms),
        result_count: None,
        error: None,
        results: None,
        graph: None,
    };
    let _ = query.progress_tx.send(progress.clone());
    *query.final_state.write() = Some(progress);

    Ok(StatusCode::NO_CONTENT)
}

/// SSE endpoint for query activity updates.
/// Broadcasts when the number of active queries changes.
pub async fn query_activity(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let active = state.active_query_count();
    debug!(active = active, "Query activity SSE connection opened");

    // Subscribe to updates first
    let rx = state.query_activity_tx.subscribe();

    // Create a stream that first sends current state, then updates
    let initial = QueryActivity { active };
    let initial_data = serde_json::to_string(&initial).unwrap_or_default();

    let stream = async_stream::stream! {
        // Send initial state
        yield Ok(Event::default().data(initial_data));

        // Then stream updates
        let mut stream = BroadcastStream::new(rx);
        while let Some(result) = stream.next().await {
            if let Ok(activity) = result {
                let data = serde_json::to_string(&activity).unwrap_or_default();
                yield Ok(Event::default().data(data));
            }
        }
    };

    // Use keep-alive to prevent connection timeout
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

// ============================================================================
// Query History Endpoints
// ============================================================================

/// Get query history with pagination.
#[instrument(skip(state))]
pub async fn get_query_history(
    State(state): State<AppState>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<QueryHistoryResponse>, ApiError> {
    let db = state.require_db()?;
    let page = params.page.max(1);
    let per_page = params.per_page.clamp(1, 100);
    let offset = (page - 1) * per_page;

    let (history, total): (Vec<_>, usize) =
        run_db(db, move |db| db.get_query_history(per_page, offset)).await?;

    let entries: Vec<QueryHistoryEntry> = history
        .into_iter()
        .map(|row| {
            let status = match row.status.as_str() {
                "running" => QueryStatus::Running,
                "completed" => QueryStatus::Completed,
                "failed" => QueryStatus::Failed,
                "aborted" => QueryStatus::Aborted,
                _ => QueryStatus::Completed, // Default fallback
            };
            QueryHistoryEntry {
                id: row.id,
                name: row.name,
                query: row.query,
                timestamp: row.timestamp,
                result_count: row.result_count,
                status,
                started_at: row.started_at,
                duration_ms: row.duration_ms,
                error: row.error,
                background: row.background,
            }
        })
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

/// Add a query to history.
#[instrument(skip(state, body))]
pub async fn add_query_history(
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

    let id_clone = id.clone();
    let name = body.name.clone();
    let query = body.query.clone();
    let result_count = body.result_count;
    let duration_ms = body.duration_ms;
    let error = body.error.clone();
    let background = body.background;
    let status_str_owned = status_str.to_string();
    run_db(db, move |db| {
        db.add_query_history(NewQueryHistoryEntry {
            id: &id_clone,
            name: &name,
            query: &query,
            timestamp: started_at,
            result_count,
            status: &status_str_owned,
            started_at,
            duration_ms,
            error: error.as_deref(),
            background,
        })
    })
    .await?;

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
        background: body.background,
    }))
}

/// Delete a query from history.
#[instrument(skip(state))]
pub async fn delete_query_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    let id_clone = id.clone();
    run_db(db, move |db| db.delete_query_history(&id_clone)).await?;
    info!(id = %id, "Query deleted from history");
    Ok(StatusCode::NO_CONTENT)
}

/// Clear all query history.
#[instrument(skip(state))]
pub async fn clear_query_history(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    let db = state.require_db()?;
    run_db(db, |db| db.clear_query_history()).await?;
    info!("Query history cleared");
    Ok(StatusCode::NO_CONTENT)
}

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
    let settings = settings::load();
    debug!(theme = %settings.theme, layout = %settings.default_graph_layout, "Settings loaded");
    Json(settings)
}

/// Update application settings.
#[instrument(skip(body))]
pub async fn update_settings(Json(body): Json<Settings>) -> Result<Json<Settings>, ApiError> {
    // Validate theme
    if body.theme != "dark" && body.theme != "light" {
        return Err(ApiError::BadRequest(format!(
            "Invalid theme: {}. Must be 'dark' or 'light'",
            body.theme
        )));
    }

    // Validate layout
    let valid_layouts = ["force", "hierarchical", "grid", "circular"];
    if !valid_layouts.contains(&body.default_graph_layout.as_str()) {
        return Err(ApiError::BadRequest(format!(
            "Invalid layout: {}. Must be one of: {}",
            body.default_graph_layout,
            valid_layouts.join(", ")
        )));
    }

    settings::save(&body)
        .map_err(|e| ApiError::Internal(format!("Failed to save settings: {e}")))?;

    info!(theme = %body.theme, layout = %body.default_graph_layout, "Settings updated");
    Ok(Json(body))
}

// ============================================================================
// File Browser
// ============================================================================

/// Browse directories on the server filesystem.
#[instrument(skip_all)]
pub async fn browse_directory(
    Query(params): Query<BrowseParams>,
) -> Result<Json<BrowseResponse>, ApiError> {
    use std::path::PathBuf;

    // Determine starting path
    let path = match &params.path {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => dirs::home_dir().unwrap_or_else(|| PathBuf::from("/")),
    };

    // Ensure path exists and is a directory
    if !path.exists() {
        return Err(ApiError::NotFound(format!(
            "Path does not exist: {}",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(ApiError::BadRequest(format!(
            "Path is not a directory: {}",
            path.display()
        )));
    }

    // Get canonical path
    let canonical = path
        .canonicalize()
        .map_err(|e| ApiError::Internal(format!("Failed to resolve path: {e}")))?;

    // Get parent directory
    let parent = canonical.parent().map(|p| p.to_string_lossy().to_string());

    // Read directory entries
    let mut entries = Vec::new();
    let read_dir = std::fs::read_dir(&canonical)
        .map_err(|e| ApiError::Internal(format!("Failed to read directory: {e}")))?;

    for entry in read_dir.flatten() {
        let entry_path = entry.path();
        let is_dir = entry_path.is_dir();
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip hidden files (starting with .)
        if name.starts_with('.') {
            continue;
        }

        entries.push(BrowseEntry {
            name,
            path: entry_path.to_string_lossy().to_string(),
            is_dir,
        });
    }

    // Sort: directories first, then alphabetically
    entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });

    Ok(Json(BrowseResponse {
        current: canonical.to_string_lossy().to_string(),
        parent,
        entries,
    }))
}
