//! Query execution and progress endpoints.

use crate::api::types::{
    ApiError, QueryActivity, QueryProgress, QueryRequest, QueryStartResponse, QueryStatus,
};
use crate::db::{DbError, NewQueryHistoryEntry, QueryLanguage};
use crate::graph::extract_graph_from_results;
use crate::state::{AppState, QueryDedupKey, RunningQuery};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        Json,
    },
};
use parking_lot::RwLock;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

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

    // Build dedup key from query parameters (excludes background and sync flags)
    let dedup_key = QueryDedupKey {
        query: body.query.clone(),
        extract_graph: body.extract_graph,
        language: body.language.clone(),
    };

    // Check for an identical running query we can piggyback on
    if let Some(existing_id) = state.query_dedup_index.get(&dedup_key) {
        let existing_query_id = existing_id.value().clone();
        if let Some(existing_query) = state.running_queries.get(&existing_query_id) {
            let rq = existing_query.value().clone();
            // Only share if the query hasn't completed yet
            if rq.completed_at.read().is_none() {
                rq.subscriber_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    query_id = %existing_query_id,
                    query = %body.query,
                    subscribers = rq.subscriber_count.load(std::sync::atomic::Ordering::Relaxed),
                    "Deduplicating query - sharing existing execution"
                );

                if body.sync {
                    // Sync mode: subscribe and wait for completion
                    let mut rx = rq.progress_tx.subscribe();
                    // Check if already completed (race with subscription)
                    if let Some(progress) = rq.final_state.read().clone() {
                        return match progress.status {
                            QueryStatus::Completed => Ok(Json(QueryStartResponse::Sync {
                                query_id: existing_query_id,
                                duration_ms: progress.duration_ms.unwrap_or(0),
                                result_count: progress.result_count,
                                results: progress.results,
                                graph: progress.graph,
                            })),
                            QueryStatus::Failed => Err(ApiError::BadRequest(
                                progress.error.unwrap_or_else(|| "Query failed".to_string()),
                            )),
                            _ => Err(ApiError::Internal("Query was aborted".to_string())),
                        };
                    }
                    // Wait for completion
                    while let Ok(progress) = rx.recv().await {
                        match progress.status {
                            QueryStatus::Completed => {
                                return Ok(Json(QueryStartResponse::Sync {
                                    query_id: existing_query_id,
                                    duration_ms: progress.duration_ms.unwrap_or(0),
                                    result_count: progress.result_count,
                                    results: progress.results,
                                    graph: progress.graph,
                                }));
                            }
                            QueryStatus::Failed => {
                                return Err(ApiError::BadRequest(
                                    progress.error.unwrap_or_else(|| "Query failed".to_string()),
                                ));
                            }
                            QueryStatus::Aborted => {
                                return Err(ApiError::Internal("Query was aborted".to_string()));
                            }
                            QueryStatus::Running => continue,
                        }
                    }
                    return Err(ApiError::Internal(
                        "Lost connection to shared query".to_string(),
                    ));
                }

                // Async mode: check if already completed (fast path)
                if let Some(progress) = rq.final_state.read().clone() {
                    match progress.status {
                        QueryStatus::Completed => {
                            return Ok(Json(QueryStartResponse::Sync {
                                query_id: existing_query_id,
                                duration_ms: progress.duration_ms.unwrap_or(0),
                                result_count: progress.result_count,
                                results: progress.results,
                                graph: progress.graph,
                            }));
                        }
                        QueryStatus::Failed => {
                            return Err(ApiError::BadRequest(
                                progress.error.unwrap_or_else(|| "Query failed".to_string()),
                            ));
                        }
                        _ => {} // fall through to async
                    }
                }

                return Ok(Json(QueryStartResponse::Async {
                    query_id: existing_query_id,
                }));
            }
        }
        // Stale entry in dedup index, remove it
        state.query_dedup_index.remove(&dedup_key);
    }

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
        subscriber_count: std::sync::atomic::AtomicUsize::new(1),
        dedup_key: dedup_key.clone(),
    });

    state
        .running_queries
        .insert(query_id.clone(), running_query.clone());
    state.query_dedup_index.insert(dedup_key, query_id.clone());

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
    let history = state.history();
    if let Some(ref h) = history {
        if let Err(e) = h.add(NewQueryHistoryEntry {
            id: &query_id,
            name: &body.query, // Use query text as name
            query: &body.query,
            timestamp: started_at_unix,
            result_count: None,
            status: "running",
            started_at: started_at_unix,
            duration_ms: None,
            error: None,
            background: body.background,
        }) {
            warn!(error = %e, "Failed to add query to history");
        }
    }

    // Spawn the query execution
    let query_id_clone = query_id.clone();
    let query_text = body.query.clone();
    let extract_graph = body.extract_graph;
    let language = body.language.clone();
    let sync_mode = body.sync;
    let running_query_for_task = running_query.clone();
    let state_for_task = state.clone();

    let task_handle = tokio::task::spawn_blocking(move || {
        // Helper to update query status in history
        let update_history = |status: &str,
                              duration_ms: Option<u64>,
                              result_count: Option<i64>,
                              error: Option<&str>| {
            if let Some(ref h) = history {
                if let Err(e) =
                    h.update_status(&query_id_clone, status, duration_ms, result_count, error)
                {
                    warn!(error = %e, "Failed to update query history status");
                }
            }
        };

        // Helper to mark query as done and remove from dedup index
        let mark_completed = |rq: &RunningQuery, state: &AppState| {
            *rq.completed_at.write() = Some(std::time::Instant::now());
            state.query_dedup_index.remove(&rq.dedup_key);
            state.broadcast_query_activity();
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
            mark_completed(&running_query_for_task, &state_for_task);
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
            mark_completed(&running_query_for_task, &state_for_task);
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
        mark_completed(&running_query_for_task, &state_for_task);
    });

    if sync_mode {
        // Sync mode: wait for the query to complete, then return inline results
        if let Err(e) = task_handle.await {
            return Err(ApiError::Internal(format!("Query task panicked: {e}")));
        }
        let progress = running_query
            .final_state
            .read()
            .clone()
            .ok_or_else(|| ApiError::Internal("Query completed without result".to_string()))?;
        match progress.status {
            QueryStatus::Completed => {
                debug!(query_id = %query_id, "Sync query completed");
                Ok(Json(QueryStartResponse::Sync {
                    query_id,
                    duration_ms: progress.duration_ms.unwrap_or(0),
                    result_count: progress.result_count,
                    results: progress.results,
                    graph: progress.graph,
                }))
            }
            QueryStatus::Failed => Err(ApiError::BadRequest(
                progress.error.unwrap_or_else(|| "Query failed".to_string()),
            )),
            _ => Err(ApiError::Internal("Query was aborted".to_string())),
        }
    } else {
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
/// With deduplication, multiple subscribers may share a query. Abort only
/// actually cancels the query when the last subscriber disconnects.
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

    let prev_subscribers = query
        .subscriber_count
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

    if prev_subscribers > 1 {
        // Other subscribers still need this query, don't actually cancel
        debug!(
            query_id = %query_id,
            remaining = prev_subscribers - 1,
            "Subscriber detached from shared query, keeping alive"
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    // Last subscriber - actually cancel the query
    info!(query_id = %query_id, "Aborting query (last subscriber)");

    // Cancel the token - this will signal the query to abort
    query.cancel_token.cancel();

    let duration_ms = query.started_at.elapsed().as_millis() as u64;

    // Update query history with aborted status
    if let Some(history) = state.history() {
        if let Err(e) = history.update_status(&query_id, "aborted", Some(duration_ms), None, None) {
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

    // Remove from dedup index so future identical queries start fresh
    state.query_dedup_index.remove(&query.dedup_key);

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
