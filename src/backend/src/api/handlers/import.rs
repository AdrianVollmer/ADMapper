//! BloodHound data import endpoints.

use crate::api::types::ApiError;
use crate::import::{BloodHoundImporter, ImportProgress};
use crate::state::{AppState, ImportJob};
use axum::{
    extract::{Multipart, Path, State},
    response::{
        sse::{Event, Sse},
        Json,
    },
};
use parking_lot::RwLock;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn};

/// Handle BloodHound data import via multipart upload.
#[instrument(skip(state, multipart))]
pub async fn import_bloodhound(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, ApiError> {
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
                        relationships = progress.edges_imported,
                        "ZIP imported successfully"
                    );
                    // Set final_state first, then send to channel to avoid race condition
                    // where SSE subscriber misses the final message
                    *job_for_task.final_state.write() = Some(progress.clone());
                    let _ = job_for_task.channel.send(progress.clone());
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
                            relationships = progress.edges_imported,
                            "JSON files imported successfully"
                        );
                        // Set final_state first, then send to channel to avoid race condition
                        *job_for_task.final_state.write() = Some(progress.clone());
                        let _ = job_for_task.channel.send(progress.clone());
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
