//! Import orchestration: ZIP, JSON file, multi-file, and JSON string import.

use super::{types::BloodHoundFile, BloodHoundImporter, BATCH_SIZE};
use crate::db::DbNode;
use crate::import::types::{FailedFile, ImportProgress};
use serde_json::Value as JsonValue;
use std::io::{Read, Seek};
use std::path::Path;
use tracing::{debug, error, info, warn};
use zip::ZipArchive;

/// A JSON payload to import: either raw contents read from a file/ZIP entry,
/// or a path to read lazily from disk.
enum JsonSource {
    Contents {
        filename: String,
        data: String,
        size: u64,
    },
    File {
        filename: String,
        path: std::path::PathBuf,
        size: u64,
    },
}

impl BloodHoundImporter {
    /// Import from multiple file paths (ZIP and/or JSON).
    ///
    /// ZIPs are extracted in-memory and their JSON entries are appended to the
    /// work list. Everything is then processed through a single unified progress
    /// tracker and finalized once at the end.
    pub fn import_paths<P: AsRef<Path>>(
        &mut self,
        paths: &[(String, P)],
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        info!(file_count = paths.len(), "Importing files");

        let mut sources: Vec<JsonSource> = Vec::new();

        // We collect ZIP errors here and inject them into progress later,
        // since we don't have a progress object yet at this point.
        let mut zip_errors: Vec<FailedFile> = Vec::new();

        for (filename, path) in paths {
            if filename.ends_with(".zip") {
                let zip_result = std::fs::File::open(path)
                    .map_err(|e| format!("Failed to open file: {e}"))
                    .and_then(Self::extract_zip_sources);
                match zip_result {
                    Ok(zip_sources) => sources.extend(zip_sources),
                    Err(e) => {
                        error!(filename = %filename, error = %e, "Failed to read ZIP");
                        zip_errors.push(FailedFile {
                            filename: filename.clone(),
                            error: e,
                        });
                    }
                }
            } else if filename.ends_with(".json") {
                let file_size = std::fs::metadata(path).map_or(0, |m| m.len());
                sources.push(JsonSource::File {
                    filename: filename.clone(),
                    path: path.as_ref().to_path_buf(),
                    size: file_size,
                });
            } else {
                warn!(filename = %filename, "Unsupported file type, skipping");
            }
        }

        let mut result = self.import_sources(sources, job_id)?;
        result.failed_files.extend(zip_errors);
        Ok(result)
    }

    /// Extract JSON entries from a ZIP archive into in-memory sources.
    fn extract_zip_sources<R: Read + Seek>(reader: R) -> Result<Vec<JsonSource>, String> {
        let mut archive = ZipArchive::new(reader).map_err(|e| {
            error!(error = %e, "Failed to open ZIP");
            format!("Failed to open ZIP: {e}")
        })?;

        // Collect file info first (borrow checker: can't hold index info while mutably borrowing archive)
        let json_entries: Vec<(String, u64)> = (0..archive.len())
            .filter_map(|i| {
                let file = archive.by_index(i).ok()?;
                let name = file.name().to_string();
                let size = file.size();
                if name.ends_with(".json") {
                    Some((name, size))
                } else {
                    None
                }
            })
            .collect();

        info!(file_count = json_entries.len(), "Found JSON files in ZIP");
        debug!(files = ?json_entries, "JSON files to extract");

        let mut sources = Vec::with_capacity(json_entries.len());
        for (name, size) in &json_entries {
            let mut file = archive
                .by_name(name)
                .map_err(|e| format!("Failed to read {name}: {e}"))?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .map_err(|e| format!("Failed to read {name}: {e}"))?;
            sources.push(JsonSource::Contents {
                filename: name.clone(),
                data: contents,
                size: *size,
            });
        }

        Ok(sources)
    }

    /// Core import logic: process a list of JSON sources with unified progress,
    /// then finalize.
    fn import_sources(
        &mut self,
        sources: Vec<JsonSource>,
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        // Ensure indexes exist before inserting data -- critical for MERGE
        // performance on Cypher backends. Idempotent, so safe on every import.
        self.db.ensure_indexes().map_err(|e| {
            error!(error = %e, "Failed to ensure database indexes");
            format!("Failed to ensure database indexes: {e}")
        })?;

        let total_bytes: u64 = sources
            .iter()
            .map(|s| match s {
                JsonSource::Contents { size, .. } | JsonSource::File { size, .. } => *size,
            })
            .sum();

        let mut progress = ImportProgress::new(job_id.to_string())
            .with_total_files(sources.len())
            .with_bytes_total(total_bytes);
        self.send_progress(&progress);

        for source in &sources {
            let (filename, contents, file_size) = match source {
                JsonSource::Contents {
                    filename,
                    data,
                    size,
                } => (
                    filename.clone(),
                    std::borrow::Cow::Borrowed(data.as_str()),
                    *size,
                ),
                JsonSource::File {
                    filename,
                    path,
                    size,
                } => match std::fs::read_to_string(path) {
                    Ok(data) => (filename.clone(), std::borrow::Cow::Owned(data), *size),
                    Err(e) => {
                        error!(file = %filename, error = %e, "Failed to read file");
                        progress.failed_files.push(FailedFile {
                            filename: filename.clone(),
                            error: format!("Failed to read {filename}: {e}"),
                        });
                        progress.files_processed += 1;
                        progress.bytes_processed += size;
                        continue;
                    }
                },
            };

            debug!(file = %filename, "Processing file");
            progress.set_current_file(filename.clone());
            self.send_progress(&progress);

            match self.import_json_str(&contents, &mut progress) {
                Ok(_) => {
                    info!(
                        file = %filename,
                        nodes = progress.nodes_imported,
                        relationships = progress.edges_imported,
                        "File processed"
                    );
                    progress.files_processed += 1;
                    progress.bytes_processed += file_size;
                    self.send_progress(&progress);
                }
                Err(e) => {
                    warn!(file = %filename, error = %e, "Error importing file, continuing");
                    progress
                        .failed_files
                        .push(FailedFile { filename, error: e });
                    progress.files_processed += 1;
                    progress.bytes_processed += file_size;
                }
            }
        }

        self.finalize(&mut progress)?;
        Ok(progress)
    }

    /// Import from JSON string.
    /// Flushes both nodes and edges per-file for live progress updates.
    pub(super) fn import_json_str(
        &mut self,
        contents: &str,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        // Strip UTF-8 BOM (U+FEFF) if present -- some tools write it but JSON doesn't allow it
        let contents = contents.strip_prefix('\u{FEFF}').unwrap_or(contents);

        // Parse with RawValue to defer entity parsing - reduces peak memory
        let file: BloodHoundFile = serde_json::from_str(contents).map_err(|e| {
            error!(error = %e, "Failed to parse JSON");
            format!("Invalid JSON: {e}")
        })?;

        // Infer data type from metadata or first entity
        let (data_type, version) = if let Some(meta) = &file.meta {
            (meta.data_type.clone(), meta.version)
        } else {
            // Try to infer type from first entity (parse just the first one)
            let inferred = if let Some(first_raw) = file.data.first() {
                if let Ok(first) = serde_json::from_str::<JsonValue>(first_raw.get()) {
                    if first.get("Members").is_some() {
                        "groups".to_string()
                    } else if first.get("Sessions").is_some() || first.get("LocalGroups").is_some()
                    {
                        "computers".to_string()
                    } else {
                        "users".to_string()
                    }
                } else {
                    "users".to_string()
                }
            } else {
                "users".to_string()
            };
            (inferred, None)
        };

        info!(
            entity_type = %data_type,
            version = ?version,
            count = file.data.len(),
            "Importing entities"
        );

        let mut node_batch: Vec<DbNode> = Vec::with_capacity(BATCH_SIZE);

        progress.set_stage("Extracting nodes");
        self.send_progress(progress);

        // Process each entity - parse from RawValue on demand
        for raw_entity in &file.data {
            // Parse this entity now (lazy parsing)
            let entity: JsonValue = match serde_json::from_str(raw_entity.get()) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "Failed to parse entity, skipping");
                    continue;
                }
            };

            // Extract node
            if let Some(node) = self.extract_node(&data_type, &entity) {
                if !self.seen_nodes.contains(&node.id) {
                    self.seen_nodes.insert(node.id.clone());
                    node_batch.push(node);

                    if node_batch.len() >= BATCH_SIZE {
                        self.flush_nodes(&mut node_batch, progress)?;
                    }
                }
            }

            // Extract relationships - deduplicated and buffered, flushed at end of file
            let relationships = self.extract_edges(&data_type, &entity);
            for edge in relationships {
                let key = (
                    edge.source.clone(),
                    edge.target.clone(),
                    edge.rel_type.clone(),
                );
                if self.seen_edges.insert(key) {
                    self.edge_buffer.push(edge);
                }
            }
        }

        // Flush remaining nodes
        self.flush_nodes(&mut node_batch, progress)?;

        // Edges remain in edge_buffer; all edges are flushed together after
        // all files are processed so that target nodes already exist,
        // minimising placeholder creation and MERGE overhead.

        Ok(())
    }
}
