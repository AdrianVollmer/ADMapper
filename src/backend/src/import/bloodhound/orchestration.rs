//! Import orchestration: ZIP, JSON file, multi-file, and JSON string import.

use super::{types::BloodHoundFile, BloodHoundImporter, BATCH_SIZE};
use crate::db::DbNode;
use crate::import::types::ImportProgress;
use serde_json::Value as JsonValue;
use std::io::{Read, Seek};
use std::path::Path;
use tracing::{debug, error, info, trace, warn};
use zip::ZipArchive;

impl BloodHoundImporter {
    /// Import from a ZIP file.
    pub fn import_zip<R: Read + Seek>(
        &mut self,
        reader: R,
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        info!(job_id = %job_id, "Opening ZIP archive");
        let mut archive = ZipArchive::new(reader).map_err(|e| {
            error!(error = %e, "Failed to open ZIP");
            format!("Failed to open ZIP: {e}")
        })?;

        // Collect JSON file names and their uncompressed sizes
        let json_files: Vec<(String, u64)> = (0..archive.len())
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

        let bytes_total: u64 = json_files.iter().map(|(_, size)| size).sum();

        info!(
            file_count = json_files.len(),
            bytes_total, "Found JSON files in ZIP"
        );
        debug!(files = ?json_files, "JSON files to process");

        let mut progress = ImportProgress::new(job_id.to_string())
            .with_total_files(json_files.len())
            .with_bytes_total(bytes_total);
        self.send_progress(&progress);

        // Clear existing data for fresh import
        info!("Clearing existing database data");
        self.db.clear().map_err(|e| {
            error!(error = %e, "Failed to clear database");
            format!("Failed to clear database: {e}")
        })?;

        for (file_name, file_size) in &json_files {
            debug!(file = %file_name, "Processing file");
            progress.set_current_file(file_name.clone());
            self.send_progress(&progress);

            let mut file = archive.by_name(file_name).map_err(|e| {
                error!(file = %file_name, error = %e, "Failed to open file in archive");
                format!("Failed to read {file_name}: {e}")
            })?;

            let mut contents = String::new();
            file.read_to_string(&mut contents).map_err(|e| {
                error!(file = %file_name, error = %e, "Failed to read file contents");
                format!("Failed to read {file_name}: {e}")
            })?;

            trace!(file = %file_name, size = contents.len(), "Read file contents");

            match self.import_json_str(&contents, &mut progress) {
                Ok(_) => {
                    info!(
                        file = %file_name,
                        nodes = progress.nodes_imported,
                        relationships = progress.edges_imported,
                        "File processed"
                    );
                    progress.files_processed += 1;
                    progress.bytes_processed += file_size;
                    self.send_progress(&progress);
                }
                Err(e) => {
                    warn!(file = %file_name, error = %e, "Error importing file, continuing");
                    progress.files_processed += 1;
                    progress.bytes_processed += file_size;
                }
            }
        }

        self.finalize(&mut progress)?;
        Ok(progress)
    }

    /// Import from a single JSON file.
    pub fn import_json_file<P: AsRef<Path>>(
        &mut self,
        path: P,
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        let file_size = std::fs::metadata(&path).map_or(0, |m| m.len());
        let contents =
            std::fs::read_to_string(&path).map_err(|e| format!("Failed to read file: {e}"))?;

        let mut progress = ImportProgress::new(job_id.to_string())
            .with_total_files(1)
            .with_bytes_total(file_size);
        progress.set_current_file(path.as_ref().display().to_string());
        self.send_progress(&progress);

        self.import_json_str(&contents, &mut progress)?;

        progress.files_processed = 1;
        progress.bytes_processed = file_size;

        self.finalize(&mut progress)?;
        Ok(progress)
    }

    /// Import from multiple JSON files with unified progress tracking.
    pub fn import_json_files<P: AsRef<Path>>(
        &mut self,
        paths: &[(String, P)],
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        info!(file_count = paths.len(), "Importing multiple JSON files");

        // Calculate total bytes across all files for weighted progress
        let bytes_total: u64 = paths
            .iter()
            .filter_map(|(_, path)| std::fs::metadata(path).ok())
            .map(|m| m.len())
            .sum();

        let mut progress = ImportProgress::new(job_id.to_string())
            .with_total_files(paths.len())
            .with_bytes_total(bytes_total);
        self.send_progress(&progress);

        // Clear existing data for fresh import
        info!("Clearing existing database data");
        self.db.clear().map_err(|e| {
            error!(error = %e, "Failed to clear database");
            format!("Failed to clear database: {e}")
        })?;

        for (filename, path) in paths {
            debug!(file = %filename, "Processing file");
            progress.set_current_file(filename.clone());
            self.send_progress(&progress);

            let metadata = std::fs::metadata(path).ok();
            let file_size = metadata.map_or(0, |m| m.len());

            let contents = std::fs::read_to_string(path).map_err(|e| {
                error!(file = %filename, error = %e, "Failed to read file");
                format!("Failed to read {filename}: {e}")
            })?;

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

        // Flush edges for this file - placeholder nodes handle missing targets
        if !self.edge_buffer.is_empty() {
            progress.set_stage("Writing relationships");
            self.send_progress(progress);
        }
        self.flush_edge_buffer(progress)?;

        Ok(())
    }
}
