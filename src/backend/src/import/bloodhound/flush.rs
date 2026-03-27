//! Database flush operations: nodes, edges, trust domains, finalization,
//! and orphan name resolution.

use super::{BloodHoundImporter, BATCH_SIZE};
use crate::db::DbNode;
use crate::import::types::ImportProgress;
use std::collections::HashMap;
use tracing::{debug, error, info, trace, warn};

impl BloodHoundImporter {
    pub(super) fn flush_nodes(
        &self,
        batch: &mut Vec<DbNode>,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        trace!(batch_size = batch_size, "Flushing node batch");

        let count = self.db.insert_nodes(batch).map_err(|e| {
            error!(error = %e, batch_size = batch_size, "Failed to insert nodes");
            format!("Failed to insert nodes: {e}")
        })?;

        progress.nodes_imported += count;
        debug!(
            inserted = count,
            total = progress.nodes_imported,
            "Nodes inserted"
        );
        self.send_progress(progress);
        batch.clear();
        Ok(())
    }

    /// Flush buffered domain nodes from trust relationships.
    /// Called before flushing relationships to ensure target domains exist.
    pub(super) fn flush_trust_domains(
        &mut self,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        if self.trust_domain_buffer.is_empty() {
            return Ok(());
        }

        // Filter out domains we've already seen
        let new_domains: Vec<_> = self
            .trust_domain_buffer
            .drain(..)
            .filter(|n| !self.seen_nodes.contains(&n.id))
            .collect();

        if new_domains.is_empty() {
            return Ok(());
        }

        info!(
            count = new_domains.len(),
            "Inserting domain nodes from trust relationships"
        );

        for chunk in new_domains.chunks(BATCH_SIZE) {
            let count = self.db.insert_nodes(chunk).map_err(|e| {
                error!(error = %e, "Failed to insert trust domain nodes");
                format!("Failed to insert trust domain nodes: {e}")
            })?;
            progress.nodes_imported += count;
            for node in chunk {
                self.seen_nodes.insert(node.id.clone());
            }
        }

        self.send_progress(progress);
        Ok(())
    }

    /// Flush all buffered relationships in batches.
    /// Called per-file after nodes are flushed. Placeholder nodes handle missing targets.
    pub(super) fn flush_edge_buffer(
        &mut self,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        // First flush any domain nodes from trust relationships
        self.flush_trust_domains(progress)?;

        if self.edge_buffer.is_empty() {
            return Ok(());
        }

        let total_edges = self.edge_buffer.len();
        info!(total_edges, "Flushing relationship buffer");

        // Process in batches
        for chunk in self.edge_buffer.chunks(BATCH_SIZE) {
            let batch_size = chunk.len();
            let count = self.db.insert_edges(chunk).map_err(|e| {
                error!(error = %e, batch_size, "Failed to insert relationships");
                format!("Failed to insert relationships: {e}")
            })?;

            progress.edges_imported += count;
            self.send_progress(progress);
        }

        debug!(
            total = progress.edges_imported,
            "All relationships inserted"
        );
        self.edge_buffer.clear();
        Ok(())
    }

    /// Flush deferred DCSync edges into the database.
    ///
    /// Post-processing after all files are imported: derive deferred edges,
    /// resolve orphan names, and mark completion.
    pub(super) fn finalize(&mut self, progress: &mut ImportProgress) -> Result<(), String> {
        progress.current_file = None;
        progress.set_stage("Finalizing");
        self.send_progress(progress);

        self.flush_deferred_dcsync(progress)?;

        match self.resolve_orphan_names() {
            Ok(count) if count > 0 => {
                info!(updated = count, "Resolved orphan node names");
            }
            Err(e) => {
                warn!(error = %e, "Failed to resolve orphan node names");
            }
            _ => {}
        }

        progress.complete();
        self.send_progress(progress);
        Ok(())
    }

    /// Called once after all files are processed, before orphan name resolution.
    fn flush_deferred_dcsync(&mut self, progress: &mut ImportProgress) -> Result<(), String> {
        let deferred = self.derive_deferred_dcsync();
        if deferred.is_empty() {
            return Ok(());
        }
        info!(count = deferred.len(), "Flushing deferred DCSync edges");
        for edge in deferred {
            let key = (
                edge.source.clone(),
                edge.target.clone(),
                edge.rel_type.clone(),
            );
            if self.seen_edges.insert(key) {
                self.edge_buffer.push(edge);
            }
        }
        self.flush_edge_buffer(progress)
    }

    /// Resolve placeholder node names using domain SID-to-name mappings.
    ///
    /// After import, placeholder nodes have `name = objectid` (a raw SID like
    /// `S-1-5-21-xxx-512`). This builds a domain SID -> name map from Domain
    /// nodes, then updates matching placeholders to `{DOMAIN}-{RID}` format
    /// (e.g. `CONTOSO.LOCAL-512`).
    pub(super) fn resolve_orphan_names(&self) -> Result<usize, String> {
        // Step 1: Build domain SID -> name map from Domain nodes
        let all_nodes = self
            .db
            .get_all_nodes()
            .map_err(|e| format!("Failed to get nodes for orphan name resolution: {e}"))?;

        let mut domain_map: HashMap<String, String> = HashMap::new();
        for node in &all_nodes {
            if node.label == "Domain"
                && !node.name.is_empty()
                && node.name != node.id
                && !node.name.starts_with("S-1-")
            {
                domain_map.insert(node.id.clone(), node.name.clone());
            }
        }

        if domain_map.is_empty() {
            debug!("No domain name mappings found, skipping orphan name resolution");
            return Ok(0);
        }

        info!(
            domain_count = domain_map.len(),
            "Built domain SID-to-name map for orphan resolution"
        );

        // Step 2: Collect all (objectid, friendly_name) pairs for orphan nodes
        let mut renames: Vec<(String, String)> = Vec::new();
        for node in &all_nodes {
            if node.name != node.id {
                continue;
            }
            if let Some(last_dash) = node.id.rfind('-') {
                let domain_sid = &node.id[..last_dash];
                let rid = &node.id[last_dash..]; // e.g. "-512"
                if let Some(domain_name) = domain_map.get(domain_sid) {
                    renames.push((node.id.clone(), format!("{}{}", domain_name, rid)));
                }
            }
        }

        if renames.is_empty() {
            return Ok(0);
        }

        // Step 3: Batch-update using CASE expressions to avoid N individual
        // Cypher parse+plan cycles. Each chunk becomes a single query:
        //   MATCH (n) WHERE n.objectid IN [...]
        //   SET n.name = CASE n.objectid WHEN 'SID' THEN 'NAME' ... END
        let mut updated = 0;
        for chunk in renames.chunks(500) {
            let in_list: Vec<String> = chunk
                .iter()
                .map(|(id, _)| format!("'{}'", id.replace('\'', "\\'")))
                .collect();

            let case_arms: Vec<String> = chunk
                .iter()
                .map(|(id, name)| {
                    format!(
                        "WHEN '{}' THEN '{}'",
                        id.replace('\'', "\\'"),
                        name.replace('\'', "\\'")
                    )
                })
                .collect();

            let query = format!(
                "MATCH (n) WHERE n.objectid IN [{}] SET n.name = CASE n.objectid {} END",
                in_list.join(", "),
                case_arms.join(" ")
            );

            match self.db.run_custom_query(&query) {
                Ok(_) => {
                    updated += chunk.len();
                    trace!(batch_size = chunk.len(), "Resolved orphan name batch");
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        batch_size = chunk.len(),
                        "Failed to update orphan name batch"
                    );
                }
            }
        }

        if updated > 0 {
            info!(updated, "Resolved orphan node names with domain context");
        }

        Ok(updated)
    }
}
