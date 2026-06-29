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

        // Accumulate into edges_total (flush_edge_buffer may be called more than
        // once, e.g. main buffer then deferred DCSync edges).
        progress.edges_total += total_edges;
        self.send_progress(progress);

        // Process in batches; throttle progress notifications to avoid
        // saturating subscribers with hundreds of rapid-fire messages.
        let batch_count = total_edges.div_ceil(BATCH_SIZE);
        let notify_every = batch_count.div_ceil(20).max(1);

        for (i, chunk) in self.edge_buffer.chunks(BATCH_SIZE).enumerate() {
            let batch_size = chunk.len();
            let count = self.db.insert_edges(chunk).map_err(|e| {
                error!(error = %e, batch_size, "Failed to insert relationships");
                format!("Failed to insert relationships: {e}")
            })?;

            progress.edges_imported += count;
            if (i + 1) % notify_every == 0 {
                self.send_progress(progress);
            }
        }

        debug!(
            total = progress.edges_imported,
            "All relationships inserted"
        );
        self.edge_buffer.clear();
        self.send_progress(progress);
        Ok(())
    }

    /// Flush deferred DCSync edges into the database.
    ///
    /// Post-processing after all files are imported: derive deferred edges,
    /// resolve orphan names, and mark completion.
    pub(super) fn finalize(&mut self, progress: &mut ImportProgress) -> Result<(), String> {
        progress.current_file = None;

        // Flush all edges accumulated across every file in one pass.  All
        // node types have been imported by now, so most target nodes already
        // exist and placeholder creation is minimised.
        progress.set_stage("Writing relationships");
        self.send_progress(progress);
        self.flush_edge_buffer(progress)?;

        progress.set_stage("Applying post-processing rules");
        self.send_progress(progress);

        self.flush_deferred_dcsync(progress)?;
        self.assign_member_tiers();

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

    /// Assign automatic tiers based on well-known group membership.
    ///
    /// Applied in priority order (highest first), each query only sets tier
    /// where it is not already assigned (`tier IS NULL`), so a higher-priority
    /// tier that is set first cannot be overwritten by a lower-priority one.
    ///
    /// Priority order:
    ///   tier 0  — well-known privileged group objects and their members
    ///   tier 2  — Domain Computers group object and its members
    ///   tier 3  — Domain Users members
    ///
    /// The group-object queries also cover placeholder nodes created by edge
    /// insertion before their JSON file is imported.
    fn assign_member_tiers(&self) {
        // Build WHERE clauses for all well-known tier-0 RIDs.
        // See: https://learn.microsoft.com/en-us/windows-server/identity/ad-ds/manage/understand-security-identifiers
        let rid_clauses = super::tier_zero_rids::ALL
            .iter()
            .map(|rid| format!("g.objectid ENDS WITH '{rid}'"))
            .collect::<Vec<_>>()
            .join(" OR ");

        let tier_zero_groups_query = format!(
            "MATCH (g) WHERE ({rid_clauses} OR g.objectid = 'S-1-5-9') \
             AND g.tier IS NULL SET g.tier = 0"
        );
        let tier_zero_members_query = format!(
            "MATCH (n)-[:MemberOf]->(g) WHERE ({rid_clauses} OR g.objectid = 'S-1-5-9') \
             AND n.tier IS NULL SET n.tier = 0"
        );

        let tier_two_groups_query = "\
            MATCH (g) WHERE g.objectid ENDS WITH '-515' \
              AND g.tier IS NULL \
            SET g.tier = 2";
        let tier_two_members_query = "\
            MATCH (n)-[:MemberOf]->(g) \
            WHERE g.objectid ENDS WITH '-515' \
              AND n.tier IS NULL \
            SET n.tier = 2";

        let tier_three_members_query = "\
            MATCH (n)-[:MemberOf]->(g) \
            WHERE g.objectid ENDS WITH '-513' \
              AND n.tier IS NULL \
            SET n.tier = 3";

        for (label, query) in [
            ("tier-0 group objects", tier_zero_groups_query.as_str()),
            ("tier-0 group members", tier_zero_members_query.as_str()),
            ("Domain Computers group object", tier_two_groups_query),
            ("Domain Computers members", tier_two_members_query),
            ("Domain Users members", tier_three_members_query),
        ] {
            match self.db.run_custom_query(query) {
                Ok(_) => info!("Assigned tiers to {}", label),
                Err(e) => warn!(error = %e, "Failed to assign tiers to {}", label),
            }
        }
    }

    /// Resolve placeholder node names using domain SID-to-name mappings.
    ///
    /// After import, placeholder nodes have `name = objectid` (a raw SID like
    /// `S-1-5-21-xxx-512`). This builds a domain SID -> name map from Domain
    /// nodes, then updates matching placeholders to `{DOMAIN}-{RID}` format
    /// (e.g. `CONTOSO.LOCAL-512`).
    pub(super) fn resolve_orphan_names(&self) -> Result<usize, String> {
        // Step 1: Fetch only Domain nodes with resolved names (server-side filter).
        // This avoids transferring all graph nodes across the DB connection.
        let domain_result = self
            .db
            .run_custom_query(
                "MATCH (n:Domain) \
                 WHERE n.name IS NOT NULL AND n.name <> n.objectid \
                 RETURN n.objectid AS id, n.name AS name",
            )
            .map_err(|e| format!("Failed to query domain nodes: {e}"))?;

        let mut domain_map: HashMap<String, String> = HashMap::new();
        if let Some(rows) = domain_result.get("rows").and_then(|v| v.as_array()) {
            for row in rows {
                if let Some(cols) = row.as_array() {
                    if let (Some(id), Some(name)) = (
                        cols.first().and_then(|v| v.as_str()),
                        cols.get(1).and_then(|v| v.as_str()),
                    ) {
                        if !name.starts_with("S-1-") {
                            domain_map.insert(id.to_string(), name.to_string());
                        }
                    }
                }
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

        // Step 2: Fetch only orphan nodes (name == objectid) server-side, then
        // compute friendly names in memory against the domain map.
        // We match by checking if the objectid starts with a known domain SID
        // followed by a dash. This handles both simple RIDs (e.g. "-512") and
        // compound well-known SID suffixes (e.g. "-S-1-5-11").
        let orphan_result = self
            .db
            .run_custom_query(
                "MATCH (n) WHERE n.name = n.objectid OR n.name IS NULL RETURN n.objectid AS id",
            )
            .map_err(|e| format!("Failed to query orphan nodes: {e}"))?;

        let mut renames: Vec<(String, String)> = Vec::new();
        if let Some(rows) = orphan_result.get("rows").and_then(|v| v.as_array()) {
            for row in rows {
                let node_id = row
                    .as_array()
                    .and_then(|cols| cols.first())
                    .and_then(|v| v.as_str());
                if let Some(node_id) = node_id {
                    for (sid, name) in &domain_map {
                        let prefix = format!("{}-", sid);
                        if let Some(suffix) = node_id.strip_prefix(&prefix) {
                            renames.push((node_id.to_string(), format!("{}-{}", name, suffix)));
                            break;
                        }
                    }
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
