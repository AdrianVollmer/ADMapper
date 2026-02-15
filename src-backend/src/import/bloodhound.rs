//! BloodHound JSON/ZIP importer.

use crate::db::GraphDatabase;
use crate::import::types::ImportProgress;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::io::{Read, Seek};
use std::path::Path;
use tokio::sync::broadcast;
use tracing::{debug, info, warn, error, trace};
use zip::ZipArchive;

/// Batch size for database inserts.
const BATCH_SIZE: usize = 1000;

/// BloodHound file metadata.
#[derive(Debug, Deserialize)]
struct BloodHoundMeta {
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default)]
    #[allow(dead_code)]
    version: Option<i32>,
}

/// BloodHound file structure.
#[derive(Debug, Deserialize)]
struct BloodHoundFile {
    meta: Option<BloodHoundMeta>,
    data: Vec<JsonValue>,
}

/// BloodHound data importer.
pub struct BloodHoundImporter {
    db: GraphDatabase,
    progress_tx: broadcast::Sender<ImportProgress>,
    /// Track which object IDs we've seen to avoid duplicate nodes
    seen_nodes: HashSet<String>,
}

impl BloodHoundImporter {
    pub fn new(db: GraphDatabase, progress_tx: broadcast::Sender<ImportProgress>) -> Self {
        Self {
            db,
            progress_tx,
            seen_nodes: HashSet::new(),
        }
    }

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

        // Collect JSON file names
        let json_files: Vec<String> = (0..archive.len())
            .filter_map(|i| {
                let file = archive.by_index(i).ok()?;
                let name = file.name().to_string();
                if name.ends_with(".json") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        info!(file_count = json_files.len(), "Found JSON files in ZIP");
        debug!(files = ?json_files, "JSON files to process");

        let mut progress = ImportProgress::new(job_id.to_string()).with_total_files(json_files.len());
        self.send_progress(&progress);

        // Clear existing data for fresh import
        info!("Clearing existing database data");
        self.db.clear().map_err(|e| {
            error!(error = %e, "Failed to clear database");
            format!("Failed to clear database: {e}")
        })?;

        for file_name in &json_files {
            debug!(file = %file_name, "Processing file");
            progress.set_current_file(file_name.clone());
            self.send_progress(&progress);

            let mut file = archive
                .by_name(file_name)
                .map_err(|e| {
                    error!(file = %file_name, error = %e, "Failed to open file in archive");
                    format!("Failed to read {file_name}: {e}")
                })?;

            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .map_err(|e| {
                    error!(file = %file_name, error = %e, "Failed to read file contents");
                    format!("Failed to read {file_name}: {e}")
                })?;

            trace!(file = %file_name, size = contents.len(), "Read file contents");

            match self.import_json_str(&contents, &mut progress) {
                Ok(_) => {
                    info!(
                        file = %file_name,
                        nodes = progress.nodes_imported,
                        edges = progress.edges_imported,
                        "File processed"
                    );
                    progress.files_processed += 1;
                    self.send_progress(&progress);
                }
                Err(e) => {
                    warn!(file = %file_name, error = %e, "Error importing file, continuing");
                    progress.files_processed += 1;
                }
            }
        }

        progress.complete();
        self.send_progress(&progress);
        Ok(progress)
    }

    /// Import from a single JSON file.
    pub fn import_json_file<P: AsRef<Path>>(
        &mut self,
        path: P,
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read file: {e}"))?;

        let mut progress = ImportProgress::new(job_id.to_string()).with_total_files(1);
        progress.set_current_file(path.as_ref().display().to_string());
        self.send_progress(&progress);

        self.import_json_str(&contents, &mut progress)?;

        progress.files_processed = 1;
        progress.complete();
        self.send_progress(&progress);
        Ok(progress)
    }

    /// Import from JSON string.
    fn import_json_str(
        &mut self,
        contents: &str,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        let file: BloodHoundFile = serde_json::from_str(contents).map_err(|e| {
            error!(error = %e, "Failed to parse JSON");
            format!("Invalid JSON: {e}")
        })?;

        let data_type = file
            .meta
            .as_ref()
            .map(|m| m.data_type.clone())
            .unwrap_or_else(|| {
                // Try to infer type from data
                if let Some(first) = file.data.first() {
                    if first.get("Members").is_some() {
                        "groups".to_string()
                    } else if first.get("Sessions").is_some() || first.get("LocalGroups").is_some() {
                        "computers".to_string()
                    } else {
                        "users".to_string()
                    }
                } else {
                    "unknown".to_string()
                }
            });

        info!(
            entity_type = %data_type,
            count = file.data.len(),
            "Importing entities"
        );

        let mut node_batch = Vec::with_capacity(BATCH_SIZE);
        let mut edge_batch = Vec::with_capacity(BATCH_SIZE);

        for entity in &file.data {
            // Extract node
            if let Some(node) = self.extract_node(&data_type, entity) {
                if !self.seen_nodes.contains(&node.0) {
                    self.seen_nodes.insert(node.0.clone());
                    node_batch.push(node);

                    if node_batch.len() >= BATCH_SIZE {
                        self.flush_nodes(&mut node_batch, progress)?;
                    }
                }
            }

            // Extract edges
            let edges = self.extract_edges(&data_type, entity);
            for edge in edges {
                edge_batch.push(edge);

                if edge_batch.len() >= BATCH_SIZE {
                    self.flush_edges(&mut edge_batch, progress)?;
                }
            }
        }

        // Flush remaining
        self.flush_nodes(&mut node_batch, progress)?;
        self.flush_edges(&mut edge_batch, progress)?;

        Ok(())
    }

    /// Extract a node from a BloodHound entity.
    fn extract_node(
        &self,
        data_type: &str,
        entity: &JsonValue,
    ) -> Option<(String, String, String, JsonValue)> {
        let object_id = entity
            .get("ObjectIdentifier")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        let properties = entity.get("Properties").cloned().unwrap_or(JsonValue::Null);

        let name = properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&object_id)
            .to_string();

        let node_type = match data_type.to_lowercase().as_str() {
            "users" => "User",
            "groups" => "Group",
            "computers" => "Computer",
            "domains" => "Domain",
            "gpos" => "GPO",
            "ous" => "OU",
            "containers" => "Container",
            "certtemplates" => "CertTemplate",
            "enterprisecas" => "EnterpriseCA",
            "rootcas" => "RootCA",
            "aiacas" => "AIACA",
            "ntauthstores" => "NTAuthStore",
            _ => "Unknown",
        };

        Some((object_id, name, node_type.to_string(), properties))
    }

    /// Extract edges from a BloodHound entity.
    fn extract_edges(
        &self,
        data_type: &str,
        entity: &JsonValue,
    ) -> Vec<(String, String, String, JsonValue)> {
        let mut edges = Vec::new();

        let object_id = match entity.get("ObjectIdentifier").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return edges,
        };

        // Members -> MemberOf edges (group contains members)
        if let Some(members) = entity.get("Members").and_then(|v| v.as_array()) {
            for member in members {
                if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push((
                        member_id.to_string(),
                        object_id.clone(),
                        "MemberOf".to_string(),
                        JsonValue::Null,
                    ));
                }
            }
        }

        // Sessions -> HasSession edges (user has session on computer)
        for session_field in ["Sessions", "PrivilegedSessions", "RegistrySessions"] {
            if let Some(sessions) = entity
                .get(session_field)
                .and_then(|v| v.get("Results"))
                .and_then(|v| v.as_array())
            {
                for session in sessions {
                    if let Some(user_sid) = session.get("UserSID").and_then(|v| v.as_str()) {
                        edges.push((
                            user_sid.to_string(),
                            object_id.clone(),
                            "HasSession".to_string(),
                            JsonValue::Null,
                        ));
                    }
                }
            }
        }

        // LocalGroups -> AdminTo, CanRDP, etc.
        if let Some(local_groups) = entity.get("LocalGroups").and_then(|v| v.as_array()) {
            for group in local_groups {
                let group_name = group.get("Name").and_then(|v| v.as_str()).unwrap_or("");
                let edge_type = self.local_group_to_edge_type(group_name);

                if let Some(results) = group.get("Results").and_then(|v| v.as_array()) {
                    for member in results {
                        if let Some(member_id) =
                            member.get("ObjectIdentifier").and_then(|v| v.as_str())
                        {
                            edges.push((
                                member_id.to_string(),
                                object_id.clone(),
                                edge_type.to_string(),
                                JsonValue::Null,
                            ));
                        }
                    }
                }
            }
        }

        // ACEs -> Permission edges
        if let Some(aces) = entity.get("Aces").and_then(|v| v.as_array()) {
            for ace in aces {
                if let (Some(principal_sid), Some(right_name)) = (
                    ace.get("PrincipalSID").and_then(|v| v.as_str()),
                    ace.get("RightName").and_then(|v| v.as_str()),
                ) {
                    let edge_type = self.ace_to_edge_type(right_name);
                    let is_inherited = ace
                        .get("IsInherited")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    edges.push((
                        principal_sid.to_string(),
                        object_id.clone(),
                        edge_type.to_string(),
                        serde_json::json!({"inherited": is_inherited}),
                    ));
                }
            }
        }

        // ContainedBy -> Contains edge (reversed direction)
        if let Some(contained_by) = entity.get("ContainedBy") {
            if let Some(container_id) = contained_by.get("ObjectIdentifier").and_then(|v| v.as_str())
            {
                edges.push((
                    container_id.to_string(),
                    object_id.clone(),
                    "Contains".to_string(),
                    JsonValue::Null,
                ));
            }
        }

        // AllowedToDelegate
        if let Some(delegates) = entity.get("AllowedToDelegate").and_then(|v| v.as_array()) {
            for delegate in delegates {
                if let Some(target_id) = delegate.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push((
                        object_id.clone(),
                        target_id.to_string(),
                        "AllowedToDelegate".to_string(),
                        JsonValue::Null,
                    ));
                }
            }
        }

        // AllowedToAct
        if let Some(actors) = entity.get("AllowedToAct").and_then(|v| v.as_array()) {
            for actor in actors {
                if let Some(actor_id) = actor.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push((
                        actor_id.to_string(),
                        object_id.clone(),
                        "AllowedToAct".to_string(),
                        JsonValue::Null,
                    ));
                }
            }
        }

        // GPLink for GPOs
        if data_type == "gpos" {
            // GPLinks are typically in the OU/Domain files pointing to GPOs
        }

        // Links for OUs (GPO links)
        if let Some(links) = entity.get("Links").and_then(|v| v.as_array()) {
            for link in links {
                if let Some(gpo_id) = link.get("GUID").and_then(|v| v.as_str()) {
                    edges.push((
                        object_id.clone(),
                        gpo_id.to_string(),
                        "GPLink".to_string(),
                        JsonValue::Null,
                    ));
                }
            }
        }

        // Trusts for domains
        if let Some(trusts) = entity.get("Trusts").and_then(|v| v.as_array()) {
            for trust in trusts {
                if let Some(target_sid) = trust.get("TargetDomainSid").and_then(|v| v.as_str()) {
                    let trust_direction = trust
                        .get("TrustDirection")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    // Bidirectional or outbound trust
                    if trust_direction == 2 || trust_direction == 3 {
                        edges.push((
                            target_sid.to_string(),
                            object_id.clone(),
                            "TrustedBy".to_string(),
                            serde_json::json!({"direction": trust_direction}),
                        ));
                    }
                    // Bidirectional or inbound trust
                    if trust_direction == 1 || trust_direction == 3 {
                        edges.push((
                            object_id.clone(),
                            target_sid.to_string(),
                            "TrustedBy".to_string(),
                            serde_json::json!({"direction": trust_direction}),
                        ));
                    }
                }
            }
        }

        edges
    }

    /// Map local group name to edge type.
    fn local_group_to_edge_type(&self, group_name: &str) -> &'static str {
        match group_name.to_uppercase().as_str() {
            s if s.contains("ADMINISTRATORS") => "AdminTo",
            s if s.contains("REMOTE DESKTOP") => "CanRDP",
            s if s.contains("REMOTE MANAGEMENT") => "CanPSRemote",
            s if s.contains("DISTRIBUTED COM") => "ExecuteDCOM",
            _ => "LocalGroupMember",
        }
    }

    /// Map ACE right name to edge type.
    fn ace_to_edge_type(&self, right_name: &str) -> &'static str {
        match right_name {
            "GenericAll" => "GenericAll",
            "GenericWrite" => "GenericWrite",
            "WriteOwner" => "WriteOwner",
            "WriteDacl" => "WriteDacl",
            "Owns" => "Owns",
            "AddMember" => "AddMember",
            "ForceChangePassword" => "ForceChangePassword",
            "AllExtendedRights" => "AllExtendedRights",
            "AddKeyCredentialLink" => "AddKeyCredentialLink",
            "AddAllowedToAct" => "AddAllowedToAct",
            "ReadLAPSPassword" => "ReadLAPSPassword",
            "ReadGMSAPassword" => "ReadGMSAPassword",
            "GetChanges" => "GetChanges",
            "GetChangesAll" => "GetChangesAll",
            "GetChangesInFilteredSet" => "GetChangesInFilteredSet",
            "WriteSPN" => "WriteSPN",
            "WriteAccountRestrictions" => "WriteAccountRestrictions",
            _ => "ACE",
        }
    }

    fn flush_nodes(
        &self,
        batch: &mut Vec<(String, String, String, JsonValue)>,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        trace!(batch_size = batch_size, "Flushing node batch");

        let count = self
            .db
            .insert_nodes(batch)
            .map_err(|e| {
                error!(error = %e, batch_size = batch_size, "Failed to insert nodes");
                format!("Failed to insert nodes: {e}")
            })?;

        progress.nodes_imported += count;
        debug!(inserted = count, total = progress.nodes_imported, "Nodes inserted");
        self.send_progress(progress);
        batch.clear();
        Ok(())
    }

    fn flush_edges(
        &self,
        batch: &mut Vec<(String, String, String, JsonValue)>,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        trace!(batch_size = batch_size, "Flushing edge batch");

        let count = self
            .db
            .insert_edges(batch)
            .map_err(|e| {
                error!(error = %e, batch_size = batch_size, "Failed to insert edges");
                format!("Failed to insert edges: {e}")
            })?;

        progress.edges_imported += count;
        debug!(inserted = count, total = progress.edges_imported, "Edges inserted");
        self.send_progress(progress);
        batch.clear();
        Ok(())
    }

    fn send_progress(&self, progress: &ImportProgress) {
        let _ = self.progress_tx.send(progress.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ace_to_edge_type() {
        let db = GraphDatabase::in_memory().unwrap();
        let (tx, _) = broadcast::channel(1);
        let importer = BloodHoundImporter::new(db, tx);

        assert_eq!(importer.ace_to_edge_type("GenericAll"), "GenericAll");
        assert_eq!(importer.ace_to_edge_type("WriteDacl"), "WriteDacl");
        assert_eq!(importer.ace_to_edge_type("Unknown"), "ACE");
    }

    #[test]
    fn test_local_group_to_edge_type() {
        let db = GraphDatabase::in_memory().unwrap();
        let (tx, _) = broadcast::channel(1);
        let importer = BloodHoundImporter::new(db, tx);

        assert_eq!(
            importer.local_group_to_edge_type("Administrators"),
            "AdminTo"
        );
        assert_eq!(
            importer.local_group_to_edge_type("Remote Desktop Users"),
            "CanRDP"
        );
    }
}
