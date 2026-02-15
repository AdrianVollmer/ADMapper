//! BloodHound JSON/ZIP importer.

use crate::db::{DbEdge, DbNode, GraphDatabase};
use crate::import::types::ImportProgress;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::io::{Read, Seek};
use std::path::Path;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};
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

        let mut progress =
            ImportProgress::new(job_id.to_string()).with_total_files(json_files.len());
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
        let contents =
            std::fs::read_to_string(&path).map_err(|e| format!("Failed to read file: {e}"))?;

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
                    } else if first.get("Sessions").is_some() || first.get("LocalGroups").is_some()
                    {
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

        let mut node_batch: Vec<DbNode> = Vec::with_capacity(BATCH_SIZE);
        let mut edge_batch: Vec<DbEdge> = Vec::with_capacity(BATCH_SIZE);

        for entity in &file.data {
            // Extract node
            if let Some(node) = self.extract_node(&data_type, entity) {
                if !self.seen_nodes.contains(&node.id) {
                    self.seen_nodes.insert(node.id.clone());
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
    fn extract_node(&self, data_type: &str, entity: &JsonValue) -> Option<DbNode> {
        let id = entity
            .get("ObjectIdentifier")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        let properties = entity.get("Properties").cloned().unwrap_or(JsonValue::Null);

        let label = properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&id)
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

        Some(DbNode {
            id,
            label,
            node_type: node_type.to_string(),
            properties,
        })
    }

    /// Extract edges from a BloodHound entity.
    fn extract_edges(&self, _data_type: &str, entity: &JsonValue) -> Vec<DbEdge> {
        let object_id = match entity.get("ObjectIdentifier").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return Vec::new(),
        };

        let mut edges = Vec::new();
        self.extract_member_edges(entity, &object_id, &mut edges);
        self.extract_session_edges(entity, &object_id, &mut edges);
        self.extract_local_group_edges(entity, &object_id, &mut edges);
        self.extract_ace_edges(entity, &object_id, &mut edges);
        self.extract_containment_edges(entity, &object_id, &mut edges);
        self.extract_delegation_edges(entity, &object_id, &mut edges);
        self.extract_gpo_link_edges(entity, &object_id, &mut edges);
        self.extract_trust_edges(entity, &object_id, &mut edges);
        edges
    }

    /// Extract MemberOf edges from group membership.
    fn extract_member_edges(&self, entity: &JsonValue, object_id: &str, edges: &mut Vec<DbEdge>) {
        let Some(members) = entity.get("Members").and_then(|v| v.as_array()) else {
            return;
        };
        for member in members {
            if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                edges.push(DbEdge {
                    source: member_id.to_string(),
                    target: object_id.to_string(),
                    edge_type: "MemberOf".to_string(),
                    properties: JsonValue::Null,
                });
            }
        }
    }

    /// Extract HasSession edges from computer sessions.
    fn extract_session_edges(&self, entity: &JsonValue, object_id: &str, edges: &mut Vec<DbEdge>) {
        for session_field in ["Sessions", "PrivilegedSessions", "RegistrySessions"] {
            let Some(sessions) = entity
                .get(session_field)
                .and_then(|v| v.get("Results"))
                .and_then(|v| v.as_array())
            else {
                continue;
            };
            for session in sessions {
                if let Some(user_sid) = session.get("UserSID").and_then(|v| v.as_str()) {
                    edges.push(DbEdge {
                        source: user_sid.to_string(),
                        target: object_id.to_string(),
                        edge_type: "HasSession".to_string(),
                        properties: JsonValue::Null,
                    });
                }
            }
        }
    }

    /// Extract local group membership edges (AdminTo, CanRDP, etc.).
    fn extract_local_group_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        edges: &mut Vec<DbEdge>,
    ) {
        let Some(local_groups) = entity.get("LocalGroups").and_then(|v| v.as_array()) else {
            return;
        };
        for group in local_groups {
            let group_name = group.get("Name").and_then(|v| v.as_str()).unwrap_or("");
            let edge_type = self.local_group_to_edge_type(group_name);

            let Some(results) = group.get("Results").and_then(|v| v.as_array()) else {
                continue;
            };
            for member in results {
                if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push(DbEdge {
                        source: member_id.to_string(),
                        target: object_id.to_string(),
                        edge_type: edge_type.to_string(),
                        properties: JsonValue::Null,
                    });
                }
            }
        }
    }

    /// Extract ACE permission edges.
    fn extract_ace_edges(&self, entity: &JsonValue, object_id: &str, edges: &mut Vec<DbEdge>) {
        let Some(aces) = entity.get("Aces").and_then(|v| v.as_array()) else {
            return;
        };
        for ace in aces {
            let (Some(principal_sid), Some(right_name)) = (
                ace.get("PrincipalSID").and_then(|v| v.as_str()),
                ace.get("RightName").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let edge_type = self.ace_to_edge_type(right_name);
            let is_inherited = ace
                .get("IsInherited")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            edges.push(DbEdge {
                source: principal_sid.to_string(),
                target: object_id.to_string(),
                edge_type: edge_type.to_string(),
                properties: serde_json::json!({"inherited": is_inherited}),
            });
        }
    }

    /// Extract containment and delegation edges.
    fn extract_containment_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        edges: &mut Vec<DbEdge>,
    ) {
        // ContainedBy -> Contains edge (reversed direction)
        if let Some(container_id) = entity
            .get("ContainedBy")
            .and_then(|v| v.get("ObjectIdentifier"))
            .and_then(|v| v.as_str())
        {
            edges.push(DbEdge {
                source: container_id.to_string(),
                target: object_id.to_string(),
                edge_type: "Contains".to_string(),
                properties: JsonValue::Null,
            });
        }
    }

    /// Extract delegation edges (AllowedToDelegate, AllowedToAct).
    fn extract_delegation_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        edges: &mut Vec<DbEdge>,
    ) {
        // AllowedToDelegate
        if let Some(delegates) = entity.get("AllowedToDelegate").and_then(|v| v.as_array()) {
            for delegate in delegates {
                if let Some(target_id) = delegate.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push(DbEdge {
                        source: object_id.to_string(),
                        target: target_id.to_string(),
                        edge_type: "AllowedToDelegate".to_string(),
                        properties: JsonValue::Null,
                    });
                }
            }
        }

        // AllowedToAct
        if let Some(actors) = entity.get("AllowedToAct").and_then(|v| v.as_array()) {
            for actor in actors {
                if let Some(actor_id) = actor.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    edges.push(DbEdge {
                        source: actor_id.to_string(),
                        target: object_id.to_string(),
                        edge_type: "AllowedToAct".to_string(),
                        properties: JsonValue::Null,
                    });
                }
            }
        }
    }

    /// Extract GPO link edges.
    fn extract_gpo_link_edges(&self, entity: &JsonValue, object_id: &str, edges: &mut Vec<DbEdge>) {
        let Some(links) = entity.get("Links").and_then(|v| v.as_array()) else {
            return;
        };
        for link in links {
            if let Some(gpo_id) = link.get("GUID").and_then(|v| v.as_str()) {
                edges.push(DbEdge {
                    source: object_id.to_string(),
                    target: gpo_id.to_string(),
                    edge_type: "GPLink".to_string(),
                    properties: JsonValue::Null,
                });
            }
        }
    }

    /// Extract domain trust edges.
    fn extract_trust_edges(&self, entity: &JsonValue, object_id: &str, edges: &mut Vec<DbEdge>) {
        let Some(trusts) = entity.get("Trusts").and_then(|v| v.as_array()) else {
            return;
        };
        for trust in trusts {
            let Some(target_sid) = trust.get("TargetDomainSid").and_then(|v| v.as_str()) else {
                continue;
            };
            let trust_direction = trust
                .get("TrustDirection")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // Bidirectional or outbound trust
            if trust_direction == 2 || trust_direction == 3 {
                edges.push(DbEdge {
                    source: target_sid.to_string(),
                    target: object_id.to_string(),
                    edge_type: "TrustedBy".to_string(),
                    properties: serde_json::json!({"direction": trust_direction}),
                });
            }
            // Bidirectional or inbound trust
            if trust_direction == 1 || trust_direction == 3 {
                edges.push(DbEdge {
                    source: object_id.to_string(),
                    target: target_sid.to_string(),
                    edge_type: "TrustedBy".to_string(),
                    properties: serde_json::json!({"direction": trust_direction}),
                });
            }
        }
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

    fn flush_edges(
        &self,
        batch: &mut Vec<DbEdge>,
        progress: &mut ImportProgress,
    ) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        trace!(batch_size = batch_size, "Flushing edge batch");

        let count = self.db.insert_edges(batch).map_err(|e| {
            error!(error = %e, batch_size = batch_size, "Failed to insert edges");
            format!("Failed to insert edges: {e}")
        })?;

        progress.edges_imported += count;
        debug!(
            inserted = count,
            total = progress.edges_imported,
            "Edges inserted"
        );
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

    /// Helper to create an importer for testing
    fn test_importer() -> BloodHoundImporter {
        let db = GraphDatabase::in_memory().unwrap();
        let (tx, _) = broadcast::channel(100);
        BloodHoundImporter::new(db, tx)
    }

    // ========================================================================
    // Node Extraction Tests
    // ========================================================================

    #[test]
    fn test_extract_node_user() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-USER",
            "Properties": {
                "name": "testuser@corp.local",
                "enabled": true,
                "pwdlastset": 12345678
            }
        });

        let node = importer.extract_node("users", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.id, "S-1-5-21-1234-USER");
        assert_eq!(node.label, "testuser@corp.local");
        assert_eq!(node.node_type, "User");
        assert_eq!(node.properties["enabled"], true);
    }

    #[test]
    fn test_extract_node_computer() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-COMP",
            "Properties": {
                "name": "DC01.corp.local",
                "operatingsystem": "Windows Server 2019"
            }
        });

        let node = importer.extract_node("computers", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.id, "S-1-5-21-1234-COMP");
        assert_eq!(node.label, "DC01.corp.local");
        assert_eq!(node.node_type, "Computer");
    }

    #[test]
    fn test_extract_node_group() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-GROUP",
            "Properties": {
                "name": "Domain Admins"
            }
        });

        let node = importer.extract_node("groups", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.node_type, "Group");
        assert_eq!(node.label, "Domain Admins");
    }

    #[test]
    fn test_extract_node_missing_id() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "Properties": {
                "name": "testuser@corp.local"
            }
        });

        let node = importer.extract_node("users", &entity);
        assert!(node.is_none());
    }

    #[test]
    fn test_extract_node_missing_name() {
        let importer = test_importer();

        // If name is missing, should use ObjectIdentifier as label
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-USER",
            "Properties": {}
        });

        let node = importer.extract_node("users", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.label, "S-1-5-21-1234-USER");
    }

    // ========================================================================
    // Edge Extraction Tests
    // ========================================================================

    #[test]
    fn test_extract_edges_memberof() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-GROUP1",
            "Members": [
                {"ObjectIdentifier": "S-1-5-21-USER1", "ObjectType": "User"},
                {"ObjectIdentifier": "S-1-5-21-USER2", "ObjectType": "User"}
            ]
        });

        let edges = importer.extract_edges("groups", &entity);

        assert_eq!(edges.len(), 2);
        // Members point TO the group (MemberOf)
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-GROUP1"
            && e.edge_type == "MemberOf"));
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-USER2"
            && e.target == "S-1-5-21-GROUP1"
            && e.edge_type == "MemberOf"));
    }

    #[test]
    fn test_extract_edges_sessions() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP1",
            "Sessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-USER1", "ComputerSID": "S-1-5-21-COMP1"}
                ]
            },
            "PrivilegedSessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-ADMIN1", "ComputerSID": "S-1-5-21-COMP1"}
                ]
            }
        });

        let edges = importer.extract_edges("computers", &entity);

        assert_eq!(edges.len(), 2);
        assert!(edges
            .iter()
            .all(|e| e.edge_type == "HasSession" && e.target == "S-1-5-21-COMP1"));
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-USER1"));
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-ADMIN1"));
    }

    #[test]
    fn test_extract_edges_aces() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-TARGET",
            "Aces": [
                {
                    "PrincipalSID": "S-1-5-21-ATTACKER",
                    "RightName": "GenericAll",
                    "IsInherited": false
                },
                {
                    "PrincipalSID": "S-1-5-21-USER1",
                    "RightName": "WriteDacl",
                    "IsInherited": true
                }
            ]
        });

        let edges = importer.extract_edges("users", &entity);

        assert_eq!(edges.len(), 2);

        let generic_all = edges
            .iter()
            .find(|e| e.source == "S-1-5-21-ATTACKER")
            .unwrap();
        assert_eq!(generic_all.edge_type, "GenericAll");
        assert_eq!(generic_all.properties["inherited"], false);

        let write_dacl = edges
            .iter()
            .find(|e| e.source == "S-1-5-21-USER1")
            .unwrap();
        assert_eq!(write_dacl.edge_type, "WriteDacl");
        assert_eq!(write_dacl.properties["inherited"], true);
    }

    #[test]
    fn test_extract_edges_trusts() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN2",
                    "TrustDirection": 3  // Bidirectional
                }
            ]
        });

        let edges = importer.extract_edges("domains", &entity);

        // Bidirectional trust creates 2 edges
        assert_eq!(edges.len(), 2);
        assert!(edges
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN2" && e.target == "S-1-5-21-DOMAIN1"));
        assert!(edges
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN1" && e.target == "S-1-5-21-DOMAIN2"));
    }

    #[test]
    fn test_extract_edges_containedby() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-USER1",
            "ContainedBy": {
                "ObjectIdentifier": "S-1-5-21-OU1",
                "ObjectType": "OU"
            }
        });

        let edges = importer.extract_edges("users", &entity);

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source, "S-1-5-21-OU1");
        assert_eq!(edges[0].target, "S-1-5-21-USER1");
        assert_eq!(edges[0].edge_type, "Contains");
    }

    #[test]
    fn test_extract_edges_delegation() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-USER1",
            "AllowedToDelegate": [
                {"ObjectIdentifier": "S-1-5-21-SERVICE1"}
            ],
            "AllowedToAct": [
                {"ObjectIdentifier": "S-1-5-21-ACTOR1"}
            ]
        });

        let edges = importer.extract_edges("users", &entity);

        assert_eq!(edges.len(), 2);
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-SERVICE1"
            && e.edge_type == "AllowedToDelegate"));
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-ACTOR1"
            && e.target == "S-1-5-21-USER1"
            && e.edge_type == "AllowedToAct"));
    }

    #[test]
    fn test_extract_edges_local_groups() {
        let importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP1",
            "LocalGroups": [
                {
                    "Name": "Administrators",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-ADMIN1"}
                    ]
                },
                {
                    "Name": "Remote Desktop Users",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-USER1"}
                    ]
                }
            ]
        });

        let edges = importer.extract_edges("computers", &entity);

        assert_eq!(edges.len(), 2);
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-ADMIN1"
            && e.target == "S-1-5-21-COMP1"
            && e.edge_type == "AdminTo"));
        assert!(edges.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-COMP1"
            && e.edge_type == "CanRDP"));
    }

    // ========================================================================
    // Import Tests
    // ========================================================================

    #[test]
    fn test_import_json_str_users() {
        let mut importer = test_importer();

        let json_content = serde_json::json!({
            "meta": {"type": "users", "version": 5},
            "data": [
                {
                    "ObjectIdentifier": "S-1-5-21-USER1",
                    "Properties": {"name": "user1@corp.local"}
                },
                {
                    "ObjectIdentifier": "S-1-5-21-USER2",
                    "Properties": {"name": "user2@corp.local"}
                }
            ]
        });

        let mut progress = ImportProgress::new("test".to_string());
        let result = importer.import_json_str(&json_content.to_string(), &mut progress);

        assert!(result.is_ok());
        assert_eq!(progress.nodes_imported, 2);

        // Verify nodes are in database
        let (node_count, _) = importer.db.get_stats().unwrap();
        assert_eq!(node_count, 2);
    }

    #[test]
    fn test_import_json_str_groups_with_members() {
        let mut importer = test_importer();

        let json_content = serde_json::json!({
            "meta": {"type": "groups", "version": 5},
            "data": [
                {
                    "ObjectIdentifier": "S-1-5-21-GROUP1",
                    "Properties": {"name": "Domain Admins"},
                    "Members": [
                        {"ObjectIdentifier": "S-1-5-21-USER1", "ObjectType": "User"},
                        {"ObjectIdentifier": "S-1-5-21-USER2", "ObjectType": "User"}
                    ]
                }
            ]
        });

        let mut progress = ImportProgress::new("test".to_string());
        let result = importer.import_json_str(&json_content.to_string(), &mut progress);

        assert!(result.is_ok());
        assert_eq!(progress.nodes_imported, 1);
        assert_eq!(progress.edges_imported, 2); // 2 MemberOf edges

        // Verify edges are in database
        let (_, edge_count) = importer.db.get_stats().unwrap();
        assert_eq!(edge_count, 2);
    }

    #[test]
    fn test_import_json_str_infers_type() {
        let mut importer = test_importer();

        // No meta.type - should infer from data structure
        let json_content = serde_json::json!({
            "data": [
                {
                    "ObjectIdentifier": "S-1-5-21-GROUP1",
                    "Properties": {"name": "Test Group"},
                    "Members": []
                }
            ]
        });

        let mut progress = ImportProgress::new("test".to_string());
        let result = importer.import_json_str(&json_content.to_string(), &mut progress);

        assert!(result.is_ok());
        // Should infer as "groups" due to Members field
        let nodes = importer.db.get_all_nodes().unwrap();
        assert_eq!(nodes[0].node_type, "Group");
    }

    #[test]
    fn test_import_json_str_invalid() {
        let mut importer = test_importer();

        let invalid_json = "not valid json {{{";
        let mut progress = ImportProgress::new("test".to_string());

        let result = importer.import_json_str(invalid_json, &mut progress);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_deduplicates_nodes() {
        let mut importer = test_importer();

        // Import same entity twice
        let json_content = serde_json::json!({
            "meta": {"type": "users"},
            "data": [
                {"ObjectIdentifier": "S-1-5-21-USER1", "Properties": {"name": "user1"}},
                {"ObjectIdentifier": "S-1-5-21-USER1", "Properties": {"name": "user1"}}
            ]
        });

        let mut progress = ImportProgress::new("test".to_string());
        importer
            .import_json_str(&json_content.to_string(), &mut progress)
            .unwrap();

        // Should only have 1 node due to deduplication
        let (node_count, _) = importer.db.get_stats().unwrap();
        assert_eq!(node_count, 1);
    }
}
