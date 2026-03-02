//! BloodHound JSON/ZIP importer.

use crate::db::{DatabaseBackend, DbEdge, DbNode};
use crate::import::types::ImportProgress;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::io::{Read, Seek};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};
use zip::ZipArchive;

/// Batch size for database inserts.
const BATCH_SIZE: usize = 1000;

/// Well-known high-value RIDs in Active Directory.
/// These are built-in privileged groups that attackers typically target.
/// See: https://learn.microsoft.com/en-us/windows-server/identity/ad-ds/manage/understand-security-identifiers
mod high_value_rids {
    // Domain-relative RIDs
    pub const DOMAIN_ADMINS: &str = "-512";
    pub const DOMAIN_CONTROLLERS: &str = "-516";
    pub const CERT_PUBLISHERS: &str = "-517";
    pub const SCHEMA_ADMINS: &str = "-518";
    pub const ENTERPRISE_ADMINS: &str = "-519";
    pub const GROUP_POLICY_CREATOR_OWNERS: &str = "-520";
    pub const READONLY_DOMAIN_CONTROLLERS: &str = "-521";
    pub const PROTECTED_USERS: &str = "-525";
    pub const KEY_ADMINS: &str = "-526";
    pub const ENTERPRISE_KEY_ADMINS: &str = "-527";

    // Well-known Builtin RIDs (S-1-5-32-xxx)
    pub const ADMINISTRATORS: &str = "-544";
    pub const ACCOUNT_OPERATORS: &str = "-548";
    pub const SERVER_OPERATORS: &str = "-549";
    pub const PRINT_OPERATORS: &str = "-550";
    pub const BACKUP_OPERATORS: &str = "-551";

    // Enterprise-wide RIDs
    pub const ENTERPRISE_DOMAIN_CONTROLLERS: &str = "-498";

    /// All high-value RID suffixes
    pub const ALL: &[&str] = &[
        DOMAIN_ADMINS,
        DOMAIN_CONTROLLERS,
        CERT_PUBLISHERS,
        SCHEMA_ADMINS,
        ENTERPRISE_ADMINS,
        GROUP_POLICY_CREATOR_OWNERS,
        READONLY_DOMAIN_CONTROLLERS,
        PROTECTED_USERS,
        KEY_ADMINS,
        ENTERPRISE_KEY_ADMINS,
        ADMINISTRATORS,
        ACCOUNT_OPERATORS,
        SERVER_OPERATORS,
        PRINT_OPERATORS,
        BACKUP_OPERATORS,
        ENTERPRISE_DOMAIN_CONTROLLERS,
    ];
}

/// User Account Control flags from Active Directory.
/// See: https://learn.microsoft.com/en-us/troubleshoot/windows-server/active-directory/useraccountcontrol-manipulate-account-properties
#[allow(dead_code)]
mod uac_flags {
    pub const SCRIPT: i64 = 0x0001;
    pub const ACCOUNTDISABLE: i64 = 0x0002;
    pub const HOMEDIR_REQUIRED: i64 = 0x0008;
    pub const LOCKOUT: i64 = 0x0010;
    pub const PASSWD_NOTREQD: i64 = 0x0020;
    pub const PASSWD_CANT_CHANGE: i64 = 0x0040;
    pub const ENCRYPTED_TEXT_PWD_ALLOWED: i64 = 0x0080;
    pub const NORMAL_ACCOUNT: i64 = 0x0200;
    pub const INTERDOMAIN_TRUST_ACCOUNT: i64 = 0x0800;
    pub const WORKSTATION_TRUST_ACCOUNT: i64 = 0x1000;
    pub const SERVER_TRUST_ACCOUNT: i64 = 0x2000;
    pub const DONT_EXPIRE_PASSWORD: i64 = 0x10000;
    pub const SMARTCARD_REQUIRED: i64 = 0x40000;
    pub const TRUSTED_FOR_DELEGATION: i64 = 0x80000;
    pub const NOT_DELEGATED: i64 = 0x100000;
    pub const USE_DES_KEY_ONLY: i64 = 0x200000;
    pub const DONT_REQ_PREAUTH: i64 = 0x400000;
    pub const PASSWORD_EXPIRED: i64 = 0x800000;
    pub const TRUSTED_TO_AUTH_FOR_DELEGATION: i64 = 0x1000000;
}

/// BloodHound file metadata.
#[derive(Debug, Deserialize)]
struct BloodHoundMeta {
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default)]
    version: Option<i32>,
}

/// BloodHound file structure with lazy parsing.
/// Uses RawValue to defer parsing of individual entities until needed,
/// reducing peak memory usage for large files.
#[derive(Debug, Deserialize)]
struct BloodHoundFile<'a> {
    meta: Option<BloodHoundMeta>,
    #[serde(borrow)]
    data: Vec<&'a RawValue>,
}

/// BloodHound data importer.
pub struct BloodHoundImporter {
    db: Arc<dyn DatabaseBackend>,
    progress_tx: broadcast::Sender<ImportProgress>,
    /// Track which object IDs we've seen to avoid duplicate nodes
    seen_nodes: HashSet<String>,
    /// Buffer relationships within current file, flushed per-file for live progress
    edge_buffer: Vec<DbEdge>,
    /// Buffer domain nodes from trust relationships (for orphaned domains)
    trust_domain_buffer: Vec<DbNode>,
}

impl BloodHoundImporter {
    pub fn new(
        db: Arc<dyn DatabaseBackend>,
        progress_tx: broadcast::Sender<ImportProgress>,
    ) -> Self {
        Self {
            db,
            progress_tx,
            seen_nodes: HashSet::new(),
            edge_buffer: Vec::new(),
            trust_domain_buffer: Vec::new(),
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
                        relationships = progress.edges_imported,
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

    /// Import from multiple JSON files with unified progress tracking.
    pub fn import_json_files<P: AsRef<Path>>(
        &mut self,
        paths: &[(String, P)],
        job_id: &str,
    ) -> Result<ImportProgress, String> {
        info!(file_count = paths.len(), "Importing multiple JSON files");

        let mut progress = ImportProgress::new(job_id.to_string()).with_total_files(paths.len());
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
                    self.send_progress(&progress);
                }
                Err(e) => {
                    warn!(file = %filename, error = %e, "Error importing file, continuing");
                    progress.files_processed += 1;
                }
            }
        }

        progress.complete();
        self.send_progress(&progress);
        Ok(progress)
    }

    /// Import from JSON string.
    /// Flushes both nodes and edges per-file for live progress updates.
    fn import_json_str(
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

            // Extract relationships - buffered and flushed at end of file
            let relationships = self.extract_edges(&data_type, &entity);
            self.edge_buffer.extend(relationships);
        }

        // Flush remaining nodes
        self.flush_nodes(&mut node_batch, progress)?;

        // Flush edges for this file - placeholder nodes handle missing targets
        self.flush_edge_buffer(progress)?;

        Ok(())
    }

    /// Extract a node from a BloodHound entity.
    fn extract_node(&self, data_type: &str, entity: &JsonValue) -> Option<DbNode> {
        let id = entity
            .get("ObjectIdentifier")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        let mut properties = entity.get("Properties").cloned().unwrap_or(JsonValue::Null);

        // Ensure objectid (SID) is always present in properties
        // Some BloodHound exports don't include it, but we need it for queries
        if let Some(props) = properties.as_object_mut() {
            if !props.contains_key("objectid") {
                props.insert("objectid".to_string(), JsonValue::String(id.clone()));
            }

            // Expand useraccountcontrol into individual boolean properties
            Self::expand_uac_flags(props);

            // Mark high-value objects based on SID
            Self::mark_high_value(props, &id);
        }

        let label = properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&id)
            .to_string();

        let node_type = self.normalize_type(data_type);

        // Domains are always high-value targets
        if node_type == "Domain" {
            if let Some(props) = properties.as_object_mut() {
                if !props.contains_key("is_highvalue") {
                    props.insert("is_highvalue".to_string(), JsonValue::Bool(true));
                }
            }
        }

        Some(DbNode {
            id,
            name: label,
            label: node_type.to_string(),
            properties,
        })
    }

    /// Expand useraccountcontrol bitmask into individual boolean properties.
    /// Also converts the raw UAC value to hex format for display.
    fn expand_uac_flags(props: &mut serde_json::Map<String, JsonValue>) {
        // Look for useraccountcontrol (case-insensitive)
        let uac_value = props
            .get("useraccountcontrol")
            .or_else(|| props.get("UserAccountControl"))
            .and_then(|v| v.as_i64());

        let Some(uac) = uac_value else {
            return;
        };

        // Convert to hex string for display (e.g., "0x10200")
        props.insert(
            "useraccountcontrol_hex".to_string(),
            JsonValue::String(format!("0x{:X}", uac)),
        );

        // Extract security-relevant flags as individual boolean properties
        // Only set properties if they don't already exist (BloodHound may have them)

        // enabled = NOT ACCOUNTDISABLE (most important flag)
        if !props.contains_key("enabled") {
            let enabled = (uac & uac_flags::ACCOUNTDISABLE) == 0;
            props.insert("enabled".to_string(), JsonValue::Bool(enabled));
        }

        // password_not_required - security risk
        if !props.contains_key("password_not_required") {
            let flag = (uac & uac_flags::PASSWD_NOTREQD) != 0;
            if flag {
                props.insert("password_not_required".to_string(), JsonValue::Bool(true));
            }
        }

        // password_never_expires - common misconfiguration
        if !props.contains_key("password_never_expires") {
            let flag = (uac & uac_flags::DONT_EXPIRE_PASSWORD) != 0;
            if flag {
                props.insert("password_never_expires".to_string(), JsonValue::Bool(true));
            }
        }

        // smartcard_required
        if !props.contains_key("smartcard_required") {
            let flag = (uac & uac_flags::SMARTCARD_REQUIRED) != 0;
            if flag {
                props.insert("smartcard_required".to_string(), JsonValue::Bool(true));
            }
        }

        // trusted_for_delegation - unconstrained delegation (high risk)
        if !props.contains_key("trusted_for_delegation") {
            let flag = (uac & uac_flags::TRUSTED_FOR_DELEGATION) != 0;
            if flag {
                props.insert("trusted_for_delegation".to_string(), JsonValue::Bool(true));
            }
        }

        // not_delegated - protected from delegation
        if !props.contains_key("not_delegated") {
            let flag = (uac & uac_flags::NOT_DELEGATED) != 0;
            if flag {
                props.insert("not_delegated".to_string(), JsonValue::Bool(true));
            }
        }

        // dont_require_preauth - AS-REP roastable (critical for attackers)
        if !props.contains_key("dont_require_preauth") {
            let flag = (uac & uac_flags::DONT_REQ_PREAUTH) != 0;
            if flag {
                props.insert("dont_require_preauth".to_string(), JsonValue::Bool(true));
            }
        }

        // password_expired
        if !props.contains_key("password_expired") {
            let flag = (uac & uac_flags::PASSWORD_EXPIRED) != 0;
            if flag {
                props.insert("password_expired".to_string(), JsonValue::Bool(true));
            }
        }

        // trusted_to_auth_for_delegation - constrained delegation with protocol transition
        if !props.contains_key("trusted_to_auth_for_delegation") {
            let flag = (uac & uac_flags::TRUSTED_TO_AUTH_FOR_DELEGATION) != 0;
            if flag {
                props.insert(
                    "trusted_to_auth_for_delegation".to_string(),
                    JsonValue::Bool(true),
                );
            }
        }

        // account_locked_out
        if !props.contains_key("account_locked_out") {
            let flag = (uac & uac_flags::LOCKOUT) != 0;
            if flag {
                props.insert("account_locked_out".to_string(), JsonValue::Bool(true));
            }
        }
    }

    /// Mark high-value objects based on their SID.
    /// Sets is_highvalue=true for objects with well-known privileged RIDs.
    fn mark_high_value(props: &mut serde_json::Map<String, JsonValue>, object_id: &str) {
        // Skip if already marked
        if props.contains_key("is_highvalue") {
            return;
        }

        // Check if the object's SID ends with a high-value RID
        let is_high_value = high_value_rids::ALL
            .iter()
            .any(|rid| object_id.ends_with(rid));

        if is_high_value {
            props.insert("is_highvalue".to_string(), JsonValue::Bool(true));
        }
    }

    /// Extract relationships from a BloodHound entity.
    fn extract_edges(&mut self, data_type: &str, entity: &JsonValue) -> Vec<DbEdge> {
        let object_id = match entity.get("ObjectIdentifier").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return Vec::new(),
        };

        // Normalize type name for consistency
        let node_type = self.normalize_type(data_type);

        let mut relationships = Vec::new();
        self.extract_member_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_session_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_local_group_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_ace_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_containment_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_delegation_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_gpo_link_edges(entity, &object_id, &node_type, &mut relationships);
        self.extract_trust_edges(entity, &object_id, &mut relationships);
        relationships
    }

    /// Extract MemberOf relationships from group membership.
    fn extract_member_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(members) = entity.get("Members").and_then(|v| v.as_array()) else {
            return;
        };
        for member in members {
            if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                let member_type = member.get("ObjectType").and_then(|v| v.as_str());
                relationships.push(DbEdge {
                    source: member_id.to_string(),
                    target: object_id.to_string(),
                    rel_type: "MemberOf".to_string(),
                    properties: JsonValue::Null,
                    source_type: member_type.map(String::from),
                    target_type: Some(target_type.to_string()),
                });
            }
        }
    }

    /// Extract HasSession relationships from computer sessions.
    fn extract_session_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
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
                    relationships.push(DbEdge {
                        source: user_sid.to_string(),
                        target: object_id.to_string(),
                        rel_type: "HasSession".to_string(),
                        properties: JsonValue::Null,
                        source_type: Some("User".to_string()),
                        target_type: Some(target_type.to_string()),
                    });
                }
            }
        }
    }

    /// Extract local group membership relationships (AdminTo, CanRDP, etc.).
    fn extract_local_group_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(local_groups) = entity.get("LocalGroups").and_then(|v| v.as_array()) else {
            return;
        };
        for group in local_groups {
            let group_name = group.get("Name").and_then(|v| v.as_str()).unwrap_or("");
            let rel_type = self.local_group_to_edge_type(group_name);

            let Some(results) = group.get("Results").and_then(|v| v.as_array()) else {
                continue;
            };
            for member in results {
                if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let member_type = member.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: member_id.to_string(),
                        target: object_id.to_string(),
                        rel_type: rel_type.to_string(),
                        properties: JsonValue::Null,
                        source_type: member_type.map(String::from),
                        target_type: Some(target_type.to_string()),
                    });
                }
            }
        }
    }

    /// Extract ACE permission relationships.
    fn extract_ace_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
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
            let rel_type = self.ace_to_edge_type(right_name);
            let is_inherited = ace
                .get("IsInherited")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let principal_type = ace.get("PrincipalType").and_then(|v| v.as_str());

            relationships.push(DbEdge {
                source: principal_sid.to_string(),
                target: object_id.to_string(),
                rel_type: rel_type.to_string(),
                properties: serde_json::json!({"inherited": is_inherited}),
                source_type: principal_type.map(String::from),
                target_type: Some(target_type.to_string()),
            });
        }
    }

    /// Extract containment relationships.
    fn extract_containment_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        // ContainedBy -> Contains relationship (reversed direction)
        if let Some(contained_by) = entity.get("ContainedBy") {
            if let Some(container_id) = contained_by
                .get("ObjectIdentifier")
                .and_then(|v| v.as_str())
            {
                let container_type = contained_by.get("ObjectType").and_then(|v| v.as_str());
                relationships.push(DbEdge {
                    source: container_id.to_string(),
                    target: object_id.to_string(),
                    rel_type: "Contains".to_string(),
                    properties: JsonValue::Null,
                    source_type: container_type.map(String::from),
                    target_type: Some(target_type.to_string()),
                });
            }
        }
    }

    /// Extract delegation relationships (AllowedToDelegate, AllowedToAct).
    fn extract_delegation_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        source_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        // AllowedToDelegate
        if let Some(delegates) = entity.get("AllowedToDelegate").and_then(|v| v.as_array()) {
            for delegate in delegates {
                if let Some(target_id) = delegate.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let target_type = delegate.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: object_id.to_string(),
                        target: target_id.to_string(),
                        rel_type: "AllowedToDelegate".to_string(),
                        properties: JsonValue::Null,
                        source_type: Some(source_type.to_string()),
                        target_type: target_type.map(String::from),
                    });
                }
            }
        }

        // AllowedToAct
        if let Some(actors) = entity.get("AllowedToAct").and_then(|v| v.as_array()) {
            for actor in actors {
                if let Some(actor_id) = actor.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let actor_type = actor.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: actor_id.to_string(),
                        target: object_id.to_string(),
                        rel_type: "AllowedToAct".to_string(),
                        properties: JsonValue::Null,
                        source_type: actor_type.map(String::from),
                        target_type: Some(source_type.to_string()),
                    });
                }
            }
        }
    }

    /// Extract GPO link relationships.
    fn extract_gpo_link_edges(
        &self,
        entity: &JsonValue,
        object_id: &str,
        source_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(links) = entity.get("Links").and_then(|v| v.as_array()) else {
            return;
        };
        for link in links {
            if let Some(gpo_id) = link.get("GUID").and_then(|v| v.as_str()) {
                relationships.push(DbEdge {
                    source: object_id.to_string(),
                    target: gpo_id.to_string(),
                    rel_type: "GPLink".to_string(),
                    properties: JsonValue::Null,
                    source_type: Some(source_type.to_string()),
                    target_type: Some("GPO".to_string()),
                });
            }
        }
    }

    /// Extract domain trust relationships and collect target domain nodes.
    fn extract_trust_edges(
        &mut self,
        entity: &JsonValue,
        object_id: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(trusts) = entity.get("Trusts").and_then(|v| v.as_array()) else {
            return;
        };
        for trust in trusts {
            let Some(target_sid) = trust.get("TargetDomainSid").and_then(|v| v.as_str()) else {
                continue;
            };

            // Extract target domain name if available, create a placeholder node
            if let Some(target_name) = trust.get("TargetDomainName").and_then(|v| v.as_str()) {
                if !self.seen_nodes.contains(target_sid) {
                    self.trust_domain_buffer.push(DbNode {
                        id: target_sid.to_string(),
                        name: target_name.to_string(),
                        label: "Domain".to_string(),
                        properties: serde_json::json!({
                            "name": target_name,
                            "domainsid": target_sid,
                            "collected": false
                        }),
                    });
                }
            }

            // Parse TrustDirection - supports both integer (legacy) and string (BloodHound CE) formats
            // Integer: 0=Disabled, 1=Inbound, 2=Outbound, 3=Bidirectional
            // String: "Disabled", "Inbound", "Outbound", "Bidirectional"
            let trust_direction = match trust.get("TrustDirection") {
                Some(JsonValue::Number(n)) => n.as_i64().unwrap_or(0),
                Some(JsonValue::String(s)) => match s.to_lowercase().as_str() {
                    "inbound" => 1,
                    "outbound" => 2,
                    "bidirectional" => 3,
                    _ => 0,
                },
                _ => 0,
            };

            // Bidirectional or outbound trust
            if trust_direction == 2 || trust_direction == 3 {
                relationships.push(DbEdge {
                    source: target_sid.to_string(),
                    target: object_id.to_string(),
                    rel_type: "TrustedBy".to_string(),
                    properties: serde_json::json!({"direction": trust_direction}),
                    source_type: Some("Domain".to_string()),
                    target_type: Some("Domain".to_string()),
                });
            }
            // Bidirectional or inbound trust
            if trust_direction == 1 || trust_direction == 3 {
                relationships.push(DbEdge {
                    source: object_id.to_string(),
                    target: target_sid.to_string(),
                    rel_type: "TrustedBy".to_string(),
                    properties: serde_json::json!({"direction": trust_direction}),
                    source_type: Some("Domain".to_string()),
                    target_type: Some("Domain".to_string()),
                });
            }
        }
    }

    /// Map local group name to relationship type.
    fn local_group_to_edge_type(&self, group_name: &str) -> &'static str {
        match group_name.to_uppercase().as_str() {
            s if s.contains("ADMINISTRATORS") => "AdminTo",
            s if s.contains("REMOTE DESKTOP") => "CanRDP",
            s if s.contains("REMOTE MANAGEMENT") => "CanPSRemote",
            s if s.contains("DISTRIBUTED COM") => "ExecuteDCOM",
            _ => "LocalGroupMember",
        }
    }

    /// Normalize BloodHound type name to standard format.
    fn normalize_type(&self, data_type: &str) -> String {
        match data_type.to_lowercase().as_str() {
            "users" | "user" => "User",
            "groups" | "group" => "Group",
            "computers" | "computer" => "Computer",
            "domains" | "domain" => "Domain",
            "gpos" | "gpo" => "GPO",
            "ous" | "ou" => "OU",
            "containers" | "container" => "Container",
            "certtemplates" | "certtemplate" => "CertTemplate",
            "enterprisecas" | "enterpriseca" => "EnterpriseCA",
            "rootcas" | "rootca" => "RootCA",
            "aiacas" | "aiaca" => "AIACA",
            "ntauthstores" | "ntauthstore" => "NTAuthStore",
            _ => "Base", // Unknown types get "Base" label
        }
        .to_string()
    }

    /// Map ACE right name to relationship type.
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

    /// Flush buffered domain nodes from trust relationships.
    /// Called before flushing relationships to ensure target domains exist.
    fn flush_trust_domains(&mut self, progress: &mut ImportProgress) -> Result<(), String> {
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
    fn flush_edge_buffer(&mut self, progress: &mut ImportProgress) -> Result<(), String> {
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

    fn send_progress(&self, progress: &ImportProgress) {
        let _ = self.progress_tx.send(progress.clone());
    }
}

#[cfg(all(test, feature = "cozo"))]
mod tests {
    use super::*;
    use crate::db::cozo::GraphDatabase;

    #[test]
    fn test_ace_to_edge_type() {
        let db = Arc::new(GraphDatabase::in_memory().unwrap());
        let (tx, _) = broadcast::channel(1);
        let importer = BloodHoundImporter::new(db, tx);

        assert_eq!(importer.ace_to_edge_type("GenericAll"), "GenericAll");
        assert_eq!(importer.ace_to_edge_type("WriteDacl"), "WriteDacl");
        assert_eq!(importer.ace_to_edge_type("Unknown"), "ACE");
    }

    #[test]
    fn test_local_group_to_edge_type() {
        let db = Arc::new(GraphDatabase::in_memory().unwrap());
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
        let db = Arc::new(GraphDatabase::in_memory().unwrap());
        let (tx, _) = broadcast::channel(100);
        BloodHoundImporter::new(db, tx)
    }

    // ========================================================================
    // Node Extraction Tests
    // ========================================================================

    #[test]
    fn test_extract_node_user() {
        let mut importer = test_importer();

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
        assert_eq!(node.name, "testuser@corp.local");
        assert_eq!(node.label, "User");
        assert_eq!(node.properties["enabled"], true);
    }

    #[test]
    fn test_extract_node_computer() {
        let mut importer = test_importer();

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
        assert_eq!(node.name, "DC01.corp.local");
        assert_eq!(node.label, "Computer");
    }

    #[test]
    fn test_extract_node_group() {
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-GROUP",
            "Properties": {
                "name": "Domain Admins"
            }
        });

        let node = importer.extract_node("groups", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.label, "Group");
        assert_eq!(node.name, "Domain Admins");
    }

    #[test]
    fn test_extract_node_missing_id() {
        let mut importer = test_importer();

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
        let mut importer = test_importer();

        // If name is missing, should use ObjectIdentifier as label
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-USER",
            "Properties": {}
        });

        let node = importer.extract_node("users", &entity);
        assert!(node.is_some());

        let node = node.unwrap();
        assert_eq!(node.name, "S-1-5-21-1234-USER");
    }

    #[test]
    fn test_extract_node_expands_uac_flags() {
        let mut importer = test_importer();

        // UAC = 0x10200 = NORMAL_ACCOUNT (0x200) + DONT_EXPIRE_PASSWORD (0x10000)
        // Account is enabled (ACCOUNTDISABLE bit not set)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-USER",
            "Properties": {
                "name": "testuser@corp.local",
                "useraccountcontrol": 0x10200
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();

        // Check hex representation
        assert_eq!(node.properties["useraccountcontrol_hex"], "0x10200");

        // Check expanded flags
        assert_eq!(node.properties["enabled"], true); // ACCOUNTDISABLE not set
        assert_eq!(node.properties["password_never_expires"], true); // DONT_EXPIRE_PASSWORD set
    }

    #[test]
    fn test_extract_node_uac_disabled_account() {
        let mut importer = test_importer();

        // UAC = 0x202 = ACCOUNTDISABLE (0x2) + NORMAL_ACCOUNT (0x200)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-DISABLED",
            "Properties": {
                "name": "disabled@corp.local",
                "useraccountcontrol": 0x202
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();

        assert_eq!(node.properties["enabled"], false); // ACCOUNTDISABLE is set
    }

    #[test]
    fn test_extract_node_uac_asrep_roastable() {
        let mut importer = test_importer();

        // UAC = 0x400200 = NORMAL_ACCOUNT (0x200) + DONT_REQ_PREAUTH (0x400000)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-ASREP",
            "Properties": {
                "name": "asrep@corp.local",
                "useraccountcontrol": 0x400200
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();

        assert_eq!(node.properties["enabled"], true);
        assert_eq!(node.properties["dont_require_preauth"], true); // AS-REP roastable
    }

    #[test]
    fn test_extract_node_uac_preserves_existing_enabled() {
        let mut importer = test_importer();

        // If BloodHound already provides 'enabled', don't overwrite it
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-USER",
            "Properties": {
                "name": "testuser@corp.local",
                "useraccountcontrol": 0x202, // Would normally mean disabled
                "enabled": true // But BloodHound says enabled
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();

        // Should preserve the existing 'enabled' value
        assert_eq!(node.properties["enabled"], true);
    }

    // ========================================================================
    // High Value Detection Tests
    // ========================================================================

    #[test]
    fn test_extract_node_marks_domain_admins_high_value() {
        let mut importer = test_importer();

        // Domain Admins group (SID ends with -512)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-512",
            "Properties": {
                "name": "DOMAIN ADMINS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["is_highvalue"], true);
    }

    #[test]
    fn test_extract_node_marks_enterprise_admins_high_value() {
        let mut importer = test_importer();

        // Enterprise Admins group (SID ends with -519)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-519",
            "Properties": {
                "name": "ENTERPRISE ADMINS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["is_highvalue"], true);
    }

    #[test]
    fn test_extract_node_marks_builtin_administrators_high_value() {
        let mut importer = test_importer();

        // Builtin Administrators (SID ends with -544)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-32-544",
            "Properties": {
                "name": "ADMINISTRATORS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["is_highvalue"], true);
    }

    #[test]
    fn test_extract_node_marks_domain_high_value() {
        let mut importer = test_importer();

        // Domain objects should be high value
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890",
            "Properties": {
                "name": "CORP.LOCAL"
            }
        });

        let node = importer.extract_node("domains", &entity).unwrap();
        assert_eq!(node.properties["is_highvalue"], true);
    }

    #[test]
    fn test_extract_node_regular_user_not_high_value() {
        let mut importer = test_importer();

        // Regular user should NOT be high value
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-1001",
            "Properties": {
                "name": "regularuser@corp.local"
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();
        assert!(node.properties.get("is_highvalue").is_none());
    }

    #[test]
    fn test_extract_node_preserves_existing_highvalue() {
        let mut importer = test_importer();

        // If BloodHound already marks as high value, preserve it
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-1001",
            "Properties": {
                "name": "specialuser@corp.local",
                "is_highvalue": true
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();
        assert_eq!(node.properties["is_highvalue"], true);
    }

    // ========================================================================
    // Relationship Extraction Tests
    // ========================================================================

    #[test]
    fn test_extract_edges_memberof() {
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-GROUP1",
            "Members": [
                {"ObjectIdentifier": "S-1-5-21-USER1", "ObjectType": "User"},
                {"ObjectIdentifier": "S-1-5-21-USER2", "ObjectType": "User"}
            ]
        });

        let relationships = importer.extract_edges("groups", &entity);

        assert_eq!(relationships.len(), 2);
        // Members point TO the group (MemberOf)
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-GROUP1"
            && e.rel_type == "MemberOf"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-USER2"
            && e.target == "S-1-5-21-GROUP1"
            && e.rel_type == "MemberOf"));
    }

    #[test]
    fn test_extract_edges_sessions() {
        let mut importer = test_importer();

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

        let relationships = importer.extract_edges("computers", &entity);

        assert_eq!(relationships.len(), 2);
        assert!(relationships
            .iter()
            .all(|e| e.rel_type == "HasSession" && e.target == "S-1-5-21-COMP1"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-USER1"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-ADMIN1"));
    }

    #[test]
    fn test_extract_edges_aces() {
        let mut importer = test_importer();

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

        let relationships = importer.extract_edges("users", &entity);

        assert_eq!(relationships.len(), 2);

        let generic_all = relationships
            .iter()
            .find(|e| e.source == "S-1-5-21-ATTACKER")
            .unwrap();
        assert_eq!(generic_all.rel_type, "GenericAll");
        assert_eq!(generic_all.properties["inherited"], false);

        let write_dacl = relationships
            .iter()
            .find(|e| e.source == "S-1-5-21-USER1")
            .unwrap();
        assert_eq!(write_dacl.rel_type, "WriteDacl");
        assert_eq!(write_dacl.properties["inherited"], true);
    }

    #[test]
    fn test_extract_edges_trusts() {
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN2",
                    "TrustDirection": 3  // Bidirectional
                }
            ]
        });

        let relationships = importer.extract_edges("domains", &entity);

        // Bidirectional trust creates 2 relationships
        assert_eq!(relationships.len(), 2);
        assert!(relationships
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN2" && e.target == "S-1-5-21-DOMAIN1"));
        assert!(relationships
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN1" && e.target == "S-1-5-21-DOMAIN2"));
    }

    #[test]
    fn test_extract_edges_trusts_string_format() {
        // Test BloodHound CE format which uses string values for TrustDirection
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN2",
                    "TrustDirection": "Bidirectional"
                },
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN3",
                    "TrustDirection": "Outbound"
                },
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN4",
                    "TrustDirection": "Inbound"
                }
            ]
        });

        let relationships = importer.extract_edges("domains", &entity);

        // Bidirectional creates 2 relationships, Outbound creates 1, Inbound creates 1 = 4 total
        assert_eq!(relationships.len(), 4);

        // Bidirectional with DOMAIN2
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-DOMAIN2"
            && e.target == "S-1-5-21-DOMAIN1"
            && e.rel_type == "TrustedBy"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-DOMAIN1"
            && e.target == "S-1-5-21-DOMAIN2"
            && e.rel_type == "TrustedBy"));

        // Outbound to DOMAIN3 (we trust them)
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-DOMAIN3"
            && e.target == "S-1-5-21-DOMAIN1"
            && e.rel_type == "TrustedBy"));

        // Inbound from DOMAIN4 (they trust us)
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-DOMAIN1"
            && e.target == "S-1-5-21-DOMAIN4"
            && e.rel_type == "TrustedBy"));
    }

    #[test]
    fn test_extract_edges_containedby() {
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-USER1",
            "ContainedBy": {
                "ObjectIdentifier": "S-1-5-21-OU1",
                "ObjectType": "OU"
            }
        });

        let relationships = importer.extract_edges("users", &entity);

        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].source, "S-1-5-21-OU1");
        assert_eq!(relationships[0].target, "S-1-5-21-USER1");
        assert_eq!(relationships[0].rel_type, "Contains");
    }

    #[test]
    fn test_extract_edges_delegation() {
        let mut importer = test_importer();

        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-USER1",
            "AllowedToDelegate": [
                {"ObjectIdentifier": "S-1-5-21-SERVICE1"}
            ],
            "AllowedToAct": [
                {"ObjectIdentifier": "S-1-5-21-ACTOR1"}
            ]
        });

        let relationships = importer.extract_edges("users", &entity);

        assert_eq!(relationships.len(), 2);
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-SERVICE1"
            && e.rel_type == "AllowedToDelegate"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-ACTOR1"
            && e.target == "S-1-5-21-USER1"
            && e.rel_type == "AllowedToAct"));
    }

    #[test]
    fn test_extract_edges_local_groups() {
        let mut importer = test_importer();

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

        let relationships = importer.extract_edges("computers", &entity);

        assert_eq!(relationships.len(), 2);
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-ADMIN1"
            && e.target == "S-1-5-21-COMP1"
            && e.rel_type == "AdminTo"));
        assert!(relationships.iter().any(|e| e.source == "S-1-5-21-USER1"
            && e.target == "S-1-5-21-COMP1"
            && e.rel_type == "CanRDP"));
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
        assert_eq!(progress.edges_imported, 2); // 2 MemberOf relationships

        // Verify relationships are in database
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
        assert_eq!(nodes[0].label, "Group");
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
