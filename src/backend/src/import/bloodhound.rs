//! BloodHound JSON/ZIP importer.

use crate::db::{DatabaseBackend, DbEdge, DbNode};
use crate::import::types::ImportProgress;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};
use zip::ZipArchive;

/// Batch size for database inserts.
const BATCH_SIZE: usize = 2000;

/// Well-known tier-0 RIDs in Active Directory.
/// These are built-in privileged groups that attackers typically target.
/// See: https://learn.microsoft.com/en-us/windows-server/identity/ad-ds/manage/understand-security-identifiers
mod tier_zero_rids {
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
    pub const ENTERPRISE_READONLY_DOMAIN_CONTROLLERS: &str = "-498";

    // Well-known SIDs (not domain-relative RIDs)
    // See: https://learn.microsoft.com/en-us/windows-server/identity/ad-ds/manage/understand-security-identifiers
    pub const ENTERPRISE_DOMAIN_CONTROLLERS: &str = "-S-1-5-9";

    /// All tier-0 SID suffixes (works via ends_with matching).
    /// Includes both domain-relative RIDs (e.g. "-512") and well-known SIDs (e.g. "S-1-5-9").
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
        ENTERPRISE_READONLY_DOMAIN_CONTROLLERS,
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
    /// Track which edges we've seen to avoid duplicates across entities.
    /// Key is (source, target, rel_type).
    seen_edges: HashSet<(String, String, String)>,
    /// Buffer relationships within current file, flushed per-file for live progress
    edge_buffer: Vec<DbEdge>,
    /// Buffer domain nodes from trust relationships (for orphaned domains)
    trust_domain_buffer: Vec<DbNode>,
    /// Per-domain tracking of principals with GetChanges ACE (for deferred DCSync)
    dcsync_get_changes: HashMap<String, HashSet<String>>,
    /// Per-domain tracking of principals with GetChangesAll ACE (for deferred DCSync)
    dcsync_get_changes_all: HashMap<String, HashSet<String>>,
    /// Group -> members (from Members arrays and PrimaryGroupSID) for DCSync expansion
    group_members: HashMap<String, HashSet<String>>,
    /// Domain SID -> domain name (for well-known SID resolution in DCSync)
    domain_sid_to_name: HashMap<String, String>,
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
            seen_edges: HashSet::new(),
            edge_buffer: Vec::new(),
            trust_domain_buffer: Vec::new(),
            dcsync_get_changes: HashMap::new(),
            dcsync_get_changes_all: HashMap::new(),
            group_members: HashMap::new(),
            domain_sid_to_name: HashMap::new(),
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

        // Derive deferred edges (DCSync through group membership)
        self.flush_deferred_dcsync(&mut progress)?;

        // Resolve placeholder node names using domain SID-to-name mappings
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
        self.send_progress(&progress);
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

        // Derive deferred edges (DCSync through group membership)
        self.flush_deferred_dcsync(&mut progress)?;

        // Resolve placeholder node names using domain SID-to-name mappings
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

        // Derive deferred edges (DCSync through group membership)
        self.flush_deferred_dcsync(&mut progress)?;

        // Resolve placeholder node names using domain SID-to-name mappings
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

            // Assign tier based on SID (tier 0 for privileged groups)
            Self::assign_tier(props, &id);
        }

        let label = properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&id)
            .to_string();

        let node_type = self.normalize_type(data_type);

        // Domains are always tier 0
        if node_type == "Domain" {
            if let Some(props) = properties.as_object_mut() {
                if !props.contains_key("tier") {
                    props.insert("tier".to_string(), JsonValue::Number(0.into()));
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

    /// Well-known RIDs that should receive tier 2.
    const TIER_TWO_RIDS: &'static [&'static str] = &[
        "-515", // Domain Computers
    ];

    /// Assign tier based on the object's SID.
    /// Sets tier=0 for privileged RIDs, tier=2 for well-known non-privileged groups.
    fn assign_tier(props: &mut serde_json::Map<String, JsonValue>, objectid: &str) {
        // Skip if already assigned
        if props.contains_key("tier") {
            return;
        }

        // Check if the object's SID ends with a tier-0 RID
        let is_tier_zero = tier_zero_rids::ALL
            .iter()
            .any(|rid| objectid.ends_with(rid));

        if is_tier_zero {
            props.insert("tier".to_string(), JsonValue::Number(0.into()));
            return;
        }

        // Check if the object's SID ends with a tier-2 RID
        let is_tier_two = Self::TIER_TWO_RIDS
            .iter()
            .any(|rid| objectid.ends_with(rid));

        if is_tier_two {
            props.insert("tier".to_string(), JsonValue::Number(2.into()));
        }
    }

    /// Extract relationships from a BloodHound entity.
    fn extract_edges(&mut self, data_type: &str, entity: &JsonValue) -> Vec<DbEdge> {
        let objectid = match entity.get("ObjectIdentifier").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return Vec::new(),
        };

        // Normalize type name for consistency
        let node_type = self.normalize_type(data_type);

        let mut relationships = Vec::new();
        self.extract_member_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_primary_group_edge(entity, &objectid, &node_type, &mut relationships);
        self.extract_session_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_local_group_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_ace_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_containment_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_delegation_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_gpo_link_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_trust_edges(entity, &objectid, &mut relationships);
        self.emit_wellknown_memberof(entity, &objectid, &node_type, &mut relationships);
        self.derive_dcsync_edges(&objectid, &node_type, &mut relationships);
        self.extract_pki_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_domain_sid_edges(entity, &objectid, &node_type, &mut relationships);
        self.extract_coerce_to_tgt(entity, &objectid, &node_type, &mut relationships);

        // Track state for deferred DCSync derivation
        self.track_dcsync_state(entity, &objectid, &node_type, &relationships);

        relationships
    }

    /// Extract MemberOf relationships from group membership.
    fn extract_member_edges(
        &self,
        entity: &JsonValue,
        objectid: &str,
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
                    target: objectid.to_string(),
                    rel_type: "MemberOf".to_string(),
                    properties: JsonValue::Null,
                    source_type: member_type.map(String::from),
                    target_type: Some(target_type.to_string()),
                });
            }
        }
    }

    /// Extract MemberOf edge from PrimaryGroupSID.
    ///
    /// Every user and computer in AD has a primary group (typically "Domain
    /// Users" or "Domain Computers").  This membership is NOT listed in the
    /// group's `Members` array -- it's stored as `PrimaryGroupSID` on the
    /// entity itself.
    fn extract_primary_group_edge(
        &self,
        entity: &JsonValue,
        objectid: &str,
        source_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(pg_sid) = entity.get("PrimaryGroupSID").and_then(|v| v.as_str()) else {
            return;
        };
        if pg_sid.is_empty() {
            return;
        }
        relationships.push(DbEdge {
            source: objectid.to_string(),
            target: pg_sid.to_string(),
            rel_type: "MemberOf".to_string(),
            properties: JsonValue::Null,
            source_type: Some(source_type.to_string()),
            target_type: Some("Group".to_string()),
        });
    }

    /// Extract HasSession relationships from computer sessions.
    fn extract_session_edges(
        &self,
        entity: &JsonValue,
        objectid: &str,
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
                    // Direction: Computer -> User ("this computer has a session for this user")
                    relationships.push(DbEdge {
                        source: objectid.to_string(),
                        target: user_sid.to_string(),
                        rel_type: "HasSession".to_string(),
                        properties: JsonValue::Null,
                        source_type: Some(target_type.to_string()),
                        target_type: Some("User".to_string()),
                    });
                }
            }
        }
    }

    /// Extract local group membership relationships (AdminTo, CanRDP, etc.).
    fn extract_local_group_edges(
        &self,
        entity: &JsonValue,
        objectid: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(local_groups) = entity.get("LocalGroups").and_then(|v| v.as_array()) else {
            return;
        };
        for group in local_groups {
            let group_name = group.get("Name").and_then(|v| v.as_str()).unwrap_or("");
            let Some(rel_type) = self.local_group_to_edge_type(group_name) else {
                continue;
            };

            let Some(results) = group.get("Results").and_then(|v| v.as_array()) else {
                continue;
            };
            for member in results {
                if let Some(member_id) = member.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let member_type = member.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: member_id.to_string(),
                        target: objectid.to_string(),
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
        objectid: &str,
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

            // BH CE drops self-referencing ACEs (node granting rights to itself)
            if principal_sid == objectid {
                continue;
            }

            // Only recognized ACE rights produce edges; unknown rights are dropped
            let Some(rel_type) = self.ace_to_edge_type(right_name) else {
                trace!(right_name, "Skipping unrecognized ACE right");
                continue;
            };

            let is_inherited = ace
                .get("IsInherited")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let principal_type = ace.get("PrincipalType").and_then(|v| v.as_str());

            relationships.push(DbEdge {
                source: principal_sid.to_string(),
                target: objectid.to_string(),
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
        objectid: &str,
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
                    target: objectid.to_string(),
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
        objectid: &str,
        source_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        // AllowedToDelegate
        if let Some(delegates) = entity.get("AllowedToDelegate").and_then(|v| v.as_array()) {
            for delegate in delegates {
                if let Some(target_id) = delegate.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let target_type = delegate.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: objectid.to_string(),
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
                        target: objectid.to_string(),
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
        objectid: &str,
        source_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let Some(links) = entity.get("Links").and_then(|v| v.as_array()) else {
            return;
        };
        for link in links {
            if let Some(gpo_id) = link.get("GUID").and_then(|v| v.as_str()) {
                let enforced = link
                    .get("IsEnforced")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                // Direction: GPO -> OU/Domain ("this GPO is linked to this OU/Domain")
                relationships.push(DbEdge {
                    source: gpo_id.to_string(),
                    target: objectid.to_string(),
                    rel_type: "GPLink".to_string(),
                    properties: serde_json::json!({"enforced": enforced}),
                    source_type: Some("GPO".to_string()),
                    target_type: Some(source_type.to_string()),
                });
            }
        }
    }

    /// Extract domain trust relationships and collect target domain nodes.
    fn extract_trust_edges(
        &mut self,
        entity: &JsonValue,
        objectid: &str,
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

            // Determine trust edge type from TrustType.
            // Intra-forest trusts (ParentChild, TreeRoot, Shortcut) use
            // SameForestTrust; everything else (External, Forest, Unknown)
            // uses CrossForestTrust.  Matches BloodHound CE semantics.
            let trust_type_str = trust
                .get("TrustType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let rel_type = match trust_type_str.to_lowercase().as_str() {
                "parentchild" | "treeroot" | "shortcut" => "SameForestTrust",
                _ => "CrossForestTrust",
            };

            let props = serde_json::json!({
                "direction": trust_direction,
                "trusttype": trust_type_str,
                "isTransitive": trust.get("IsTransitive").and_then(|v| v.as_bool()).unwrap_or(false),
                "sidFilteringEnabled": trust.get("SidFilteringEnabled").and_then(|v| v.as_bool()).unwrap_or(false),
            });

            // Outbound or bidirectional: WE trust THEM. Edge: us -> them.
            if trust_direction == 2 || trust_direction == 3 {
                relationships.push(DbEdge {
                    source: objectid.to_string(),
                    target: target_sid.to_string(),
                    rel_type: rel_type.to_string(),
                    properties: props.clone(),
                    source_type: Some("Domain".to_string()),
                    target_type: Some("Domain".to_string()),
                });
            }
            // Inbound or bidirectional: THEY trust US. Edge: them -> us.
            if trust_direction == 1 || trust_direction == 3 {
                relationships.push(DbEdge {
                    source: target_sid.to_string(),
                    target: objectid.to_string(),
                    rel_type: rel_type.to_string(),
                    properties: props,
                    source_type: Some("Domain".to_string()),
                    target_type: Some("Domain".to_string()),
                });
            }
        }
    }

    /// Derive DCSync edges when a principal holds both GetChanges and
    /// GetChangesAll on the same target (typically a Domain).
    fn derive_dcsync_edges(
        &self,
        objectid: &str,
        target_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        // Collect principals that have GetChanges on this object
        let get_changes: HashSet<&str> = relationships
            .iter()
            .filter(|e| e.target == objectid && e.rel_type == "GetChanges")
            .map(|e| e.source.as_str())
            .collect();

        if get_changes.is_empty() {
            return;
        }

        // Find principals that also have GetChangesAll
        let dcsync_principals: Vec<String> = relationships
            .iter()
            .filter(|e| {
                e.target == objectid
                    && e.rel_type == "GetChangesAll"
                    && get_changes.contains(e.source.as_str())
            })
            .map(|e| e.source.clone())
            .collect();

        for principal in dcsync_principals {
            relationships.push(DbEdge {
                source: principal,
                target: objectid.to_string(),
                rel_type: "DCSync".to_string(),
                properties: JsonValue::Null,
                source_type: None,
                target_type: Some(target_type.to_string()),
            });
        }
    }

    /// Extract PKI/ADCS topology edges from Enterprise CAs.
    ///
    /// Handles: PublishedTo (from EnabledCertTemplates), HostsCAService (from
    /// HostingComputer), EnterpriseCAFor (from Properties.domainsid),
    /// IssuedSignedBy (from CARegistryData.CertChain), and CA-specific ACEs
    /// from CARegistryData.CASecurity.
    fn extract_pki_edges(
        &self,
        entity: &JsonValue,
        objectid: &str,
        node_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        if node_type != "EnterpriseCA" {
            return;
        }

        // EnabledCertTemplates -> PublishedTo (Template -> CA)
        if let Some(templates) = entity
            .get("EnabledCertTemplates")
            .and_then(|v| v.as_array())
        {
            for tmpl in templates {
                if let Some(tmpl_id) = tmpl.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    relationships.push(DbEdge {
                        source: tmpl_id.to_string(),
                        target: objectid.to_string(),
                        rel_type: "PublishedTo".to_string(),
                        properties: JsonValue::Null,
                        source_type: Some("CertTemplate".to_string()),
                        target_type: Some(node_type.to_string()),
                    });
                }
            }
        }

        // HostingComputer -> HostsCAService (Computer -> CA)
        if let Some(host_id) = entity.get("HostingComputer").and_then(|v| v.as_str()) {
            if !host_id.is_empty() {
                relationships.push(DbEdge {
                    source: host_id.to_string(),
                    target: objectid.to_string(),
                    rel_type: "HostsCAService".to_string(),
                    properties: JsonValue::Null,
                    source_type: Some("Computer".to_string()),
                    target_type: Some(node_type.to_string()),
                });
            }
        }

        // Properties.domainsid -> EnterpriseCAFor (CA -> Domain)
        if let Some(domain_sid) = entity
            .get("Properties")
            .and_then(|v| v.get("domainsid"))
            .and_then(|v| v.as_str())
        {
            if !domain_sid.is_empty() {
                relationships.push(DbEdge {
                    source: objectid.to_string(),
                    target: domain_sid.to_string(),
                    rel_type: "EnterpriseCAFor".to_string(),
                    properties: JsonValue::Null,
                    source_type: Some(node_type.to_string()),
                    target_type: Some("Domain".to_string()),
                });
            }
        }

        // CARegistryData.CertChain -> IssuedSignedBy (CA -> RootCA)
        if let Some(chain) = entity
            .get("CARegistryData")
            .and_then(|v| v.get("CertChain"))
            .and_then(|v| v.as_array())
        {
            for cert in chain {
                if let Some(cert_id) = cert.get("ObjectIdentifier").and_then(|v| v.as_str()) {
                    let cert_type = cert.get("ObjectType").and_then(|v| v.as_str());
                    relationships.push(DbEdge {
                        source: objectid.to_string(),
                        target: cert_id.to_string(),
                        rel_type: "IssuedSignedBy".to_string(),
                        properties: JsonValue::Null,
                        source_type: Some(node_type.to_string()),
                        target_type: cert_type.map(String::from),
                    });
                }
            }
        }

        // CARegistryData.CASecurity -> ACE edges (ManageCA, Enroll, etc.)
        if let Some(aces) = entity
            .get("CARegistryData")
            .and_then(|v| v.get("CASecurity"))
            .and_then(|v| v.get("Data"))
            .and_then(|v| v.as_array())
        {
            for ace in aces {
                let (Some(principal_sid), Some(right_name)) = (
                    ace.get("PrincipalSID").and_then(|v| v.as_str()),
                    ace.get("RightName").and_then(|v| v.as_str()),
                ) else {
                    continue;
                };
                if principal_sid == objectid {
                    continue;
                }
                // CASecurity only produces ManageCA, ManageCertificates, Enroll
                // in BH CE. Other ACE types (Owns, GenericAll, etc.) are dropped.
                let rel_type = match right_name {
                    "ManageCA" => "ManageCA",
                    "ManageCertificates" => "ManageCertificates",
                    "Enroll" => "Enroll",
                    _ => continue,
                };
                let is_inherited = ace
                    .get("IsInherited")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let principal_type = ace.get("PrincipalType").and_then(|v| v.as_str());
                relationships.push(DbEdge {
                    source: principal_sid.to_string(),
                    target: objectid.to_string(),
                    rel_type: rel_type.to_string(),
                    properties: serde_json::json!({"inherited": is_inherited}),
                    source_type: principal_type.map(String::from),
                    target_type: Some(node_type.to_string()),
                });
            }
        }
    }

    /// Extract domain-relationship edges for RootCAs and NTAuth stores.
    ///
    /// Handles: RootCAFor (RootCA -> Domain from DomainSID), NTAuthStoreFor
    /// (NTAuth -> Domain from DomainSID), TrustedForNTAuth (CA -> NTAuth from
    /// NTAuthCertificates).
    fn extract_domain_sid_edges(
        &self,
        entity: &JsonValue,
        objectid: &str,
        node_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        let domain_sid = entity.get("DomainSID").and_then(|v| v.as_str());

        match node_type {
            "RootCA" => {
                if let Some(sid) = domain_sid {
                    if !sid.is_empty() {
                        relationships.push(DbEdge {
                            source: objectid.to_string(),
                            target: sid.to_string(),
                            rel_type: "RootCAFor".to_string(),
                            properties: JsonValue::Null,
                            source_type: Some(node_type.to_string()),
                            target_type: Some("Domain".to_string()),
                        });
                    }
                }
            }
            "NTAuthStore" => {
                if let Some(sid) = domain_sid {
                    if !sid.is_empty() {
                        relationships.push(DbEdge {
                            source: objectid.to_string(),
                            target: sid.to_string(),
                            rel_type: "NTAuthStoreFor".to_string(),
                            properties: JsonValue::Null,
                            source_type: Some(node_type.to_string()),
                            target_type: Some("Domain".to_string()),
                        });
                    }
                }

                // NTAuthCertificates -> TrustedForNTAuth (CA -> NTAuth)
                if let Some(certs) = entity.get("NTAuthCertificates").and_then(|v| v.as_array()) {
                    for cert in certs {
                        if let Some(cert_id) = cert.get("ObjectIdentifier").and_then(|v| v.as_str())
                        {
                            let cert_type = cert.get("ObjectType").and_then(|v| v.as_str());
                            relationships.push(DbEdge {
                                source: cert_id.to_string(),
                                target: objectid.to_string(),
                                rel_type: "TrustedForNTAuth".to_string(),
                                properties: JsonValue::Null,
                                source_type: cert_type.map(String::from),
                                target_type: Some(node_type.to_string()),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Extract CoerceToTGT edges for computers with unconstrained delegation.
    ///
    /// BH CE creates a (Computer)-[CoerceToTGT]->(Domain) edge when a computer
    /// has `unconstraineddelegation=true` in its properties.
    fn extract_coerce_to_tgt(
        &self,
        entity: &JsonValue,
        objectid: &str,
        node_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        if node_type != "Computer" {
            return;
        }
        let props = entity.get("Properties");
        let unconstrained = props
            .and_then(|p| p.get("unconstraineddelegation"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !unconstrained {
            return;
        }
        let Some(domain_sid) = props
            .and_then(|p| p.get("domainsid"))
            .and_then(|v| v.as_str())
        else {
            return;
        };
        if domain_sid.is_empty() {
            return;
        }
        relationships.push(DbEdge {
            source: objectid.to_string(),
            target: domain_sid.to_string(),
            rel_type: "CoerceToTGT".to_string(),
            properties: JsonValue::Null,
            source_type: Some(node_type.to_string()),
            target_type: Some("Domain".to_string()),
        });
    }

    /// Emit well-known implicit MemberOf edges for a domain.
    ///
    /// BH CE materializes these implicit group memberships that exist in every
    /// AD domain but are not present in SharpHound's Members arrays:
    /// - Guest (-501) -> Everyone (S-1-1-0)
    /// - Domain Users (-513) -> Authenticated Users (S-1-5-11)
    /// - Domain Computers (-515) -> Authenticated Users (S-1-5-11)
    /// - Authenticated Users (S-1-5-11) -> Everyone (S-1-1-0)
    fn emit_wellknown_memberof(
        &self,
        _entity: &JsonValue,
        objectid: &str,
        node_type: &str,
        relationships: &mut Vec<DbEdge>,
    ) {
        if node_type != "Domain" {
            return;
        }
        let sid = objectid;
        let pairs: &[(&str, &str, &str, &str)] = &[
            ("-501", "User", "-S-1-1-0", "Group"),   // Guest -> Everyone
            ("-513", "Group", "-S-1-5-11", "Group"), // Domain Users -> Auth Users
            ("-515", "Group", "-S-1-5-11", "Group"), // Domain Computers -> Auth Users
            ("-S-1-5-11", "Group", "-S-1-1-0", "Group"), // Auth Users -> Everyone
        ];
        for &(src_suffix, src_type, tgt_suffix, tgt_type) in pairs {
            relationships.push(DbEdge {
                source: format!("{sid}{src_suffix}"),
                target: format!("{sid}{tgt_suffix}"),
                rel_type: "MemberOf".to_string(),
                properties: JsonValue::Null,
                source_type: Some(src_type.to_string()),
                target_type: Some(tgt_type.to_string()),
            });
        }
    }

    /// Track state needed for deferred DCSync derivation.
    ///
    /// Collects: GetChanges/GetChangesAll principals per domain, group
    /// memberships, PrimaryGroupSID memberships, and domain name mappings.
    fn track_dcsync_state(
        &mut self,
        entity: &JsonValue,
        objectid: &str,
        node_type: &str,
        relationships: &[DbEdge],
    ) {
        // Track domain name -> SID mapping
        if node_type == "Domain" {
            if let Some(name) = entity
                .get("Properties")
                .and_then(|p| p.get("name"))
                .and_then(|v| v.as_str())
            {
                self.domain_sid_to_name
                    .insert(objectid.to_string(), name.to_uppercase());
            }
        }

        // Track GetChanges / GetChangesAll from emitted edges
        for edge in relationships {
            if edge.rel_type == "GetChanges" {
                self.dcsync_get_changes
                    .entry(edge.target.clone())
                    .or_default()
                    .insert(edge.source.clone());
            } else if edge.rel_type == "GetChangesAll" {
                self.dcsync_get_changes_all
                    .entry(edge.target.clone())
                    .or_default()
                    .insert(edge.source.clone());
            }
        }

        // Track group memberships from MemberOf edges
        for edge in relationships {
            if edge.rel_type == "MemberOf" {
                self.group_members
                    .entry(edge.target.clone())
                    .or_default()
                    .insert(edge.source.clone());
            }
        }

        // Track DC implicit membership in Enterprise Domain Controllers.
        // DCs have PrimaryGroupSID ending in -516 (Domain Controllers).
        // All DCs are implicitly members of Enterprise Domain Controllers
        // ({DomainName}-S-1-5-9), which typically holds GetChanges.
        if let Some(pg_sid) = entity.get("PrimaryGroupSID").and_then(|v| v.as_str()) {
            if pg_sid.ends_with("-516") {
                if let Some(domain_sid) = entity
                    .get("Properties")
                    .and_then(|p| p.get("domainsid"))
                    .and_then(|v| v.as_str())
                {
                    if let Some(domain_name) = self.domain_sid_to_name.get(domain_sid) {
                        let edc_sid = format!("{}-S-1-5-9", domain_name);
                        self.group_members
                            .entry(edc_sid)
                            .or_default()
                            .insert(objectid.to_string());
                    }
                }
            }
        }
    }

    /// Derive DCSync edges from transitive group membership.
    ///
    /// Called after all entities are processed. Expands group memberships one
    /// level into the GetChanges/GetChangesAll sets, then creates DCSync edges
    /// for principals that hold both rights on a domain.
    pub fn derive_deferred_dcsync(&self) -> Vec<DbEdge> {
        let mut result = Vec::new();

        // Collect all domain OIDs that have any DCSync ACEs
        let domains: HashSet<&String> = self
            .dcsync_get_changes
            .keys()
            .chain(self.dcsync_get_changes_all.keys())
            .collect();

        for domain_oid in domains {
            let gc = self.dcsync_get_changes.get(domain_oid);
            let gca = self.dcsync_get_changes_all.get(domain_oid);

            let (Some(gc), Some(gca)) = (gc, gca) else {
                continue;
            };

            // Expand one level: for each group in the set, add its members
            let mut expanded_gc: HashSet<&str> = gc.iter().map(|s| s.as_str()).collect();
            for principal in gc {
                if let Some(members) = self.group_members.get(principal) {
                    for m in members {
                        expanded_gc.insert(m.as_str());
                    }
                }
            }

            let mut expanded_gca: HashSet<&str> = gca.iter().map(|s| s.as_str()).collect();
            for principal in gca {
                if let Some(members) = self.group_members.get(principal) {
                    for m in members {
                        expanded_gca.insert(m.as_str());
                    }
                }
            }

            // Intersect: principals with both rights get DCSync
            for principal in expanded_gc.intersection(&expanded_gca) {
                // Skip if this principal already has a direct DCSync edge
                // (those are emitted by derive_dcsync_edges during entity processing)
                if gc.contains(*principal) && gca.contains(*principal) {
                    continue;
                }
                result.push(DbEdge {
                    source: principal.to_string(),
                    target: domain_oid.clone(),
                    rel_type: "DCSync".to_string(),
                    properties: JsonValue::Null,
                    source_type: None,
                    target_type: Some("Domain".to_string()),
                });
            }
        }

        debug!(count = result.len(), "Derived deferred DCSync edges");
        result
    }

    /// Map local group name to relationship type.
    ///
    /// Returns `None` for unrecognized group names -- BH CE only creates edges
    /// for the well-known local group types, not a generic fallback.
    fn local_group_to_edge_type(&self, group_name: &str) -> Option<&'static str> {
        let upper = group_name.to_uppercase();
        if upper.contains("ADMINISTRATORS") {
            Some("AdminTo")
        } else if upper.contains("REMOTE DESKTOP") {
            Some("CanRDP")
        } else if upper.contains("REMOTE MANAGEMENT") {
            Some("CanPSRemote")
        } else if upper.contains("DISTRIBUTED COM") {
            Some("ExecuteDCOM")
        } else if upper.contains("REMOTE INTERACTIVE LOGON") {
            Some("RemoteInteractiveLogonRight")
        } else {
            None
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
    /// Map an ACE right name to its BH CE edge type.
    ///
    /// Returns `None` for unrecognized rights -- BH CE never creates generic
    /// "ACE" edges; only specifically recognized rights produce edges.
    fn ace_to_edge_type(&self, right_name: &str) -> Option<&'static str> {
        Some(match right_name {
            // Core AD permissions
            "GenericAll" => "GenericAll",
            "GenericWrite" => "GenericWrite",
            "WriteOwner" => "WriteOwner",
            "WriteDacl" => "WriteDacl",
            "Owns" => "Owns",
            "AddMember" => "AddMember",
            "AddSelf" => "AddSelf",
            "ForceChangePassword" => "ForceChangePassword",
            "AllExtendedRights" => "AllExtendedRights",
            "AddKeyCredentialLink" => "AddKeyCredentialLink",
            "AddAllowedToAct" => "AddAllowedToAct",
            "WriteSPN" => "WriteSPN",
            "WriteAccountRestrictions" => "WriteAccountRestrictions",
            // LAPS / gMSA / sMSA
            "ReadLAPSPassword" => "ReadLAPSPassword",
            "ReadGMSAPassword" => "ReadGMSAPassword",
            "SyncLAPSPassword" => "SyncLAPSPassword",
            "DumpSMSAPassword" => "DumpSMSAPassword",
            // DCSync components
            "GetChanges" => "GetChanges",
            "GetChangesAll" => "GetChangesAll",
            "GetChangesInFilteredSet" => "GetChangesInFilteredSet",
            // PKI / ADCS
            "Enroll" => "Enroll",
            "ManageCA" => "ManageCA",
            "ManageCertificates" => "ManageCertificates",
            "WritePKINameFlag" => "WritePKINameFlag",
            "WritePKIEnrollmentFlag" => "WritePKIEnrollmentFlag",
            "HostsCAService" => "HostsCAService",
            "DelegatedEnrollmentAgent" => "DelegatedEnrollmentAgent",
            _ => return None,
        })
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

    /// Flush deferred DCSync edges into the database.
    ///
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
    /// `S-1-5-21-xxx-512`). This builds a domain SID → name map from Domain
    /// nodes, then updates matching placeholders to `{DOMAIN}-{RID}` format
    /// (e.g. `CONTOSO.LOCAL-512`).
    fn resolve_orphan_names(&self) -> Result<usize, String> {
        // Step 1: Build domain SID → name map from Domain nodes
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

    fn send_progress(&self, progress: &ImportProgress) {
        let _ = self.progress_tx.send(progress.clone());
    }
}

#[cfg(all(test, feature = "crustdb"))]
mod tests {
    use super::*;
    use crate::db::crustdb::CrustDatabase;

    #[test]
    fn test_ace_to_edge_type() {
        let db = Arc::new(CrustDatabase::in_memory().unwrap());
        let (tx, _) = broadcast::channel(1);
        let importer = BloodHoundImporter::new(db, tx);

        assert_eq!(importer.ace_to_edge_type("GenericAll"), Some("GenericAll"));
        assert_eq!(importer.ace_to_edge_type("WriteDacl"), Some("WriteDacl"));
        assert_eq!(importer.ace_to_edge_type("Enroll"), Some("Enroll"));
        assert_eq!(importer.ace_to_edge_type("AddSelf"), Some("AddSelf"));
        assert_eq!(
            importer.ace_to_edge_type("Unknown"),
            None,
            "Unknown rights should return None, not generic ACE"
        );
    }

    #[test]
    fn test_local_group_to_edge_type() {
        let db = Arc::new(CrustDatabase::in_memory().unwrap());
        let (tx, _) = broadcast::channel(1);
        let importer = BloodHoundImporter::new(db, tx);

        assert_eq!(
            importer.local_group_to_edge_type("Administrators"),
            Some("AdminTo")
        );
        assert_eq!(
            importer.local_group_to_edge_type("Remote Desktop Users"),
            Some("CanRDP")
        );
        assert_eq!(
            importer.local_group_to_edge_type("Remote Interactive Logon"),
            Some("RemoteInteractiveLogonRight")
        );
        assert_eq!(importer.local_group_to_edge_type("Unknown Group"), None,);
    }

    /// Helper to create an importer for testing
    fn test_importer() -> BloodHoundImporter {
        let db = Arc::new(CrustDatabase::in_memory().unwrap());
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
        assert_eq!(node.name, "testuser@corp.local");
        assert_eq!(node.label, "User");
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
        assert_eq!(node.name, "DC01.corp.local");
        assert_eq!(node.label, "Computer");
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
        assert_eq!(node.label, "Group");
        assert_eq!(node.name, "Domain Admins");
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
        assert_eq!(node.name, "S-1-5-21-1234-USER");
    }

    #[test]
    fn test_extract_node_expands_uac_flags() {
        let importer = test_importer();

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
        let importer = test_importer();

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
        let importer = test_importer();

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
        let importer = test_importer();

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
    // Tier Assignment Tests
    // ========================================================================

    #[test]
    fn test_extract_node_marks_domain_admins_tier_zero() {
        let importer = test_importer();

        // Domain Admins group (SID ends with -512)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-512",
            "Properties": {
                "name": "DOMAIN ADMINS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["tier"], 0);
    }

    #[test]
    fn test_extract_node_marks_enterprise_admins_tier_zero() {
        let importer = test_importer();

        // Enterprise Admins group (SID ends with -519)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-519",
            "Properties": {
                "name": "ENTERPRISE ADMINS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["tier"], 0);
    }

    #[test]
    fn test_extract_node_marks_builtin_administrators_tier_zero() {
        let importer = test_importer();

        // Builtin Administrators (SID ends with -544)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-32-544",
            "Properties": {
                "name": "ADMINISTRATORS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["tier"], 0);
    }

    #[test]
    fn test_extract_node_marks_enterprise_domain_controllers_tier_zero() {
        let importer = test_importer();

        // Enterprise Domain Controllers (SID ends with -S-1-5-9)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-S-1-5-9",
            "Properties": {
                "name": "ENTERPRISE DOMAIN CONTROLLERS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["tier"], 0);
    }

    #[test]
    fn test_extract_node_marks_domain_tier_zero() {
        let importer = test_importer();

        // Domain objects should be tier 0
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890",
            "Properties": {
                "name": "CORP.LOCAL"
            }
        });

        let node = importer.extract_node("domains", &entity).unwrap();
        assert_eq!(node.properties["tier"], 0);
    }

    #[test]
    fn test_extract_node_regular_user_default_tier() {
        let importer = test_importer();

        // Regular user should NOT have tier set (defaults to 3 at query time)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-1001",
            "Properties": {
                "name": "regularuser@corp.local"
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();
        assert!(node.properties.get("tier").is_none());
    }

    #[test]
    fn test_extract_node_marks_domain_computers_tier_two() {
        let importer = test_importer();

        // Domain Computers group (SID ends with -515)
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-515",
            "Properties": {
                "name": "DOMAIN COMPUTERS@CORP.LOCAL"
            }
        });

        let node = importer.extract_node("groups", &entity).unwrap();
        assert_eq!(node.properties["tier"], 2);
    }

    #[test]
    fn test_extract_node_preserves_existing_tier() {
        let importer = test_importer();

        // If tier is already set, preserve it
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234567890-1001",
            "Properties": {
                "name": "specialuser@corp.local",
                "tier": 1
            }
        });

        let node = importer.extract_node("users", &entity).unwrap();
        assert_eq!(node.properties["tier"], 1);
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
        // Direction: Computer -> User
        assert!(relationships
            .iter()
            .all(|e| e.rel_type == "HasSession" && e.source == "S-1-5-21-COMP1"));
        assert!(relationships.iter().any(|e| e.target == "S-1-5-21-USER1"));
        assert!(relationships.iter().any(|e| e.target == "S-1-5-21-ADMIN1"));
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
        let trusts: Vec<_> = relationships
            .iter()
            .filter(|e| e.rel_type.ends_with("Trust"))
            .collect();

        // Bidirectional trust creates 2 trust relationships
        assert_eq!(trusts.len(), 2);
        assert!(trusts
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN2" && e.target == "S-1-5-21-DOMAIN1"));
        assert!(trusts
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN1" && e.target == "S-1-5-21-DOMAIN2"));
        // No TrustType -> defaults to CrossForestTrust
        assert!(trusts.iter().all(|e| e.rel_type == "CrossForestTrust"));
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
                    "TrustDirection": "Bidirectional",
                    "TrustType": "ParentChild"
                },
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN3",
                    "TrustDirection": "Outbound",
                    "TrustType": "External"
                },
                {
                    "TargetDomainSid": "S-1-5-21-DOMAIN4",
                    "TrustDirection": "Inbound",
                    "TrustType": "ParentChild"
                }
            ]
        });

        let relationships = importer.extract_edges("domains", &entity);
        let trusts: Vec<_> = relationships
            .iter()
            .filter(|e| e.rel_type.ends_with("Trust"))
            .collect();

        // Bidirectional creates 2, Outbound creates 1, Inbound creates 1 = 4 trust edges
        assert_eq!(trusts.len(), 4);

        // Bidirectional with DOMAIN2 (ParentChild = SameForestTrust)
        assert!(trusts.iter().any(|e| e.source == "S-1-5-21-DOMAIN2"
            && e.target == "S-1-5-21-DOMAIN1"
            && e.rel_type == "SameForestTrust"));
        assert!(trusts.iter().any(|e| e.source == "S-1-5-21-DOMAIN1"
            && e.target == "S-1-5-21-DOMAIN2"
            && e.rel_type == "SameForestTrust"));

        // Outbound to DOMAIN3 (External = CrossForestTrust): we trust them
        assert!(trusts.iter().any(|e| e.source == "S-1-5-21-DOMAIN1"
            && e.target == "S-1-5-21-DOMAIN3"
            && e.rel_type == "CrossForestTrust"));

        // Inbound from DOMAIN4 (ParentChild = SameForestTrust): they trust us
        assert!(trusts.iter().any(|e| e.source == "S-1-5-21-DOMAIN4"
            && e.target == "S-1-5-21-DOMAIN1"
            && e.rel_type == "SameForestTrust"));
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

    #[test]
    fn test_import_deduplicates_edges() {
        let mut importer = test_importer();

        // Import a group with two members that reference the same user,
        // which produces duplicate MemberOf edges.
        let json_content = serde_json::json!({
            "meta": {"type": "groups"},
            "data": [
                {
                    "ObjectIdentifier": "S-1-5-21-GROUP1",
                    "Properties": {"name": "group1"},
                    "Members": [
                        {"ObjectIdentifier": "S-1-5-21-USER1", "ObjectType": "User"},
                        {"ObjectIdentifier": "S-1-5-21-USER1", "ObjectType": "User"}
                    ]
                }
            ]
        });

        let mut progress = ImportProgress::new("test".to_string());
        importer
            .import_json_str(&json_content.to_string(), &mut progress)
            .unwrap();

        // Should only have 1 edge due to deduplication
        let (_, edge_count) = importer.db.get_stats().unwrap();
        assert_eq!(edge_count, 1);
    }

    // ========================================================================
    // BH CE Compatibility Tests
    //
    // These tests verify that admapper produces the same edges as BloodHound
    // CE for the same input data.  Each test targets a specific edge type
    // where cross-backend comparison revealed discrepancies.
    // ========================================================================

    /// BH CE creates HasSession edges with direction Computer -> User.
    /// A session means "this computer has an active session for this user".
    #[test]
    fn test_bhce_has_session_direction() {
        let mut importer = test_importer();

        let computer = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP1",
            "Sessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-USER1", "ComputerSID": "S-1-5-21-COMP1"}
                ],
                "Collected": true
            },
            "PrivilegedSessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-ADMIN1", "ComputerSID": "S-1-5-21-COMP1"}
                ],
                "Collected": true
            },
            "RegistrySessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-USER2", "ComputerSID": "S-1-5-21-COMP1"}
                ],
                "Collected": true
            }
        });

        let edges = importer.extract_edges("computers", &computer);
        let sessions: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "HasSession")
            .collect();

        assert_eq!(sessions.len(), 3);
        // BH CE direction: Computer (source) -> User (target)
        for edge in &sessions {
            assert_eq!(
                edge.source, "S-1-5-21-COMP1",
                "HasSession source should be the computer, got {}",
                edge.source,
            );
        }
        assert!(sessions.iter().any(|e| e.target == "S-1-5-21-USER1"));
        assert!(sessions.iter().any(|e| e.target == "S-1-5-21-ADMIN1"));
        assert!(sessions.iter().any(|e| e.target == "S-1-5-21-USER2"));
    }

    /// BH CE creates GPLink edges with direction GPO -> OU/Domain.
    /// "This GPO is linked to this OU/Domain."
    #[test]
    fn test_bhce_gplink_direction() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Links": [
                {"GUID": "GPO-GUID-1", "IsEnforced": false},
                {"GUID": "GPO-GUID-2", "IsEnforced": true}
            ]
        });

        let edges = importer.extract_edges("domains", &domain);
        let gplinks: Vec<_> = edges.iter().filter(|e| e.rel_type == "GPLink").collect();

        assert_eq!(gplinks.len(), 2);
        // BH CE direction: GPO (source) -> OU/Domain (target)
        for edge in &gplinks {
            assert_eq!(
                edge.target, "S-1-5-21-DOMAIN1",
                "GPLink target should be the domain/OU, got {}",
                edge.target,
            );
        }
        assert!(gplinks.iter().any(|e| e.source == "GPO-GUID-1"));
        assert!(gplinks.iter().any(|e| e.source == "GPO-GUID-2"));
    }

    /// BH CE creates MemberOf edges from PrimaryGroupSID for every
    /// user and computer.  A user with PrimaryGroupSID pointing to
    /// Domain Users (-513) should get a MemberOf edge to that group.
    #[test]
    fn test_bhce_primary_group_creates_memberof() {
        let mut importer = test_importer();

        let user = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-1001",
            "PrimaryGroupSID": "S-1-5-21-1234-513",
            "Properties": {"name": "jdoe@corp.local"}
        });

        let edges = importer.extract_edges("users", &user);
        let memberof: Vec<_> = edges.iter().filter(|e| e.rel_type == "MemberOf").collect();

        assert_eq!(
            memberof.len(),
            1,
            "PrimaryGroupSID should produce a MemberOf edge"
        );
        assert_eq!(memberof[0].source, "S-1-5-21-1234-1001");
        assert_eq!(memberof[0].target, "S-1-5-21-1234-513");
    }

    /// BH CE creates MemberOf edges from PrimaryGroupSID for computers
    /// too (typically pointing to Domain Computers, RID -515).
    #[test]
    fn test_bhce_primary_group_computer() {
        let mut importer = test_importer();

        let computer = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-1103",
            "PrimaryGroupSID": "S-1-5-21-1234-515",
            "Properties": {"name": "DC01.corp.local"}
        });

        let edges = importer.extract_edges("computers", &computer);
        let memberof: Vec<_> = edges.iter().filter(|e| e.rel_type == "MemberOf").collect();

        assert_eq!(
            memberof.len(),
            1,
            "Computer PrimaryGroupSID should produce a MemberOf edge"
        );
        assert_eq!(memberof[0].source, "S-1-5-21-1234-1103");
        assert_eq!(memberof[0].target, "S-1-5-21-1234-515");
    }

    /// BH CE derives a DCSync edge when a principal has both GetChanges
    /// AND GetChangesAll ACEs on a domain object.  The individual ACE
    /// edges should still be created too.
    #[test]
    fn test_bhce_dcsync_derived_edge() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Aces": [
                {
                    "PrincipalSID": "S-1-5-21-ATTACKER",
                    "RightName": "GetChanges",
                    "IsInherited": false
                },
                {
                    "PrincipalSID": "S-1-5-21-ATTACKER",
                    "RightName": "GetChangesAll",
                    "IsInherited": false
                }
            ]
        });

        let edges = importer.extract_edges("domains", &domain);

        // Should have GetChanges, GetChangesAll, AND a derived DCSync edge
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-ATTACKER"
                && e.target == "S-1-5-21-DOMAIN1"
                && e.rel_type == "GetChanges"),
            "Should have GetChanges edge"
        );
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-ATTACKER"
                && e.target == "S-1-5-21-DOMAIN1"
                && e.rel_type == "GetChangesAll"),
            "Should have GetChangesAll edge"
        );
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-ATTACKER"
                && e.target == "S-1-5-21-DOMAIN1"
                && e.rel_type == "DCSync"),
            "Should have derived DCSync edge when both GetChanges + GetChangesAll exist"
        );
    }

    /// DCSync should NOT be created when only one of GetChanges /
    /// GetChangesAll is present.
    #[test]
    fn test_bhce_no_dcsync_without_both_rights() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN1",
            "Aces": [
                {
                    "PrincipalSID": "S-1-5-21-USER1",
                    "RightName": "GetChanges",
                    "IsInherited": false
                }
            ]
        });

        let edges = importer.extract_edges("domains", &domain);

        assert!(
            edges.iter().any(|e| e.rel_type == "GetChanges"),
            "Should have GetChanges edge"
        );
        assert!(
            !edges.iter().any(|e| e.rel_type == "DCSync"),
            "Should NOT have DCSync when only GetChanges is present"
        );
    }

    /// BH CE uses SameForestTrust / CrossForestTrust instead of generic
    /// TrustedBy.  A ParentChild trust (intra-forest) should produce
    /// SameForestTrust edges.
    #[test]
    fn test_bhce_trust_types_intra_forest() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-PARENT",
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-CHILD",
                    "TargetDomainName": "CHILD.CORP.LOCAL",
                    "TrustDirection": "Bidirectional",
                    "TrustType": "ParentChild",
                    "IsTransitive": true,
                    "SidFilteringEnabled": false
                }
            ]
        });

        let edges = importer.extract_edges("domains", &domain);
        let trust_edges: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.rel_type == "TrustedBy"
                    || e.rel_type == "SameForestTrust"
                    || e.rel_type == "CrossForestTrust"
            })
            .collect();

        // Bidirectional trust should create 2 edges
        assert_eq!(trust_edges.len(), 2);
        // Intra-forest (ParentChild) trusts should use SameForestTrust
        for edge in &trust_edges {
            assert_eq!(
                edge.rel_type, "SameForestTrust",
                "ParentChild trust should produce SameForestTrust, got {}",
                edge.rel_type,
            );
        }
    }

    /// Cross-forest (External) trusts should produce CrossForestTrust edges.
    #[test]
    fn test_bhce_trust_types_cross_forest() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-CORP",
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-PARTNER",
                    "TargetDomainName": "PARTNER.COM",
                    "TrustDirection": "Outbound",
                    "TrustType": "External",
                    "IsTransitive": false,
                    "SidFilteringEnabled": true
                }
            ]
        });

        let edges = importer.extract_edges("domains", &domain);
        let trust_edges: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.rel_type == "TrustedBy"
                    || e.rel_type == "SameForestTrust"
                    || e.rel_type == "CrossForestTrust"
            })
            .collect();

        assert_eq!(trust_edges.len(), 1);
        assert_eq!(
            trust_edges[0].rel_type, "CrossForestTrust",
            "External trust should produce CrossForestTrust, got {}",
            trust_edges[0].rel_type,
        );
    }

    /// Full import: a computer with sessions, local groups, and ACEs
    /// should produce all expected edge types.
    #[test]
    fn test_bhce_computer_full_edges() {
        let mut importer = test_importer();

        let computer = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234-1103",
            "PrimaryGroupSID": "S-1-5-21-1234-515",
            "Properties": {"name": "WS01.corp.local"},
            "Sessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-1234-1001", "ComputerSID": "S-1-5-21-1234-1103"}
                ],
                "Collected": true
            },
            "PrivilegedSessions": {"Results": [], "Collected": true},
            "RegistrySessions": {"Results": [], "Collected": true},
            "LocalGroups": [
                {
                    "Name": "Administrators",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-1234-512", "ObjectType": "Group"}
                    ]
                },
                {
                    "Name": "Remote Desktop Users",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-1234-513", "ObjectType": "Group"}
                    ]
                }
            ],
            "Aces": [
                {
                    "PrincipalSID": "S-1-5-21-1234-512",
                    "RightName": "GenericAll",
                    "IsInherited": false
                }
            ],
            "ContainedBy": {
                "ObjectIdentifier": "OU-GUID-1",
                "ObjectType": "OU"
            }
        });

        let edges = importer.extract_edges("computers", &computer);
        let types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();

        // Must have MemberOf from PrimaryGroupSID
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-1234-1103"
                && e.target == "S-1-5-21-1234-515"
                && e.rel_type == "MemberOf"),
            "Missing MemberOf from PrimaryGroupSID; edge types: {:?}",
            types,
        );

        // Must have HasSession (Computer -> User direction)
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-1234-1103"
                && e.target == "S-1-5-21-1234-1001"
                && e.rel_type == "HasSession"),
            "Missing or reversed HasSession; edge types: {:?}",
            types,
        );

        // Must have AdminTo
        assert!(
            edges
                .iter()
                .any(|e| e.rel_type == "AdminTo" && e.target == "S-1-5-21-1234-1103"),
            "Missing AdminTo; edge types: {:?}",
            types,
        );

        // Must have CanRDP
        assert!(
            edges
                .iter()
                .any(|e| e.rel_type == "CanRDP" && e.target == "S-1-5-21-1234-1103"),
            "Missing CanRDP; edge types: {:?}",
            types,
        );

        // Must have GenericAll ACE
        assert!(
            edges.iter().any(|e| e.rel_type == "GenericAll"),
            "Missing GenericAll ACE; edge types: {:?}",
            types,
        );

        // Must have Contains (from ContainedBy)
        assert!(
            edges.iter().any(|e| e.source == "OU-GUID-1"
                && e.target == "S-1-5-21-1234-1103"
                && e.rel_type == "Contains"),
            "Missing Contains; edge types: {:?}",
            types,
        );
    }

    /// Full edge extraction for a domain with trusts, GPLinks, and
    /// DCSync-capable ACEs.
    #[test]
    fn test_bhce_domain_full_edges() {
        let mut importer = test_importer();

        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-1234",
            "Properties": {
                "name": "CORP.LOCAL",
                "domainsid": "S-1-5-21-1234",
                "functionallevel": "2016"
            },
            "Links": [
                {"GUID": "GPO-DEFAULT", "IsEnforced": false}
            ],
            "Trusts": [
                {
                    "TargetDomainSid": "S-1-5-21-5678",
                    "TargetDomainName": "CHILD.CORP.LOCAL",
                    "TrustDirection": "Bidirectional",
                    "TrustType": "ParentChild",
                    "IsTransitive": true,
                    "SidFilteringEnabled": false
                }
            ],
            "Aces": [
                {
                    "PrincipalSID": "S-1-5-21-1234-512",
                    "RightName": "GetChanges",
                    "IsInherited": false
                },
                {
                    "PrincipalSID": "S-1-5-21-1234-512",
                    "RightName": "GetChangesAll",
                    "IsInherited": false
                }
            ]
        });

        let edges = importer.extract_edges("domains", &domain);
        let types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();

        // GPLink: GPO -> Domain
        assert!(
            edges.iter().any(|e| e.source == "GPO-DEFAULT"
                && e.target == "S-1-5-21-1234"
                && e.rel_type == "GPLink"),
            "GPLink should point from GPO to Domain; edge types: {:?}",
            types,
        );

        // Trust: should be SameForestTrust, not TrustedBy
        let trust_edges: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.rel_type == "SameForestTrust"
                    || e.rel_type == "CrossForestTrust"
                    || e.rel_type == "TrustedBy"
            })
            .collect();
        assert_eq!(trust_edges.len(), 2, "Bidirectional trust = 2 edges");
        for edge in &trust_edges {
            assert_ne!(
                edge.rel_type, "TrustedBy",
                "Should use SameForestTrust/CrossForestTrust, not TrustedBy"
            );
        }

        // DCSync: derived from GetChanges + GetChangesAll
        assert!(
            edges.iter().any(|e| e.source == "S-1-5-21-1234-512"
                && e.target == "S-1-5-21-1234"
                && e.rel_type == "DCSync"),
            "Missing derived DCSync edge; edge types: {:?}",
            types,
        );
    }

    // ========================================================================
    // BH CE Compatibility: ACE Right Name Mapping (covers 319-edge gap)
    // ========================================================================

    #[test]
    fn test_bhce_ace_pki_rights() {
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "CERT-TEMPLATE-1",
            "Aces": [
                {"PrincipalSID": "S-1-5-21-USER-1", "RightName": "Enroll", "IsInherited": false, "PrincipalType": "Group"},
                {"PrincipalSID": "S-1-5-21-USER-2", "RightName": "ManageCA", "IsInherited": false, "PrincipalType": "User"},
                {"PrincipalSID": "S-1-5-21-USER-3", "RightName": "ManageCertificates", "IsInherited": false, "PrincipalType": "User"},
            ]
        });
        let edges = importer.extract_edges("certtemplate", &entity);
        assert!(
            edges
                .iter()
                .any(|e| e.source == "S-1-5-21-USER-1" && e.rel_type == "Enroll"),
            "Enroll ACE right must map to Enroll edge type, not generic ACE"
        );
        assert!(
            edges
                .iter()
                .any(|e| e.source == "S-1-5-21-USER-2" && e.rel_type == "ManageCA"),
            "ManageCA ACE right must map to ManageCA edge type"
        );
        assert!(
            edges
                .iter()
                .any(|e| e.source == "S-1-5-21-USER-3" && e.rel_type == "ManageCertificates"),
            "ManageCertificates ACE right must map to ManageCertificates edge type"
        );
        assert!(
            !edges.iter().any(|e| e.rel_type == "ACE"),
            "No generic ACE edges should exist when right names are recognized"
        );
    }

    #[test]
    fn test_bhce_ace_pki_write_flags() {
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "CERT-TEMPLATE-2",
            "Aces": [
                {"PrincipalSID": "S-1-5-21-USER-1", "RightName": "WritePKINameFlag", "IsInherited": false, "PrincipalType": "User"},
                {"PrincipalSID": "S-1-5-21-USER-2", "RightName": "WritePKIEnrollmentFlag", "IsInherited": false, "PrincipalType": "User"},
                {"PrincipalSID": "S-1-5-21-USER-3", "RightName": "HostsCAService", "IsInherited": false, "PrincipalType": "Computer"},
                {"PrincipalSID": "S-1-5-21-USER-4", "RightName": "DelegatedEnrollmentAgent", "IsInherited": false, "PrincipalType": "User"},
            ]
        });
        let edges = importer.extract_edges("certtemplate", &entity);

        let types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(
            types.contains(&"WritePKINameFlag"),
            "Missing WritePKINameFlag; got: {types:?}"
        );
        assert!(
            types.contains(&"WritePKIEnrollmentFlag"),
            "Missing WritePKIEnrollmentFlag; got: {types:?}"
        );
        assert!(
            types.contains(&"HostsCAService"),
            "Missing HostsCAService; got: {types:?}"
        );
        assert!(
            types.contains(&"DelegatedEnrollmentAgent"),
            "Missing DelegatedEnrollmentAgent; got: {types:?}"
        );
        assert!(
            !types.contains(&"ACE"),
            "No generic ACE edges when rights are recognized; got: {types:?}"
        );
    }

    #[test]
    fn test_bhce_ace_additional_security_rights() {
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-GROUP-1",
            "Aces": [
                {"PrincipalSID": "S-1-5-21-USER-1", "RightName": "AddSelf", "IsInherited": false, "PrincipalType": "User"},
                {"PrincipalSID": "S-1-5-21-COMP-1", "RightName": "SyncLAPSPassword", "IsInherited": false, "PrincipalType": "Group"},
                {"PrincipalSID": "S-1-5-21-COMP-2", "RightName": "DumpSMSAPassword", "IsInherited": false, "PrincipalType": "Computer"},
            ]
        });
        let edges = importer.extract_edges("groups", &entity);

        let types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(
            types.contains(&"AddSelf"),
            "Missing AddSelf; got: {types:?}"
        );
        assert!(
            types.contains(&"SyncLAPSPassword"),
            "Missing SyncLAPSPassword; got: {types:?}"
        );
        assert!(
            types.contains(&"DumpSMSAPassword"),
            "Missing DumpSMSAPassword; got: {types:?}"
        );
        assert!(
            !types.contains(&"ACE"),
            "No generic ACE edges when rights are recognized; got: {types:?}"
        );
    }

    #[test]
    fn test_bhce_ace_self_referencing_filtered() {
        let mut importer = test_importer();
        // ACE where PrincipalSID == ObjectIdentifier (self-referencing) should be dropped.
        // BH CE never creates edges from a node to itself.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-SELF-1",
            "Aces": [
                {"PrincipalSID": "S-1-5-21-SELF-1", "RightName": "GenericWrite", "IsInherited": false, "PrincipalType": "Group"},
                {"PrincipalSID": "S-1-5-21-SELF-1", "RightName": "WriteDacl", "IsInherited": false, "PrincipalType": "Group"},
                {"PrincipalSID": "S-1-5-21-SELF-1", "RightName": "WriteOwner", "IsInherited": false, "PrincipalType": "Group"},
                {"PrincipalSID": "S-1-5-21-OTHER", "RightName": "GenericWrite", "IsInherited": false, "PrincipalType": "User"},
            ]
        });
        let edges = importer.extract_edges("groups", &entity);

        // Only the non-self-referencing edge should survive
        assert_eq!(
            edges.len(),
            1,
            "Self-referencing ACE edges should be filtered; got: {:?}",
            edges
                .iter()
                .map(|e| (&e.source, &e.rel_type, &e.target))
                .collect::<Vec<_>>()
        );
        assert_eq!(edges[0].source, "S-1-5-21-OTHER");
    }

    #[test]
    fn test_bhce_no_local_group_member_fallback() {
        let mut importer = test_importer();
        // BH CE only creates edges for recognized local group names (Administrators,
        // Remote Desktop Users, etc.).  Unrecognized names should not produce a
        // generic LocalGroupMember edge.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP-1",
            "LocalGroups": [
                {
                    "Name": "Administrators",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-ADMIN", "ObjectType": "User"}
                    ]
                },
                {
                    "Name": "Some Obscure Group",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-MEMBER", "ObjectType": "User"}
                    ]
                }
            ]
        });
        let edges = importer.extract_edges("computers", &entity);

        let admin_edges: Vec<_> = edges.iter().filter(|e| e.rel_type == "AdminTo").collect();
        assert_eq!(
            admin_edges.len(),
            1,
            "AdminTo edge for Administrators group"
        );

        let fallback_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "LocalGroupMember")
            .collect();
        assert_eq!(
            fallback_edges.len(),
            0,
            "Unrecognized local group names should not create LocalGroupMember edges; got: {:?}",
            fallback_edges
                .iter()
                .map(|e| (&e.source, &e.target))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bhce_session_dedup_priv_and_registry() {
        let mut importer = test_importer();
        // Same session in PrivilegedSessions AND RegistrySessions should produce
        // only ONE HasSession edge (dedup at extract_edges level via the import pipeline).
        // At the extract_edges level, both are emitted; dedup happens upstream.
        // This test verifies that at least both source correctly as Computer->User.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP-1",
            "PrivilegedSessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-USER-1", "ComputerSID": "S-1-5-21-COMP-1"}
                ]
            },
            "RegistrySessions": {
                "Results": [
                    {"UserSID": "S-1-5-21-USER-1", "ComputerSID": "S-1-5-21-COMP-1"}
                ]
            }
        });
        let edges = importer.extract_edges("computers", &entity);

        let sessions: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "HasSession")
            .collect();
        // Both produce Computer->User direction
        for sess in &sessions {
            assert_eq!(
                sess.source, "S-1-5-21-COMP-1",
                "HasSession source must be the computer"
            );
            assert_eq!(
                sess.target, "S-1-5-21-USER-1",
                "HasSession target must be the user"
            );
        }
        // extract_edges emits both; pipeline dedup keeps one.
        // Verify direction is consistent.
        assert!(sessions.len() >= 1, "At least one HasSession edge expected");
    }

    #[test]
    fn test_bhce_gplink_includes_enforced_property() {
        let mut importer = test_importer();
        // GPLink objects in SharpHound have an "IsEnforced" boolean property.
        // BH CE stores this on the edge.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-OU1",
            "Links": [
                {"GUID": "GPO-GUID-1", "IsEnforced": true},
                {"GUID": "GPO-GUID-2", "IsEnforced": false},
            ]
        });
        let edges = importer.extract_edges("ous", &entity);

        let gplinks: Vec<_> = edges.iter().filter(|e| e.rel_type == "GPLink").collect();
        assert_eq!(gplinks.len(), 2);

        let enforced_edge = gplinks.iter().find(|e| e.source == "GPO-GUID-1").unwrap();
        assert_eq!(
            enforced_edge
                .properties
                .get("enforced")
                .and_then(|v| v.as_bool()),
            Some(true),
            "GPLink edge should carry the IsEnforced property"
        );
        let non_enforced_edge = gplinks.iter().find(|e| e.source == "GPO-GUID-2").unwrap();
        assert_eq!(
            non_enforced_edge
                .properties
                .get("enforced")
                .and_then(|v| v.as_bool()),
            Some(false),
            "GPLink edge should carry the IsEnforced property"
        );
    }

    #[test]
    fn test_bhce_ace_no_unknown_fallback() {
        let mut importer = test_importer();
        // Verify that a completely unknown ACE right name (not in BH CE's vocabulary)
        // does NOT create a generic "ACE" edge. Only recognized rights produce edges.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-TARGET",
            "Aces": [
                {"PrincipalSID": "S-1-5-21-SRC1", "RightName": "GenericAll", "IsInherited": false, "PrincipalType": "User"},
                {"PrincipalSID": "S-1-5-21-SRC2", "RightName": "TotallyFakeRight", "IsInherited": false, "PrincipalType": "User"},
            ]
        });
        let edges = importer.extract_edges("groups", &entity);

        // GenericAll should exist
        assert!(
            edges.iter().any(|e| e.rel_type == "GenericAll"),
            "GenericAll should be recognized"
        );
        // Unknown right should NOT produce a generic ACE edge
        assert!(
            !edges.iter().any(|e| e.rel_type == "ACE"),
            "Unknown ACE right names should be dropped, not create generic ACE edges; got: {:?}",
            edges
                .iter()
                .map(|e| (&e.source, &e.rel_type))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bhce_local_group_remote_interactive_logon() {
        let mut importer = test_importer();
        // BH CE recognizes "Remote Interactive Logon" as a local group name and
        // creates RemoteInteractiveLogonRight edges. Our code should do the same.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP-1",
            "LocalGroups": [
                {
                    "Name": "Remote Interactive Logon",
                    "Results": [
                        {"ObjectIdentifier": "S-1-5-21-USER-1", "ObjectType": "User"},
                    ]
                }
            ]
        });
        let edges = importer.extract_edges("computers", &entity);

        let ril_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "RemoteInteractiveLogonRight")
            .collect();
        assert_eq!(
            ril_edges.len(),
            1,
            "Remote Interactive Logon group should create RemoteInteractiveLogonRight edge; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
    }

    // ========================================================================
    // BH CE Compatibility: PKI / ADCS Edge Extraction
    // ========================================================================

    #[test]
    fn test_bhce_enterprise_ca_published_to() {
        let mut importer = test_importer();
        // BH CE creates PublishedTo edges from EnabledCertTemplates on Enterprise CAs.
        // Each enabled template gets a (Template)-[PublishedTo]->(CA) edge.
        let entity = serde_json::json!({
            "ObjectIdentifier": "CA-GUID-1",
            "EnabledCertTemplates": [
                {"ObjectIdentifier": "TMPL-1", "ObjectType": "CertTemplate"},
                {"ObjectIdentifier": "TMPL-2", "ObjectType": "CertTemplate"},
            ],
            "Properties": {"name": "MY-CA@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        let published: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "PublishedTo")
            .collect();
        assert_eq!(
            published.len(),
            2,
            "Each EnabledCertTemplate should create a PublishedTo edge; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert!(published.iter().all(|e| e.target == "CA-GUID-1"));
        assert!(published.iter().any(|e| e.source == "TMPL-1"));
        assert!(published.iter().any(|e| e.source == "TMPL-2"));
    }

    #[test]
    fn test_bhce_enterprise_ca_hosts_ca_service() {
        let mut importer = test_importer();
        // BH CE creates a HostsCAService edge from the hosting computer to the CA.
        let entity = serde_json::json!({
            "ObjectIdentifier": "CA-GUID-1",
            "HostingComputer": "S-1-5-21-COMP-DC",
            "Properties": {"name": "MY-CA@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        let hosts: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "HostsCAService")
            .collect();
        assert_eq!(
            hosts.len(),
            1,
            "HostingComputer should create a HostsCAService edge; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert_eq!(hosts[0].source, "S-1-5-21-COMP-DC");
        assert_eq!(hosts[0].target, "CA-GUID-1");
    }

    #[test]
    fn test_bhce_enterprise_ca_registry_aces() {
        let mut importer = test_importer();
        // BH CE processes CARegistryData.CASecurity ACEs for ManageCA/ManageCertificates.
        // These are CA-specific ACEs, separate from the entity's top-level Aces array.
        let entity = serde_json::json!({
            "ObjectIdentifier": "CA-GUID-1",
            "CARegistryData": {
                "CASecurity": {
                    "Data": [
                        {"PrincipalSID": "S-1-5-21-ADMIN", "PrincipalType": "Group", "RightName": "ManageCA", "IsInherited": false},
                        {"PrincipalSID": "S-1-5-21-USER1", "PrincipalType": "User", "RightName": "ManageCertificates", "IsInherited": false},
                        {"PrincipalSID": "S-1-5-21-GROUP1", "PrincipalType": "Group", "RightName": "Enroll", "IsInherited": false},
                    ]
                }
            },
            "Properties": {"name": "MY-CA@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        let types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(
            types.contains(&"ManageCA"),
            "CARegistryData ACEs should produce ManageCA edges; got: {types:?}"
        );
        assert!(
            types.contains(&"ManageCertificates"),
            "CARegistryData ACEs should produce ManageCertificates edges; got: {types:?}"
        );
        assert!(
            types.contains(&"Enroll"),
            "CARegistryData ACEs should produce Enroll edges; got: {types:?}"
        );
    }

    #[test]
    fn test_bhce_root_ca_for_domain() {
        let mut importer = test_importer();
        // BH CE creates RootCAFor edges from Root CAs to their domain.
        let entity = serde_json::json!({
            "ObjectIdentifier": "ROOTCA-GUID-1",
            "DomainSID": "S-1-5-21-DOMAIN",
            "Properties": {"name": "ROOT-CA@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("rootcas", &entity);

        let root_for: Vec<_> = edges.iter().filter(|e| e.rel_type == "RootCAFor").collect();
        assert_eq!(
            root_for.len(),
            1,
            "Root CA should create RootCAFor edge to its domain; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert_eq!(root_for[0].source, "ROOTCA-GUID-1");
        assert_eq!(root_for[0].target, "S-1-5-21-DOMAIN");
    }

    #[test]
    fn test_bhce_ntauth_store_for_domain() {
        let mut importer = test_importer();
        // BH CE creates NTAuthStoreFor edges from NTAuth stores to their domain.
        let entity = serde_json::json!({
            "ObjectIdentifier": "NTAUTH-GUID-1",
            "DomainSID": "S-1-5-21-DOMAIN",
            "Properties": {"name": "NTAUTH@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("ntauthstores", &entity);

        let nta_for: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "NTAuthStoreFor")
            .collect();
        assert_eq!(
            nta_for.len(),
            1,
            "NTAuth store should create NTAuthStoreFor edge to its domain; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert_eq!(nta_for[0].source, "NTAUTH-GUID-1");
        assert_eq!(nta_for[0].target, "S-1-5-21-DOMAIN");
    }

    #[test]
    fn test_bhce_enterprise_ca_for_domain() {
        let mut importer = test_importer();
        // BH CE creates EnterpriseCAFor edges from Enterprise CAs to their domain.
        let entity = serde_json::json!({
            "ObjectIdentifier": "CA-GUID-1",
            "Properties": {
                "name": "MY-CA@CORP.LOCAL",
                "domainsid": "S-1-5-21-DOMAIN"
            }
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        let eca_for: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "EnterpriseCAFor")
            .collect();
        assert_eq!(
            eca_for.len(),
            1,
            "Enterprise CA should create EnterpriseCAFor edge to its domain; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert_eq!(eca_for[0].source, "CA-GUID-1");
        assert_eq!(eca_for[0].target, "S-1-5-21-DOMAIN");
    }

    #[test]
    fn test_bhce_issued_signed_by() {
        let mut importer = test_importer();
        // BH CE creates IssuedSignedBy edges from Enterprise CAs to their
        // Root CA chain.  The CARegistryData contains certificate chain info.
        let entity = serde_json::json!({
            "ObjectIdentifier": "CA-GUID-1",
            "CARegistryData": {
                "CertChain": [
                    {"ObjectIdentifier": "ROOTCA-1", "ObjectType": "RootCA"},
                    {"ObjectIdentifier": "ROOTCA-2", "ObjectType": "RootCA"},
                ]
            },
            "Properties": {"name": "MY-CA@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        let issued: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "IssuedSignedBy")
            .collect();
        assert_eq!(
            issued.len(),
            2,
            "CertChain entries should create IssuedSignedBy edges; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert!(issued.iter().all(|e| e.source == "CA-GUID-1"));
    }

    #[test]
    fn test_bhce_trusted_for_ntauth() {
        let mut importer = test_importer();
        // BH CE creates TrustedForNTAuth edges from NTAuth stores referencing
        // specific Enterprise CAs.
        let entity = serde_json::json!({
            "ObjectIdentifier": "NTAUTH-GUID-1",
            "DomainSID": "S-1-5-21-DOMAIN",
            "NTAuthCertificates": [
                {"ObjectIdentifier": "CA-GUID-1", "ObjectType": "EnterpriseCA"},
                {"ObjectIdentifier": "CA-GUID-2", "ObjectType": "EnterpriseCA"},
            ],
            "Properties": {"name": "NTAUTH@CORP.LOCAL"}
        });
        let edges = importer.extract_edges("ntauthstores", &entity);

        let trusted: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "TrustedForNTAuth")
            .collect();
        assert_eq!(
            trusted.len(),
            2,
            "NTAuthCertificates should create TrustedForNTAuth edges; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert!(trusted.iter().all(|e| e.target == "NTAUTH-GUID-1"));
    }

    #[test]
    fn test_bhce_computer_coerce_to_tgt() {
        let mut importer = test_importer();
        // BH CE creates CoerceToTGT edges from computers with unconstrained
        // delegation (unconstraineddelegation=true) to their domain.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP-1",
            "Properties": {
                "name": "DC01.CORP.LOCAL",
                "domainsid": "S-1-5-21-DOMAIN",
                "unconstraineddelegation": true
            }
        });
        let edges = importer.extract_edges("computers", &entity);

        let coerce: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "CoerceToTGT")
            .collect();
        assert_eq!(
            coerce.len(),
            1,
            "Computer with unconstrained delegation should create CoerceToTGT to domain; got: {:?}",
            edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
        );
        assert_eq!(coerce[0].source, "S-1-5-21-COMP-1");
        assert_eq!(coerce[0].target, "S-1-5-21-DOMAIN");
    }

    #[test]
    fn test_bhce_computer_no_coerce_without_delegation() {
        let mut importer = test_importer();
        // Computer WITHOUT unconstrained delegation should NOT get CoerceToTGT.
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-COMP-2",
            "Properties": {
                "name": "SRV01.CORP.LOCAL",
                "domainsid": "S-1-5-21-DOMAIN",
                "unconstraineddelegation": false
            }
        });
        let edges = importer.extract_edges("computers", &entity);

        assert!(
            !edges.iter().any(|e| e.rel_type == "CoerceToTGT"),
            "Computer without unconstrained delegation should not have CoerceToTGT"
        );
    }

    // ========================================================================
    // BH CE Parity: Trust direction (CrossForestTrust / SameForestTrust)
    // ========================================================================

    #[test]
    fn test_bhce_inbound_trust_direction() {
        // Inbound trust: the OTHER domain trusts THIS domain.
        // BH CE convention: edge from trusting -> trusted.
        // So: target_domain -> this_domain.
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-PHANTOM",
            "Trusts": [{
                "TargetDomainSid": "S-1-5-21-REVENANT",
                "TargetDomainName": "REVENANT.CORP",
                "TrustDirection": "Inbound",
                "TrustType": "External"
            }]
        });
        let edges = importer.extract_edges("domains", &entity);
        let trust: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "CrossForestTrust")
            .collect();
        assert_eq!(trust.len(), 1);
        assert_eq!(trust[0].source, "S-1-5-21-REVENANT");
        assert_eq!(trust[0].target, "S-1-5-21-PHANTOM");
    }

    #[test]
    fn test_bhce_outbound_trust_direction() {
        // Outbound trust: THIS domain trusts the OTHER domain.
        // BH CE convention: edge from trusting -> trusted.
        // So: this_domain -> target_domain.
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-PHANTOM",
            "Trusts": [{
                "TargetDomainSid": "S-1-5-21-WRAITH",
                "TargetDomainName": "WRAITH.CORP",
                "TrustDirection": "Outbound",
                "TrustType": "External"
            }]
        });
        let edges = importer.extract_edges("domains", &entity);
        let trust: Vec<_> = edges
            .iter()
            .filter(|e| e.rel_type == "CrossForestTrust")
            .collect();
        assert_eq!(trust.len(), 1);
        assert_eq!(trust[0].source, "S-1-5-21-PHANTOM");
        assert_eq!(trust[0].target, "S-1-5-21-WRAITH");
    }

    // ========================================================================
    // BH CE Parity: Well-known implicit MemberOf edges
    // ========================================================================

    #[test]
    fn test_bhce_wellknown_memberof_domain() {
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN",
            "Properties": {
                "name": "TEST.CORP",
                "domainsid": "S-1-5-21-DOMAIN"
            }
        });
        let edges = importer.extract_edges("domains", &entity);
        let memberof: Vec<_> = edges.iter().filter(|e| e.rel_type == "MemberOf").collect();

        // Guest -> Everyone
        assert!(
            memberof.iter().any(|e| e.source == "S-1-5-21-DOMAIN-501"
                && e.target == "S-1-5-21-DOMAIN-S-1-1-0"),
            "Guest should be MemberOf Everyone"
        );
        // Domain Users -> Authenticated Users
        assert!(
            memberof.iter().any(
                |e| e.source == "S-1-5-21-DOMAIN-513" && e.target == "S-1-5-21-DOMAIN-S-1-5-11"
            ),
            "Domain Users should be MemberOf Authenticated Users"
        );
        // Domain Computers -> Authenticated Users
        assert!(
            memberof.iter().any(
                |e| e.source == "S-1-5-21-DOMAIN-515" && e.target == "S-1-5-21-DOMAIN-S-1-5-11"
            ),
            "Domain Computers should be MemberOf Authenticated Users"
        );
        // Authenticated Users -> Everyone
        assert!(
            memberof
                .iter()
                .any(|e| e.source == "S-1-5-21-DOMAIN-S-1-5-11"
                    && e.target == "S-1-5-21-DOMAIN-S-1-1-0"),
            "Authenticated Users should be MemberOf Everyone"
        );

        assert_eq!(
            memberof.len(),
            4,
            "Expected exactly 4 well-known MemberOf edges; got: {:?}",
            memberof
                .iter()
                .map(|e| format!("{} -> {}", e.source, e.target))
                .collect::<Vec<_>>()
        );
    }

    // ========================================================================
    // BH CE Parity: DCSync through group membership
    // ========================================================================

    #[test]
    fn test_bhce_dcsync_through_group_membership() {
        let mut importer = test_importer();

        // Domain with GetChanges on group-A and GetChangesAll on group-B
        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN",
            "Properties": { "name": "TEST.CORP", "domainsid": "S-1-5-21-DOMAIN" },
            "Aces": [
                { "PrincipalSID": "S-1-5-21-DOMAIN-GROUP-A", "RightName": "GetChanges", "IsInherited": false, "PrincipalType": "Group" },
                { "PrincipalSID": "S-1-5-21-DOMAIN-GROUP-B", "RightName": "GetChangesAll", "IsInherited": false, "PrincipalType": "Group" }
            ]
        });
        importer.extract_edges("domains", &domain);

        // Group A has USER1 as member
        let group_a = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN-GROUP-A",
            "Members": [{ "ObjectIdentifier": "S-1-5-21-DOMAIN-USER1", "ObjectType": "User" }]
        });
        importer.extract_edges("groups", &group_a);

        // Group B also has USER1 as member
        let group_b = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN-GROUP-B",
            "Members": [{ "ObjectIdentifier": "S-1-5-21-DOMAIN-USER1", "ObjectType": "User" }]
        });
        importer.extract_edges("groups", &group_b);

        let deferred = importer.derive_deferred_dcsync();

        assert!(
            deferred.iter().any(|e| e.source == "S-1-5-21-DOMAIN-USER1"
                && e.target == "S-1-5-21-DOMAIN"
                && e.rel_type == "DCSync"),
            "User member of both GetChanges group and GetChangesAll group should get DCSync; got: {:?}",
            deferred
        );
    }

    #[test]
    fn test_bhce_dcsync_dc_via_primary_group() {
        let mut importer = test_importer();

        // Domain with Enterprise Domain Controllers (S-1-5-9) having GetChanges
        // and Domain Controllers (-516) having GetChangesAll
        let domain = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN",
            "Properties": { "name": "TEST.CORP", "domainsid": "S-1-5-21-DOMAIN" },
            "Aces": [
                { "PrincipalSID": "TEST.CORP-S-1-5-9", "RightName": "GetChanges", "IsInherited": false, "PrincipalType": "Group" },
                { "PrincipalSID": "S-1-5-21-DOMAIN-516", "RightName": "GetChangesAll", "IsInherited": false, "PrincipalType": "Group" }
            ]
        });
        importer.extract_edges("domains", &domain);

        // DC with PrimaryGroupSID = Domain Controllers (-516)
        let dc = serde_json::json!({
            "ObjectIdentifier": "S-1-5-21-DOMAIN-1000",
            "PrimaryGroupSID": "S-1-5-21-DOMAIN-516",
            "Properties": {
                "name": "DC01.TEST.CORP",
                "domainsid": "S-1-5-21-DOMAIN"
            }
        });
        importer.extract_edges("computers", &dc);

        let deferred = importer.derive_deferred_dcsync();

        assert!(
            deferred.iter().any(|e| e.source == "S-1-5-21-DOMAIN-1000"
                && e.target == "S-1-5-21-DOMAIN"
                && e.rel_type == "DCSync"),
            "DC should get DCSync through Domain Controllers (GetChangesAll) + Enterprise Domain Controllers (GetChanges); got: {:?}",
            deferred
        );
    }

    // ========================================================================
    // BH CE Parity: CASecurity ACEs should not emit Owns
    // ========================================================================

    #[test]
    fn test_bhce_ca_security_no_owns() {
        let mut importer = test_importer();
        let entity = serde_json::json!({
            "ObjectIdentifier": "ENTERPRISE-CA-1",
            "CARegistryData": {
                "CASecurity": {
                    "Data": [
                        {
                            "PrincipalSID": "S-1-5-21-DOMAIN-S-1-5-32-544",
                            "RightName": "Owns",
                            "IsInherited": false,
                            "PrincipalType": "Group"
                        },
                        {
                            "PrincipalSID": "S-1-5-21-DOMAIN-ADMIN",
                            "RightName": "ManageCA",
                            "IsInherited": false,
                            "PrincipalType": "User"
                        }
                    ]
                }
            }
        });
        let edges = importer.extract_edges("enterprisecas", &entity);

        assert!(
            edges.iter().any(|e| e.rel_type == "ManageCA"),
            "ManageCA should be emitted from CASecurity"
        );
        assert!(
            !edges.iter().any(|e| e.rel_type == "Owns"),
            "Owns should NOT be emitted from CASecurity ACEs"
        );
    }

    #[test]
    fn test_resolve_orphan_names_batched() {
        let importer = test_importer();
        let domain_sid = "S-1-5-21-1111-2222-3333";

        // Insert a Domain node with a proper name
        importer
            .db
            .insert_node(DbNode {
                id: domain_sid.to_string(),
                name: "CONTOSO.LOCAL".to_string(),
                label: "Domain".to_string(),
                properties: serde_json::json!({}),
            })
            .unwrap();

        // Insert orphan nodes whose name == objectid (raw SID)
        let orphan_rids = ["-512", "-519", "-1001"];
        for rid in &orphan_rids {
            let sid = format!("{}{}", domain_sid, rid);
            importer
                .db
                .insert_node(DbNode {
                    id: sid.clone(),
                    name: sid.clone(), // name == id triggers resolution
                    label: "Group".to_string(),
                    properties: serde_json::json!({}),
                })
                .unwrap();
        }

        let updated = importer.resolve_orphan_names().unwrap();
        assert_eq!(updated, 3);

        // Verify names were actually updated in the database
        let all_nodes = importer.db.get_all_nodes().unwrap();
        for rid in &orphan_rids {
            let sid = format!("{}{}", domain_sid, rid);
            let node = all_nodes.iter().find(|n| n.id == sid).unwrap();
            assert_eq!(
                node.name,
                format!("CONTOSO.LOCAL{}", rid),
                "Orphan {} should have resolved name",
                sid
            );
        }
    }
}
