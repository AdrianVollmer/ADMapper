//! BloodHound JSON/ZIP importer.

mod dcsync;
mod edges;
mod flush;
mod mapping;
mod nodes;
mod orchestration;
mod types;

#[cfg(all(test, feature = "crustdb"))]
mod tests;

use crate::db::{DatabaseBackend, DbEdge, DbNode};
use crate::import::types::ImportProgress;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;

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

    fn send_progress(&self, progress: &ImportProgress) {
        let _ = self.progress_tx.send(progress.clone());
    }
}
