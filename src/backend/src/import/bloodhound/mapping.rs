//! Helper functions for mapping BloodHound ACE rights and local group
//! identifiers to relationship types.
//!
//! Mappings are derived from the shared relationship type definitions
//! in `src/shared/relationship_types.json`.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use super::BloodHoundImporter;

/// Parsed shared definitions (compile-time embedded).
#[derive(Deserialize)]
struct SharedDefs {
    relationship_types: Vec<SharedRelType>,
    local_group_mappings: HashMap<String, String>,
    local_group_name_fallbacks: HashMap<String, String>,
}

#[derive(Deserialize)]
struct SharedRelType {
    name: String,
    #[serde(default)]
    ace_right: bool,
}

static SHARED: LazyLock<SharedDefs> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../../../shared/relationship_types.json"))
        .expect("shared relationship_types.json must be valid")
});

/// Set of recognized ACE right names that produce relationships.
static ACE_RIGHTS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    SHARED
        .relationship_types
        .iter()
        .filter(|t| t.ace_right)
        .map(|t| t.name.as_str())
        .collect()
});

impl BloodHoundImporter {
    /// Map a local group to a relationship type.
    ///
    /// Prefers matching by well-known RID suffix on `ObjectIdentifier` (stable
    /// across locales). Falls back to case-insensitive substring matching on the
    /// group `Name` for types without a well-known SID (e.g.
    /// RemoteInteractiveLogonRight) or older data formats.
    pub(super) fn local_group_to_relationship_type(
        object_identifier: Option<&str>,
        group_name: &str,
    ) -> Option<&'static str> {
        // Try RID-based matching first (locale-independent).
        if let Some(sid) = object_identifier {
            for (rid_suffix, rel_type) in &SHARED.local_group_mappings {
                if sid.ends_with(rid_suffix.as_str()) {
                    // Return a &'static str by looking up the name in SHARED.
                    return SHARED
                        .relationship_types
                        .iter()
                        .find(|t| t.name == *rel_type)
                        .map(|t| t.name.as_str());
                }
            }
        }

        // Fall back to name-based matching (for types without a well-known SID
        // or data that doesn't include ObjectIdentifier on local groups).
        let upper = group_name.to_uppercase();
        for (pattern, rel_type) in &SHARED.local_group_name_fallbacks {
            if upper.contains(pattern.as_str()) {
                return SHARED
                    .relationship_types
                    .iter()
                    .find(|t| t.name == *rel_type)
                    .map(|t| t.name.as_str());
            }
        }

        None
    }

    /// Map an ACE right name to its relationship type.
    ///
    /// Returns `None` for unrecognized rights. Only rights marked as
    /// `ace_right: true` in the shared definitions produce relationships.
    pub(super) fn ace_to_relationship_type(right_name: &str) -> Option<&'static str> {
        if ACE_RIGHTS.contains(right_name) {
            // Return &'static str from the shared definitions.
            SHARED
                .relationship_types
                .iter()
                .find(|t| t.name == right_name)
                .map(|t| t.name.as_str())
        } else {
            None
        }
    }
}
