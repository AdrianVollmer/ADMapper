//! DCSync state tracking and deferred edge derivation.

use super::BloodHoundImporter;
use crate::db::DbEdge;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use tracing::debug;

impl BloodHoundImporter {
    /// Derive DCSync edges when a principal holds both GetChanges and
    /// GetChangesAll on the same target (typically a Domain).
    pub(super) fn derive_dcsync_edges(
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

    /// Track state needed for deferred DCSync derivation.
    ///
    /// Collects: GetChanges/GetChangesAll principals per domain, group
    /// memberships, PrimaryGroupSID memberships, and domain name mappings.
    pub(super) fn track_dcsync_state(
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
    pub(super) fn derive_deferred_dcsync(&self) -> Vec<DbEdge> {
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
}
