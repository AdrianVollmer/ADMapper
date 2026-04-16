//! Edge extraction from BloodHound entities.

use super::BloodHoundImporter;
use crate::db::types::normalize_node_type;
use crate::db::DbEdge;
use serde_json::Value as JsonValue;
use tracing::trace;

impl BloodHoundImporter {
    /// Extract relationships from a BloodHound entity.
    pub(super) fn extract_edges(&mut self, data_type: &str, entity: &JsonValue) -> Vec<DbEdge> {
        let objectid = match entity.get("ObjectIdentifier").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return Vec::new(),
        };

        // Normalize type name for consistency
        let node_type = normalize_node_type(data_type);

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

        // Set exploit_likelihood from default for each relationship type
        for rel in &mut relationships {
            let likelihood = crate::exploit_likelihood::default_for(&rel.rel_type);
            let props = match &rel.properties {
                serde_json::Value::Object(map) => {
                    let mut new_map = map.clone();
                    new_map.insert(
                        "exploit_likelihood".to_string(),
                        serde_json::json!(likelihood),
                    );
                    serde_json::Value::Object(new_map)
                }
                _ => serde_json::json!({"exploit_likelihood": likelihood}),
            };
            rel.properties = props;
        }

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
            let group_sid = group.get("ObjectIdentifier").and_then(|v| v.as_str());
            let Some(rel_type) = Self::local_group_to_relationship_type(group_sid, group_name)
            else {
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
            let Some(rel_type) = Self::ace_to_relationship_type(right_name) else {
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
                    self.trust_domain_buffer.push(crate::db::DbNode {
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
}
