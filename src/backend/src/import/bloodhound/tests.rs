//! Tests for the BloodHound importer.

use super::*;
use crate::db::crustdb::CrustDatabase;
use crate::db::DbNode;
use crate::import::types::ImportProgress;
use rstest::rstest;

#[rstest]
#[case("GenericAll", Some("GenericAll"))]
#[case("WriteDacl", Some("WriteDacl"))]
#[case("Enroll", Some("Enroll"))]
#[case("AddSelf", Some("AddSelf"))]
#[case("Unknown", None)]
fn test_ace_to_relationship_type(#[case] right_name: &str, #[case] expected: Option<&str>) {
    assert_eq!(
        BloodHoundImporter::ace_to_relationship_type(right_name),
        expected,
        "ACE right '{}' should map to {:?}",
        right_name,
        expected,
    );
}

#[rstest]
// RID-based matching (preferred)
#[case(Some("S-1-5-32-544"), "Administrators", Some("AdminTo"))]
#[case(Some("S-1-5-32-555"), "Remote Desktop Users", Some("CanRDP"))]
#[case(Some("S-1-5-32-580"), "Remote Management Users", Some("CanPSRemote"))]
#[case(Some("S-1-5-32-562"), "Distributed COM Users", Some("ExecuteDCOM"))]
// Name-based fallback (no ObjectIdentifier)
#[case(None, "Administrators", Some("AdminTo"))]
#[case(None, "Remote Desktop Users", Some("CanRDP"))]
#[case(None, "Remote Interactive Logon", Some("RemoteInteractiveLogonRight"))]
#[case(None, "Unknown Group", None)]
// RID takes precedence over name
#[case(Some("S-1-5-32-544"), "Wrong Name", Some("AdminTo"))]
fn test_local_group_to_relationship_type(
    #[case] object_identifier: Option<&str>,
    #[case] group_name: &str,
    #[case] expected: Option<&str>,
) {
    assert_eq!(
        BloodHoundImporter::local_group_to_relationship_type(object_identifier, group_name),
        expected,
    );
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

#[rstest]
#[case("users", "S-1-5-21-1234-USER", "testuser@corp.local", "User")]
#[case("computers", "S-1-5-21-1234-COMP", "DC01.corp.local", "Computer")]
#[case("groups", "S-1-5-21-1234-GROUP", "Domain Admins", "Group")]
fn test_extract_node_by_type(
    #[case] data_type: &str,
    #[case] sid: &str,
    #[case] name: &str,
    #[case] expected_label: &str,
) {
    let importer = test_importer();

    let entity = serde_json::json!({
        "ObjectIdentifier": sid,
        "Properties": {
            "name": name
        }
    });

    let node = importer.extract_node(data_type, &entity);
    assert!(node.is_some());

    let node = node.unwrap();
    assert_eq!(node.id, sid);
    assert_eq!(node.name, name);
    assert_eq!(node.label, expected_label);
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

#[rstest]
#[case(0x202, "enabled", false)] // ACCOUNTDISABLE (0x2) + NORMAL_ACCOUNT (0x200)
#[case(0x400200, "enabled", true)] // NORMAL_ACCOUNT (0x200) + DONT_REQ_PREAUTH (0x400000)
#[case(0x400200, "dont_require_preauth", true)] // AS-REP roastable
fn test_extract_node_uac_flags(
    #[case] uac_value: u32,
    #[case] property: &str,
    #[case] expected: bool,
) {
    let importer = test_importer();

    let entity = serde_json::json!({
        "ObjectIdentifier": "S-1-5-21-1234-UAC-TEST",
        "Properties": {
            "name": "uactest@corp.local",
            "useraccountcontrol": uac_value
        }
    });

    let node = importer.extract_node("users", &entity).unwrap();
    assert_eq!(
        node.properties[property], expected,
        "UAC 0x{:X} should set {}={}",
        uac_value, property, expected,
    );
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

#[rstest]
// Tier-0 group objects
#[case(
    "S-1-5-21-1234567890-512",
    "groups",
    "DOMAIN ADMINS@CORP.LOCAL",
    Some(0)
)]
#[case(
    "S-1-5-21-1234567890-516",
    "groups",
    "DOMAIN CONTROLLERS@CORP.LOCAL",
    Some(0)
)]
#[case("S-1-5-32-544", "groups", "ADMINISTRATORS@CORP.LOCAL", Some(0))]
#[case(
    "S-1-5-21-1234567890-S-1-5-9",
    "groups",
    "ENTERPRISE DOMAIN CONTROLLERS@CORP.LOCAL",
    Some(0)
)]
#[case("S-1-5-9", "groups", "ENTERPRISE DOMAIN CONTROLLERS", Some(0))]
// Domains are always tier 0
#[case("S-1-5-21-1234567890", "domains", "CORP.LOCAL", Some(0))]
// Domain Computers group object is tier 3
#[case(
    "S-1-5-21-1234567890-515",
    "groups",
    "DOMAIN COMPUTERS@CORP.LOCAL",
    Some(3)
)]
// Other privileged groups are no longer auto-assigned; user defines them
#[case(
    "S-1-5-21-1234567890-519",
    "groups",
    "ENTERPRISE ADMINS@CORP.LOCAL",
    None
)]
#[case("S-1-5-21-1234567890-518", "groups", "SCHEMA ADMINS@CORP.LOCAL", None)]
// Regular objects: no auto-tier
#[case("S-1-5-21-1234567890-1001", "users", "regularuser@corp.local", None)]
fn test_tier_assignment(
    #[case] sid: &str,
    #[case] data_type: &str,
    #[case] name: &str,
    #[case] expected_tier: Option<i64>,
) {
    let importer = test_importer();

    let entity = serde_json::json!({
        "ObjectIdentifier": sid,
        "Properties": {
            "name": name
        }
    });

    let node = importer.extract_node(data_type, &entity).unwrap();
    match expected_tier {
        Some(tier) => assert_eq!(
            node.properties["tier"], tier,
            "SID {} should be tier {}",
            sid, tier,
        ),
        None => assert!(
            node.properties.get("tier").is_none(),
            "SID {} should not have a tier assigned",
            sid,
        ),
    }
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
                "ObjectIdentifier": "S-1-5-32-544",
                "Name": "Administrators",
                "Results": [
                    {"ObjectIdentifier": "S-1-5-21-ADMIN1"}
                ]
            },
            {
                "ObjectIdentifier": "S-1-5-32-555",
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

/// BH CE creates MemberOf edges from PrimaryGroupSID for both users and computers.
#[rstest]
#[case("users", "S-1-5-21-1234-1001", "S-1-5-21-1234-513", "jdoe@corp.local")]
#[case(
    "computers",
    "S-1-5-21-1234-1103",
    "S-1-5-21-1234-515",
    "DC01.corp.local"
)]
fn test_bhce_primary_group_creates_memberof(
    #[case] data_type: &str,
    #[case] entity_sid: &str,
    #[case] group_sid: &str,
    #[case] name: &str,
) {
    let mut importer = test_importer();

    let entity = serde_json::json!({
        "ObjectIdentifier": entity_sid,
        "PrimaryGroupSID": group_sid,
        "Properties": {"name": name}
    });

    let edges = importer.extract_edges(data_type, &entity);
    let memberof: Vec<_> = edges.iter().filter(|e| e.rel_type == "MemberOf").collect();

    assert_eq!(
        memberof.len(),
        1,
        "PrimaryGroupSID should produce a MemberOf edge for {}",
        data_type,
    );
    assert_eq!(memberof[0].source, entity_sid);
    assert_eq!(memberof[0].target, group_sid);
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
                "ObjectIdentifier": "S-1-5-32-544",
                "Name": "Administrators",
                "Results": [
                    {"ObjectIdentifier": "S-1-5-21-1234-512", "ObjectType": "Group"}
                ]
            },
            {
                "ObjectIdentifier": "S-1-5-32-555",
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
                "ObjectIdentifier": "S-1-5-32-544",
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

/// PKI entities create domain-relationship edges (RootCAFor, NTAuthStoreFor,
/// EnterpriseCAFor) pointing from the PKI object to its domain.
#[rstest]
#[case(
    "rootcas", "ROOTCA-GUID-1", "RootCAFor",
    serde_json::json!({"ObjectIdentifier": "ROOTCA-GUID-1", "DomainSID": "S-1-5-21-DOMAIN", "Properties": {"name": "ROOT-CA@CORP.LOCAL"}})
)]
#[case(
    "ntauthstores", "NTAUTH-GUID-1", "NTAuthStoreFor",
    serde_json::json!({"ObjectIdentifier": "NTAUTH-GUID-1", "DomainSID": "S-1-5-21-DOMAIN", "Properties": {"name": "NTAUTH@CORP.LOCAL"}})
)]
#[case(
    "enterprisecas", "CA-GUID-1", "EnterpriseCAFor",
    serde_json::json!({"ObjectIdentifier": "CA-GUID-1", "Properties": {"name": "MY-CA@CORP.LOCAL", "domainsid": "S-1-5-21-DOMAIN"}})
)]
fn test_bhce_pki_domain_relationship(
    #[case] data_type: &str,
    #[case] expected_source: &str,
    #[case] expected_rel_type: &str,
    #[case] entity: serde_json::Value,
) {
    let mut importer = test_importer();
    let edges = importer.extract_edges(data_type, &entity);

    let matched: Vec<_> = edges
        .iter()
        .filter(|e| e.rel_type == expected_rel_type)
        .collect();
    assert_eq!(
        matched.len(),
        1,
        "{} should create exactly one {} edge; got: {:?}",
        data_type,
        expected_rel_type,
        edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
    );
    assert_eq!(matched[0].source, expected_source);
    assert_eq!(matched[0].target, "S-1-5-21-DOMAIN");
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

/// CoerceToTGT edges should only be created for computers with unconstrained delegation.
#[rstest]
#[case(true, 1)]
#[case(false, 0)]
fn test_bhce_computer_coerce_to_tgt(
    #[case] unconstrained_delegation: bool,
    #[case] expected_count: usize,
) {
    let mut importer = test_importer();
    let entity = serde_json::json!({
        "ObjectIdentifier": "S-1-5-21-COMP-1",
        "Properties": {
            "name": "DC01.CORP.LOCAL",
            "domainsid": "S-1-5-21-DOMAIN",
            "unconstraineddelegation": unconstrained_delegation
        }
    });
    let edges = importer.extract_edges("computers", &entity);

    let coerce: Vec<_> = edges
        .iter()
        .filter(|e| e.rel_type == "CoerceToTGT")
        .collect();
    assert_eq!(
        coerce.len(),
        expected_count,
        "unconstrained_delegation={} should produce {} CoerceToTGT edges; got: {:?}",
        unconstrained_delegation,
        expected_count,
        edges.iter().map(|e| &e.rel_type).collect::<Vec<_>>()
    );
    if expected_count > 0 {
        assert_eq!(coerce[0].source, "S-1-5-21-COMP-1");
        assert_eq!(coerce[0].target, "S-1-5-21-DOMAIN");
    }
}

// ========================================================================
// BH CE Parity: Trust direction (CrossForestTrust / SameForestTrust)
// ========================================================================

/// Inbound: other domain trusts this domain -> edge from target to this.
/// Outbound: this domain trusts other domain -> edge from this to target.
#[rstest]
#[case("Inbound", "S-1-5-21-OTHER", "S-1-5-21-PHANTOM")]
#[case("Outbound", "S-1-5-21-PHANTOM", "S-1-5-21-OTHER")]
fn test_bhce_trust_direction(
    #[case] direction: &str,
    #[case] expected_source: &str,
    #[case] expected_target: &str,
) {
    let mut importer = test_importer();
    let entity = serde_json::json!({
        "ObjectIdentifier": "S-1-5-21-PHANTOM",
        "Trusts": [{
            "TargetDomainSid": "S-1-5-21-OTHER",
            "TargetDomainName": "OTHER.CORP",
            "TrustDirection": direction,
            "TrustType": "External"
        }]
    });
    let edges = importer.extract_edges("domains", &entity);
    let trust: Vec<_> = edges
        .iter()
        .filter(|e| e.rel_type == "CrossForestTrust")
        .collect();
    assert_eq!(trust.len(), 1);
    assert_eq!(trust[0].source, expected_source);
    assert_eq!(trust[0].target, expected_target);
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
        memberof
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN-501" && e.target == "S-1-5-21-DOMAIN-S-1-1-0"),
        "Guest should be MemberOf Everyone"
    );
    // Domain Users -> Authenticated Users
    assert!(
        memberof
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN-513" && e.target == "S-1-5-21-DOMAIN-S-1-5-11"),
        "Domain Users should be MemberOf Authenticated Users"
    );
    // Domain Computers -> Authenticated Users
    assert!(
        memberof
            .iter()
            .any(|e| e.source == "S-1-5-21-DOMAIN-515" && e.target == "S-1-5-21-DOMAIN-S-1-5-11"),
        "Domain Computers should be MemberOf Authenticated Users"
    );
    // Authenticated Users -> Everyone
    assert!(
        memberof.iter().any(
            |e| e.source == "S-1-5-21-DOMAIN-S-1-5-11" && e.target == "S-1-5-21-DOMAIN-S-1-1-0"
        ),
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
    // Include both simple RIDs and compound well-known SID suffixes
    let orphan_suffixes = ["-512", "-519", "-1001", "-S-1-5-11", "-S-1-1-0"];
    for suffix in &orphan_suffixes {
        let sid = format!("{}{}", domain_sid, suffix);
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
    assert_eq!(updated, 5);

    // Verify names were actually updated in the database
    let all_nodes = importer.db.get_all_nodes().unwrap();
    for suffix in &orphan_suffixes {
        let sid = format!("{}{}", domain_sid, suffix);
        let node = all_nodes.iter().find(|n| n.id == sid).unwrap();
        assert_eq!(
            node.name,
            format!("CONTOSO.LOCAL{}", suffix),
            "Orphan {} should have resolved name",
            sid
        );
    }
}
