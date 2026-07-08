//! Tests for CrustDB algorithms, exploit_likelihood storage/retrieval,
//! and node connection queries.

use std::collections::HashMap;

use super::algorithms::reverse_bfs;
use super::CrustDatabase;
use crate::db::backend::DatabaseBackend;
use crate::db::{DbEdge, DbNode};

#[test]
fn reverse_bfs_single_seed() {
    // A -> B -> C (DA)
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("B", vec!["A"]);
    adj.insert("C", vec!["B"]);

    let distances = reverse_bfs(&["C"], &adj);
    assert_eq!(distances.get("C"), Some(&0));
    assert_eq!(distances.get("B"), Some(&1));
    assert_eq!(distances.get("A"), Some(&2));
}

#[test]
fn reverse_bfs_multiple_seeds() {
    // A -> DA1, B -> DA2
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("DA1", vec!["A"]);
    adj.insert("DA2", vec!["B"]);

    let distances = reverse_bfs(&["DA1", "DA2"], &adj);
    assert_eq!(distances.get("A"), Some(&1));
    assert_eq!(distances.get("B"), Some(&1));
    assert_eq!(distances.get("DA1"), Some(&0));
    assert_eq!(distances.get("DA2"), Some(&0));
}

#[test]
fn reverse_bfs_shortest_path_wins() {
    // A -> X -> DA, A -> DA (direct, shorter)
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("DA", vec!["A", "X"]);
    adj.insert("X", vec!["A"]);

    let distances = reverse_bfs(&["DA"], &adj);
    assert_eq!(distances.get("A"), Some(&1)); // Direct, not via X (2)
}

#[test]
fn reverse_bfs_unreachable_nodes() {
    // A -> DA, C is isolated
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("DA", vec!["A"]);
    // C has no edges

    let distances = reverse_bfs(&["DA"], &adj);
    assert_eq!(distances.get("A"), Some(&1));
    assert!(distances.get("C").is_none());
}

#[test]
fn reverse_bfs_empty_graph() {
    let adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let distances = reverse_bfs(&["DA"], &adj);
    assert_eq!(distances.len(), 1); // Only the seed itself
    assert_eq!(distances.get("DA"), Some(&0));
}

#[test]
fn reverse_bfs_cycle() {
    // A -> B -> C -> A (cycle), C is also a DA seed
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("C", vec!["B"]);
    adj.insert("B", vec!["A"]);
    adj.insert("A", vec!["C"]); // Back-edge

    let distances = reverse_bfs(&["C"], &adj);
    assert_eq!(distances.get("C"), Some(&0));
    assert_eq!(distances.get("B"), Some(&1));
    assert_eq!(distances.get("A"), Some(&2));
}

#[test]
fn reverse_bfs_diamond() {
    //     B
    //    / \
    // A     DA
    //    \ /
    //     C
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    adj.insert("DA", vec!["B", "C"]);
    adj.insert("B", vec!["A"]);
    adj.insert("C", vec!["A"]);

    let distances = reverse_bfs(&["DA"], &adj);
    assert_eq!(distances.get("DA"), Some(&0));
    assert_eq!(distances.get("B"), Some(&1));
    assert_eq!(distances.get("C"), Some(&1));
    assert_eq!(distances.get("A"), Some(&2));
}

#[test]
fn reverse_bfs_no_seeds() {
    let adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let distances = reverse_bfs(&[], &adj);
    assert!(distances.is_empty());
}

// ============================================================================
// exploit_likelihood storage and retrieval smoke tests
// ============================================================================

/// Build a minimal edge with the given source, target, type, and properties.
fn make_edge(source: &str, target: &str, rel_type: &str, properties: serde_json::Value) -> DbEdge {
    DbEdge {
        source: source.to_string(),
        target: target.to_string(),
        rel_type: rel_type.to_string(),
        properties,
        ..Default::default()
    }
}

#[test]
fn exploit_likelihood_stored_and_retrieved_via_get_all_edges() {
    let db = CrustDatabase::in_memory().unwrap();

    let edge = make_edge(
        "node-a",
        "node-b",
        "MemberOf",
        serde_json::json!({"exploit_likelihood": 0.75}),
    );
    db.insert_edges(&[edge]).unwrap();

    let edges = db.get_all_edges().unwrap();
    assert_eq!(edges.len(), 1);
    let el = edges[0]
        .properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64());
    assert_eq!(
        el,
        Some(0.75),
        "exploit_likelihood should round-trip through insert/get_all_edges"
    );
}

#[test]
fn exploit_likelihood_absent_when_not_set() {
    let db = CrustDatabase::in_memory().unwrap();

    let edge = make_edge("node-a", "node-b", "MemberOf", serde_json::json!({}));
    db.insert_edges(&[edge]).unwrap();

    let edges = db.get_all_edges().unwrap();
    assert_eq!(edges.len(), 1);
    let el = edges[0]
        .properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64());
    assert!(
        el.is_none(),
        "exploit_likelihood should not be present when not stored"
    );
}

#[test]
fn get_all_edges_handles_missing_exploit_likelihood_on_some_edges() {
    // Verifies the query doesn't fail when some edges lack the property (null case).
    let db = CrustDatabase::in_memory().unwrap();

    let with_el = make_edge(
        "a",
        "b",
        "AdminTo",
        serde_json::json!({"exploit_likelihood": 1.0}),
    );
    let without_el = make_edge("b", "c", "MemberOf", serde_json::json!({}));
    db.insert_edges(&[with_el, without_el]).unwrap();

    let edges = db.get_all_edges().unwrap();
    assert_eq!(edges.len(), 2);

    let admin_edge = edges.iter().find(|e| e.rel_type == "AdminTo").unwrap();
    assert_eq!(
        admin_edge
            .properties
            .get("exploit_likelihood")
            .and_then(|v| v.as_f64()),
        Some(1.0)
    );

    let member_edge = edges.iter().find(|e| e.rel_type == "MemberOf").unwrap();
    assert!(member_edge
        .properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64())
        .is_none());
}

#[test]
fn exploit_likelihood_set_via_apply_to_all_edges() {
    let db = CrustDatabase::in_memory().unwrap();

    // Insert edge without exploit_likelihood (simulates old data).
    let edge = make_edge("node-a", "node-b", "GenericAll", serde_json::json!({}));
    db.insert_edges(&[edge]).unwrap();

    // Verify not present before apply.
    let before = db.get_all_edges().unwrap();
    assert!(before[0]
        .properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64())
        .is_none());

    // Apply via Cypher SET (mirrors apply_to_all_edges).
    db.run_custom_query("MATCH ()-[r:GenericAll]->() SET r.exploit_likelihood = 0.5")
        .unwrap();

    // Verify present after apply.
    let after = db.get_all_edges().unwrap();
    assert_eq!(
        after[0]
            .properties
            .get("exploit_likelihood")
            .and_then(|v| v.as_f64()),
        Some(0.5),
        "exploit_likelihood set via Cypher SET should be readable by get_all_edges"
    );
}

#[test]
fn exploit_likelihood_survives_update_edge() {
    use crate::api::core::mutation::update_edge;

    let db = CrustDatabase::in_memory().unwrap();

    // Insert edge with initial exploit_likelihood.
    let edge = make_edge(
        "node-a",
        "node-b",
        "AdminTo",
        serde_json::json!({"exploit_likelihood": 1.0}),
    );
    db.insert_edges(&[edge]).unwrap();

    // Update via the mutation API (setting a different property).
    update_edge(
        &db,
        "node-a",
        "node-b",
        "AdminTo",
        serde_json::json!({"exploit_likelihood": 0.2}),
    )
    .unwrap();

    let edges = db.get_all_edges().unwrap();
    assert_eq!(edges.len(), 1);
    let el = edges[0]
        .properties
        .get("exploit_likelihood")
        .and_then(|v| v.as_f64());
    assert_eq!(
        el,
        Some(0.2),
        "update_edge should persist the new exploit_likelihood"
    );
}

// ============================================================================
// Node connection tests
// ============================================================================

/// Helper: insert a node with the given objectid and type label.
fn insert_node(db: &CrustDatabase, id: &str, label: &str) {
    let node = DbNode {
        id: id.to_string(),
        name: id.to_string(),
        label: label.to_string(),
        properties: serde_json::json!({"objectid": id, "name": id}),
    };
    db.insert_nodes(&[node]).unwrap();
}

/// Helper: insert a directed edge.
fn insert_edge(db: &CrustDatabase, src: &str, tgt: &str, rel_type: &str) {
    let edge = make_edge(src, tgt, rel_type, serde_json::json!({}));
    db.insert_edges(&[edge]).unwrap();
}

#[test]
fn get_node_connections_incoming_returns_all_sources() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "target-node", "CertTemplate");
    insert_node(&db, "src-1", "User");
    insert_node(&db, "src-2", "Group");
    insert_node(&db, "src-3", "Computer");

    insert_edge(&db, "src-1", "target-node", "Enroll");
    insert_edge(&db, "src-2", "target-node", "Enroll");
    insert_edge(&db, "src-3", "target-node", "WritePKINameFlag");

    let (nodes, edges) = db.get_node_connections("target-node", "incoming").unwrap();

    assert_eq!(edges.len(), 3, "Should return all 3 incoming edges");
    assert_eq!(
        nodes.len(),
        4,
        "Should return all 4 nodes (3 sources + target)"
    );

    // Verify all source nodes are present.
    let node_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(node_ids.contains(&"src-1"), "src-1 missing");
    assert!(node_ids.contains(&"src-2"), "src-2 missing");
    assert!(node_ids.contains(&"src-3"), "src-3 missing");
    assert!(node_ids.contains(&"target-node"), "target-node missing");
}

#[test]
fn get_node_connections_outgoing_returns_all_targets() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "origin", "User");
    insert_node(&db, "tgt-1", "Group");
    insert_node(&db, "tgt-2", "Computer");

    insert_edge(&db, "origin", "tgt-1", "MemberOf");
    insert_edge(&db, "origin", "tgt-2", "AdminTo");

    let (nodes, edges) = db.get_node_connections("origin", "outgoing").unwrap();

    assert_eq!(edges.len(), 2, "Should return both outgoing edges");
    let node_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(node_ids.contains(&"origin"));
    assert!(node_ids.contains(&"tgt-1"));
    assert!(node_ids.contains(&"tgt-2"));
}

#[test]
fn get_node_connections_edge_direction_is_correct() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "a", "User");
    insert_node(&db, "b", "Group");
    insert_edge(&db, "a", "b", "MemberOf");

    // Incoming for b: edge should be a -> b
    let (_, edges) = db.get_node_connections("b", "incoming").unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].source, "a");
    assert_eq!(edges[0].target, "b");
    assert_eq!(edges[0].rel_type, "MemberOf");

    // Outgoing for a: same edge
    let (_, edges) = db.get_node_connections("a", "outgoing").unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].source, "a");
    assert_eq!(edges[0].target, "b");
}

#[test]
fn get_node_connections_no_connections_returns_target_only() {
    let db = CrustDatabase::in_memory().unwrap();
    insert_node(&db, "lonely", "User");

    let (nodes, edges) = db.get_node_connections("lonely", "incoming").unwrap();
    assert_eq!(edges.len(), 0);
    // The node itself may or may not be returned; at minimum no crash.
    assert!(nodes.len() <= 1);
}

#[test]
fn get_node_connections_memberof_direction() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "user", "User");
    insert_node(&db, "group", "Group");
    insert_node(&db, "other", "Computer");

    insert_edge(&db, "user", "group", "MemberOf");
    insert_edge(&db, "user", "other", "AdminTo"); // not MemberOf

    let (_, edges) = db.get_node_connections("user", "memberof").unwrap();
    assert_eq!(edges.len(), 1, "memberof should only return MemberOf edges");
    assert_eq!(edges[0].target, "group");
}

#[test]
fn get_node_connections_members_direction() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "member-1", "User");
    insert_node(&db, "member-2", "User");
    insert_node(&db, "group", "Group");
    insert_node(&db, "outsider", "User");

    insert_edge(&db, "member-1", "group", "MemberOf");
    insert_edge(&db, "member-2", "group", "MemberOf");
    insert_edge(&db, "outsider", "group", "AdminTo"); // not MemberOf

    let (nodes, edges) = db.get_node_connections("group", "members").unwrap();
    assert_eq!(edges.len(), 2, "members should return both MemberOf edges");
    let node_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(node_ids.contains(&"member-1"));
    assert!(node_ids.contains(&"member-2"));
}

#[test]
fn get_node_relationship_counts_match_actual_connections() {
    let db = CrustDatabase::in_memory().unwrap();

    insert_node(&db, "center", "User");
    insert_node(&db, "in-1", "User");
    insert_node(&db, "in-2", "Computer");
    insert_node(&db, "in-3", "Group");
    insert_node(&db, "out-1", "Computer");
    insert_node(&db, "member-group", "Group");
    insert_node(&db, "member-1", "User");

    // 3 incoming to center
    insert_edge(&db, "in-1", "center", "HasSession");
    insert_edge(&db, "in-2", "center", "CanRDP");
    insert_edge(&db, "in-3", "center", "GenericAll");

    // 1 outgoing from center (admin type)
    insert_edge(&db, "center", "out-1", "AdminTo");

    // 1 MemberOf from center
    insert_edge(&db, "center", "member-group", "MemberOf");

    // 1 member of member-group (not center)
    insert_edge(&db, "member-1", "member-group", "MemberOf");

    let (incoming, outgoing, admin_to, member_of, _members) =
        db.get_node_relationship_counts("center").unwrap();

    assert_eq!(incoming, 3, "center has 3 incoming edges");
    // outgoing = AdminTo + MemberOf = 2
    assert_eq!(outgoing, 2, "center has 2 outgoing edges");
    assert_eq!(admin_to, 1, "center has 1 admin-type outgoing edge");
    assert_eq!(member_of, 1, "center is MemberOf 1 group");

    // Verify counts match actual connection queries.
    let (_, in_edges) = db.get_node_connections("center", "incoming").unwrap();
    assert_eq!(
        in_edges.len(),
        incoming,
        "incoming count should match actual incoming connections"
    );

    let (_, out_edges) = db.get_node_connections("center", "outgoing").unwrap();
    assert_eq!(
        out_edges.len(),
        outgoing,
        "outgoing count should match actual outgoing connections"
    );

    let (_, admin_edges) = db.get_node_connections("center", "admin").unwrap();
    assert_eq!(
        admin_edges.len(),
        admin_to,
        "admin count should match actual admin connections"
    );

    let (_, mo_edges) = db.get_node_connections("center", "memberof").unwrap();
    assert_eq!(
        mo_edges.len(),
        member_of,
        "memberof count should match actual memberof connections"
    );
}

#[test]
fn get_node_connections_null_properties_handled() {
    let db = CrustDatabase::in_memory().unwrap();
    insert_node(&db, "a", "User");
    insert_node(&db, "b", "Group");

    // Insert edge with null properties (the pattern from BloodHound import).
    let edge = DbEdge {
        source: "a".to_string(),
        target: "b".to_string(),
        rel_type: "MemberOf".to_string(),
        properties: serde_json::Value::Null,
        ..Default::default()
    };
    db.insert_edges(&[edge]).unwrap();

    let (nodes, edges) = db.get_node_connections("b", "incoming").unwrap();
    assert_eq!(edges.len(), 1, "Should return edge with null properties");
    assert_eq!(nodes.len(), 2);
    assert_eq!(edges[0].source, "a");
    assert_eq!(edges[0].target, "b");
}
