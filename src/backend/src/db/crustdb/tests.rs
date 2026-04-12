//! Tests for CrustDB algorithms and exploit_likelihood storage/retrieval.

use std::collections::HashMap;

use super::algorithms::reverse_bfs;
use super::CrustDatabase;
use crate::db::DbEdge;

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
