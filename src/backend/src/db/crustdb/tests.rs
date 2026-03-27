//! Tests for CrustDB algorithms.

use std::collections::HashMap;

use super::algorithms::reverse_bfs;

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
