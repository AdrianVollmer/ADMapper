//! In-memory adjacency list cache for fast graph traversals.
//!
//! Replaces per-node SQLite queries during BFS/expand operations with
//! O(1) HashMap lookups. Built lazily from a single `scan_all_relationships()`
//! call and invalidated on any graph mutation.

use std::collections::HashMap;

/// A single adjacency entry: (neighbor_node_id, relationship_id, relationship_type).
pub type AdjEntry = (i64, i64, String);

/// In-memory adjacency list cache for the graph topology.
///
/// Stores outgoing and incoming neighbor lists keyed by node ID.
/// Built from a single SQL scan, then used for O(1) neighbor lookups
/// during BFS, shortest path, and expand operations.
#[derive(Debug)]
pub struct AdjacencyCache {
    /// node_id → [(target_id, rel_id, rel_type)]
    outgoing: HashMap<i64, Vec<AdjEntry>>,
    /// node_id → [(source_id, rel_id, rel_type)]
    incoming: HashMap<i64, Vec<AdjEntry>>,
}

impl AdjacencyCache {
    /// Build the adjacency cache from the storage backend.
    ///
    /// Uses `scan_relationship_topology()` which skips JSON property
    /// deserialization, only loading (id, source, target, type) tuples.
    pub fn build(storage: &super::SqliteStorage) -> crate::error::Result<Self> {
        let topology = storage.scan_relationship_topology()?;

        let mut outgoing: HashMap<i64, Vec<AdjEntry>> = HashMap::new();
        let mut incoming: HashMap<i64, Vec<AdjEntry>> = HashMap::new();

        for (rel_id, source, target, rel_type) in topology {
            outgoing
                .entry(source)
                .or_default()
                .push((target, rel_id, rel_type.clone()));
            incoming
                .entry(target)
                .or_default()
                .push((source, rel_id, rel_type));
        }

        Ok(Self { outgoing, incoming })
    }

    /// Get outgoing neighbors for a node: [(target_id, rel_id, rel_type)].
    #[inline]
    pub fn outgoing(&self, node_id: i64) -> &[AdjEntry] {
        self.outgoing.get(&node_id).map_or(&[], |v| v.as_slice())
    }

    /// Get incoming neighbors for a node: [(source_id, rel_id, rel_type)].
    #[inline]
    pub fn incoming(&self, node_id: i64) -> &[AdjEntry] {
        self.incoming.get(&node_id).map_or(&[], |v| v.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteStorage;

    fn setup_graph() -> SqliteStorage {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes: A(1), B(2), C(3)
        let a = storage
            .insert_node(&["Node".into()], &serde_json::json!({"name": "A"}))
            .unwrap();
        let b = storage
            .insert_node(&["Node".into()], &serde_json::json!({"name": "B"}))
            .unwrap();
        let c = storage
            .insert_node(&["Node".into()], &serde_json::json!({"name": "C"}))
            .unwrap();

        // Relationships: A->B (KNOWS, id=1), A->C (LIKES, id=2), B->C (KNOWS, id=3)
        storage
            .insert_relationship(a, b, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_relationship(a, c, "LIKES", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_relationship(b, c, "KNOWS", &serde_json::json!({}))
            .unwrap();

        storage
    }

    #[test]
    fn test_build_from_storage() {
        let storage = setup_graph();
        let cache = AdjacencyCache::build(&storage).unwrap();

        // Node A (id=1) has 2 outgoing, 0 incoming
        assert_eq!(cache.outgoing(1).len(), 2);
        assert_eq!(cache.incoming(1).len(), 0);

        // Node B (id=2) has 1 outgoing, 1 incoming
        assert_eq!(cache.outgoing(2).len(), 1);
        assert_eq!(cache.incoming(2).len(), 1);

        // Node C (id=3) has 0 outgoing, 2 incoming
        assert_eq!(cache.outgoing(3).len(), 0);
        assert_eq!(cache.incoming(3).len(), 2);
    }

    #[test]
    fn test_outgoing_contents() {
        let storage = setup_graph();
        let cache = AdjacencyCache::build(&storage).unwrap();

        let a_out = cache.outgoing(1);
        let targets: Vec<i64> = a_out.iter().map(|(t, _, _)| *t).collect();
        assert!(targets.contains(&2)); // A->B
        assert!(targets.contains(&3)); // A->C

        let types: Vec<&str> = a_out.iter().map(|(_, _, t)| t.as_str()).collect();
        assert!(types.contains(&"KNOWS"));
        assert!(types.contains(&"LIKES"));
    }

    #[test]
    fn test_incoming_contents() {
        let storage = setup_graph();
        let cache = AdjacencyCache::build(&storage).unwrap();

        let c_in = cache.incoming(3);
        let sources: Vec<i64> = c_in.iter().map(|(s, _, _)| *s).collect();
        assert!(sources.contains(&1)); // A->C
        assert!(sources.contains(&2)); // B->C
    }

    #[test]
    fn test_nonexistent_node_returns_empty() {
        let storage = setup_graph();
        let cache = AdjacencyCache::build(&storage).unwrap();

        assert!(cache.outgoing(999).is_empty());
        assert!(cache.incoming(999).is_empty());
    }

    #[test]
    fn test_empty_graph() {
        let storage = SqliteStorage::in_memory().unwrap();
        let cache = AdjacencyCache::build(&storage).unwrap();

        assert!(cache.outgoing(1).is_empty());
        assert!(cache.incoming(1).is_empty());
    }

    #[test]
    fn test_rel_ids_are_correct() {
        let storage = setup_graph();
        let cache = AdjacencyCache::build(&storage).unwrap();

        // All rel_ids across the cache should be 1, 2, 3
        let mut all_rel_ids: Vec<i64> = Vec::new();
        for (_, rel_id, _) in cache.outgoing(1) {
            all_rel_ids.push(*rel_id);
        }
        for (_, rel_id, _) in cache.outgoing(2) {
            all_rel_ids.push(*rel_id);
        }
        all_rel_ids.sort();
        assert_eq!(all_rel_ids, vec![1, 2, 3]);
    }
}
