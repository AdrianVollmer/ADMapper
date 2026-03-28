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
    /// Issues a single `scan_all_relationships()` query and partitions
    /// the results into outgoing/incoming adjacency lists.
    pub fn build(storage: &super::SqliteStorage) -> crate::error::Result<Self> {
        let relationships = storage.scan_all_relationships()?;

        let mut outgoing: HashMap<i64, Vec<AdjEntry>> = HashMap::new();
        let mut incoming: HashMap<i64, Vec<AdjEntry>> = HashMap::new();

        for rel in relationships {
            outgoing.entry(rel.source).or_default().push((
                rel.target,
                rel.id,
                rel.rel_type.clone(),
            ));
            incoming
                .entry(rel.target)
                .or_default()
                .push((rel.source, rel.id, rel.rel_type));
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
