use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use std::collections::HashMap;

impl super::Database {
    /// Insert multiple nodes in a single transaction.
    ///
    /// Each node is specified as (labels, properties).
    /// Returns a vector of the created node IDs in the same order as the input.
    ///
    /// This is significantly faster than executing individual CREATE statements
    /// because it uses prepared statements and batches all inserts in one transaction.
    pub fn insert_nodes_batch(
        &self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        self.require_writable()?;
        let mut storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.insert_nodes_batch(nodes)
    }

    /// Upsert multiple nodes in a single transaction.
    ///
    /// Each node is specified as (labels, properties).
    /// Returns a vector of the node IDs (either created or existing) in the same order as input.
    ///
    /// **Key difference from insert_nodes_batch**:
    /// - If a node with the same objectid already exists, its properties are **merged**
    ///   using json_patch (new properties are added, existing are updated) rather than
    ///   replaced entirely.
    /// - Labels are also merged (added if not present).
    ///
    /// This enables streaming relationship import: when an relationship references a node that doesn't
    /// exist yet, an orphan node can be created with just the objectid. When the full
    /// node data arrives later, upsert_nodes_batch merges in the complete properties.
    pub fn upsert_nodes_batch(
        &self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        self.require_writable()?;
        let mut storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.upsert_nodes_batch(nodes)
    }

    /// Get or create a node by objectid, returning its internal ID.
    ///
    /// If the node exists, returns its ID without modifying it.
    /// If it doesn't exist, creates an orphan node with just the objectid
    /// and the specified label, ready to be upserted later with full properties.
    ///
    /// This is useful for streaming relationship import where relationships may reference
    /// nodes that haven't been imported yet.
    pub fn get_or_create_node_by_objectid(&self, objectid: &str, label: &str) -> Result<i64> {
        self.require_writable()?;
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.get_or_create_node_by_objectid(objectid, label)
    }

    /// Insert multiple relationships in a single transaction.
    ///
    /// Each relationship is specified as (source_node_id, target_node_id, rel_type, properties).
    /// Returns a vector of the created relationship IDs in the same order as the input.
    ///
    /// Use `find_node_by_property` or `build_property_index` to look up node IDs first.
    pub fn insert_relationships_batch(
        &self,
        relationships: &[(i64, i64, String, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        self.require_writable()?;
        let mut storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.insert_relationships_batch(relationships)
    }

    /// Find a node ID by a property value.
    ///
    /// Searches for nodes where the JSON properties contain the specified key-value pair.
    pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.find_node_by_property(property, value)
    }

    /// Build an index of property values to node IDs for efficient batch lookups.
    ///
    /// This is useful when inserting relationships that reference nodes by a property value
    /// (like objectid) rather than by database ID.
    pub fn build_property_index(&self, property: &str) -> Result<HashMap<String, i64>> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.build_property_index(property)
    }

    /// Get all relationships for a node by objectid (both incoming and outgoing).
    ///
    /// Returns (source_objectid, target_objectid, rel_type) tuples.
    /// This is more efficient than using Cypher queries for relationship retrieval.
    pub fn get_node_relationships_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.get_node_relationships_by_objectid(objectid)
    }

    /// Get incoming connections to a node by objectid.
    ///
    /// Returns all nodes that have relationships pointing TO the specified node,
    /// along with those relationships. Uses direct SQL with the objectid index
    /// for optimal performance O(degree) instead of O(N) for full scans.
    ///
    /// Returns `(Vec<Node>, Vec<Relationship>)` where nodes include both the target node
    /// and all source nodes of incoming relationships.
    pub fn get_incoming_connections_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        let storage = self.get_read_storage();
        storage.get_incoming_connections_by_objectid(objectid)
    }

    /// Get outgoing connections from a node by objectid.
    ///
    /// Returns all nodes that the specified node has relationships pointing TO,
    /// along with those relationships. Uses direct SQL with the objectid index
    /// for optimal performance O(degree) instead of O(N) for full scans.
    ///
    /// Returns `(Vec<Node>, Vec<Relationship>)` where nodes include both the source node
    /// and all target nodes of outgoing relationships.
    pub fn get_outgoing_connections_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        let storage = self.get_read_storage();
        storage.get_outgoing_connections_by_objectid(objectid)
    }

    /// Get counts for all node labels in a single efficient query.
    ///
    /// Returns a HashMap of label name to count.
    /// This is much faster than running separate Cypher COUNT queries for each label.
    pub fn get_label_counts(&self) -> Result<HashMap<String, usize>> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.get_label_counts()
    }

    /// Find outgoing relationships from a node by objectid.
    ///
    /// Returns `(target_objectid, rel_type)` tuples for all outgoing relationships.
    /// This is optimized for BFS traversal where we only need neighbor identifiers,
    /// not full node/relationship objects.
    ///
    /// Uses the dedicated objectid column index for O(1) node lookup,
    /// then O(degree) for relationship retrieval.
    pub fn find_outgoing_relationships_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<Vec<(String, String)>> {
        let storage = self.get_read_storage();
        storage.find_outgoing_relationships_by_objectid(objectid)
    }
}
