use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};

use super::DatabaseStats;

impl super::Database {
    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.stats()
    }

    /// Get database file size in bytes.
    ///
    /// Returns the total size of the database file (page_count * page_size).
    /// Returns 0 for in-memory databases.
    pub fn database_size(&self) -> Result<usize> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.database_size()
    }

    /// Clear all data from the database.
    /// This is much faster than using Cypher DELETE queries.
    pub fn clear(&self) -> Result<()> {
        self.require_writable()?;
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let result = storage.clear();
        self.invalidate_adjacency_cache();
        result
    }

    /// Get a node by its ID.
    pub fn get_node(&self, node_id: i64) -> Result<Option<Node>> {
        let storage = self.get_read_storage();
        storage.get_node(node_id)
    }

    /// Get a relationship by its ID.
    ///
    /// Useful for resolving relationship IDs returned by algorithms like relationship betweenness.
    pub fn get_relationship(&self, rel_id: i64) -> Result<Option<Relationship>> {
        let storage = self.get_read_storage();
        storage.get_relationship(rel_id)
    }

    /// Get all distinct relationship types.
    ///
    /// Uses direct SQL query on the normalized rel_types table for O(distinct_types)
    /// performance instead of O(relationships) via Cypher MATCH.
    pub fn get_all_relationship_types(&self) -> Result<Vec<String>> {
        let storage = self.get_read_storage();
        storage.get_all_relationship_types()
    }

    /// Get all distinct node labels.
    ///
    /// Uses direct SQL query on the normalized node_labels table for O(distinct_labels)
    /// performance instead of O(nodes) via Cypher MATCH.
    pub fn get_all_labels(&self) -> Result<Vec<String>> {
        let storage = self.get_read_storage();
        storage.get_all_labels()
    }

    /// Set a numeric property on all relationships for each given type, in one transaction.
    ///
    /// This is significantly faster than running one Cypher SET query per type because
    /// it avoids parse/plan overhead and batches all writes into a single SQLite transaction.
    pub fn update_relationship_property_by_types(
        &self,
        property: &str,
        values: &std::collections::HashMap<String, f64>,
    ) -> Result<usize> {
        self.require_writable()?;
        let mut storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let result = storage.update_relationship_property_by_types(property, values);
        self.invalidate_adjacency_cache();
        result
    }
}
