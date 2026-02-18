//! CrustDB - Embedded Graph Database
//!
//! A lightweight, embedded graph database with:
//! - SQLite storage backend
//! - Cypher query language support
//! - Property graph model
//!
//! # Example
//!
//! ```no_run
//! use crustdb::Database;
//!
//! let db = Database::open("my_graph.db").unwrap();
//!
//! // Create nodes
//! db.execute("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
//! db.execute("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
//!
//! // Create relationship
//! db.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//!             CREATE (a)-[:KNOWS]->(b)").unwrap();
//!
//! // Query
//! let results = db.execute("MATCH (n:Person) RETURN n.name, n.age").unwrap();
//! ```

mod error;
mod graph;
mod query;
mod storage;

pub use error::{Error, Result};
pub use graph::{Edge, Node, PropertyValue};
pub use query::{QueryResult, QueryStats, ResultValue, Row};

use std::path::Path;
use std::sync::Mutex;
use storage::SqliteStorage;

/// Main database handle.
///
/// Uses Mutex for thread-safety. For concurrent queries, consider using
/// a connection pool in the future.
pub struct Database {
    storage: Mutex<SqliteStorage>,
}

impl Database {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = SqliteStorage::open(path)?;
        Ok(Self {
            storage: Mutex::new(storage),
        })
    }

    /// Create an in-memory database.
    pub fn in_memory() -> Result<Self> {
        let storage = SqliteStorage::in_memory()?;
        Ok(Self {
            storage: Mutex::new(storage),
        })
    }

    /// Execute a Cypher query.
    pub fn execute(&self, query: &str) -> Result<QueryResult> {
        let statement = query::parser::parse(query)?;
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        query::executor::execute(&statement, &storage)
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.stats()
    }

    /// Clear all data from the database.
    /// This is much faster than using Cypher DELETE queries.
    pub fn clear(&self) -> Result<()> {
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.clear()
    }

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
        let mut storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.insert_nodes_batch(nodes)
    }

    /// Insert multiple edges in a single transaction.
    ///
    /// Each edge is specified as (source_node_id, target_node_id, edge_type, properties).
    /// Returns a vector of the created edge IDs in the same order as the input.
    ///
    /// Use `find_node_by_property` or `build_property_index` to look up node IDs first.
    pub fn insert_edges_batch(
        &self,
        edges: &[(i64, i64, String, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        let mut storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.insert_edges_batch(edges)
    }

    /// Find a node ID by a property value.
    ///
    /// Searches for nodes where the JSON properties contain the specified key-value pair.
    pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>> {
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.find_node_by_property(property, value)
    }

    /// Build an index of property values to node IDs for efficient batch lookups.
    ///
    /// This is useful when inserting edges that reference nodes by a property value
    /// (like object_id) rather than by database ID.
    pub fn build_property_index(
        &self,
        property: &str,
    ) -> Result<std::collections::HashMap<String, i64>> {
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.build_property_index(property)
    }

    /// Get all edges for a node by object_id (both incoming and outgoing).
    ///
    /// Returns (source_object_id, target_object_id, edge_type) tuples.
    /// This is more efficient than using Cypher queries for edge retrieval.
    pub fn get_node_edges_by_object_id(
        &self,
        object_id: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let storage = self.storage.lock().map_err(|e| Error::Internal(e.to_string()))?;
        storage.get_node_edges_by_object_id(object_id)
    }
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Total number of nodes.
    pub node_count: usize,
    /// Total number of edges.
    pub edge_count: usize,
    /// Number of distinct node labels.
    pub label_count: usize,
    /// Number of distinct edge types.
    pub edge_type_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_create_single_node() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.label_count, 1);
    }

    #[test]
    fn test_database_create_relationship() {
        let db = Database::in_memory().unwrap();

        let result = db.execute(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})"
        ).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.edge_type_count, 1);
    }

    #[test]
    fn test_database_multiple_creates() {
        let db = Database::in_memory().unwrap();

        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_database_complex_pattern() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 3);
        assert_eq!(result.stats.relationships_created, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 2);
        assert_eq!(stats.edge_type_count, 2); // KNOWS, WORKS_AT
    }

    #[test]
    fn test_database_syntax_error() {
        let db = Database::in_memory().unwrap();

        let result = db.execute("CREATE n:Person");
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_insert_nodes() {
        let db = Database::in_memory().unwrap();

        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "object_id": "acme-3"}),
            ),
        ];

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 3);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_batch_insert_edges() {
        let db = Database::in_memory().unwrap();

        // Create nodes first
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "object_id": "acme-3"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(node_ids.len(), 3);

        // Create edges using node IDs
        let edges = vec![
            (
                node_ids[0],
                node_ids[1],
                "KNOWS".to_string(),
                serde_json::json!({"since": 2020}),
            ),
            (
                node_ids[0],
                node_ids[2],
                "WORKS_AT".to_string(),
                serde_json::json!({}),
            ),
        ];

        let edge_ids = db.insert_edges_batch(&edges).unwrap();
        assert_eq!(edge_ids.len(), 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 2);
        assert_eq!(stats.edge_type_count, 2);
    }

    #[test]
    fn test_property_index() {
        let db = Database::in_memory().unwrap();

        // Create nodes with object_id property
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();

        // Build property index
        let index = db.build_property_index("object_id").unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(index.get("alice-1"), Some(&node_ids[0]));
        assert_eq!(index.get("bob-2"), Some(&node_ids[1]));

        // Find node by property
        let found = db.find_node_by_property("object_id", "alice-1").unwrap();
        assert_eq!(found, Some(node_ids[0]));

        let not_found = db.find_node_by_property("object_id", "nobody").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_batch_insert_large() {
        let db = Database::in_memory().unwrap();

        // Create 1000 nodes in a batch
        let nodes: Vec<_> = (0..1000)
            .map(|i| {
                (
                    vec!["TestNode".to_string()],
                    serde_json::json!({"id": i, "object_id": format!("node-{}", i)}),
                )
            })
            .collect();

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 1000);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1000);
    }

    #[test]
    fn test_count_aggregate() {
        let db = Database::in_memory().unwrap();

        // Create some nodes
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        // Count all nodes
        let result = db.execute("MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(result.rows.len(), 1, "Should return single row");

        // Extract count
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 3, "Should count 3 nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }

        // Count by label
        let result = db.execute("MATCH (n:Person) RETURN count(n)").unwrap();
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 Person nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }

    #[test]
    fn test_count_edges() {
        let db = Database::in_memory().unwrap();

        // Create nodes with relationships
        db.execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        db.execute("CREATE (c:Person)-[:WORKS_AT]->(d:Company)")
            .unwrap();

        // Count all edges
        let result = db.execute("MATCH ()-[r]->() RETURN count(r)").unwrap();
        assert_eq!(result.rows.len(), 1);

        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 edges");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }
}
