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
pub use query::QueryResult;

use std::path::Path;
use storage::SqliteStorage;

/// Main database handle.
pub struct Database {
    storage: SqliteStorage,
}

impl Database {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = SqliteStorage::open(path)?;
        Ok(Self { storage })
    }

    /// Create an in-memory database.
    pub fn in_memory() -> Result<Self> {
        let storage = SqliteStorage::in_memory()?;
        Ok(Self { storage })
    }

    /// Execute a Cypher query.
    pub fn execute(&self, query: &str) -> Result<QueryResult> {
        let statement = query::parser::parse(query)?;
        query::executor::execute(&statement, &self.storage)
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        self.storage.stats()
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

        let result = db.execute("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();

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

        let result = db.execute(
            "CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)"
        ).unwrap();

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
}
