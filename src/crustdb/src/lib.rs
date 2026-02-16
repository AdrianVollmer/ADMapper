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
        // TODO: Parse and execute Cypher query
        let _ = query;
        todo!("Cypher execution not yet implemented")
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
