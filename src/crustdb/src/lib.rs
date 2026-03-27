//! CrustDB - Embedded Graph Database
//!
//! A lightweight, embedded graph database with:
//! - SQLite storage backend
//! - Cypher query language support
//! - Property graph model
//! - Optional in-memory entity cache for traversal performance

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
//!
//! # Entity Cache
//!
//! For workloads involving graph traversals (BFS, shortest path), enabling the
//! entity cache can significantly improve performance by reducing SQLite lookups
//! for repeatedly accessed nodes and relationships.
//!
//! ```no_run
//! use crustdb::{Database, EntityCacheConfig};
//!
//! let db = Database::open("my_graph.db").unwrap();
//!
//! // Enable caching with 500k entries for both nodes and relationships
//! db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));
//!
//! // Now shortest path queries will cache nodes/relationships during traversal
//! db.execute("MATCH p = shortestPath((a:User)-[*]->(b:Domain)) RETURN p").unwrap();
//!
//! // Check cache statistics
//! let stats = db.entity_cache_stats();
//! println!("Node hit rate: {:.1}%", stats.node_hit_rate() * 100.0);
//! println!("Relationship hit rate: {:.1}%", stats.relationship_hit_rate() * 100.0);
//!
//! // Disable caching when not needed
//! db.set_entity_cache(EntityCacheConfig::disabled());
//! ```
//!
//! The cache is automatically cleared on write operations (CREATE, SET, DELETE)
//! to maintain consistency. For read-heavy workloads with graph traversals,
//! a cache capacity of 100k-500k entries is recommended.

mod database;
mod error;
mod graph;
mod query;
mod storage;

pub use database::{Database, DatabaseStats, NewQueryHistoryEntry, QueryHistoryRow};
pub use error::{Error, Result};
pub use graph::{Node, Path, PropertyValue, Relationship};
pub use query::executor::algorithms::RelationshipBetweenness;
pub use query::executor::ResourceLimits;
pub use query::{QueryResult, QueryStats, ResultValue, Row};
pub use storage::CacheStats;
pub use storage::{EntityCache, EntityCacheConfig, EntityCacheStats};
