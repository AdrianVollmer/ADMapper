//! Database module for graph storage.
//!
//! Supports multiple backends:
//! - CozoDB (Datalog-based)
//! - KuzuDB (Cypher-based) - currently active
//! - Neo4j (Cypher-based, network) - stub
//! - FalkorDB (Cypher-based, Redis) - stub

pub mod backend;
pub mod cozo;
pub mod falkordb;
pub mod kuzu;
pub mod neo4j;
pub mod url;

// Re-export common types
pub use backend::{DatabaseBackend, QueryLanguage};
pub use cozo::{DbEdge, DbError, DbNode, DetailedStats, SecurityInsights};
pub use url::{DatabaseType, DatabaseUrl};

// Re-export database implementations
pub use cozo::GraphDatabase as CozoDatabase;
pub use falkordb::FalkorDbDatabase;
pub use kuzu::KuzuDatabase;
pub use neo4j::Neo4jDatabase;
