//! Database module for graph storage.
//!
//! Supports multiple backends (all optional):
//! - KuzuDB (Cypher-based, file) - default, use `--features kuzu`
//! - CozoDB (Datalog-based, sled) - use `--features cozo`
//! - CrustDB (Cypher-based, SQLite) - use `--features crustdb`
//! - Neo4j (Cypher-based, network) - stub
//! - FalkorDB (Cypher-based, Redis) - stub

pub mod backend;
#[cfg(feature = "cozo")]
pub mod cozo;
#[cfg(feature = "crustdb")]
pub mod crustdb;
pub mod falkordb;
#[cfg(feature = "kuzu")]
pub mod kuzu;
pub mod neo4j;
pub mod types;
pub mod url;

// Re-export common types
pub use backend::{DatabaseBackend, QueryLanguage};
pub use types::{DbEdge, DbError, DbNode, DetailedStats, SecurityInsights};
pub use url::{DatabaseType, DatabaseUrl};

// Re-export database implementations
#[cfg(feature = "cozo")]
pub use cozo::GraphDatabase as CozoDatabase;
#[cfg(feature = "crustdb")]
pub use crustdb::CrustDatabase;
pub use falkordb::FalkorDbDatabase;
#[cfg(feature = "kuzu")]
pub use kuzu::KuzuDatabase;
pub use neo4j::Neo4jDatabase;
