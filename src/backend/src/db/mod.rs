//! Database module for graph storage.
//!
//! Supports multiple backends (all optional):
//! - KuzuDB (Cypher-based, file) - default, use `--features kuzu`
//! - CozoDB (Datalog-based, sled) - use `--features cozo`
//! - CrustDB (Cypher-based, SQLite) - use `--features crustdb`
//! - Neo4j (Cypher-based, network) - use `--features neo4j`
//! - FalkorDB (Cypher-based, Redis) - use `--features falkordb`

pub mod backend;
#[cfg(feature = "cozo")]
pub mod cozo;
#[cfg(feature = "crustdb")]
pub mod crustdb;
#[cfg(feature = "falkordb")]
pub mod falkordb;
#[cfg(feature = "kuzu")]
pub mod kuzu;
#[cfg(feature = "neo4j")]
pub mod neo4j;
pub mod types;
pub mod url;

// Re-export common types
pub use backend::{DatabaseBackend, QueryLanguage};
pub use types::{
    ChokePointsResponse, DbEdge, DbError, DbNode, DetailedStats, NewQueryHistoryEntry, Result,
    SecurityInsights,
};
pub use url::{DatabaseType, DatabaseUrl};

// Re-export database implementations
#[cfg(feature = "cozo")]
pub use cozo::GraphDatabase as CozoDatabase;
#[cfg(feature = "crustdb")]
pub use crustdb::CrustDatabase;
#[cfg(feature = "falkordb")]
pub use falkordb::FalkorDbDatabase;
#[cfg(feature = "kuzu")]
pub use kuzu::KuzuDatabase;
#[cfg(feature = "neo4j")]
pub use neo4j::Neo4jDatabase;
