//! Database module for graph storage.
//!
//! Supports multiple backends (all optional):
//! - CrustDB (Cypher-based, SQLite) - use `--features crustdb`
//! - Neo4j (Cypher-based, network) - use `--features neo4j`
//! - FalkorDB (Cypher-based, Redis) - use `--features falkordb`

pub mod algorithms;
pub mod backend;
#[cfg(feature = "crustdb")]
pub mod crustdb;
#[cfg(feature = "falkordb")]
pub mod falkordb;
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
#[cfg(feature = "crustdb")]
pub use crustdb::CrustDatabase;
#[cfg(feature = "falkordb")]
pub use falkordb::FalkorDbDatabase;
#[cfg(feature = "neo4j")]
pub use neo4j::Neo4jDatabase;
