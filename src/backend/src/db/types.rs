//! Common types for all database backends.

use serde_json::Value as JsonValue;
use thiserror::Error;

/// A node stored in the database.
#[derive(Clone, Debug)]
pub struct DbNode {
    pub id: String,
    pub label: String,
    pub node_type: String,
    pub properties: JsonValue,
}

/// An edge stored in the database.
#[derive(Clone, Debug)]
pub struct DbEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: JsonValue,
}

/// Detailed statistics about the database.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DetailedStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub users: usize,
    pub computers: usize,
    pub groups: usize,
    pub domains: usize,
    pub ous: usize,
    pub gpos: usize,
}

/// Security insight for a well-known principal reachability.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ReachabilityInsight {
    pub principal_name: String,
    pub principal_id: Option<String>,
    pub reachable_count: usize,
}

/// Security insights computed from the graph.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SecurityInsights {
    /// Users who have a path to Domain Admins
    pub effective_da_count: usize,
    /// Users who are direct or transitive members of Domain Admins
    pub real_da_count: usize,
    /// Ratio of effective DAs to real DAs
    pub da_ratio: f64,
    /// Total users in the database
    pub total_users: usize,
    /// Percentage of users that are effective DAs
    pub effective_da_percentage: f64,
    /// Objects reachable from well-known principals
    pub reachability: Vec<ReachabilityInsight>,
    /// Users with paths to Domain Admins (for export)
    pub effective_das: Vec<(String, String, usize)>,
    /// Users who are members of Domain Admins (for export)
    pub real_das: Vec<(String, String)>,
}

/// Database error type.
#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(feature = "cozo")]
impl From<cozo::Error> for DbError {
    fn from(e: cozo::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "kuzu")]
impl From<kuzu::Error> for DbError {
    fn from(e: kuzu::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "crustdb")]
impl From<crustdb::Error> for DbError {
    fn from(e: crustdb::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "neo4j")]
impl From<neo4rs::Error> for DbError {
    fn from(e: neo4rs::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "falkordb")]
impl From<falkordb::FalkorDBError> for DbError {
    fn from(e: falkordb::FalkorDBError) -> Self {
        DbError::Database(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DbError>;
