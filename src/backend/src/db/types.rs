//! Common types for all database backends.

use serde::Serialize;
use serde_json::Value as JsonValue;
use thiserror::Error;

/// A node stored in the database.
///
/// This type is used both for internal storage and API responses.
/// The `node_type` field is serialized as "type" for API compatibility.
#[derive(Clone, Debug, Serialize)]
pub struct DbNode {
    pub id: String,
    pub label: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub properties: JsonValue,
}

/// An edge stored in the database.
#[derive(Clone, Debug, Default)]
pub struct DbEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: JsonValue,
    /// Optional type hint for source node (for creating placeholders)
    pub source_type: Option<String>,
    /// Optional type hint for target node (for creating placeholders)
    pub target_type: Option<String>,
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

/// Well-known principals to check for reachability in security insights.
///
/// Format: (display_name, SID_pattern)
/// - Patterns starting with '-' are domain-relative SID suffixes
/// - Other patterns are exact well-known SIDs
pub const WELL_KNOWN_PRINCIPALS: &[(&str, &str)] = &[
    ("Everyone", "S-1-1-0"),
    ("Authenticated Users", "S-1-5-11"),
    ("Domain Users", "-513"),
    ("Domain Computers", "-515"),
];

/// SID suffix for Domain Admins group.
pub const DOMAIN_ADMIN_SID_SUFFIX: &str = "-512";

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

impl SecurityInsights {
    /// Create SecurityInsights with computed ratios from raw counts.
    ///
    /// This helper reduces duplication across backend implementations.
    pub fn from_counts(
        total_users: usize,
        real_das: Vec<(String, String)>,
        effective_das: Vec<(String, String, usize)>,
        reachability: Vec<ReachabilityInsight>,
    ) -> Self {
        let real_da_count = real_das.len();
        let effective_da_count = effective_das.len();

        let da_ratio = if real_da_count > 0 {
            effective_da_count as f64 / real_da_count as f64
        } else {
            0.0
        };

        let effective_da_percentage = if total_users > 0 {
            (effective_da_count as f64 / total_users as f64) * 100.0
        } else {
            0.0
        };

        Self {
            effective_da_count,
            real_da_count,
            da_ratio,
            total_users,
            effective_da_percentage,
            reachability,
            effective_das,
            real_das,
        }
    }
}

/// A row from the query history table.
#[derive(Clone, Debug)]
pub struct QueryHistoryRow {
    pub id: String,
    pub name: String,
    pub query: String,
    pub timestamp: i64,
    pub result_count: Option<i64>,
    pub status: String,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
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
