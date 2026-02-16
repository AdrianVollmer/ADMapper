//! Database backend trait for multi-database support.
//!
//! Defines the common interface that all database backends must implement.

use serde_json::Value as JsonValue;

use super::cozo::{DbEdge, DbError, DbNode, DetailedStats, SecurityInsights};

pub type Result<T> = std::result::Result<T, DbError>;

/// Query language supported by a database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLanguage {
    /// Cypher query language (Neo4j, KuzuDB, FalkorDB)
    Cypher,
    /// Datalog query language (CozoDB)
    Datalog,
}

impl QueryLanguage {
    /// Parse from string representation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cypher" => Some(QueryLanguage::Cypher),
            "datalog" => Some(QueryLanguage::Datalog),
            _ => None,
        }
    }
}

/// Trait defining the common interface for all database backends.
///
/// This allows the application to work with multiple database backends
/// (Neo4j, FalkorDB, CozoDB, KuzuDB) through a unified interface.
pub trait DatabaseBackend: Send + Sync {
    /// Get the name of this database backend.
    fn name(&self) -> &'static str;

    /// Check if this backend supports the given query language.
    fn supports_language(&self, lang: QueryLanguage) -> bool;

    /// Get the default query language for this backend.
    fn default_language(&self) -> QueryLanguage;

    // ========================================================================
    // Basic CRUD Operations
    // ========================================================================

    /// Clear all data from the database.
    fn clear(&self) -> Result<()>;

    /// Insert a single node.
    fn insert_node(&self, node: DbNode) -> Result<()>;

    /// Insert a single edge.
    fn insert_edge(&self, edge: DbEdge) -> Result<()>;

    /// Insert a batch of nodes.
    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize>;

    /// Insert a batch of edges.
    fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize>;

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get basic node and edge counts.
    fn get_stats(&self) -> Result<(usize, usize)>;

    /// Get detailed statistics including counts by type.
    fn get_detailed_stats(&self) -> Result<DetailedStats>;

    /// Get security insights from the graph.
    fn get_security_insights(&self) -> Result<SecurityInsights>;

    // ========================================================================
    // Node/Edge Retrieval
    // ========================================================================

    /// Get all nodes.
    fn get_all_nodes(&self) -> Result<Vec<DbNode>>;

    /// Get all edges.
    fn get_all_edges(&self) -> Result<Vec<DbEdge>>;

    /// Get nodes by their IDs.
    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>>;

    /// Get edges between a set of nodes.
    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>>;

    /// Get all distinct edge types.
    fn get_edge_types(&self) -> Result<Vec<String>>;

    /// Get all distinct node types.
    fn get_node_types(&self) -> Result<Vec<String>>;

    // ========================================================================
    // Search
    // ========================================================================

    /// Search nodes by label (case-insensitive substring match).
    fn search_nodes(&self, query: &str, limit: usize) -> Result<Vec<DbNode>>;

    /// Resolve a node identifier (object ID or label) to an object ID.
    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>>;

    // ========================================================================
    // Node Connections
    // ========================================================================

    /// Get connections for a node.
    /// Returns (nodes, edges) for the connections in the specified direction.
    /// - `incoming`: edges where node is target
    /// - `outgoing`: edges where node is source
    /// - `admin`: outgoing admin permission edges (AdminTo, GenericAll, etc.)
    /// - `memberof`: outgoing MemberOf edges
    /// - `members`: incoming MemberOf edges
    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)>;

    // ========================================================================
    // Path Finding
    // ========================================================================

    /// Find shortest path between two nodes.
    /// Returns the path as a list of (node_id, edge_type) pairs.
    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>>;

    /// Find all users with paths to Domain Admins.
    fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>>;

    // ========================================================================
    // Custom Query
    // ========================================================================

    /// Run a custom query in the backend's native language.
    fn run_custom_query(&self, query: &str) -> Result<JsonValue>;

    /// Run a custom query with explicit language specification.
    /// Returns an error if the language is not supported.
    fn run_query_with_language(&self, query: &str, lang: QueryLanguage) -> Result<JsonValue> {
        if !self.supports_language(lang) {
            return Err(DbError::Cozo(format!(
                "Database backend '{}' does not support {:?} queries",
                self.name(),
                lang
            )));
        }
        self.run_custom_query(query)
    }

    // ========================================================================
    // Query History
    // ========================================================================

    /// Add a query to history.
    fn add_query_history(
        &self,
        id: &str,
        name: &str,
        query: &str,
        timestamp: i64,
        result_count: Option<i64>,
    ) -> Result<()>;

    /// Get query history with pagination.
    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<(String, String, String, i64, Option<i64>)>, usize)>;

    /// Delete a query from history.
    fn delete_query_history(&self, id: &str) -> Result<()>;

    /// Clear all query history.
    fn clear_query_history(&self) -> Result<()>;
}
