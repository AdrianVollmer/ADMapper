//! Database backend trait for multi-database support.
//!
//! Defines the common interface that all database backends must implement.

use serde_json::Value as JsonValue;
use std::str::FromStr;

use super::types::{
    DbEdge, DbError, DbNode, DetailedStats, QueryHistoryRow, Result, SecurityInsights,
};

/// Query language supported by a database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLanguage {
    /// Cypher query language (Neo4j, KuzuDB, FalkorDB)
    Cypher,
    /// Datalog query language (CozoDB)
    Datalog,
}

impl FromStr for QueryLanguage {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cypher" => Ok(QueryLanguage::Cypher),
            "datalog" => Ok(QueryLanguage::Datalog),
            other => Err(format!("Unknown query language: {}", other)),
        }
    }
}

/// Trait defining the common interface for all database backends.
///
/// This allows the application to work with multiple database backends
/// (Neo4j, FalkorDB, CozoDB, KuzuDB) through a unified interface.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
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

    /// Get edge counts for a node (for badge display).
    /// Returns (incoming, outgoing, admin_to, member_of, members).
    ///
    /// # Performance Warning
    /// The default implementation loads ALL edges into memory and scans them linearly.
    /// For large graphs (100k+ edges), this is severely inefficient.
    /// All backends should override this method with an efficient indexed query.
    fn get_node_edge_counts(&self, node_id: &str) -> Result<(usize, usize, usize, usize, usize)> {
        // WARNING: This default implementation is O(n) where n = total edges.
        // Backends should override with efficient indexed queries.
        tracing::warn!(
            node_id = %node_id,
            "Using inefficient default get_node_edge_counts - backend should override"
        );
        let all_edges = self.get_all_edges()?;

        let admin_types: std::collections::HashSet<&str> = [
            "AdminTo",
            "GenericAll",
            "GenericWrite",
            "Owns",
            "WriteDacl",
            "WriteOwner",
            "AllExtendedRights",
            "ForceChangePassword",
            "AddMember",
        ]
        .into_iter()
        .collect();

        let mut incoming = 0;
        let mut outgoing = 0;
        let mut admin_to = 0;
        let mut member_of = 0;
        let mut members = 0;

        for edge in &all_edges {
            if edge.target == node_id {
                incoming += 1;
                if edge.edge_type == "MemberOf" {
                    members += 1;
                }
            }
            if edge.source == node_id {
                outgoing += 1;
                if edge.edge_type == "MemberOf" {
                    member_of += 1;
                }
                if admin_types.contains(edge.edge_type.as_str()) {
                    admin_to += 1;
                }
            }
        }

        Ok((incoming, outgoing, admin_to, member_of, members))
    }

    /// Check if a node is a transitive member of a target group.
    /// Uses MemberOf edges to traverse group membership.
    fn is_member_of(&self, node_id: &str, target_id: &str) -> Result<bool> {
        // Default implementation using BFS over MemberOf edges
        let all_edges = self.get_all_edges()?;

        // Build adjacency for MemberOf edges only
        let mut member_of_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for edge in &all_edges {
            if edge.edge_type == "MemberOf" {
                member_of_adj
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
        }

        // BFS from node_id to find if we can reach target_id
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(node_id.to_string());
        visited.insert(node_id.to_string());

        while let Some(current) = queue.pop_front() {
            if current == target_id {
                return Ok(true);
            }
            if let Some(targets) = member_of_adj.get(&current) {
                for target in targets {
                    if !visited.contains(target) {
                        visited.insert(target.clone());
                        queue.push_back(target.clone());
                    }
                }
            }
        }

        Ok(false)
    }

    /// Find the first group matching a SID suffix that the node is a member of.
    /// Returns the group's object_id if found.
    ///
    /// # Performance Warning
    /// The default implementation loads ALL nodes and ALL edges into memory.
    /// For large graphs (50k+ nodes, 200k+ edges), this is severely inefficient.
    /// Called multiple times on node hover, this can freeze the UI.
    /// All backends should override this with an efficient graph traversal query.
    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        // WARNING: This default implementation is O(n+m) where n = nodes, m = edges.
        // Backends should override with efficient graph traversal queries.
        tracing::warn!(
            node_id = %node_id,
            sid_suffix = %sid_suffix,
            "Using inefficient default find_membership_by_sid_suffix - backend should override"
        );
        let all_nodes = self.get_all_nodes()?;
        let all_edges = self.get_all_edges()?;

        // Find all groups with matching SID suffix
        let target_groups: Vec<&str> = all_nodes
            .iter()
            .filter(|n| n.id.ends_with(sid_suffix))
            .map(|n| n.id.as_str())
            .collect();

        if target_groups.is_empty() {
            return Ok(None);
        }

        // Build adjacency for MemberOf edges
        let mut member_of_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for edge in &all_edges {
            if edge.edge_type == "MemberOf" {
                member_of_adj
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
        }

        // BFS from node_id
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(node_id.to_string());
        visited.insert(node_id.to_string());

        while let Some(current) = queue.pop_front() {
            // Check if current is one of the target groups
            for &target in &target_groups {
                if current == target {
                    return Ok(Some(target.to_string()));
                }
            }
            if let Some(targets) = member_of_adj.get(&current) {
                for target in targets {
                    if !visited.contains(target) {
                        visited.insert(target.clone());
                        queue.push_back(target.clone());
                    }
                }
            }
        }

        Ok(None)
    }

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
            return Err(DbError::Database(format!(
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

    /// Add a query to history with status tracking.
    fn add_query_history(
        &self,
        id: &str,
        name: &str,
        query: &str,
        timestamp: i64,
        result_count: Option<i64>,
        status: &str,
        started_at: i64,
        duration_ms: Option<u64>,
        error: Option<&str>,
        background: bool,
    ) -> Result<()>;

    /// Update a query's status in history.
    fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()>;

    /// Get query history with pagination.
    /// Returns: (history_rows, total_count)
    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)>;

    /// Delete a query from history.
    fn delete_query_history(&self, id: &str) -> Result<()>;

    /// Clear all query history.
    fn clear_query_history(&self) -> Result<()>;
}
