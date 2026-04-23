//! Database backend trait for multi-database support.
//!
//! Defines the common interface that all database backends must implement.

use serde_json::Value as JsonValue;
use std::str::FromStr;

use super::types::{
    ChokePointsResponse, DbEdge, DbError, DbNode, DetailedStats, Result, SecurityInsights,
};

/// Query language supported by a database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLanguage {
    /// Cypher query language (CrustDB, Neo4j, FalkorDB)
    Cypher,
}

impl FromStr for QueryLanguage {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cypher" => Ok(QueryLanguage::Cypher),
            other => Err(format!("Unknown query language: {}", other)),
        }
    }
}

/// Trait defining the common interface for all database backends.
///
/// This allows the application to work with multiple database backends
/// (CrustDB, Neo4j, FalkorDB) through a unified interface.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub trait DatabaseBackend: Send + Sync {
    /// Get the name of this database backend.
    fn name(&self) -> &'static str;

    /// Check if this backend supports the given query language.
    fn supports_language(&self, lang: QueryLanguage) -> bool;

    /// Get the default query language for this backend.
    fn default_language(&self) -> QueryLanguage;

    /// Verify the database connection is alive and credentials are valid.
    ///
    /// This should perform a lightweight query to confirm connectivity.
    fn ping(&self) -> Result<()>;

    // ========================================================================
    // Basic CRUD Operations
    // ========================================================================

    /// Clear all data from the database.
    fn clear(&self) -> Result<()>;

    /// Insert a single node.
    fn insert_node(&self, node: DbNode) -> Result<()>;

    /// Insert a single relationship.
    fn insert_edge(&self, relationship: DbEdge) -> Result<()>;

    /// Insert a batch of nodes.
    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize>;

    /// Insert a batch of relationships.
    fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize>;

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get basic node and relationship counts.
    fn get_stats(&self) -> Result<(usize, usize)>;

    /// Get detailed statistics including counts by type.
    fn get_detailed_stats(&self) -> Result<DetailedStats>;

    /// Get security insights from the graph.
    fn get_security_insights(&self) -> Result<SecurityInsights>;

    /// Get choke points using relationship betweenness centrality.
    ///
    /// Returns the top relationships through which the most shortest paths pass.
    /// These are critical relationships whose removal would disrupt the most attack paths.
    ///
    /// Default implementation fetches all nodes/edges and runs Brandes' algorithm.
    /// Backends with native graph analytics can override for better performance.
    fn get_choke_points(&self, top_k: usize) -> Result<ChokePointsResponse> {
        let nodes = self.get_all_nodes()?;
        let edges = self.get_all_edges()?;
        Ok(super::algorithms::relationship_betweenness_centrality(
            &nodes, &edges, true, top_k,
        ))
    }

    // ========================================================================
    // Node/Relationship Retrieval
    // ========================================================================

    /// Get all nodes.
    fn get_all_nodes(&self) -> Result<Vec<DbNode>>;

    /// Get all relationships.
    fn get_all_edges(&self) -> Result<Vec<DbEdge>>;

    /// Get nodes by their IDs.
    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>>;

    /// Get relationships between a set of nodes.
    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>>;

    /// Get all distinct relationship types.
    fn get_relationship_types(&self) -> Result<Vec<String>>;

    /// Get all distinct node types.
    fn get_node_types(&self) -> Result<Vec<String>>;

    // ========================================================================
    // Search
    // ========================================================================

    /// Search nodes by name/objectid (case-insensitive substring match).
    /// If `label` is `Some`, restrict to nodes with that Cypher label.
    fn search_nodes(&self, query: &str, limit: usize, label: Option<&str>) -> Result<Vec<DbNode>>;

    /// Resolve a node identifier (object ID or label) to an object ID.
    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>>;

    // ========================================================================
    // Node Connections
    // ========================================================================

    /// Get connections for a node.
    /// Returns (nodes, relationships) for the connections in the specified direction.
    /// - `incoming`: relationships where node is target
    /// - `outgoing`: relationships where node is source
    /// - `admin`: outgoing admin permission relationships (AdminTo, GenericAll, etc.)
    /// - `memberof`: outgoing MemberOf relationships
    /// - `members`: incoming MemberOf relationships
    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)>;

    /// Get relationship counts for a node (for badge display).
    /// Returns (incoming, outgoing, admin_to, member_of, members).
    ///
    /// Each backend must provide an efficient implementation using indexed queries.
    fn get_node_relationship_counts(
        &self,
        node_id: &str,
    ) -> Result<(usize, usize, usize, usize, usize)>;

    /// Check if a node is a transitive member of a target group.
    /// Uses MemberOf relationships to traverse group membership.
    ///
    /// Each backend must provide an efficient implementation (e.g., BFS over
    /// per-node edges for in-memory backends, or a graph traversal query for
    /// Cypher-based backends).
    fn is_member_of(&self, node_id: &str, target_id: &str) -> Result<bool>;

    /// Find the first group matching a SID suffix that the node is a member of.
    /// Returns the group's objectid if found.
    ///
    /// Each backend must provide an efficient implementation using graph
    /// traversal queries rather than loading all nodes and edges into memory.
    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>>;

    // ========================================================================
    // Path Finding
    // ========================================================================

    /// Find shortest path between two nodes.
    /// Returns the path as a list of (node_id, rel_type) pairs.
    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>>;

    /// Find all users with paths to Domain Admins.
    fn find_paths_to_domain_admins(
        &self,
        exclude_relationship_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>>;

    // ========================================================================
    // Custom Query
    // ========================================================================

    /// Set exploit_likelihood on all edges for each given relationship type.
    ///
    /// The default implementation issues one Cypher SET query per type (safe for all
    /// backends). CrustDB overrides this with a single SQL transaction for much better
    /// performance — avoiding per-query parse/plan overhead and WAL round-trips.
    fn update_exploit_likelihoods(
        &self,
        likelihoods: &std::collections::HashMap<String, f64>,
    ) -> Result<usize> {
        let mut updated = 0;
        for (rel_type, likelihood) in likelihoods {
            let safe_type: String = rel_type
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if safe_type.is_empty() {
                continue;
            }
            let query = format!(
                "MATCH ()-[r:{}]->() SET r.exploit_likelihood = {}",
                safe_type, likelihood
            );
            self.run_custom_query(&query)?;
            updated += 1;
        }
        Ok(updated)
    }

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
    // Query Cache (optional, CrustDB only)
    // ========================================================================

    /// Get cache statistics (entry count, size in bytes).
    /// Returns None if this backend doesn't support caching.
    fn get_cache_stats(&self) -> Result<Option<(usize, usize)>> {
        Ok(None)
    }

    /// Clear the query cache.
    /// Returns Ok(false) if this backend doesn't support caching.
    fn clear_cache(&self) -> Result<bool> {
        Ok(false)
    }

    /// Get database file size in bytes.
    /// Returns None if not applicable (e.g., remote databases).
    fn get_database_size(&self) -> Result<Option<usize>> {
        Ok(None)
    }
}
