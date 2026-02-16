//! Neo4j database backend (stub implementation).
//!
//! Requires `neo4rs` crate for full implementation.

use serde_json::Value as JsonValue;

use super::backend::{DatabaseBackend, QueryLanguage, Result};
use super::cozo::{DbEdge, DbError, DbNode, DetailedStats, SecurityInsights};

/// Neo4j database backend.
///
/// This is currently a stub implementation. Full implementation requires
/// the `neo4rs` crate and an async runtime.
pub struct Neo4jDatabase {
    _connection_string: String,
    _username: Option<String>,
    _password: Option<String>,
    _database: Option<String>,
}

impl Neo4jDatabase {
    /// Create a new Neo4j database connection.
    pub fn new(
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
        database: Option<String>,
    ) -> Result<Self> {
        let connection_string = format!("bolt://{}:{}", host, port);

        // TODO: Actually establish connection using neo4rs
        // For now, just store the configuration
        Ok(Self {
            _connection_string: connection_string,
            _username: username,
            _password: password,
            _database: database,
        })
    }

    fn not_implemented<T>(&self) -> Result<T> {
        Err(DbError::Cozo(
            "Neo4j backend is not yet implemented".to_string(),
        ))
    }
}

impl DatabaseBackend for Neo4jDatabase {
    fn name(&self) -> &'static str {
        "Neo4j"
    }

    fn supports_language(&self, lang: QueryLanguage) -> bool {
        matches!(lang, QueryLanguage::Cypher)
    }

    fn default_language(&self) -> QueryLanguage {
        QueryLanguage::Cypher
    }

    fn clear(&self) -> Result<()> {
        self.not_implemented()
    }

    fn insert_node(&self, _node: DbNode) -> Result<()> {
        self.not_implemented()
    }

    fn insert_edge(&self, _edge: DbEdge) -> Result<()> {
        self.not_implemented()
    }

    fn insert_nodes(&self, _nodes: &[DbNode]) -> Result<usize> {
        self.not_implemented()
    }

    fn insert_edges(&self, _edges: &[DbEdge]) -> Result<usize> {
        self.not_implemented()
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        self.not_implemented()
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        self.not_implemented()
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        self.not_implemented()
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        self.not_implemented()
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        self.not_implemented()
    }

    fn get_nodes_by_ids(&self, _ids: &[String]) -> Result<Vec<DbNode>> {
        self.not_implemented()
    }

    fn get_edges_between(&self, _node_ids: &[String]) -> Result<Vec<DbEdge>> {
        self.not_implemented()
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        self.not_implemented()
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        self.not_implemented()
    }

    fn search_nodes(&self, _query: &str, _limit: usize) -> Result<Vec<DbNode>> {
        self.not_implemented()
    }

    fn resolve_node_identifier(&self, _identifier: &str) -> Result<Option<String>> {
        self.not_implemented()
    }

    fn get_node_connections(
        &self,
        _node_id: &str,
        _direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        self.not_implemented()
    }

    fn shortest_path(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        self.not_implemented()
    }

    fn find_paths_to_domain_admins(
        &self,
        _exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        self.not_implemented()
    }

    fn run_custom_query(&self, _query: &str) -> Result<JsonValue> {
        self.not_implemented()
    }

    fn add_query_history(
        &self,
        _id: &str,
        _name: &str,
        _query: &str,
        _timestamp: i64,
        _result_count: Option<i64>,
    ) -> Result<()> {
        self.not_implemented()
    }

    fn get_query_history(
        &self,
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<(String, String, String, i64, Option<i64>)>, usize)> {
        self.not_implemented()
    }

    fn delete_query_history(&self, _id: &str) -> Result<()> {
        self.not_implemented()
    }

    fn clear_query_history(&self) -> Result<()> {
        self.not_implemented()
    }
}
