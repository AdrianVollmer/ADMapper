//! Neo4j database backend.
//!
//! Uses the `neo4rs` crate for connecting to Neo4j via Bolt protocol.
//!
//! Shared Cypher logic lives in `cypher_common`; this file contains only
//! Neo4j-specific connection handling and methods that require
//! Neo4j-specific query syntax.

use neo4rs::{
    query, ConfigBuilder, Graph, Node as Neo4jNode, Path, Query, Relation, Row,
    UnboundedRelation,
};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::cypher_common::{self, CypherExecutor};
use super::types::{
    DbEdge, DbNode, DetailedStats, Result, SecurityInsights, DOMAIN_ADMIN_SID_SUFFIX,
};

/// Neo4j database backend.
pub struct Neo4jDatabase {
    graph: Arc<Graph>,
    /// Either use an existing runtime handle or our own runtime
    runtime: RuntimeHandle,
}

/// Either a handle to an existing runtime or an owned runtime
enum RuntimeHandle {
    Handle(Handle),
    Owned(Runtime),
}

impl RuntimeHandle {
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            RuntimeHandle::Handle(h) => tokio::task::block_in_place(|| h.block_on(future)),
            RuntimeHandle::Owned(rt) => rt.block_on(future),
        }
    }
}

impl Neo4jDatabase {
    /// Create a new Neo4j database connection.
    pub fn new(
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
        _database: Option<String>,
        use_ssl: bool,
    ) -> Result<Self> {
        let uri = if use_ssl {
            format!("neo4j+s://{}:{}", host, port)
        } else {
            format!("{}:{}", host, port)
        };
        let user = username.unwrap_or_else(|| "neo4j".to_string());
        let pass = password.unwrap_or_else(|| "neo4j".to_string());

        info!(uri = %uri, user = %user, use_ssl = %use_ssl, "Connecting to Neo4j");

        let runtime = match Handle::try_current() {
            Ok(handle) => RuntimeHandle::Handle(handle),
            Err(_) => {
                let rt = Runtime::new().map_err(|e| {
                    super::types::DbError::Database(format!("Failed to create runtime: {}", e))
                })?;
                RuntimeHandle::Owned(rt)
            }
        };

        let config = ConfigBuilder::default()
            .uri(&uri)
            .user(&user)
            .password(&pass)
            .max_connections(3)
            .build()?;
        let graph = Graph::connect(config)?;

        info!("Connected to Neo4j");

        Ok(Self {
            graph: Arc::new(graph),
            runtime,
        })
    }

    /// Convert a Neo4j node to DbNode.
    fn neo4j_node_to_db_node(node: &Neo4jNode) -> DbNode {
        let id = node
            .get::<String>("objectid")
            .or_else(|_| node.get::<i64>("id").map(|id| id.to_string()))
            .unwrap_or_else(|_| format!("node_{}", node.id()));

        let name = node
            .get::<String>("name")
            .or_else(|_| node.get::<String>("label"))
            .unwrap_or_else(|_| id.clone());

        let labels = node.labels();
        let label = labels
            .iter()
            .find(|l| **l != "Base")
            .or_else(|| labels.first())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let mut properties = Map::new();
        for key in node.keys() {
            if let Ok(val) = node.get::<String>(key) {
                properties.insert(key.to_string(), JsonValue::String(val));
            } else if let Ok(val) = node.get::<i64>(key) {
                properties.insert(key.to_string(), JsonValue::Number(val.into()));
            } else if let Ok(val) = node.get::<f64>(key) {
                if let Some(n) = serde_json::Number::from_f64(val) {
                    properties.insert(key.to_string(), JsonValue::Number(n));
                }
            } else if let Ok(val) = node.get::<bool>(key) {
                properties.insert(key.to_string(), JsonValue::Bool(val));
            }
        }

        DbNode {
            id,
            name,
            label,
            properties: JsonValue::Object(properties),
        }
    }

    /// Convert a Neo4j Row to a positional Vec<JsonValue>.
    ///
    /// Handles Node, Relation, Path, and scalar types so that shared logic
    /// in `cypher_common` can parse results identically to FalkorDB.
    fn row_to_json_vec(row: &Row) -> Vec<JsonValue> {
        let keys: Vec<String> = row.keys().iter().map(|k| k.to_string()).collect();
        keys.iter()
            .map(|col| {
                if let Ok(node) = row.get::<Neo4jNode>(col) {
                    let db_node = Self::neo4j_node_to_db_node(&node);
                    json!({
                        "_type": "node",
                        "id": node.id(),
                        "objectid": db_node.id,
                        "labels": node.labels(),
                        "properties": db_node.properties,
                    })
                } else if let Ok(rel) = row.get::<Relation>(col) {
                    Self::relation_to_json(&rel)
                } else if let Ok(path) = row.get::<Path>(col) {
                    Self::path_to_json(&path)
                } else if let Ok(val) = row.get::<String>(col) {
                    JsonValue::String(val)
                } else if let Ok(val) = row.get::<i64>(col) {
                    JsonValue::Number(val.into())
                } else if let Ok(val) = row.get::<f64>(col) {
                    serde_json::Number::from_f64(val)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null)
                } else if let Ok(val) = row.get::<bool>(col) {
                    JsonValue::Bool(val)
                } else if let Ok(val) = row.get::<Vec<String>>(col) {
                    JsonValue::Array(val.into_iter().map(JsonValue::String).collect())
                } else if let Ok(val) = row.get::<Vec<i64>>(col) {
                    JsonValue::Array(
                        val.into_iter()
                            .map(|v| JsonValue::Number(v.into()))
                            .collect(),
                    )
                } else {
                    JsonValue::Null
                }
            })
            .collect()
    }

    /// Convert a Neo4j Relation to JSON.
    fn relation_to_json(rel: &Relation) -> JsonValue {
        let mut props = Map::new();
        for key in rel.keys() {
            if let Ok(v) = rel.get::<String>(key) {
                props.insert(key.to_string(), JsonValue::String(v));
            } else if let Ok(v) = rel.get::<i64>(key) {
                props.insert(key.to_string(), JsonValue::Number(v.into()));
            } else if let Ok(v) = rel.get::<bool>(key) {
                props.insert(key.to_string(), JsonValue::Bool(v));
            }
        }
        json!({
            "_type": "relationship",
            "id": rel.id(),
            "source": rel.start_node_id(),
            "target": rel.end_node_id(),
            "rel_type": rel.typ(),
            "properties": props,
        })
    }

    /// Convert a Neo4j UnboundedRelation (from paths) to JSON.
    fn unbounded_relation_to_json(rel: &UnboundedRelation) -> JsonValue {
        let mut props = Map::new();
        for key in rel.keys() {
            if let Ok(v) = rel.get::<String>(key) {
                props.insert(key.to_string(), JsonValue::String(v));
            } else if let Ok(v) = rel.get::<i64>(key) {
                props.insert(key.to_string(), JsonValue::Number(v.into()));
            } else if let Ok(v) = rel.get::<bool>(key) {
                props.insert(key.to_string(), JsonValue::Bool(v));
            }
        }
        json!({
            "_type": "relationship",
            "id": rel.id(),
            "rel_type": rel.typ(),
            "properties": props,
        })
    }

    /// Convert a Neo4j Path to JSON.
    fn path_to_json(path: &Path) -> JsonValue {
        let path_nodes = path.nodes();
        let nodes: Vec<JsonValue> = path_nodes
            .iter()
            .map(|node| {
                let db_node = Self::neo4j_node_to_db_node(node);
                json!({
                    "_type": "node",
                    "id": node.id(),
                    "objectid": db_node.id,
                    "labels": node.labels(),
                    "properties": db_node.properties,
                })
            })
            .collect();
        let relationships: Vec<JsonValue> = path
            .rels()
            .iter()
            .enumerate()
            .map(|(i, rel)| {
                let mut rel_json = Self::unbounded_relation_to_json(rel);
                // Override source/target from path node positions
                if let Some(obj) = rel_json.as_object_mut() {
                    let source = path_nodes.get(i).map(|n| n.id()).unwrap_or(0);
                    let target = path_nodes.get(i + 1).map(|n| n.id()).unwrap_or(0);
                    obj.insert("source".to_string(), json!(source));
                    obj.insert("target".to_string(), json!(target));
                }
                rel_json
            })
            .collect();
        json!({
            "_type": "path",
            "nodes": nodes,
            "relationships": relationships,
        })
    }

    /// Execute a typed query and return all rows.
    /// Used by backend-specific methods that need parameterized queries.
    fn execute_query(&self, q: Query) -> Result<Vec<Row>> {
        let graph = self.graph.clone();
        self.runtime.block_on(async {
            let mut result = graph.execute(q).await?;
            let mut rows = Vec::new();
            while let Some(row) = result.next().await? {
                rows.push(row);
            }
            Ok(rows)
        })
    }

    /// Execute a typed write-only query.
    /// Used by backend-specific methods that need parameterized queries.
    fn run_query(&self, q: Query) -> Result<()> {
        let graph = self.graph.clone();
        self.runtime.block_on(async {
            graph.run(q).await?;
            Ok(())
        })
    }
}

// ========================================================================
// CypherExecutor implementation
// ========================================================================

impl CypherExecutor for Neo4jDatabase {
    fn exec_rows(&self, cypher: &str) -> Result<Vec<Vec<JsonValue>>> {
        let rows = self.execute_query(query(cypher))?;
        Ok(rows.iter().map(|row| Self::row_to_json_vec(row)).collect())
    }

    fn exec_write(&self, cypher: &str) -> Result<()> {
        self.run_query(query(cypher))
    }
}

// ========================================================================
// DatabaseBackend implementation
// ========================================================================

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

    fn ping(&self) -> Result<()> {
        self.run_query(query("RETURN 1"))?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        info!("Clearing all data from Neo4j");
        self.run_query(query("MATCH (n) DETACH DELETE n"))?;

        debug!("Creating objectid indexes for faster imports");
        for label in cypher_common::NODE_LABELS {
            // Try modern syntax first (Neo4j 4.0+)
            let modern_query = format!(
                "CREATE INDEX idx_{}_objectid IF NOT EXISTS FOR (n:{}) ON (n.objectid)",
                label.to_lowercase(),
                label
            );
            if self.run_query(query(&modern_query)).is_err() {
                // Fall back to legacy syntax
                let legacy_query = format!("CREATE INDEX ON :{}(objectid)", label);
                if let Err(e) = self.run_query(query(&legacy_query)) {
                    debug!("Index creation skipped for {}: {}", label, e);
                }
            }
        }

        debug!("Database cleared and indexes created");
        Ok(())
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        self.insert_nodes(&[node])?;
        Ok(())
    }

    fn insert_edge(&self, relationship: DbEdge) -> Result<()> {
        self.insert_edges(&[relationship])?;
        Ok(())
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        cypher_common::insert_nodes(self, nodes)
    }

    fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize> {
        if relationships.is_empty() {
            return Ok(0);
        }

        // Group relationships by type for efficient batching
        let mut edges_by_type: std::collections::HashMap<String, Vec<&DbEdge>> =
            std::collections::HashMap::new();
        for relationship in relationships {
            edges_by_type
                .entry(relationship.rel_type.clone())
                .or_default()
                .push(relationship);
        }

        // Neo4j supports parameterized UNWIND with a single MERGE for both
        // placeholder nodes and edge creation.
        let mut inserted = 0;
        for (rel_type, type_edges) in edges_by_type {
            for chunk in type_edges.chunks(cypher_common::BATCH_SIZE) {
                let srcs: Vec<String> = chunk.iter().map(|e| e.source.clone()).collect();
                let tgts: Vec<String> = chunk.iter().map(|e| e.target.clone()).collect();
                let src_types: Vec<String> = chunk
                    .iter()
                    .map(|e| e.source_type.clone().unwrap_or_else(|| "Base".to_string()))
                    .collect();
                let tgt_types: Vec<String> = chunk
                    .iter()
                    .map(|e| e.target_type.clone().unwrap_or_else(|| "Base".to_string()))
                    .collect();
                let props: Vec<String> = chunk
                    .iter()
                    .map(|e| serde_json::to_string(&e.properties).unwrap_or_default())
                    .collect();

                let q = query(&format!(
                    "UNWIND range(0, size($srcs)-1) AS i \
                     MERGE (a:Base {{objectid: $srcs[i]}}) \
                     ON CREATE SET a.placeholder = true, a.node_type = $src_types[i] \
                     MERGE (b:Base {{objectid: $tgts[i]}}) \
                     ON CREATE SET b.placeholder = true, b.node_type = $tgt_types[i] \
                     MERGE (a)-[r:{}]->(b) \
                     SET r.properties = $props[i] \
                     RETURN count(r) AS created",
                    rel_type
                ))
                .param("srcs", srcs)
                .param("tgts", tgts)
                .param("src_types", src_types)
                .param("tgt_types", tgt_types)
                .param("props", props);

                match self.execute_query(q) {
                    Ok(rows) => {
                        let created = rows
                            .first()
                            .and_then(|r| r.get::<i64>("created").ok())
                            .unwrap_or(0) as usize;
                        inserted += created;
                    }
                    Err(e) => {
                        debug!("Failed to create {} relationships batch: {}", rel_type, e);
                    }
                }
            }
        }

        Ok(inserted)
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        cypher_common::get_stats(self)
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        cypher_common::get_detailed_stats(self)
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        let total_users = cypher_common::count_total_users(self)?;

        // Neo4j supports p = shortestPath(...) in MATCH
        let real_da_query = format!(
            "MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..]->(g)) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN DISTINCT u.objectid AS id, u.name AS name",
            DOMAIN_ADMIN_SID_SUFFIX
        );
        let real_da_rows = self.execute_query(query(&real_da_query))?;
        let real_das: Vec<(String, String)> = real_da_rows
            .iter()
            .filter_map(|r| {
                let id = r.get::<String>("id").ok()?;
                let name = r.get::<String>("name").ok().unwrap_or_else(|| id.clone());
                Some((id, name))
            })
            .collect();

        let effective_das: Vec<(String, String, usize)> = self
            .find_paths_to_domain_admins(&[])?
            .into_iter()
            .map(|(id, _label, name, hops)| (id, name, hops))
            .collect();

        let reachability = cypher_common::compute_reachability(self);

        Ok(SecurityInsights::from_counts(
            total_users,
            real_das,
            effective_das,
            reachability,
        ))
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        cypher_common::get_all_nodes(self)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        cypher_common::get_all_edges(self)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        cypher_common::get_nodes_by_ids(self, ids)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        cypher_common::get_edges_between(self, node_ids)
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        cypher_common::get_edge_types(self)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        cypher_common::get_node_types(self)
    }

    fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        cypher_common::search_nodes(self, search_query, limit)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        cypher_common::resolve_node_identifier(self, identifier)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        cypher_common::get_node_connections(self, node_id, direction)
    }

    fn get_node_relationship_counts(
        &self,
        node_id: &str,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        let admin_types = cypher_common::admin_types_cypher_list();

        // Neo4j supports CALL subqueries to avoid Cartesian product explosion
        let q = query(&format!(
            "MATCH (n {{objectid: $id}})
             CALL {{ WITH n MATCH (n)<-[]-(in_node) RETURN count(DISTINCT in_node) AS incoming }}
             CALL {{ WITH n MATCH (n)-[]->(out_node) RETURN count(DISTINCT out_node) AS outgoing }}
             CALL {{ WITH n MATCH (n)-[admin]->(admin_node) WHERE type(admin) IN [{admin_types}] RETURN count(DISTINCT admin_node) AS admin_to }}
             CALL {{ WITH n MATCH (n)-[:MemberOf]->(mo_node) RETURN count(DISTINCT mo_node) AS member_of }}
             CALL {{ WITH n MATCH (n)<-[:MemberOf]-(mem_node) RETURN count(DISTINCT mem_node) AS members }}
             RETURN incoming, outgoing, admin_to, member_of, members"
        ))
        .param("id", node_id.to_string());

        let rows = self.execute_query(q)?;

        if let Some(row) = rows.first() {
            let incoming = row.get::<i64>("incoming").unwrap_or(0) as usize;
            let outgoing = row.get::<i64>("outgoing").unwrap_or(0) as usize;
            let admin_to = row.get::<i64>("admin_to").unwrap_or(0) as usize;
            let member_of = row.get::<i64>("member_of").unwrap_or(0) as usize;
            let members = row.get::<i64>("members").unwrap_or(0) as usize;
            Ok((incoming, outgoing, admin_to, member_of, members))
        } else {
            Ok((0, 0, 0, 0, 0))
        }
    }

    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        let q = query(
            "MATCH p = shortestPath((n {objectid: $id})-[:MemberOf*1..20]->(g)) \
             WHERE g.objectid ENDS WITH $suffix AND n <> g \
             RETURN g.objectid",
        )
        .param("id", node_id.to_string())
        .param("suffix", sid_suffix.to_string());

        let rows = self.execute_query(q)?;

        if let Some(row) = rows.first() {
            if let Ok(group_id) = row.get::<String>("g.objectid") {
                return Ok(Some(group_id));
            }
        }

        Ok(None)
    }

    fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let q = query(
            "MATCH (a {objectid: $from}), (b {objectid: $to}), \
             p = shortestPath((a)-[*..20]->(b)) \
             RETURN [n IN nodes(p) | n.objectid] AS node_ids, \
                    [r IN relationships(p) | type(r)] AS rel_types",
        )
        .param("from", from.to_string())
        .param("to", to.to_string());

        let rows = self.execute_query(q)?;

        if let Some(row) = rows.first() {
            if let (Ok(node_ids), Ok(rel_types)) = (
                row.get::<Vec<String>>("node_ids"),
                row.get::<Vec<String>>("rel_types"),
            ) {
                let mut path = Vec::new();
                for (i, node_id) in node_ids.iter().enumerate() {
                    let rel_type = if i < rel_types.len() {
                        Some(rel_types[i].clone())
                    } else {
                        None
                    };
                    path.push((node_id.clone(), rel_type));
                }
                if let Some(last) = path.last_mut() {
                    last.1 = None;
                }
                return Ok(Some(path));
            }
        }

        Ok(None)
    }

    fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        debug!(exclude = ?exclude_edge_types, "Finding paths to Domain Admins");

        let exclude_clause = cypher_common::build_exclude_clause(exclude_edge_types);

        // Neo4j supports p = shortestPath(...) in MATCH
        let q = query(&format!(
            "MATCH (u:User), (da:Group), \
             p = shortestPath((u)-[*1..10]->(da)) \
             WHERE da.objectid ENDS WITH '-512' {} \
             RETURN u.objectid AS id, u.name AS name, length(p) AS hops \
             ORDER BY hops, name",
            exclude_clause
        ));

        let rows = self.execute_query(q)?;

        let mut results = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for row in rows {
            if let (Ok(id), Ok(name), Ok(hops)) = (
                row.get::<String>("id"),
                row.get::<String>("name"),
                row.get::<i64>("hops"),
            ) {
                if !seen.contains(&id) {
                    seen.insert(id.clone());
                    results.push((id, "User".to_string(), name, hops as usize));
                }
            }
        }

        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    fn run_custom_query(&self, cypher: &str) -> Result<JsonValue> {
        debug!(query = %cypher, "Running custom Cypher query");

        let graph = self.graph.clone();
        let cypher = cypher.to_string();
        let (headers, rows) = self.runtime.block_on(async {
            let mut stream = graph.execute(query(&cypher)).await?;
            let mut rows = Vec::new();
            let mut headers: Vec<String> = Vec::new();

            while let Some(row) = stream.next().await? {
                if headers.is_empty() {
                    headers = row.keys().iter().map(|k| k.to_string()).collect();
                }
                rows.push(JsonValue::Array(Self::row_to_json_vec(&row)));
            }

            Ok::<_, neo4rs::Error>((headers, rows))
        })?;

        Ok(json!({
            "headers": headers,
            "rows": rows
        }))
    }
}
