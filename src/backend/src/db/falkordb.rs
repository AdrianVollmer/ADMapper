//! FalkorDB database backend.
//!
//! FalkorDB is a Redis-based graph database that supports Cypher queries.
//! Uses the `falkordb` crate for connection.
//!
//! Shared Cypher logic lives in `cypher_common`; this file contains only
//! FalkorDB-specific connection handling and methods that require
//! FalkorDB-specific query syntax.

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, SyncGraph};
use serde_json::{json, Map, Value as JsonValue};
use std::sync::Mutex;
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::cypher_common::{self, CypherExecutor};
use super::types::{
    DbEdge, DbError, DbNode, DetailedStats, Result, SecurityInsights, DOMAIN_ADMIN_SID_SUFFIX,
};

/// FalkorDB database backend.
pub struct FalkorDbDatabase {
    graph: Mutex<SyncGraph>,
}

impl FalkorDbDatabase {
    /// Create a new FalkorDB database connection.
    pub fn new(
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let uri = if let (Some(user), Some(pass)) = (&username, &password) {
            format!("falkor://{}:{}@{}:{}", user, pass, host, port)
        } else {
            format!("falkor://{}:{}", host, port)
        };

        info!(uri = %uri, "Connecting to FalkorDB");

        let connection_info: FalkorConnectionInfo = uri
            .as_str()
            .try_into()
            .map_err(|e| DbError::Database(format!("Invalid connection info: {}", e)))?;

        let client = FalkorClientBuilder::new()
            .with_connection_info(connection_info)
            .with_num_connections(std::num::NonZero::new(3).unwrap())
            .build()
            .map_err(|e| DbError::Database(format!("Failed to build client: {}", e)))?;

        // Remove the default 10,000-row result set cap so unbounded queries
        // return all rows (important for cross-backend consistency).
        // -1 means unlimited; 0 would mean "return zero results".
        client
            .config_set("RESULTSET_SIZE", -1_i64)
            .map_err(|e| DbError::Database(format!("Failed to set RESULTSET_SIZE: {}", e)))?;

        // Use "admapper" as the default graph name
        let graph = client.select_graph("admapper");

        info!("Connected to FalkorDB (RESULTSET_SIZE=unlimited)");

        Ok(Self {
            graph: Mutex::new(graph),
        })
    }

    /// Execute a query and parse the results, returning headers and rows.
    fn execute_query_full(&self, cypher: &str) -> Result<(Vec<String>, Vec<Vec<JsonValue>>)> {
        let mut graph = self
            .graph
            .lock()
            .map_err(|e| DbError::Database(format!("Lock poisoned: {}", e)))?;

        let result = graph
            .query(cypher)
            .execute()
            .map_err(|e| DbError::Database(e.to_string()))?;

        let headers = result.header.clone();
        let mut rows = Vec::new();
        for record in result.data {
            let mut row = Vec::new();
            for value in record {
                row.push(falkor_value_to_json(value));
            }
            rows.push(row);
        }

        Ok((headers, rows))
    }

    /// Execute a query and return rows only.
    fn execute_query(&self, cypher: &str) -> Result<Vec<Vec<JsonValue>>> {
        let (_, rows) = self.execute_query_full(cypher)?;
        Ok(rows)
    }

    /// Execute a write-only query.
    fn run_query(&self, cypher: &str) -> Result<()> {
        let mut graph = self
            .graph
            .lock()
            .map_err(|e| DbError::Database(format!("Lock poisoned: {}", e)))?;

        graph
            .query(cypher)
            .execute()
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(())
    }
}

// ========================================================================
// CypherExecutor implementation
// ========================================================================

impl CypherExecutor for FalkorDbDatabase {
    fn exec_rows(&self, cypher: &str) -> Result<Vec<Vec<JsonValue>>> {
        self.execute_query(cypher)
    }

    fn exec_write(&self, cypher: &str) -> Result<()> {
        self.run_query(cypher)
    }
}

// ========================================================================
// FalkorDB value conversion
// ========================================================================

/// Convert FalkorDB value to JSON.
fn falkor_value_to_json(value: falkordb::FalkorValue) -> JsonValue {
    match value {
        falkordb::FalkorValue::None => JsonValue::Null,
        falkordb::FalkorValue::Bool(b) => JsonValue::Bool(b),
        falkordb::FalkorValue::I64(i) => JsonValue::Number(i.into()),
        falkordb::FalkorValue::F64(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        falkordb::FalkorValue::String(s) => JsonValue::String(s),
        falkordb::FalkorValue::Array(arr) => {
            JsonValue::Array(arr.into_iter().map(falkor_value_to_json).collect())
        }
        falkordb::FalkorValue::Map(map) => {
            let obj: Map<String, JsonValue> = map
                .into_iter()
                .map(|(k, v)| (k, falkor_value_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        falkordb::FalkorValue::Node(node) => {
            let objectid = node
                .properties
                .get("objectid")
                .and_then(|v| {
                    if let falkordb::FalkorValue::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| node.entity_id.to_string());
            let props: Map<String, JsonValue> = node
                .properties
                .into_iter()
                .map(|(k, v)| (k, falkor_value_to_json(v)))
                .collect();
            json!({
                "_type": "node",
                "id": node.entity_id,
                "objectid": objectid,
                "labels": node.labels,
                "properties": props
            })
        }
        falkordb::FalkorValue::Edge(edge) => {
            let props: Map<String, JsonValue> = edge
                .properties
                .into_iter()
                .map(|(k, v)| (k, falkor_value_to_json(v)))
                .collect();
            json!({
                "_type": "relationship",
                "id": edge.entity_id,
                "source": edge.src_node_id,
                "target": edge.dst_node_id,
                "rel_type": edge.relationship_type,
                "properties": props
            })
        }
        falkordb::FalkorValue::Path(path) => {
            let nodes: Vec<JsonValue> = path
                .nodes
                .into_iter()
                .map(|n| falkor_value_to_json(falkordb::FalkorValue::Node(n)))
                .collect();
            let relationships: Vec<JsonValue> = path
                .relationships
                .into_iter()
                .map(|e| falkor_value_to_json(falkordb::FalkorValue::Edge(e)))
                .collect();
            json!({ "_type": "path", "nodes": nodes, "relationships": relationships })
        }
        falkordb::FalkorValue::Point(point) => {
            json!({ "latitude": point.latitude, "longitude": point.longitude })
        }
        falkordb::FalkorValue::Vec32(_) => JsonValue::String("[vector]".to_string()),
        falkordb::FalkorValue::Unparseable(s) => JsonValue::String(s),
    }
}

// ========================================================================
// DatabaseBackend implementation
// ========================================================================

impl DatabaseBackend for FalkorDbDatabase {
    fn name(&self) -> &'static str {
        "FalkorDB"
    }

    fn supports_language(&self, lang: QueryLanguage) -> bool {
        matches!(lang, QueryLanguage::Cypher)
    }

    fn default_language(&self) -> QueryLanguage {
        QueryLanguage::Cypher
    }

    fn ping(&self) -> Result<()> {
        self.run_query("RETURN 1")?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        info!("Clearing all data from FalkorDB");
        self.run_query("MATCH (n) DETACH DELETE n")?;

        debug!("Creating objectid indexes for faster imports");
        for label in cypher_common::NODE_LABELS {
            let index_query = format!("CREATE INDEX FOR (n:{}) ON (n.objectid)", label);
            if let Err(e) = self.run_query(&index_query) {
                debug!("Index creation skipped for {}: {}", label, e);
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

        // FalkorDB requires a two-step approach: ensure placeholder nodes exist
        // first, then create edges. This avoids FalkorDB's UNWIND+MERGE row
        // collapsing which causes subsequent CREATE to lose edges when multiple
        // rows reference the same source/target node.
        let mut inserted = 0;
        for (rel_type, type_edges) in edges_by_type {
            for chunk in type_edges.chunks(cypher_common::BATCH_SIZE) {
                let items: Vec<String> = chunk
                    .iter()
                    .map(|e| {
                        let src = e.source.replace('\\', "\\\\").replace('\'', "\\'");
                        let tgt = e.target.replace('\\', "\\\\").replace('\'', "\\'");
                        let src_type = e
                            .source_type
                            .as_deref()
                            .unwrap_or("Base")
                            .replace('\\', "\\\\")
                            .replace('\'', "\\'");
                        let tgt_type = e
                            .target_type
                            .as_deref()
                            .unwrap_or("Base")
                            .replace('\\', "\\\\")
                            .replace('\'', "\\'");
                        let props = serde_json::to_string(&e.properties)
                            .unwrap_or_default()
                            .replace('\'', "\\'");
                        format!(
                            "{{src: '{}', tgt: '{}', src_type: '{}', tgt_type: '{}', props: '{}'}}",
                            src, tgt, src_type, tgt_type, props
                        )
                    })
                    .collect();

                let items_str = items.join(", ");

                // Step 1: Ensure all referenced nodes exist (placeholder if needed).
                let ensure_nodes = format!(
                    "UNWIND [{}] AS row \
                     MERGE (a:Base {{objectid: row.src}}) \
                     ON CREATE SET a.placeholder = true, a.node_type = row.src_type \
                     MERGE (b:Base {{objectid: row.tgt}}) \
                     ON CREATE SET b.placeholder = true, b.node_type = row.tgt_type",
                    items_str
                );
                self.run_query(&ensure_nodes)?;

                // Step 2: Create edges using MATCH on :Base (nodes guaranteed to exist).
                let create_edges = format!(
                    "UNWIND [{}] AS row \
                     MATCH (a:Base {{objectid: row.src}}) \
                     MATCH (b:Base {{objectid: row.tgt}}) \
                     CREATE (a)-[r:{}]->(b) \
                     SET r.properties = row.props \
                     RETURN count(r) AS created",
                    items_str, rel_type
                );

                let rows = self.execute_query(&create_edges)?;
                let created = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as usize;
                inserted += created;
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

        // FalkorDB requires WITH shortestPath(...) syntax
        let real_da_rows = self.exec_rows(&format!(
            "MATCH (u:User), (g:Group) \
             WHERE g.objectid ENDS WITH '{}' \
             WITH u, g, shortestPath((u)-[:MemberOf*1..]->(g)) AS p \
             WHERE p IS NOT NULL \
             RETURN DISTINCT u.objectid AS id, u.name AS name",
            DOMAIN_ADMIN_SID_SUFFIX
        ))?;
        let real_das = cypher_common::parse_real_das(&real_da_rows);

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

    fn search_nodes(&self, query: &str, limit: usize, label: Option<&str>) -> Result<Vec<DbNode>> {
        cypher_common::search_nodes(self, query, limit, label)
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
        let id_escaped = node_id.replace('\'', "\\'");
        let admin_types = cypher_common::admin_types_cypher_list();

        // Use WITH chaining to avoid Cartesian product explosion.
        // FalkorDB doesn't support CALL subqueries, so we chain aggregations.
        let cypher = format!(
            "MATCH (n {{objectid: '{id_escaped}'}})
             OPTIONAL MATCH (n)<-[]-(in_node)
             WITH n, count(DISTINCT in_node) AS incoming
             OPTIONAL MATCH (n)-[]->(out_node)
             WITH n, incoming, count(DISTINCT out_node) AS outgoing
             OPTIONAL MATCH (n)-[admin]->(admin_node) WHERE type(admin) IN [{admin_types}]
             WITH n, incoming, outgoing, count(DISTINCT admin_node) AS admin_to
             OPTIONAL MATCH (n)-[:MemberOf]->(mo_node)
             WITH n, incoming, outgoing, admin_to, count(DISTINCT mo_node) AS member_of
             OPTIONAL MATCH (n)<-[:MemberOf]-(mem_node)
             RETURN incoming, outgoing, admin_to, member_of, count(DISTINCT mem_node) AS members"
        );

        let rows = self.execute_query(&cypher)?;

        if let Some(row) = rows.first() {
            let incoming = row.first().and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let outgoing = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let admin_to = row.get(2).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let member_of = row.get(3).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let members = row.get(4).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            Ok((incoming, outgoing, admin_to, member_of, members))
        } else {
            Ok((0, 0, 0, 0, 0))
        }
    }

    fn is_member_of(&self, node_id: &str, target_id: &str) -> Result<bool> {
        let id_escaped = node_id.replace('\'', "\\'");
        let target_escaped = target_id.replace('\'', "\\'");

        let cypher = format!(
            "MATCH (n {{objectid: '{}'}}), (t {{objectid: '{}'}}) \
             WITH n, t, shortestPath((n)-[:MemberOf*1..20]->(t)) AS p \
             WHERE p IS NOT NULL \
             RETURN true AS found",
            id_escaped, target_escaped
        );

        let rows = self.execute_query(&cypher)?;
        Ok(!rows.is_empty())
    }

    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        let id_escaped = node_id.replace('\'', "\\'");
        let suffix_escaped = sid_suffix.replace('\'', "\\'");

        // FalkorDB requires WITH shortestPath(...) syntax
        let cypher = format!(
            "MATCH (n {{objectid: '{}'}}), (g) \
             WHERE g.objectid ENDS WITH '{}' \
             WITH n, g, shortestPath((n)-[:MemberOf*1..20]->(g)) AS p \
             WHERE p IS NOT NULL \
             RETURN g.objectid",
            id_escaped, suffix_escaped
        );

        let rows = self.execute_query(&cypher)?;

        if let Some(row) = rows.first() {
            if let Some(group_id) = row.first().and_then(|v| v.as_str()) {
                return Ok(Some(group_id.to_string()));
            }
        }

        Ok(None)
    }

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let from_escaped = from.replace('\'', "\\'");
        let to_escaped = to.replace('\'', "\\'");

        // FalkorDB requires WITH shortestPath(...) syntax
        let cypher = format!(
            "MATCH (a {{objectid: '{}'}}), (b {{objectid: '{}'}}) \
             WITH shortestPath((a)-[*..20]->(b)) AS p \
             RETURN nodes(p) AS nodes, relationships(p) AS rels",
            from_escaped, to_escaped
        );

        let rows = self.execute_query(&cypher)?;

        if let Some(row) = rows.first() {
            if row.len() >= 2 {
                let nodes = row[0].as_array();
                let rels = row[1].as_array();

                if let (Some(nodes), Some(rels)) = (nodes, rels) {
                    let mut path = Vec::new();
                    for (i, node) in nodes.iter().enumerate() {
                        let node_id = node
                            .as_object()
                            .and_then(|o| o.get("properties"))
                            .and_then(|p| p.get("objectid"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        let rel_type = if i < rels.len() {
                            rels[i]
                                .as_object()
                                .and_then(|o| o.get("rel_type"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        };

                        path.push((node_id, rel_type));
                    }

                    if let Some(last) = path.last_mut() {
                        last.1 = None;
                    }

                    return Ok(Some(path));
                }
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

        // FalkorDB requires WITH shortestPath(...) syntax
        let cypher = format!(
            "MATCH (u:User), (da:Group) \
             WHERE da.objectid ENDS WITH '-512' \
             WITH u, da, shortestPath((u)-[*1..10]->(da)) AS p \
             WHERE p IS NOT NULL {} \
             RETURN u.objectid AS id, u.name AS name, length(p) AS hops \
             ORDER BY hops, name",
            exclude_clause
        );

        let rows = self.execute_query(&cypher)?;
        let results = cypher_common::parse_paths_to_da_results(&rows);

        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    fn run_custom_query(&self, cypher: &str) -> Result<JsonValue> {
        debug!(query = %cypher, "Running custom Cypher query");
        let (headers, rows) = self.execute_query_full(cypher)?;
        Ok(json!({ "headers": headers, "rows": rows }))
    }
}
