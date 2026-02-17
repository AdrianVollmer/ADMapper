//! FalkorDB database backend.
//!
//! FalkorDB is a Redis-based graph database that supports Cypher queries.
//! Uses the `falkordb` crate for connection.

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, SyncGraph};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{
    DbEdge, DbError, DbNode, DetailedStats, ReachabilityInsight, Result, SecurityInsights,
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
            .build()
            .map_err(|e| DbError::Database(format!("Failed to build client: {}", e)))?;

        // Use "admapper" as the default graph name
        let graph = client.select_graph("admapper");

        info!("Connected to FalkorDB");

        Ok(Self {
            graph: Mutex::new(graph),
        })
    }

    /// Execute a query and parse the results.
    fn execute_query(&self, cypher: &str) -> Result<Vec<Vec<JsonValue>>> {
        let mut graph = self
            .graph
            .lock()
            .map_err(|e| DbError::Database(format!("Lock poisoned: {}", e)))?;

        let result = graph
            .query(cypher)
            .execute()
            .map_err(|e| DbError::Database(e.to_string()))?;

        let mut rows = Vec::new();
        for record in result.data {
            let mut row = Vec::new();
            for value in record {
                row.push(falkor_value_to_json(value));
            }
            rows.push(row);
        }

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

    /// Parse a node from FalkorDB result.
    fn parse_node(value: &JsonValue) -> Option<DbNode> {
        let obj = value.as_object()?;

        let id = obj
            .get("properties")
            .and_then(|p| p.get("objectid"))
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("id").and_then(|v| v.as_i64()).map(|_| ""))
            .map(|s| s.to_string())
            .unwrap_or_default();

        let label = obj
            .get("properties")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| id.clone());

        let node_type = obj
            .get("labels")
            .and_then(|l| l.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let properties = obj
            .get("properties")
            .cloned()
            .unwrap_or(JsonValue::Object(Map::new()));

        Some(DbNode {
            id,
            label,
            node_type,
            properties,
        })
    }
}

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
            let mut obj = Map::new();
            obj.insert("id".to_string(), JsonValue::Number(node.entity_id.into()));
            obj.insert(
                "labels".to_string(),
                JsonValue::Array(node.labels.into_iter().map(JsonValue::String).collect()),
            );
            let props: Map<String, JsonValue> = node
                .properties
                .into_iter()
                .map(|(k, v)| (k, falkor_value_to_json(v)))
                .collect();
            obj.insert("properties".to_string(), JsonValue::Object(props));
            JsonValue::Object(obj)
        }
        falkordb::FalkorValue::Edge(edge) => {
            let mut obj = Map::new();
            obj.insert("id".to_string(), JsonValue::Number(edge.entity_id.into()));
            obj.insert(
                "type".to_string(),
                JsonValue::String(edge.relationship_type),
            );
            obj.insert(
                "source".to_string(),
                JsonValue::Number(edge.src_node_id.into()),
            );
            obj.insert(
                "target".to_string(),
                JsonValue::Number(edge.dst_node_id.into()),
            );
            let props: Map<String, JsonValue> = edge
                .properties
                .into_iter()
                .map(|(k, v)| (k, falkor_value_to_json(v)))
                .collect();
            obj.insert("properties".to_string(), JsonValue::Object(props));
            JsonValue::Object(obj)
        }
        falkordb::FalkorValue::Path(path) => {
            let nodes: Vec<JsonValue> = path
                .nodes
                .into_iter()
                .map(|n| falkor_value_to_json(falkordb::FalkorValue::Node(n)))
                .collect();
            let edges: Vec<JsonValue> = path
                .relationships
                .into_iter()
                .map(|e| falkor_value_to_json(falkordb::FalkorValue::Edge(e)))
                .collect();
            json!({ "nodes": nodes, "edges": edges })
        }
        falkordb::FalkorValue::Point(point) => {
            json!({ "latitude": point.latitude, "longitude": point.longitude })
        }
        falkordb::FalkorValue::Vec32(_) => {
            // Vec32 is a special vector type, represent as string
            JsonValue::String("[vector]".to_string())
        }
        falkordb::FalkorValue::Unparseable(s) => JsonValue::String(s),
    }
}

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

    fn clear(&self) -> Result<()> {
        info!("Clearing all data from FalkorDB");
        // Delete all relationships first, then all nodes
        self.run_query("MATCH ()-[r]->() DELETE r")?;
        self.run_query("MATCH (n) DELETE n")?;
        debug!("Database cleared");
        Ok(())
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        self.insert_nodes(&[node])?;
        Ok(())
    }

    fn insert_edge(&self, edge: DbEdge) -> Result<()> {
        self.insert_edges(&[edge])?;
        Ok(())
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        for node in nodes {
            let props_str = serde_json::to_string(&node.properties)?.replace('\'', "\\'");
            let label = node.label.replace('\'', "\\'");
            let id = node.id.replace('\'', "\\'");

            let cypher = format!(
                "MERGE (n:{} {{objectid: '{}'}}) SET n.name = '{}', n.properties = '{}'",
                node.node_type, id, label, props_str
            );

            if let Err(e) = self.run_query(&cypher) {
                debug!("Failed to insert node {}: {}", node.id, e);
            }
        }

        Ok(nodes.len())
    }

    fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        for edge in edges {
            let props_str = serde_json::to_string(&edge.properties)?.replace('\'', "\\'");
            let src = edge.source.replace('\'', "\\'");
            let tgt = edge.target.replace('\'', "\\'");

            let cypher = format!(
                "MATCH (a {{objectid: '{}'}}), (b {{objectid: '{}'}}) \
                 MERGE (a)-[r:{}]->(b) SET r.properties = '{}'",
                src, tgt, edge.edge_type, props_str
            );

            if let Err(e) = self.run_query(&cypher) {
                debug!(
                    "Failed to create edge {} -> {}: {}",
                    edge.source, edge.target, e
                );
            }
        }

        Ok(edges.len())
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        let node_rows = self.execute_query("MATCH (n) RETURN count(n) AS count")?;
        let node_count = node_rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        let edge_rows = self.execute_query("MATCH ()-[r]->() RETURN count(r) AS count")?;
        let edge_count = edge_rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        Ok((node_count, edge_count))
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        let (total_nodes, total_edges) = self.get_stats()?;

        // Get counts by label
        let rows =
            self.execute_query("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count")?;

        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for row in rows {
            if row.len() >= 2 {
                if let (Some(label), Some(count)) = (row[0].as_str(), row[1].as_i64()) {
                    type_counts.insert(label.to_string(), count as usize);
                }
            }
        }

        Ok(DetailedStats {
            total_nodes,
            total_edges,
            users: type_counts.get("User").copied().unwrap_or(0),
            computers: type_counts.get("Computer").copied().unwrap_or(0),
            groups: type_counts.get("Group").copied().unwrap_or(0),
            domains: type_counts.get("Domain").copied().unwrap_or(0),
            ous: type_counts.get("OU").copied().unwrap_or(0),
            gpos: type_counts.get("GPO").copied().unwrap_or(0),
        })
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        // Count total users
        let user_rows = self.execute_query("MATCH (n:User) RETURN count(n) AS count")?;
        let total_users = user_rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        // Find real DAs
        let real_da_rows = self.execute_query(
            "MATCH (u:User)-[:MemberOf*1..]->(g:Group) \
             WHERE g.objectid ENDS WITH '-512' \
             RETURN DISTINCT u.objectid AS id, u.name AS name",
        )?;

        let real_das: Vec<(String, String)> = real_da_rows
            .iter()
            .filter_map(|r| {
                let id = r.get(0)?.as_str()?.to_string();
                let name = r.get(1).and_then(|v| v.as_str()).unwrap_or(&id).to_string();
                Some((id, name))
            })
            .collect();
        let real_da_count = real_das.len();

        // Find effective DAs
        let effective_da_rows = self.execute_query(
            "MATCH p = (u:User)-[*1..10]->(g:Group) \
             WHERE g.objectid ENDS WITH '-512' \
             RETURN DISTINCT u.objectid AS id, u.name AS name, min(length(p)) AS hops",
        )?;

        let effective_das: Vec<(String, String, usize)> = effective_da_rows
            .iter()
            .filter_map(|r| {
                let id = r.get(0)?.as_str()?.to_string();
                let name = r.get(1).and_then(|v| v.as_str()).unwrap_or(&id).to_string();
                let hops = r.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
                Some((id, name, hops))
            })
            .collect();
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

        // Compute reachability
        let well_known = [
            ("Everyone", "S-1-1-0"),
            ("Authenticated Users", "S-1-5-11"),
            ("Domain Users", "-513"),
            ("Domain Computers", "-515"),
        ];

        let mut reachability = Vec::new();
        for (name, pattern) in well_known {
            let cypher = if pattern.starts_with('-') {
                format!(
                    "MATCH (p) WHERE p.objectid ENDS WITH '{}' \
                     OPTIONAL MATCH (p)-[*1..5]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                )
            } else {
                format!(
                    "MATCH (p {{objectid: '{}'}}) \
                     OPTIONAL MATCH (p)-[*1..5]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                )
            };

            let rows = self.execute_query(&cypher).unwrap_or_default();
            let (principal_id, reachable_count) = rows
                .first()
                .map(|r| {
                    let id = r.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let cnt = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                    (id, cnt)
                })
                .unwrap_or((None, 0));

            reachability.push(ReachabilityInsight {
                principal_name: name.to_string(),
                principal_id,
                reachable_count,
            });
        }

        Ok(SecurityInsights {
            effective_da_count,
            real_da_count,
            da_ratio,
            total_users,
            effective_da_percentage,
            reachability,
            effective_das,
            real_das,
        })
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let rows = self.execute_query("MATCH (n) RETURN n")?;

        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|v| Self::parse_node(v))
            .collect();

        Ok(nodes)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let rows = self.execute_query(
            "MATCH (a)-[r]->(b) RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel"
        )?;

        let edges: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.get(0)?.as_str()?.to_string();
                let tgt = r.get(1)?.as_str()?.to_string();
                let typ = r.get(2)?.as_str()?.to_string();
                let props = r
                    .get(3)
                    .and_then(|v| v.get("properties"))
                    .cloned()
                    .unwrap_or(JsonValue::Object(Map::new()));
                Some(DbEdge {
                    source: src,
                    target: tgt,
                    edge_type: typ,
                    properties: props,
                })
            })
            .collect();

        Ok(edges)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let id_list: Vec<String> = ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "\\'")))
            .collect();

        let cypher = format!(
            "MATCH (n) WHERE n.objectid IN [{}] RETURN n",
            id_list.join(", ")
        );

        let rows = self.execute_query(&cypher)?;
        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|v| Self::parse_node(v))
            .collect();

        Ok(nodes)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let id_list: Vec<String> = node_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "\\'")))
            .collect();
        let id_set = id_list.join(", ");

        let cypher = format!(
            "MATCH (a)-[r]->(b) \
             WHERE a.objectid IN [{}] AND b.objectid IN [{}] \
             RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel",
            id_set, id_set
        );

        let rows = self.execute_query(&cypher)?;
        let edges: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.get(0)?.as_str()?.to_string();
                let tgt = r.get(1)?.as_str()?.to_string();
                let typ = r.get(2)?.as_str()?.to_string();
                let props = r
                    .get(3)
                    .and_then(|v| v.get("properties"))
                    .cloned()
                    .unwrap_or(JsonValue::Object(Map::new()));
                Some(DbEdge {
                    source: src,
                    target: tgt,
                    edge_type: typ,
                    properties: props,
                })
            })
            .collect();

        Ok(edges)
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        let rows = self.execute_query("MATCH ()-[r]->() RETURN DISTINCT type(r) AS typ")?;

        let types: Vec<String> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        Ok(types)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        let rows = self.execute_query("MATCH (n) RETURN DISTINCT labels(n)[0] AS label")?;

        let types: Vec<String> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        Ok(types)
    }

    fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let pattern = search_query.replace('\'', "\\'").to_lowercase();

        let cypher = format!(
            "MATCH (n) WHERE toLower(n.name) CONTAINS '{}' OR toLower(n.objectid) CONTAINS '{}' \
             RETURN n LIMIT {}",
            pattern, pattern, limit
        );

        let rows = self.execute_query(&cypher)?;
        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|v| Self::parse_node(v))
            .collect();

        debug!(query = %search_query, found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        let id_escaped = identifier.replace('\'', "\\'");

        // Try exact objectid match
        let cypher = format!(
            "MATCH (n {{objectid: '{}'}}) RETURN n.objectid AS id LIMIT 1",
            id_escaped
        );

        let rows = self.execute_query(&cypher)?;
        if let Some(id) = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
        {
            return Ok(Some(id.to_string()));
        }

        // Try case-insensitive name match
        let cypher = format!(
            "MATCH (n) WHERE toLower(n.name) = toLower('{}') RETURN n.objectid AS id LIMIT 1",
            id_escaped
        );

        let rows = self.execute_query(&cypher)?;
        if let Some(id) = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
        {
            return Ok(Some(id.to_string()));
        }

        Ok(None)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        let id_escaped = node_id.replace('\'', "\\'");

        let cypher = match direction {
            "incoming" => format!(
                "MATCH (a)-[r]->(b {{objectid: '{}'}}) RETURN a, r, b",
                id_escaped
            ),
            "outgoing" => format!(
                "MATCH (a {{objectid: '{}'}})-[r]->(b) RETURN a, r, b",
                id_escaped
            ),
            "admin" => format!(
                "MATCH (a {{objectid: '{}'}})-[r]->(b) \
                 WHERE type(r) IN ['AdminTo', 'GenericAll', 'GenericWrite', 'Owns', 'WriteDacl', 'WriteOwner', 'AllExtendedRights', 'ForceChangePassword', 'AddMember'] \
                 RETURN a, r, b",
                id_escaped
            ),
            "memberof" => format!(
                "MATCH (a {{objectid: '{}'}})-[r:MemberOf]->(b) RETURN a, r, b",
                id_escaped
            ),
            "members" => format!(
                "MATCH (a)-[r:MemberOf]->(b {{objectid: '{}'}}) RETURN a, r, b",
                id_escaped
            ),
            _ => format!(
                "MATCH (a {{objectid: '{}'}})-[r]-(b) RETURN a, r, b",
                id_escaped
            ),
        };

        let rows = self.execute_query(&cypher)?;

        let mut node_ids: HashSet<String> = HashSet::new();
        node_ids.insert(node_id.to_string());

        let mut edges = Vec::new();
        for row in &rows {
            if row.len() >= 3 {
                if let (Some(src_node), Some(tgt_node)) =
                    (Self::parse_node(&row[0]), Self::parse_node(&row[2]))
                {
                    node_ids.insert(src_node.id.clone());
                    node_ids.insert(tgt_node.id.clone());

                    if let Some(rel) = row[1].as_object() {
                        let edge_type = rel
                            .get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("RELATED")
                            .to_string();
                        let props = rel
                            .get("properties")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Map::new()));

                        edges.push(DbEdge {
                            source: src_node.id,
                            target: tgt_node.id,
                            edge_type,
                            properties: props,
                        });
                    }
                }
            }
        }

        let node_id_vec: Vec<String> = node_ids.into_iter().collect();
        let nodes = self.get_nodes_by_ids(&node_id_vec)?;

        Ok((nodes, edges))
    }

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let from_escaped = from.replace('\'', "\\'");
        let to_escaped = to.replace('\'', "\\'");

        let cypher = format!(
            "MATCH p = shortestPath((a {{objectid: '{}'}})-[*..20]->(b {{objectid: '{}'}})) \
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

                        let edge_type = if i < rels.len() {
                            rels[i]
                                .as_object()
                                .and_then(|o| o.get("type"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        };

                        path.push((node_id, edge_type));
                    }

                    // Last node has no outgoing edge
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

        let exclude_clause = if exclude_edge_types.is_empty() {
            String::new()
        } else {
            let types: Vec<String> = exclude_edge_types
                .iter()
                .map(|t| format!("'{}'", t))
                .collect();
            format!(
                "AND NONE(r IN relationships(p) WHERE type(r) IN [{}])",
                types.join(", ")
            )
        };

        let cypher = format!(
            "MATCH p = (u:User)-[*1..10]->(da:Group) \
             WHERE da.objectid ENDS WITH '-512' {} \
             RETURN u.objectid AS id, u.name AS name, min(length(p)) AS hops \
             ORDER BY hops, name",
            exclude_clause
        );

        let rows = self.execute_query(&cypher)?;

        let mut results = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for row in rows {
            if row.len() >= 3 {
                if let (Some(id), Some(name), Some(hops)) =
                    (row[0].as_str(), row[1].as_str(), row[2].as_i64())
                {
                    if !seen.contains(id) {
                        seen.insert(id.to_string());
                        results.push((
                            id.to_string(),
                            "User".to_string(),
                            name.to_string(),
                            hops as usize,
                        ));
                    }
                }
            }
        }

        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    fn run_custom_query(&self, cypher: &str) -> Result<JsonValue> {
        debug!(query = %cypher, "Running custom Cypher query");

        let rows = self.execute_query(cypher)?;

        Ok(json!({ "results": rows }))
    }

    fn add_query_history(
        &self,
        id: &str,
        name: &str,
        query_str: &str,
        timestamp: i64,
        result_count: Option<i64>,
        status: &str,
        started_at: i64,
        duration_ms: Option<u64>,
        error: Option<&str>,
    ) -> Result<()> {
        let id_escaped = id.replace('\'', "\\'");
        let name_escaped = name.replace('\'', "\\'");
        let query_escaped = query_str.replace('\'', "\\'");
        let status_escaped = status.replace('\'', "\\'");
        let error_escaped = error.map(|e| e.replace('\'', "\\'")).unwrap_or_default();

        let cypher = format!(
            "CREATE (h:QueryHistory {{id: '{}', name: '{}', query: '{}', timestamp: {}, result_count: {}, status: '{}', started_at: {}, duration_ms: {}, error: '{}'}})",
            id_escaped, name_escaped, query_escaped, timestamp, result_count.unwrap_or(0),
            status_escaped, started_at, duration_ms.unwrap_or(0), error_escaped
        );

        self.run_query(&cypher)
    }

    fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let id_escaped = id.replace('\'', "\\'");
        let status_escaped = status.replace('\'', "\\'");
        let error_escaped = error.map(|e| e.replace('\'', "\\'")).unwrap_or_default();

        let cypher = format!(
            "MATCH (h:QueryHistory {{id: '{}'}}) SET h.status = '{}', h.duration_ms = {}, h.result_count = {}, h.error = '{}'",
            id_escaped, status_escaped, duration_ms.unwrap_or(0), result_count.unwrap_or(0), error_escaped
        );

        self.run_query(&cypher)
    }

    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(
        Vec<(
            String,
            String,
            String,
            i64,
            Option<i64>,
            String,
            i64,
            Option<u64>,
            Option<String>,
        )>,
        usize,
    )> {
        // Get total count
        let count_rows = self.execute_query("MATCH (h:QueryHistory) RETURN count(h) AS count")?;
        let total = count_rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        // Get paginated results
        let cypher = format!(
            "MATCH (h:QueryHistory) \
             RETURN h.id AS id, h.name AS name, h.query AS query, h.timestamp AS ts, h.result_count AS cnt, \
                    h.status AS status, h.started_at AS started_at, h.duration_ms AS duration_ms, h.error AS error \
             ORDER BY h.timestamp DESC \
             SKIP {} LIMIT {}",
            offset, limit
        );

        let rows = self.execute_query(&cypher)?;

        let history: Vec<(
            String,
            String,
            String,
            i64,
            Option<i64>,
            String,
            i64,
            Option<u64>,
            Option<String>,
        )> = rows
            .iter()
            .filter_map(|r| {
                let id = r.get(0)?.as_str()?.to_string();
                let name = r.get(1)?.as_str()?.to_string();
                let query = r.get(2)?.as_str()?.to_string();
                let ts = r.get(3)?.as_i64()?;
                let cnt = r.get(4).and_then(|v| v.as_i64());
                let status = r.get(5).and_then(|v| v.as_str()).unwrap_or("completed").to_string();
                let started_at = r.get(6).and_then(|v| v.as_i64()).unwrap_or(ts);
                let duration_ms = r.get(7).and_then(|v| v.as_u64());
                let error = r.get(8).and_then(|v| v.as_str()).filter(|e| !e.is_empty()).map(String::from);
                Some((id, name, query, ts, cnt, status, started_at, duration_ms, error))
            })
            .collect();

        Ok((history, total))
    }

    fn delete_query_history(&self, id: &str) -> Result<()> {
        let id_escaped = id.replace('\'', "\\'");
        let cypher = format!("MATCH (h:QueryHistory {{id: '{}'}}) DELETE h", id_escaped);
        self.run_query(&cypher)
    }

    fn clear_query_history(&self) -> Result<()> {
        self.run_query("MATCH (h:QueryHistory) DELETE h")
    }
}
