//! Neo4j database backend.
//!
//! Uses the `neo4rs` crate for connecting to Neo4j via Bolt protocol.

use neo4rs::{query, Graph, Node as Neo4jNode, Query, Relation, Row};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{
    DbEdge, DbNode, DetailedStats, NewQueryHistoryEntry, QueryHistoryRow, ReachabilityInsight,
    Result, SecurityInsights, DOMAIN_ADMIN_SID_SUFFIX, WELL_KNOWN_PRINCIPALS,
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
            RuntimeHandle::Handle(h) => {
                tokio::task::block_in_place(|| h.block_on(future))
            }
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

        // Try to get existing runtime handle, or create our own runtime
        let runtime = match Handle::try_current() {
            Ok(handle) => RuntimeHandle::Handle(handle),
            Err(_) => {
                let rt = Runtime::new().map_err(|e| {
                    super::types::DbError::Database(format!("Failed to create runtime: {}", e))
                })?;
                RuntimeHandle::Owned(rt)
            }
        };

        // Connect to Neo4j
        let graph = runtime.block_on(async { Graph::new(&uri, &user, &pass).await })?;

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
            .or_else(|_| node.get::<String>("object_id"))
            .or_else(|_| node.get::<i64>("id").map(|id| id.to_string()))
            .unwrap_or_else(|_| format!("node_{}", node.id()));

        let name = node
            .get::<String>("name")
            .or_else(|_| node.get::<String>("label"))
            .unwrap_or_else(|_| id.clone());

        let label = node
            .labels()
            .first()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        // Convert all properties to JSON
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

    /// Convert a Neo4j relation to DbEdge.
    fn neo4j_relation_to_db_edge(rel: &Relation, source_id: &str, target_id: &str) -> DbEdge {
        let edge_type = rel.typ().to_string();

        // Convert properties to JSON
        let mut properties = Map::new();
        for key in rel.keys() {
            if let Ok(val) = rel.get::<String>(key) {
                properties.insert(key.to_string(), JsonValue::String(val));
            } else if let Ok(val) = rel.get::<i64>(key) {
                properties.insert(key.to_string(), JsonValue::Number(val.into()));
            } else if let Ok(val) = rel.get::<bool>(key) {
                properties.insert(key.to_string(), JsonValue::Bool(val));
            }
        }

        DbEdge {
            source: source_id.to_string(),
            target: target_id.to_string(),
            edge_type,
            properties: JsonValue::Object(properties),
            ..Default::default()
        }
    }

    /// Execute a query and return all rows.
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

    /// Execute a write-only query.
    fn run_query(&self, q: Query) -> Result<()> {
        let graph = self.graph.clone();
        self.runtime.block_on(async {
            graph.run(q).await?;
            Ok(())
        })
    }

    /// Flatten BloodHound node properties into a single JSON object.
    fn flatten_node_properties(node: &DbNode) -> JsonValue {
        let mut props = Map::new();

        // Add core identifiers
        props.insert("objectid".to_string(), json!(node.id));
        props.insert("name".to_string(), json!(node.name));

        // Flatten BloodHound properties into top-level fields
        if let JsonValue::Object(bh_props) = &node.properties {
            for (key, value) in bh_props {
                // Skip null values and empty arrays
                if value.is_null() {
                    continue;
                }
                if let Some(arr) = value.as_array() {
                    if arr.is_empty() {
                        continue;
                    }
                }
                // Don't overwrite core fields
                let key_lower = key.to_lowercase();
                if key_lower != "objectid" && key_lower != "name" {
                    props.insert(key_lower, value.clone());
                }
            }
        }

        JsonValue::Object(props)
    }

    /// Convert a JSON object to Cypher property syntax with escaping.
    fn json_to_cypher_props(value: &JsonValue) -> String {
        let obj = match value.as_object() {
            Some(o) => o,
            None => return "{}".to_string(),
        };

        let pairs: Vec<String> = obj
            .iter()
            .filter_map(|(k, v)| {
                let val_str = Self::json_value_to_cypher(v)?;
                Some(format!("{}: {}", k, val_str))
            })
            .collect();

        format!("{{{}}}", pairs.join(", "))
    }

    /// Convert a JSON value to Cypher literal syntax.
    fn json_value_to_cypher(value: &JsonValue) -> Option<String> {
        match value {
            JsonValue::Null => None,
            JsonValue::Bool(b) => Some(b.to_string()),
            JsonValue::Number(n) => Some(n.to_string()),
            JsonValue::String(s) => {
                // Escape backslashes first, then single quotes
                let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
                Some(format!("'{}'", escaped))
            }
            JsonValue::Array(arr) => {
                let items: Vec<String> =
                    arr.iter().filter_map(Self::json_value_to_cypher).collect();
                Some(format!("[{}]", items.join(", ")))
            }
            JsonValue::Object(_) => {
                // Skip nested objects - Cypher doesn't support them directly
                None
            }
        }
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
        info!("Clearing all data from Neo4j");
        // Delete all relationships first, then all nodes
        self.run_query(query("MATCH ()-[r]->() DELETE r"))?;
        self.run_query(query("MATCH (n) DELETE n"))?;
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

        // Group nodes by label for efficient batching
        let mut nodes_by_label: HashMap<String, Vec<&DbNode>> = HashMap::new();
        for node in nodes {
            nodes_by_label
                .entry(node.label.clone())
                .or_default()
                .push(node);
        }

        // Batch insert nodes of each label using UNWIND with flattened properties
        const BATCH_SIZE: usize = 200;
        for (cypher_label, label_nodes) in nodes_by_label {
            for chunk in label_nodes.chunks(BATCH_SIZE) {
                // Build list of flattened property maps
                let items: Vec<String> = chunk
                    .iter()
                    .map(|n| {
                        let flat_props = Neo4jDatabase::flatten_node_properties(n);
                        Neo4jDatabase::json_to_cypher_props(&flat_props)
                    })
                    .collect();

                let cypher = format!(
                    "UNWIND [{}] AS props \
                     MERGE (n:{} {{objectid: props.objectid}}) \
                     SET n += props",
                    items.join(", "),
                    cypher_label
                );

                self.run_query(query(&cypher))?;
            }
        }

        Ok(nodes.len())
    }

    fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        // Group edges by type for efficient batching
        let mut edges_by_type: HashMap<String, Vec<&DbEdge>> = HashMap::new();
        for edge in edges {
            edges_by_type
                .entry(edge.edge_type.clone())
                .or_default()
                .push(edge);
        }

        // Batch insert edges of each type using UNWIND
        // Use MERGE for nodes to create placeholders if they don't exist
        const BATCH_SIZE: usize = 500;
        let mut inserted = 0;
        for (edge_type, type_edges) in edges_by_type {
            for chunk in type_edges.chunks(BATCH_SIZE) {
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

                // MERGE nodes (creates placeholders if not exist), then create edge
                // Note: We match on objectid only (not label) so placeholder nodes merge
                // correctly with real nodes inserted later
                let q = query(&format!(
                    "UNWIND range(0, size($srcs)-1) AS i \
                     MERGE (a {{objectid: $srcs[i]}}) \
                     ON CREATE SET a.placeholder = true, a.node_type = $src_types[i] \
                     MERGE (b {{objectid: $tgts[i]}}) \
                     ON CREATE SET b.placeholder = true, b.node_type = $tgt_types[i] \
                     MERGE (a)-[r:{}]->(b) \
                     SET r.properties = $props[i] \
                     RETURN count(r) AS created",
                    edge_type
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
                        debug!("Failed to create {} edges batch: {}", edge_type, e);
                    }
                }
            }
        }

        Ok(inserted)
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        let node_rows = self.execute_query(query("MATCH (n) RETURN count(n) AS count"))?;
        let node_count = node_rows
            .first()
            .and_then(|r| r.get::<i64>("count").ok())
            .unwrap_or(0) as usize;

        let edge_rows = self.execute_query(query("MATCH ()-[r]->() RETURN count(r) AS count"))?;
        let edge_count = edge_rows
            .first()
            .and_then(|r| r.get::<i64>("count").ok())
            .unwrap_or(0) as usize;

        Ok((node_count, edge_count))
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        let (total_nodes, total_edges) = self.get_stats()?;

        // Get counts by label
        let rows = self.execute_query(query(
            "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count",
        ))?;

        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for row in rows {
            if let (Ok(label), Ok(count)) = (row.get::<String>("label"), row.get::<i64>("count")) {
                type_counts.insert(label, count as usize);
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
        let user_rows = self.execute_query(query("MATCH (n:User) RETURN count(n) AS count"))?;
        let total_users = user_rows
            .first()
            .and_then(|r| r.get::<i64>("count").ok())
            .unwrap_or(0) as usize;

        // Find real DAs (direct MemberOf path to DA groups)
        let real_da_query = format!(
            "MATCH (u:User)-[:MemberOf*1..]->(g:Group) \
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

        // Find effective DAs (any path to DA groups)
        let effective_da_query = format!(
            "MATCH p = (u:User)-[*1..10]->(g:Group) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN DISTINCT u.objectid AS id, u.name AS name, min(length(p)) AS hops",
            DOMAIN_ADMIN_SID_SUFFIX
        );
        let effective_da_rows = self.execute_query(query(&effective_da_query))?;

        let effective_das: Vec<(String, String, usize)> = effective_da_rows
            .iter()
            .filter_map(|r| {
                let id = r.get::<String>("id").ok()?;
                let name = r.get::<String>("name").ok().unwrap_or_else(|| id.clone());
                let hops = r.get::<i64>("hops").ok().unwrap_or(1) as usize;
                Some((id, name, hops))
            })
            .collect();

        // Compute reachability from well-known principals
        let mut reachability = Vec::new();
        for (name, pattern) in WELL_KNOWN_PRINCIPALS {
            let q = if pattern.starts_with('-') {
                query(&format!(
                    "MATCH (p) WHERE p.objectid ENDS WITH '{}' \
                     OPTIONAL MATCH (p)-[*1..5]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                ))
            } else {
                query(&format!(
                    "MATCH (p {{objectid: '{}'}}) \
                     OPTIONAL MATCH (p)-[*1..5]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                ))
            };

            let rows = self.execute_query(q).unwrap_or_default();
            let (principal_id, reachable_count) = rows
                .first()
                .map(|r| {
                    let id = r.get::<String>("id").ok();
                    let cnt = r.get::<i64>("cnt").ok().unwrap_or(0) as usize;
                    (id, cnt)
                })
                .unwrap_or((None, 0));

            reachability.push(ReachabilityInsight {
                principal_name: name.to_string(),
                principal_id,
                reachable_count,
            });
        }

        Ok(SecurityInsights::from_counts(
            total_users,
            real_das,
            effective_das,
            reachability,
        ))
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let rows = self.execute_query(query("MATCH (n) RETURN n"))?;

        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.get::<Neo4jNode>("n").ok())
            .map(|n| Self::neo4j_node_to_db_node(&n))
            .collect();

        Ok(nodes)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let rows = self.execute_query(query(
            "MATCH (a)-[r]->(b) RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel"
        ))?;

        let edges: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.get::<String>("src").ok()?;
                let tgt = r.get::<String>("tgt").ok()?;
                let rel = r.get::<Relation>("rel").ok()?;
                Some(Self::neo4j_relation_to_db_edge(&rel, &src, &tgt))
            })
            .collect();

        Ok(edges)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let q = query("MATCH (n) WHERE n.objectid IN $ids RETURN n").param("ids", ids.to_vec());

        let rows = self.execute_query(q)?;
        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.get::<Neo4jNode>("n").ok())
            .map(|n| Self::neo4j_node_to_db_node(&n))
            .collect();

        Ok(nodes)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let q = query(
            "MATCH (a)-[r]->(b) \
             WHERE a.objectid IN $ids AND b.objectid IN $ids \
             RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel",
        )
        .param("ids", node_ids.to_vec());

        let rows = self.execute_query(q)?;
        let edges: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.get::<String>("src").ok()?;
                let tgt = r.get::<String>("tgt").ok()?;
                let rel = r.get::<Relation>("rel").ok()?;
                Some(Self::neo4j_relation_to_db_edge(&rel, &src, &tgt))
            })
            .collect();

        Ok(edges)
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        let rows = self.execute_query(query("MATCH ()-[r]->() RETURN DISTINCT type(r) AS typ"))?;

        let types: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get::<String>("typ").ok())
            .collect();

        Ok(types)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        let rows = self.execute_query(query("MATCH (n) RETURN DISTINCT labels(n)[0] AS label"))?;

        let types: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get::<String>("label").ok())
            .collect();

        Ok(types)
    }

    fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        // Use toLower and CONTAINS for case-insensitive search (simpler than regex)
        let q = query(
            "MATCH (n) WHERE toLower(n.name) CONTAINS toLower($search) OR toLower(n.objectid) CONTAINS toLower($search) RETURN n LIMIT $limit"
        )
        .param("search", search_query.to_string())
        .param("limit", limit as i64);

        let rows = self.execute_query(q)?;
        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.get::<Neo4jNode>("n").ok())
            .map(|n| Self::neo4j_node_to_db_node(&n))
            .collect();

        debug!(query = %search_query, found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        // Try exact objectid match
        let q = query("MATCH (n {objectid: $id}) RETURN n.objectid AS id LIMIT 1")
            .param("id", identifier.to_string());

        let rows = self.execute_query(q)?;
        if let Some(row) = rows.first() {
            if let Ok(id) = row.get::<String>("id") {
                return Ok(Some(id));
            }
        }

        // Try case-insensitive name match
        let q = query(
            "MATCH (n) WHERE toLower(n.name) = toLower($name) RETURN n.objectid AS id LIMIT 1",
        )
        .param("name", identifier.to_string());

        let rows = self.execute_query(q)?;
        if let Some(row) = rows.first() {
            if let Ok(id) = row.get::<String>("id") {
                return Ok(Some(id));
            }
        }

        Ok(None)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        let q = match direction {
            "incoming" => query(
                "MATCH (a)-[r]->(b {objectid: $id}) RETURN a, r, b"
            ),
            "outgoing" => query(
                "MATCH (a {objectid: $id})-[r]->(b) RETURN a, r, b"
            ),
            "admin" => query(
                "MATCH (a {objectid: $id})-[r]->(b) \
                 WHERE type(r) IN ['AdminTo', 'GenericAll', 'GenericWrite', 'Owns', 'WriteDacl', 'WriteOwner', 'AllExtendedRights', 'ForceChangePassword', 'AddMember'] \
                 RETURN a, r, b"
            ),
            "memberof" => query(
                "MATCH (a {objectid: $id})-[r:MemberOf]->(b) RETURN a, r, b"
            ),
            "members" => query(
                "MATCH (a)-[r:MemberOf]->(b {objectid: $id}) RETURN a, r, b"
            ),
            _ => query(
                "MATCH (a {objectid: $id})-[r]-(b) RETURN a, r, b"
            ),
        }
        .param("id", node_id.to_string());

        let rows = self.execute_query(q)?;

        let mut node_ids: HashSet<String> = HashSet::new();
        node_ids.insert(node_id.to_string());

        let mut edges = Vec::new();
        for row in &rows {
            if let (Ok(a), Ok(r), Ok(b)) = (
                row.get::<Neo4jNode>("a"),
                row.get::<Relation>("r"),
                row.get::<Neo4jNode>("b"),
            ) {
                let src = Self::neo4j_node_to_db_node(&a);
                let tgt = Self::neo4j_node_to_db_node(&b);
                node_ids.insert(src.id.clone());
                node_ids.insert(tgt.id.clone());
                edges.push(Self::neo4j_relation_to_db_edge(&r, &src.id, &tgt.id));
            }
        }

        let node_id_vec: Vec<String> = node_ids.into_iter().collect();
        let nodes = self.get_nodes_by_ids(&node_id_vec)?;

        Ok((nodes, edges))
    }

    fn get_node_edge_counts(&self, node_id: &str) -> Result<(usize, usize, usize, usize, usize)> {
        // Use a single query with multiple counts for efficiency
        // Count unique NODES, not edges (e.g., 3 edges from same node = 1 incoming)
        let q = query(
            "MATCH (n {objectid: $id})
             OPTIONAL MATCH (n)<-[]-(in_node)
             OPTIONAL MATCH (n)-[]->(out_node)
             OPTIONAL MATCH (n)-[admin]->(admin_node) WHERE type(admin) IN ['AdminTo', 'GenericAll', 'GenericWrite', 'Owns', 'WriteDacl', 'WriteOwner', 'AllExtendedRights', 'ForceChangePassword', 'AddMember']
             OPTIONAL MATCH (n)-[:MemberOf]->(mo_node)
             OPTIONAL MATCH (n)<-[:MemberOf]-(mem_node)
             RETURN count(DISTINCT in_node) AS incoming,
                    count(DISTINCT out_node) AS outgoing,
                    count(DISTINCT admin_node) AS admin_to,
                    count(DISTINCT mo_node) AS member_of,
                    count(DISTINCT mem_node) AS members"
        )
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
        // Use variable-length path to find transitive MemberOf membership
        let q = query(
            "MATCH (n {objectid: $id})-[:MemberOf*1..20]->(g) \
             WHERE g.objectid ENDS WITH $suffix \
             RETURN g.objectid LIMIT 1",
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

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let q = query(
            "MATCH p = shortestPath((a {objectid: $from})-[*..20]->(b {objectid: $to})) \
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
                    let edge_type = if i < rel_types.len() {
                        Some(rel_types[i].clone())
                    } else {
                        None
                    };
                    path.push((node_id.clone(), edge_type));
                }
                // Last node has no outgoing edge
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

        let q = query(&format!(
            "MATCH p = (u:User)-[*1..10]->(da:Group) \
             WHERE da.objectid ENDS WITH '-512' {} \
             RETURN u.objectid AS id, u.name AS name, min(length(p)) AS hops \
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

        // Execute the query and collect results
        let graph = self.graph.clone();
        let cypher = cypher.to_string();
        let result = self.runtime.block_on(async {
            let mut stream = graph.execute(query(&cypher)).await?;
            let mut rows = Vec::new();

            while let Some(row) = stream.next().await? {
                // Neo4rs Row doesn't expose column names, so we try common patterns
                // For custom queries, users should use aliases like "RETURN x AS result"
                let mut obj = Map::new();

                // Try common column names that might be returned
                let try_columns = [
                    "n",
                    "m",
                    "r",
                    "p",
                    "result",
                    "count",
                    "total",
                    "name",
                    "id",
                    "value",
                    "nodes",
                    "relationships",
                    "path",
                ];

                for col in try_columns {
                    if let Ok(val) = row.get::<String>(col) {
                        obj.insert(col.to_string(), JsonValue::String(val));
                    } else if let Ok(val) = row.get::<i64>(col) {
                        obj.insert(col.to_string(), JsonValue::Number(val.into()));
                    } else if let Ok(val) = row.get::<f64>(col) {
                        if let Some(n) = serde_json::Number::from_f64(val) {
                            obj.insert(col.to_string(), JsonValue::Number(n));
                        }
                    } else if let Ok(val) = row.get::<bool>(col) {
                        obj.insert(col.to_string(), JsonValue::Bool(val));
                    } else if let Ok(node) = row.get::<Neo4jNode>(col) {
                        let db_node = Neo4jDatabase::neo4j_node_to_db_node(&node);
                        obj.insert(
                            col.to_string(),
                            json!({
                                "id": db_node.id,
                                "name": db_node.name,
                                "type": db_node.label,
                                "properties": db_node.properties,
                            }),
                        );
                    }
                }

                if !obj.is_empty() {
                    rows.push(JsonValue::Object(obj));
                }
            }

            Ok::<_, neo4rs::Error>(rows)
        })?;

        Ok(json!({ "results": result }))
    }

    fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        let q = query(
            "CREATE (h:QueryHistory {id: $id, name: $name, query: $query, timestamp: $ts, result_count: $cnt, status: $status, started_at: $started_at, duration_ms: $duration_ms, error: $error, background: $background})"
        )
        .param("id", entry.id.to_string())
        .param("name", entry.name.to_string())
        .param("query", entry.query.to_string())
        .param("ts", entry.timestamp)
        .param("cnt", entry.result_count.unwrap_or(0))
        .param("status", entry.status.to_string())
        .param("started_at", entry.started_at)
        .param("duration_ms", entry.duration_ms.map(|d| d as i64).unwrap_or(0))
        .param("error", entry.error.unwrap_or("").to_string())
        .param("background", entry.background);

        self.run_query(q)
    }

    fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let q = query(
            "MATCH (h:QueryHistory {id: $id}) \
             SET h.status = $status, h.duration_ms = $duration_ms, h.result_count = $result_count, h.error = $error"
        )
        .param("id", id.to_string())
        .param("status", status.to_string())
        .param("duration_ms", duration_ms.map(|d| d as i64).unwrap_or(0))
        .param("result_count", result_count.unwrap_or(0))
        .param("error", error.unwrap_or("").to_string());

        self.run_query(q)
    }

    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        // Get total count
        let count_rows =
            self.execute_query(query("MATCH (h:QueryHistory) RETURN count(h) AS count"))?;
        let total = count_rows
            .first()
            .and_then(|r| r.get::<i64>("count").ok())
            .unwrap_or(0) as usize;

        // Get paginated results
        let q = query(
            "MATCH (h:QueryHistory) \
             RETURN h.id AS id, h.name AS name, h.query AS query, h.timestamp AS ts, h.result_count AS cnt, \
                    h.status AS status, h.started_at AS started_at, h.duration_ms AS duration_ms, h.error AS error, \
                    h.background AS background \
             ORDER BY h.timestamp DESC \
             SKIP $offset LIMIT $limit"
        )
        .param("offset", offset as i64)
        .param("limit", limit as i64);

        let rows = self.execute_query(q)?;

        let history: Vec<QueryHistoryRow> = rows
            .iter()
            .filter_map(|r| {
                let id = r.get::<String>("id").ok()?;
                let name = r.get::<String>("name").ok()?;
                let query = r.get::<String>("query").ok()?;
                let timestamp = r.get::<i64>("ts").ok()?;
                let result_count = r.get::<i64>("cnt").ok();
                let status = r
                    .get::<String>("status")
                    .ok()
                    .unwrap_or_else(|| "completed".to_string());
                let started_at = r.get::<i64>("started_at").ok().unwrap_or(timestamp);
                let duration_ms = r.get::<i64>("duration_ms").ok().map(|d| d as u64);
                let error = r.get::<String>("error").ok().filter(|e| !e.is_empty());
                let background = r.get::<bool>("background").ok().unwrap_or(false);
                Some(QueryHistoryRow {
                    id,
                    name,
                    query,
                    timestamp,
                    result_count,
                    status,
                    started_at,
                    duration_ms,
                    error,
                    background,
                })
            })
            .collect();

        Ok((history, total))
    }

    fn delete_query_history(&self, id: &str) -> Result<()> {
        let q = query("MATCH (h:QueryHistory {id: $id}) DELETE h").param("id", id.to_string());
        self.run_query(q)
    }

    fn clear_query_history(&self) -> Result<()> {
        self.run_query(query("MATCH (h:QueryHistory) DELETE h"))
    }
}
