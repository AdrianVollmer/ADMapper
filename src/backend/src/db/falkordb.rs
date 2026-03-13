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
    DOMAIN_ADMIN_SID_SUFFIX, WELL_KNOWN_PRINCIPALS,
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

        let name = obj
            .get("properties")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| id.clone());

        let label = obj
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
            name,
            label,
            properties,
        })
    }

    /// Flatten BloodHound node properties into a single JSON object.
    fn flatten_node_properties(node: &DbNode) -> JsonValue {
        let mut props = Map::new();

        // Add core identifiers - include both objectid (BloodHound standard) and
        // objectid (internal standard) for query compatibility across all backends
        props.insert("objectid".to_string(), json!(node.id));
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
            // Get objectid from properties if available
            let objectid = node
                .properties
                .get("objectid")
                .or_else(|| node.properties.get("objectid"))
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

    fn ping(&self) -> Result<()> {
        self.run_query("RETURN 1")?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        info!("Clearing all data from FalkorDB");
        // Delete all relationships first, then all nodes
        self.run_query("MATCH ()-[r]->() DELETE r")?;
        self.run_query("MATCH (n) DELETE n")?;

        // Create indexes on objectid for fast MERGE lookups during import
        // BloodHound node types that need indexes
        debug!("Creating objectid indexes for faster imports");
        let labels = [
            "User",
            "Computer",
            "Group",
            "Domain",
            "OU",
            "GPO",
            "Container",
            "CertTemplate",
            "EnterpriseCA",
            "RootCA",
            "AIACA",
            "NTAuthStore",
            "Base", // For placeholder nodes
        ];

        for label in labels {
            // FalkorDB uses CREATE INDEX syntax
            let index_query = format!("CREATE INDEX FOR (n:{}) ON (n.objectid)", label);
            // Ignore errors (index may already exist)
            let _ = self.run_query(&index_query);
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
        if nodes.is_empty() {
            return Ok(0);
        }

        // Group nodes by label for efficient batching
        let mut nodes_by_label: std::collections::HashMap<String, Vec<&DbNode>> =
            std::collections::HashMap::new();
        for node in nodes {
            nodes_by_label
                .entry(node.label.clone())
                .or_default()
                .push(node);
        }

        // Batch insert nodes of each label using UNWIND with flattened properties
        // Use MERGE on objectid only to find existing placeholder nodes, then set label
        const BATCH_SIZE: usize = 200;
        for (cypher_label, label_nodes) in nodes_by_label {
            for chunk in label_nodes.chunks(BATCH_SIZE) {
                // Build list of flattened property maps
                let items: Vec<String> = chunk
                    .iter()
                    .map(|n| {
                        let flat_props = FalkorDbDatabase::flatten_node_properties(n);
                        FalkorDbDatabase::json_to_cypher_props(&flat_props)
                    })
                    .collect();

                // MERGE on objectid only (finds placeholders), then add label and set properties
                // REMOVE n.placeholder clears the placeholder marker if this was a placeholder
                let cypher = format!(
                    "UNWIND [{}] AS props \
                     MERGE (n {{objectid: props.objectid}}) \
                     SET n:{}, n += props \
                     REMOVE n.placeholder",
                    items.join(", "),
                    cypher_label
                );

                self.run_query(&cypher)?;
            }
        }

        Ok(nodes.len())
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

        // Batch insert relationships of each type using UNWIND
        // Use MERGE for nodes to create placeholders if they don't exist
        const BATCH_SIZE: usize = 200;
        let mut inserted = 0;
        for (rel_type, type_edges) in edges_by_type {
            for chunk in type_edges.chunks(BATCH_SIZE) {
                // Build the list literal for UNWIND
                let items: Vec<String> = chunk
                    .iter()
                    .map(|e| {
                        // Escape backslashes first, then quotes, to avoid double-escaping
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
                            .replace('\\', "\\\\")
                            .replace('\'', "\\'");
                        format!(
                            "{{src: '{}', tgt: '{}', src_type: '{}', tgt_type: '{}', props: '{}'}}",
                            src, tgt, src_type, tgt_type, props
                        )
                    })
                    .collect();

                let items_str = items.join(", ");

                // Step 1: Ensure all referenced nodes exist (placeholder if needed).
                // Done as a separate query to avoid FalkorDB's UNWIND+MERGE row
                // collapsing, which causes subsequent CREATE to lose edges when
                // multiple rows reference the same source/target node.
                let ensure_nodes = format!(
                    "UNWIND [{}] AS row \
                     MERGE (a {{objectid: row.src}}) \
                     ON CREATE SET a.placeholder = true, a.node_type = row.src_type \
                     MERGE (b {{objectid: row.tgt}}) \
                     ON CREATE SET b.placeholder = true, b.node_type = row.tgt_type",
                    items_str
                );
                self.run_query(&ensure_nodes)?;

                // Step 2: Create edges using MATCH (nodes guaranteed to exist).
                let create_edges = format!(
                    "UNWIND [{}] AS row \
                     MATCH (a {{objectid: row.src}}) \
                     MATCH (b {{objectid: row.tgt}}) \
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
            database_size_bytes: None,
            cache_entries: None,
            cache_size_bytes: None,
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
        let real_da_query = format!(
            "MATCH (u:User)-[:MemberOf*1..]->(g:Group) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN DISTINCT u.objectid AS id, u.name AS name",
            DOMAIN_ADMIN_SID_SUFFIX
        );
        let real_da_rows = self.execute_query(&real_da_query)?;

        let real_das: Vec<(String, String)> = real_da_rows
            .iter()
            .filter_map(|r| {
                let id = r.first()?.as_str()?.to_string();
                let name = r.get(1).and_then(|v| v.as_str()).unwrap_or(&id).to_string();
                Some((id, name))
            })
            .collect();

        // Find effective DAs by reusing the paths-to-DA query with no exclusions.
        let effective_das: Vec<(String, String, usize)> = self
            .find_paths_to_domain_admins(&[])?
            .into_iter()
            .map(|(id, _label, name, hops)| (id, name, hops))
            .collect();

        // Compute reachability from well-known principals.
        // Look up each principal but skip the expensive untyped variable-length
        // path traversal (OPTIONAL MATCH -[*1..5]->) which causes timeouts in
        // FalkorDB. Use a simple direct-neighbor count instead.
        let mut reachability = Vec::new();
        for (name, pattern) in WELL_KNOWN_PRINCIPALS {
            let cypher = if pattern.starts_with('-') {
                format!(
                    "MATCH (p) WHERE p.objectid ENDS WITH '{}' \
                     OPTIONAL MATCH (p)-[]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                )
            } else {
                format!(
                    "MATCH (p {{objectid: '{}'}}) \
                     OPTIONAL MATCH (p)-[]->(t) \
                     RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                    pattern
                )
            };

            let rows = self.execute_query(&cypher).unwrap_or_default();
            let (principal_id, reachable_count) = rows
                .first()
                .map(|r| {
                    let id = r.first().and_then(|v| v.as_str()).map(|s| s.to_string());
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

        Ok(SecurityInsights::from_counts(
            total_users,
            real_das,
            effective_das,
            reachability,
        ))
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let rows = self.execute_query("MATCH (n) RETURN n")?;

        let nodes: Vec<DbNode> = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(Self::parse_node)
            .collect();

        Ok(nodes)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let rows = self.execute_query(
            "MATCH (a)-[r]->(b) RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel"
        )?;

        let relationships: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.first()?.as_str()?.to_string();
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
                    rel_type: typ,
                    properties: props,
                    ..Default::default()
                })
            })
            .collect();

        Ok(relationships)
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
            .filter_map(Self::parse_node)
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
        let relationships: Vec<DbEdge> = rows
            .iter()
            .filter_map(|r| {
                let src = r.first()?.as_str()?.to_string();
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
                    rel_type: typ,
                    properties: props,
                    ..Default::default()
                })
            })
            .collect();

        Ok(relationships)
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
            .filter_map(Self::parse_node)
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

        let mut relationships = Vec::new();
        for row in &rows {
            if row.len() >= 3 {
                if let (Some(src_node), Some(tgt_node)) =
                    (Self::parse_node(&row[0]), Self::parse_node(&row[2]))
                {
                    node_ids.insert(src_node.id.clone());
                    node_ids.insert(tgt_node.id.clone());

                    if let Some(rel) = row[1].as_object() {
                        let rel_type = rel
                            .get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("RELATED")
                            .to_string();
                        let props = rel
                            .get("properties")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Map::new()));

                        relationships.push(DbEdge {
                            source: src_node.id,
                            target: tgt_node.id,
                            rel_type,
                            properties: props,
                            ..Default::default()
                        });
                    }
                }
            }
        }

        let node_id_vec: Vec<String> = node_ids.into_iter().collect();
        let nodes = self.get_nodes_by_ids(&node_id_vec)?;

        Ok((nodes, relationships))
    }

    fn get_node_relationship_counts(
        &self,
        node_id: &str,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        let id_escaped = node_id.replace('\'', "\\'");

        // Use WITH to chain counts and avoid Cartesian product explosion
        // Each OPTIONAL MATCH is followed by aggregation before the next
        let cypher = format!(
            "MATCH (n {{objectid: '{}'}})
             OPTIONAL MATCH (n)<-[]-(in_node)
             WITH n, count(DISTINCT in_node) AS incoming
             OPTIONAL MATCH (n)-[]->(out_node)
             WITH n, incoming, count(DISTINCT out_node) AS outgoing
             OPTIONAL MATCH (n)-[admin]->(admin_node) WHERE type(admin) IN ['AdminTo', 'GenericAll', 'GenericWrite', 'Owns', 'WriteDacl', 'WriteOwner', 'AllExtendedRights', 'ForceChangePassword', 'AddMember']
             WITH n, incoming, outgoing, count(DISTINCT admin_node) AS admin_to
             OPTIONAL MATCH (n)-[:MemberOf]->(mo_node)
             WITH n, incoming, outgoing, admin_to, count(DISTINCT mo_node) AS member_of
             OPTIONAL MATCH (n)<-[:MemberOf]-(mem_node)
             RETURN incoming, outgoing, admin_to, member_of, count(DISTINCT mem_node) AS members",
            id_escaped
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

    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        let id_escaped = node_id.replace('\'', "\\'");
        let suffix_escaped = sid_suffix.replace('\'', "\\'");

        // Use variable-length path to find transitive MemberOf membership
        let cypher = format!(
            "MATCH (n {{objectid: '{}'}})-[:MemberOf*1..20]->(g) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN g.objectid LIMIT 1",
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

        // FalkorDB requires endpoints to be resolved before shortestPath call
        // and uses WITH clause syntax per docs: WITH shortestPath(...) as p
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
                                .and_then(|o| o.get("type"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        };

                        path.push((node_id, rel_type));
                    }

                    // Last node has no outgoing relationship
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

        // FalkorDB only supports shortestPath in WITH/RETURN clauses,
        // not in MATCH patterns.
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

        // Return in expected format with rows array
        Ok(json!({ "rows": rows }))
    }
}
