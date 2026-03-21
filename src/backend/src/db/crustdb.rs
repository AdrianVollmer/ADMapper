//! CrustDB-backed graph database for storing AD graph data.
//!
//! Uses Cypher queries for graph operations via the embedded crustdb engine.

use crustdb::{Database, EntityCacheConfig};
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

/// Normalize BloodHound type name to standard format.
/// This ensures consistent labeling regardless of case in source data.
fn normalize_node_type(data_type: &str) -> String {
    match data_type.to_lowercase().as_str() {
        "users" | "user" => "User",
        "groups" | "group" => "Group",
        "computers" | "computer" => "Computer",
        "domains" | "domain" => "Domain",
        "gpos" | "gpo" => "GPO",
        "ous" | "ou" => "OU",
        "containers" | "container" => "Container",
        "certtemplates" | "certtemplate" => "CertTemplate",
        "enterprisecas" | "enterpriseca" => "EnterpriseCA",
        "rootcas" | "rootca" => "RootCA",
        "aiacas" | "aiaca" => "AIACA",
        "ntauthstores" | "ntauthstore" => "NTAuthStore",
        _ => "Base",
    }
    .to_string()
}

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{
    DbEdge, DbError, DbNode, DetailedStats, ReachabilityInsight, Result, SecurityInsights,
    DOMAIN_ADMIN_SID_SUFFIX, WELL_KNOWN_PRINCIPALS,
};

/// A graph database backed by CrustDB.
///
/// Database handles its own thread-safety internally via Mutex.
/// For concurrent queries, a connection pool would be needed.
#[derive(Clone)]
pub struct CrustDatabase {
    db: Arc<Database>,
}

impl CrustDatabase {
    /// Create or open a database at the given path.
    ///
    /// If `enable_caching` is true, query results for read-only queries will be cached
    /// and automatically invalidated when data changes.
    pub fn new<P: AsRef<Path>>(path: P, enable_caching: bool) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!(path = %path_str, caching = %enable_caching, "Opening CrustDB");

        let mut db = Database::open(&path_str).map_err(|e| DbError::Database(e.to_string()))?;
        db.set_caching(enable_caching);
        // Enable entity cache for faster BFS/shortest path traversals
        db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));
        // Cap intermediate bindings to prevent OOM on explosive queries
        db.set_max_intermediate_bindings(Some(5_000_000));

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        info!("CrustDB initialized successfully");
        Ok(instance)
    }

    /// Create an in-memory database (useful for testing).
    pub fn in_memory() -> Result<Self> {
        debug!("Creating in-memory CrustDB");
        let mut db = Database::in_memory().map_err(|e| DbError::Database(e.to_string()))?;
        db.set_caching(true); // Enable caching by default for tests too
                              // Enable entity cache for faster BFS/shortest path traversals
        db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));
        // Cap intermediate bindings to prevent OOM on explosive queries
        db.set_max_intermediate_bindings(Some(5_000_000));

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Initialize the schema by creating indexes and base structures.
    fn init_schema(&self) -> Result<()> {
        debug!("Initializing CrustDB schema");
        // CrustDB auto-creates nodes/relationships on first use

        // Create property indexes for commonly queried fields
        // These significantly speed up node lookups by objectid and name
        self.db
            .create_property_index("objectid")
            .map_err(|e| DbError::Database(e.to_string()))?;
        self.db
            .create_property_index("name")
            .map_err(|e| DbError::Database(e.to_string()))?;

        debug!("Property indexes created for objectid and name");
        Ok(())
    }

    /// Execute a Cypher query and return the raw result.
    fn execute(&self, query: &str) -> Result<crustdb::QueryResult> {
        self.db
            .execute(query)
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Clear all data from the database.
    pub fn clear(&self) -> Result<()> {
        info!("Clearing all data from CrustDB");
        self.db
            .clear()
            .map_err(|e| DbError::Database(e.to_string()))?;
        debug!("Database cleared");
        Ok(())
    }

    /// Insert a batch of nodes using efficient batch upsert.
    ///
    /// This uses CrustDB's native batch upsert which wraps all upserts
    /// in a single transaction with prepared statements.
    ///
    /// If a node with the same objectid already exists (e.g., an orphan placeholder
    /// created during relationship insertion), its properties are **merged** rather than
    /// replaced. This enables streaming relationship import.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        // Convert DbNodes to the format expected by CrustDB batch upsert.
        // Every node gets a "Base" label in addition to its type-specific
        // label, matching Neo4j/FalkorDB conventions.
        let batch: Vec<(Vec<String>, serde_json::Value)> = nodes
            .iter()
            .map(|node| {
                let labels = if node.label == "Base" {
                    vec!["Base".to_string()]
                } else {
                    vec![node.label.clone(), "Base".to_string()]
                };
                // Flatten BloodHound properties into top-level fields
                let props = Self::flatten_node_properties(node);
                (labels, props)
            })
            .collect();

        // Use upsert to merge with any existing placeholder nodes
        match self.db.upsert_nodes_batch(&batch) {
            Ok(ids) => {
                debug!("Batch upserted {} nodes", ids.len());
                Ok(ids.len())
            }
            Err(e) => {
                debug!(
                    "Batch upsert failed, falling back to individual inserts: {}",
                    e
                );
                // Fallback to individual inserts if batch fails
                self.insert_nodes_fallback(nodes)
            }
        }
    }

    /// Fallback method for individual node inserts (used if batch fails).
    fn insert_nodes_fallback(&self, nodes: &[DbNode]) -> Result<usize> {
        let mut count = 0;
        for node in nodes {
            // Build flattened properties for Cypher
            let props = Self::flatten_node_properties(node);
            let props_str = Self::json_to_cypher_props(&props);
            let cypher_label = node.label.replace('\'', "''");

            // Add :Base as a secondary label (matching Neo4j/FalkorDB)
            let label_clause = if cypher_label == "Base" {
                "Base".to_string()
            } else {
                format!("{}:Base", cypher_label)
            };
            let query = format!("CREATE (n:{} {})", label_clause, props_str);

            if self.execute(&query).is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Flatten BloodHound node properties into a single JSON object.
    ///
    /// This merges the nested `properties` from BloodHound into top-level fields,
    /// making them directly queryable in Cypher.
    fn flatten_node_properties(node: &DbNode) -> serde_json::Value {
        let mut props = serde_json::Map::new();

        // Add core identifiers
        props.insert("objectid".to_string(), serde_json::json!(node.id));
        props.insert("name".to_string(), serde_json::json!(node.name));
        props.insert("label".to_string(), serde_json::json!(node.label));

        // Flatten BloodHound properties into top-level fields
        if let serde_json::Value::Object(bh_props) = &node.properties {
            for (key, value) in bh_props {
                // Skip null values and empty arrays to save space
                if value.is_null() {
                    continue;
                }
                if let Some(arr) = value.as_array() {
                    if arr.is_empty() {
                        continue;
                    }
                }
                // Don't overwrite core fields
                if key != "objectid" && key != "name" && key != "label" {
                    props.insert(key.clone(), value.clone());
                }
            }
        }

        serde_json::Value::Object(props)
    }

    /// Convert a JSON object to Cypher property syntax.
    fn json_to_cypher_props(value: &serde_json::Value) -> String {
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
    fn json_value_to_cypher(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Null => None,
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            serde_json::Value::Array(arr) => {
                let items: Vec<String> =
                    arr.iter().filter_map(Self::json_value_to_cypher).collect();
                Some(format!("[{}]", items.join(", ")))
            }
            serde_json::Value::Object(_) => {
                // Skip nested objects for now - Cypher doesn't support them directly
                None
            }
        }
    }

    /// Insert a batch of relationships using efficient batch insert.
    ///
    /// This builds an index of objectid -> node_id for efficient lookups,
    /// then uses CrustDB's native batch insert.
    pub fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize> {
        if relationships.is_empty() {
            return Ok(0);
        }

        // Build index of objectid -> node_id for efficient lookups
        let node_index = self.db.build_property_index("objectid")?;

        // Collect unique placeholder nodes to create (deduplicated)
        let mut placeholder_set: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for relationship in relationships {
            let source_id = node_index.get(&relationship.source);
            let target_id = node_index.get(&relationship.target);

            // Create placeholder for missing source
            if source_id.is_none() {
                let node_type = relationship
                    .source_type
                    .as_deref()
                    .map(normalize_node_type)
                    .unwrap_or_else(|| "Base".to_string());
                placeholder_set.insert((relationship.source.clone(), node_type));
            }
            // Create placeholder for missing target
            if target_id.is_none() {
                let node_type = relationship
                    .target_type
                    .as_deref()
                    .map(normalize_node_type)
                    .unwrap_or_else(|| "Base".to_string());
                placeholder_set.insert((relationship.target.clone(), node_type));
            }
        }

        // Insert placeholder nodes using batch insert
        let node_index = if !placeholder_set.is_empty() {
            debug!("Creating {} placeholder nodes", placeholder_set.len());

            let placeholder_batch: Vec<(Vec<String>, serde_json::Value)> = placeholder_set
                .iter()
                .map(|(objectid, node_type)| {
                    let labels = vec![node_type.clone()];
                    let props = serde_json::json!({
                        "objectid": objectid,
                        "name": objectid,
                        "placeholder": true,
                        "node_type": node_type,
                    });
                    (labels, props)
                })
                .collect();

            self.db.insert_nodes_batch(&placeholder_batch)?;
            debug!("Inserted {} placeholder nodes", placeholder_set.len());

            // Rebuild index after creating placeholders
            self.db.build_property_index("objectid")?
        } else {
            node_index
        };

        // Convert relationships to the format expected by CrustDB batch insert
        let mut batch: Vec<(i64, i64, String, serde_json::Value)> =
            Vec::with_capacity(relationships.len());
        let mut skipped = 0;

        for relationship in relationships {
            let source_id = node_index.get(&relationship.source);
            let target_id = node_index.get(&relationship.target);

            match (source_id, target_id) {
                (Some(&src), Some(&tgt)) => {
                    let props = serde_json::json!({
                        "properties": serde_json::to_string(&relationship.properties).unwrap_or_default()
                    });
                    batch.push((src, tgt, relationship.rel_type.clone(), props));
                }
                _ => {
                    debug!(
                        "Skipping relationship {} -> {}: source or target not found",
                        relationship.source, relationship.target
                    );
                    skipped += 1;
                }
            }
        }

        if batch.is_empty() {
            debug!("No valid relationships to insert (skipped {})", skipped);
            return Ok(0);
        }

        let ids = self.db.insert_relationships_batch(&batch)?;
        debug!(
            "Batch inserted {} relationships (skipped {})",
            ids.len(),
            skipped
        );
        Ok(ids.len())
    }

    /// Insert a single node.
    pub fn insert_node(&self, node: DbNode) -> Result<()> {
        self.insert_nodes(&[node])?;
        Ok(())
    }

    /// Insert a single relationship.
    pub fn insert_edge(&self, relationship: DbEdge) -> Result<()> {
        self.insert_edges(&[relationship])?;
        Ok(())
    }

    /// Get node and relationship counts.
    ///
    /// Uses efficient SQL via CrustDB's stats() method instead of Cypher queries.
    pub fn get_stats(&self) -> Result<(usize, usize)> {
        let stats = self
            .db
            .stats()
            .map_err(|e| DbError::Database(e.to_string()))?;
        Ok((stats.node_count, stats.relationship_count))
    }

    /// Extract count from a query result.
    #[allow(dead_code)]
    fn extract_count(&self, result: &crustdb::QueryResult) -> usize {
        result
            .rows
            .first()
            .and_then(|row| row.values.values().next())
            .and_then(|v| match v {
                crustdb::ResultValue::Property(crustdb::PropertyValue::Integer(n)) => {
                    Some(*n as usize)
                }
                _ => None,
            })
            .unwrap_or(0)
    }

    /// Get detailed stats including counts by node type.
    ///
    /// Uses efficient SQL queries via get_label_counts() instead of
    /// multiple Cypher queries, reducing ~5 seconds to ~50ms.
    pub fn get_detailed_stats(&self) -> Result<DetailedStats> {
        // Get basic stats (2 fast SQL queries)
        let stats = self
            .db
            .stats()
            .map_err(|e| DbError::Database(e.to_string()))?;

        // Get all label counts in a single SQL query
        let label_counts = self
            .db
            .get_label_counts()
            .map_err(|e| DbError::Database(e.to_string()))?;

        // Get database size and cache stats
        let database_size = self
            .db
            .database_size()
            .map_err(|e| DbError::Database(e.to_string()))?;
        let cache_stats = self
            .db
            .cache_stats()
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(DetailedStats {
            total_nodes: stats.node_count,
            total_edges: stats.relationship_count,
            users: label_counts.get("User").copied().unwrap_or(0),
            computers: label_counts.get("Computer").copied().unwrap_or(0),
            groups: label_counts.get("Group").copied().unwrap_or(0),
            domains: label_counts.get("Domain").copied().unwrap_or(0),
            ous: label_counts.get("OU").copied().unwrap_or(0),
            gpos: label_counts.get("GPO").copied().unwrap_or(0),
            database_size_bytes: Some(database_size),
            cache_entries: Some(cache_stats.entry_count),
            cache_size_bytes: Some(cache_stats.total_size_bytes),
        })
    }

    /// Get all nodes.
    pub fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let result = self.execute("MATCH (n) RETURN n")?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Extract a DbNode from a query result row.
    fn extract_db_node_from_result(
        values: &std::collections::HashMap<String, crustdb::ResultValue>,
        key: &str,
    ) -> Option<DbNode> {
        let value = values.get(key)?;

        match value {
            crustdb::ResultValue::Node {
                id: _,
                labels,
                properties,
            } => {
                let objectid = properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let name = properties
                    .get("name")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| objectid.clone());

                // Get node type: prefer Cypher labels (excluding "Base" which is a
                // generic super-label), then node_type property
                let label = labels
                    .iter()
                    .find(|l| *l != "Base")
                    .cloned()
                    .or_else(|| labels.first().cloned())
                    .or_else(|| {
                        properties.get("node_type").and_then(|v| {
                            if let crustdb::PropertyValue::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .unwrap_or_else(|| "Unknown".to_string());

                // Convert all properties to JSON
                let props_json = Self::properties_to_json(properties);

                Some(DbNode {
                    id: objectid,
                    name,
                    label,
                    properties: props_json,
                })
            }
            _ => None,
        }
    }

    /// Convert CrustDB properties to JSON.
    fn properties_to_json(
        properties: &std::collections::HashMap<String, crustdb::PropertyValue>,
    ) -> JsonValue {
        let map: serde_json::Map<String, JsonValue> = properties
            .iter()
            .map(|(k, v)| (k.clone(), Self::property_value_to_json(v)))
            .collect();
        JsonValue::Object(map)
    }

    /// Get all relationships.
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let result = self
            .execute("MATCH (a)-[r]->(b) RETURN a.objectid, b.objectid, type(r), r.properties")?;

        let mut relationships = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");
            let props_str = self.get_string_value(&row.values, "r.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            relationships.push(DbEdge {
                source,
                target,
                rel_type,
                properties,
                ..Default::default()
            });
        }

        Ok(relationships)
    }

    /// Helper to extract string value from result row.
    fn get_string_value(
        &self,
        values: &std::collections::HashMap<String, crustdb::ResultValue>,
        key: &str,
    ) -> String {
        values
            .get(key)
            .and_then(|v| match v {
                crustdb::ResultValue::Property(crustdb::PropertyValue::String(s)) => {
                    Some(s.clone())
                }
                crustdb::ResultValue::Property(crustdb::PropertyValue::Integer(n)) => {
                    Some(n.to_string())
                }
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Get all distinct relationship types.
    ///
    /// Uses direct SQL query on the normalized rel_types table for O(distinct_types)
    /// performance instead of O(edges) full scan via Cypher.
    pub fn get_edge_types(&self) -> Result<Vec<String>> {
        // Use the optimized storage method that queries rel_types table directly
        // This is O(distinct_types) instead of O(edges)
        self.db
            .get_all_relationship_types()
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Get all distinct node labels (Cypher labels).
    ///
    /// Uses direct SQL query on the normalized node_labels table for O(distinct_labels)
    /// performance instead of O(nodes) full scan via Cypher.
    pub fn get_node_types(&self) -> Result<Vec<String>> {
        // Use the optimized storage method that queries node_labels table directly.
        // Filter out "Base" which is a generic super-label, not a real node type.
        self.db
            .get_all_labels()
            .map(|labels| labels.into_iter().filter(|l| l != "Base").collect())
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Search nodes by name (case-insensitive substring match).
    pub fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let query_escaped = search_query.replace('\'', "''").to_lowercase();

        // CrustDB supports CONTAINS for string matching
        // Use toLower() for case-insensitive search
        // Search both n.name (BloodHound property) and n.objectid
        let query = format!(
            "MATCH (n) WHERE toLower(n.name) CONTAINS '{}' OR toLower(n.objectid) CONTAINS '{}' \
             RETURN n LIMIT {}",
            query_escaped, query_escaped, limit
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        debug!(query = %search_query, found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    /// Resolve a node identifier to an object ID.
    pub fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        let id_escaped = identifier.replace('\'', "''");

        // Try exact objectid match
        let query = format!(
            "MATCH (n {{objectid: '{}'}}) RETURN n.objectid LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.objectid"),
                ));
            }
        }

        // Try name match
        let query = format!(
            "MATCH (n) WHERE n.name = '{}' RETURN n.objectid LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.objectid"),
                ));
            }
        }

        Ok(None)
    }

    /// Find shortest path between two nodes using incremental BFS.
    ///
    /// Uses on-demand neighbor lookups instead of preloading the entire graph,
    /// which dramatically reduces memory usage and improves time-to-first-result
    /// for large graphs. Complexity is O(visited * avg_degree) instead of O(E).
    #[allow(clippy::type_complexity)]
    pub fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let mut visited = std::collections::HashSet::new();
        let mut parent: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        queue.push_back(from.to_string());
        visited.insert(from.to_string());

        while let Some(current) = queue.pop_front() {
            if current == to {
                let mut path = vec![(to.to_string(), None)];
                let mut node = to.to_string();
                while let Some((prev, rel_type)) = parent.get(&node) {
                    path.push((prev.clone(), Some(rel_type.clone())));
                    node = prev.clone();
                }
                path.reverse();
                return Ok(Some(path));
            }

            // Query neighbors on-demand instead of preloading entire graph
            let edges = self
                .db
                .find_outgoing_relationships_by_objectid(&current)
                .map_err(|e| DbError::Database(e.to_string()))?;

            for (neighbor, rel_type) in edges {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor.clone());
                    parent.insert(neighbor.clone(), (current.clone(), rel_type));
                    queue.push_back(neighbor);
                }
            }
        }

        Ok(None)
    }

    /// Find paths to Domain Admins.
    pub fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        debug!(exclude = ?exclude_edge_types, "Finding paths to Domain Admins");

        let nodes = self.get_all_nodes()?;
        let relationships = self.get_all_edges()?;

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with("-512"))
            .map(|n| n.id.as_str())
            .collect();

        if da_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Build adjacency list, filtering excluded relationship types
        let exclude_set: std::collections::HashSet<&str> =
            exclude_edge_types.iter().map(|s| s.as_str()).collect();

        let mut adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for relationship in &relationships {
            if !exclude_set.contains(relationship.rel_type.as_str()) {
                adj.entry(relationship.source.clone())
                    .or_default()
                    .push((relationship.target.clone(), relationship.rel_type.clone()));
            }
        }

        // BFS from each user to find paths to DA
        let users: Vec<&DbNode> = nodes.iter().filter(|n| n.label == "User").collect();

        let mut results = Vec::new();
        for user in users {
            if let Some(hops) = self.bfs_to_targets(&user.id, &da_groups, &adj) {
                results.push((user.id.clone(), user.label.clone(), user.name.clone(), hops));
            }
        }

        results.sort_by_key(|r| r.3);
        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    /// BFS to find shortest path to any target.
    fn bfs_to_targets(
        &self,
        start: &str,
        targets: &[&str],
        adj: &std::collections::HashMap<String, Vec<(String, String)>>,
    ) -> Option<usize> {
        let target_set: std::collections::HashSet<&str> = targets.iter().copied().collect();

        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();

        queue.push_back((start.to_string(), 0usize));
        visited.insert(start.to_string());

        while let Some((current, depth)) = queue.pop_front() {
            if target_set.contains(current.as_str()) {
                return Some(depth);
            }

            if let Some(neighbors) = adj.get(&current) {
                for (neighbor, _) in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(neighbor.clone());
                        queue.push_back((neighbor.clone(), depth + 1));
                    }
                }
            }
        }

        None
    }

    /// Get nodes by IDs.
    pub fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let id_list: Vec<String> = ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();

        // Return full node to get all flattened properties
        let query = format!(
            "MATCH (n) WHERE n.objectid IN [{}] RETURN n",
            id_list.join(", ")
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Get relationships between nodes.
    pub fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let id_list: Vec<String> = node_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_set = id_list.join(", ");

        let query = format!(
            "MATCH (a)-[r]->(b) \
             WHERE a.objectid IN [{}] AND b.objectid IN [{}] \
             RETURN a.objectid, b.objectid, type(r), r.properties",
            id_set, id_set
        );

        let result = self.execute(&query)?;

        let mut relationships = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");
            let props_str = self.get_string_value(&row.values, "r.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            relationships.push(DbEdge {
                source,
                target,
                rel_type,
                properties,
                ..Default::default()
            });
        }

        Ok(relationships)
    }

    /// Get node connections in a direction.
    ///
    /// For "incoming" and "outgoing" directions, uses direct SQL queries
    /// with the objectid index for O(degree) performance instead of O(N)
    /// full node scans. Other directions use Cypher queries.
    pub fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        // Use optimized SQL path for incoming/outgoing (the common case)
        match direction {
            "incoming" => {
                return self.get_node_connections_sql(node_id, true);
            }
            "outgoing" => {
                return self.get_node_connections_sql(node_id, false);
            }
            _ => {}
        }

        // For other directions (admin, memberof, members), use Cypher
        let escaped_id = node_id.replace('\'', "\\'");
        let query = match direction {
            "admin" => format!(
                "MATCH (a {{objectid: '{}'}})-[r]->(b) \
                 WHERE type(r) = 'AdminTo' OR type(r) = 'GenericAll' OR type(r) = 'GenericWrite' \
                 OR type(r) = 'Owns' OR type(r) = 'WriteDacl' OR type(r) = 'WriteOwner' \
                 OR type(r) = 'AllExtendedRights' OR type(r) = 'ForceChangePassword' \
                 OR type(r) = 'AddMember' \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            "memberof" => format!(
                "MATCH (a {{objectid: '{}'}})-[r:MemberOf]->(b) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            "members" => format!(
                "MATCH (a)-[r:MemberOf]->(b {{objectid: '{}'}}) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            _ => format!(
                "MATCH (a)-[r]-(b {{objectid: '{}'}}) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
        };

        self.get_node_connections_cypher(&query, node_id)
    }

    /// Get node connections using optimized direct SQL.
    ///
    /// This bypasses Cypher parsing and uses indexed SQL queries for
    /// O(degree) performance instead of O(N) full node scans.
    fn get_node_connections_sql(
        &self,
        node_id: &str,
        incoming: bool,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        let (crust_nodes, crust_edges) = if incoming {
            self.db
                .get_incoming_connections_by_objectid(node_id)
                .map_err(|e| DbError::Database(e.to_string()))?
        } else {
            self.db
                .get_outgoing_connections_by_objectid(node_id)
                .map_err(|e| DbError::Database(e.to_string()))?
        };

        // Build map from internal node ID to objectid for relationship conversion
        let mut internal_to_objectid: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();

        // Convert crustdb::Node to DbNode and build ID map
        let nodes: Vec<DbNode> = crust_nodes
            .into_iter()
            .map(|n| {
                let objectid = n
                    .properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| n.id.to_string());

                // Store mapping from internal ID to objectid
                internal_to_objectid.insert(n.id, objectid.clone());

                let name = n
                    .properties
                    .get("name")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| objectid.clone());

                let label = n
                    .labels
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Unknown".to_string());
                let properties = Self::properties_to_json(&n.properties);

                DbNode {
                    id: objectid,
                    name,
                    label,
                    properties,
                }
            })
            .collect();

        // Convert crustdb::Relationship to DbEdge using the ID map
        let relationships: Vec<DbEdge> = crust_edges
            .into_iter()
            .filter_map(|e| {
                // Map internal IDs to objectids
                let source_obj_id = internal_to_objectid.get(&e.source)?;
                let target_obj_id = internal_to_objectid.get(&e.target)?;

                Some(DbEdge {
                    source: source_obj_id.clone(),
                    target: target_obj_id.clone(),
                    rel_type: e.rel_type,
                    properties: Self::properties_to_json(&e.properties),
                    ..Default::default()
                })
            })
            .collect();

        Ok((nodes, relationships))
    }

    /// Execute a Cypher query and extract node connections from the result.
    fn get_node_connections_cypher(
        &self,
        query: &str,
        node_id: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        let result = self.execute(query)?;

        let mut relationships = Vec::new();
        let mut nodes_map: std::collections::HashMap<String, DbNode> =
            std::collections::HashMap::new();

        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");

            relationships.push(DbEdge {
                source: source.clone(),
                target: target.clone(),
                rel_type,
                properties: JsonValue::Null,
                ..Default::default()
            });

            // Extract node info from the result
            if let Some(crustdb::ResultValue::Node {
                labels, properties, ..
            }) = row.values.get("a")
            {
                if !nodes_map.contains_key(&source) {
                    let cypher_label = labels.first().cloned().unwrap_or_default();
                    let props = Self::props_to_json(properties);
                    let name = props
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&source)
                        .to_string();
                    nodes_map.insert(
                        source.clone(),
                        DbNode {
                            id: source.clone(),
                            name,
                            label: cypher_label,
                            properties: props,
                        },
                    );
                }
            }

            if let Some(crustdb::ResultValue::Node {
                labels, properties, ..
            }) = row.values.get("b")
            {
                if !nodes_map.contains_key(&target) {
                    let cypher_label = labels.first().cloned().unwrap_or_default();
                    let props = Self::props_to_json(properties);
                    let name = props
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&target)
                        .to_string();
                    nodes_map.insert(
                        target.clone(),
                        DbNode {
                            id: target.clone(),
                            name,
                            label: cypher_label,
                            properties: props,
                        },
                    );
                }
            }
        }

        // Always include the source node (matches Neo4j/FalkorDB behavior)
        if !nodes_map.contains_key(node_id) {
            if let Ok(source_nodes) = self.get_nodes_by_ids(&[node_id.to_string()]) {
                if let Some(source_node) = source_nodes.into_iter().next() {
                    nodes_map.insert(node_id.to_string(), source_node);
                }
            }
        }

        let nodes: Vec<DbNode> = nodes_map.into_values().collect();
        Ok((nodes, relationships))
    }

    /// Convert CrustDB properties to JSON.
    fn props_to_json(
        props: &std::collections::HashMap<String, crustdb::PropertyValue>,
    ) -> JsonValue {
        let map: serde_json::Map<String, JsonValue> = props
            .iter()
            .map(|(k, v)| (k.clone(), Self::property_value_to_json(v)))
            .collect();
        JsonValue::Object(map)
    }

    /// Get all relationships for a node (both incoming and outgoing) with relationship types.
    /// Used for efficient counting by the backend layer.
    /// Uses direct SQL for efficiency instead of Cypher queries.
    pub fn get_node_edges(&self, node_id: &str) -> Result<Vec<DbEdge>> {
        let raw_edges = self
            .db
            .get_node_relationships_by_objectid(node_id)
            .map_err(|e| DbError::Database(e.to_string()))?;

        let relationships = raw_edges
            .into_iter()
            .map(|(source, target, rel_type)| DbEdge {
                source,
                target,
                rel_type,
                properties: JsonValue::Null,
                ..Default::default()
            })
            .collect();

        Ok(relationships)
    }

    /// Find membership in a group with matching SID suffix using graph traversal.
    pub fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        let id_escaped = node_id.replace('\'', "''");
        let suffix_escaped = sid_suffix.replace('\'', "''");

        // Use variable-length path to find transitive MemberOf membership
        let query = format!(
            "MATCH p = shortestPath((n {{objectid: '{}'}})-[:MemberOf*1..20]->(g)) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN g.objectid",
            id_escaped, suffix_escaped
        );

        let result = self.execute(&query)?;

        if let Some(crustdb::ResultValue::Property(crustdb::PropertyValue::String(s))) = result
            .rows
            .first()
            .and_then(|row| row.values.get("g.objectid"))
        {
            return Ok(Some(s.clone()));
        }

        Ok(None)
    }

    /// Run a custom Cypher query.
    pub fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        debug!(query = %query, "Running custom Cypher query");

        let result = self.execute(query)?;

        let headers: Vec<String> = result.columns.clone();
        let rows: Vec<Vec<JsonValue>> = result
            .rows
            .iter()
            .map(|row| {
                headers
                    .iter()
                    .map(|col| {
                        row.values
                            .get(col)
                            .map(Self::result_value_to_json)
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect()
            })
            .collect();

        Ok(serde_json::json!({
            "headers": headers,
            "rows": rows
        }))
    }

    /// Convert a CrustDB ResultValue to JSON.
    fn result_value_to_json(v: &crustdb::ResultValue) -> JsonValue {
        match v {
            crustdb::ResultValue::Property(pv) => Self::property_value_to_json(pv),
            crustdb::ResultValue::Node {
                id,
                labels,
                properties,
            } => {
                let props: serde_json::Map<String, JsonValue> = properties
                    .iter()
                    .map(|(k, pv)| (k.clone(), Self::property_value_to_json(pv)))
                    .collect();
                // Get objectid from properties if available
                let objectid = properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| id.to_string());
                serde_json::json!({
                    "_type": "node",
                    "id": id,
                    "objectid": objectid,
                    "labels": labels,
                    "properties": props
                })
            }
            crustdb::ResultValue::Relationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let props: serde_json::Map<String, JsonValue> = properties
                    .iter()
                    .map(|(k, pv)| (k.clone(), Self::property_value_to_json(pv)))
                    .collect();
                serde_json::json!({
                    "_type": "relationship",
                    "id": id,
                    "source": source,
                    "target": target,
                    "rel_type": rel_type,
                    "properties": props
                })
            }
            crustdb::ResultValue::Path {
                nodes,
                relationships,
            } => {
                serde_json::json!({
                    "_type": "path",
                    "nodes": nodes,
                    "relationships": relationships
                })
            }
        }
    }

    /// Convert a CrustDB PropertyValue to JSON.
    fn property_value_to_json(pv: &crustdb::PropertyValue) -> JsonValue {
        match pv {
            crustdb::PropertyValue::String(s) => JsonValue::String(s.clone()),
            crustdb::PropertyValue::Integer(n) => JsonValue::Number((*n).into()),
            crustdb::PropertyValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            crustdb::PropertyValue::Bool(b) => JsonValue::Bool(*b),
            crustdb::PropertyValue::Null => JsonValue::Null,
            crustdb::PropertyValue::List(items) => {
                JsonValue::Array(items.iter().map(Self::property_value_to_json).collect())
            }
            crustdb::PropertyValue::Map(map) => {
                let obj: serde_json::Map<String, JsonValue> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::property_value_to_json(v)))
                    .collect();
                JsonValue::Object(obj)
            }
        }
    }

    /// Get security insights.
    pub fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        let nodes = self.get_all_nodes()?;
        let relationships = self.get_all_edges()?;

        let total_users = nodes.iter().filter(|n| n.label == "User").count();

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with(DOMAIN_ADMIN_SID_SUFFIX))
            .map(|n| n.id.as_str())
            .collect();

        // Build MemberOf adjacency
        let mut memberof_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for relationship in &relationships {
            if relationship.rel_type == "MemberOf" {
                memberof_adj
                    .entry(relationship.source.clone())
                    .or_default()
                    .push(relationship.target.clone());
            }
        }

        // Find real DAs (users directly or transitively in DA group via MemberOf)
        let mut real_das = Vec::new();
        for user in nodes.iter().filter(|n| n.label == "User") {
            if self.is_transitive_member(&user.id, &da_groups, &memberof_adj) {
                real_das.push((user.id.clone(), user.name.clone()));
            }
        }

        // Build full adjacency for effective DA paths
        let mut full_adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for relationship in &relationships {
            full_adj
                .entry(relationship.source.clone())
                .or_default()
                .push((relationship.target.clone(), relationship.rel_type.clone()));
        }

        // Find effective DAs (any path to DA group)
        let mut effective_das = Vec::new();
        for user in nodes.iter().filter(|n| n.label == "User") {
            if let Some(hops) = self.bfs_to_targets(&user.id, &da_groups, &full_adj) {
                effective_das.push((user.id.clone(), user.name.clone(), hops));
            }
        }

        // Simplified reachability (placeholder - returns all principals with 0 count)
        let reachability: Vec<ReachabilityInsight> = WELL_KNOWN_PRINCIPALS
            .iter()
            .map(|(name, _)| ReachabilityInsight {
                principal_name: name.to_string(),
                principal_id: None,
                reachable_count: 0,
            })
            .collect();

        Ok(SecurityInsights::from_counts(
            total_users,
            real_das,
            effective_das,
            reachability,
        ))
    }

    /// Check if a node is transitively member of any target via MemberOf.
    fn is_transitive_member(
        &self,
        start: &str,
        targets: &[&str],
        memberof_adj: &std::collections::HashMap<String, Vec<String>>,
    ) -> bool {
        let target_set: std::collections::HashSet<&str> = targets.iter().copied().collect();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();

        queue.push_back(start.to_string());
        visited.insert(start.to_string());

        while let Some(current) = queue.pop_front() {
            if target_set.contains(current.as_str()) {
                return true;
            }
            if let Some(groups) = memberof_adj.get(&current) {
                for group in groups {
                    if !visited.contains(group) {
                        visited.insert(group.clone());
                        queue.push_back(group.clone());
                    }
                }
            }
        }
        false
    }

    // Choke points: uses default DatabaseBackend::get_choke_points() which loads
    // all nodes/edges once and runs Brandes' algorithm in-memory via algorithms.rs.
    // The previous CrustDB-specific override ran per-edge Cypher queries to resolve
    // node metadata, causing O(E) query overhead.
}

// ============================================================================
// DatabaseBackend Trait Implementation
// ============================================================================

impl DatabaseBackend for CrustDatabase {
    fn name(&self) -> &'static str {
        "CrustDB"
    }

    fn supports_language(&self, lang: QueryLanguage) -> bool {
        matches!(lang, QueryLanguage::Cypher)
    }

    fn default_language(&self) -> QueryLanguage {
        QueryLanguage::Cypher
    }

    fn ping(&self) -> Result<()> {
        self.run_custom_query("RETURN 1")?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        CrustDatabase::clear(self)
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        CrustDatabase::insert_node(self, node)
    }

    fn insert_edge(&self, relationship: DbEdge) -> Result<()> {
        CrustDatabase::insert_edge(self, relationship)
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        CrustDatabase::insert_nodes(self, nodes)
    }

    fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize> {
        CrustDatabase::insert_edges(self, relationships)
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        CrustDatabase::get_stats(self)
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        CrustDatabase::get_detailed_stats(self)
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        CrustDatabase::get_security_insights(self)
    }

    // get_choke_points: uses default trait implementation (algorithms.rs)
    // which loads all nodes/edges once and runs Brandes' algorithm in-memory.

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        CrustDatabase::get_all_nodes(self)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        CrustDatabase::get_all_edges(self)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        CrustDatabase::get_nodes_by_ids(self, ids)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        CrustDatabase::get_edges_between(self, node_ids)
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        CrustDatabase::get_edge_types(self)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        CrustDatabase::get_node_types(self)
    }

    fn search_nodes(&self, query: &str, limit: usize) -> Result<Vec<DbNode>> {
        CrustDatabase::search_nodes(self, query, limit)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        CrustDatabase::resolve_node_identifier(self, identifier)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        CrustDatabase::get_node_connections(self, node_id, direction)
    }

    fn get_node_relationship_counts(
        &self,
        node_id: &str,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        // Get all relationships for this node efficiently
        let relationships = CrustDatabase::get_node_edges(self, node_id)?;

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

        // Count unique nodes, not relationships
        // e.g., if node A has 3 relationships from node B, count as 1 incoming node
        let mut incoming_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut outgoing_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut admin_to_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut member_of_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut member_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for relationship in &relationships {
            if relationship.target == node_id {
                incoming_nodes.insert(&relationship.source);
                if relationship.rel_type == "MemberOf" {
                    member_nodes.insert(&relationship.source);
                }
            }
            if relationship.source == node_id {
                outgoing_nodes.insert(&relationship.target);
                if relationship.rel_type == "MemberOf" {
                    member_of_nodes.insert(&relationship.target);
                }
                if admin_types.contains(relationship.rel_type.as_str()) {
                    admin_to_nodes.insert(&relationship.target);
                }
            }
        }

        Ok((
            incoming_nodes.len(),
            outgoing_nodes.len(),
            admin_to_nodes.len(),
            member_of_nodes.len(),
            member_nodes.len(),
        ))
    }

    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        CrustDatabase::find_membership_by_sid_suffix(self, node_id, sid_suffix)
    }

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        CrustDatabase::shortest_path(self, from, to)
    }

    fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        CrustDatabase::find_paths_to_domain_admins(self, exclude_edge_types)
    }

    fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        CrustDatabase::run_custom_query(self, query)
    }

    fn get_cache_stats(&self) -> Result<Option<(usize, usize)>> {
        let stats = self.db.cache_stats()?;
        Ok(Some((stats.entry_count, stats.total_size_bytes)))
    }

    fn clear_cache(&self) -> Result<bool> {
        self.db.clear_cache()?;
        Ok(true)
    }

    fn get_database_size(&self) -> Result<Option<usize>> {
        let size = self.db.database_size()?;
        Ok(Some(size))
    }
}
