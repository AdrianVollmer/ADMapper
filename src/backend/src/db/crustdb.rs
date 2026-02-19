//! CrustDB-backed graph database for storing AD graph data.
//!
//! Uses Cypher queries for graph operations via the embedded crustdb engine.

use crustdb::Database;
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{
    DbEdge, DbError, DbNode, DetailedStats, QueryHistoryRow, ReachabilityInsight, Result,
    SecurityInsights, DOMAIN_ADMIN_SID_SUFFIX, WELL_KNOWN_PRINCIPALS,
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
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!(path = %path_str, "Opening CrustDB");

        let db = Database::open(&path_str).map_err(|e| DbError::Database(e.to_string()))?;

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        info!("CrustDB initialized successfully");
        Ok(instance)
    }

    /// Create an in-memory database (for testing).
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        debug!("Creating in-memory CrustDB");
        let db = Database::in_memory().map_err(|e| DbError::Database(e.to_string()))?;

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Initialize the schema by creating indexes and base structures.
    fn init_schema(&self) -> Result<()> {
        debug!("Initializing CrustDB schema");
        // CrustDB auto-creates nodes/edges on first use
        // Create any necessary indexes here if supported
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

    /// Insert a batch of nodes using efficient batch insert.
    ///
    /// This uses CrustDB's native batch insert which wraps all inserts
    /// in a single transaction with prepared statements.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        // Convert DbNodes to the format expected by CrustDB batch insert
        let batch: Vec<(Vec<String>, serde_json::Value)> = nodes
            .iter()
            .map(|node| {
                let labels = vec![node.node_type.clone()];
                // Flatten BloodHound properties into top-level fields
                let props = Self::flatten_node_properties(node);
                (labels, props)
            })
            .collect();

        match self.db.insert_nodes_batch(&batch) {
            Ok(ids) => {
                debug!("Batch inserted {} nodes", ids.len());
                Ok(ids.len())
            }
            Err(e) => {
                debug!(
                    "Batch insert failed, falling back to individual inserts: {}",
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
            let node_type = node.node_type.replace('\'', "''");

            let query = format!("CREATE (n:{} {})", node_type, props_str);

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
        props.insert("object_id".to_string(), serde_json::json!(node.id));
        props.insert("label".to_string(), serde_json::json!(node.label));
        props.insert("node_type".to_string(), serde_json::json!(node.node_type));

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
                if key != "object_id" && key != "label" && key != "node_type" {
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

    /// Insert a batch of edges using efficient batch insert.
    ///
    /// This builds an index of object_id -> node_id for efficient lookups,
    /// then uses CrustDB's native batch insert.
    pub fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        // Build index of object_id -> node_id for efficient lookups
        let node_index = match self.db.build_property_index("object_id") {
            Ok(index) => index,
            Err(e) => {
                debug!("Failed to build property index, falling back: {}", e);
                return self.insert_edges_fallback(edges);
            }
        };

        // Convert edges to the format expected by CrustDB batch insert
        let mut batch: Vec<(i64, i64, String, serde_json::Value)> = Vec::with_capacity(edges.len());
        let mut skipped = 0;

        // Collect unique placeholder nodes to create (deduplicated)
        let mut placeholder_set: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for edge in edges {
            let source_id = node_index.get(&edge.source);
            let target_id = node_index.get(&edge.target);

            // Create placeholder for missing source
            if source_id.is_none() {
                let node_type = edge
                    .source_type
                    .clone()
                    .unwrap_or_else(|| "Base".to_string());
                placeholder_set.insert((edge.source.clone(), node_type));
            }
            // Create placeholder for missing target
            if target_id.is_none() {
                let node_type = edge
                    .target_type
                    .clone()
                    .unwrap_or_else(|| "Base".to_string());
                placeholder_set.insert((edge.target.clone(), node_type));
            }
        }

        // Insert placeholder nodes using batch insert
        let node_index = if !placeholder_set.is_empty() {
            debug!("Creating {} placeholder nodes", placeholder_set.len());

            // Convert to batch format: (labels, properties)
            let placeholder_batch: Vec<(Vec<String>, serde_json::Value)> = placeholder_set
                .iter()
                .map(|(object_id, node_type)| {
                    let labels = vec![node_type.clone()];
                    let props = serde_json::json!({
                        "object_id": object_id,
                        "placeholder": true,
                        "node_type": node_type,
                    });
                    (labels, props)
                })
                .collect();

            match self.db.insert_nodes_batch(&placeholder_batch) {
                Ok(ids) => {
                    debug!("Batch inserted {} placeholder nodes", ids.len());
                }
                Err(e) => {
                    debug!("Batch placeholder insert failed: {}", e);
                }
            }

            // Rebuild index after creating placeholders
            self.db
                .build_property_index("object_id")
                .unwrap_or_default()
        } else {
            node_index
        };

        for edge in edges {
            let source_id = node_index.get(&edge.source);
            let target_id = node_index.get(&edge.target);

            match (source_id, target_id) {
                (Some(&src), Some(&tgt)) => {
                    let props = serde_json::json!({
                        "properties": serde_json::to_string(&edge.properties).unwrap_or_default()
                    });
                    batch.push((src, tgt, edge.edge_type.clone(), props));
                }
                _ => {
                    debug!(
                        "Skipping edge {} -> {}: source or target not found",
                        edge.source, edge.target
                    );
                    skipped += 1;
                }
            }
        }

        if batch.is_empty() {
            debug!("No valid edges to insert (skipped {})", skipped);
            return Ok(0);
        }

        match self.db.insert_edges_batch(&batch) {
            Ok(ids) => {
                debug!("Batch inserted {} edges (skipped {})", ids.len(), skipped);
                Ok(ids.len())
            }
            Err(e) => {
                debug!("Batch edge insert failed, falling back: {}", e);
                self.insert_edges_fallback(edges)
            }
        }
    }

    /// Fallback method for individual edge inserts (used if batch fails).
    fn insert_edges_fallback(&self, edges: &[DbEdge]) -> Result<usize> {
        let mut count = 0;
        for edge in edges {
            let props_str = serde_json::to_string(&edge.properties)?;
            let source = edge.source.replace('\'', "''");
            let target = edge.target.replace('\'', "''");
            let edge_type = edge.edge_type.replace('\'', "''");
            let props_escaped = props_str.replace('\'', "''");

            let query = format!(
                "MATCH (a {{object_id: '{}'}}), (b {{object_id: '{}'}}) \
                 CREATE (a)-[:{}  {{properties: '{}'}}]->(b)",
                source, target, edge_type, props_escaped
            );

            if self.execute(&query).is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Insert a single node.
    pub fn insert_node(&self, node: DbNode) -> Result<()> {
        self.insert_nodes(&[node])?;
        Ok(())
    }

    /// Insert a single edge.
    pub fn insert_edge(&self, edge: DbEdge) -> Result<()> {
        self.insert_edges(&[edge])?;
        Ok(())
    }

    /// Get node and edge counts.
    pub fn get_stats(&self) -> Result<(usize, usize)> {
        let node_result = self.execute("MATCH (n) RETURN count(n)")?;
        let node_count = self.extract_count(&node_result);

        let edge_result = self.execute("MATCH ()-[r]->() RETURN count(r)")?;
        let edge_count = self.extract_count(&edge_result);

        Ok((node_count, edge_count))
    }

    /// Extract count from a query result.
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
    pub fn get_detailed_stats(&self) -> Result<DetailedStats> {
        let (total_nodes, total_edges) = self.get_stats()?;

        // Count by node type
        let count_type = |node_type: &str| -> usize {
            self.execute(&format!("MATCH (n:{}) RETURN count(n)", node_type))
                .map(|r| self.extract_count(&r))
                .unwrap_or(0)
        };

        Ok(DetailedStats {
            total_nodes,
            total_edges,
            users: count_type("User"),
            computers: count_type("Computer"),
            groups: count_type("Group"),
            domains: count_type("Domain"),
            ous: count_type("OU"),
            gpos: count_type("GPO"),
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
                let object_id = properties
                    .get("object_id")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let label = properties
                    .get("label")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| object_id.clone());

                let node_type = properties
                    .get("node_type")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .or_else(|| labels.first().cloned())
                    .unwrap_or_else(|| "Unknown".to_string());

                // Convert all properties to JSON
                let props_json = Self::properties_to_json(properties);

                Some(DbNode {
                    id: object_id,
                    label,
                    node_type,
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

    /// Get all edges.
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let result = self
            .execute("MATCH (a)-[r]->(b) RETURN a.object_id, b.object_id, type(r), r.properties")?;

        let mut edges = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.object_id");
            let target = self.get_string_value(&row.values, "b.object_id");
            let edge_type = self.get_string_value(&row.values, "type(r)");
            let props_str = self.get_string_value(&row.values, "r.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            edges.push(DbEdge {
                source,
                target,
                edge_type,
                properties,
                ..Default::default()
            });
        }

        Ok(edges)
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

    /// Get all distinct edge types.
    pub fn get_edge_types(&self) -> Result<Vec<String>> {
        let result = self.execute("MATCH ()-[r]->() RETURN DISTINCT type(r)")?;

        let mut types = Vec::new();
        for row in &result.rows {
            for value in row.values.values() {
                if let crustdb::ResultValue::Property(crustdb::PropertyValue::String(s)) = value {
                    types.push(s.clone());
                }
            }
        }

        Ok(types)
    }

    /// Get all distinct node types.
    pub fn get_node_types(&self) -> Result<Vec<String>> {
        let result = self.execute("MATCH (n) RETURN DISTINCT n.node_type")?;

        let mut types = Vec::new();
        for row in &result.rows {
            for value in row.values.values() {
                if let crustdb::ResultValue::Property(crustdb::PropertyValue::String(s)) = value {
                    if !s.is_empty() {
                        types.push(s.clone());
                    }
                }
            }
        }

        Ok(types)
    }

    /// Search nodes by label (case-insensitive substring match).
    pub fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let query_escaped = search_query.replace('\'', "''").to_lowercase();

        // CrustDB supports CONTAINS for string matching
        // Use toLower() for case-insensitive search
        let query = format!(
            "MATCH (n) WHERE toLower(n.label) CONTAINS '{}' OR toLower(n.object_id) CONTAINS '{}' \
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

        // Try exact object_id match
        let query = format!(
            "MATCH (n {{object_id: '{}'}}) RETURN n.object_id LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.object_id"),
                ));
            }
        }

        // Try label match
        let query = format!(
            "MATCH (n) WHERE n.label = '{}' RETURN n.object_id LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.object_id"),
                ));
            }
        }

        Ok(None)
    }

    /// Find shortest path between two nodes using BFS.
    #[allow(clippy::type_complexity)]
    pub fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        // Use BFS over edges
        let edges = self.get_all_edges()?;

        let mut adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for edge in &edges {
            adj.entry(edge.source.clone())
                .or_default()
                .push((edge.target.clone(), edge.edge_type.clone()));
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
                while let Some((prev, edge_type)) = parent.get(&node) {
                    path.push((prev.clone(), Some(edge_type.clone())));
                    node = prev.clone();
                }
                path.reverse();
                return Ok(Some(path));
            }

            if let Some(neighbors) = adj.get(&current) {
                for (neighbor, edge_type) in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(neighbor.clone());
                        parent.insert(neighbor.clone(), (current.clone(), edge_type.clone()));
                        queue.push_back(neighbor.clone());
                    }
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
        let edges = self.get_all_edges()?;

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with("-512"))
            .map(|n| n.id.as_str())
            .collect();

        if da_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Build adjacency list, filtering excluded edge types
        let exclude_set: std::collections::HashSet<&str> =
            exclude_edge_types.iter().map(|s| s.as_str()).collect();

        let mut adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for edge in &edges {
            if !exclude_set.contains(edge.edge_type.as_str()) {
                adj.entry(edge.source.clone())
                    .or_default()
                    .push((edge.target.clone(), edge.edge_type.clone()));
            }
        }

        // BFS from each user to find paths to DA
        let users: Vec<&DbNode> = nodes.iter().filter(|n| n.node_type == "User").collect();

        let mut results = Vec::new();
        for user in users {
            if let Some(hops) = self.bfs_to_targets(&user.id, &da_groups, &adj) {
                results.push((
                    user.id.clone(),
                    user.node_type.clone(),
                    user.label.clone(),
                    hops,
                ));
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
            "MATCH (n) WHERE n.object_id IN [{}] RETURN n",
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

    /// Get edges between nodes.
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
             WHERE a.object_id IN [{}] AND b.object_id IN [{}] \
             RETURN a.object_id, b.object_id, type(r), r.properties",
            id_set, id_set
        );

        let result = self.execute(&query)?;

        let mut edges = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.object_id");
            let target = self.get_string_value(&row.values, "b.object_id");
            let edge_type = self.get_string_value(&row.values, "type(r)");
            let props_str = self.get_string_value(&row.values, "r.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            edges.push(DbEdge {
                source,
                target,
                edge_type,
                properties,
                ..Default::default()
            });
        }

        Ok(edges)
    }

    /// Get node connections in a direction.
    /// Uses indexed Cypher queries instead of loading all edges.
    pub fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        // Escape single quotes in node_id for Cypher query
        let escaped_id = node_id.replace('\'', "\\'");

        // Build targeted Cypher query based on direction
        let query = match direction {
            "incoming" => format!(
                "MATCH (a)-[r]->(b {{object_id: '{}'}}) \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
            "outgoing" => format!(
                "MATCH (a {{object_id: '{}'}})-[r]->(b) \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
            "admin" => format!(
                "MATCH (a {{object_id: '{}'}})-[r]->(b) \
                 WHERE type(r) = 'AdminTo' OR type(r) = 'GenericAll' OR type(r) = 'GenericWrite' \
                 OR type(r) = 'Owns' OR type(r) = 'WriteDacl' OR type(r) = 'WriteOwner' \
                 OR type(r) = 'AllExtendedRights' OR type(r) = 'ForceChangePassword' \
                 OR type(r) = 'AddMember' \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
            "memberof" => format!(
                "MATCH (a {{object_id: '{}'}})-[r:MemberOf]->(b) \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
            "members" => format!(
                "MATCH (a)-[r:MemberOf]->(b {{object_id: '{}'}}) \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
            _ => format!(
                "MATCH (a)-[r]-(b {{object_id: '{}'}}) \
                 RETURN a.object_id, b.object_id, type(r), a, b",
                escaped_id
            ),
        };

        let result = self.execute(&query)?;

        let mut edges = Vec::new();
        let mut nodes_map: std::collections::HashMap<String, DbNode> =
            std::collections::HashMap::new();

        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.object_id");
            let target = self.get_string_value(&row.values, "b.object_id");
            let edge_type = self.get_string_value(&row.values, "type(r)");

            edges.push(DbEdge {
                source: source.clone(),
                target: target.clone(),
                edge_type,
                properties: JsonValue::Null,
                ..Default::default()
            });

            // Extract node info from the result
            if let Some(crustdb::ResultValue::Node {
                labels, properties, ..
            }) = row.values.get("a")
            {
                if !nodes_map.contains_key(&source) {
                    let node_type = labels.first().cloned().unwrap_or_default();
                    let props = Self::props_to_json(properties);
                    let label = props
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&source)
                        .to_string();
                    nodes_map.insert(
                        source.clone(),
                        DbNode {
                            id: source.clone(),
                            label,
                            node_type,
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
                    let node_type = labels.first().cloned().unwrap_or_default();
                    let props = Self::props_to_json(properties);
                    let label = props
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&target)
                        .to_string();
                    nodes_map.insert(
                        target.clone(),
                        DbNode {
                            id: target.clone(),
                            label,
                            node_type,
                            properties: props,
                        },
                    );
                }
            }
        }

        let nodes: Vec<DbNode> = nodes_map.into_values().collect();
        Ok((nodes, edges))
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

    /// Get all edges for a node (both incoming and outgoing) with edge types.
    /// Used for efficient counting by the backend layer.
    /// Uses direct SQL for efficiency instead of Cypher queries.
    pub fn get_node_edges(&self, node_id: &str) -> Result<Vec<DbEdge>> {
        let raw_edges = self
            .db
            .get_node_edges_by_object_id(node_id)
            .map_err(|e| DbError::Database(e.to_string()))?;

        let edges = raw_edges
            .into_iter()
            .map(|(source, target, edge_type)| DbEdge {
                source,
                target,
                edge_type,
                properties: JsonValue::Null,
                ..Default::default()
            })
            .collect();

        Ok(edges)
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
            "MATCH (n {{object_id: '{}'}})-[:MemberOf*1..20]->(g) \
             WHERE g.object_id ENDS WITH '{}' \
             RETURN g.object_id LIMIT 1",
            id_escaped, suffix_escaped
        );

        let result = self.execute(&query)?;

        if let Some(first_row) = result.rows.first() {
            if let Some(value) = first_row.values.get("g.object_id") {
                if let crustdb::ResultValue::Property(crustdb::PropertyValue::String(s)) = value {
                    return Ok(Some(s.clone()));
                }
            }
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
                            .map(|v| Self::result_value_to_json(v))
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
                // Get object_id from properties if available
                let object_id = properties
                    .get("object_id")
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
                    "object_id": object_id,
                    "labels": labels,
                    "properties": props
                })
            }
            crustdb::ResultValue::Edge {
                id,
                source,
                target,
                edge_type,
                properties,
            } => {
                let props: serde_json::Map<String, JsonValue> = properties
                    .iter()
                    .map(|(k, pv)| (k.clone(), Self::property_value_to_json(pv)))
                    .collect();
                serde_json::json!({
                    "_type": "edge",
                    "id": id,
                    "source": source,
                    "target": target,
                    "edge_type": edge_type,
                    "properties": props
                })
            }
            crustdb::ResultValue::Path { nodes, edges } => {
                serde_json::json!({
                    "_type": "path",
                    "nodes": nodes,
                    "edges": edges
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
        let edges = self.get_all_edges()?;

        let total_users = nodes.iter().filter(|n| n.node_type == "User").count();

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with(DOMAIN_ADMIN_SID_SUFFIX))
            .map(|n| n.id.as_str())
            .collect();

        // Build MemberOf adjacency
        let mut memberof_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for edge in &edges {
            if edge.edge_type == "MemberOf" {
                memberof_adj
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
        }

        // Find real DAs (users directly or transitively in DA group via MemberOf)
        let mut real_das = Vec::new();
        for user in nodes.iter().filter(|n| n.node_type == "User") {
            if self.is_transitive_member(&user.id, &da_groups, &memberof_adj) {
                real_das.push((user.id.clone(), user.label.clone()));
            }
        }

        // Build full adjacency for effective DA paths
        let mut full_adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for edge in &edges {
            full_adj
                .entry(edge.source.clone())
                .or_default()
                .push((edge.target.clone(), edge.edge_type.clone()));
        }

        // Find effective DAs (any path to DA group)
        let mut effective_das = Vec::new();
        for user in nodes.iter().filter(|n| n.node_type == "User") {
            if let Some(hops) = self.bfs_to_targets(&user.id, &da_groups, &full_adj) {
                effective_das.push((user.id.clone(), user.label.clone(), hops));
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

    // Query history methods (stub implementation - CrustDB doesn't persist history)
    pub fn add_query_history(
        &self,
        _id: &str,
        _name: &str,
        _query: &str,
        _timestamp: i64,
        _result_count: Option<i64>,
        _status: &str,
        _started_at: i64,
        _duration_ms: Option<u64>,
        _error: Option<&str>,
    ) -> Result<()> {
        // CrustDB doesn't persist query history yet
        Ok(())
    }

    pub fn update_query_status(
        &self,
        _id: &str,
        _status: &str,
        _duration_ms: Option<u64>,
        _result_count: Option<i64>,
        _error: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn get_query_history(
        &self,
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        // Return empty list - CrustDB doesn't persist history
        Ok((Vec::new(), 0))
    }

    pub fn delete_query_history(&self, _id: &str) -> Result<()> {
        Ok(())
    }

    pub fn clear_query_history(&self) -> Result<()> {
        Ok(())
    }
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

    fn clear(&self) -> Result<()> {
        CrustDatabase::clear(self)
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        CrustDatabase::insert_node(self, node)
    }

    fn insert_edge(&self, edge: DbEdge) -> Result<()> {
        CrustDatabase::insert_edge(self, edge)
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        CrustDatabase::insert_nodes(self, nodes)
    }

    fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        CrustDatabase::insert_edges(self, edges)
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

    fn get_node_edge_counts(&self, node_id: &str) -> Result<(usize, usize, usize, usize, usize)> {
        // Get all edges for this node efficiently
        let edges = CrustDatabase::get_node_edges(self, node_id)?;

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

        for edge in &edges {
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
    ) -> Result<()> {
        CrustDatabase::add_query_history(
            self,
            id,
            name,
            query,
            timestamp,
            result_count,
            status,
            started_at,
            duration_ms,
            error,
        )
    }

    fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        CrustDatabase::update_query_status(self, id, status, duration_ms, result_count, error)
    }

    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        CrustDatabase::get_query_history(self, limit, offset)
    }

    fn delete_query_history(&self, id: &str) -> Result<()> {
        CrustDatabase::delete_query_history(self, id)
    }

    fn clear_query_history(&self) -> Result<()> {
        CrustDatabase::clear_query_history(self)
    }
}
