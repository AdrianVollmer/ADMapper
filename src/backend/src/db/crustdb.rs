//! CrustDB-backed graph database for storing AD graph data.
//!
//! Uses Cypher queries for graph operations via the embedded crustdb engine.

use crustdb::Database;
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{DbEdge, DbError, DbNode, DetailedStats, ReachabilityInsight, Result, SecurityInsights};

/// A graph database backed by CrustDB.
#[derive(Clone)]
pub struct CrustDatabase {
    db: Arc<Database>,
}

impl CrustDatabase {
    /// Create or open a database at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy();
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
        // Delete all edges first
        let _ = self.execute("MATCH ()-[r]->() DELETE r");
        // Delete all nodes
        let _ = self.execute("MATCH (n) DELETE n");
        debug!("Database cleared");
        Ok(())
    }

    /// Insert a batch of nodes.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        for node in nodes {
            let props_str = serde_json::to_string(&node.properties)?;
            let label = node.label.replace('\'', "''");
            let props_escaped = props_str.replace('\'', "''");
            let object_id = node.id.replace('\'', "''");
            let node_type = node.node_type.replace('\'', "''");

            let query = format!(
                "CREATE (n:{} {{object_id: '{}', label: '{}', node_type: '{}', properties: '{}'}})",
                node_type, object_id, label, node_type, props_escaped
            );

            if let Err(e) = self.execute(&query) {
                debug!("Failed to create node {}: {}", object_id, e);
            }
        }

        Ok(nodes.len())
    }

    /// Insert a batch of edges.
    pub fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        for edge in edges {
            let props_str = serde_json::to_string(&edge.properties)?;
            let source = edge.source.replace('\'', "''");
            let target = edge.target.replace('\'', "''");
            let edge_type = edge.edge_type.replace('\'', "''");
            let props_escaped = props_str.replace('\'', "''");

            // Match nodes by object_id property and create edge
            let query = format!(
                "MATCH (a {{object_id: '{}'}}), (b {{object_id: '{}'}}) \
                 CREATE (a)-[:{}  {{properties: '{}'}}]->(b)",
                source, target, edge_type, props_escaped
            );

            if let Err(e) = self.execute(&query) {
                debug!("Failed to create edge {} -> {}: {}", source, target, e);
            }
        }

        Ok(edges.len())
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
                crustdb::ResultValue::Property(crustdb::graph::PropertyValue::Integer(n)) => {
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
            self.execute(&format!(
                "MATCH (n:{}) RETURN count(n)",
                node_type
            ))
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
        let result = self.execute(
            "MATCH (n) RETURN n.object_id, n.label, n.node_type, n.properties",
        )?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            let id = self.get_string_value(&row.values, "n.object_id");
            let label = self.get_string_value(&row.values, "n.label");
            let node_type = self.get_string_value(&row.values, "n.node_type");
            let props_str = self.get_string_value(&row.values, "n.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            nodes.push(DbNode {
                id,
                label,
                node_type,
                properties,
            });
        }

        Ok(nodes)
    }

    /// Get all edges.
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let result = self.execute(
            "MATCH (a)-[r]->(b) RETURN a.object_id, b.object_id, type(r), r.properties",
        )?;

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
                crustdb::ResultValue::Property(crustdb::graph::PropertyValue::String(s)) => {
                    Some(s.clone())
                }
                crustdb::ResultValue::Property(crustdb::graph::PropertyValue::Integer(n)) => {
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
                if let crustdb::ResultValue::Property(crustdb::graph::PropertyValue::String(s)) =
                    value
                {
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
                if let crustdb::ResultValue::Property(crustdb::graph::PropertyValue::String(s)) =
                    value
                {
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
        let query = format!(
            "MATCH (n) WHERE n.label CONTAINS '{}' OR n.object_id CONTAINS '{}' \
             RETURN n.object_id, n.label, n.node_type, n.properties LIMIT {}",
            query_escaped, query_escaped, limit
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            let id = self.get_string_value(&row.values, "n.object_id");
            let label = self.get_string_value(&row.values, "n.label");
            let node_type = self.get_string_value(&row.values, "n.node_type");
            let props_str = self.get_string_value(&row.values, "n.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            nodes.push(DbNode {
                id,
                label,
                node_type,
                properties,
            });
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
                return Ok(Some(self.get_string_value(&result.rows[0].values, "n.object_id")));
            }
        }

        // Try label match
        let query = format!(
            "MATCH (n) WHERE n.label = '{}' RETURN n.object_id LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(self.get_string_value(&result.rows[0].values, "n.object_id")));
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

        let query = format!(
            "MATCH (n) WHERE n.object_id IN [{}] \
             RETURN n.object_id, n.label, n.node_type, n.properties",
            id_list.join(", ")
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            let id = self.get_string_value(&row.values, "n.object_id");
            let label = self.get_string_value(&row.values, "n.label");
            let node_type = self.get_string_value(&row.values, "n.node_type");
            let props_str = self.get_string_value(&row.values, "n.properties");

            let properties = serde_json::from_str(&props_str).unwrap_or(JsonValue::Null);
            nodes.push(DbNode {
                id,
                label,
                node_type,
                properties,
            });
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
            });
        }

        Ok(edges)
    }

    /// Get node connections in a direction.
    pub fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        let all_edges = self.get_all_edges()?;

        const ADMIN_EDGE_TYPES: &[&str] = &[
            "AdminTo",
            "GenericAll",
            "GenericWrite",
            "Owns",
            "WriteDacl",
            "WriteOwner",
            "AllExtendedRights",
            "ForceChangePassword",
            "AddMember",
        ];

        let filtered_edges: Vec<DbEdge> = all_edges
            .into_iter()
            .filter(|edge| match direction {
                "incoming" => edge.target == node_id,
                "outgoing" => edge.source == node_id,
                "admin" => {
                    edge.source == node_id && ADMIN_EDGE_TYPES.contains(&edge.edge_type.as_str())
                }
                "memberof" => edge.source == node_id && edge.edge_type == "MemberOf",
                "members" => edge.target == node_id && edge.edge_type == "MemberOf",
                _ => edge.source == node_id || edge.target == node_id,
            })
            .collect();

        let mut node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        node_ids.insert(node_id.to_string());
        for edge in &filtered_edges {
            node_ids.insert(edge.source.clone());
            node_ids.insert(edge.target.clone());
        }

        let node_id_vec: Vec<String> = node_ids.into_iter().collect();
        let nodes = self.get_nodes_by_ids(&node_id_vec)?;

        Ok((nodes, filtered_edges))
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
                            .map(|v| match v {
                                crustdb::ResultValue::Property(pv) => match pv {
                                    crustdb::graph::PropertyValue::String(s) => {
                                        JsonValue::String(s.clone())
                                    }
                                    crustdb::graph::PropertyValue::Integer(n) => {
                                        JsonValue::Number((*n).into())
                                    }
                                    crustdb::graph::PropertyValue::Float(f) => serde_json::Number::from_f64(*f)
                                        .map(JsonValue::Number)
                                        .unwrap_or(JsonValue::Null),
                                    crustdb::graph::PropertyValue::Bool(b) => JsonValue::Bool(*b),
                                    crustdb::graph::PropertyValue::Null => JsonValue::Null,
                                    _ => JsonValue::String(format!("{:?}", pv)),
                                },
                                _ => JsonValue::String(format!("{:?}", v)),
                            })
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

    /// Get security insights.
    pub fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        let nodes = self.get_all_nodes()?;
        let edges = self.get_all_edges()?;

        let total_users = nodes.iter().filter(|n| n.node_type == "User").count();

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with("-512"))
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
        let real_da_count = real_das.len();

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

        // Simplified reachability (placeholder)
        let reachability = vec![
            ReachabilityInsight {
                principal_name: "Everyone".to_string(),
                principal_id: None,
                reachable_count: 0,
            },
            ReachabilityInsight {
                principal_name: "Authenticated Users".to_string(),
                principal_id: None,
                reachable_count: 0,
            },
        ];

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

    // Query history methods (simplified in-memory for now)
    pub fn add_query_history(
        &self,
        _id: &str,
        _name: &str,
        _query: &str,
        _timestamp: i64,
        _result_count: Option<i64>,
    ) -> Result<()> {
        // TODO: Implement query history storage
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn get_query_history(
        &self,
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<(String, String, String, i64, Option<i64>)>, usize)> {
        // TODO: Implement query history retrieval
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
    ) -> Result<()> {
        CrustDatabase::add_query_history(self, id, name, query, timestamp, result_count)
    }

    fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<(String, String, String, i64, Option<i64>)>, usize)> {
        CrustDatabase::get_query_history(self, limit, offset)
    }

    fn delete_query_history(&self, id: &str) -> Result<()> {
        CrustDatabase::delete_query_history(self, id)
    }

    fn clear_query_history(&self) -> Result<()> {
        CrustDatabase::clear_query_history(self)
    }
}
