//! KuzuDB-backed graph database for storing AD graph data.
//!
//! Uses Cypher queries for graph operations.

use kuzu::{Connection, Database, SystemConfig};
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, trace};

use super::backend::{DatabaseBackend, QueryLanguage};
use super::types::{DbEdge, DbNode, DetailedStats, ReachabilityInsight, Result, SecurityInsights};

/// A graph database backed by KuzuDB.
#[derive(Clone)]
pub struct KuzuDatabase {
    db: Arc<Database>,
}

impl KuzuDatabase {
    /// Create or open a database at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy();
        info!(path = %path_str, "Opening KuzuDB");

        let db = Database::new(path_str.as_ref(), SystemConfig::default())?;

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        info!("KuzuDB initialized successfully");
        Ok(instance)
    }

    /// Create an in-memory database (for testing).
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        debug!("Creating in-memory KuzuDB");
        // KuzuDB uses empty string for in-memory
        let db = Database::new("", SystemConfig::default())?;

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Create a new connection to the database.
    fn conn(&self) -> Result<Connection<'_>> {
        Ok(Connection::new(&self.db)?)
    }

    /// AD node types supported by the schema
    const NODE_TYPES: &'static [&'static str] = &[
        "User",
        "Group",
        "Computer",
        "Domain",
        "OU",
        "GPO",
        "Container",
        "CertTemplate",
        "EnterpriseCA",
        "AIACA",
        "RootCA",
        "NTAuthStore",
        "IssuancePolicy",
        "Unknown", // Fallback for unrecognized types
    ];

    /// Initialize the schema if tables don't exist.
    fn init_schema(&self) -> Result<()> {
        debug!("Initializing KuzuDB schema");
        let conn = self.conn()?;

        // Create node tables for each AD object type
        // Schema includes common BloodHound properties as columns for efficient querying
        for node_type in Self::NODE_TYPES {
            let create_table = format!(
                r#"CREATE NODE TABLE IF NOT EXISTS {}(
                    object_id STRING PRIMARY KEY,
                    label STRING,
                    name STRING,
                    domain STRING,
                    distinguishedname STRING,
                    enabled BOOL,
                    admincount BOOL,
                    samaccountname STRING,
                    description STRING,
                    operatingsystem STRING,
                    properties STRING
                )"#,
                node_type
            );
            match conn.query(&create_table) {
                Ok(_) => debug!("Created {} table", node_type),
                Err(e) => trace!("{} table might already exist: {}", node_type, e),
            }
        }

        // Create REL TABLE GROUP for edges between all node types
        // This allows any node type to connect to any other
        let mut from_to_pairs = Vec::new();
        for from_type in Self::NODE_TYPES {
            for to_type in Self::NODE_TYPES {
                from_to_pairs.push(format!("FROM {} TO {}", from_type, to_type));
            }
        }

        let create_edges = format!(
            r#"CREATE REL TABLE GROUP IF NOT EXISTS Edge(
                {},
                edge_type STRING,
                properties STRING
            )"#,
            from_to_pairs.join(", ")
        );

        match conn.query(&create_edges) {
            Ok(_) => debug!("Created Edge relationship table group"),
            Err(e) => trace!("Edge table might already exist: {}", e),
        }

        // Create QueryHistory table for storing query history
        let create_query_history = r#"
            CREATE NODE TABLE IF NOT EXISTS QueryHistory(
                id STRING PRIMARY KEY,
                name STRING,
                query STRING,
                timestamp INT64,
                result_count INT64,
                status STRING,
                started_at INT64,
                duration_ms INT64,
                error STRING
            )
        "#;

        match conn.query(create_query_history) {
            Ok(_) => debug!("Created QueryHistory table"),
            Err(e) => trace!("QueryHistory table might already exist: {}", e),
        }

        Ok(())
    }

    /// Map a node type string to a valid table name
    fn normalize_node_type(node_type: &str) -> &'static str {
        match node_type {
            "User" => "User",
            "Group" => "Group",
            "Computer" => "Computer",
            "Domain" => "Domain",
            "OU" => "OU",
            "GPO" => "GPO",
            "Container" => "Container",
            "CertTemplate" => "CertTemplate",
            "EnterpriseCA" => "EnterpriseCA",
            "AIACA" => "AIACA",
            "RootCA" => "RootCA",
            "NTAuthStore" => "NTAuthStore",
            "IssuancePolicy" => "IssuancePolicy",
            _ => "Unknown",
        }
    }

    /// Clear all data from the database.
    pub fn clear(&self) -> Result<()> {
        info!("Clearing all data from KuzuDB");
        let conn = self.conn()?;

        // Delete all edges first (due to foreign key constraints)
        conn.query("MATCH ()-[e:Edge]->() DELETE e")?;

        // Delete nodes from all tables
        for node_type in Self::NODE_TYPES {
            let query = format!("MATCH (n:{}) DELETE n", node_type);
            if let Err(e) = conn.query(&query) {
                trace!("Error clearing {} table: {}", node_type, e);
            }
        }

        debug!("Database cleared");
        Ok(())
    }

    /// Flatten BloodHound node properties into individual values for Cypher querying.
    fn extract_property_string(props: &JsonValue, key: &str) -> Option<String> {
        props
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    fn extract_property_bool(props: &JsonValue, key: &str) -> Option<bool> {
        props.get(key).and_then(|v| v.as_bool())
    }

    fn escape_string(s: &str) -> String {
        s.replace('\\', "\\\\").replace('\'', "''")
    }

    /// Insert a batch of nodes.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        let conn = self.conn()?;

        for node in nodes {
            let props_str = serde_json::to_string(&node.properties)?;
            let table_name = Self::normalize_node_type(&node.node_type);

            // Extract common properties for dedicated columns
            let object_id = Self::escape_string(&node.id);
            let label = Self::escape_string(&node.label);
            let name = Self::extract_property_string(&node.properties, "name")
                .map(|s| Self::escape_string(&s))
                .unwrap_or_else(|| label.clone());
            let domain = Self::extract_property_string(&node.properties, "domain")
                .map(|s| Self::escape_string(&s))
                .unwrap_or_default();
            let distinguishedname =
                Self::extract_property_string(&node.properties, "distinguishedname")
                    .map(|s| Self::escape_string(&s))
                    .unwrap_or_default();
            let enabled = Self::extract_property_bool(&node.properties, "enabled").unwrap_or(false);
            let admincount =
                Self::extract_property_bool(&node.properties, "admincount").unwrap_or(false);
            let samaccountname = Self::extract_property_string(&node.properties, "samaccountname")
                .map(|s| Self::escape_string(&s))
                .unwrap_or_default();
            let description = Self::extract_property_string(&node.properties, "description")
                .map(|s| Self::escape_string(&s))
                .unwrap_or_default();
            let operatingsystem =
                Self::extract_property_string(&node.properties, "operatingsystem")
                    .map(|s| Self::escape_string(&s))
                    .unwrap_or_default();
            let props_escaped = Self::escape_string(&props_str);

            let query = format!(
                "CREATE (n:{} {{object_id: '{}', label: '{}', name: '{}', domain: '{}', \
                 distinguishedname: '{}', enabled: {}, admincount: {}, samaccountname: '{}', \
                 description: '{}', operatingsystem: '{}', properties: '{}'}})",
                table_name,
                object_id,
                label,
                name,
                domain,
                distinguishedname,
                enabled,
                admincount,
                samaccountname,
                description,
                operatingsystem,
                props_escaped
            );

            if let Err(e) = conn.query(&query) {
                // Node might already exist, try merge instead
                trace!("Create failed, trying merge: {}", e);
                let merge_query = format!(
                    "MERGE (n:{} {{object_id: '{}'}}) \
                     SET n.label = '{}', n.name = '{}', n.domain = '{}', \
                     n.distinguishedname = '{}', n.enabled = {}, n.admincount = {}, \
                     n.samaccountname = '{}', n.description = '{}', n.operatingsystem = '{}', \
                     n.properties = '{}'",
                    table_name,
                    object_id,
                    label,
                    name,
                    domain,
                    distinguishedname,
                    enabled,
                    admincount,
                    samaccountname,
                    description,
                    operatingsystem,
                    props_escaped
                );
                conn.query(&merge_query)?;
            }
        }

        Ok(nodes.len())
    }

    /// Find which table contains a node with the given object_id
    fn find_node_type(&self, conn: &Connection<'_>, object_id: &str) -> Option<&'static str> {
        let escaped_id = object_id.replace('\'', "''");
        for node_type in Self::NODE_TYPES {
            let query = format!(
                "MATCH (n:{} {{object_id: '{}'}}) RETURN n.object_id LIMIT 1",
                node_type, escaped_id
            );
            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                if result_str.lines().skip(1).next().is_some() {
                    return Some(node_type);
                }
            }
        }
        None
    }

    /// Insert a batch of edges.
    /// Note: This requires source and target nodes to already exist.
    pub fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        let conn = self.conn()?;

        // Build a cache of node types for efficiency
        let mut node_type_cache: std::collections::HashMap<String, &'static str> =
            std::collections::HashMap::new();

        for edge in edges {
            let props_str = serde_json::to_string(&edge.properties)?;
            let source = edge.source.replace('\'', "''");
            let target = edge.target.replace('\'', "''");
            let edge_type = edge.edge_type.replace('\'', "''");
            let props_escaped = props_str.replace('\'', "''");

            // Look up source node type (with caching)
            let src_type = if let Some(t) = node_type_cache.get(&edge.source) {
                *t
            } else if let Some(t) = self.find_node_type(&conn, &edge.source) {
                node_type_cache.insert(edge.source.clone(), t);
                t
            } else {
                debug!("Source node not found: {}", edge.source);
                continue;
            };

            // Look up target node type (with caching)
            let tgt_type = if let Some(t) = node_type_cache.get(&edge.target) {
                *t
            } else if let Some(t) = self.find_node_type(&conn, &edge.target) {
                node_type_cache.insert(edge.target.clone(), t);
                t
            } else {
                debug!("Target node not found: {}", edge.target);
                continue;
            };

            let query = format!(
                "MATCH (a:{} {{object_id: '{}'}}), (b:{} {{object_id: '{}'}}) \
                 CREATE (a)-[:Edge {{edge_type: '{}', properties: '{}'}}]->(b)",
                src_type, source, tgt_type, target, edge_type, props_escaped
            );

            if let Err(e) = conn.query(&query) {
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
        let conn = self.conn()?;

        // Sum node counts across all node tables
        let mut node_count = 0;
        for node_type in Self::NODE_TYPES {
            let query = format!("MATCH (n:{}) RETURN count(n)", node_type);
            if let Ok(result) = conn.query(&query) {
                node_count += self.extract_count(&result);
            }
        }

        let edge_result = conn.query("MATCH ()-[e:Edge]->() RETURN count(e)")?;
        let edge_count = self.extract_count(&edge_result);

        Ok((node_count, edge_count))
    }

    /// Extract count from a query result.
    fn extract_count(&self, result: &kuzu::QueryResult) -> usize {
        // QueryResult can be converted to string and parsed
        let result_str = result.to_string();
        // Parse the count from the result - format is typically a table with one row
        result_str
            .lines()
            .skip(1) // Skip header
            .next()
            .and_then(|line| line.trim().parse::<usize>().ok())
            .unwrap_or(0)
    }

    /// Get detailed stats including counts by node type.
    pub fn get_detailed_stats(&self) -> Result<DetailedStats> {
        let (node_count, edge_count) = self.get_stats()?;
        let conn = self.conn()?;

        // Get counts by querying each node table directly
        let mut type_counts = std::collections::HashMap::new();
        for node_type in Self::NODE_TYPES {
            let query = format!("MATCH (n:{}) RETURN count(n)", node_type);
            if let Ok(result) = conn.query(&query) {
                let count = self.extract_count(&result);
                if count > 0 {
                    type_counts.insert(node_type.to_string(), count);
                }
            }
        }

        Ok(DetailedStats {
            total_nodes: node_count,
            total_edges: edge_count,
            users: type_counts.get("User").copied().unwrap_or(0),
            computers: type_counts.get("Computer").copied().unwrap_or(0),
            groups: type_counts.get("Group").copied().unwrap_or(0),
            domains: type_counts.get("Domain").copied().unwrap_or(0),
            ous: type_counts.get("OU").copied().unwrap_or(0),
            gpos: type_counts.get("GPO").copied().unwrap_or(0),
        })
    }

    /// Get all nodes from all tables.
    pub fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let conn = self.conn()?;
        let mut nodes = Vec::new();

        // Query each node table
        for node_type in Self::NODE_TYPES {
            let query = format!(
                "MATCH (n:{}) RETURN n.object_id, n.label, n.properties",
                node_type
            );
            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                for line in result_str.lines().skip(1) {
                    let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                    if parts.len() >= 3 {
                        let properties = serde_json::from_str(parts[2]).unwrap_or(JsonValue::Null);
                        nodes.push(DbNode {
                            id: parts[0].to_string(),
                            label: parts[1].to_string(),
                            node_type: node_type.to_string(),
                            properties,
                        });
                    }
                }
            }
        }

        Ok(nodes)
    }

    /// Get all edges.
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let conn = self.conn()?;
        let mut edges = Vec::new();

        // Query edges between each pair of node types
        for src_type in Self::NODE_TYPES {
            for tgt_type in Self::NODE_TYPES {
                let query = format!(
                    "MATCH (a:{})-[e:Edge]->(b:{}) RETURN a.object_id, b.object_id, e.edge_type, e.properties",
                    src_type, tgt_type
                );
                if let Ok(result) = conn.query(&query) {
                    let result_str = result.to_string();
                    for line in result_str.lines().skip(1) {
                        let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                        if parts.len() >= 4 {
                            let properties =
                                serde_json::from_str(parts[3]).unwrap_or(JsonValue::Null);
                            edges.push(DbEdge {
                                source: parts[0].to_string(),
                                target: parts[1].to_string(),
                                edge_type: parts[2].to_string(),
                                properties,
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        Ok(edges)
    }

    /// Get all distinct edge types.
    pub fn get_edge_types(&self) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let result = conn.query("MATCH ()-[e:Edge]->() RETURN DISTINCT e.edge_type")?;

        let mut types = Vec::new();
        let result_str = result.to_string();
        for line in result_str.lines().skip(1) {
            let edge_type = line.trim();
            if !edge_type.is_empty() {
                types.push(edge_type.to_string());
            }
        }

        Ok(types)
    }

    /// Get all distinct node types (returns types that have at least one node).
    pub fn get_node_types(&self) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut types = Vec::new();

        for node_type in Self::NODE_TYPES {
            let query = format!("MATCH (n:{}) RETURN count(n)", node_type);
            if let Ok(result) = conn.query(&query) {
                if self.extract_count(&result) > 0 {
                    types.push(node_type.to_string());
                }
            }
        }

        Ok(types)
    }

    /// Search nodes by label/name (case-insensitive substring match).
    pub fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let conn = self.conn()?;
        let query_escaped = search_query.replace('\'', "''").to_lowercase();
        let mut nodes = Vec::new();

        // Search each node table
        for node_type in Self::NODE_TYPES {
            if nodes.len() >= limit {
                break;
            }

            let remaining = limit - nodes.len();
            let query = format!(
                "MATCH (n:{}) WHERE lower(n.label) CONTAINS '{}' OR lower(n.object_id) CONTAINS '{}' \
                 OR lower(n.name) CONTAINS '{}' \
                 RETURN n.object_id, n.label, n.properties LIMIT {}",
                node_type, query_escaped, query_escaped, query_escaped, remaining
            );

            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                for line in result_str.lines().skip(1) {
                    let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                    if parts.len() >= 3 {
                        let properties = serde_json::from_str(parts[2]).unwrap_or(JsonValue::Null);
                        nodes.push(DbNode {
                            id: parts[0].to_string(),
                            label: parts[1].to_string(),
                            node_type: node_type.to_string(),
                            properties,
                        });
                    }
                }
            }
        }

        debug!(query = %search_query, found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    /// Resolve a node identifier (object ID or label) to an object ID.
    pub fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        let conn = self.conn()?;
        let id_escaped = identifier.replace('\'', "''");
        let id_lower = id_escaped.to_lowercase();

        // First try exact object_id match across all tables
        for node_type in Self::NODE_TYPES {
            let query = format!(
                "MATCH (n:{}) WHERE n.object_id = '{}' RETURN n.object_id LIMIT 1",
                node_type, id_escaped
            );
            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                if let Some(line) = result_str.lines().skip(1).next() {
                    let id = line.trim();
                    if !id.is_empty() {
                        return Ok(Some(id.to_string()));
                    }
                }
            }
        }

        // Then try label match (case-insensitive) across all tables
        for node_type in Self::NODE_TYPES {
            let query = format!(
                "MATCH (n:{}) WHERE lower(n.label) = '{}' RETURN n.object_id LIMIT 1",
                node_type, id_lower
            );
            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                if let Some(line) = result_str.lines().skip(1).next() {
                    let id = line.trim();
                    if !id.is_empty() {
                        return Ok(Some(id.to_string()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Find shortest path between two nodes using Cypher.
    #[allow(clippy::type_complexity)]
    pub fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let conn = self.conn()?;

        // Look up node types from the typed tables
        let from_type = self.find_node_type(&conn, from);
        let to_type = self.find_node_type(&conn, to);

        // If we can find both node types, try a typed Cypher shortest path query
        if let (Some(from_type), Some(to_type)) = (from_type, to_type) {
            let from_escaped = from.replace('\'', "''");
            let to_escaped = to.replace('\'', "''");

            // Use Cypher shortest path with typed nodes
            let query = format!(
                "MATCH p = (a:{} {{object_id: '{}'}})-[e:Edge* SHORTEST 1..20]->(b:{} {{object_id: '{}'}}) \
                 RETURN nodes(p), relationships(p)",
                from_type, from_escaped, to_type, to_escaped
            );

            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();

                // Parse the path result
                // This is a simplified parser - may need adjustment based on actual output format
                let path = Vec::new();

                for line in result_str.lines().skip(1) {
                    if line.trim().is_empty() {
                        continue;
                    }

                    // For now, fall back to manual BFS if parsing is complex
                    // TODO: Properly parse Cypher path result
                    debug!("Shortest path query returned: {}", line);
                }

                if !path.is_empty() {
                    return Ok(Some(path));
                }
            }
        }

        // Fall back to BFS if typed query fails or types not found
        self.shortest_path_bfs(from, to)
    }

    /// Fallback BFS implementation for shortest path.
    fn shortest_path_bfs(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        let edges = self.get_all_edges()?;

        // Build adjacency list
        let mut adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for edge in &edges {
            adj.entry(edge.source.clone())
                .or_default()
                .push((edge.target.clone(), edge.edge_type.clone()));
        }

        // BFS
        let mut visited = std::collections::HashSet::new();
        let mut parent: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        queue.push_back(from.to_string());
        visited.insert(from.to_string());

        while let Some(current) = queue.pop_front() {
            if current == to {
                // Reconstruct path
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

    /// Find all nodes that can reach Domain Admins groups.
    /// Uses typed queries: User -> Group (where Group SID ends in -512)
    pub fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        debug!(exclude = ?exclude_edge_types, "Finding paths to Domain Admins");

        let conn = self.conn()?;

        // Build WHERE clause to exclude certain edge types
        let exclude_clause = if exclude_edge_types.is_empty() {
            String::new()
        } else {
            let conditions: Vec<String> = exclude_edge_types
                .iter()
                .map(|t| format!("e.edge_type <> '{}'", t.replace('\'', "''")))
                .collect();
            format!(
                "AND ALL(e IN relationships(p) WHERE {})",
                conditions.join(" AND ")
            )
        };

        // Find all users with paths to DA groups (SID ending in -512)
        // DA groups are in the Group table
        let query = format!(
            "MATCH p = (u:User)-[:Edge*1..10]->(da:Group) \
             WHERE da.object_id ENDS WITH '-512' {} \
             RETURN u.object_id, u.label, length(p) as hops \
             ORDER BY hops, u.label",
            exclude_clause
        );

        let result = conn.query(&query)?;
        let result_str = result.to_string();

        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for line in result_str.lines().skip(1) {
            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() >= 3 {
                let id = parts[0].to_string();
                // Deduplicate - keep shortest path for each user
                if seen.contains(&id) {
                    continue;
                }
                seen.insert(id.clone());

                if let Ok(hops) = parts[2].parse::<usize>() {
                    results.push((id, "User".to_string(), parts[1].to_string(), hops));
                }
            }
        }

        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    /// Get nodes by their IDs.
    pub fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.conn()?;
        let id_list: Vec<String> = ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_set = id_list.join(", ");

        let mut nodes = Vec::new();

        // Query each node table for the requested IDs
        for node_type in Self::NODE_TYPES {
            let query = format!(
                "MATCH (n:{}) WHERE n.object_id IN [{}] \
                 RETURN n.object_id, n.label, n.properties",
                node_type, id_set
            );

            if let Ok(result) = conn.query(&query) {
                let result_str = result.to_string();
                for line in result_str.lines().skip(1) {
                    let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                    if parts.len() >= 3 {
                        let properties = serde_json::from_str(parts[2]).unwrap_or(JsonValue::Null);
                        nodes.push(DbNode {
                            id: parts[0].to_string(),
                            label: parts[1].to_string(),
                            node_type: node_type.to_string(),
                            properties,
                        });
                    }
                }
            }
        }

        Ok(nodes)
    }

    /// Get edges between a set of nodes.
    pub fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.conn()?;
        let id_list: Vec<String> = node_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_set = id_list.join(", ");

        let mut edges = Vec::new();

        // Query edges between each pair of node types
        for src_type in Self::NODE_TYPES {
            for tgt_type in Self::NODE_TYPES {
                let query = format!(
                    "MATCH (a:{})-[e:Edge]->(b:{}) \
                     WHERE a.object_id IN [{}] AND b.object_id IN [{}] \
                     RETURN a.object_id, b.object_id, e.edge_type, e.properties",
                    src_type, tgt_type, id_set, id_set
                );

                if let Ok(result) = conn.query(&query) {
                    let result_str = result.to_string();
                    for line in result_str.lines().skip(1) {
                        let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                        if parts.len() >= 4 {
                            let properties =
                                serde_json::from_str(parts[3]).unwrap_or(JsonValue::Null);
                            edges.push(DbEdge {
                                source: parts[0].to_string(),
                                target: parts[1].to_string(),
                                edge_type: parts[2].to_string(),
                                properties,
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        Ok(edges)
    }

    /// Get connections for a node in the specified direction.
    pub fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        let all_edges = self.get_all_edges()?;

        // Admin permission edge types
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

        // Filter edges based on direction
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

        // Collect all node IDs involved (including the original node)
        let mut node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        node_ids.insert(node_id.to_string());
        for edge in &filtered_edges {
            node_ids.insert(edge.source.clone());
            node_ids.insert(edge.target.clone());
        }

        // Fetch all involved nodes
        let node_id_vec: Vec<String> = node_ids.into_iter().collect();
        let nodes = self.get_nodes_by_ids(&node_id_vec)?;

        Ok((nodes, filtered_edges))
    }

    /// Run a custom Cypher query.
    pub fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        debug!(query = %query, "Running custom Cypher query");

        let conn = self.conn()?;
        let result = conn.query(query)?;

        // Convert result to JSON
        let result_str = result.to_string();
        let lines: Vec<&str> = result_str.lines().collect();

        if lines.is_empty() {
            return Ok(serde_json::json!({ "rows": [], "headers": [] }));
        }

        // Parse header
        let headers: Vec<String> = lines[0].split('|').map(|s| s.trim().to_string()).collect();

        // Parse rows
        let rows: Vec<Vec<JsonValue>> = lines[1..]
            .iter()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                line.split('|')
                    .map(|s| JsonValue::String(s.trim().to_string()))
                    .collect()
            })
            .collect();

        Ok(serde_json::json!({
            "headers": headers,
            "rows": rows
        }))
    }

    /// Compute security insights from the graph.
    pub fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights with Cypher");

        let conn = self.conn()?;

        // Count total users (from User table)
        let user_result = conn.query("MATCH (n:User) RETURN count(n)")?;
        let total_users = self.extract_count(&user_result);

        // Find "real" DAs - users who are members of DA groups (SID ends with -512)
        // Using typed tables: User -> Group path with MemberOf edges
        let real_da_query = "
            MATCH p = (u:User)-[:Edge*1..10]->(da:Group)
            WHERE da.object_id ENDS WITH '-512'
            AND ALL(e IN relationships(p) WHERE e.edge_type = 'MemberOf')
            RETURN DISTINCT u.object_id, u.label
        ";
        let real_da_result = conn.query(real_da_query)?;
        let real_das: Vec<(String, String)> = self.parse_id_label_pairs(&real_da_result);
        let real_da_count = real_das.len();

        // Find "effective" DAs - users with any path to DA group (SID -512)
        let effective_da_query = "
            MATCH p = (u:User)-[:Edge*1..10]->(da:Group)
            WHERE da.object_id ENDS WITH '-512'
            RETURN DISTINCT u.object_id, u.label, min(length(p)) as hops
        ";
        let effective_result = conn.query(effective_da_query)?;
        let effective_das: Vec<(String, String, usize)> =
            self.parse_effective_das(&effective_result);
        let effective_da_count = effective_das.len();

        // Compute ratio and percentage
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

        // Compute reachability from well-known principals (all are Groups)
        // SIDs: S-1-1-0 = Everyone, S-1-5-11 = Authenticated Users
        // -513 = Domain Users, -515 = Domain Computers
        let well_known_principals = [
            ("Everyone", "S-1-1-0"),
            ("Authenticated Users", "S-1-5-11"),
            ("Domain Users", "-513"),
            ("Domain Computers", "-515"),
        ];

        let mut reachability = Vec::new();
        for (name, id_pattern) in well_known_principals {
            let (principal_id, reachable_count) = if id_pattern.starts_with('-') {
                // Suffix match - these are domain-relative SIDs (Groups)
                let query = format!(
                    "MATCH (p:Group) WHERE p.object_id ENDS WITH '{}' \
                     OPTIONAL MATCH (p)-[:Edge*1..5]->(target) \
                     RETURN p.object_id, count(DISTINCT target)",
                    id_pattern
                );
                match conn.query(&query) {
                    Ok(result) => {
                        let result_str = result.to_string();
                        let mut id = None;
                        let mut count = 0;
                        for line in result_str.lines().skip(1) {
                            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                            if parts.len() >= 2 {
                                id = Some(parts[0].to_string());
                                count = parts[1].parse().unwrap_or(0);
                                break;
                            }
                        }
                        (id, count)
                    }
                    Err(_) => (None, 0),
                }
            } else {
                // Exact SID match - well-known SIDs (Groups)
                let query = format!(
                    "MATCH (p:Group {{object_id: '{}'}}) \
                     OPTIONAL MATCH (p)-[:Edge*1..5]->(target) \
                     RETURN p.object_id, count(DISTINCT target)",
                    id_pattern
                );
                match conn.query(&query) {
                    Ok(result) => {
                        let result_str = result.to_string();
                        let mut id = None;
                        let mut count = 0;
                        for line in result_str.lines().skip(1) {
                            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
                            if parts.len() >= 2 {
                                id = Some(parts[0].to_string());
                                count = parts[1].parse().unwrap_or(0);
                                break;
                            }
                        }
                        (id, count)
                    }
                    Err(_) => (None, 0),
                }
            };

            reachability.push(ReachabilityInsight {
                principal_name: name.to_string(),
                principal_id,
                reachable_count,
            });
        }

        debug!(
            effective_das = effective_da_count,
            real_das = real_da_count,
            total_users = total_users,
            "Security insights computed"
        );

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

    /// Parse ID/label pairs from query result.
    fn parse_id_label_pairs(&self, result: &kuzu::QueryResult) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        let result_str = result.to_string();
        for line in result_str.lines().skip(1) {
            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() >= 2 {
                pairs.push((parts[0].to_string(), parts[1].to_string()));
            }
        }
        pairs
    }

    /// Parse effective DAs from query result.
    fn parse_effective_das(&self, result: &kuzu::QueryResult) -> Vec<(String, String, usize)> {
        let mut results = Vec::new();
        let result_str = result.to_string();
        for line in result_str.lines().skip(1) {
            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() >= 3 {
                if let Ok(hops) = parts[2].parse::<usize>() {
                    results.push((parts[0].to_string(), parts[1].to_string(), hops));
                }
            }
        }
        results
    }

    // Query history methods
    pub fn add_query_history(
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
        let conn = self.conn()?;
        let id_escaped = id.replace('\'', "''");
        let name_escaped = name.replace('\'', "''");
        let query_escaped = query.replace('\'', "''");
        let status_escaped = status.replace('\'', "''");
        let error_escaped = error.map(|e| e.replace('\'', "''")).unwrap_or_default();
        let count = result_count.unwrap_or(0);
        let duration = duration_ms.unwrap_or(0) as i64;

        let cypher = format!(
            "CREATE (h:QueryHistory {{id: '{}', name: '{}', query: '{}', timestamp: {}, result_count: {}, status: '{}', started_at: {}, duration_ms: {}, error: '{}'}})",
            id_escaped, name_escaped, query_escaped, timestamp, count, status_escaped, started_at, duration, error_escaped
        );

        conn.query(&cypher)?;
        Ok(())
    }

    pub fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn()?;
        let id_escaped = id.replace('\'', "''");
        let status_escaped = status.replace('\'', "''");
        let error_escaped = error.map(|e| e.replace('\'', "''")).unwrap_or_default();

        let mut set_parts = vec![format!("h.status = '{}'", status_escaped)];
        if let Some(duration) = duration_ms {
            set_parts.push(format!("h.duration_ms = {}", duration as i64));
        }
        if let Some(count) = result_count {
            set_parts.push(format!("h.result_count = {}", count));
        }
        if error.is_some() {
            set_parts.push(format!("h.error = '{}'", error_escaped));
        }

        let cypher = format!(
            "MATCH (h:QueryHistory {{id: '{}'}}) SET {}",
            id_escaped,
            set_parts.join(", ")
        );

        conn.query(&cypher)?;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn get_query_history(
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
        let conn = self.conn()?;

        // Get total count
        let count_result = conn.query("MATCH (h:QueryHistory) RETURN count(h)")?;
        let total = self.extract_count(&count_result);

        // Get paginated results
        let query = format!(
            "MATCH (h:QueryHistory) \
             RETURN h.id, h.name, h.query, h.timestamp, h.result_count, h.status, h.started_at, h.duration_ms, h.error \
             ORDER BY h.started_at DESC \
             SKIP {} LIMIT {}",
            offset, limit
        );

        let result = conn.query(&query)?;
        let mut history = Vec::new();
        let result_str = result.to_string();

        for line in result_str.lines().skip(1) {
            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() >= 9 {
                let timestamp: i64 = parts[3].parse().unwrap_or(0);
                let result_count: Option<i64> = parts[4].parse().ok();
                let status = parts[5].to_string();
                let started_at: i64 = parts[6].parse().unwrap_or(0);
                let duration_ms: Option<u64> = parts[7].parse().ok();
                let error = if parts[8].is_empty() {
                    None
                } else {
                    Some(parts[8].to_string())
                };
                history.push((
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].to_string(),
                    timestamp,
                    result_count,
                    status,
                    started_at,
                    duration_ms,
                    error,
                ));
            }
        }

        Ok((history, total))
    }

    pub fn delete_query_history(&self, id: &str) -> Result<()> {
        let conn = self.conn()?;
        let id_escaped = id.replace('\'', "''");
        conn.query(&format!(
            "MATCH (h:QueryHistory {{id: '{}'}}) DELETE h",
            id_escaped
        ))?;
        Ok(())
    }

    pub fn clear_query_history(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.query("MATCH (h:QueryHistory) DELETE h")?;
        Ok(())
    }
}

// ============================================================================
// DatabaseBackend Trait Implementation
// ============================================================================

impl DatabaseBackend for KuzuDatabase {
    fn name(&self) -> &'static str {
        "KuzuDB"
    }

    fn supports_language(&self, lang: QueryLanguage) -> bool {
        matches!(lang, QueryLanguage::Cypher)
    }

    fn default_language(&self) -> QueryLanguage {
        QueryLanguage::Cypher
    }

    fn clear(&self) -> Result<()> {
        KuzuDatabase::clear(self)
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        KuzuDatabase::insert_node(self, node)
    }

    fn insert_edge(&self, edge: DbEdge) -> Result<()> {
        KuzuDatabase::insert_edge(self, edge)
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        KuzuDatabase::insert_nodes(self, nodes)
    }

    fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        KuzuDatabase::insert_edges(self, edges)
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        KuzuDatabase::get_stats(self)
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        KuzuDatabase::get_detailed_stats(self)
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        KuzuDatabase::get_security_insights(self)
    }

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        KuzuDatabase::get_all_nodes(self)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        KuzuDatabase::get_all_edges(self)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        KuzuDatabase::get_nodes_by_ids(self, ids)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        KuzuDatabase::get_edges_between(self, node_ids)
    }

    fn get_edge_types(&self) -> Result<Vec<String>> {
        KuzuDatabase::get_edge_types(self)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        KuzuDatabase::get_node_types(self)
    }

    fn search_nodes(&self, query: &str, limit: usize) -> Result<Vec<DbNode>> {
        KuzuDatabase::search_nodes(self, query, limit)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        KuzuDatabase::resolve_node_identifier(self, identifier)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        KuzuDatabase::get_node_connections(self, node_id, direction)
    }

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        KuzuDatabase::shortest_path(self, from, to)
    }

    fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        KuzuDatabase::find_paths_to_domain_admins(self, exclude_edge_types)
    }

    fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        KuzuDatabase::run_custom_query(self, query)
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
        KuzuDatabase::add_query_history(
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
        KuzuDatabase::update_query_status(self, id, status, duration_ms, result_count, error)
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
        KuzuDatabase::get_query_history(self, limit, offset)
    }

    fn delete_query_history(&self, id: &str) -> Result<()> {
        KuzuDatabase::delete_query_history(self, id)
    }

    fn clear_query_history(&self) -> Result<()> {
        KuzuDatabase::clear_query_history(self)
    }
}
