//! CozoDB-backed graph database for storing AD graph data.

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, trace};

/// A node stored in the database.
#[derive(Clone, Debug)]
pub struct DbNode {
    pub id: String,
    pub label: String,
    pub node_type: String,
    pub properties: JsonValue,
}

/// An edge stored in the database.
#[derive(Clone, Debug)]
pub struct DbEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: JsonValue,
}

/// Detailed statistics about the database.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DetailedStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub users: usize,
    pub computers: usize,
    pub groups: usize,
    pub domains: usize,
    pub ous: usize,
    pub gpos: usize,
}

/// Security insight for a well-known principal reachability.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ReachabilityInsight {
    pub principal_name: String,
    pub principal_id: Option<String>,
    pub reachable_count: usize,
}

/// Security insights computed from the graph.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SecurityInsights {
    /// Users who have a path to Domain Admins
    pub effective_da_count: usize,
    /// Users who are direct or transitive members of Domain Admins
    pub real_da_count: usize,
    /// Ratio of effective DAs to real DAs
    pub da_ratio: f64,
    /// Total users in the database
    pub total_users: usize,
    /// Percentage of users that are effective DAs
    pub effective_da_percentage: f64,
    /// Objects reachable from well-known principals
    pub reachability: Vec<ReachabilityInsight>,
    /// Users with paths to Domain Admins (for export)
    pub effective_das: Vec<(String, String, usize)>,
    /// Users who are members of Domain Admins (for export)
    pub real_das: Vec<(String, String)>,
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    Cozo(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl From<cozo::Error> for DbError {
    fn from(e: cozo::Error) -> Self {
        DbError::Cozo(e.to_string())
    }
}

impl From<kuzu::Error> for DbError {
    fn from(e: kuzu::Error) -> Self {
        DbError::Cozo(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DbError>;

/// A graph database backed by CozoDB with SQLite storage.
#[derive(Clone)]
pub struct GraphDatabase {
    db: Arc<DbInstance>,
}

impl GraphDatabase {
    /// Create or open a database at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str: Cow<str> = path.as_ref().to_string_lossy();
        info!(path = %path_str, "Opening CozoDB with SQLite backend");
        let db = DbInstance::new("sqlite", path_str.as_ref(), "")?;
        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        info!("Database initialized successfully");
        Ok(instance)
    }

    /// Create an in-memory database (for testing).
    pub fn in_memory() -> Result<Self> {
        debug!("Creating in-memory database");
        let db = DbInstance::new("mem", "", "")?;
        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Initialize the schema if relations don't exist.
    fn init_schema(&self) -> Result<()> {
        debug!("Initializing database schema");

        // Create nodes relation
        // object_id is the primary key, stores label, type, and JSON properties
        let create_nodes = r#"
            :create nodes {
                object_id: String
                =>
                label: String,
                node_type: String,
                properties: String
            }
        "#;

        // Create edges relation
        // Composite key of source, target, edge_type
        let create_edges = r#"
            :create edges {
                source: String,
                target: String,
                edge_type: String
                =>
                properties: String
            }
        "#;

        // Create query_history relation
        let create_query_history = r#"
            :create query_history {
                id: String
                =>
                name: String,
                query: String,
                timestamp: Int,
                result_count: Int?
            }
        "#;

        // Try to create relations, ignore if they already exist
        match self
            .db
            .run_script(create_nodes, Default::default(), ScriptMutability::Mutable)
        {
            Ok(_) => debug!("Created nodes relation"),
            Err(_) => trace!("Nodes relation already exists"),
        }
        match self
            .db
            .run_script(create_edges, Default::default(), ScriptMutability::Mutable)
        {
            Ok(_) => debug!("Created edges relation"),
            Err(_) => trace!("Edges relation already exists"),
        }
        match self.db.run_script(
            create_query_history,
            Default::default(),
            ScriptMutability::Mutable,
        ) {
            Ok(_) => debug!("Created query_history relation"),
            Err(_) => trace!("Query_history relation already exists"),
        }

        Ok(())
    }

    /// Run a query and parse results with a custom parser function.
    /// This eliminates boilerplate for the common pattern of running a query
    /// and mapping rows to domain objects.
    fn query_rows<T, F>(&self, query: &str, parser: F) -> Result<Vec<T>>
    where
        F: Fn(&JsonValue) -> Option<T>,
    {
        let result = self
            .db
            .run_script(query, Default::default(), ScriptMutability::Immutable)?;
        let json = result.into_json();

        let items = json["rows"]
            .as_array()
            .map(|rows| rows.iter().filter_map(&parser).collect())
            .unwrap_or_default();

        Ok(items)
    }

    /// Clear all data from the database.
    pub fn clear(&self) -> Result<()> {
        info!("Clearing all data from database");
        // Delete all nodes and edges
        self.db.run_script(
            "?[object_id] := *nodes{object_id} :delete nodes {object_id}",
            Default::default(),
            ScriptMutability::Mutable,
        )?;
        self.db.run_script(
            "?[source, target, edge_type] := *edges{source, target, edge_type} :delete edges {source, target, edge_type}",
            Default::default(),
            ScriptMutability::Mutable,
        )?;
        debug!("Database cleared");
        Ok(())
    }

    /// Get all distinct edge types in the database.
    pub fn get_edge_types(&self) -> Result<Vec<String>> {
        let result = self.db.run_script(
            "?[edge_type] := *edges{edge_type}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let json = result.into_json();

        let types = json["rows"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(types)
    }

    /// Get all distinct node types in the database.
    pub fn get_node_types(&self) -> Result<Vec<String>> {
        let result = self.db.run_script(
            "?[node_type] := *nodes{node_type}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let json = result.into_json();

        let types = json["rows"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(types)
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

    /// Insert a batch of nodes.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        // Build the data rows
        let mut rows = Vec::with_capacity(nodes.len());
        for node in nodes {
            let props_str = serde_json::to_string(&node.properties)?;
            rows.push(vec![
                DataValue::Str(node.id.clone().into()),
                DataValue::Str(node.label.clone().into()),
                DataValue::Str(node.node_type.clone().into()),
                DataValue::Str(props_str.into()),
            ]);
        }

        let params = NamedRows {
            headers: vec![
                "object_id".to_string(),
                "label".to_string(),
                "node_type".to_string(),
                "properties".to_string(),
            ],
            rows,
            next: None,
        };

        let mut relations = BTreeMap::new();
        relations.insert("nodes".to_string(), params);
        self.db.import_relations(relations)?;

        Ok(nodes.len())
    }

    /// Insert a batch of edges.
    pub fn insert_edges(&self, edges: &[DbEdge]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        let mut rows = Vec::with_capacity(edges.len());
        for edge in edges {
            let props_str = serde_json::to_string(&edge.properties)?;
            rows.push(vec![
                DataValue::Str(edge.source.clone().into()),
                DataValue::Str(edge.target.clone().into()),
                DataValue::Str(edge.edge_type.clone().into()),
                DataValue::Str(props_str.into()),
            ]);
        }

        let params = NamedRows {
            headers: vec![
                "source".to_string(),
                "target".to_string(),
                "edge_type".to_string(),
                "properties".to_string(),
            ],
            rows,
            next: None,
        };

        let mut relations = BTreeMap::new();
        relations.insert("edges".to_string(), params);
        self.db.import_relations(relations)?;

        Ok(edges.len())
    }

    /// Get node and edge counts.
    pub fn get_stats(&self) -> Result<(usize, usize)> {
        let node_result = self.db.run_script(
            "?[count(object_id)] := *nodes{object_id}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let node_json = node_result.into_json();
        let node_count = node_json["rows"]
            .get(0)
            .and_then(|r| r.get(0))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        let edge_result = self.db.run_script(
            "?[count(source)] := *edges{source}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let edge_json = edge_result.into_json();
        let edge_count = edge_json["rows"]
            .get(0)
            .and_then(|r| r.get(0))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        Ok((node_count, edge_count))
    }

    /// Get detailed stats including counts by node type.
    pub fn get_detailed_stats(&self) -> Result<DetailedStats> {
        let (node_count, edge_count) = self.get_stats()?;

        // Get counts by node type
        let type_result = self.db.run_script(
            "?[node_type, count(object_id)] := *nodes{object_id, node_type}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let type_json = type_result.into_json();

        let mut type_counts = std::collections::HashMap::new();
        if let Some(rows) = type_json["rows"].as_array() {
            for row in rows {
                if let (Some(node_type), Some(count)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_u64()),
                ) {
                    type_counts.insert(node_type.to_string(), count as usize);
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

    /// Compute security insights from the graph.
    pub fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        let nodes = self.get_all_nodes()?;
        let edges = self.get_all_edges()?;

        // Count total users
        let total_users = nodes.iter().filter(|n| n.node_type == "User").count();

        // Find Domain Admins groups (SID ending in -512)
        let da_nodes: Vec<&DbNode> = nodes.iter().filter(|n| n.id.ends_with("-512")).collect();

        // Find "real" DAs - users who are direct or transitive members of DA
        // Build adjacency for MemberOf edges only
        let mut member_of_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for edge in &edges {
            if edge.edge_type == "MemberOf" {
                member_of_adj
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
        }

        // BFS from each user to find if they can reach a DA group via MemberOf
        let da_ids: std::collections::HashSet<&str> =
            da_nodes.iter().map(|n| n.id.as_str()).collect();

        let mut real_das: Vec<(String, String)> = Vec::new();
        for node in &nodes {
            if node.node_type != "User" {
                continue;
            }

            // BFS to find if user can reach DA via MemberOf
            let mut visited = std::collections::HashSet::new();
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(node.id.clone());
            visited.insert(node.id.clone());

            while let Some(current) = queue.pop_front() {
                if da_ids.contains(current.as_str()) {
                    real_das.push((node.id.clone(), node.label.clone()));
                    break;
                }
                if let Some(targets) = member_of_adj.get(&current) {
                    for target in targets {
                        if !visited.contains(target) {
                            visited.insert(target.clone());
                            queue.push_back(target.clone());
                        }
                    }
                }
            }
        }

        let real_da_count = real_das.len();

        // Find "effective" DAs - users with any path to DA (already implemented)
        let effective_results = self.find_paths_to_domain_admins(&[])?;
        let effective_da_count = effective_results.len();
        let effective_das: Vec<(String, String, usize)> = effective_results
            .into_iter()
            .map(|(id, _node_type, label, hops)| (id, label, hops))
            .collect();

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

        // Compute reachability from well-known principals
        let well_known_principals = [
            ("Everyone", "S-1-1-0"),
            ("Authenticated Users", "S-1-5-11"),
            ("Domain Users", "-513"),   // SID suffix
            ("Domain Computers", "-515"), // SID suffix
        ];

        // Build forward adjacency for reachability (all edge types)
        let mut forward_adj: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for edge in &edges {
            forward_adj
                .entry(edge.source.clone())
                .or_default()
                .push(edge.target.clone());
        }

        let mut reachability = Vec::new();
        for (name, id_pattern) in well_known_principals {
            // Find the principal node(s)
            let principal_ids: Vec<&str> = nodes
                .iter()
                .filter(|n| {
                    if id_pattern.starts_with('-') {
                        // Suffix match
                        n.id.ends_with(id_pattern)
                    } else {
                        // Exact match
                        n.id == id_pattern
                    }
                })
                .map(|n| n.id.as_str())
                .collect();

            if principal_ids.is_empty() {
                reachability.push(ReachabilityInsight {
                    principal_name: name.to_string(),
                    principal_id: None,
                    reachable_count: 0,
                });
                continue;
            }

            // BFS from all principal nodes
            let mut visited = std::collections::HashSet::new();
            let mut queue = std::collections::VecDeque::new();
            for id in &principal_ids {
                visited.insert(id.to_string());
                queue.push_back(id.to_string());
            }

            while let Some(current) = queue.pop_front() {
                if let Some(targets) = forward_adj.get(&current) {
                    for target in targets {
                        if !visited.contains(target) {
                            visited.insert(target.clone());
                            queue.push_back(target.clone());
                        }
                    }
                }
            }

            // Count reachable non-trivial objects (exclude the principal itself)
            let reachable_count = visited.len().saturating_sub(principal_ids.len());

            reachability.push(ReachabilityInsight {
                principal_name: name.to_string(),
                principal_id: principal_ids.first().map(|s| s.to_string()),
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

    /// Get all nodes (for graph rendering).
    pub fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        self.query_rows(
            "?[object_id, label, node_type, properties] := *nodes{object_id, label, node_type, properties}",
            Self::parse_node_row,
        )
    }

    /// Parse a database row into a DbNode.
    fn parse_node_row(row: &JsonValue) -> Option<DbNode> {
        let id = row.get(0).and_then(|v| v.as_str())?;
        let label = row.get(1).and_then(|v| v.as_str())?;
        let node_type = row.get(2).and_then(|v| v.as_str())?;
        let props_str = row.get(3).and_then(|v| v.as_str())?;
        let properties = serde_json::from_str(props_str).unwrap_or(JsonValue::Null);

        Some(DbNode {
            id: id.to_string(),
            label: label.to_string(),
            node_type: node_type.to_string(),
            properties,
        })
    }

    /// Get all edges (for graph rendering).
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        self.query_rows(
            "?[source, target, edge_type, properties] := *edges{source, target, edge_type, properties}",
            Self::parse_edge_row,
        )
    }

    /// Parse a database row into a DbEdge.
    fn parse_edge_row(row: &JsonValue) -> Option<DbEdge> {
        let source = row.get(0).and_then(|v| v.as_str())?;
        let target = row.get(1).and_then(|v| v.as_str())?;
        let edge_type = row.get(2).and_then(|v| v.as_str())?;
        let props_str = row.get(3).and_then(|v| v.as_str())?;
        let properties = serde_json::from_str(props_str).unwrap_or(JsonValue::Null);

        Some(DbEdge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
            properties,
        })
    }

    /// Resolve a node identifier (object ID or label) to an object ID.
    /// Returns the object ID if found, None otherwise.
    /// First checks for exact object ID match, then tries exact label match.
    /// When multiple nodes have the same label, prefers SID-style IDs (S-1-5-...) over GUIDs.
    pub fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        let all_nodes = self.get_all_nodes()?;

        // First, check for exact object ID match
        for node in &all_nodes {
            if node.id == identifier {
                return Ok(Some(node.id.clone()));
            }
        }

        // Then, check for exact label match (case-insensitive)
        // Collect all matches to handle duplicates
        let identifier_lower = identifier.to_lowercase();
        let matches: Vec<&DbNode> = all_nodes
            .iter()
            .filter(|node| node.label.to_lowercase() == identifier_lower)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches[0].id.clone())),
            _ => {
                // Multiple matches - prefer SID-style IDs (S-1-5-...) over GUIDs
                // SIDs are more reliable identifiers for Active Directory objects
                let sid_match = matches.iter().find(|n| n.id.starts_with("S-1-5-"));
                if let Some(node) = sid_match {
                    return Ok(Some(node.id.clone()));
                }
                // Fall back to first match if no SID found
                Ok(Some(matches[0].id.clone()))
            }
        }
    }

    /// Search nodes by label (case-insensitive substring match).
    pub fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let query_lower = search_query.to_lowercase();
        debug!(query = %search_query, limit = limit, "Searching nodes");

        // CozoDB doesn't have LIKE/ILIKE, so we fetch all and filter
        // For large datasets, consider adding a full-text search index
        let nodes: Vec<DbNode> = self
            .query_rows(
                "?[object_id, label, node_type, properties] := *nodes{object_id, label, node_type, properties}",
                Self::parse_node_row,
            )?
            .into_iter()
            .filter(|node| {
                node.label.to_lowercase().contains(&query_lower)
                    || node.id.to_lowercase().contains(&query_lower)
            })
            .take(limit)
            .collect();

        debug!(found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    /// Find shortest path between two nodes using BFS.
    /// Returns the path as a list of (node_id, edge_type) pairs.
    #[allow(clippy::type_complexity)]
    pub fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        debug!(from = %from, to = %to, "Finding shortest path");

        // Get all edges for BFS
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
                debug!(path_len = path.len(), "Path found");
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

        debug!("No path found");
        Ok(None)
    }

    /// Find all nodes that can reach Domain Admins groups (SID ending in -512).
    /// Uses reverse BFS from all DA groups.
    /// Returns list of (node_id, node_type, node_label, hop_count) for nodes that can reach DA.
    pub fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        debug!(exclude = ?exclude_edge_types, "Finding paths to Domain Admins");

        let nodes = self.get_all_nodes()?;
        let edges = self.get_all_edges()?;

        // Find all Domain Admins groups (SID ending in -512)
        let da_nodes: Vec<&DbNode> = nodes.iter().filter(|n| n.id.ends_with("-512")).collect();

        if da_nodes.is_empty() {
            debug!("No Domain Admins groups found");
            return Ok(Vec::new());
        }

        debug!(da_count = da_nodes.len(), "Found Domain Admins groups");

        // Build reverse adjacency list (target -> sources)
        // This lets us do reverse BFS from DA to find all nodes that can reach it
        let exclude_set: std::collections::HashSet<&str> =
            exclude_edge_types.iter().map(|s| s.as_str()).collect();

        let mut reverse_adj: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();

        for edge in &edges {
            if !exclude_set.contains(edge.edge_type.as_str()) {
                reverse_adj
                    .entry(edge.target.clone())
                    .or_default()
                    .push((edge.source.clone(), edge.edge_type.clone()));
            }
        }

        // BFS from all DA nodes simultaneously
        let mut visited: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        // Initialize with all DA nodes at distance 0
        for da in &da_nodes {
            visited.insert(da.id.clone(), 0);
            queue.push_back((da.id.clone(), 0usize));
        }

        // Reverse BFS
        while let Some((current, dist)) = queue.pop_front() {
            if let Some(sources) = reverse_adj.get(&current) {
                for (source, _edge_type) in sources {
                    if !visited.contains_key(source) {
                        visited.insert(source.clone(), dist + 1);
                        queue.push_back((source.clone(), dist + 1));
                    }
                }
            }
        }

        // Build result: filter to User nodes only, exclude DA nodes themselves
        let node_map: std::collections::HashMap<&str, &DbNode> =
            nodes.iter().map(|n| (n.id.as_str(), n)).collect();

        let mut results: Vec<(String, String, String, usize)> = visited
            .into_iter()
            .filter_map(|(id, hops)| {
                if hops == 0 {
                    return None; // Exclude DA nodes themselves
                }
                let node = node_map.get(id.as_str())?;
                if node.node_type == "User" {
                    Some((id, node.node_type.clone(), node.label.clone(), hops))
                } else {
                    None
                }
            })
            .collect();

        // Sort by hop count, then by label
        results.sort_by(|a, b| a.3.cmp(&b.3).then_with(|| a.2.cmp(&b.2)));

        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    /// Get nodes by their IDs.
    pub fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let all_nodes = self.get_all_nodes()?;
        let id_set: std::collections::HashSet<&str> = ids.iter().map(|s| s.as_str()).collect();

        Ok(all_nodes
            .into_iter()
            .filter(|node| id_set.contains(node.id.as_str()))
            .collect())
    }

    /// Get edges between a set of nodes.
    pub fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let all_edges = self.get_all_edges()?;
        let id_set: std::collections::HashSet<&str> = node_ids.iter().map(|s| s.as_str()).collect();

        Ok(all_edges
            .into_iter()
            .filter(|edge| {
                id_set.contains(edge.source.as_str()) && id_set.contains(edge.target.as_str())
            })
            .collect())
    }

    /// Run a custom CozoDB query and extract nodes/edges from results.
    /// The query should return rows with columns that can be matched to node IDs.
    pub fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        debug!(query = %query, "Running custom query");

        let result = self
            .db
            .run_script(query, Default::default(), ScriptMutability::Immutable)?;
        let json = result.into_json();

        Ok(json)
    }

    /// Add a query to history.
    pub fn add_query_history(
        &self,
        id: &str,
        name: &str,
        query: &str,
        timestamp: i64,
        result_count: Option<i64>,
    ) -> Result<()> {
        debug!(id = %id, name = %name, "Adding query to history");

        let result_val = match result_count {
            Some(c) => DataValue::from(c),
            None => DataValue::Null,
        };

        let rows = vec![vec![
            DataValue::Str(id.into()),
            DataValue::Str(name.into()),
            DataValue::Str(query.into()),
            DataValue::from(timestamp),
            result_val,
        ]];

        let params = NamedRows {
            headers: vec![
                "id".to_string(),
                "name".to_string(),
                "query".to_string(),
                "timestamp".to_string(),
                "result_count".to_string(),
            ],
            rows,
            next: None,
        };

        let mut relations = BTreeMap::new();
        relations.insert("query_history".to_string(), params);
        self.db.import_relations(relations)?;

        Ok(())
    }

    /// Get query history, ordered by timestamp descending.
    #[allow(clippy::type_complexity)]
    pub fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<(String, String, String, i64, Option<i64>)>, usize)> {
        debug!(limit = limit, offset = offset, "Getting query history");

        // Get total count
        let count_result = self.db.run_script(
            "?[count(id)] := *query_history{id}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        let count_json = count_result.into_json();
        let total = count_json["rows"]
            .get(0)
            .and_then(|r| r.get(0))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        // Get paginated results, ordered by timestamp desc
        let query = format!(
            "?[id, name, query, timestamp, result_count] := *query_history{{id, name, query, timestamp, result_count}} :order -timestamp :limit {} :offset {}",
            limit, offset
        );

        let result = self
            .db
            .run_script(&query, Default::default(), ScriptMutability::Immutable)?;
        let json = result.into_json();
        let rows = json["rows"].as_array();

        let mut history = Vec::new();
        if let Some(rows) = rows {
            for row in rows {
                if let (Some(id), Some(name), Some(query), Some(timestamp)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_str()),
                    row.get(2).and_then(|v| v.as_str()),
                    row.get(3).and_then(|v| v.as_i64()),
                ) {
                    let result_count = row.get(4).and_then(|v| v.as_i64());
                    history.push((
                        id.to_string(),
                        name.to_string(),
                        query.to_string(),
                        timestamp,
                        result_count,
                    ));
                }
            }
        }

        Ok((history, total))
    }

    /// Delete a query from history.
    pub fn delete_query_history(&self, id: &str) -> Result<()> {
        debug!(id = %id, "Deleting query from history");

        let query = format!("?[id] <- [[\"{id}\"]] :delete query_history {{id}}");

        self.db
            .run_script(&query, Default::default(), ScriptMutability::Mutable)?;
        Ok(())
    }

    /// Clear all query history.
    pub fn clear_query_history(&self) -> Result<()> {
        debug!("Clearing all query history");

        self.db.run_script(
            "?[id] := *query_history{id} :delete query_history {id}",
            Default::default(),
            ScriptMutability::Mutable,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_db() {
        let db = GraphDatabase::in_memory().unwrap();
        let (nodes, edges) = db.get_stats().unwrap();
        assert_eq!(nodes, 0);
        assert_eq!(edges, 0);
    }

    #[test]
    fn test_insert_nodes() {
        let db = GraphDatabase::in_memory().unwrap();

        let nodes = vec![
            DbNode {
                id: "user-1".to_string(),
                label: "admin@corp.local".to_string(),
                node_type: "User".to_string(),
                properties: serde_json::json!({"enabled": true}),
            },
            DbNode {
                id: "group-1".to_string(),
                label: "Domain Admins".to_string(),
                node_type: "Group".to_string(),
                properties: serde_json::json!({}),
            },
        ];

        let count = db.insert_nodes(&nodes).unwrap();
        assert_eq!(count, 2);

        let (node_count, _) = db.get_stats().unwrap();
        assert_eq!(node_count, 2);
    }

    #[test]
    fn test_insert_edges() {
        let db = GraphDatabase::in_memory().unwrap();

        let edges = vec![DbEdge {
            source: "user-1".to_string(),
            target: "group-1".to_string(),
            edge_type: "MemberOf".to_string(),
            properties: serde_json::json!({}),
        }];

        let count = db.insert_edges(&edges).unwrap();
        assert_eq!(count, 1);

        let (_, edge_count) = db.get_stats().unwrap();
        assert_eq!(edge_count, 1);
    }

    #[test]
    fn test_clear() {
        let db = GraphDatabase::in_memory().unwrap();

        let nodes = vec![DbNode {
            id: "user-1".to_string(),
            label: "admin".to_string(),
            node_type: "User".to_string(),
            properties: serde_json::json!({}),
        }];
        db.insert_nodes(&nodes).unwrap();

        db.clear().unwrap();

        let (node_count, _) = db.get_stats().unwrap();
        assert_eq!(node_count, 0);
    }

    /// Helper to create a test database with sample data.
    fn setup_test_db() -> GraphDatabase {
        let db = GraphDatabase::in_memory().unwrap();

        let nodes = vec![
            DbNode {
                id: "S-1-5-21-USER1".to_string(),
                label: "admin@corp.local".to_string(),
                node_type: "User".to_string(),
                properties: serde_json::json!({"enabled": true}),
            },
            DbNode {
                id: "S-1-5-21-USER2".to_string(),
                label: "jsmith@corp.local".to_string(),
                node_type: "User".to_string(),
                properties: serde_json::json!({"enabled": true}),
            },
            DbNode {
                id: "S-1-5-21-GROUP1".to_string(),
                label: "Domain Admins".to_string(),
                node_type: "Group".to_string(),
                properties: serde_json::json!({}),
            },
            DbNode {
                id: "S-1-5-21-GROUP2".to_string(),
                label: "IT Staff".to_string(),
                node_type: "Group".to_string(),
                properties: serde_json::json!({}),
            },
            DbNode {
                id: "S-1-5-21-COMP1".to_string(),
                label: "DC01.corp.local".to_string(),
                node_type: "Computer".to_string(),
                properties: serde_json::json!({}),
            },
        ];
        db.insert_nodes(&nodes).unwrap();

        let edges = vec![
            DbEdge {
                source: "S-1-5-21-USER1".to_string(),
                target: "S-1-5-21-GROUP1".to_string(),
                edge_type: "MemberOf".to_string(),
                properties: serde_json::json!({}),
            },
            DbEdge {
                source: "S-1-5-21-USER2".to_string(),
                target: "S-1-5-21-GROUP2".to_string(),
                edge_type: "MemberOf".to_string(),
                properties: serde_json::json!({}),
            },
            DbEdge {
                source: "S-1-5-21-GROUP2".to_string(),
                target: "S-1-5-21-GROUP1".to_string(),
                edge_type: "MemberOf".to_string(),
                properties: serde_json::json!({}),
            },
            DbEdge {
                source: "S-1-5-21-GROUP1".to_string(),
                target: "S-1-5-21-COMP1".to_string(),
                edge_type: "AdminTo".to_string(),
                properties: serde_json::json!({}),
            },
        ];
        db.insert_edges(&edges).unwrap();

        db
    }

    // ========================================================================
    // Search Tests
    // ========================================================================

    #[test]
    fn test_search_nodes_case_insensitive() {
        let db = setup_test_db();

        // Search for "jsmith" - unique, tests case insensitivity
        let results = db.search_nodes("jsmith", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].label, "jsmith@corp.local");

        // Search with uppercase should find lowercase
        let results = db.search_nodes("JSMITH", 10).unwrap();
        assert_eq!(results.len(), 1);

        // Mixed case should work
        let results = db.search_nodes("JsMiTh", 10).unwrap();
        assert_eq!(results.len(), 1);

        // "admin" matches both "admin@corp.local" AND "Domain Admins"
        let results = db.search_nodes("admin", 10).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_nodes_limit() {
        let db = setup_test_db();

        // Search for ".local" which matches all 3 entities with that suffix
        let results = db.search_nodes(".local", 100).unwrap();
        assert!(results.len() >= 3); // admin@corp.local, jsmith@corp.local, DC01.corp.local

        // Now limit to 1
        let results = db.search_nodes(".local", 1).unwrap();
        assert_eq!(results.len(), 1);

        // Limit to 2
        let results = db.search_nodes(".local", 2).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_nodes_partial_match() {
        let db = setup_test_db();

        // Partial match on label
        let results = db.search_nodes("smith", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].label, "jsmith@corp.local");

        // Partial match on ID
        let results = db.search_nodes("USER1", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "S-1-5-21-USER1");

        // Match that returns multiple results
        let results = db.search_nodes("GROUP", 10).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_nodes_no_match() {
        let db = setup_test_db();

        let results = db.search_nodes("nonexistent", 10).unwrap();
        assert!(results.is_empty());
    }

    // ========================================================================
    // Resolve Node Identifier Tests
    // ========================================================================

    #[test]
    fn test_resolve_node_identifier_by_id() {
        let db = setup_test_db();

        // Resolve by exact object ID
        let resolved = db.resolve_node_identifier("S-1-5-21-USER1").unwrap();
        assert_eq!(resolved, Some("S-1-5-21-USER1".to_string()));
    }

    #[test]
    fn test_resolve_node_identifier_by_label() {
        let db = setup_test_db();

        // Resolve by exact label (case-insensitive)
        let resolved = db.resolve_node_identifier("admin@corp.local").unwrap();
        assert_eq!(resolved, Some("S-1-5-21-USER1".to_string()));

        // Case insensitive
        let resolved = db.resolve_node_identifier("ADMIN@CORP.LOCAL").unwrap();
        assert_eq!(resolved, Some("S-1-5-21-USER1".to_string()));

        // Group by label
        let resolved = db.resolve_node_identifier("Domain Admins").unwrap();
        assert_eq!(resolved, Some("S-1-5-21-GROUP1".to_string()));
    }

    #[test]
    fn test_resolve_node_identifier_not_found() {
        let db = setup_test_db();

        let resolved = db.resolve_node_identifier("nonexistent").unwrap();
        assert!(resolved.is_none());
    }

    // ========================================================================
    // Shortest Path Tests
    // ========================================================================

    #[test]
    fn test_shortest_path_direct() {
        let db = setup_test_db();

        // Direct edge: USER1 -> GROUP1
        let path = db
            .shortest_path("S-1-5-21-USER1", "S-1-5-21-GROUP1")
            .unwrap();

        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].0, "S-1-5-21-USER1");
        assert_eq!(path[0].1, Some("MemberOf".to_string()));
        assert_eq!(path[1].0, "S-1-5-21-GROUP1");
        assert_eq!(path[1].1, None);
    }

    #[test]
    fn test_shortest_path_multi_hop() {
        let db = setup_test_db();

        // Multi-hop: USER2 -> GROUP2 -> GROUP1 -> COMP1
        let path = db
            .shortest_path("S-1-5-21-USER2", "S-1-5-21-COMP1")
            .unwrap();

        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path.len(), 4);
        assert_eq!(path[0].0, "S-1-5-21-USER2");
        assert_eq!(path[1].0, "S-1-5-21-GROUP2");
        assert_eq!(path[2].0, "S-1-5-21-GROUP1");
        assert_eq!(path[3].0, "S-1-5-21-COMP1");
    }

    #[test]
    fn test_shortest_path_no_path() {
        let db = setup_test_db();

        // No path from COMP1 to USER1 (edges are directional)
        let path = db
            .shortest_path("S-1-5-21-COMP1", "S-1-5-21-USER1")
            .unwrap();

        assert!(path.is_none());
    }

    #[test]
    fn test_shortest_path_same_node() {
        let db = setup_test_db();

        // Path from node to itself should return single-node path
        let path = db
            .shortest_path("S-1-5-21-USER1", "S-1-5-21-USER1")
            .unwrap();

        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0, "S-1-5-21-USER1");
    }

    #[test]
    fn test_shortest_path_nonexistent_node() {
        let db = setup_test_db();

        // Path involving nonexistent node
        let path = db.shortest_path("nonexistent", "S-1-5-21-USER1").unwrap();
        assert!(path.is_none());

        let path = db.shortest_path("S-1-5-21-USER1", "nonexistent").unwrap();
        assert!(path.is_none());
    }

    // ========================================================================
    // Query History Tests
    // ========================================================================

    #[test]
    fn test_query_history_crud() {
        let db = GraphDatabase::in_memory().unwrap();

        // Add entries
        db.add_query_history("id1", "Query 1", "?[x] := x = 1", 1000, Some(1))
            .unwrap();
        db.add_query_history("id2", "Query 2", "?[x] := x = 2", 2000, Some(2))
            .unwrap();

        // Read entries
        let (history, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 2);
        assert_eq!(history.len(), 2);

        // Delete one entry
        db.delete_query_history("id1").unwrap();
        let (history, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(history[0].0, "id2");

        // Clear all
        db.clear_query_history().unwrap();
        let (history, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
        assert!(history.is_empty());
    }

    #[test]
    fn test_query_history_ordering() {
        let db = GraphDatabase::in_memory().unwrap();

        // Add entries with different timestamps
        db.add_query_history("oldest", "Old", "q1", 1000, None)
            .unwrap();
        db.add_query_history("middle", "Mid", "q2", 2000, None)
            .unwrap();
        db.add_query_history("newest", "New", "q3", 3000, None)
            .unwrap();

        // Should be ordered by timestamp descending (newest first)
        let (history, _) = db.get_query_history(10, 0).unwrap();
        assert_eq!(history[0].0, "newest");
        assert_eq!(history[1].0, "middle");
        assert_eq!(history[2].0, "oldest");
    }

    #[test]
    fn test_query_history_pagination() {
        let db = GraphDatabase::in_memory().unwrap();

        // Add 5 entries
        for i in 0..5 {
            db.add_query_history(
                &format!("id{}", i),
                &format!("Query {}", i),
                "query",
                i as i64 * 1000,
                None,
            )
            .unwrap();
        }

        // Page 1 (limit 2)
        let (page1, total) = db.get_query_history(2, 0).unwrap();
        assert_eq!(total, 5);
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].0, "id4"); // newest
        assert_eq!(page1[1].0, "id3");

        // Page 2 (limit 2, offset 2)
        let (page2, total) = db.get_query_history(2, 2).unwrap();
        assert_eq!(total, 5);
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].0, "id2");
        assert_eq!(page2[1].0, "id1");

        // Page 3 (limit 2, offset 4)
        let (page3, total) = db.get_query_history(2, 4).unwrap();
        assert_eq!(total, 5);
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].0, "id0"); // oldest
    }

    // ========================================================================
    // Node/Edge Retrieval Tests
    // ========================================================================

    #[test]
    fn test_get_nodes_by_ids_all_exist() {
        let db = setup_test_db();

        let ids = vec!["S-1-5-21-USER1".to_string(), "S-1-5-21-GROUP1".to_string()];
        let nodes = db.get_nodes_by_ids(&ids).unwrap();

        assert_eq!(nodes.len(), 2);
        let node_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(node_ids.contains(&"S-1-5-21-USER1"));
        assert!(node_ids.contains(&"S-1-5-21-GROUP1"));
    }

    #[test]
    fn test_get_nodes_by_ids_partial() {
        let db = setup_test_db();

        // Mix of existing and nonexistent IDs
        let ids = vec![
            "S-1-5-21-USER1".to_string(),
            "nonexistent".to_string(),
            "S-1-5-21-GROUP1".to_string(),
        ];
        let nodes = db.get_nodes_by_ids(&ids).unwrap();

        // Should only return the 2 that exist
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_get_nodes_by_ids_empty() {
        let db = setup_test_db();

        let nodes = db.get_nodes_by_ids(&[]).unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_get_edges_between_subset() {
        let db = setup_test_db();

        // Get edges between USER1, GROUP1, GROUP2
        let ids = vec![
            "S-1-5-21-USER1".to_string(),
            "S-1-5-21-GROUP1".to_string(),
            "S-1-5-21-GROUP2".to_string(),
        ];
        let edges = db.get_edges_between(&ids).unwrap();

        // Should include USER1->GROUP1 and GROUP2->GROUP1
        // Should NOT include USER2->GROUP2 (USER2 not in subset)
        // Should NOT include GROUP1->COMP1 (COMP1 not in subset)
        assert_eq!(edges.len(), 2);

        let edge_types: Vec<(&str, &str)> = edges
            .iter()
            .map(|e| (e.source.as_str(), e.target.as_str()))
            .collect();
        assert!(edge_types.contains(&("S-1-5-21-USER1", "S-1-5-21-GROUP1")));
        assert!(edge_types.contains(&("S-1-5-21-GROUP2", "S-1-5-21-GROUP1")));
    }

    #[test]
    fn test_get_edges_between_no_edges() {
        let db = setup_test_db();

        // These two nodes have no direct edges between them
        let ids = vec!["S-1-5-21-USER1".to_string(), "S-1-5-21-USER2".to_string()];
        let edges = db.get_edges_between(&ids).unwrap();

        assert!(edges.is_empty());
    }

    #[test]
    fn test_get_edges_between_empty() {
        let db = setup_test_db();

        let edges = db.get_edges_between(&[]).unwrap();
        assert!(edges.is_empty());
    }

    // ========================================================================
    // Custom Query Tests
    // ========================================================================

    #[test]
    fn test_run_custom_query_valid() {
        let db = setup_test_db();

        let result = db
            .run_custom_query("?[object_id] := *nodes{object_id}")
            .unwrap();

        assert!(result.get("rows").is_some());
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 5); // 5 nodes in test data
    }

    #[test]
    fn test_run_custom_query_with_filter() {
        let db = setup_test_db();

        let result = db
            .run_custom_query(
                "?[object_id, label] := *nodes{object_id, label, node_type}, node_type = 'User'",
            )
            .unwrap();

        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2); // 2 users in test data
    }
}
