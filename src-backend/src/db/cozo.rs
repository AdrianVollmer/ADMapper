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

    /// Get all nodes (for graph rendering).
    pub fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let result = self.db.run_script(
            "?[object_id, label, node_type, properties] := *nodes{object_id, label, node_type, properties}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;

        let json = result.into_json();
        let rows = json["rows"].as_array();

        let mut nodes = Vec::new();
        if let Some(rows) = rows {
            for row in rows {
                if let (Some(id), Some(label), Some(node_type), Some(properties)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_str()),
                    row.get(2).and_then(|v| v.as_str()),
                    row.get(3).and_then(|v| v.as_str()),
                ) {
                    let properties: JsonValue =
                        serde_json::from_str(properties).unwrap_or(JsonValue::Null);
                    nodes.push(DbNode {
                        id: id.to_string(),
                        label: label.to_string(),
                        node_type: node_type.to_string(),
                        properties,
                    });
                }
            }
        }

        Ok(nodes)
    }

    /// Get all edges (for graph rendering).
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let result = self.db.run_script(
            "?[source, target, edge_type, properties] := *edges{source, target, edge_type, properties}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;

        let json = result.into_json();
        let rows = json["rows"].as_array();

        let mut edges = Vec::new();
        if let Some(rows) = rows {
            for row in rows {
                if let (Some(source), Some(target), Some(edge_type), Some(properties)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_str()),
                    row.get(2).and_then(|v| v.as_str()),
                    row.get(3).and_then(|v| v.as_str()),
                ) {
                    let properties: JsonValue =
                        serde_json::from_str(properties).unwrap_or(JsonValue::Null);
                    edges.push(DbEdge {
                        source: source.to_string(),
                        target: target.to_string(),
                        edge_type: edge_type.to_string(),
                        properties,
                    });
                }
            }
        }

        Ok(edges)
    }

    /// Search nodes by label (case-insensitive substring match).
    pub fn search_nodes(&self, query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let query_lower = query.to_lowercase();
        debug!(query = %query, limit = limit, "Searching nodes");

        // CozoDB doesn't have LIKE/ILIKE, so we fetch all and filter
        // For large datasets, consider adding a full-text search index
        let result = self.db.run_script(
            "?[object_id, label, node_type, properties] := *nodes{object_id, label, node_type, properties}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;

        let json = result.into_json();
        let rows = json["rows"].as_array();

        let mut nodes = Vec::new();
        if let Some(rows) = rows {
            for row in rows {
                if let (Some(id), Some(label), Some(node_type), Some(properties)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_str()),
                    row.get(2).and_then(|v| v.as_str()),
                    row.get(3).and_then(|v| v.as_str()),
                ) {
                    // Case-insensitive search on label and id
                    if label.to_lowercase().contains(&query_lower)
                        || id.to_lowercase().contains(&query_lower)
                    {
                        let properties: JsonValue =
                            serde_json::from_str(properties).unwrap_or(JsonValue::Null);
                        nodes.push(DbNode {
                            id: id.to_string(),
                            label: label.to_string(),
                            node_type: node_type.to_string(),
                            properties,
                        });
                        if nodes.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

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
                // Fix: edge types should be on the source node, not target
                let mut fixed_path: Vec<(String, Option<String>)> = Vec::new();
                for i in 0..path.len() {
                    if i == path.len() - 1 {
                        fixed_path.push((path[i].0.clone(), None));
                    } else {
                        fixed_path.push((path[i].0.clone(), path[i + 1].1.clone()));
                    }
                }
                debug!(path_len = fixed_path.len(), "Path found");
                return Ok(Some(fixed_path));
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
}
