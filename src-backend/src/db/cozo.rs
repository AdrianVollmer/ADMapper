//! CozoDB-backed graph database for storing AD graph data.

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, trace, warn};

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

        // Try to create relations, ignore if they already exist
        match self.db.run_script(create_nodes, Default::default(), ScriptMutability::Mutable) {
            Ok(_) => debug!("Created nodes relation"),
            Err(_) => trace!("Nodes relation already exists"),
        }
        match self.db.run_script(create_edges, Default::default(), ScriptMutability::Mutable) {
            Ok(_) => debug!("Created edges relation"),
            Err(_) => trace!("Edges relation already exists"),
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
    pub fn insert_nodes(&self, nodes: &[(String, String, String, JsonValue)]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        // Build the data rows
        let mut rows = Vec::with_capacity(nodes.len());
        for (object_id, label, node_type, properties) in nodes {
            let props_str = serde_json::to_string(properties)?;
            rows.push(vec![
                DataValue::Str(object_id.clone().into()),
                DataValue::Str(label.clone().into()),
                DataValue::Str(node_type.clone().into()),
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
    pub fn insert_edges(&self, edges: &[(String, String, String, JsonValue)]) -> Result<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        let mut rows = Vec::with_capacity(edges.len());
        for (source, target, edge_type, properties) in edges {
            let props_str = serde_json::to_string(properties)?;
            rows.push(vec![
                DataValue::Str(source.clone().into()),
                DataValue::Str(target.clone().into()),
                DataValue::Str(edge_type.clone().into()),
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
    pub fn get_all_nodes(&self) -> Result<Vec<(String, String, String, JsonValue)>> {
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
                if let (Some(object_id), Some(label), Some(node_type), Some(properties)) = (
                    row.get(0).and_then(|v| v.as_str()),
                    row.get(1).and_then(|v| v.as_str()),
                    row.get(2).and_then(|v| v.as_str()),
                    row.get(3).and_then(|v| v.as_str()),
                ) {
                    let props: JsonValue =
                        serde_json::from_str(properties).unwrap_or(JsonValue::Null);
                    nodes.push((
                        object_id.to_string(),
                        label.to_string(),
                        node_type.to_string(),
                        props,
                    ));
                }
            }
        }

        Ok(nodes)
    }

    /// Get all edges (for graph rendering).
    pub fn get_all_edges(&self) -> Result<Vec<(String, String, String, JsonValue)>> {
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
                    let props: JsonValue =
                        serde_json::from_str(properties).unwrap_or(JsonValue::Null);
                    edges.push((
                        source.to_string(),
                        target.to_string(),
                        edge_type.to_string(),
                        props,
                    ));
                }
            }
        }

        Ok(edges)
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
            (
                "user-1".to_string(),
                "admin@corp.local".to_string(),
                "User".to_string(),
                serde_json::json!({"enabled": true}),
            ),
            (
                "group-1".to_string(),
                "Domain Admins".to_string(),
                "Group".to_string(),
                serde_json::json!({}),
            ),
        ];

        let count = db.insert_nodes(&nodes).unwrap();
        assert_eq!(count, 2);

        let (node_count, _) = db.get_stats().unwrap();
        assert_eq!(node_count, 2);
    }

    #[test]
    fn test_insert_edges() {
        let db = GraphDatabase::in_memory().unwrap();

        let edges = vec![(
            "user-1".to_string(),
            "group-1".to_string(),
            "MemberOf".to_string(),
            serde_json::json!({}),
        )];

        let count = db.insert_edges(&edges).unwrap();
        assert_eq!(count, 1);

        let (_, edge_count) = db.get_stats().unwrap();
        assert_eq!(edge_count, 1);
    }

    #[test]
    fn test_clear() {
        let db = GraphDatabase::in_memory().unwrap();

        let nodes = vec![(
            "user-1".to_string(),
            "admin".to_string(),
            "User".to_string(),
            serde_json::json!({}),
        )];
        db.insert_nodes(&nodes).unwrap();

        db.clear().unwrap();

        let (node_count, _) = db.get_stats().unwrap();
        assert_eq!(node_count, 0);
    }
}
