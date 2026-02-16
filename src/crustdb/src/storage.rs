//! SQLite storage backend for the graph database.

use crate::error::Result;
use crate::graph::{Edge, Node, PropertyValue};
use crate::DatabaseStats;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::path::Path;

/// Current schema version.
const SCHEMA_VERSION: i32 = 1;

/// SQLite-based storage backend.
pub struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Create an in-memory database.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Initialize the database schema.
    fn init_schema(&self) -> Result<()> {
        // Enable foreign keys
        self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        // Check current schema version
        let version = self.get_schema_version();

        if version == 0 {
            // Fresh database - create schema
            self.create_schema_v1()?;
        } else if version < SCHEMA_VERSION {
            // Run migrations
            self.migrate(version)?;
        }

        Ok(())
    }

    /// Get current schema version (0 if no schema exists).
    fn get_schema_version(&self) -> i32 {
        self.conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Create the initial schema (v1).
    fn create_schema_v1(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Metadata table for schema versioning
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO meta (key, value) VALUES ('schema_version', '1');

            -- Normalized node labels
            CREATE TABLE node_labels (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            -- Normalized edge types
            CREATE TABLE edge_types (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            -- Nodes table
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                properties TEXT NOT NULL DEFAULT '{}'
            );

            -- Node to label mapping (many-to-many)
            CREATE TABLE node_label_map (
                node_id INTEGER NOT NULL,
                label_id INTEGER NOT NULL,
                PRIMARY KEY (node_id, label_id),
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (label_id) REFERENCES node_labels(id) ON DELETE RESTRICT
            );
            CREATE INDEX idx_node_label_map_label ON node_label_map(label_id);
            CREATE INDEX idx_node_label_map_node ON node_label_map(node_id);

            -- Edges table
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                type_id INTEGER NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (type_id) REFERENCES edge_types(id) ON DELETE RESTRICT
            );
            CREATE INDEX idx_edges_source ON edges(source_id);
            CREATE INDEX idx_edges_target ON edges(target_id);
            CREATE INDEX idx_edges_type ON edges(type_id);
            CREATE INDEX idx_edges_source_type ON edges(source_id, type_id);
            CREATE INDEX idx_edges_target_type ON edges(target_id, type_id);
            "#,
        )?;
        Ok(())
    }

    /// Run migrations from old_version to current.
    fn migrate(&self, _old_version: i32) -> Result<()> {
        // Future migrations go here
        Ok(())
    }

    /// Get or create a node label ID.
    fn get_or_create_label(&self, label: &str) -> Result<i64> {
        // Try to get existing
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM node_labels WHERE name = ?1",
                params![label],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create new
        self.conn.execute(
            "INSERT INTO node_labels (name) VALUES (?1)",
            params![label],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get or create an edge type ID.
    fn get_or_create_edge_type(&self, edge_type: &str) -> Result<i64> {
        // Try to get existing
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM edge_types WHERE name = ?1",
                params![edge_type],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create new
        self.conn.execute(
            "INSERT INTO edge_types (name) VALUES (?1)",
            params![edge_type],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert a node into the database.
    pub fn insert_node(&self, labels: &[String], properties: &serde_json::Value) -> Result<i64> {
        let props_json = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT INTO nodes (properties) VALUES (?1)",
            params![props_json],
        )?;
        let node_id = self.conn.last_insert_rowid();

        for label in labels {
            let label_id = self.get_or_create_label(label)?;
            self.conn.execute(
                "INSERT INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
                params![node_id, label_id],
            )?;
        }

        Ok(node_id)
    }

    /// Insert an edge into the database.
    pub fn insert_edge(
        &self,
        source_id: i64,
        target_id: i64,
        edge_type: &str,
        properties: &serde_json::Value,
    ) -> Result<i64> {
        let type_id = self.get_or_create_edge_type(edge_type)?;
        let props_json = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT INTO edges (source_id, target_id, type_id, properties) VALUES (?1, ?2, ?3, ?4)",
            params![source_id, target_id, type_id, props_json],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
        let node: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT id, properties FROM nodes WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let Some((id, props_json)) = node else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        // Get labels via join
        let mut label_stmt = self.conn.prepare(
            "SELECT nl.name FROM node_labels nl
             JOIN node_label_map nlm ON nl.id = nlm.label_id
             WHERE nlm.node_id = ?1",
        )?;
        let labels: Vec<String> = label_stmt
            .query_map(params![id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Some(Node {
            id,
            labels,
            properties,
        }))
    }

    /// Get an edge by ID.
    pub fn get_edge(&self, id: i64) -> Result<Option<Edge>> {
        let edge: Option<(i64, i64, i64, String, String)> = self
            .conn
            .query_row(
                "SELECT e.id, e.source_id, e.target_id, et.name, e.properties
                 FROM edges e
                 JOIN edge_types et ON e.type_id = et.id
                 WHERE e.id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((id, source, target, edge_type, props_json)) = edge else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        Ok(Some(Edge {
            id,
            source,
            target,
            edge_type,
            properties,
        }))
    }

    /// Delete a node and its associated edges.
    pub fn delete_node(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Delete an edge.
    pub fn delete_edge(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM edges WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Scan all nodes in the database.
    pub fn scan_all_nodes(&self) -> Result<Vec<Node>> {
        let mut stmt = self.conn.prepare("SELECT id FROM nodes")?;

        let node_ids: Vec<i64> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut nodes = Vec::with_capacity(node_ids.len());
        for id in node_ids {
            if let Some(node) = self.get_node(id)? {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Find nodes by label.
    pub fn find_nodes_by_label(&self, label: &str) -> Result<Vec<Node>> {
        let mut stmt = self.conn.prepare(
            "SELECT n.id, n.properties FROM nodes n
             JOIN node_label_map nlm ON n.id = nlm.node_id
             JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE nl.name = ?1",
        )?;

        let node_ids: Vec<i64> = stmt
            .query_map(params![label], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut nodes = Vec::with_capacity(node_ids.len());
        for id in node_ids {
            if let Some(node) = self.get_node(id)? {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Find edges by type.
    pub fn find_edges_by_type(&self, edge_type: &str) -> Result<Vec<Edge>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.id FROM edges e
             JOIN edge_types et ON e.type_id = et.id
             WHERE et.name = ?1",
        )?;

        let edge_ids: Vec<i64> = stmt
            .query_map(params![edge_type], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut edges = Vec::with_capacity(edge_ids.len());
        for id in edge_ids {
            if let Some(edge) = self.get_edge(id)? {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Find outgoing edges from a node.
    pub fn find_outgoing_edges(&self, node_id: i64) -> Result<Vec<Edge>> {
        let mut stmt = self.conn.prepare("SELECT id FROM edges WHERE source_id = ?1")?;

        let edge_ids: Vec<i64> = stmt
            .query_map(params![node_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut edges = Vec::with_capacity(edge_ids.len());
        for id in edge_ids {
            if let Some(edge) = self.get_edge(id)? {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Find incoming edges to a node.
    pub fn find_incoming_edges(&self, node_id: i64) -> Result<Vec<Edge>> {
        let mut stmt = self.conn.prepare("SELECT id FROM edges WHERE target_id = ?1")?;

        let edge_ids: Vec<i64> = stmt
            .query_map(params![node_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut edges = Vec::with_capacity(edge_ids.len());
        for id in edge_ids {
            if let Some(edge) = self.get_edge(id)? {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let node_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;

        let edge_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM edges", [], |row| row.get(0))?;

        let label_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM node_labels", [], |row| row.get(0))?;

        let edge_type_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM edge_types", [], |row| row.get(0))?;

        Ok(DatabaseStats {
            node_count,
            edge_count,
            label_count,
            edge_type_count,
        })
    }

    /// Begin a transaction.
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.conn.transaction()?)
    }

    /// Get all node labels.
    pub fn get_all_labels(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT name FROM node_labels ORDER BY name")?;
        let labels: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(labels)
    }

    /// Get all edge types.
    pub fn get_all_edge_types(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT name FROM edge_types ORDER BY name")?;
        let types: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_query_node() {
        let storage = SqliteStorage::in_memory().unwrap();

        let props = serde_json::json!({"name": "Alice", "age": 30});
        let node_id = storage
            .insert_node(&["Person".to_string()], &props)
            .unwrap();

        let node = storage.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.id, node_id);
        assert!(node.has_label("Person"));
        assert_eq!(
            node.get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_create_node_with_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();

        let props = serde_json::json!({"name": "Charlie"});
        let node_id = storage
            .insert_node(&["Person".to_string(), "Actor".to_string()], &props)
            .unwrap();

        let node = storage.get_node(node_id).unwrap().unwrap();
        assert!(node.has_label("Person"));
        assert!(node.has_label("Actor"));
    }

    #[test]
    fn test_create_and_query_edge() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Alice"}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();

        let edge_id = storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({"since": 2020}))
            .unwrap();

        let edge = storage.get_edge(edge_id).unwrap().unwrap();
        assert_eq!(edge.source, alice_id);
        assert_eq!(edge.target, bob_id);
        assert_eq!(edge.edge_type, "KNOWS");
    }

    #[test]
    fn test_find_nodes_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Alice"}))
            .unwrap();
        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();
        storage
            .insert_node(&["Company".to_string()], &serde_json::json!({"name": "Acme"}))
            .unwrap();

        let people = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(people.len(), 2);

        let companies = storage.find_nodes_by_label("Company").unwrap();
        assert_eq!(companies.len(), 1);
    }

    #[test]
    fn test_find_edges_by_type() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let acme_id = storage
            .insert_node(&["Company".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, acme_id, "WORKS_AT", &serde_json::json!({}))
            .unwrap();

        let knows_edges = storage.find_edges_by_type("KNOWS").unwrap();
        assert_eq!(knows_edges.len(), 1);

        let works_at_edges = storage.find_edges_by_type("WORKS_AT").unwrap();
        assert_eq!(works_at_edges.len(), 1);
    }

    #[test]
    fn test_outgoing_incoming_edges() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let charlie_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, charlie_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(bob_id, alice_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        let alice_outgoing = storage.find_outgoing_edges(alice_id).unwrap();
        assert_eq!(alice_outgoing.len(), 2);

        let alice_incoming = storage.find_incoming_edges(alice_id).unwrap();
        assert_eq!(alice_incoming.len(), 1);
    }

    #[test]
    fn test_stats() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        storage
            .insert_node(&["Company".to_string()], &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.label_count, 2);
        assert_eq!(stats.edge_type_count, 1);
    }

    #[test]
    fn test_delete_node_cascades() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();

        let edge_id = storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        // Delete alice - should cascade delete the edge
        storage.delete_node(alice_id).unwrap();

        assert!(storage.get_node(alice_id).unwrap().is_none());
        assert!(storage.get_edge(edge_id).unwrap().is_none());
        assert!(storage.get_node(bob_id).unwrap().is_some());
    }

    #[test]
    fn test_get_all_labels_and_types() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(
                &["Person".to_string(), "Actor".to_string()],
                &serde_json::json!({}),
            )
            .unwrap();
        let movie_id = storage
            .insert_node(&["Movie".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, movie_id, "ACTED_IN", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, movie_id, "DIRECTED", &serde_json::json!({}))
            .unwrap();

        let labels = storage.get_all_labels().unwrap();
        assert_eq!(labels, vec!["Actor", "Movie", "Person"]);

        let types = storage.get_all_edge_types().unwrap();
        assert_eq!(types, vec!["ACTED_IN", "DIRECTED"]);
    }
}
