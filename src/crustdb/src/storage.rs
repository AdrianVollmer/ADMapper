//! SQLite storage backend for the graph database.

use crate::error::{Error, Result};
use crate::graph::{Edge, Node, PropertyValue};
use crate::DatabaseStats;
use rusqlite::{Connection, params, OptionalExtension};
use std::path::Path;

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
        self.conn.execute_batch(
            r#"
            -- Nodes table
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY,
                properties TEXT NOT NULL DEFAULT '{}'
            );

            -- Node labels (many-to-many)
            CREATE TABLE IF NOT EXISTS node_labels (
                node_id INTEGER NOT NULL,
                label TEXT NOT NULL,
                PRIMARY KEY (node_id, label),
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_node_labels_label ON node_labels(label);

            -- Edges table
            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                edge_type TEXT NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
            CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
            CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);

            -- Enable foreign keys
            PRAGMA foreign_keys = ON;
            "#,
        )?;
        Ok(())
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
            self.conn.execute(
                "INSERT INTO node_labels (node_id, label) VALUES (?1, ?2)",
                params![node_id, label],
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
        let props_json = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT INTO edges (source_id, target_id, edge_type, properties) VALUES (?1, ?2, ?3, ?4)",
            params![source_id, target_id, edge_type, props_json],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, properties FROM nodes WHERE id = ?1"
        )?;

        let node = stmt.query_row(params![id], |row| {
            let id: i64 = row.get(0)?;
            let props_json: String = row.get(1)?;
            Ok((id, props_json))
        }).optional()?;

        let Some((id, props_json)) = node else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        // Get labels
        let mut label_stmt = self.conn.prepare(
            "SELECT label FROM node_labels WHERE node_id = ?1"
        )?;
        let labels: Vec<String> = label_stmt
            .query_map(params![id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Some(Node { id, labels, properties }))
    }

    /// Get an edge by ID.
    pub fn get_edge(&self, id: i64) -> Result<Option<Edge>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_id, target_id, edge_type, properties FROM edges WHERE id = ?1"
        )?;

        let edge: Option<(i64, i64, i64, String, String)> = stmt.query_row(params![id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        }).optional()?;

        let Some((id, source, target, edge_type, props_json)) = edge else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        Ok(Some(Edge { id, source, target, edge_type, properties }))
    }

    /// Delete a node and its associated edges.
    pub fn delete_node(&self, id: i64) -> Result<bool> {
        let affected = self.conn.execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Delete an edge.
    pub fn delete_edge(&self, id: i64) -> Result<bool> {
        let affected = self.conn.execute("DELETE FROM edges WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let node_count: usize = self.conn.query_row(
            "SELECT COUNT(*) FROM nodes",
            [],
            |row| row.get(0),
        )?;

        let edge_count: usize = self.conn.query_row(
            "SELECT COUNT(*) FROM edges",
            [],
            |row| row.get(0),
        )?;

        let label_count: usize = self.conn.query_row(
            "SELECT COUNT(DISTINCT label) FROM node_labels",
            [],
            |row| row.get(0),
        )?;

        let edge_type_count: usize = self.conn.query_row(
            "SELECT COUNT(DISTINCT edge_type) FROM edges",
            [],
            |row| row.get(0),
        )?;

        Ok(DatabaseStats {
            node_count,
            edge_count,
            label_count,
            edge_type_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_query_node() {
        let storage = SqliteStorage::in_memory().unwrap();

        let props = serde_json::json!({"name": "Alice", "age": 30});
        let node_id = storage.insert_node(&["Person".to_string()], &props).unwrap();

        let node = storage.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.id, node_id);
        assert!(node.has_label("Person"));
        assert_eq!(node.get("name"), Some(&PropertyValue::String("Alice".to_string())));
    }

    #[test]
    fn test_create_and_query_edge() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage.insert_node(&["Person".to_string()], &serde_json::json!({"name": "Alice"})).unwrap();
        let bob_id = storage.insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"})).unwrap();

        let edge_id = storage.insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({"since": 2020})).unwrap();

        let edge = storage.get_edge(edge_id).unwrap().unwrap();
        assert_eq!(edge.source, alice_id);
        assert_eq!(edge.target, bob_id);
        assert_eq!(edge.edge_type, "KNOWS");
    }

    #[test]
    fn test_stats() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage.insert_node(&["Person".to_string()], &serde_json::json!({})).unwrap();
        let bob_id = storage.insert_node(&["Person".to_string()], &serde_json::json!({})).unwrap();
        storage.insert_node(&["Company".to_string()], &serde_json::json!({})).unwrap();
        storage.insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({})).unwrap();

        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.label_count, 2);
        assert_eq!(stats.edge_type_count, 1);
    }
}
