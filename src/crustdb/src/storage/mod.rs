//! SQLite storage backend for the graph database.
//!
//! This module provides a SQLite-based storage layer for the graph database,
//! organized into the following submodules:
//!
//! - `schema`: Database schema creation and migrations
//! - `crud`: Node and edge CRUD operations
//! - `query`: Query operations (find, count, scan)
//! - `history`: Query history management
//! - `cache`: Query cache management
//! - `index`: Property index management

mod cache;
mod crud;
mod history;
mod index;
mod query;
mod schema;

use crate::error::{Error, Result};
use crate::DatabaseStats;
use rusqlite::{Connection, Transaction};
use std::path::Path;

/// Validate a property name to prevent JSON path injection.
///
/// Property names must contain only alphanumeric characters and underscores,
/// and must not be empty. This prevents injection attacks in JSON path expressions.
fn validate_property_name(property: &str) -> Result<()> {
    if property.is_empty() {
        return Err(Error::InvalidProperty(
            "Property name cannot be empty".to_string(),
        ));
    }
    if !property
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::InvalidProperty(format!(
            "Property name '{}' contains invalid characters (only alphanumeric and underscore allowed)",
            property
        )));
    }
    Ok(())
}

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

    /// Open an existing database in read-only mode.
    ///
    /// This is used for read pool connections. The schema is assumed to exist
    /// (created by the primary write connection). Read-only connections can
    /// execute queries concurrently without blocking each other or the writer.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        use rusqlite::OpenFlags;
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        // Set busy timeout for read connections too
        conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
        Ok(Self { conn })
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let node_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;

        let edge_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))?;

        let label_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM node_labels", [], |row| row.get(0))?;

        let edge_type_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM rel_types", [], |row| row.get(0))?;

        Ok(DatabaseStats {
            node_count,
            edge_count,
            label_count,
            edge_type_count,
        })
    }

    /// Get database file size in bytes (page_count * page_size).
    pub fn database_size(&self) -> Result<usize> {
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok((page_count * page_size) as usize)
    }

    /// Clear all data from the database (nodes, relationships, labels, types).
    /// This is much faster than deleting via Cypher queries.
    pub fn clear(&self) -> Result<()> {
        // Delete in order respecting foreign key relationships
        self.conn.execute("DELETE FROM node_label_map", [])?;
        self.conn.execute("DELETE FROM relationships", [])?;
        self.conn.execute("DELETE FROM nodes", [])?;
        self.conn.execute("DELETE FROM rel_types", [])?;
        self.conn.execute("DELETE FROM node_labels", [])?;
        Ok(())
    }

    /// Checkpoint the WAL file, merging it into the main database file.
    ///
    /// This is called during graceful shutdown to ensure WAL files are cleaned up.
    /// Uses TRUNCATE mode which merges WAL and then truncates it to zero size.
    pub fn checkpoint(&self) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    /// Begin a transaction.
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.conn.transaction()?)
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached entries.
    pub entry_count: usize,
    /// Total size of cached results in bytes.
    pub total_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::PropertyValue;

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
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();

        let rel_id = storage
            .insert_edge(
                alice_id,
                bob_id,
                "KNOWS",
                &serde_json::json!({"since": 2020}),
            )
            .unwrap();

        let relationship = storage.get_edge(rel_id).unwrap().unwrap();
        assert_eq!(relationship.source, alice_id);
        assert_eq!(relationship.target, bob_id);
        assert_eq!(relationship.rel_type, "KNOWS");
    }

    #[test]
    fn test_find_nodes_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();
        storage
            .insert_node(
                &["Company".to_string()],
                &serde_json::json!({"name": "Acme"}),
            )
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

        let rel_id = storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        // Delete alice - should cascade delete the relationship
        storage.delete_node(alice_id).unwrap();

        assert!(storage.get_node(alice_id).unwrap().is_none());
        assert!(storage.get_edge(rel_id).unwrap().is_none());
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

    #[test]
    fn test_get_label_counts() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Insert nodes with various labels
        storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();
        storage
            .insert_node(
                &["Company".to_string()],
                &serde_json::json!({"name": "Acme"}),
            )
            .unwrap();
        storage
            .insert_node(&["User".to_string()], &serde_json::json!({"name": "User1"}))
            .unwrap();

        let counts = storage.get_label_counts().unwrap();

        assert_eq!(counts.get("Person"), Some(&2));
        assert_eq!(counts.get("Company"), Some(&1));
        assert_eq!(counts.get("User"), Some(&1));
        assert_eq!(counts.get("Unknown"), None);
    }

    #[test]
    fn test_property_name_validation() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with a valid property
        let props = serde_json::json!({"object_id": "test123"});
        storage.insert_node(&["Test".to_string()], &props).unwrap();

        // Valid property names should work
        assert!(storage
            .find_node_by_property("object_id", "test123")
            .is_ok());
        assert!(storage.find_node_by_property("valid_name", "value").is_ok());
        assert!(storage.find_node_by_property("name123", "value").is_ok());

        // Invalid property names should be rejected
        assert!(storage.find_node_by_property("", "value").is_err());
        assert!(storage.find_node_by_property("name.path", "value").is_err());
        assert!(storage.find_node_by_property("name'--", "value").is_err());
        assert!(storage.find_node_by_property("name)", "value").is_err());
        assert!(storage.find_node_by_property("name$", "value").is_err());
        assert!(storage
            .find_node_by_property("name space", "value")
            .is_err());

        // Same validation for build_property_index
        assert!(storage.build_property_index("object_id").is_ok());
        assert!(storage.build_property_index("").is_err());
        assert!(storage.build_property_index("name'--").is_err());
    }

    #[test]
    fn test_property_indexes() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Initially no property indexes
        let indexes = storage.list_property_indexes().unwrap();
        assert!(indexes.is_empty());

        // Create an index
        storage.create_property_index("object_id").unwrap();
        assert!(storage.has_property_index("object_id").unwrap());

        // List should show it
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes, vec!["object_id"]);

        // Creating same index again is a no-op
        storage.create_property_index("object_id").unwrap();
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes.len(), 1);

        // Create another index
        storage.create_property_index("name").unwrap();
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.contains(&"object_id".to_string()));
        assert!(indexes.contains(&"name".to_string()));

        // Drop an index
        assert!(storage.drop_property_index("object_id").unwrap());
        assert!(!storage.has_property_index("object_id").unwrap());

        // Drop non-existent index returns false
        assert!(!storage.drop_property_index("object_id").unwrap());

        // Invalid property names should be rejected
        assert!(storage.create_property_index("").is_err());
        assert!(storage.create_property_index("name'--").is_err());
    }

    #[test]
    fn test_upsert_nodes_batch_merges_properties() {
        let mut storage = SqliteStorage::in_memory().unwrap();

        // Insert an orphan/placeholder node with minimal properties
        let orphans = vec![(
            vec!["Base".to_string()],
            serde_json::json!({
                "object_id": "S-1-5-21-TEST",
                "name": "S-1-5-21-TEST",
                "placeholder": true
            }),
        )];
        let orphan_ids = storage.upsert_nodes_batch(&orphans).unwrap();
        assert_eq!(orphan_ids.len(), 1);

        // Verify placeholder node exists
        let node = storage.get_node(orphan_ids[0]).unwrap().unwrap();
        assert_eq!(node.get("placeholder"), Some(&PropertyValue::Bool(true)));
        assert_eq!(
            node.get("name"),
            Some(&PropertyValue::String("S-1-5-21-TEST".to_string()))
        );

        // Now upsert with full properties (non-placeholder)
        let full_nodes = vec![(
            vec!["User".to_string()],
            serde_json::json!({
                "object_id": "S-1-5-21-TEST",
                "name": "Full User Name",
                "displayname": "Test User"
            }),
        )];
        let full_ids = storage.upsert_nodes_batch(&full_nodes).unwrap();
        assert_eq!(full_ids.len(), 1);
        assert_eq!(full_ids[0], orphan_ids[0]); // Same node ID

        // Verify properties were merged and placeholder was removed
        let node = storage.get_node(full_ids[0]).unwrap().unwrap();
        assert_eq!(node.get("placeholder"), None); // Placeholder removed
        assert_eq!(
            node.get("name"),
            Some(&PropertyValue::String("Full User Name".to_string()))
        );
        assert_eq!(
            node.get("displayname"),
            Some(&PropertyValue::String("Test User".to_string()))
        );

        // Verify labels were merged
        assert!(node.has_label("Base"));
        assert!(node.has_label("User"));
    }

    #[test]
    fn test_upsert_keeps_placeholder_when_merging_placeholders() {
        let mut storage = SqliteStorage::in_memory().unwrap();

        // Insert first placeholder
        let first = vec![(
            vec!["Base".to_string()],
            serde_json::json!({
                "object_id": "PLACEHOLDER-TEST",
                "name": "First Name",
                "placeholder": true
            }),
        )];
        storage.upsert_nodes_batch(&first).unwrap();

        // Merge with second placeholder (keeps placeholder property)
        let second = vec![(
            vec!["Other".to_string()],
            serde_json::json!({
                "object_id": "PLACEHOLDER-TEST",
                "displayname": "Display",
                "placeholder": true
            }),
        )];
        let ids = storage.upsert_nodes_batch(&second).unwrap();

        // Verify placeholder is still there
        let node = storage.get_node(ids[0]).unwrap().unwrap();
        assert_eq!(node.get("placeholder"), Some(&PropertyValue::Bool(true)));
        assert_eq!(
            node.get("name"),
            Some(&PropertyValue::String("First Name".to_string()))
        );
        assert_eq!(
            node.get("displayname"),
            Some(&PropertyValue::String("Display".to_string()))
        );
    }

    #[test]
    fn test_find_outgoing_edges_by_object_id() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with object_id
        let alice_id = storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"object_id": "alice-1", "name": "Alice"}),
            )
            .unwrap();
        let bob_id = storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"object_id": "bob-2", "name": "Bob"}),
            )
            .unwrap();
        let charlie_id = storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"object_id": "charlie-3", "name": "Charlie"}),
            )
            .unwrap();

        // Create edges: alice -> bob (KNOWS), alice -> charlie (WORKS_WITH)
        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, charlie_id, "WORKS_WITH", &serde_json::json!({}))
            .unwrap();
        // Also bob -> charlie to ensure we don't get extra edges
        storage
            .insert_edge(bob_id, charlie_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        // Find outgoing edges from alice
        let alice_edges = storage.find_outgoing_edges_by_object_id("alice-1").unwrap();
        assert_eq!(alice_edges.len(), 2);

        // Check we get the correct targets and types
        let edge_set: std::collections::HashSet<_> = alice_edges.into_iter().collect();
        assert!(edge_set.contains(&("bob-2".to_string(), "KNOWS".to_string())));
        assert!(edge_set.contains(&("charlie-3".to_string(), "WORKS_WITH".to_string())));

        // Find outgoing edges from bob
        let bob_edges = storage.find_outgoing_edges_by_object_id("bob-2").unwrap();
        assert_eq!(bob_edges.len(), 1);
        assert_eq!(bob_edges[0], ("charlie-3".to_string(), "KNOWS".to_string()));

        // Find outgoing edges from charlie (none)
        let charlie_edges = storage
            .find_outgoing_edges_by_object_id("charlie-3")
            .unwrap();
        assert!(charlie_edges.is_empty());

        // Non-existent node returns empty
        let nobody_edges = storage.find_outgoing_edges_by_object_id("nobody").unwrap();
        assert!(nobody_edges.is_empty());
    }
}
