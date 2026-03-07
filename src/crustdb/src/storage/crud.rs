//! Node and relationship CRUD operations.

use crate::error::Result;
use crate::graph::{Node, PropertyValue, Relationship};
use rusqlite::{params, OptionalExtension};

use super::SqliteStorage;

impl SqliteStorage {
    /// Get or create a node label ID.
    pub(crate) fn get_or_create_label(&self, label: &str) -> Result<i64> {
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
        self.conn
            .execute("INSERT INTO node_labels (name) VALUES (?1)", params![label])?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get or create a relationship type ID.
    pub(crate) fn get_or_create_relationship_type(&self, rel_type: &str) -> Result<i64> {
        // Try to get existing
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM rel_types WHERE name = ?1",
                params![rel_type],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create new
        self.conn.execute(
            "INSERT INTO rel_types (name) VALUES (?1)",
            params![rel_type],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert a node into the database.
    pub fn insert_node(&self, labels: &[String], properties: &serde_json::Value) -> Result<i64> {
        let props_json = serde_json::to_string(properties)?;
        // Extract object_id from properties for the dedicated column
        let object_id = properties
            .get("object_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        self.conn.execute(
            "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))",
            params![object_id, props_json],
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

    /// Insert a relationship into the database.
    pub fn insert_relationship(
        &self,
        source_id: i64,
        target_id: i64,
        rel_type: &str,
        properties: &serde_json::Value,
    ) -> Result<i64> {
        let type_id = self.get_or_create_relationship_type(rel_type)?;
        let props_json = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT INTO relationships (source_id, target_id, type_id, properties) VALUES (?1, ?2, ?3, jsonb(?4))",
            params![source_id, target_id, type_id, props_json],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert multiple nodes in a single transaction.
    ///
    /// Returns a vector of the created node IDs in the same order as the input.
    pub fn insert_nodes_batch(
        &mut self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut node_ids = Vec::with_capacity(nodes.len());

        // Pre-collect all unique labels and create them
        let mut label_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (labels, _) in nodes {
            for label in labels {
                if !label_cache.contains_key(label) {
                    let label_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM node_labels WHERE name = ?1",
                            params![label],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let label_id = match label_id {
                        Some(id) => id,
                        None => {
                            tx.execute(
                                "INSERT INTO node_labels (name) VALUES (?1)",
                                params![label],
                            )?;
                            tx.last_insert_rowid()
                        }
                    };
                    label_cache.insert(label.clone(), label_id);
                }
            }
        }

        // Insert nodes using prepared statement
        {
            let mut node_stmt =
                tx.prepare("INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))")?;
            let mut label_stmt =
                tx.prepare("INSERT INTO node_label_map (node_id, label_id) VALUES (?1, ?2)")?;

            for (labels, properties) in nodes {
                let props_json = serde_json::to_string(properties)?;
                // Extract object_id from properties for the dedicated column
                let object_id = properties
                    .get("object_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                node_stmt.execute(params![object_id, props_json])?;
                let node_id = tx.last_insert_rowid();
                node_ids.push(node_id);

                for label in labels {
                    if let Some(&label_id) = label_cache.get(label) {
                        label_stmt.execute(params![node_id, label_id])?;
                    }
                }
            }
        }

        tx.commit()?;
        Ok(node_ids)
    }

    /// Upsert multiple nodes in a single transaction.
    ///
    /// If a node with the same object_id already exists, its properties are merged
    /// (new properties are added, existing properties are updated) rather than
    /// replaced entirely. Labels are also merged.
    ///
    /// Returns a vector of the node IDs (internal SQLite IDs) in the same order as the input.
    pub fn upsert_nodes_batch(
        &mut self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut node_ids = Vec::with_capacity(nodes.len());

        // Pre-collect all unique labels and create them
        let mut label_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (labels, _) in nodes {
            for label in labels {
                if !label_cache.contains_key(label) {
                    let label_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM node_labels WHERE name = ?1",
                            params![label],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let label_id = match label_id {
                        Some(id) => id,
                        None => {
                            tx.execute(
                                "INSERT INTO node_labels (name) VALUES (?1)",
                                params![label],
                            )?;
                            tx.last_insert_rowid()
                        }
                    };
                    label_cache.insert(label.clone(), label_id);
                }
            }
        }

        // Upsert nodes using prepared statements
        // json_patch merges the new properties into the existing ones
        // If incoming data is not a placeholder, remove the placeholder property from existing node
        {
            let mut upsert_stmt = tx.prepare(
                "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))
                 ON CONFLICT(object_id) DO UPDATE SET
                   properties = CASE
                     WHEN json_extract(?2, '$.placeholder') = 1 THEN jsonb(json_patch(json(properties), json(?2)))
                     ELSE jsonb(json_remove(json_patch(json(properties), json(?2)), '$.placeholder'))
                   END",
            )?;
            let mut get_id_stmt = tx.prepare("SELECT id FROM nodes WHERE object_id = ?1")?;
            let mut label_stmt = tx.prepare(
                "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            )?;

            for (labels, properties) in nodes {
                let props_json = serde_json::to_string(properties)?;
                // Extract object_id from properties for the dedicated column
                let object_id = properties
                    .get("object_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if let Some(ref oid) = object_id {
                    upsert_stmt.execute(params![oid, props_json])?;
                    // Get the node ID (either newly inserted or existing)
                    let node_id: i64 = get_id_stmt.query_row(params![oid], |row| row.get(0))?;
                    node_ids.push(node_id);

                    // Merge labels (INSERT OR IGNORE handles duplicates)
                    for label in labels {
                        if let Some(&label_id) = label_cache.get(label) {
                            label_stmt.execute(params![node_id, label_id])?;
                        }
                    }
                } else {
                    // No object_id, fall back to regular insert
                    tx.execute(
                        "INSERT INTO nodes (object_id, properties) VALUES (NULL, jsonb(?1))",
                        params![props_json],
                    )?;
                    let node_id = tx.last_insert_rowid();
                    node_ids.push(node_id);

                    for label in labels {
                        if let Some(&label_id) = label_cache.get(label) {
                            label_stmt.execute(params![node_id, label_id])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(node_ids)
    }

    /// Get or create a node by object_id, returning its internal ID.
    ///
    /// If the node exists, returns its ID without modifying it.
    /// If it doesn't exist, creates an orphan node with just the object_id
    /// and the specified label, ready to be upserted later with full properties.
    pub fn get_or_create_node_by_object_id(&self, object_id: &str, label: &str) -> Result<i64> {
        // Try to find existing node
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM nodes WHERE object_id = ?1",
                params![object_id],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create orphan node with minimal properties
        let props = serde_json::json!({
            "object_id": object_id,
            "name": object_id,
            "placeholder": true
        });
        let props_json = serde_json::to_string(&props)?;

        self.conn.execute(
            "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))",
            params![object_id, props_json],
        )?;
        let node_id = self.conn.last_insert_rowid();

        // Add label
        let label_id = self.get_or_create_label(label)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            params![node_id, label_id],
        )?;

        Ok(node_id)
    }

    /// Insert multiple relationships in a single transaction.
    ///
    /// Each relationship is specified as (source_id, target_id, rel_type, properties).
    /// Returns a vector of the created relationship IDs in the same order as the input.
    pub fn insert_relationships_batch(
        &mut self,
        relationships: &[(i64, i64, String, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if relationships.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut rel_ids = Vec::with_capacity(relationships.len());

        // Pre-collect all unique relationship types and create them
        let mut type_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (_, _, rel_type, _) in relationships {
            if !type_cache.contains_key(rel_type) {
                let type_id: Option<i64> = tx
                    .query_row(
                        "SELECT id FROM rel_types WHERE name = ?1",
                        params![rel_type],
                        |row| row.get(0),
                    )
                    .optional()?;
                let type_id = match type_id {
                    Some(id) => id,
                    None => {
                        tx.execute(
                            "INSERT INTO rel_types (name) VALUES (?1)",
                            params![rel_type],
                        )?;
                        tx.last_insert_rowid()
                    }
                };
                type_cache.insert(rel_type.clone(), type_id);
            }
        }

        // Insert relationships using prepared statement
        {
            let mut rel_stmt = tx.prepare(
                "INSERT INTO relationships (source_id, target_id, type_id, properties) VALUES (?1, ?2, ?3, jsonb(?4))",
            )?;

            for (source_id, target_id, rel_type, properties) in relationships {
                let props_json = serde_json::to_string(properties)?;
                let type_id = type_cache.get(rel_type).copied().unwrap_or(0);
                rel_stmt.execute(params![source_id, target_id, type_id, props_json])?;
                rel_ids.push(tx.last_insert_rowid());
            }
        }

        tx.commit()?;
        Ok(rel_ids)
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
        // Use json() to convert JSONB blob back to JSON text
        let node: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT id, json(properties) FROM nodes WHERE id = ?1",
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
        let mut label_stmt = self.conn.prepare_cached(
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

    /// Get a relationship by ID.
    pub fn get_relationship(&self, id: i64) -> Result<Option<Relationship>> {
        // Use json() to convert JSONB blob back to JSON text
        let relationship: Option<(i64, i64, i64, String, String)> = self
            .conn
            .query_row(
                "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
                 FROM relationships e
                 JOIN rel_types et ON e.type_id = et.id
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

        let Some((id, source, target, rel_type, props_json)) = relationship else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        Ok(Some(Relationship {
            id,
            source,
            target,
            rel_type,
            properties,
        }))
    }

    /// Delete a node and its associated relationships.
    pub fn delete_node(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Delete a relationship.
    pub fn delete_relationship(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM relationships WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Check if a node has any connected relationships.
    pub fn has_relationships(&self, node_id: i64) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE source_id = ?1 OR target_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Update a single property on a node.
    pub fn update_node_property(
        &self,
        node_id: i64,
        property: &str,
        value: &PropertyValue,
    ) -> Result<bool> {
        // Get current properties (use json() to convert JSONB to text)
        let current: Option<String> = self
            .conn
            .query_row(
                "SELECT json(properties) FROM nodes WHERE id = ?1",
                params![node_id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(current_json) = current else {
            return Ok(false); // Node doesn't exist
        };

        // Parse, update, and serialize
        let mut properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&current_json)?;
        properties.insert(property.to_string(), value.clone());
        let new_json = serde_json::to_string(&properties)?;

        let affected = self.conn.execute(
            "UPDATE nodes SET properties = jsonb(?1) WHERE id = ?2",
            params![new_json, node_id],
        )?;
        Ok(affected > 0)
    }

    /// Add a label to a node.
    pub fn add_node_label(&self, node_id: i64, label: &str) -> Result<bool> {
        // Check if node exists
        let exists: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM nodes WHERE id = ?1",
                params![node_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        if !exists {
            return Ok(false);
        }

        let label_id = self.get_or_create_label(label)?;

        // Try to insert (ignore if already exists)
        self.conn.execute(
            "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            params![node_id, label_id],
        )?;
        Ok(true)
    }
}
