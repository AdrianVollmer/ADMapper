//! Query operations for finding and counting nodes and relationships.

use crate::error::Result;
use crate::graph::{Node, PropertyValue, Relationship};
use rusqlite::{params, OptionalExtension};

use super::{validate_property_name, SqliteStorage};

impl SqliteStorage {
    /// Find a node ID by a property value.
    ///
    /// Searches for nodes where the JSON properties contain the specified key-value pair.
    /// Property names must contain only alphanumeric characters and underscores.
    /// Optimized path for objectid which uses a dedicated indexed column.
    pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>> {
        validate_property_name(property)?;

        // Use the dedicated objectid column for faster lookups
        let query = if property == "objectid" {
            "SELECT id FROM nodes WHERE objectid = ?1 LIMIT 1".to_string()
        } else {
            format!(
                "SELECT id FROM nodes WHERE json_extract(properties, '$.{}') = ?1 LIMIT 1",
                property
            )
        };
        let result: Option<i64> = self
            .conn
            .query_row(&query, params![value], |row| row.get(0))
            .optional()?;
        Ok(result)
    }

    /// Find nodes by property value with optional label filter.
    ///
    /// Uses indexed property lookup when available (via `create_property_index`).
    /// Property names must contain only alphanumeric characters and underscores.
    pub fn find_nodes_by_property(
        &self,
        property: &str,
        value: &serde_json::Value,
        labels: &[String],
        limit: Option<u64>,
    ) -> Result<Vec<Node>> {
        validate_property_name(property)?;

        // Convert JSON value to rusqlite Value with correct type for comparison
        let sql_value: rusqlite::types::Value = match value {
            serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    rusqlite::types::Value::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    rusqlite::types::Value::Real(f)
                } else {
                    return Ok(Vec::new());
                }
            }
            serde_json::Value::Bool(b) => {
                // SQLite stores booleans as integers (0/1)
                rusqlite::types::Value::Integer(if *b { 1 } else { 0 })
            }
            serde_json::Value::Null => rusqlite::types::Value::Null,
            _ => return Ok(Vec::new()), // Arrays/objects not supported for index lookup
        };

        let limit_clause = limit.map(|n| format!(" LIMIT {}", n)).unwrap_or_default();

        let sql = if labels.is_empty() {
            // No label filter - just property lookup
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM nodes n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 WHERE json_extract(n.properties, '$.{}') = ?1
                 GROUP BY n.id{}",
                property, limit_clause
            )
        } else {
            // With label filter - use subquery for efficiency
            let label_placeholders: Vec<String> =
                (2..=labels.len() + 1).map(|i| format!("?{}", i)).collect();
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(all_labels.name) as labels
                 FROM (
                     SELECT DISTINCT nodes.id, nodes.properties
                     FROM nodes
                     JOIN node_label_map nlm ON nodes.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE json_extract(nodes.properties, '$.{}') = ?1
                       AND nl.name IN ({})
                     {}
                 ) AS n
                 LEFT JOIN node_label_map nlm2 ON n.id = nlm2.node_id
                 LEFT JOIN node_labels all_labels ON nlm2.label_id = all_labels.id
                 GROUP BY n.id, n.properties",
                property,
                label_placeholders.join(", "),
                limit_clause
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;

        if labels.is_empty() {
            self.collect_nodes_from_stmt(&mut stmt, [&sql_value as &dyn rusqlite::ToSql])
        } else {
            // Build dynamic params: [sql_value, label1, label2, ...]
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            param_values.push(Box::new(sql_value));
            for label in labels {
                param_values.push(Box::new(label.clone()));
            }
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();
            self.collect_nodes_from_stmt(&mut stmt, params_refs.as_slice())
        }
    }

    /// Find nodes where a property ends with a given suffix.
    /// Used for pattern matching like `objectid ENDS WITH '-519'`.
    pub fn find_nodes_by_property_suffix(
        &self,
        property: &str,
        suffix: &str,
        labels: &[String],
    ) -> Result<Vec<Node>> {
        self.find_nodes_by_property_pattern(property, &format!("%{}", suffix), labels)
    }

    /// Find nodes where a property starts with a given prefix.
    /// Used for pattern matching like `objectid STARTS WITH 'S-1-5'`.
    pub fn find_nodes_by_property_prefix(
        &self,
        property: &str,
        prefix: &str,
        labels: &[String],
    ) -> Result<Vec<Node>> {
        self.find_nodes_by_property_pattern(property, &format!("{}%", prefix), labels)
    }

    /// Find nodes where a property contains a given substring.
    /// Used for pattern matching like `name CONTAINS 'admin'`.
    pub fn find_nodes_by_property_contains(
        &self,
        property: &str,
        substring: &str,
        labels: &[String],
    ) -> Result<Vec<Node>> {
        self.find_nodes_by_property_pattern(property, &format!("%{}%", substring), labels)
    }

    /// Internal helper for LIKE-based property pattern matching.
    fn find_nodes_by_property_pattern(
        &self,
        property: &str,
        pattern: &str,
        labels: &[String],
    ) -> Result<Vec<Node>> {
        validate_property_name(property)?;

        let sql = if labels.is_empty() {
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM nodes n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 WHERE json_extract(n.properties, '$.{}') LIKE ?1
                 GROUP BY n.id",
                property
            )
        } else {
            let label_placeholders: Vec<String> =
                (2..=labels.len() + 1).map(|i| format!("?{}", i)).collect();
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(all_labels.name) as labels
                 FROM (
                     SELECT DISTINCT nodes.id, nodes.properties
                     FROM nodes
                     JOIN node_label_map nlm ON nodes.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE json_extract(nodes.properties, '$.{}') LIKE ?1
                       AND nl.name IN ({})
                 ) AS n
                 LEFT JOIN node_label_map nlm2 ON n.id = nlm2.node_id
                 LEFT JOIN node_labels all_labels ON nlm2.label_id = all_labels.id
                 GROUP BY n.id, n.properties",
                property,
                label_placeholders.join(", ")
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;

        if labels.is_empty() {
            self.collect_nodes_from_stmt(&mut stmt, [&pattern as &dyn rusqlite::ToSql])
        } else {
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            param_values.push(Box::new(pattern.to_string()));
            for label in labels {
                param_values.push(Box::new(label.clone()));
            }
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();
            self.collect_nodes_from_stmt(&mut stmt, params_refs.as_slice())
        }
    }

    /// Build an index of property values to node IDs for efficient batch lookups.
    ///
    /// Returns a HashMap from property value to node ID.
    /// Property names must contain only alphanumeric characters and underscores.
    /// Optimized path for objectid which uses a dedicated indexed column.
    pub fn build_property_index(
        &self,
        property: &str,
    ) -> Result<std::collections::HashMap<String, i64>> {
        validate_property_name(property)?;

        // Use the dedicated objectid column for faster lookups
        let query = if property == "objectid" {
            "SELECT id, objectid FROM nodes WHERE objectid IS NOT NULL".to_string()
        } else {
            format!(
                "SELECT id, json_extract(properties, '$.{}') FROM nodes WHERE json_extract(properties, '$.{}') IS NOT NULL",
                property, property
            )
        };
        let mut stmt = self.conn.prepare(&query)?;
        let mut index = std::collections::HashMap::new();

        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let value: String = row.get(1)?;
            Ok((id, value))
        })?;

        for row in rows {
            let (id, value) = row?;
            index.insert(value, id);
        }

        Ok(index)
    }

    /// Scan all nodes in the database.
    pub fn scan_all_nodes(&self) -> Result<Vec<Node>> {
        self.get_all_nodes_limit(None)
    }

    /// Find nodes by label.
    pub fn find_nodes_by_label(&self, label: &str) -> Result<Vec<Node>> {
        self.find_nodes_by_label_limit(label, None)
    }

    /// Find relationships by type.
    #[allow(dead_code)]
    pub fn find_relationships_by_type(&self, rel_type: &str) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT r.id, r.source_id, r.target_id, rt.name, json(r.properties)
             FROM relationships r
             JOIN rel_types rt ON r.type_id = rt.id
             WHERE rt.name = ?1",
        )?;

        self.collect_relationships_from_stmt(&mut stmt, params![rel_type])
    }

    /// Scan all relationships in the database.
    pub fn scan_all_relationships(&self) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT r.id, r.source_id, r.target_id, rt.name, json(r.properties)
             FROM relationships r
             JOIN rel_types rt ON r.type_id = rt.id",
        )?;

        self.collect_relationships_from_stmt(&mut stmt, [])
    }

    /// Helper: collect relationships from a prepared statement that returns
    /// (id, source_id, target_id, rel_type, properties).
    pub(crate) fn collect_relationships_from_stmt<P: rusqlite::Params>(
        &self,
        stmt: &mut rusqlite::Statement,
        params: P,
    ) -> Result<Vec<Relationship>> {
        let rows = stmt.query_map(params, |row| {
            let id: i64 = row.get(0)?;
            let source: i64 = row.get(1)?;
            let target: i64 = row.get(2)?;
            let rel_type: String = row.get(3)?;
            let properties_json: String = row.get(4)?;
            Ok((id, source, target, rel_type, properties_json))
        })?;

        let mut relationships = Vec::new();
        for row_result in rows {
            let (id, source, target, rel_type, properties_json) = row_result?;

            let properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&properties_json)?;

            relationships.push(Relationship {
                id,
                source,
                target,
                rel_type,
                properties,
            });
        }

        Ok(relationships)
    }

    /// Count all nodes.
    pub fn count_nodes(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Count nodes with a specific label.
    pub fn count_nodes_by_label(&self, label: &str) -> Result<u64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM nodes n
             JOIN node_label_map nlm ON n.id = nlm.node_id
             JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE nl.name = ?1",
            params![label],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Count all relationships.
    pub fn count_relationships(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Count relationships with a specific type.
    pub fn count_relationships_by_type(&self, rel_type: &str) -> Result<u64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships r
             JOIN rel_types rt ON r.type_id = rt.id
             WHERE rt.name = ?1",
            params![rel_type],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Find nodes by label with optional limit.
    pub fn find_nodes_by_label_limit(&self, label: &str, limit: Option<u64>) -> Result<Vec<Node>> {
        // Use subquery to limit nodes BEFORE joining for all labels
        // This ensures we only process N nodes instead of all matching nodes
        // Use json() to convert JSONB to text for deserialization
        let sql = match limit {
            Some(n) => format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM (
                     SELECT DISTINCT nodes.id, nodes.properties
                     FROM nodes
                     JOIN node_label_map nlm ON nodes.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE nl.name = ?1
                     LIMIT {}
                 ) AS n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 GROUP BY n.id, n.properties",
                n
            ),
            None => "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                     FROM nodes n
                     JOIN node_label_map nlm ON n.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE n.id IN (
                         SELECT DISTINCT nlm2.node_id FROM node_label_map nlm2
                         JOIN node_labels nl2 ON nlm2.label_id = nl2.id
                         WHERE nl2.name = ?1
                     )
                     GROUP BY n.id"
                .to_string(),
        };

        let mut stmt = self.conn.prepare(&sql)?;
        self.collect_nodes_from_stmt(&mut stmt, params![label])
    }

    /// Get all nodes with optional limit.
    pub fn get_all_nodes_limit(&self, limit: Option<u64>) -> Result<Vec<Node>> {
        // Use subquery to limit nodes BEFORE joining for labels
        // This ensures we only process N nodes instead of all nodes
        // Use json() to convert JSONB to text for deserialization
        let sql = match limit {
            Some(n) => format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM (SELECT id, properties FROM nodes LIMIT {}) AS n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 GROUP BY n.id, n.properties",
                n
            ),
            None => "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                     FROM nodes n
                     LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                     LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                     GROUP BY n.id"
                .to_string(),
        };

        let mut stmt = self.conn.prepare(&sql)?;
        self.collect_nodes_from_stmt(&mut stmt, [])
    }

    /// Helper: collect nodes from a prepared statement that returns (id, properties, labels).
    pub(crate) fn collect_nodes_from_stmt<P: rusqlite::Params>(
        &self,
        stmt: &mut rusqlite::Statement,
        params: P,
    ) -> Result<Vec<Node>> {
        let rows = stmt.query_map(params, |row| {
            let id: i64 = row.get(0)?;
            let properties_json: String = row.get(1)?;
            let labels_concat: Option<String> = row.get(2)?;
            Ok((id, properties_json, labels_concat))
        })?;

        let mut nodes = Vec::new();
        for row_result in rows {
            let (id, properties_json, labels_concat) = row_result?;

            let properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&properties_json)?;

            let labels: Vec<String> = labels_concat
                .map(|s| s.split(',').map(|l| l.to_string()).collect())
                .unwrap_or_default();

            nodes.push(Node {
                id,
                labels,
                properties,
            });
        }

        Ok(nodes)
    }

    /// Find outgoing relationships from a node.
    pub fn find_outgoing_relationships(&self, node_id: i64) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT r.id, r.source_id, r.target_id, rt.name, json(r.properties)
             FROM relationships r
             JOIN rel_types rt ON r.type_id = rt.id
             WHERE r.source_id = ?1",
        )?;

        self.collect_relationships_from_stmt(&mut stmt, params![node_id])
    }

    /// Find incoming relationships to a node.
    pub fn find_incoming_relationships(&self, node_id: i64) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT r.id, r.source_id, r.target_id, rt.name, json(r.properties)
             FROM relationships r
             JOIN rel_types rt ON r.type_id = rt.id
             WHERE r.target_id = ?1",
        )?;

        self.collect_relationships_from_stmt(&mut stmt, params![node_id])
    }

    /// Count outgoing relationships from a node.
    #[allow(dead_code)]
    pub fn count_outgoing_relationships(&self, node_id: i64) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE source_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count incoming relationships to a node.
    #[allow(dead_code)]
    pub fn count_incoming_relationships(&self, node_id: i64) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE target_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count incoming relationships to a node by objectid.
    /// Uses the dedicated objectid column for efficient indexed lookup.
    #[allow(dead_code)]
    pub fn count_incoming_relationships_by_objectid(&self, objectid: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships r \
             JOIN nodes n ON r.target_id = n.id \
             WHERE n.objectid = ?1",
            params![objectid],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count outgoing relationships from a node by objectid.
    /// Uses the dedicated objectid column for efficient indexed lookup.
    #[allow(dead_code)]
    pub fn count_outgoing_relationships_by_objectid(&self, objectid: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships r \
             JOIN nodes n ON r.source_id = n.id \
             WHERE n.objectid = ?1",
            params![objectid],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Get all relationships for a node by objectid (both incoming and outgoing).
    /// Returns (source_objectid, target_objectid, rel_type) tuples.
    /// Uses the dedicated objectid column for efficient indexed lookup.
    pub fn get_node_relationships_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let mut relationships = Vec::new();

        // Query for relationships where node is source or target, using dedicated objectid column
        let mut stmt = self.conn.prepare_cached(
            "SELECT
                src.objectid AS src_id,
                tgt.objectid AS tgt_id,
                et.name AS rel_type
             FROM relationships e
             JOIN nodes src ON e.source_id = src.id
             JOIN nodes tgt ON e.target_id = tgt.id
             JOIN rel_types et ON e.type_id = et.id
             WHERE src.objectid = ?1
                OR tgt.objectid = ?1",
        )?;

        let rows = stmt.query_map(params![objectid], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        for row in rows {
            relationships.push(row?);
        }

        Ok(relationships)
    }

    /// Get incoming connections to a node by objectid.
    ///
    /// Returns all nodes that have relationships pointing TO the specified node,
    /// along with those relationships. This uses direct SQL with the objectid index
    /// for optimal performance, avoiding full node scans.
    ///
    /// Returns (Vec<Node>, Vec<Relationship>) where nodes are the source nodes of
    /// incoming relationships, and relationships are the relationships.
    pub fn get_incoming_connections_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        self.get_connections_by_objectid(objectid, true)
    }

    /// Get outgoing connections from a node by objectid.
    ///
    /// Returns all nodes that the specified node has relationships pointing TO,
    /// along with those relationships. This uses direct SQL with the objectid index
    /// for optimal performance.
    ///
    /// Returns (Vec<Node>, Vec<Relationship>) where nodes are the target nodes of
    /// outgoing relationships, and relationships are the relationships.
    pub fn get_outgoing_connections_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        self.get_connections_by_objectid(objectid, false)
    }

    /// Shared implementation for incoming/outgoing connection queries.
    ///
    /// When `incoming` is true, finds relationships pointing TO the node (by objectid)
    /// and returns source nodes. When false, finds relationships pointing FROM the node
    /// and returns target nodes.
    fn get_connections_by_objectid(
        &self,
        objectid: &str,
        incoming: bool,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        // First find the node's internal ID using the dedicated objectid column
        let anchor_id: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM nodes WHERE objectid = ?1",
                params![objectid],
                |row| row.get(0),
            )
            .optional()?;

        let Some(anchor_id) = anchor_id else {
            return Ok((Vec::new(), Vec::new()));
        };

        // For incoming: join on source (the "other" node), filter on target = anchor
        // For outgoing: join on target (the "other" node), filter on source = anchor
        let (join_column, where_column) = if incoming {
            ("source_id", "target_id")
        } else {
            ("target_id", "source_id")
        };

        let sql = format!(
            "SELECT
                e.id AS rel_id,
                e.source_id,
                e.target_id,
                et.name AS rel_type,
                json(e.properties) AS edge_props,
                other.id AS other_node_id,
                json(other.properties) AS other_props,
                GROUP_CONCAT(DISTINCT nl.name) AS other_labels
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             JOIN nodes other ON e.{} = other.id
             LEFT JOIN node_label_map nlm ON other.id = nlm.node_id
             LEFT JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE e.{} = ?1
             GROUP BY e.id, other.id",
            join_column, where_column
        );

        let mut stmt = self.conn.prepare_cached(&sql)?;

        let mut nodes_map: std::collections::HashMap<i64, Node> = std::collections::HashMap::new();
        let mut relationships = Vec::new();

        let rows = stmt.query_map(params![anchor_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,            // rel_id
                row.get::<_, i64>(1)?,            // source_id
                row.get::<_, i64>(2)?,            // target_id
                row.get::<_, String>(3)?,         // rel_type
                row.get::<_, String>(4)?,         // edge_props
                row.get::<_, i64>(5)?,            // other_node_id
                row.get::<_, String>(6)?,         // other_props
                row.get::<_, Option<String>>(7)?, // other_labels
            ))
        })?;

        for row_result in rows {
            let (
                rel_id,
                source_id,
                target_id,
                rel_type,
                edge_props,
                other_node_id,
                other_props,
                other_labels,
            ) = row_result?;

            // Add relationship
            let edge_properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&edge_props)?;
            relationships.push(Relationship {
                id: rel_id,
                source: source_id,
                target: target_id,
                rel_type,
                properties: edge_properties,
            });

            // Add the other node if not already present
            if let std::collections::hash_map::Entry::Vacant(e) = nodes_map.entry(other_node_id) {
                let properties: std::collections::HashMap<String, PropertyValue> =
                    serde_json::from_str(&other_props)?;
                let labels: Vec<String> = other_labels
                    .map(|s| s.split(',').map(|l| l.to_string()).collect())
                    .unwrap_or_default();
                e.insert(Node {
                    id: other_node_id,
                    labels,
                    properties,
                });
            }
        }

        // Also fetch and add the anchor node itself
        if let Some(anchor_node) = self.get_node(anchor_id)? {
            nodes_map.insert(anchor_id, anchor_node);
        }

        Ok((nodes_map.into_values().collect(), relationships))
    }

    /// Get all node labels.
    pub fn get_all_labels(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM node_labels ORDER BY name")?;
        let labels: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(labels)
    }

    /// Get all relationship types.
    pub fn get_all_relationship_types(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM rel_types ORDER BY name")?;
        let types: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(types)
    }

    /// Find outgoing relationships from a node by objectid.
    ///
    /// Returns `(target_objectid, rel_type)` tuples for all outgoing relationships.
    /// This is optimized for BFS traversal where we only need neighbor identifiers,
    /// not full node/relationship objects.
    ///
    /// Uses the dedicated objectid column index for O(1) node lookup,
    /// then O(degree) for relationship retrieval.
    pub fn find_outgoing_relationships_by_objectid(
        &self,
        objectid: &str,
    ) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT tgt.objectid, rt.name
             FROM relationships r
             JOIN nodes src ON r.source_id = src.id
             JOIN nodes tgt ON r.target_id = tgt.id
             JOIN rel_types rt ON r.type_id = rt.id
             WHERE src.objectid = ?1",
        )?;

        let rows = stmt.query_map(params![objectid], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut rels = Vec::new();
        for row in rows {
            rels.push(row?);
        }

        Ok(rels)
    }

    /// Get counts for all node labels in a single query.
    /// Returns a HashMap of label name to count.
    pub fn get_label_counts(&self) -> Result<std::collections::HashMap<String, usize>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT nl.name, COUNT(*) as cnt
             FROM node_labels nl
             JOIN node_label_map nlm ON nl.id = nlm.label_id
             GROUP BY nl.id, nl.name",
        )?;

        let mut counts = std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, usize>(1)?))
        })?;

        for row in rows {
            let (label, count) = row?;
            counts.insert(label, count);
        }

        Ok(counts)
    }
}
