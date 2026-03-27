//! Node operations: insert, query, search, and resolve.

use tracing::debug;

use super::super::types::{DbNode, Result};
use super::CrustDatabase;

impl CrustDatabase {
    /// Insert a batch of nodes using efficient batch upsert.
    ///
    /// This uses CrustDB's native batch upsert which wraps all upserts
    /// in a single transaction with prepared statements.
    ///
    /// If a node with the same objectid already exists (e.g., an orphan placeholder
    /// created during relationship insertion), its properties are **merged** rather than
    /// replaced. This enables streaming relationship import.
    pub fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        if nodes.is_empty() {
            return Ok(0);
        }

        // Convert DbNodes to the format expected by CrustDB batch upsert.
        // Every node gets a "Base" label in addition to its type-specific
        // label, matching Neo4j/FalkorDB conventions.
        let batch: Vec<(Vec<String>, serde_json::Value)> = nodes
            .iter()
            .map(|node| {
                let labels = if node.label == "Base" {
                    vec!["Base".to_string()]
                } else {
                    vec![node.label.clone(), "Base".to_string()]
                };
                // Flatten BloodHound properties into top-level fields
                let props = Self::flatten_node_properties(node);
                (labels, props)
            })
            .collect();

        // Use upsert to merge with any existing placeholder nodes
        match self.db.upsert_nodes_batch(&batch) {
            Ok(ids) => {
                debug!("Batch upserted {} nodes", ids.len());
                Ok(ids.len())
            }
            Err(e) => {
                debug!(
                    "Batch upsert failed, falling back to individual inserts: {}",
                    e
                );
                // Fallback to individual inserts if batch fails
                self.insert_nodes_fallback(nodes)
            }
        }
    }

    /// Fallback method for individual node inserts (used if batch fails).
    fn insert_nodes_fallback(&self, nodes: &[DbNode]) -> Result<usize> {
        let mut count = 0;
        for node in nodes {
            // Build flattened properties for Cypher
            let props = Self::flatten_node_properties(node);
            let props_str = Self::json_to_cypher_props(&props);
            let cypher_label = node.label.replace('\'', "''");

            // Add :Base as a secondary label (matching Neo4j/FalkorDB)
            let label_clause = if cypher_label == "Base" {
                "Base".to_string()
            } else {
                format!("{}:Base", cypher_label)
            };
            let query = format!("CREATE (n:{} {})", label_clause, props_str);

            if self.execute(&query).is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Flatten BloodHound node properties into a single JSON object.
    ///
    /// This merges the nested `properties` from BloodHound into top-level fields,
    /// making them directly queryable in Cypher.
    pub(crate) fn flatten_node_properties(node: &DbNode) -> serde_json::Value {
        let mut props = serde_json::Map::new();

        // Add core identifiers
        props.insert("objectid".to_string(), serde_json::json!(node.id));
        props.insert("name".to_string(), serde_json::json!(node.name));
        props.insert("label".to_string(), serde_json::json!(node.label));

        // Flatten BloodHound properties into top-level fields
        if let serde_json::Value::Object(bh_props) = &node.properties {
            for (key, value) in bh_props {
                // Skip null values and empty arrays to save space
                if value.is_null() {
                    continue;
                }
                if let Some(arr) = value.as_array() {
                    if arr.is_empty() {
                        continue;
                    }
                }
                // Don't overwrite core fields
                if key != "objectid" && key != "name" && key != "label" {
                    props.insert(key.clone(), value.clone());
                }
            }
        }

        serde_json::Value::Object(props)
    }

    /// Convert a JSON object to Cypher property syntax.
    pub(crate) fn json_to_cypher_props(value: &serde_json::Value) -> String {
        let obj = match value.as_object() {
            Some(o) => o,
            None => return "{}".to_string(),
        };

        let pairs: Vec<String> = obj
            .iter()
            .filter_map(|(k, v)| {
                let val_str = Self::json_value_to_cypher(v)?;
                Some(format!("{}: {}", k, val_str))
            })
            .collect();

        format!("{{{}}}", pairs.join(", "))
    }

    /// Convert a JSON value to Cypher literal syntax.
    pub(crate) fn json_value_to_cypher(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Null => None,
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            serde_json::Value::Array(arr) => {
                let items: Vec<String> =
                    arr.iter().filter_map(Self::json_value_to_cypher).collect();
                Some(format!("[{}]", items.join(", ")))
            }
            serde_json::Value::Object(_) => {
                // Skip nested objects for now - Cypher doesn't support them directly
                None
            }
        }
    }

    /// Get all nodes.
    pub fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        let result = self.execute("MATCH (n) RETURN n")?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Extract a DbNode from a query result row.
    pub(crate) fn extract_db_node_from_result(
        values: &std::collections::HashMap<String, crustdb::ResultValue>,
        key: &str,
    ) -> Option<DbNode> {
        let value = values.get(key)?;

        match value {
            crustdb::ResultValue::Node {
                id: _,
                labels,
                properties,
            } => {
                let objectid = properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let name = properties
                    .get("name")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| objectid.clone());

                // Get node type: prefer Cypher labels (excluding "Base" which is a
                // generic super-label), then node_type property
                let label = labels
                    .iter()
                    .find(|l| *l != "Base")
                    .cloned()
                    .or_else(|| labels.first().cloned())
                    .or_else(|| {
                        properties.get("node_type").and_then(|v| {
                            if let crustdb::PropertyValue::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .unwrap_or_else(|| "Unknown".to_string());

                // Convert all properties to JSON
                let props_json = Self::properties_to_json(properties);

                Some(DbNode {
                    id: objectid,
                    name,
                    label,
                    properties: props_json,
                })
            }
            _ => None,
        }
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

        // Return full node to get all flattened properties
        let query = format!(
            "MATCH (n) WHERE n.objectid IN [{}] RETURN n",
            id_list.join(", ")
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Search nodes by name (case-insensitive substring match).
    pub fn search_nodes(&self, search_query: &str, limit: usize) -> Result<Vec<DbNode>> {
        let query_escaped = search_query.replace('\'', "''").to_lowercase();

        // CrustDB supports CONTAINS for string matching
        // Use toLower() for case-insensitive search
        // Search both n.name (BloodHound property) and n.objectid
        let query = format!(
            "MATCH (n) WHERE toLower(n.name) CONTAINS '{}' OR toLower(n.objectid) CONTAINS '{}' \
             RETURN n LIMIT {}",
            query_escaped, query_escaped, limit
        );

        let result = self.execute(&query)?;

        let mut nodes = Vec::new();
        for row in &result.rows {
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "n") {
                nodes.push(node);
            }
        }

        debug!(query = %search_query, found = nodes.len(), "Search complete");
        Ok(nodes)
    }

    /// Resolve a node identifier to an object ID.
    pub fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        let id_escaped = identifier.replace('\'', "''");

        // Try exact objectid match
        let query = format!(
            "MATCH (n {{objectid: '{}'}}) RETURN n.objectid LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.objectid"),
                ));
            }
        }

        // Try name match
        let query = format!(
            "MATCH (n) WHERE n.name = '{}' RETURN n.objectid LIMIT 1",
            id_escaped
        );
        if let Ok(result) = self.execute(&query) {
            if !result.rows.is_empty() {
                return Ok(Some(
                    self.get_string_value(&result.rows[0].values, "n.objectid"),
                ));
            }
        }

        Ok(None)
    }
}
