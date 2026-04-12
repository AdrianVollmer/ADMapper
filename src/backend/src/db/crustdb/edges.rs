//! Edge/relationship operations: insert and query.

use serde_json::Value as JsonValue;
use tracing::debug;

use super::super::types::{normalize_node_type, DbEdge, DbError, DbNode, Result};
use super::CrustDatabase;

impl CrustDatabase {
    /// Insert a batch of relationships using efficient batch insert.
    ///
    /// This builds an index of objectid -> node_id for efficient lookups,
    /// then uses CrustDB's native batch insert.
    pub fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize> {
        if relationships.is_empty() {
            return Ok(0);
        }

        // Build index of objectid -> node_id for efficient lookups
        let node_index = self.db.build_property_index("objectid")?;

        // Collect unique placeholder nodes to create (deduplicated by objectid)
        let mut placeholder_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for relationship in relationships {
            let source_id = node_index.get(&relationship.source);
            let target_id = node_index.get(&relationship.target);

            // Create placeholder for missing source
            if source_id.is_none() {
                placeholder_map
                    .entry(relationship.source.clone())
                    .or_insert_with(|| {
                        relationship
                            .source_type
                            .as_deref()
                            .map(normalize_node_type)
                            .unwrap_or_else(|| "Base".to_string())
                    });
            }
            // Create placeholder for missing target
            if target_id.is_none() {
                placeholder_map
                    .entry(relationship.target.clone())
                    .or_insert_with(|| {
                        relationship
                            .target_type
                            .as_deref()
                            .map(normalize_node_type)
                            .unwrap_or_else(|| "Base".to_string())
                    });
            }
        }

        // Upsert placeholder nodes (handles pre-existing nodes gracefully)
        let node_index = if !placeholder_map.is_empty() {
            debug!("Creating {} placeholder nodes", placeholder_map.len());

            let placeholder_batch: Vec<(Vec<String>, serde_json::Value)> = placeholder_map
                .iter()
                .map(|(objectid, node_type)| {
                    let labels = vec!["Base".to_string()];
                    let props = serde_json::json!({
                        "objectid": objectid,
                        "name": objectid,
                        "placeholder": true,
                        "node_type": node_type,
                    });
                    (labels, props)
                })
                .collect();

            self.db.upsert_nodes_batch(&placeholder_batch)?;
            debug!("Upserted {} placeholder nodes", placeholder_map.len());

            // Rebuild index after creating placeholders
            self.db.build_property_index("objectid")?
        } else {
            node_index
        };

        // Convert relationships to the format expected by CrustDB batch insert.
        // All DbEdge properties are stored as top-level CrustDB relationship properties,
        // matching how Neo4j stores them natively. No blob encoding.
        let mut batch: Vec<(i64, i64, String, serde_json::Value)> =
            Vec::with_capacity(relationships.len());
        let mut skipped = 0;

        for relationship in relationships {
            let source_id = node_index.get(&relationship.source);
            let target_id = node_index.get(&relationship.target);

            match (source_id, target_id) {
                (Some(&src), Some(&tgt)) => {
                    let props = match &relationship.properties {
                        serde_json::Value::Object(_) => relationship.properties.clone(),
                        _ => serde_json::json!({}),
                    };
                    batch.push((src, tgt, relationship.rel_type.clone(), props));
                }
                _ => {
                    debug!(
                        "Skipping relationship {} -> {}: source or target not found",
                        relationship.source, relationship.target
                    );
                    skipped += 1;
                }
            }
        }

        if batch.is_empty() {
            debug!("No valid relationships to insert (skipped {})", skipped);
            return Ok(0);
        }

        let ids = self.db.insert_relationships_batch(&batch)?;
        debug!(
            "Batch inserted {} relationships (skipped {})",
            ids.len(),
            skipped
        );
        Ok(ids.len())
    }

    /// Insert a single node.
    pub fn insert_node(&self, node: DbNode) -> Result<()> {
        self.insert_nodes(&[node])?;
        Ok(())
    }

    /// Insert a single relationship.
    pub fn insert_edge(&self, relationship: DbEdge) -> Result<()> {
        self.insert_edges(&[relationship])?;
        Ok(())
    }

    /// Extract relationship properties from a CrustDB result value.
    ///
    /// Handles two formats:
    /// - **New**: all properties stored directly as top-level CrustDB properties
    /// - **Legacy**: properties bundled in a `properties` blob string (old encoding)
    ///
    /// For the legacy format, the blob is parsed and any top-level properties
    /// (e.g. `exploit_likelihood` set via `SET r.exploit_likelihood`) take precedence.
    fn extract_rel_properties(
        properties: &std::collections::HashMap<String, crustdb::PropertyValue>,
    ) -> JsonValue {
        // Check for the legacy blob under the "properties" key.
        let blob_props = properties.get("properties").and_then(|v| {
            if let crustdb::PropertyValue::String(s) = v {
                serde_json::from_str::<JsonValue>(s).ok()
            } else {
                None
            }
        });

        if let Some(mut props) = blob_props {
            // Legacy format: start from blob, overlay any top-level properties.
            if let Some(obj) = props.as_object_mut() {
                for (k, v) in properties {
                    if k != "properties" {
                        obj.insert(k.clone(), Self::property_value_to_json(v));
                    }
                }
            }
            props
        } else {
            // New format: all properties are directly available.
            Self::properties_to_json(properties)
        }
    }

    /// Get all relationships.
    pub fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        let result =
            self.execute("MATCH (a)-[r]->(b) RETURN a.objectid, b.objectid, type(r), r")?;

        let mut relationships = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");

            let properties = match row.values.get("r") {
                Some(crustdb::ResultValue::Relationship { properties, .. }) => {
                    Self::extract_rel_properties(properties)
                }
                _ => serde_json::json!({}),
            };

            relationships.push(DbEdge {
                source,
                target,
                rel_type,
                properties,
                ..Default::default()
            });
        }

        Ok(relationships)
    }

    /// Get relationships between nodes.
    pub fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let id_list: Vec<String> = node_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_set = id_list.join(", ");

        let query = format!(
            "MATCH (a)-[r]->(b) \
             WHERE a.objectid IN [{}] AND b.objectid IN [{}] \
             RETURN a.objectid, b.objectid, type(r), r",
            id_set, id_set
        );

        let result = self.execute(&query)?;

        let mut relationships = Vec::new();
        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");

            let properties = match row.values.get("r") {
                Some(crustdb::ResultValue::Relationship { properties, .. }) => {
                    Self::extract_rel_properties(properties)
                }
                _ => serde_json::json!({}),
            };

            relationships.push(DbEdge {
                source,
                target,
                rel_type,
                properties,
                ..Default::default()
            });
        }

        Ok(relationships)
    }

    /// Get all distinct relationship types.
    ///
    /// Uses direct SQL query on the normalized rel_types table for O(distinct_types)
    /// performance instead of O(edges) full scan via Cypher.
    pub fn get_edge_types(&self) -> Result<Vec<String>> {
        // Use the optimized storage method that queries rel_types table directly
        // This is O(distinct_types) instead of O(edges)
        self.db
            .get_all_relationship_types()
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Get all distinct node labels (Cypher labels).
    ///
    /// Uses direct SQL query on the normalized node_labels table for O(distinct_labels)
    /// performance instead of O(nodes) full scan via Cypher.
    pub fn get_node_types(&self) -> Result<Vec<String>> {
        // Use the optimized storage method that queries node_labels table directly.
        // Filter out "Base" which is a generic super-label, not a real node type.
        self.db
            .get_all_labels()
            .map(|labels| labels.into_iter().filter(|l| l != "Base").collect())
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Get all relationships for a node (both incoming and outgoing) with relationship types.
    /// Used for efficient counting by the backend layer.
    /// Uses direct SQL for efficiency instead of Cypher queries.
    pub fn get_node_edges(&self, node_id: &str) -> Result<Vec<DbEdge>> {
        let raw_edges = self
            .db
            .get_node_relationships_by_objectid(node_id)
            .map_err(|e| DbError::Database(e.to_string()))?;

        let relationships = raw_edges
            .into_iter()
            .map(|(source, target, rel_type)| DbEdge {
                source,
                target,
                rel_type,
                properties: JsonValue::Null,
                ..Default::default()
            })
            .collect();

        Ok(relationships)
    }
}
