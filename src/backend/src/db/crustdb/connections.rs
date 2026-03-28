//! Node connection queries: incoming, outgoing, and Cypher-based.

use serde_json::Value as JsonValue;
use tracing::debug;

use super::super::types::{DbEdge, DbError, DbNode, Result};
use super::CrustDatabase;

impl CrustDatabase {
    /// Get node connections in a direction.
    ///
    /// For "incoming" and "outgoing" directions, uses direct SQL queries
    /// with the objectid index for O(degree) performance instead of O(N)
    /// full node scans. Other directions use Cypher queries.
    pub fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        debug!(node_id = %node_id, direction = %direction, "Getting node connections");

        // Use optimized SQL path for incoming/outgoing (the common case)
        match direction {
            "incoming" => {
                return self.get_node_connections_sql(node_id, true);
            }
            "outgoing" => {
                return self.get_node_connections_sql(node_id, false);
            }
            _ => {}
        }

        // For other directions (admin, memberof, members), use Cypher
        let escaped_id = node_id.replace('\'', "''");
        let query = match direction {
            "admin" => format!(
                "MATCH (a {{objectid: '{}'}})-[r]->(b) \
                 WHERE type(r) = 'AdminTo' OR type(r) = 'GenericAll' OR type(r) = 'GenericWrite' \
                 OR type(r) = 'Owns' OR type(r) = 'WriteDacl' OR type(r) = 'WriteOwner' \
                 OR type(r) = 'AllExtendedRights' OR type(r) = 'ForceChangePassword' \
                 OR type(r) = 'AddMember' \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            "memberof" => format!(
                "MATCH (a {{objectid: '{}'}})-[r:MemberOf]->(b) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            "members" => format!(
                "MATCH (a)-[r:MemberOf]->(b {{objectid: '{}'}}) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
            _ => format!(
                "MATCH (a)-[r]-(b {{objectid: '{}'}}) \
                 RETURN a.objectid, b.objectid, type(r), a, b",
                escaped_id
            ),
        };

        self.get_node_connections_cypher(&query, node_id)
    }

    /// Get node connections using optimized direct SQL.
    ///
    /// This bypasses Cypher parsing and uses indexed SQL queries for
    /// O(degree) performance instead of O(N) full node scans.
    fn get_node_connections_sql(
        &self,
        node_id: &str,
        incoming: bool,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        let (crust_nodes, crust_edges) = if incoming {
            self.db
                .get_incoming_connections_by_objectid(node_id)
                .map_err(|e| DbError::Database(e.to_string()))?
        } else {
            self.db
                .get_outgoing_connections_by_objectid(node_id)
                .map_err(|e| DbError::Database(e.to_string()))?
        };

        // Build map from internal node ID to objectid for relationship conversion
        let mut internal_to_objectid: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();

        // Convert crustdb::Node to DbNode and build ID map
        let nodes: Vec<DbNode> = crust_nodes
            .into_iter()
            .map(|n| {
                let objectid = n
                    .properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| n.id.to_string());

                // Store mapping from internal ID to objectid
                internal_to_objectid.insert(n.id, objectid.clone());

                let name = n
                    .properties
                    .get("name")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| objectid.clone());

                let label = n
                    .labels
                    .iter()
                    .find(|l| l.as_str() != "Base")
                    .cloned()
                    .or_else(|| n.labels.first().cloned())
                    .or_else(|| {
                        n.properties.get("node_type").and_then(|v| {
                            if let crustdb::PropertyValue::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .unwrap_or_else(|| "Unknown".to_string());
                let properties = Self::properties_to_json(&n.properties);

                DbNode {
                    id: objectid,
                    name,
                    label,
                    properties,
                }
            })
            .collect();

        // Convert crustdb::Relationship to DbEdge using the ID map
        let relationships: Vec<DbEdge> = crust_edges
            .into_iter()
            .filter_map(|e| {
                // Map internal IDs to objectids
                let source_obj_id = internal_to_objectid.get(&e.source)?;
                let target_obj_id = internal_to_objectid.get(&e.target)?;

                Some(DbEdge {
                    source: source_obj_id.clone(),
                    target: target_obj_id.clone(),
                    rel_type: e.rel_type,
                    properties: Self::properties_to_json(&e.properties),
                    ..Default::default()
                })
            })
            .collect();

        Ok((nodes, relationships))
    }

    /// Execute a Cypher query and extract node connections from the result.
    fn get_node_connections_cypher(
        &self,
        query: &str,
        node_id: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        let result = self.execute(query)?;

        let mut relationships = Vec::new();
        let mut nodes_map: std::collections::HashMap<String, DbNode> =
            std::collections::HashMap::new();

        for row in &result.rows {
            let source = self.get_string_value(&row.values, "a.objectid");
            let target = self.get_string_value(&row.values, "b.objectid");
            let rel_type = self.get_string_value(&row.values, "type(r)");

            relationships.push(DbEdge {
                source: source.clone(),
                target: target.clone(),
                rel_type,
                properties: JsonValue::Null,
                ..Default::default()
            });

            // Extract node info from the result using shared helper
            if let Some(node) = Self::extract_db_node_from_result(&row.values, "a") {
                nodes_map.entry(source.clone()).or_insert(node);
            }

            if let Some(node) = Self::extract_db_node_from_result(&row.values, "b") {
                nodes_map.entry(target.clone()).or_insert(node);
            }
        }

        // Always include the source node (matches Neo4j/FalkorDB behavior)
        if !nodes_map.contains_key(node_id) {
            if let Ok(source_nodes) = self.get_nodes_by_ids(&[node_id.to_string()]) {
                if let Some(source_node) = source_nodes.into_iter().next() {
                    nodes_map.insert(node_id.to_string(), source_node);
                }
            }
        }

        let nodes: Vec<DbNode> = nodes_map.into_values().collect();
        Ok((nodes, relationships))
    }
}
