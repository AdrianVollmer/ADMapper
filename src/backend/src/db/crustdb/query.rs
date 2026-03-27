//! Custom query execution and result conversion helpers.

use serde_json::Value as JsonValue;
use tracing::debug;

use super::super::types::Result;
use super::CrustDatabase;

impl CrustDatabase {
    /// Run a custom Cypher query.
    pub fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        debug!(query = %query, "Running custom Cypher query");

        let result = self.execute(query)?;

        let headers: Vec<String> = result.columns.clone();
        let rows: Vec<Vec<JsonValue>> = result
            .rows
            .iter()
            .map(|row| {
                headers
                    .iter()
                    .map(|col| {
                        row.values
                            .get(col)
                            .map(Self::result_value_to_json)
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect()
            })
            .collect();

        Ok(serde_json::json!({
            "headers": headers,
            "rows": rows
        }))
    }

    /// Convert a CrustDB ResultValue to JSON.
    pub(crate) fn result_value_to_json(v: &crustdb::ResultValue) -> JsonValue {
        match v {
            crustdb::ResultValue::Property(pv) => Self::property_value_to_json(pv),
            crustdb::ResultValue::Node {
                id,
                labels,
                properties,
            } => {
                let props: serde_json::Map<String, JsonValue> = properties
                    .iter()
                    .map(|(k, pv)| (k.clone(), Self::property_value_to_json(pv)))
                    .collect();
                // Get objectid from properties if available
                let objectid = properties
                    .get("objectid")
                    .and_then(|v| {
                        if let crustdb::PropertyValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| id.to_string());
                serde_json::json!({
                    "_type": "node",
                    "id": id,
                    "objectid": objectid,
                    "labels": labels,
                    "properties": props
                })
            }
            crustdb::ResultValue::Relationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let props: serde_json::Map<String, JsonValue> = properties
                    .iter()
                    .map(|(k, pv)| (k.clone(), Self::property_value_to_json(pv)))
                    .collect();
                serde_json::json!({
                    "_type": "relationship",
                    "id": id,
                    "source": source,
                    "target": target,
                    "rel_type": rel_type,
                    "properties": props
                })
            }
            crustdb::ResultValue::Path {
                nodes,
                relationships,
            } => {
                serde_json::json!({
                    "_type": "path",
                    "nodes": nodes,
                    "relationships": relationships
                })
            }
        }
    }

    /// Convert a CrustDB PropertyValue to JSON.
    pub(crate) fn property_value_to_json(pv: &crustdb::PropertyValue) -> JsonValue {
        match pv {
            crustdb::PropertyValue::String(s) => JsonValue::String(s.clone()),
            crustdb::PropertyValue::Integer(n) => JsonValue::Number((*n).into()),
            crustdb::PropertyValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            crustdb::PropertyValue::Bool(b) => JsonValue::Bool(*b),
            crustdb::PropertyValue::Null => JsonValue::Null,
            crustdb::PropertyValue::List(items) => {
                JsonValue::Array(items.iter().map(Self::property_value_to_json).collect())
            }
            crustdb::PropertyValue::Map(map) => {
                let obj: serde_json::Map<String, JsonValue> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::property_value_to_json(v)))
                    .collect();
                JsonValue::Object(obj)
            }
            crustdb::PropertyValue::Node(n) => {
                let mut obj = serde_json::Map::new();
                obj.insert("id".into(), JsonValue::Number(n.id.into()));
                obj.insert(
                    "labels".into(),
                    JsonValue::Array(
                        n.labels.iter().map(|l| JsonValue::String(l.clone())).collect(),
                    ),
                );
                obj.insert(
                    "properties".into(),
                    Self::property_value_to_json(&crustdb::PropertyValue::Map(n.properties.clone())),
                );
                JsonValue::Object(obj)
            }
            crustdb::PropertyValue::Relationship(r) => {
                let mut obj = serde_json::Map::new();
                obj.insert("id".into(), JsonValue::Number(r.id.into()));
                obj.insert("source".into(), JsonValue::Number(r.source.into()));
                obj.insert("target".into(), JsonValue::Number(r.target.into()));
                obj.insert("rel_type".into(), JsonValue::String(r.rel_type.clone()));
                obj.insert(
                    "properties".into(),
                    Self::property_value_to_json(&crustdb::PropertyValue::Map(r.properties.clone())),
                );
                JsonValue::Object(obj)
            }
            crustdb::PropertyValue::Path(p) => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "nodes".into(),
                    JsonValue::Array(
                        p.nodes
                            .iter()
                            .map(|n| Self::property_value_to_json(&crustdb::PropertyValue::Node(n.clone())))
                            .collect(),
                    ),
                );
                obj.insert(
                    "relationships".into(),
                    JsonValue::Array(
                        p.relationships
                            .iter()
                            .map(|r| {
                                Self::property_value_to_json(&crustdb::PropertyValue::Relationship(r.clone()))
                            })
                            .collect(),
                    ),
                );
                JsonValue::Object(obj)
            }
        }
    }

    /// Convert CrustDB properties to JSON.
    pub(crate) fn properties_to_json(
        properties: &std::collections::HashMap<String, crustdb::PropertyValue>,
    ) -> JsonValue {
        let map: serde_json::Map<String, JsonValue> = properties
            .iter()
            .map(|(k, v)| (k.clone(), Self::property_value_to_json(v)))
            .collect();
        JsonValue::Object(map)
    }
}
