use crate::error::{Error, Result};
use crate::graph::PropertyValue;
use crate::query::planner::{PlanExpr, PlanLiteral};
use crate::query::{PathNode, PathRelationship, ResultValue};

/// Convert a PropertyValue to a ResultValue for query output.
pub(super) fn eval_to_result_value(v: PropertyValue) -> ResultValue {
    match v {
        PropertyValue::Node(n) => ResultValue::Node {
            id: n.id,
            labels: n.labels,
            properties: n.properties,
        },
        PropertyValue::Relationship(e) => ResultValue::Relationship {
            id: e.id,
            source: e.source,
            target: e.target,
            rel_type: e.rel_type,
            properties: e.properties,
        },
        PropertyValue::Path(p) => ResultValue::Path {
            nodes: p
                .nodes
                .into_iter()
                .map(|n| PathNode {
                    id: n.id,
                    labels: n.labels,
                    properties: n.properties,
                })
                .collect(),
            relationships: p
                .relationships
                .into_iter()
                .map(|e| PathRelationship {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    rel_type: e.rel_type,
                    properties: e.properties,
                })
                .collect(),
        },
        other => ResultValue::Property(other),
    }
}

pub(super) fn plan_properties_to_json(props: &[(String, PlanExpr)]) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, expr) in props {
        let value = match expr {
            PlanExpr::Literal(lit) => match lit {
                PlanLiteral::Null => serde_json::Value::Null,
                PlanLiteral::Bool(b) => serde_json::Value::Bool(*b),
                PlanLiteral::Int(i) => serde_json::Value::Number((*i).into()),
                PlanLiteral::Float(f) => serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                PlanLiteral::String(s) => serde_json::Value::String(s.clone()),
            },
            _ => {
                return Err(Error::Cypher(
                    "Only literal values supported in CREATE properties".into(),
                ))
            }
        };
        map.insert(key.clone(), value);
    }
    Ok(serde_json::Value::Object(map))
}
