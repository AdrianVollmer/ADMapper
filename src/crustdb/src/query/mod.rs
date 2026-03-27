//! Cypher query parsing and execution.

pub mod ast;
pub mod executor;
pub mod operators;
pub mod parser;
pub mod planner;

use crate::graph::PropertyValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Result of a Cypher query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Result rows.
    pub rows: Vec<Row>,
    /// Execution statistics.
    pub stats: QueryStats,
}

impl QueryResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            stats: QueryStats::default(),
        }
    }

    /// Check if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// A single row in the query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Values in this row, keyed by column name.
    pub values: HashMap<String, ResultValue>,
}

impl Row {
    /// Get a value by column name.
    pub fn get(&self, column: &str) -> Option<&ResultValue> {
        self.values.get(column)
    }
}

/// A node in a path result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathNode {
    pub id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, PropertyValue>,
}

impl Hash for PathNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.labels.hash(state);
        hash_property_map(&self.properties, state);
    }
}

/// A relationship in a path result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRelationship {
    pub id: i64,
    pub source: i64,
    pub target: i64,
    pub rel_type: String,
    pub properties: HashMap<String, PropertyValue>,
}

impl Hash for PathRelationship {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.source.hash(state);
        self.target.hash(state);
        self.rel_type.hash(state);
        hash_property_map(&self.properties, state);
    }
}

/// A value in a query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ResultValue {
    /// A property value.
    Property(PropertyValue),
    /// A node.
    Node {
        id: i64,
        labels: Vec<String>,
        properties: HashMap<String, PropertyValue>,
    },
    /// An relationship.
    Relationship {
        id: i64,
        source: i64,
        target: i64,
        rel_type: String,
        properties: HashMap<String, PropertyValue>,
    },
    /// A path (sequence of nodes and relationships) with full data.
    Path {
        nodes: Vec<PathNode>,
        relationships: Vec<PathRelationship>,
    },
}

impl Hash for ResultValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ResultValue::Property(pv) => pv.hash(state),
            ResultValue::Node {
                id,
                labels,
                properties,
            } => {
                id.hash(state);
                labels.hash(state);
                hash_property_map(properties, state);
            }
            ResultValue::Relationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                id.hash(state);
                source.hash(state);
                target.hash(state);
                rel_type.hash(state);
                hash_property_map(properties, state);
            }
            ResultValue::Path {
                nodes,
                relationships,
            } => {
                nodes.hash(state);
                relationships.hash(state);
            }
        }
    }
}

/// Hash a HashMap<String, PropertyValue> in a stable (sorted-key) order.
fn hash_property_map<H: Hasher>(map: &HashMap<String, PropertyValue>, state: &mut H) {
    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    for (k, v) in pairs {
        k.hash(state);
        v.hash(state);
    }
}

/// Query execution statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    /// Number of nodes created.
    pub nodes_created: usize,
    /// Number of nodes deleted.
    pub nodes_deleted: usize,
    /// Number of relationships created.
    pub relationships_created: usize,
    /// Number of relationships deleted.
    pub relationships_deleted: usize,
    /// Number of properties set.
    pub properties_set: usize,
    /// Number of labels added.
    pub labels_added: usize,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}
