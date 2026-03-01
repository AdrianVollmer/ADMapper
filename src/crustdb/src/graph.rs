//! Graph data structures: nodes, relationships, and properties.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Property value types supported by the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Integer value (stored as i64).
    Integer(i64),
    /// Floating point value.
    Float(f64),
    /// String value.
    String(String),
    /// List of values.
    List(Vec<PropertyValue>),
    /// Map of values.
    Map(HashMap<String, PropertyValue>),
}

impl From<bool> for PropertyValue {
    fn from(v: bool) -> Self {
        PropertyValue::Bool(v)
    }
}

impl From<i64> for PropertyValue {
    fn from(v: i64) -> Self {
        PropertyValue::Integer(v)
    }
}

impl From<i32> for PropertyValue {
    fn from(v: i32) -> Self {
        PropertyValue::Integer(v as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        PropertyValue::Float(v)
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        PropertyValue::String(v)
    }
}

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        PropertyValue::String(v.to_string())
    }
}

/// A node in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: i64,
    /// Node labels (e.g., "Person", "Company").
    pub labels: Vec<String>,
    /// Node properties.
    pub properties: HashMap<String, PropertyValue>,
}

impl Node {
    /// Create a new node with the given ID and labels.
    pub fn new(id: i64, labels: Vec<String>) -> Self {
        Self {
            id,
            labels,
            properties: HashMap::new(),
        }
    }

    /// Add a property to the node.
    pub fn with_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<PropertyValue>,
    ) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Check if the node has a specific label.
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }

    /// Get a property value by key.
    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }
}

/// An relationship (relationship) in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Unique relationship identifier.
    pub id: i64,
    /// Source node ID.
    pub source: i64,
    /// Target node ID.
    pub target: i64,
    /// Relationship type (e.g., "KNOWS", "WORKS_AT").
    pub rel_type: String,
    /// Relationship properties.
    pub properties: HashMap<String, PropertyValue>,
}

impl Relationship {
    /// Create a new relationship.
    pub fn new(id: i64, source: i64, target: i64, rel_type: impl Into<String>) -> Self {
        Self {
            id,
            source,
            target,
            rel_type: rel_type.into(),
            properties: HashMap::new(),
        }
    }

    /// Add a property to the relationship.
    pub fn with_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<PropertyValue>,
    ) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Get a property value by key.
    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }
}
