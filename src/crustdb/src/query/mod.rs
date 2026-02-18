//! Cypher query parsing and execution.

pub mod executor;
pub mod parser;
pub mod planner;

use crate::graph::PropertyValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathNode {
    pub id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, PropertyValue>,
}

/// An edge in a path result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEdge {
    pub id: i64,
    pub source: i64,
    pub target: i64,
    pub edge_type: String,
    pub properties: HashMap<String, PropertyValue>,
}

/// A value in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// An edge.
    Edge {
        id: i64,
        source: i64,
        target: i64,
        edge_type: String,
        properties: HashMap<String, PropertyValue>,
    },
    /// A path (sequence of nodes and edges) with full data.
    Path {
        nodes: Vec<PathNode>,
        edges: Vec<PathEdge>,
    },
}

/// Query execution statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    /// Number of nodes created.
    pub nodes_created: usize,
    /// Number of nodes deleted.
    pub nodes_deleted: usize,
    /// Number of edges created.
    pub relationships_created: usize,
    /// Number of edges deleted.
    pub relationships_deleted: usize,
    /// Number of properties set.
    pub properties_set: usize,
    /// Number of labels added.
    pub labels_added: usize,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}
