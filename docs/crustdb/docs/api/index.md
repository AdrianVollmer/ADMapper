# API Reference

CrustDB provides a Rust API for embedded graph database operations. The `Database` struct is the main entry point.

## Sections

| Section | Description |
|---------|-------------|
| [Database Lifecycle](database.md#database-lifecycle) | Opening databases, clearing data, getting statistics |
| [Query Execution](database.md#query-execution) | Running Cypher queries |
| [Batch Operations](database.md#batch-operations) | Bulk insert and upsert of nodes and relationships |
| [Graph Traversal](database.md#graph-traversal) | Direct lookups and neighbor retrieval |
| [Indexes](database.md#indexes) | Property index management |
| [Caching](database.md#caching) | Query result caching |
| [Graph Algorithms](database.md#graph-algorithms) | Built-in algorithms |
| [Query History](database.md#query-history) | Query history management |

## Core Types

### QueryResult

The result of executing a Cypher query.

```rust
pub struct QueryResult {
    pub columns: Vec<String>,    // Column names from RETURN
    pub rows: Vec<Row>,          // Result rows
    pub stats: QueryStats,       // Execution statistics
}
```

### Row

A single row in the query result.

```rust
pub struct Row {
    pub values: HashMap<String, ResultValue>,
}

impl Row {
    pub fn get(&self, column: &str) -> Option<&ResultValue>;
}
```

### ResultValue

A value in a query result cell.

```rust
pub enum ResultValue {
    Property(PropertyValue),
    Node { id: i64, labels: Vec<String>, properties: HashMap<String, PropertyValue> },
    Relationship { id: i64, source: i64, target: i64, rel_type: String, properties: HashMap<String, PropertyValue> },
    Path { nodes: Vec<PathNode>, relationships: Vec<PathEdge> },
}
```

### PropertyValue

A property value stored on a node or relationship.

```rust
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<PropertyValue>),
    Map(HashMap<String, PropertyValue>),
}
```

### QueryStats

Statistics from query execution.

```rust
pub struct QueryStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub labels_added: usize,
    pub execution_time_ms: u64,
}
```

### DatabaseStats

Statistics about the database contents.

```rust
pub struct DatabaseStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub label_count: usize,
    pub edge_type_count: usize,
}
```

### CacheStats

Statistics about the query cache.

```rust
pub struct CacheStats {
    pub entry_count: usize,
    pub total_size: usize,
}
```

### EdgeBetweenness

Result of relationship betweenness centrality computation.

```rust
pub struct EdgeBetweenness {
    pub scores: HashMap<i64, f64>,
    pub nodes_processed: usize,
    pub edges_count: usize,
}

impl EdgeBetweenness {
    pub fn top_k(&self, k: usize) -> Vec<(i64, f64)>;
    pub fn above_threshold(&self, threshold: f64) -> Vec<(i64, f64)>;
}
```

### Error

Error types returned by CrustDB operations.

```rust
pub enum Error {
    Parse(String),        // Cypher syntax error
    Execution(String),    // Runtime execution error
    Storage(String),      // SQLite storage error
    NotFound(String),     // Entity not found
    Internal(String),     // Internal error
}
```

## Module Structure

| Module | Description |
|--------|-------------|
| `crustdb` | Main crate, exports `Database` and core types |
| `crustdb::query` | Query parsing, planning, and execution |
| `crustdb::graph` | Node, Relationship, and PropertyValue types |
| `crustdb::error` | Error types |

## Re-exports

```rust
pub use error::{Error, Result};
pub use graph::{Relationship, Node, PropertyValue};
pub use query::executor::algorithms::EdgeBetweenness;
pub use query::{QueryResult, QueryStats, ResultValue, Row};
pub use storage::CacheStats;
```
