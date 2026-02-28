# API Reference

CrustDB provides a Rust API for embedded graph database operations.

## Core Types

### Database

The main entry point. Manages connections, executes queries, and provides access to graph operations.

```rust
use crustdb::Database;

let db = Database::open("graph.db")?;
let result = db.execute("MATCH (n) RETURN n")?;
```

See [Database API](database.md) for full documentation.

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
    Edge { id: i64, source: i64, target: i64, edge_type: String, properties: HashMap<String, PropertyValue> },
    Path { nodes: Vec<PathNode>, edges: Vec<PathEdge> },
}
```

### PropertyValue

A property value stored on a node or edge.

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
| `crustdb::graph` | Node, Edge, and PropertyValue types |
| `crustdb::error` | Error types |

## Re-exports

The following types are re-exported from the crate root:

```rust
pub use error::{Error, Result};
pub use graph::{Edge, Node, PropertyValue};
pub use query::executor::algorithms::EdgeBetweenness;
pub use query::{QueryResult, QueryStats, ResultValue, Row};
pub use storage::CacheStats;
```
