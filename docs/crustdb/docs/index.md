# CrustDB

CrustDB is an embedded graph database written in Rust. It uses SQLite as its storage backend and supports the Cypher query language for pattern matching and graph traversal.

## Key Features

- **Embedded**: Links directly into your application. No separate server process.
- **Cypher Support**: Industry-standard query language for property graphs.
- **SQLite Backend**: ACID transactions, reliability, and zero configuration.
- **Connection Pooling**: Concurrent read queries with a configurable connection pool.
- **Query Caching**: Optional result caching with automatic invalidation.
- **Graph Algorithms**: Built-in algorithms like edge betweenness centrality.

## Quick Example

```rust
use crustdb::Database;

let db = Database::open("my_graph.db")?;

// Create nodes
db.execute("CREATE (n:Person {name: 'Alice', age: 30})")?;
db.execute("CREATE (n:Person {name: 'Bob', age: 25})")?;

// Create relationship
db.execute(
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
     CREATE (a)-[:KNOWS]->(b)"
)?;

// Query
let results = db.execute("MATCH (n:Person) RETURN n.name, n.age")?;
for row in results.rows {
    println!("{:?}", row.values);
}
```

## When to Use CrustDB

CrustDB is designed for applications that need:

- Graph queries without external dependencies
- Portable, single-file database storage
- Low latency for interactive graph exploration
- Embedded analytics on relationship data

It is particularly suited for security analysis tools, knowledge graphs, and applications where deploying a full graph database server is impractical.

## Architecture

CrustDB consists of three main components:

1. **Parser**: Converts Cypher strings into an abstract syntax tree using a pest grammar.
2. **Planner**: Transforms the AST into an optimized execution plan.
3. **Executor**: Runs the plan against the SQLite storage layer.

The storage layer manages nodes, edges, labels, and properties in normalized SQLite tables with JSON support for flexible property storage.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
crustdb = "0.1"
```

## License

MIT
