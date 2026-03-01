# CrustDB

CrustDB is an embedded graph database written in Rust. It uses SQLite as its storage backend and supports the Cypher query language for pattern matching and graph traversal.

## Key Features

- **Embedded**: Links directly into your application. No separate server process.
- **Cypher Compatible**: Supports standard Cypher syntax for queries.
- **SQLite Backend**: ACID transactions, reliability, and zero configuration.
- **Connection Pooling**: Concurrent read queries with a configurable connection pool.
- **Query Caching**: Optional result caching with automatic invalidation.
- **Graph Algorithms**: Built-in edge betweenness centrality.

## Cypher Support

CrustDB implements a practical subset of Cypher. For full details, see [Feature Support](features.md).

**Supported:**

- `CREATE`, `MATCH`, `WHERE`, `RETURN`, `SET`, `DELETE`
- Variable-length paths and shortest path queries
- Property filtering, boolean operators, string predicates
- `ORDER BY`, `LIMIT`, `SKIP`, `DISTINCT`
- `count()` aggregation

**Not yet supported:**

- `OPTIONAL MATCH`, `WITH`, `UNWIND`, `UNION`
- `CASE` expressions and list comprehensions
- `sum()`, `avg()`, `min()`, `max()` aggregations
- String and math functions

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

It is suited for knowledge graphs, recommendation engines, and applications where deploying a full graph database server is impractical.

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
