# Getting Started

This guide walks through the basics of using CrustDB in a Rust application.

## Installation

Add CrustDB to your project:

```toml
[dependencies]
crustdb = "0.1"
```

## Opening a Database

CrustDB supports both file-based and in-memory databases.

### File-Based Database

```rust
use crustdb::Database;

let db = Database::open("graph.db")?;
```

The database file is created if it does not exist. All data persists between sessions.

### In-Memory Database

```rust
let db = Database::in_memory()?;
```

In-memory databases are useful for testing and temporary data. They do not persist after the process exits.

### Connection Pool Size

By default, CrustDB creates a pool of 4 read connections for concurrent queries. You can customize this:

```rust
// Use 8 read connections
let db = Database::open_with_pool_size("graph.db", 8)?;

// No pool (all queries use single connection)
let db = Database::open_with_pool_size("graph.db", 0)?;
```

## Creating Data

Use the `CREATE` clause to add nodes and relationships.

### Creating Nodes

```rust
// Node with label and properties
db.execute("CREATE (n:Person {name: 'Alice', age: 30})")?;

// Multiple labels
db.execute("CREATE (n:User:Admin {email: 'admin@example.com'})")?;
```

### Creating Relationships

```rust
// Create two nodes connected by a relationship
db.execute(
    "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})"
)?;
```

### Connecting Existing Nodes

```rust
// First, create nodes separately
db.execute("CREATE (a:Person {name: 'Alice'})")?;
db.execute("CREATE (b:Person {name: 'Bob'})")?;

// Then create a relationship between them
db.execute(
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
     CREATE (a)-[:KNOWS]->(b)"
)?;
```

## Querying Data

Use `MATCH` to find patterns and `RETURN` to specify output.

### Basic Queries

```rust
// Find all people
let result = db.execute("MATCH (n:Person) RETURN n.name")?;

// Find with property filter
let result = db.execute("MATCH (n:Person {name: 'Alice'}) RETURN n")?;

// Find relationships
let result = db.execute(
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
)?;
```

### Working with Results

```rust
let result = db.execute("MATCH (n:Person) RETURN n.name AS name, n.age AS age")?;

// Column names
println!("Columns: {:?}", result.columns);

// Iterate rows
for row in result.rows {
    if let Some(name) = row.get("name") {
        println!("Name: {:?}", name);
    }
}

// Check statistics
println!("Nodes matched: {}", result.rows.len());
```

## Updating Data

Use `SET` to modify properties.

```rust
// Update a property
db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")?;

// Add a new property
db.execute("MATCH (n:Person {name: 'Alice'}) SET n.email = 'alice@example.com'")?;

// Add a label
db.execute("MATCH (n:Person {name: 'Alice'}) SET n:Employee")?;
```

## Deleting Data

Use `DELETE` to remove nodes and relationships.

```rust
// Delete a node (must have no relationships)
db.execute("MATCH (n:Person {name: 'Bob'}) DELETE n")?;

// Delete with relationships (DETACH)
db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")?;

// Delete specific relationship
db.execute("MATCH (a)-[r:KNOWS]->(b) DELETE r")?;
```

## Batch Operations

For bulk data loading, use the batch API methods instead of individual Cypher statements.

```rust
use serde_json::json;

// Batch insert nodes
let nodes = vec![
    (vec!["Person".to_string()], json!({"name": "Alice", "objectid": "alice-1"})),
    (vec!["Person".to_string()], json!({"name": "Bob", "objectid": "bob-2"})),
    (vec!["Company".to_string()], json!({"name": "Acme", "objectid": "acme-3"})),
];

let node_ids = db.insert_nodes_batch(&nodes)?;

// Batch insert relationships
let relationships = vec![
    (node_ids[0], node_ids[1], "KNOWS".to_string(), json!({"since": 2020})),
    (node_ids[0], node_ids[2], "WORKS_AT".to_string(), json!({})),
];

let edge_ids = db.insert_edges_batch(&relationships)?;
```

## Property Indexes

Create indexes on frequently queried properties for better performance.

```rust
// Create an index on objectid
db.create_property_index("objectid")?;

// Queries using indexed properties are faster
let result = db.execute("MATCH (n {objectid: 'alice-1'}) RETURN n")?;

// List current indexes
let indexes = db.list_property_indexes()?;

// Remove an index
db.drop_property_index("objectid")?;
```

## Database Statistics

```rust
let stats = db.stats()?;
println!("Nodes: {}", stats.node_count);
println!("Edges: {}", stats.edge_count);
println!("Labels: {}", stats.label_count);
println!("Relationship types: {}", stats.edge_type_count);
```

## Error Handling

CrustDB operations return `Result<T, crustdb::Error>`. Common errors include:

- `Error::Parse`: Invalid Cypher syntax
- `Error::Execution`: Runtime query errors
- `Error::Storage`: SQLite-level errors
- `Error::NotFound`: Referenced node or relationship does not exist

```rust
match db.execute("INVALID QUERY") {
    Ok(result) => println!("Success"),
    Err(crustdb::Error::Parse(msg)) => println!("Parse error: {}", msg),
    Err(e) => println!("Error: {}", e),
}
```

## Next Steps

- [Feature Support](features.md) - Supported Cypher features
- [API Reference](api/index.md) - Full API documentation
