# Database API

The `Database` struct is the main entry point for CrustDB operations.

## Database Lifecycle

### `Database::open`

Open or create a database file.

```rust
pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `path` | `impl AsRef<Path>` | Path to the database file |

**Returns:** `Result<Database>` - A new database instance with a connection pool of 1 write + 4 read connections.

**Example:**

```rust
let db = Database::open("graph.db")?;
```

---

### `Database::open_with_pool_size`

Open a database with a custom read connection pool size.

```rust
pub fn open_with_pool_size<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `path` | `impl AsRef<Path>` | Path to the database file |
| `pool_size` | `usize` | Number of read connections (0 = single connection mode) |

**Returns:** `Result<Database>` - A new database instance.

**Example:**

```rust
// High concurrency: 8 read connections
let db = Database::open_with_pool_size("graph.db", 8)?;

// Single connection mode
let db = Database::open_with_pool_size("graph.db", 0)?;
```

---

### `Database::in_memory`

Create an in-memory database.

```rust
pub fn in_memory() -> Result<Self>
```

**Arguments:** None.

**Returns:** `Result<Database>` - An in-memory database instance. Data is lost when the instance is dropped.

**Example:**

```rust
let db = Database::in_memory()?;
db.execute("CREATE (n:Person {name: 'Alice'})")?;
```

---

### `Database::clear`

Delete all nodes and edges from the database.

```rust
pub fn clear(&self) -> Result<()>
```

**Arguments:** None.

**Returns:** `Result<()>` - Success or error.

**Example:**

```rust
db.clear()?;
let stats = db.stats()?;
assert_eq!(stats.node_count, 0);
```

---

### `Database::stats`

Get statistics about the database contents.

```rust
pub fn stats(&self) -> Result<DatabaseStats>
```

**Arguments:** None.

**Returns:** `Result<DatabaseStats>` with fields:

| Field | Type | Description |
|-------|------|-------------|
| `node_count` | `usize` | Total number of nodes |
| `edge_count` | `usize` | Total number of edges |
| `label_count` | `usize` | Number of distinct labels |
| `edge_type_count` | `usize` | Number of distinct edge types |

**Example:**

```rust
let stats = db.stats()?;
println!("Nodes: {}, Edges: {}", stats.node_count, stats.edge_count);
```

---

### `Database::database_size`

Get the database file size in bytes.

```rust
pub fn database_size(&self) -> Result<usize>
```

**Arguments:** None.

**Returns:** `Result<usize>` - File size in bytes. Returns 0 for in-memory databases.

**Example:**

```rust
let size = db.database_size()?;
println!("Database size: {} KB", size / 1024);
```

---

## Query Execution

### `Database::execute`

Execute a Cypher query.

```rust
pub fn execute(&self, query: &str) -> Result<QueryResult>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `query` | `&str` | A Cypher query string |

**Returns:** `Result<QueryResult>` with fields:

| Field | Type | Description |
|-------|------|-------------|
| `columns` | `Vec<String>` | Column names from RETURN clause |
| `rows` | `Vec<Row>` | Result rows |
| `stats` | `QueryStats` | Execution statistics |

Read-only queries use a pooled read connection. Write queries use the write connection.

**Example:**

```rust
// Create data
db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;

// Query data
let result = db.execute("MATCH (p:Person) RETURN p.name")?;
for row in result.rows {
    println!("{:?}", row.get("p.name"));
}
```

---

## Batch Operations

### `Database::insert_nodes_batch`

Insert multiple nodes in a single transaction.

```rust
pub fn insert_nodes_batch(
    &self,
    nodes: &[(Vec<String>, serde_json::Value)],
) -> Result<Vec<i64>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `nodes` | `&[(Vec<String>, Value)]` | Vector of (labels, properties) tuples |

**Returns:** `Result<Vec<i64>>` - Vector of created node IDs, in the same order as input.

**Example:**

```rust
use serde_json::json;

let nodes = vec![
    (vec!["Person".into()], json!({"name": "Alice", "age": 30})),
    (vec!["Person".into()], json!({"name": "Bob", "age": 25})),
    (vec!["Company".into()], json!({"name": "Acme"})),
];
let ids = db.insert_nodes_batch(&nodes)?;
// ids = [1, 2, 3]
```

---

### `Database::upsert_nodes_batch`

Insert or update nodes by `object_id` property.

```rust
pub fn upsert_nodes_batch(
    &self,
    nodes: &[(Vec<String>, serde_json::Value)],
) -> Result<Vec<i64>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `nodes` | `&[(Vec<String>, Value)]` | Vector of (labels, properties) tuples. Each must have an `object_id` property. |

**Returns:** `Result<Vec<i64>>` - Vector of node IDs (created or existing).

If a node with the same `object_id` exists, properties are merged (new properties added, existing updated). Labels are also merged.

**Example:**

```rust
use serde_json::json;

// First upsert creates node
let ids1 = db.upsert_nodes_batch(&[
    (vec!["Person".into()], json!({"object_id": "alice", "name": "Alice"})),
])?;

// Second upsert updates existing node
let ids2 = db.upsert_nodes_batch(&[
    (vec!["Person".into()], json!({"object_id": "alice", "age": 30})),
])?;

assert_eq!(ids1[0], ids2[0]); // Same node ID
```

---

### `Database::insert_edges_batch`

Insert multiple edges in a single transaction.

```rust
pub fn insert_edges_batch(
    &self,
    edges: &[(i64, i64, String, serde_json::Value)],
) -> Result<Vec<i64>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `edges` | `&[(i64, i64, String, Value)]` | Vector of (source_id, target_id, edge_type, properties) tuples |

**Returns:** `Result<Vec<i64>>` - Vector of created edge IDs.

**Example:**

```rust
use serde_json::json;

let node_ids = db.insert_nodes_batch(&[
    (vec!["Person".into()], json!({"name": "Alice"})),
    (vec!["Person".into()], json!({"name": "Bob"})),
])?;

let edge_ids = db.insert_edges_batch(&[
    (node_ids[0], node_ids[1], "KNOWS".into(), json!({"since": 2020})),
])?;
```

---

### `Database::get_or_create_node_by_object_id`

Get an existing node by `object_id`, or create a placeholder if it doesn't exist.

```rust
pub fn get_or_create_node_by_object_id(&self, object_id: &str, label: &str) -> Result<i64>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `object_id` | `&str` | The object_id to look up |
| `label` | `&str` | Label to use if creating a new node |

**Returns:** `Result<i64>` - The node ID.

**Example:**

```rust
// Creates placeholder node
let id1 = db.get_or_create_node_by_object_id("alice", "Person")?;

// Returns same ID
let id2 = db.get_or_create_node_by_object_id("alice", "Person")?;
assert_eq!(id1, id2);
```

---

## Graph Traversal

### `Database::find_node_by_property`

Find a node by a property value.

```rust
pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `property` | `&str` | Property name to search |
| `value` | `&str` | Property value to match |

**Returns:** `Result<Option<i64>>` - The node ID if found, `None` otherwise.

**Example:**

```rust
db.execute("CREATE (n:Person {name: 'Alice'})")?;

let id = db.find_node_by_property("name", "Alice")?;
assert!(id.is_some());
```

---

### `Database::build_property_index`

Build an in-memory map from property values to node IDs.

```rust
pub fn build_property_index(&self, property: &str) -> Result<HashMap<String, i64>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `property` | `&str` | Property name to index |

**Returns:** `Result<HashMap<String, i64>>` - Map from property value to node ID.

Useful for batch edge insertion when edges reference nodes by a property.

**Example:**

```rust
let index = db.build_property_index("object_id")?;
let alice_id = index.get("alice").copied();
```

---

### `Database::get_incoming_connections_by_object_id`

Get all nodes and edges pointing to a node.

```rust
pub fn get_incoming_connections_by_object_id(
    &self,
    object_id: &str,
) -> Result<(Vec<Node>, Vec<Edge>)>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `object_id` | `&str` | The target node's object_id |

**Returns:** `Result<(Vec<Node>, Vec<Edge>)>` - Source nodes and their edges to the target.

**Example:**

```rust
db.execute("
    CREATE (a:Person {object_id: 'alice', name: 'Alice'})
    CREATE (b:Person {object_id: 'bob', name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
")?;

let (nodes, edges) = db.get_incoming_connections_by_object_id("bob")?;
// nodes contains Alice, edges contains the KNOWS edge
```

---

### `Database::get_outgoing_connections_by_object_id`

Get all nodes and edges originating from a node.

```rust
pub fn get_outgoing_connections_by_object_id(
    &self,
    object_id: &str,
) -> Result<(Vec<Node>, Vec<Edge>)>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `object_id` | `&str` | The source node's object_id |

**Returns:** `Result<(Vec<Node>, Vec<Edge>)>` - Target nodes and edges from the source.

**Example:**

```rust
let (nodes, edges) = db.get_outgoing_connections_by_object_id("alice")?;
```

---

### `Database::get_node_edges_by_object_id`

Get all edges connected to a node (both directions).

```rust
pub fn get_node_edges_by_object_id(
    &self,
    object_id: &str,
) -> Result<Vec<(String, String, String)>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `object_id` | `&str` | The node's object_id |

**Returns:** `Result<Vec<(String, String, String)>>` - Tuples of (source_object_id, target_object_id, edge_type).

**Example:**

```rust
let edges = db.get_node_edges_by_object_id("alice")?;
for (src, tgt, edge_type) in edges {
    println!("{} -[{}]-> {}", src, edge_type, tgt);
}
```

---

### `Database::get_edge`

Get an edge by its ID.

```rust
pub fn get_edge(&self, edge_id: i64) -> Result<Option<Edge>>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `edge_id` | `i64` | The edge ID |

**Returns:** `Result<Option<Edge>>` - The edge if found.

**Example:**

```rust
if let Some(edge) = db.get_edge(42)? {
    println!("{} -[{}]-> {}", edge.source, edge.edge_type, edge.target);
}
```

---

### `Database::get_label_counts`

Get the count of nodes for each label.

```rust
pub fn get_label_counts(&self) -> Result<HashMap<String, usize>>
```

**Arguments:** None.

**Returns:** `Result<HashMap<String, usize>>` - Map from label name to node count.

**Example:**

```rust
let counts = db.get_label_counts()?;
println!("Person nodes: {}", counts.get("Person").unwrap_or(&0));
```

---

## Indexes

### `Database::create_property_index`

Create a SQLite expression index on a JSON property.

```rust
pub fn create_property_index(&self, property: &str) -> Result<()>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `property` | `&str` | Property name to index |

**Returns:** `Result<()>` - Success or error.

Speeds up queries that filter by this property.

**Example:**

```rust
db.create_property_index("object_id")?;

// Now queries like this use the index:
db.execute("MATCH (n {object_id: 'alice'}) RETURN n")?;
```

---

### `Database::drop_property_index`

Remove a property index.

```rust
pub fn drop_property_index(&self, property: &str) -> Result<bool>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `property` | `&str` | Property name |

**Returns:** `Result<bool>` - `true` if index existed, `false` if not.

**Example:**

```rust
let existed = db.drop_property_index("object_id")?;
```

---

### `Database::list_property_indexes`

List all property indexes.

```rust
pub fn list_property_indexes(&self) -> Result<Vec<String>>
```

**Arguments:** None.

**Returns:** `Result<Vec<String>>` - List of indexed property names.

**Example:**

```rust
let indexes = db.list_property_indexes()?;
println!("Indexed properties: {:?}", indexes);
```

---

### `Database::has_property_index`

Check if a property index exists.

```rust
pub fn has_property_index(&self, property: &str) -> Result<bool>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `property` | `&str` | Property name |

**Returns:** `Result<bool>` - Whether the index exists.

**Example:**

```rust
if db.has_property_index("object_id")? {
    println!("object_id is indexed");
}
```

---

## Caching

CrustDB supports optional query result caching. When enabled, repeated execution of the same read-only query returns cached results.

### How Caching Works

1. When a read-only query executes, its AST is hashed to create a cache key
2. Results are stored in a SQLite table
3. Subsequent executions of the same query return cached results
4. Any write operation (CREATE, SET, DELETE) invalidates the entire cache

### `Database::set_caching`

Enable or disable query caching.

```rust
pub fn set_caching(&mut self, enabled: bool)
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `enabled` | `bool` | Whether to enable caching |

**Returns:** Nothing.

**Example:**

```rust
let mut db = Database::open("graph.db")?;
db.set_caching(true);

// First execution computes and caches
db.execute("MATCH (n:Person) RETURN n.name")?;

// Second execution uses cache
db.execute("MATCH (n:Person) RETURN n.name")?;
```

---

### `Database::caching_enabled`

Check if caching is currently enabled.

```rust
pub fn caching_enabled(&self) -> bool
```

**Arguments:** None.

**Returns:** `bool` - Whether caching is enabled.

**Example:**

```rust
if db.caching_enabled() {
    println!("Query caching is active");
}
```

---

### `Database::clear_cache`

Manually clear the query cache.

```rust
pub fn clear_cache(&self) -> Result<()>
```

**Arguments:** None.

**Returns:** `Result<()>` - Success or error.

**Example:**

```rust
db.clear_cache()?;
```

---

### `Database::cache_stats`

Get statistics about the query cache.

```rust
pub fn cache_stats(&self) -> Result<CacheStats>
```

**Arguments:** None.

**Returns:** `Result<CacheStats>` with fields:

| Field | Type | Description |
|-------|------|-------------|
| `entry_count` | `usize` | Number of cached queries |
| `total_size` | `usize` | Total size in bytes |

**Example:**

```rust
let stats = db.cache_stats()?;
println!("Cached: {} entries, {} bytes", stats.entry_count, stats.total_size);
```

---

## Graph Algorithms

### `Database::edge_betweenness_centrality`

Compute edge betweenness centrality for all edges.

Edge betweenness measures how many shortest paths pass through each edge. Edges with high betweenness are "choke points" - removing them would disrupt many paths.

```rust
pub fn edge_betweenness_centrality(
    &self,
    edge_types: Option<&[&str]>,
    directed: bool,
) -> Result<EdgeBetweenness>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `edge_types` | `Option<&[&str]>` | Filter to specific edge types, or `None` for all |
| `directed` | `bool` | Whether to treat edges as directed |

**Returns:** `Result<EdgeBetweenness>` with fields:

| Field | Type | Description |
|-------|------|-------------|
| `scores` | `HashMap<i64, f64>` | Edge ID to betweenness score |
| `nodes_processed` | `usize` | Number of nodes processed |
| `edges_count` | `usize` | Number of edges analyzed |

**Complexity:** O(V * E) where V = nodes, E = edges.

Results are cached and automatically invalidated when data changes.

**Example:**

```rust
// Compute for all edges, directed
let result = db.edge_betweenness_centrality(None, true)?;

// Get top 5 choke points
for (edge_id, score) in result.top_k(5) {
    if let Some(edge) = db.get_edge(edge_id)? {
        println!("{} -> {} ({}): score = {:.2}",
            edge.source, edge.target, edge.edge_type, score);
    }
}

// Filter by edge type
let result = db.edge_betweenness_centrality(Some(&["KNOWS", "WORKS_WITH"]), true)?;

// Get edges above a threshold
for (edge_id, score) in result.above_threshold(100.0) {
    println!("High betweenness edge {}: {}", edge_id, score);
}
```

---

## Query History

### `Database::add_query_history`

Add a query to the history.

```rust
pub fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `entry` | `NewQueryHistoryEntry` | The history entry to add |

**Returns:** `Result<()>` - Success or error.

---

### `Database::get_query_history`

Get paginated query history.

```rust
pub fn get_query_history(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `limit` | `usize` | Maximum entries to return |
| `offset` | `usize` | Number of entries to skip |

**Returns:** `Result<(Vec<QueryHistoryRow>, usize)>` - (rows, total_count).

**Example:**

```rust
let (history, total) = db.get_query_history(10, 0)?;
println!("Showing {} of {} queries", history.len(), total);
```

---

### `Database::update_query_status`

Update the status of a query in history.

```rust
pub fn update_query_status(
    &self,
    id: &str,
    status: &str,
    duration_ms: Option<u64>,
    result_count: Option<i64>,
    error: Option<&str>,
) -> Result<()>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `id` | `&str` | Query history entry ID |
| `status` | `&str` | New status string |
| `duration_ms` | `Option<u64>` | Execution duration |
| `result_count` | `Option<i64>` | Number of results |
| `error` | `Option<&str>` | Error message if failed |

**Returns:** `Result<()>` - Success or error.

---

### `Database::delete_query_history`

Delete a single query from history.

```rust
pub fn delete_query_history(&self, id: &str) -> Result<()>
```

**Arguments:**

| Name | Type | Description |
|------|------|-------------|
| `id` | `&str` | Query history entry ID |

**Returns:** `Result<()>` - Success or error.

---

### `Database::clear_query_history`

Clear all query history.

```rust
pub fn clear_query_history(&self) -> Result<()>
```

**Arguments:** None.

**Returns:** `Result<()>` - Success or error.

**Example:**

```rust
db.clear_query_history()?;
```
