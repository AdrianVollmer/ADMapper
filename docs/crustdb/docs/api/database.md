# Database API

The `Database` struct is the main entry point for CrustDB operations.

## Opening a Database

### `Database::open`

Open or create a database file.

```rust
pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
```

Creates a connection pool with one write connection and 4 read connections.

**Example:**

```rust
let db = Database::open("graph.db")?;
```

### `Database::open_with_pool_size`

Open a database with a custom read connection pool size.

```rust
pub fn open_with_pool_size<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self>
```

**Parameters:**

- `pool_size = 0`: All queries use the write connection
- `pool_size > 0`: Creates N read-only connections for concurrent reads

**Example:**

```rust
let db = Database::open_with_pool_size("graph.db", 8)?;
```

### `Database::in_memory`

Create an in-memory database.

```rust
pub fn in_memory() -> Result<Self>
```

In-memory databases cannot use connection pooling. All queries use a single connection.

**Example:**

```rust
let db = Database::in_memory()?;
```

## Query Execution

### `execute`

Execute a Cypher query.

```rust
pub fn execute(&self, query: &str) -> Result<QueryResult>
```

Read-only queries use a pooled read connection. Write queries use the write connection.

**Example:**

```rust
let result = db.execute("MATCH (n:Person) RETURN n.name")?;
for row in result.rows {
    println!("{:?}", row.values);
}
```

## Database Statistics

### `stats`

Get database statistics.

```rust
pub fn stats(&self) -> Result<DatabaseStats>
```

**Returns:**

```rust
DatabaseStats {
    node_count: usize,
    edge_count: usize,
    label_count: usize,
    edge_type_count: usize,
}
```

### `database_size`

Get database file size in bytes.

```rust
pub fn database_size(&self) -> Result<usize>
```

Returns 0 for in-memory databases.

### `clear`

Delete all data from the database.

```rust
pub fn clear(&self) -> Result<()>
```

Faster than using Cypher DELETE queries.

## Batch Operations

### `insert_nodes_batch`

Insert multiple nodes in a single transaction.

```rust
pub fn insert_nodes_batch(
    &self,
    nodes: &[(Vec<String>, serde_json::Value)],
) -> Result<Vec<i64>>
```

**Parameters:**

- `nodes`: Vector of (labels, properties) tuples

**Returns:** Vector of created node IDs in the same order as input.

**Example:**

```rust
let nodes = vec![
    (vec!["Person".to_string()], json!({"name": "Alice"})),
    (vec!["Person".to_string()], json!({"name": "Bob"})),
];
let ids = db.insert_nodes_batch(&nodes)?;
```

### `upsert_nodes_batch`

Insert or update nodes by object_id.

```rust
pub fn upsert_nodes_batch(
    &self,
    nodes: &[(Vec<String>, serde_json::Value)],
) -> Result<Vec<i64>>
```

If a node with the same `object_id` exists, properties are merged (new properties added, existing updated). Labels are also merged.

### `insert_edges_batch`

Insert multiple edges in a single transaction.

```rust
pub fn insert_edges_batch(
    &self,
    edges: &[(i64, i64, String, serde_json::Value)],
) -> Result<Vec<i64>>
```

**Parameters:**

- `edges`: Vector of (source_id, target_id, edge_type, properties) tuples

### `get_or_create_node_by_object_id`

Get or create a placeholder node by object_id.

```rust
pub fn get_or_create_node_by_object_id(&self, object_id: &str, label: &str) -> Result<i64>
```

If the node exists, returns its ID. Otherwise, creates a placeholder node with just the object_id and label.

## Property Lookup

### `find_node_by_property`

Find a node by property value.

```rust
pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>>
```

### `build_property_index`

Build an in-memory index of property values to node IDs.

```rust
pub fn build_property_index(&self, property: &str) -> Result<HashMap<String, i64>>
```

Useful for batch edge insertion when edges reference nodes by a property like `object_id`.

## Property Indexes

### `create_property_index`

Create a SQLite expression index on a JSON property.

```rust
pub fn create_property_index(&self, property: &str) -> Result<()>
```

Speeds up queries that filter by this property.

**Example:**

```rust
db.create_property_index("object_id")?;
// Now queries like MATCH (n {object_id: '...'}) use the index
```

### `drop_property_index`

Remove a property index.

```rust
pub fn drop_property_index(&self, property: &str) -> Result<bool>
```

Returns `Ok(true)` if index existed, `Ok(false)` if not.

### `list_property_indexes`

List all property indexes.

```rust
pub fn list_property_indexes(&self) -> Result<Vec<String>>
```

### `has_property_index`

Check if a property index exists.

```rust
pub fn has_property_index(&self, property: &str) -> Result<bool>
```

## Connection Queries

### `get_incoming_connections_by_object_id`

Get incoming edges to a node.

```rust
pub fn get_incoming_connections_by_object_id(
    &self,
    object_id: &str,
) -> Result<(Vec<Node>, Vec<Edge>)>
```

Returns all nodes with edges pointing to the specified node, along with those edges. Uses indexed lookups for O(degree) performance.

### `get_outgoing_connections_by_object_id`

Get outgoing edges from a node.

```rust
pub fn get_outgoing_connections_by_object_id(
    &self,
    object_id: &str,
) -> Result<(Vec<Node>, Vec<Edge>)>
```

### `get_node_edges_by_object_id`

Get all edges for a node (both directions).

```rust
pub fn get_node_edges_by_object_id(
    &self,
    object_id: &str,
) -> Result<Vec<(String, String, String)>>
```

Returns (source_object_id, target_object_id, edge_type) tuples.

### `get_label_counts`

Get counts for all node labels.

```rust
pub fn get_label_counts(&self) -> Result<HashMap<String, usize>>
```

## Caching

### `set_caching`

Enable or disable query caching.

```rust
pub fn set_caching(&mut self, enabled: bool)
```

When enabled, read-only query results are cached and automatically invalidated on data modification.

### `caching_enabled`

Check if caching is enabled.

```rust
pub fn caching_enabled(&self) -> bool
```

### `clear_cache`

Clear the query cache.

```rust
pub fn clear_cache(&self) -> Result<()>
```

### `cache_stats`

Get cache statistics.

```rust
pub fn cache_stats(&self) -> Result<CacheStats>
```

## Graph Algorithms

### `edge_betweenness_centrality`

Compute edge betweenness centrality.

```rust
pub fn edge_betweenness_centrality(
    &self,
    edge_types: Option<&[&str]>,
    directed: bool,
) -> Result<EdgeBetweenness>
```

**Parameters:**

- `edge_types`: Optional filter for specific edge types
- `directed`: Whether to treat edges as directed

**Returns:** `EdgeBetweenness` with scores for all edges.

Results are cached and automatically invalidated on data changes.

### `get_edge`

Get an edge by ID.

```rust
pub fn get_edge(&self, edge_id: i64) -> Result<Option<Edge>>
```

Useful for resolving edge IDs from algorithm results.

## Query History

### `add_query_history`

Add a query to history.

```rust
pub fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()>
```

### `get_query_history`

Get paginated query history.

```rust
pub fn get_query_history(&self, limit: usize, offset: usize) -> Result<(Vec<QueryHistoryRow>, usize)>
```

Returns (rows, total_count).

### `update_query_status`

Update the status of a query.

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

### `delete_query_history`

Delete a query from history.

```rust
pub fn delete_query_history(&self, id: &str) -> Result<()>
```

### `clear_query_history`

Clear all query history.

```rust
pub fn clear_query_history(&self) -> Result<()>
```
