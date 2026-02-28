# Query Caching

CrustDB supports optional query result caching for read-only queries. When enabled, repeated execution of the same query returns cached results without re-executing against the database.

## Enabling Caching

Caching is disabled by default. Enable it on a Database instance:

```rust
let mut db = Database::open("graph.db")?;
db.set_caching(true);
```

## How It Works

1. When a read-only query executes, its AST is hashed to create a cache key
2. Results are stored in a SQLite table alongside the query hash
3. Subsequent executions of the same query return the cached result
4. Any write operation (CREATE, SET, DELETE) invalidates the entire cache

## Automatic Invalidation

CrustDB uses SQLite triggers to automatically clear the cache when data changes:

- Node insertions, updates, or deletions
- Edge insertions, updates, or deletions
- Label modifications

This ensures cached results never become stale.

## Cache API

### Enable/Disable

```rust
db.set_caching(true);   // Enable
db.set_caching(false);  // Disable
```

### Check Status

```rust
if db.caching_enabled() {
    println!("Caching is enabled");
}
```

### Manual Clear

```rust
db.clear_cache()?;
```

### Statistics

```rust
let stats = db.cache_stats()?;
println!("Cached entries: {}", stats.entry_count);
println!("Total size: {} bytes", stats.total_size);
```

## What Gets Cached

**Cached (read-only queries):**

```cypher
MATCH (n:Person) RETURN n.name
MATCH (a)-[r]->(b) RETURN a, type(r), b
MATCH (n) WHERE n.age > 30 RETURN count(n)
```

**Not cached (write queries):**

```cypher
CREATE (n:Person {name: 'Alice'})
MATCH (n) SET n.updated = true
MATCH (n) DELETE n
```

## Algorithm Results

Graph algorithm results are also cached:

```rust
// First call computes (expensive)
let result1 = db.edge_betweenness_centrality(None, true)?;

// Second call returns cached result (fast)
let result2 = db.edge_betweenness_centrality(None, true)?;
```

Algorithm caches use a separate key that includes the algorithm parameters (edge types, directed flag).

## Performance Considerations

### When to Enable

Enable caching when:

- The same queries are executed repeatedly
- Read operations significantly outnumber writes
- Query execution time is a bottleneck

### When to Avoid

Avoid caching when:

- Queries are unique (no repetition)
- Write operations are frequent (constant cache invalidation)
- Memory is constrained

### Cache Size

The cache stores serialized query results. For queries returning large result sets, this can consume significant space. Monitor with `cache_stats()`.

## Example

```rust
use crustdb::Database;

fn main() -> crustdb::Result<()> {
    let mut db = Database::open("graph.db")?;

    // Enable caching
    db.set_caching(true);

    // Create test data
    db.execute("CREATE (n:User {name: 'Alice', active: true})")?;
    db.execute("CREATE (n:User {name: 'Bob', active: false})")?;

    // First query execution - populates cache
    let result1 = db.execute("MATCH (n:User) WHERE n.active = true RETURN n.name")?;
    println!("First query: {} rows", result1.rows.len());

    // Second execution - returns cached result
    let result2 = db.execute("MATCH (n:User) WHERE n.active = true RETURN n.name")?;
    println!("Second query: {} rows (from cache)", result2.rows.len());

    // Check cache stats
    let stats = db.cache_stats()?;
    println!("Cache entries: {}", stats.entry_count);

    // Modify data - cache is invalidated
    db.execute("MATCH (n:User {name: 'Bob'}) SET n.active = true")?;

    // Cache was cleared
    let stats = db.cache_stats()?;
    println!("Cache entries after write: {}", stats.entry_count);

    // Next query re-executes and re-caches
    let result3 = db.execute("MATCH (n:User) WHERE n.active = true RETURN n.name")?;
    println!("Third query: {} rows (fresh result)", result3.rows.len());

    Ok(())
}
```

## Implementation Details

The cache is stored in a `query_cache` table:

| Column | Type | Description |
|--------|------|-------------|
| query_hash | TEXT | Hash of the query AST |
| query_text | TEXT | Original query for debugging |
| result | BLOB | Serialized JSON result |
| created_at | INTEGER | Unix timestamp |

Triggers on `nodes` and `edges` tables execute `DELETE FROM query_cache` on any modification.
