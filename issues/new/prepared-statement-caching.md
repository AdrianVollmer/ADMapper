# Prepared Statement Caching

## Problem

Currently, every query execution compiles SQL statements from scratch. For frequently-used queries (node lookups, edge traversals), this adds unnecessary overhead.

## Current Behavior

In `storage.rs`, queries are prepared inline:

```rust
pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
    let mut stmt = self.conn.prepare(
        "SELECT id, labels, properties FROM nodes WHERE id = ?"
    )?;
    // ...
}
```

Each call to `prepare()` parses and compiles the SQL. For hot paths like graph traversal, this is repeated thousands of times.

## Proposed Solution

### Option A: rusqlite's `cached_statement`

rusqlite provides `prepare_cached()` which maintains an LRU cache:

```rust
pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
    let mut stmt = self.conn.prepare_cached(
        "SELECT id, labels, properties FROM nodes WHERE id = ?"
    )?;
    // ...
}
```

**Pros:** Minimal code change, built-in LRU eviction
**Cons:** Requires `&mut self` or internal mutability for the cache

### Option B: Explicit Statement Cache

Create a struct holding pre-compiled statements:

```rust
pub struct SqliteStorage {
    conn: Connection,
    stmts: StatementCache,
}

struct StatementCache {
    get_node: Statement<'static>,
    get_edge: Statement<'static>,
    find_outgoing: Statement<'static>,
    find_incoming: Statement<'static>,
    // ...
}
```

**Pros:** No runtime lookup overhead, explicit about what's cached
**Cons:** Lifetime complexity with `Statement<'conn>`, more boilerplate

### Option C: Lazy Static Statements

Use `OnceCell` or lazy initialization:

```rust
impl SqliteStorage {
    fn get_node_stmt(&self) -> &Statement {
        self.get_node_stmt.get_or_init(|| {
            self.conn.prepare("SELECT ...").unwrap()
        })
    }
}
```

## High-Impact Statements to Cache

Based on usage patterns in `executor/pattern.rs`:

1. `get_node` - called for every node access
2. `get_edge` - called for every edge access
3. `find_outgoing_edges` - core of graph traversal
4. `find_incoming_edges` - core of graph traversal
5. `find_nodes_by_label` - pattern matching entry point
6. `find_edges_by_type` - relationship filtering

## Implementation Steps

1. Audit `storage.rs` for all `prepare()` calls
2. Choose caching strategy (Option A recommended for simplicity)
3. Replace `prepare()` with `prepare_cached()` for hot paths
4. Benchmark before/after on traversal-heavy queries

## Complexity

Medium - requires understanding rusqlite's caching semantics and may need internal mutability (`RefCell`) if using `prepare_cached()` with `&self` methods.

## See Also

- rusqlite docs: https://docs.rs/rusqlite/latest/rusqlite/struct.Connection.html#method.prepare_cached
- `issues/new/code-review-crustdb.md` - Original identification
