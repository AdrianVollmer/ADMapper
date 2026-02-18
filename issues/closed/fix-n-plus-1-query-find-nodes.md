# Fix N+1 Query Problem in find_nodes_by_label

## Problem

`SqliteStorage::find_nodes_by_label()` currently has an N+1 query problem:

```rust
// storage.rs:563-582
let node_ids: Vec<i64> = stmt.query_map(...)  // Query 1: get all IDs
for id in node_ids {
    self.get_node(id)?  // Query 2..N+1: fetch each node individually
}
```

For 10,000 nodes, this executes 10,001 SQL queries instead of 1.

## Solution

Fetch all node data in a single query:

```rust
pub fn find_nodes_by_label(&self, label: &str) -> Result<Vec<Node>> {
    let mut stmt = self.conn.prepare(
        "SELECT n.id, n.properties FROM nodes n
         JOIN node_label_map nlm ON n.id = nlm.node_id
         JOIN node_labels nl ON nlm.label_id = nl.id
         WHERE nl.name = ?1",
    )?;

    let nodes = stmt.query_map(params![label], |row| {
        let id: i64 = row.get(0)?;
        let properties_json: String = row.get(1)?;
        // Parse properties and build Node directly
        Ok(Node { id, ... })
    })?;

    nodes.collect()
}
```

Also need to fetch labels for each node. Options:
1. Second query with `WHERE node_id IN (...)`
2. Join with GROUP_CONCAT for labels
3. Lazy-load labels only when needed

## Affected Functions

- `find_nodes_by_label()`
- `find_edges_by_type()` (same pattern)
- `get_all_nodes()` (verify)
- `get_all_edges()` (verify)

## Expected Impact

Should reduce query time by ~10-100x for large result sets.
