# Incremental BFS for shortestPath

## Problem

The `shortest_path()` method in `src/backend/src/db/crustdb.rs` loads the
entire graph into memory before performing BFS:

```rust
pub fn shortest_path(&self, from: &str, to: &str) -> Result<...> {
    let relationships = self.get_all_edges()?;  // Loads ALL edges
    // ... build adjacency list ...
    // ... BFS ...
}
```

For large graphs, this causes:
- High memory usage (all edges in RAM)
- Slow startup (must load before search begins)
- OOM on very large graphs

## Solution

Use incremental neighbor lookup during BFS instead of preloading:

```rust
while let Some(current) = queue.pop_front() {
    if current == target { return path; }

    // Query neighbors on-demand instead of preloading
    let edges = self.db.find_outgoing_edges_by_object_id(&current)?;
    for edge in edges {
        if !visited.contains(&edge.target) {
            // ...
        }
    }
}
```

The storage layer already has `find_outgoing_edges()` and indexed lookups
via `object_id`. This changes complexity from O(E) preload + O(V+E) BFS
to O(visited * avg_degree) which is much better when the path is short.

## Complexity

**Implementation: Medium**
- Need to add `find_outgoing_edges_by_object_id()` to storage layer
- Refactor BFS to use incremental queries
- ~100-150 lines of code

**Risk: Low**
- Well-understood algorithm change
- Easy to test with existing fixtures

## Payoff

**Impact: High for large graphs**
- Eliminates OOM on graphs too large to fit in memory
- Faster time-to-first-result (no preload delay)
- Memory usage proportional to path length, not graph size

**E2E benchmark showed:** shortestPath at 1878ms (5-171x slower than others)

## Files to Modify

- `src/crustdb/src/storage/query.rs` - Add indexed edge lookup by object_id
- `src/backend/src/db/crustdb.rs` - Refactor `shortest_path()` to use incremental BFS
- `src/crustdb/src/lib.rs` - Expose new storage method
