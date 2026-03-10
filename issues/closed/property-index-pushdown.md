# Property Index Pushdown in Cypher Pattern Executor

## Summary

The Cypher pattern executor should leverage property indexes when scanning nodes
with property filters but no label filters.

## Problem

Currently, when executing a pattern like `MATCH (a)-[r]->(b {objectid: '...'})`:

1. The executor correctly flips traversal to start from the filtered target `b`
2. But `scan_nodes()` in `pattern.rs` falls back to `scan_all_nodes()` because
   there's no label on the pattern
3. The property filter is applied AFTER loading all nodes into memory via
   `filter_by_properties()`

This makes queries O(N) where N = total nodes, even when we have a property
index that could make it O(1).

## Current Code Flow

```rust
// pattern.rs:1091-1094
pub fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - scan all nodes
        storage.scan_all_nodes()  // <-- BOTTLENECK
    } else {
        // Use label index
    }
}
```

## Proposed Solution

Modify `scan_nodes()` to detect when:
1. Pattern has NO labels (can't use label index)
2. Pattern HAS properties with an available property index
3. In that case, use `storage.find_node_by_property()` or a new
   `find_nodes_by_property()` method

### Implementation Approach

1. Add `find_nodes_by_property(property: &str, value: &PropertyValue) -> Vec<Node>`
   to `SqliteStorage`

2. Modify `scan_nodes()`:
   ```rust
   pub fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
       // Check if we can use a property index
       if pattern.labels.is_empty() {
           if let Some(Expression::Map(props)) = &pattern.properties {
               // Try to find an indexed property we can use
               for (key, value) in props {
                   if storage.has_property_index(key)? {
                       if let Expression::Literal(lit) = value {
                           // Use property index lookup
                           return storage.find_nodes_by_property(key, lit);
                       }
                   }
               }
           }
           // Fallback to full scan
           storage.scan_all_nodes()
       } else {
           // Use label index (existing code)
       }
   }
   ```

3. Consider caching `has_property_index()` results to avoid repeated SQLite
   metadata queries

### Benefits

- General solution that improves ALL Cypher queries with indexed property filters
- Works for any indexed property, not just `objectid`
- Maintains Cypher semantics while optimizing execution

### Complexity

- Medium: requires changes to the pattern executor
- Need to handle multi-property patterns (pick most selective index)
- Need to propagate storage reference or index metadata through the executor

## Related

- Commit d3033e64 added traversal direction flipping but didn't address the
  scan_all_nodes bottleneck
- The storage layer already has `find_node_by_property()` and property index
  infrastructure

## Files to Modify

- `src/crustdb/src/query/executor/pattern.rs` - main changes
- `src/crustdb/src/storage.rs` - may need `find_nodes_by_property()` variant
