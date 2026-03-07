# Optimize Security Insights Computation

## Problem

`get_security_insights()` in `src/backend/src/db/crustdb.rs` loads the
entire graph into memory:

```rust
pub fn get_security_insights(&self) -> Result<SecurityInsights> {
    let nodes = self.get_all_nodes()?;         // ALL nodes
    let relationships = self.get_all_edges()?;  // ALL edges
    // ... multiple BFS operations ...
}
```

This is problematic because:
- Loads entire graph twice (nodes + edges)
- Builds multiple in-memory adjacency lists
- Runs BFS from every user to every DA group
- O(Users * V) BFS operations

## Solution

### Phase 1: Reverse BFS (Quick Win)

Instead of BFS from each user TO DA groups, do reverse BFS FROM DA groups:

```rust
// Current: O(Users * V) - BFS from each user
for user in users {
    bfs_to_da(user, da_groups);
}

// Better: O(V) - Single reverse BFS from DA groups
let distances = reverse_bfs_from(da_groups);
for user in users {
    if let Some(dist) = distances.get(&user.id) {
        effective_das.push((user, dist));
    }
}
```

### Phase 2: Incremental Queries (Medium)

Use indexed queries instead of loading all edges:
- Query MemberOf edges specifically
- Use label indexes to find Users/Groups directly

### Phase 3: Materialized Views (Future)

For dashboards that refresh frequently, precompute and cache:
- DA reachability bitmap
- User-to-DA hop counts

## Complexity

**Phase 1 - Reverse BFS:**
- Implementation: Low (~50 lines)
- Payoff: High (O(Users) -> O(1) for the BFS count)

**Phase 2 - Incremental Queries:**
- Implementation: Medium (~100 lines)
- Payoff: Medium (reduces memory, not compute)

**Phase 3 - Materialized Views:**
- Implementation: High (new caching infrastructure)
- Payoff: High for dashboards

## Payoff

**Impact: High**
- Security insights currently blocks UI on large graphs
- Users * V BFS operations is the dominant cost
- Reverse BFS reduces this to single V traversal

**E2E showed:** Insights API at 2073ms on small test data

## Files to Modify

- `src/backend/src/db/crustdb.rs` - Refactor `get_security_insights()`
- `src/backend/src/db/types.rs` - May need intermediate result types
