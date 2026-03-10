# CrustDB Variable-Length Path Query Optimization

## Problem

After commit `0f3cdf0`, CrustDB e2e tests started timing out on five tests:

1. Node status API works (30s timeout)
2. Choke points API works (30s timeout)
3. Choke points cached call fast (30s timeout)
4. Shortest path API works (30s timeout)
5. Perf: Incoming KNOWS edges (24s, exceeded 3s threshold)

Root cause: The commit changed `node_status_quick` to `node_status_full`, which now
runs expensive variable-length path queries like:

```cypher
MATCH p = (a)-[*1..20]->(b) WHERE a.objectid = '...' AND b.objectid ENDS WITH '-519'
RETURN length(p) LIMIT 1
```

CrustDB's executor explored the entire reachable graph (up to 20 hops) before
applying the WHERE filter and LIMIT, making these queries extremely slow.

## Solution

Implemented query optimizer improvements for variable-length path queries:

### 1. LIMIT Pushdown

Added `limit` field to `VariableLengthExpand` operator. The optimizer now pushes
`LIMIT` into the BFS execution, enabling early termination once enough results
are found.

### 2. Target Predicate Pushdown

Added `target_property_filter` field to `VariableLengthExpand` that supports:
- `property = value` (equality)
- `property ENDS WITH 'suffix'`
- `property STARTS WITH 'prefix'`
- `property CONTAINS 'substring'`

The optimizer extracts these predicates from WHERE clauses and pushes them into
the BFS traversal. At execution time, matching target nodes are pre-resolved
and used for early termination.

### 3. Storage Methods

Added efficient SQL-based lookups for pattern matching:
- `find_nodes_by_property_suffix()`
- `find_nodes_by_property_prefix()`
- `find_nodes_by_property_contains()`

## Files Changed

- `src/crustdb/src/query/planner.rs` - Added `TargetPropertyFilter` type, optimizer passes
- `src/crustdb/src/query/executor/plan_exec.rs` - Added filter resolution and early termination
- `src/crustdb/src/storage.rs` - Added pattern matching storage methods

## Testing

- Added `test_plan_variable_length_limit_pushdown` test
- Added `test_plan_variable_length_filter_pushdown` test
- All existing tests pass (134 total)

## Performance Impact

For queries like the one used in `node_status_full`:
- Before: Explores all ~50,000 nodes within 20 hops, then filters
- After: Pre-resolves target nodes (1-2 Enterprise Admin groups), BFS terminates
  immediately when found, returns after first match due to LIMIT 1

Expected improvement: 30+ seconds -> milliseconds for typical AD graphs.
