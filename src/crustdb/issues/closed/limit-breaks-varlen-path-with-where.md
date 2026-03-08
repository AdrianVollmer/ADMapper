# LIMIT clause breaks variable-length paths with WHERE clause filters

## Summary

Adding a `LIMIT` clause to a variable-length path query with WHERE clause filters causes the query to return empty results, even when matching paths exist.

## Reproduction

```cypher
-- Setup: Create a path USER_0 -> GROUP_0 -> ... -> HV_GROUP (is_highvalue: true)
-- (6-hop path exists)

-- This query WORKS (returns results):
MATCH (a)-[*1..20]->(b)
WHERE a.object_id = 'USER_0' AND b.is_highvalue = true
RETURN b.object_id
-- Returns: [HV_GROUP]

-- Adding LIMIT 1 BREAKS it (returns empty):
MATCH (a)-[*1..20]->(b)
WHERE a.object_id = 'USER_0' AND b.is_highvalue = true
RETURN b.object_id LIMIT 1
-- Returns: []
```

## Expected Behavior

`LIMIT 1` should return the first matching result. Since the query without LIMIT returns results, adding LIMIT should return exactly 1 of those results.

## Actual Behavior

Adding `LIMIT 1` causes the query to return empty results.

## Analysis

The LIMIT pushdown optimization may be incorrectly interacting with the variable-length path expansion and WHERE clause filtering. Possible causes:

1. LIMIT is being pushed down before the WHERE filter is applied
2. The path expansion terminates early without finding valid paths
3. The result collection is cleared or skipped due to LIMIT logic

## Workaround

Remove the LIMIT clause and handle limiting in application code:

```rust
let result = db.execute("MATCH (a)-[*1..20]->(b) WHERE a.object_id = 'USER_0' AND b.is_highvalue = true RETURN b.object_id").unwrap();
let found = !result.rows.is_empty();
// If you need just first result:
let first = result.rows.first();
```

## Impact

- High: LIMIT is a fundamental SQL/Cypher clause
- Prevents early termination optimization for "exists" queries
- Forces fetching all results when only one is needed
- May cause performance issues on large result sets

## Performance Note

This bug has significant performance implications. Without working LIMIT, queries that only need to check existence must:
1. Find ALL matching paths (not just the first)
2. Return ALL results to the client
3. Discard all but one in application code

For "path to high-value" queries, this means exploring more of the graph than necessary.

## Related Files

- `src/query/planner.rs` - LIMIT pushdown logic
- `src/query/executor/plan_exec.rs` - `execute_limit` and path expansion
- `src/query/operators.rs` - `PlanOperator::Limit`

## Test Reference

See `tests/path_to_highvalue_test.rs` which demonstrates this bug and uses the workaround.
