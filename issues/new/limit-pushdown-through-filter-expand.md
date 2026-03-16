# LIMIT Pushdown Through Filter for Expand and ShortestPath

**Impact: High** | **Complexity: Medium**

## Problem

The LIMIT-through-Filter optimization path only handles `VariableLengthExpand`.
When a Filter is fully eliminated (all predicates pushed down), the optimizer
checks if the result is a `VariableLengthExpand` and pushes LIMIT into it.
But it misses `Expand` and `ShortestPath`.

```cypher
-- Single-hop: LIMIT not pushed through
MATCH (a:User)-[:MEMBER_OF]->(b:Group) WHERE b.name = 'Admins' RETURN a LIMIT 1

-- ShortestPath: LIMIT not pushed through
MATCH p = shortestPath((a)-[*1..5]->(b)) WHERE b.id ENDS WITH '-512' RETURN p LIMIT 1
```

This means queries that should stop after the first result instead process
the entire graph.

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs`, the `Limit -> Project ->
Filter -> ?` path (lines 175-234) only handles `VariableLengthExpand` after
filter optimization:

```rust
// Line 216: Only matches VariableLengthExpand
PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
    p.limit = Some(count);
    ...
}
// Falls through to "keep LIMIT on top" for Expand and ShortestPath
```

Additionally, `ShortestPathParams` lacks a `limit` field entirely.

## Proposed Fix

1. Add `limit: Option<u64>` to `ShortestPathParams`
2. Add `Expand` and `ShortestPath` branches to the
   `Limit -> Project -> Filter -> ?` path (after filter optimization)
3. Add direct `Limit -> ShortestPath` handling (line 103 area)
4. Update `execute_shortest_path()` to respect the limit (stop after N
   results across all source bindings)

## Files to Modify

- `src/crustdb/src/query/operators.rs` — add `limit` to `ShortestPathParams`
- `src/crustdb/src/query/planner/optimizer.rs` — extend LIMIT pushdown paths
- `src/crustdb/src/query/planner/match_plan.rs` — initialize new field
- `src/crustdb/src/query/executor/plan_exec/expand.rs` — respect limit
