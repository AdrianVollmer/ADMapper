# Top-N Optimization: LIMIT Pushdown Through Sort

**Impact: Medium** | **Complexity: Medium**

## Problem

Queries with `ORDER BY ... LIMIT N` sort the entire result set and then
discard all but N rows. For large result sets with small limits, this is
wasteful — a heap-based top-N algorithm would avoid the full sort.

```cypher
MATCH (n:User) RETURN n.name ORDER BY n.lastLogin DESC LIMIT 10
```

Current behavior: sort all ~50,000 users by lastLogin, return top 10.
Optimal: maintain a 10-element heap, single pass over all users.

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs`, the LIMIT handler
(lines 103-251) has no branch for `Sort`:

```rust
PlanOperator::Limit { source, count } => {
    match *source {
        PlanOperator::NodeScan { .. } => ...,
        PlanOperator::Expand(_) => ...,
        PlanOperator::VariableLengthExpand(_) => ...,
        PlanOperator::Project { .. } => ...,
        // No handler for Sort!
        other => PlanOperator::Limit { source: Box::new(optimize_operator(other)), count },
    }
}
```

## Proposed Fix

Two approaches:

### Option A: Push LIMIT into Sort (simple)
Add a `limit: Option<u64>` field to the Sort handling in the executor.
When set, use a bounded BinaryHeap instead of full sort.

### Option B: TopN operator (cleaner)
Add a `PlanOperator::TopN { source, sort_keys, count }` that replaces
the `Limit(Sort(...))` pattern. The executor implements it as a single
heap-based pass.

Option A is simpler and sufficient for most cases.

## Files to Modify

- `src/crustdb/src/query/operators.rs` — add limit field to Sort or add TopN
- `src/crustdb/src/query/planner/optimizer.rs` — detect Limit(Sort) pattern
- `src/crustdb/src/query/executor/plan_exec/project.rs` — implement top-N
