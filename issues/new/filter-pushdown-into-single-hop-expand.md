# Filter Pushdown into Single-Hop Expand

**Impact: High** | **Complexity: Medium**

## Problem

When executing single-hop relationship queries with WHERE filters, the filter
is applied *after* expansion rather than being pushed into the operator. This
means every neighbor of every source node is loaded from SQLite, hydrated into
a full Node struct, and bound — only to be discarded by the Filter above.

```cypher
MATCH (a:User)-[:MEMBER_OF]->(b:Group) WHERE b.name = 'Domain Admins' RETURN a
```

Current plan:
```
Project -> Filter(b.name = 'Domain Admins') -> Expand -> NodeScan(:User)
```

Optimal plan:
```
Project -> Expand(target_property_filter: Eq(name, 'Domain Admins')) -> NodeScan(:User)
```

On a graph with 50k users each having 5-10 group memberships, this loads
250k-500k group nodes when only ~50 match.

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs` (lines 254-262), the
`PlanOperator::Filter` optimization only matches `VariableLengthExpand` and
`ShortestPath`:

```rust
PlanOperator::Filter { source, predicate } => match *source {
    PlanOperator::VariableLengthExpand(p) => push_filter_into_var_len_expand(p, predicate),
    PlanOperator::ShortestPath(p) => push_filter_into_shortest_path(p, predicate),
    other => PlanOperator::Filter { ... }, // Expand falls through here
},
```

The `ExpandParams` struct (`src/crustdb/src/query/operators.rs:21`) has no
`target_property_filter` field, so there is nowhere to push the filter.

## Proposed Fix

1. Add `target_property_filter: Option<TargetPropertyFilter>` to `ExpandParams`
2. Add a `push_filter_into_expand()` function mirroring the existing
   `push_filter_into_var_len_expand()` and `push_filter_into_shortest_path()`
3. Add an `Expand` branch to the Filter match arm in the optimizer
4. Update `execute_expand()` to resolve the filter to target IDs via SQL
   and skip non-matching neighbors
5. Also push source equality filters into the NodeScan under Expand
   (reuse `try_push_source_filter_into_node_scan`, extending it for `Expand`)

The existing infrastructure (`extract_target_property_filter`,
`resolve_target_property_filter`, `try_push_into_inner_scan`) can be reused
directly.

## Files to Modify

- `src/crustdb/src/query/operators.rs` — add field to `ExpandParams`
- `src/crustdb/src/query/planner/optimizer.rs` — add Expand branch + helper
- `src/crustdb/src/query/executor/plan_exec/expand.rs` — filter in executor
