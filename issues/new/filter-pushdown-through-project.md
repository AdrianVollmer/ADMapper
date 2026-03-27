# Filter Pushdown Through Intermediate Operators

**Criticality: Medium** | **Complexity: Medium**

## Problem

The optimizer only pushes filters into their immediate child operator. When
intermediate operators (Project, Sort, Skip) sit between a Filter and the
operator that could benefit from the predicate, the pushdown doesn't happen.

```cypher
-- Plan: Filter -> Project -> Expand -> NodeScan
-- The filter on b.prop can't reach the Expand because Project is in between
MATCH (a)-[r]->(b) WITH b RETURN b.name WHERE b.active = true
```

More commonly, query plans generated from complex MATCH/WITH/WHERE
combinations produce these intermediate layers.

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs` (lines 254-262), the Filter
handler only pattern-matches its immediate source:

```rust
PlanOperator::Filter { source, predicate } => match *source {
    PlanOperator::VariableLengthExpand(p) => ...,
    PlanOperator::ShortestPath(p) => ...,
    other => PlanOperator::Filter { ... }, // No deeper inspection
},
```

No attempt is made to push through transparent operators like Project
(when it doesn't rename columns) or Sort.

## Proposed Fix

Add cases for Filter above transparent operators:

```rust
PlanOperator::Project { source, columns, distinct: false } => {
    // Check if predicate references only columns that pass through unchanged
    // If so, push Filter below Project
}
PlanOperator::Sort { source, keys } => {
    // Sort doesn't affect rows, push Filter below it
}
```

This requires checking that the predicate's variable references are not
altered by the intermediate operator. For Project with simple column
pass-through (no renaming/computation), this is safe. For Sort and Skip,
it's always safe.

## Files to Modify

- `src/crustdb/src/query/planner/optimizer.rs` — extend Filter match arm
  with Project/Sort/Skip pass-through
