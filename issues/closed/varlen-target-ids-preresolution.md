# Pre-Resolve target_ids for VariableLengthExpand

**Impact: Medium** | **Complexity: Low**

## Problem

`VarLenExpandParams` has a `target_ids: Option<Vec<i64>>` field that the
executor already handles — when set, BFS only reports paths ending at those
specific node IDs. However, the planner always sets it to `None`
(`match_plan.rs:272`) and the optimizer never populates it.

This means BFS checks target labels on every explored node by scanning the
label set, rather than doing a fast `HashSet::contains()` on pre-resolved IDs.

## Root Cause

In `src/crustdb/src/query/planner/match_plan.rs`, line 272:

```rust
target_ids: None, // Never populated
```

The optimizer has no pass that resolves target labels to node IDs.

## Proposed Fix

Add an optimizer pass that, when a `VariableLengthExpand` has non-empty
`target_labels` and no `target_property_filter`, resolves the labels to
node IDs at plan time. This is beneficial when:

- Target labels are selective (fewer than ~10,000 matching nodes)
- The BFS would otherwise check label membership at every depth

This is low complexity because the executor already handles `target_ids`
and the resolution is a simple `find_nodes_by_label` call. The main
consideration is that this requires storage access at optimization time,
which the optimizer currently doesn't have. Two approaches:

1. **Executor-side**: Move the pre-resolution into the executor's expand
   function (check if `target_ids` is None but `target_labels` is non-empty,
   then pre-resolve). This avoids changing the optimizer's interface.
2. **Optimizer-side**: Pass storage access to the optimizer for plan-time
   resolution.

Option 1 is simpler and sufficient.

## Files to Modify

- `src/crustdb/src/query/executor/plan_exec/expand.rs` — pre-resolve
  target labels to IDs in `execute_variable_length_expand` when
  `target_ids` is None
