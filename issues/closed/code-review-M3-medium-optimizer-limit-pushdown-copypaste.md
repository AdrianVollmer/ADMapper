# LIMIT pushdown optimizer logic is copy-pasted

## Severity: MEDIUM

## Problem

In `src/crustdb/src/query/planner/optimizer/mod.rs` (lines 174-288), the
LIMIT pushdown logic spans 114 lines with 7 nesting levels. The same
pattern is copy-pasted across 6 different operator combinations
(Project→Expand, Project→VariableLengthExpand, Project→ShortestPath, etc.).

Hard to maintain and easy to miss edge cases when adding new operators.

## Solution

Extract a helper function:

```rust
fn push_limit_into_operator(op: PlanOperator, limit: usize) -> PlanOperator {
    match op {
        PlanOperator::Expand { .. } => { /* apply limit */ }
        PlanOperator::VariableLengthExpand { .. } => { /* apply limit */ }
        PlanOperator::ShortestPath { .. } => { /* apply limit */ }
        other => other, // can't push through
    }
}
```

Then the outer match just calls this helper uniformly.
