# CrossJoin Reordering by Estimated Cardinality

**Impact: High** | **Complexity: Low**

## Problem

Comma-separated MATCH patterns produce CrossJoin operators where the left
side becomes the outer loop. No reordering is attempted, so a query like:

```cypher
MATCH (a:User), (b:Domain) WHERE ...
```

With 50,000 Users and 3 Domains, the cross join produces
50,000 x 3 = 150,000 intermediate bindings. If reordered to put Domain first:
3 x 50,000 = 150,000 — same total, but the outer loop has only 3 iterations,
which matters when subsequent filters eliminate rows early.

More critically, when filters reference only one side, placing the filtered
side as the inner loop means the outer loop does unnecessary work. If a
subsequent Filter checks `b.name = 'CORP'`, reordering to scan Domain first
would produce only 1 x 50,000 = 50,000 bindings after the filter is applied.

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs` (lines 330-333), CrossJoin
is passed through with no analysis:

```rust
PlanOperator::CrossJoin { left, right } => PlanOperator::CrossJoin {
    left: Box::new(optimize_operator(*left)),
    right: Box::new(optimize_operator(*right)),
},
```

## Proposed Fix

1. Estimate cardinality of left and right subtrees:
   - `NodeScan` with labels: count nodes by label (SQL `SELECT COUNT(*)`)
   - `NodeScan` with property filter: use a heuristic fraction
   - `Expand`: multiply source cardinality by average fan-out
   - Unknown: treat as large
2. Place the smaller estimated side as `left` (outer loop)
3. Optionally, if a Filter sits above the CrossJoin and references only one
   side's variables, consider pushing the filter into that side before
   reordering

This is low complexity because the reordering is a single swap at plan time
and doesn't require executor changes.

## Files to Modify

- `src/crustdb/src/query/planner/optimizer.rs` — add cardinality estimation
  and CrossJoin reordering
