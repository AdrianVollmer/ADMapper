# Broader DISTINCT Optimizations

**Criticality: Low** | **Complexity: Medium**

## Problem

Only `RETURN DISTINCT type(r)` is optimized to a dedicated
`RelationshipTypesScan` operator. All other DISTINCT queries perform full
materialization followed by deduplication, even when more efficient
strategies exist.

```cypher
-- Could scan label metadata directly instead of scanning all nodes
MATCH (n) RETURN DISTINCT labels(n)

-- Could use a property index or early-exit scan
MATCH (n:User) RETURN DISTINCT n.department
```

## Root Cause

In `src/crustdb/src/query/planner/optimizer.rs` (lines 268-283), the
Project/DISTINCT optimization only matches the specific pattern
`RETURN DISTINCT type(r)` over an Expand:

```rust
if distinct && columns.len() == 1 {
    if let PlanExpr::Function { name, args } = &columns[0].expr {
        if name.to_uppercase() == "TYPE" && args.len() == 1 {
            // Only this pattern is optimized
        }
    }
}
```

## Proposed Fix

1. **DISTINCT labels(n)**: Add a `LabelsScan` operator that queries
   distinct label combinations directly from the labels table
2. **DISTINCT n.property on indexed properties**: When a property index
   exists, scan distinct values from the index instead of the full node set
3. **Early DISTINCT**: For general cases, push DISTINCT closer to the data
   source. When scanning nodes with DISTINCT on a single property, add
   a dedup step inside the scan loop rather than materializing all rows

These are independent improvements that can be tackled incrementally.

## Files to Modify

- `src/crustdb/src/query/operators.rs` — add new scan operators
- `src/crustdb/src/query/planner/optimizer.rs` — extend DISTINCT detection
- `src/crustdb/src/storage/` — add SQL queries for distinct scans
- `src/crustdb/src/query/executor/` — implement new operators
