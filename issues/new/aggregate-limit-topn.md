# Aggregate + LIMIT Optimization

**Impact: Low** | **Complexity: Low**

## Problem

Queries combining aggregation with LIMIT compute all groups before
discarding most of them:

```cypher
MATCH (n:User)-[:MEMBER_OF]->(g:Group)
RETURN g.name, count(n) AS members
ORDER BY members DESC LIMIT 5
```

Current behavior: compute member counts for all ~5,000 groups, sort all
groups, return top 5. When combined with the top-N sort optimization
(separate issue), this becomes less severe but the full aggregation is
still performed.

## Root Cause

The optimizer has no handler for `Limit` above `Aggregate`. The LIMIT
handler (lines 103-251 of `optimizer.rs`) doesn't recognize the pattern
and keeps LIMIT on top without informing the aggregate.

## Proposed Fix

When `ORDER BY ... LIMIT N` sits above an Aggregate, and the ORDER BY
key is one of the aggregate results:

1. Pass the limit hint to the aggregate executor
2. Use a bounded heap to track only the top-N groups during aggregation
3. Discard groups that can't make it into the top-N early

This is most effective when N is small relative to the number of groups.
Without ORDER BY, LIMIT on Aggregate is less useful since group order
is arbitrary.

## Files to Modify

- `src/crustdb/src/query/planner/optimizer.rs` — detect Limit(Sort(Aggregate))
- `src/crustdb/src/query/executor/plan_exec/project.rs` — bounded aggregate
