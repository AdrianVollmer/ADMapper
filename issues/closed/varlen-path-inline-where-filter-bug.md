# Variable-length path fails with inline source filter + WHERE boolean target filter

## Summary

Variable-length path queries fail to return results when combining:
1. Inline property filter on the source node: `(a {object_id: 'USER_0'})`
2. WHERE clause boolean filter on the target node: `WHERE b.is_highvalue = true`

## Reproduction

```cypher
-- Setup: Create a path USER_0 -> GROUP_0 -> HV_GROUP (is_highvalue: true)
CREATE (:Group {object_id: 'HV_GROUP', is_highvalue: true})
CREATE (:Group {object_id: 'GROUP_0'})
CREATE (:User {object_id: 'USER_0'})
-- (create relationships via API due to MATCH...CREATE bug)

-- This query returns EMPTY (BUG):
MATCH (a {object_id: 'USER_0'})-[*1..20]->(b)
WHERE b.is_highvalue = true
RETURN b.object_id
-- Returns: []

-- But these equivalent queries WORK:

-- 1. Both filters in WHERE clause:
MATCH (a)-[*1..20]->(b)
WHERE a.object_id = 'USER_0' AND b.is_highvalue = true
RETURN b.object_id
-- Returns: [HV_GROUP]

-- 2. Label + inline source filter:
MATCH (a:User {object_id: 'USER_0'})-[*1..20]->(b)
WHERE b.is_highvalue = true
RETURN b.object_id
-- Returns: [HV_GROUP]

-- 3. Inline filters on both (non-boolean):
MATCH (a {object_id: 'USER_0'})-[*1..20]->(b {object_id: 'HV_GROUP'})
RETURN b.object_id
-- Returns: [HV_GROUP]
```

## Expected Behavior

All equivalent query formulations should return the same results. The path from USER_0 to HV_GROUP exists and should be found.

## Actual Behavior

The specific combination of:
- Inline property filter on source (without label)
- WHERE clause with boolean equality on target

Returns empty results even when the path exists.

## Analysis

The bug appears to be in how predicate pushdown interacts with variable-length path expansion when:
1. Source has inline property filter (pushed into NodeScan)
2. Target has WHERE clause boolean filter

The combination causes the target filter to be incorrectly applied or the path expansion to fail.

## Workaround

Put both filters in the WHERE clause:

```cypher
MATCH (a)-[*1..20]->(b)
WHERE a.object_id = 'USER_0' AND b.is_highvalue = true
RETURN b.object_id
```

## Impact

- Medium: Breaks a common query pattern
- Users must know to avoid this specific combination
- May cause silent incorrect results (empty when should have data)

## Related Files

- `src/query/planner.rs` - Predicate pushdown logic
- `src/query/executor/plan_exec.rs` - Variable-length path execution
- `src/query/operators.rs` - `TargetPropertyFilter` handling
