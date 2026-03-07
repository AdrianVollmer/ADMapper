# Optimize Cypher type() Function Queries

## Problem

While the API layer now uses optimized SQL for `get_edge_types()`, Cypher
queries using `type(r)` still perform full edge scans:

```cypher
MATCH ()-[r]->() RETURN DISTINCT type(r)  -- Still slow via Cypher
MATCH (a)-[r]->(b) WHERE type(r) = 'MemberOf' RETURN a, b  -- Full scan + filter
```

The query executor evaluates `type(r)` by:
1. Scanning all edges
2. Joining with rel_types table for each edge
3. Filtering/grouping in memory

## Solution

### Query Planner Optimization

Detect patterns involving `type(r)` and rewrite:

**Pattern 1: DISTINCT type enumeration**
```cypher
MATCH ()-[r]->() RETURN DISTINCT type(r)
-- Rewrite to: SELECT name FROM rel_types
```

**Pattern 2: Type filter in WHERE**
```cypher
MATCH (a)-[r]->(b) WHERE type(r) = 'MemberOf'
-- Rewrite to use type_id index:
-- SELECT ... FROM edges WHERE type_id = (SELECT id FROM rel_types WHERE name = 'MemberOf')
```

**Pattern 3: Type in relationship pattern**
```cypher
MATCH (a)-[:MemberOf]->(b)
-- Already optimized (uses type_id directly)
```

### Implementation Approach

Add optimization pass in query executor:
1. Detect `RETURN DISTINCT type(r)` with no other columns -> use SQL shortcut
2. Detect `WHERE type(r) = '...'` -> push down to SQL with type_id

## Complexity

**Implementation: Medium-High**
- Need to add AST pattern detection
- Modify query planner in `src/crustdb/src/query/executor/`
- ~200-300 lines

**Risk: Medium**
- Query planner changes can have subtle bugs
- Need comprehensive test coverage

## Payoff

**Impact: Medium**
- Benefits ad-hoc Cypher queries in the query editor
- API already optimized (uses direct SQL)
- Main benefit is for users writing custom queries

**E2E showed:** type(r) queries at 1852ms (35x slower than Neo4j)

## Files to Modify

- `src/crustdb/src/query/executor/plan_exec.rs` - Add optimization pass
- `src/crustdb/src/query/executor/pattern.rs` - Detect type() patterns
- Add tests in `src/crustdb/tests/`
