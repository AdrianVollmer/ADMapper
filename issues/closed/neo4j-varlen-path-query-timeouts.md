# Neo4j Variable-Length Path Queries Timeout

## Problem

Two consistency queries hit the 60-second timeout on Neo4j:

1. **builtin/paths-to-da** (60,260ms):
   ```cypher
   MATCH p = (u:User)-[*1..5]->(da:Group)
   WHERE da.objectid ENDS WITH '-512'
   RETURN p
   ```

2. **insights/effective-domain-admins** (60,331ms on Neo4j, also fails on
   FalkorDB at 1,055ms with timeout):
   ```cypher
   MATCH (u:User)-[*1..10]->(g:Group)
   WHERE g.objectid ENDS WITH '-512'
   RETURN DISTINCT u
   ```

Both use **untyped** variable-length path patterns (`[*1..N]`), meaning
Neo4j explores ALL relationship types at every hop. With even moderate
graph density this causes combinatorial explosion.

CrustDB handles these in 2.5s and 3s respectively, likely because its
query planner is more aggressive about pruning. FalkorDB handles
paths-to-da (3.5s) but times out on effective-domain-admins.

## Affected Tests

- `Consistency: builtin/paths-to-da` -- Neo4j timeout
- `Consistency: insights/effective-domain-admins` -- Neo4j timeout,
  FalkorDB timeout
- `Cross-backend: builtin/paths-to-da` -- cannot compare (Neo4j missing)

## Suggested Fix

### Option A: Constrain Relationship Types

The paths to Domain Admins realistically use a bounded set of
relationship types (MemberOf, AdminTo, GenericAll, WriteDacl, etc.).
Constraining the path pattern dramatically reduces the search space:

```cypher
MATCH p = (u:User)-[:MemberOf|AdminTo|GenericAll|WriteDacl|...
  *1..5]->(da:Group)
WHERE da.objectid ENDS WITH '-512'
RETURN p
```

The `insights/real-domain-admins` query already does this (uses
`[:MemberOf*1..10]`) and completes in <1s on all backends.

### Option B: Backend-Specific Query Variants

For complex path queries, maintain per-backend query variants that
account for each engine's optimizer capabilities. This is already done
for `shortestPath` syntax in the e2e perf tests.

### Option C: Increase Timeout with Index Hints

Add Neo4j-specific index hints and increase the timeout. Less
desirable since it doesn't fix the underlying performance cliff.

## Files to Modify

- `src/frontend/components/queries/builtin-queries.ts` -- paths-to-da
  query (line ~205)
- `src/backend/src/db/neo4j.rs` -- `find_paths_to_domain_admins`
  (line 875), consider constraining relationship types
- `e2e/lib/runner.py` -- `EXTRA_CONSISTENCY_QUERIES` for
  effective-domain-admins (line 1352)

## Severity

Medium. Affects Neo4j users running path analysis on medium/large
graphs. The timeouts make these queries unusable on Neo4j.
