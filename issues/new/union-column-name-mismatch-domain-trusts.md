# UNION Column Name Mismatch in domain-trusts Query

## Problem

The builtin `domain-trusts` query uses different return column names in
its UNION branches:

``` cypher
MATCH (d:Domain) RETURN d
UNION ALL
MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p
```

The first branch returns column `d`, the second returns column `p`. This
violates the openCypher UNION requirement that all branches must have
the same return column names. CrustDB is lenient about this, but both
Neo4j and FalkorDB reject it:

- Neo4j:
  `Neo.ClientError.Statement.SyntaxError: All sub queries in an UNION must have the same return column names`
- FalkorDB: `sub queries in a UNION must have the same column names`

## Affected Tests

- `Consistency: builtin/domain-trusts` -- fails on Neo4j and FalkorDB

## Suggested Fix

Normalize the return column names to a common alias:

``` cypher
MATCH (d:Domain) RETURN d AS result
UNION ALL
MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p AS result
```

The graph extraction logic in the handler already inspects result values
for `_type` markers (node vs path), so the column name itself is
irrelevant for visualization.

Also: CrustDB should behave like the other databases and throw an error
without this fix.

## Files to Modify

- `src/frontend/components/queries/builtin-queries.ts` (line 31)

## Severity

Low. This is a one-line fix.

However, the CrustDB change might be more involved.
