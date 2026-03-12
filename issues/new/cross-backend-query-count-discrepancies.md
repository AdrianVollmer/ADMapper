# Cross-Backend Query Count Discrepancies

## Problem

11 out of 23 cross-backend consistency queries return different result
counts across CrustDB, Neo4j, and FalkorDB. From the 2026-03-12 e2e run:

| Query                        | CrustDB | Neo4j | FalkorDB | Notes                  |
|------------------------------|---------|-------|----------|------------------------|
| builtin/admin-sessions       | 60      | 29    | 26       | Large spread           |
| builtin/all-domain-admins    | 19      | 19    | 16       | FalkorDB low           |
| builtin/genericall           | 3500    | 3293  | 3176     | All different          |
| builtin/genericwrite         | 592     | 587   | 587      | CrustDB high           |
| builtin/high-value-groups    | 26      | 0     | 26       | Neo4j returns 0        |
| builtin/high-value-users     | 39      | 39    | 34       | FalkorDB low           |
| builtin/local-admins         | 2       | 2     | 1        | FalkorDB off by 1      |
| builtin/paths-to-da          | 88      | -     | 10000    | Neo4j timeout, FDB max |
| builtin/writedacl            | 2603    | 2229  | 2226     | Large spread           |
| builtin/writeowner           | 2569    | 2195  | 2169     | Large spread           |
| insights/real-domain-admins  | 20      | 0     | 17       | Neo4j returns 0        |

These discrepancies fall into distinct categories:

### Category 1: Neo4j Returns 0 (Broken Result Extraction)

`high-value-groups` and `real-domain-admins` return 0 on Neo4j. This is
almost certainly the column-guessing bug in `run_custom_query` (see
`neo4j-custom-query-column-guessing.md`). The queries return columns not
in the hardcoded `try_columns` list, so Neo4j reports 0 rows.

### Category 2: FalkorDB Paths-to-DA Returns 10000

FalkorDB returns exactly 10,000 for `paths-to-da` while CrustDB returns
88. This looks like FalkorDB is hitting a result limit cap (default
`GRAPH.QUERY` result set size in FalkorDB/RedisGraph is 10,000). The
query returns paths (`RETURN p`), and with many intermediate hops the
path count explodes. CrustDB likely deduplicates or caps differently.

### Category 3: Genuine Semantic Differences

The remaining discrepancies (admin-sessions, genericall, writedacl,
writeowner, etc.) show graduated differences where all backends return
non-zero values but disagree. Possible causes:

- **Variable-length path semantics**: Untyped `[*1..N]` may traverse
  different relationship types on different backends depending on how
  each engine handles relationship type filtering.
- **DISTINCT handling**: Queries returning paths (`RETURN p`) can produce
  different counts depending on how each backend defines path uniqueness.
- **Import differences**: Subtle differences in how BloodHound JSON is
  imported into each backend (property types, relationship directions).
- **Query plan differences**: Different optimization strategies may yield
  different result sets for queries with implicit deduplication.

## Suggested Investigation

1. **Fix the Neo4j column-guessing bug first** -- this will resolve
   category 1 and may resolve some category 3 differences.
2. **Check FalkorDB result set limit** -- if `GRAPH.QUERY` has a default
   cap of 10,000, add explicit LIMIT or configure the cap. Or change the
   query to `RETURN DISTINCT u` instead of `RETURN p`.
3. **For genuine differences**, pick one query (e.g., `admin-sessions`)
   and compare the actual result sets (not just counts) across backends
   to identify the root cause. Likely candidates:
   - Import fidelity: Do all three backends have the same node/edge counts
     after import?
   - Relationship direction: Are any edges imported with flipped direction
     on some backends?

## Files to Modify

- `src/backend/src/db/neo4j.rs` -- fix result extraction (see sibling issue)
- `src/backend/src/db/falkordb.rs` -- check result set limits
- `e2e/lib/runner.py` -- consider adding a pre-consistency check that
  verifies all backends have identical node/edge counts after import

## Severity

Medium-High. Users switching backends will see different results for the
same queries, which undermines trust in the tool. The Neo4j zeros and the
FalkorDB 10,000 are bugs; the graduated differences need investigation.
