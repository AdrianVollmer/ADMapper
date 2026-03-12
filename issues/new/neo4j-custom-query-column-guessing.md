# Neo4j Custom Query Result Extraction is Broken

## Problem

`run_custom_query` in `src/backend/src/db/neo4j.rs` (line 924) cannot
extract column names from Neo4j result streams, so it guesses them from a
hardcoded list:

```rust
let try_columns = [
    "n", "m", "a", "b", "r", "e", "p", "result", "count", "total",
    "name", "id", "value", "nodes", "relationships", "path", "src",
    "tgt", "hops", "node_ids", "rel_types", "source", "target",
    "length", "type",
];
```

This is fundamentally fragile. Any query using a column alias not in this
list produces zero rows, causing `result_count=Some(0)` even when the
query succeeds on the database.

Observable in the server logs: every Neo4j custom query reports
`result_count=Some(0)` and `has_graph=false`, regardless of actual
results.

Missing aliases include: `u`, `c`, `g`, `d`, `t`, `users`, `computers`,
`groups`, `edges`, `enabled`, `labels`, `rel_type`, and any user-defined
alias.

## Affected Tests

On the Neo4j backend:

- `Simple count query` -- `MATCH (n) RETURN count(n) AS total` returns
  empty results dict (the query goes async; the history entry does not
  carry the full results JSON)
- `Perf: Outgoing KNOWS edges` -- `None`
- `Perf: Incoming KNOWS edges` -- `None`
- `Perf: Complex WHERE with AND` -- `None`
- `Perf: Complex WHERE with OR` -- `None`
- `Perf: Shortest path (10 hops)` -- `None`
- `Perf: Combined pattern with filters` -- `None`

Several other query tests pass only because they check `response.ok` but
not the actual result values.

### Interaction with Async Mode

This issue is amplified by the async query flow. When a Neo4j query takes
longer than 50ms, the handler returns async mode. The e2e test client then
polls query history, which stores `result_count` (from the broken row
count) but not the full `results` JSON. The perf tests call
`_extract_count(results, "total")` which needs the full results dict,
so they get `None` for any query that went async.

The split between passing and failing perf tests maps exactly to sync vs
async: queries completing in <50ms return inline (sync) and happen to
contain the `results` dict; queries >50ms go async and the results are
lost.

## Suggested Fix

### Option A: Use neo4rs Column Metadata

Check if newer versions of `neo4rs` expose column names from the result
stream (e.g., `stream.columns()` or `Row::keys()`). This would eliminate
the guessing entirely.

### Option B: Parse Column Names from the Query

Extract `RETURN` clause aliases from the Cypher string using a lightweight
parser. Not robust for complex queries, but covers the common cases.

### Option C: Expand the Hardcoded List (Band-aid)

Add all missing single-letter aliases (`u`, `c`, `g`, `d`, `t`, `x`) and
common multi-letter ones. This is the quick fix but will keep breaking for
new queries.

### Option D: Try All Possible Types for Each Column

Instead of trying known column names, try extracting values positionally.
Some neo4rs Row methods may support index-based access.

Regardless of which approach is chosen, the e2e perf tests should also
use `result_count` from the response body as a fallback when the `results`
dict is not present (async mode).

## Files to Modify

- `src/backend/src/db/neo4j.rs` -- `run_custom_query` (line 924+)
- `e2e/lib/runner.py` -- perf tests should fall back to `result_count`
  for async responses

## Severity

High. This breaks nearly all custom query result extraction on Neo4j.
