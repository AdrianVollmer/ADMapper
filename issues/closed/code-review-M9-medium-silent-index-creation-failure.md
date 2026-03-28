# Index creation errors silently ignored

## Severity: MEDIUM

## Problem

In `src/backend/src/db/falkordb.rs` (line 290):
```rust
let _ = self.run_query(&index_query);
```

And `src/backend/src/db/neo4j.rs` (lines 229-233) similarly discards index
creation results.

If index creation fails (syntax error, DB state issue, permissions), queries
will silently degrade in performance. This is hard to diagnose since the
application appears to work correctly, just slowly.

## Solution

Log index creation failures at `warn` level at minimum:

```rust
if let Err(e) = self.run_query(&index_query) {
    tracing::warn!("Failed to create index: {e}");
}
```

This makes performance issues diagnosable without failing the startup flow.
