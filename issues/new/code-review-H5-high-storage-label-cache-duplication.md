# Duplicated label cache logic in storage CRUD

## Severity: HIGH

## Problem

In `src/crustdb/src/storage/crud.rs` (lines 113-212), `insert_nodes_batch`
and `upsert_nodes_batch` contain 25+ identical lines for building a label
cache (querying existing labels, inserting new ones, caching IDs).

A bug fix in one function won't propagate to the other.

## Solution

Extract a shared helper:

```rust
fn build_label_cache(
    tx: &Transaction,
    nodes: &[(Vec<String>, Map<String, Value>)],
) -> Result<HashMap<String, i64>> {
    // shared label pre-caching logic
}
```

Call it from both `insert_nodes_batch` and `upsert_nodes_batch`.
