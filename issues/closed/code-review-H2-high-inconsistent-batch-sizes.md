# Inconsistent batch sizes across database backends

## Severity: HIGH

## Problem

FalkorDB uses batch size 500 for both nodes and edges
(`falkordb.rs:325,372`), while Neo4j uses 500 for nodes but 1000 for edges
(`neo4j.rs:267,313`).

This means import behavior differs silently between backends with no
documented reason for the discrepancy. Likely a copy-paste artifact where one
was changed but not the other.

## Solution

Define batch size constants in a shared location (e.g. `db/types.rs`) and
use them in both backends. Document the rationale if different sizes are
intentional per backend.

```rust
pub const NODE_BATCH_SIZE: usize = 500;
pub const EDGE_BATCH_SIZE: usize = 500;
```

This will also be naturally resolved by the C1 shared backend refactor.
