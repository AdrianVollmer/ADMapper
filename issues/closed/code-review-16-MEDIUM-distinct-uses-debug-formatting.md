# DISTINCT dedup uses Debug formatting as hash key

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

Multiple places in CrustDB use `format!("{:?}", ...)` as a deduplication key
for DISTINCT:

```rust
// plan_exec/project.rs:33-35
let key = format!("{:?}", row.values);
if seen.insert(key) { unique_rows.push(row); }

// executor/mod.rs:549, 573-576
```

`Debug` formatting is not guaranteed to be a stable or correct equality
representation. Two semantically equal values could have different `Debug`
output (e.g., HashMap iteration order), and two different values could
theoretically have the same `Debug` output.

## Solution

Implement `Hash` and `Eq` on `ResultValue` (or the row type) and use the
values directly as hash keys. Or implement a custom stable serialization
for dedup purposes.
