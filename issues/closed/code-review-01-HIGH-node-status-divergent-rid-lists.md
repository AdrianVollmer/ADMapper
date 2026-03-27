# node_status implemented twice with divergent tier-0 RID lists

**Severity: HIGH** | **Category: inconsistency / bug**

## Problem

`node_status` is implemented independently in two places:

- `src/backend/src/api/core/nodes.rs:175` (used by Tauri desktop path)
- `src/backend/src/api/handlers/nodes.rs:178` (used by HTTP web path)

The handler re-implements ~100 lines of the core function instead of
delegating. Critically, the tier-0 RID lists differ:

```rust
// core/nodes.rs:175
const OTHER_TIER_ZERO_RIDS: &[&str] = &["-518", "-516", "-498", "-544", "-548", "-549", "-551"];

// handlers/nodes.rs:178
const OTHER_TIER_ZERO_RIDS: &[&str] = &[
    "-518", "-516", "-498", "-S-1-5-9", "-544", "-548", "-549", "-551",
];
```

The handler includes `"-S-1-5-9"` (Enterprise Domain Controllers) while the
core does not. This means the **Tauri desktop app and the HTTP web server
produce different security status results for the same node**.

## Solution

Delete the inline implementation in `handlers/nodes.rs` and have it delegate
to `core::node_status_full`, wrapping with `spawn_blocking` for async. Decide
whether `"-S-1-5-9"` should be included (it probably should), and have the
single canonical list in `core/nodes.rs`.
