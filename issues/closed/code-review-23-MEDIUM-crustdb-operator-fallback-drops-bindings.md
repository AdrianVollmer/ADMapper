# CrustDB: execute_operator_on_bindings silently drops bindings in fallback

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

In `plan_exec/mod.rs:609-611`:

```rust
// Fallback: execute normally (ignoring provided bindings)
_ => execute_operator(op, storage, ctx, cache),
```

The `execute_operator_on_bindings` function silently discards the `bindings`
parameter for any operator not explicitly handled and falls back to the
non-binding version. If a caller passes bindings expecting them to be used,
this produces wrong results without any error.

## Solution

Either:
1. Return an error for unhandled operators ("operator X does not support bindings")
2. Or ensure all relevant operators are handled explicitly
3. At minimum, add a `debug_assert!` or log warning when bindings are dropped
