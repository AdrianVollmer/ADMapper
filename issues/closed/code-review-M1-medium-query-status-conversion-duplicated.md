# Duplicated query history status conversion

## Severity: MEDIUM

## Problem

Status string-to-enum conversion appears in two places with subtly different
match arms:

- `src/backend/src/api/handlers/history.rs` (lines 34-40): explicitly
  matches "running", "completed", "failed", "aborted"
- `src/backend/src/api/core/query.rs` (lines 78-84): matches "running",
  "failed", "aborted" with a catch-all default

If status values change, both locations must be updated.

## Solution

Implement `From<&str>` for `QueryStatus` in `api/types.rs`:

```rust
impl From<&str> for QueryStatus {
    fn from(s: &str) -> Self {
        match s {
            "running" => Self::Running,
            "failed" => Self::Failed,
            "aborted" => Self::Aborted,
            _ => Self::Completed,
        }
    }
}
```
