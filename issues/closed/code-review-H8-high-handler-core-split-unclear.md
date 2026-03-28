# Handler/Core split is unclear and inconsistent

## Severity: HIGH

## Problem

The backend uses a `api/handlers/` + `api/core/` split intended to share
logic between Axum HTTP handlers and Tauri commands. In practice:

- `handlers/query.rs` (556 lines) contains massive query dedup, progress
  broadcasting, SSE streaming, and cancellation logic that doesn't exist in
  core. Tauri commands get a much simpler code path.
- Some handlers are thin wrappers around core; others contain significant
  business logic.
- `CacheStats` struct is defined identically in both `tauri_commands.rs` and
  `handlers/settings.rs`.

The "shared core" pattern is violated — the two entry points have different
capabilities and behavior.

## Solution

Move query orchestration logic (dedup, progress, cancellation) into core so
both Tauri and HTTP paths use the same implementation. Handlers should only
do protocol conversion (HTTP request → core call → HTTP response).

Deduplicate `CacheStats` by moving it to `api/types.rs`.

Clarify the rule: core owns business logic, handlers own serialization.
