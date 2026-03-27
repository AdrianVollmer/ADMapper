# Massive structural duplication between api::core, api::handlers, and tauri_commands

**Severity: HIGH** | **Category: duplicate-code / bad-abstraction**

## Problem

The architecture was designed to share business logic via `api::core` so both
Axum HTTP handlers and Tauri IPC commands can reuse it. In practice, many
handlers re-implement core logic inline, often with subtle differences:

- `handlers::nodes::node_status` (~100 lines duplicated from core, with
  divergent RID list — see separate issue)
- `handlers::paths::check_path_to_condition` (completely different
  implementation, ~90 lines vs ~25 lines in core)
- `handlers::mutation::add_node` / `add_edge` (handler doesn't delegate to
  core, inconsistent with `update_*` and `delete_*` which DO delegate)
- `handlers::database::database_supported` (independently re-implemented)
- `handlers::settings::update_settings` / `browse_directory` (validation
  logic duplicated verbatim)
- `handlers::history::get_query_history` (re-implements pagination + row
  mapping, with subtly different status type: String vs enum)
- `tauri_commands::import_from_paths` (~190 lines re-implementing import
  logic that already exists in core)

Additionally, nearly every struct in `api/core/mod.rs` has an identical twin
in `api/types.rs` (e.g., `DatabaseStatus`, `PathStep`, `PathResponse`,
`BrowseEntry`, `BrowseResponse`, `QueryHistoryEntry`, etc.).

## Solution

1. Delete handler-level re-implementations and have handlers delegate to
   `core::*` functions, adding async wrappers (`spawn_blocking`) where needed.
2. Consolidate types: pick one canonical location (likely `api/types.rs`) and
   remove the duplicates from `core/mod.rs`.
3. For `tauri_commands::import_from_paths`, use `core::import_from_paths`
   with its existing `progress_callback` parameter, adapting it for Tauri
   event emission.
