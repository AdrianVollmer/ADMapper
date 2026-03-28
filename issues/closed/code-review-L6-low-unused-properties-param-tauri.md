# Unused properties parameter in Tauri commands

## Severity: LOW

## Problem

In `src/backend/src/tauri_commands.rs` (lines 307, 327), `update_node()` and
`update_edge()` accept a `properties` parameter marked with
`#[allow(unused_variables)]`:

```rust
#[allow(unused_variables)]
pub async fn update_node(
    state: State<'_, AppState>,
    id: String,
    properties: HashMap<String, String>,  // ← never used
) -> Result<(), String> { ... }
```

This indicates either an incomplete feature or a copy-paste from a template.

## Solution

Either implement property updates (if the feature is planned) or remove the
parameter from the signature. If it's a planned feature, replace the allow
attribute with a `todo!()` or tracking comment.
