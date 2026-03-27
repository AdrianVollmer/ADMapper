//! Parity test: ensures every frontend Tauri command mapping has a
//! corresponding Tauri command registered in the invoke_handler.
//!
//! This prevents the recurring bug where a new API endpoint is added to the
//! HTTP router and the frontend COMMAND_MAPPING, but the Tauri command
//! wrapper is forgotten, causing the endpoint to fail silently in desktop mode.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Resolve a path relative to the workspace root (two levels up from Cargo.toml).
fn workspace_path(relative: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // src/backend -> workspace root
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("Cannot find workspace root")
        .join(relative)
}

/// Extract Tauri command names from the frontend COMMAND_MAPPING in client.ts.
///
/// Parses lines like: `"GET /api/graph/tier-violations": "tier_violations",`
fn extract_frontend_command_names() -> BTreeSet<String> {
    let path = workspace_path("src/frontend/api/client.ts");
    let client_ts = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {e}", path.display()));

    let mut in_mapping = false;
    let mut commands = BTreeSet::new();

    for line in client_ts.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("const COMMAND_MAPPING") {
            in_mapping = true;
            continue;
        }
        if in_mapping && trimmed == "};" {
            break;
        }
        if in_mapping {
            // Lines look like: "GET /api/graph/node/:id": "node_get",
            // Keys can contain colons (URL params), so split on the last `": "`
            // which separates the TS object key from its value.
            if let Some(sep_pos) = trimmed.rfind("\": ") {
                let value_part = &trimmed[sep_pos + 3..];
                let name = value_part
                    .trim_matches(|c: char| c == '"' || c == '\'' || c == ',' || c.is_whitespace());
                if !name.is_empty() {
                    commands.insert(name.to_string());
                }
            }
        }
    }

    assert!(
        !commands.is_empty(),
        "Failed to parse any commands from COMMAND_MAPPING in client.ts"
    );
    commands
}

/// Extract registered Tauri command names from the invoke_handler block in lib.rs.
///
/// Parses lines like: `tauri_commands::tier_violations,`
fn extract_registered_tauri_commands() -> BTreeSet<String> {
    let path = workspace_path("src/backend/src/lib.rs");
    let lib_rs = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {e}", path.display()));

    let mut in_handler = false;
    let mut commands = BTreeSet::new();

    for line in lib_rs.lines() {
        let trimmed = line.trim();
        if trimmed.contains("invoke_handler(tauri::generate_handler![") {
            in_handler = true;
            continue;
        }
        if in_handler && trimmed.starts_with("])") {
            break;
        }
        if in_handler {
            // Lines look like: tauri_commands::command_name,
            let cleaned = trimmed.trim_start_matches("//").trim();
            if let Some(name) = cleaned.strip_prefix("tauri_commands::") {
                let name = name.trim_end_matches(',').trim();
                if !name.is_empty() && !name.starts_with("//") {
                    commands.insert(name.to_string());
                }
            }
        }
    }

    assert!(
        !commands.is_empty(),
        "Failed to parse any commands from invoke_handler in lib.rs"
    );
    commands
}

#[test]
fn test_every_frontend_command_has_tauri_handler() {
    let frontend_commands = extract_frontend_command_names();
    let registered_commands = extract_registered_tauri_commands();

    let missing: Vec<&String> = frontend_commands
        .iter()
        .filter(|cmd| !registered_commands.contains(cmd.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "Frontend COMMAND_MAPPING references Tauri commands that are NOT registered \
         in the invoke_handler.\n\
         Missing commands: {:?}\n\n\
         To fix: add a #[tauri::command] wrapper in tauri_commands.rs and register \
         it in the invoke_handler in lib.rs.\n\
         Registered commands: {:?}",
        missing,
        registered_commands
    );
}
