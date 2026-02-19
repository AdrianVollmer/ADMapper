//! Application settings management.
//!
//! Handles persistence of user preferences to XDG_CONFIG_HOME/admapper/settings.json.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Application settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// UI theme: "dark" or "light"
    pub theme: String,
    /// Default graph layout: "force" or "hierarchical"
    pub default_graph_layout: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            default_graph_layout: "force".to_string(),
        }
    }
}

/// Get the path to the settings file.
pub fn settings_path() -> PathBuf {
    let config_dir = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".config")
        });
    config_dir.join("admapper").join("settings.json")
}

/// Load settings from disk, returning defaults if file doesn't exist or is invalid.
pub fn load() -> Settings {
    let path = settings_path();

    if !path.exists() {
        return Settings::default();
    }

    match fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to parse settings file, using defaults");
            Settings::default()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to read settings file, using defaults");
            Settings::default()
        }
    }
}

/// Save settings to disk.
pub fn save(settings: &Settings) -> Result<(), std::io::Error> {
    let path = settings_path();

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let contents = serde_json::to_string_pretty(settings)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    fs::write(&path, contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = Settings::default();
        assert_eq!(settings.theme, "dark");
        assert_eq!(settings.default_graph_layout, "force");
    }

    #[test]
    fn test_settings_serialization() {
        let settings = Settings {
            theme: "light".to_string(),
            default_graph_layout: "hierarchical".to_string(),
        };

        let json = serde_json::to_string(&settings).unwrap();
        assert!(json.contains("\"theme\":\"light\""));
        assert!(json.contains("\"defaultGraphLayout\":\"hierarchical\""));

        let parsed: Settings = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.theme, "light");
        assert_eq!(parsed.default_graph_layout, "hierarchical");
    }
}
