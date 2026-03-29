//! Application settings management.
//!
//! Handles persistence of user preferences to XDG_CONFIG_HOME/admapper/settings.json.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Layout settings (visgraph).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayoutSettings {
    /// Iterations for force-directed layout (1 to 5000, default 1000)
    #[serde(default = "default_iterations")]
    pub iterations: u32,
    /// Initial temperature for force-directed layout (0.01 to 1.0, default 0.1)
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    /// Direction for hierarchical layout
    #[serde(default = "default_direction")]
    pub direction: String,
}

fn default_iterations() -> u32 {
    300
}

fn default_temperature() -> f32 {
    0.1
}

fn default_direction() -> String {
    "left_to_right".to_string()
}

impl Default for LayoutSettings {
    fn default() -> Self {
        Self {
            iterations: default_iterations(),
            temperature: default_temperature(),
            direction: default_direction(),
        }
    }
}

/// Application settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// UI theme: "dark" or "light"
    pub theme: String,
    /// Default graph layout: "force", "hierarchical", or "circular"
    pub default_graph_layout: String,
    /// Whether to enable query caching for CrustDB (default: true)
    #[serde(default = "default_query_caching")]
    pub query_caching: bool,
    /// Layout settings (visgraph)
    #[serde(default)]
    pub layout: LayoutSettings,
    /// Whether nodes and relationships stay same visual size regardless of zoom level
    #[serde(default = "default_fixed_node_sizes")]
    pub fixed_node_sizes: bool,
    /// Nodes with more than this many incoming connections are auto-collapsed on load (0 = disabled)
    #[serde(default = "default_auto_collapse_threshold")]
    pub auto_collapse_threshold: u32,
}

fn default_fixed_node_sizes() -> bool {
    true
}

fn default_query_caching() -> bool {
    true
}

fn default_auto_collapse_threshold() -> u32 {
    20
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            default_graph_layout: "force".to_string(),
            query_caching: true,
            layout: LayoutSettings::default(),
            fixed_node_sizes: true,
            auto_collapse_threshold: default_auto_collapse_threshold(),
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
        assert!(settings.query_caching);
        assert_eq!(settings.layout.iterations, 300);
        assert!((settings.layout.temperature - 0.1).abs() < f32::EPSILON);
        assert_eq!(settings.layout.direction, "left_to_right");
        assert!(settings.fixed_node_sizes);
        assert_eq!(settings.auto_collapse_threshold, 20);
    }

    #[test]
    fn test_settings_serialization() {
        let settings = Settings {
            theme: "light".to_string(),
            default_graph_layout: "hierarchical".to_string(),
            query_caching: false,
            layout: LayoutSettings {
                iterations: 500,
                temperature: 0.05,
                direction: "top_to_bottom".to_string(),
            },
            fixed_node_sizes: false,
            auto_collapse_threshold: 50,
        };

        let json = serde_json::to_string(&settings).unwrap();
        assert!(json.contains("\"theme\":\"light\""));
        assert!(json.contains("\"defaultGraphLayout\":\"hierarchical\""));
        assert!(json.contains("\"queryCaching\":false"));
        assert!(json.contains("\"layout\""));
        assert!(json.contains("\"fixedNodeSizes\":false"));
        assert!(json.contains("\"autoCollapseThreshold\":50"));

        let parsed: Settings = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.theme, "light");
        assert_eq!(parsed.default_graph_layout, "hierarchical");
        assert!(!parsed.query_caching);
        assert_eq!(parsed.layout.iterations, 500);
        assert!((parsed.layout.temperature - 0.05).abs() < f32::EPSILON);
        assert_eq!(parsed.layout.direction, "top_to_bottom");
        assert!(!parsed.fixed_node_sizes);
        assert_eq!(parsed.auto_collapse_threshold, 50);
    }

    #[test]
    fn test_settings_backwards_compatibility() {
        // Old settings files without layout, fixedNodeSizes, or autoCollapseThreshold should still parse
        let json = r#"{"theme":"dark","defaultGraphLayout":"force"}"#;
        let parsed: Settings = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.theme, "dark");
        assert!(parsed.query_caching); // Default to true
        assert!(parsed.fixed_node_sizes); // Default to true
        assert_eq!(parsed.layout.iterations, 300); // Default
        assert_eq!(parsed.auto_collapse_threshold, 20); // Default
    }
}
