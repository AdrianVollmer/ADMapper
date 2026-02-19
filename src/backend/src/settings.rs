//! Application settings management.
//!
//! Handles persistence of user preferences to XDG_CONFIG_HOME/admapper/settings.json.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Force layout settings for graph visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceLayoutSettings {
    /// Gravity - how strongly nodes pull toward center (0.1 to 2.0)
    #[serde(default = "default_gravity")]
    pub gravity: f64,
    /// Scaling ratio - how spread out nodes are (1 to 50)
    #[serde(default = "default_scaling_ratio")]
    pub scaling_ratio: f64,
    /// Whether to prevent node overlap
    #[serde(default = "default_adjust_sizes")]
    pub adjust_sizes: bool,
}

fn default_gravity() -> f64 {
    0.5
}

fn default_scaling_ratio() -> f64 {
    10.0
}

fn default_adjust_sizes() -> bool {
    true
}

impl Default for ForceLayoutSettings {
    fn default() -> Self {
        Self {
            gravity: default_gravity(),
            scaling_ratio: default_scaling_ratio(),
            adjust_sizes: default_adjust_sizes(),
        }
    }
}

/// Application settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// UI theme: "dark" or "light"
    pub theme: String,
    /// Default graph layout: "force", "hierarchical", "grid", or "circular"
    pub default_graph_layout: String,
    /// Whether to enable query caching for CrustDB (default: true)
    #[serde(default = "default_query_caching")]
    pub query_caching: bool,
    /// Force layout settings
    #[serde(default)]
    pub force_layout: ForceLayoutSettings,
}

fn default_query_caching() -> bool {
    true
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            default_graph_layout: "force".to_string(),
            query_caching: true,
            force_layout: ForceLayoutSettings::default(),
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
        assert!((settings.force_layout.gravity - 0.5).abs() < f64::EPSILON);
        assert!((settings.force_layout.scaling_ratio - 10.0).abs() < f64::EPSILON);
        assert!(settings.force_layout.adjust_sizes);
    }

    #[test]
    fn test_settings_serialization() {
        let settings = Settings {
            theme: "light".to_string(),
            default_graph_layout: "hierarchical".to_string(),
            query_caching: false,
            force_layout: ForceLayoutSettings {
                gravity: 1.0,
                scaling_ratio: 20.0,
                adjust_sizes: false,
            },
        };

        let json = serde_json::to_string(&settings).unwrap();
        assert!(json.contains("\"theme\":\"light\""));
        assert!(json.contains("\"defaultGraphLayout\":\"hierarchical\""));
        assert!(json.contains("\"queryCaching\":false"));
        assert!(json.contains("\"forceLayout\""));

        let parsed: Settings = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.theme, "light");
        assert_eq!(parsed.default_graph_layout, "hierarchical");
        assert!(!parsed.query_caching);
        assert!((parsed.force_layout.gravity - 1.0).abs() < f64::EPSILON);
        assert!((parsed.force_layout.scaling_ratio - 20.0).abs() < f64::EPSILON);
        assert!(!parsed.force_layout.adjust_sizes);
    }

    #[test]
    fn test_settings_backwards_compatibility() {
        // Old settings files without query_caching should still parse
        let json = r#"{"theme":"dark","defaultGraphLayout":"force"}"#;
        let parsed: Settings = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.theme, "dark");
        assert!(parsed.query_caching); // Default to true
    }
}
