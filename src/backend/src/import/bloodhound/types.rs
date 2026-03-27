//! Type definitions used across the BloodHound importer module.

use serde::Deserialize;
use serde_json::value::RawValue;

/// BloodHound file metadata.
#[derive(Debug, Deserialize)]
pub(super) struct BloodHoundMeta {
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub version: Option<i32>,
}

/// BloodHound file structure with lazy parsing.
/// Uses RawValue to defer parsing of individual entities until needed,
/// reducing peak memory usage for large files.
#[derive(Debug, Deserialize)]
pub(super) struct BloodHoundFile<'a> {
    pub meta: Option<BloodHoundMeta>,
    #[serde(borrow)]
    pub data: Vec<&'a RawValue>,
}
