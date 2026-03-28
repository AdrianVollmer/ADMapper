//! Common types for all database backends.

use serde::Serialize;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use thiserror::Error;

/// A node stored in the database.
///
/// This type is used both for internal storage and API responses.
/// The `label` field is serialized as "type" for API compatibility.
#[derive(Clone, Debug, Serialize)]
pub struct DbNode {
    pub id: String,
    /// Display name of the node (from BloodHound's `name` property).
    pub name: String,
    /// Cypher label (e.g., "User", "Computer", "Group").
    /// Serialized as "type" for API compatibility.
    #[serde(rename = "type")]
    pub label: String,
    pub properties: JsonValue,
}

impl DbNode {
    /// Flatten BloodHound node properties into a single JSON object.
    ///
    /// Merges the nested `properties` from BloodHound into top-level fields,
    /// making them directly queryable in Cypher.
    ///
    /// When `lowercase_keys` is true (Neo4j/FalkorDB), property keys are
    /// lowercased. When false (CrustDB), keys are preserved as-is and the
    /// `label` field is included in the output.
    pub fn flatten_properties(&self, lowercase_keys: bool) -> JsonValue {
        let mut props = Map::new();

        // Add core identifiers
        props.insert("objectid".to_string(), serde_json::json!(self.id));
        props.insert("name".to_string(), serde_json::json!(self.name));

        // CrustDB includes label as a property; Neo4j/FalkorDB use Cypher labels instead
        if !lowercase_keys {
            props.insert("label".to_string(), serde_json::json!(self.label));
        }

        // Flatten BloodHound properties into top-level fields
        if let JsonValue::Object(bh_props) = &self.properties {
            for (key, value) in bh_props {
                // Skip null values and empty arrays to save space
                if value.is_null() {
                    continue;
                }
                if let Some(arr) = value.as_array() {
                    if arr.is_empty() {
                        continue;
                    }
                }
                // Don't overwrite core fields
                let insert_key = if lowercase_keys {
                    key.to_lowercase()
                } else {
                    key.clone()
                };
                if insert_key != "objectid" && insert_key != "name" && insert_key != "label" {
                    props.insert(insert_key, value.clone());
                }
            }
        }

        JsonValue::Object(props)
    }
}

/// Relationship types that represent administrative or privileged access.
///
/// Used when computing admin relationship counts for node details.
pub const ADMIN_RELATIONSHIP_TYPES: &[&str] = &[
    "AdminTo",
    "GenericAll",
    "GenericWrite",
    "Owns",
    "WriteDacl",
    "WriteOwner",
    "AllExtendedRights",
    "ForceChangePassword",
    "AddMember",
];

/// Build a HashSet from `ADMIN_RELATIONSHIP_TYPES` for O(1) lookups.
pub fn admin_types_set() -> HashSet<&'static str> {
    ADMIN_RELATIONSHIP_TYPES.iter().copied().collect()
}

/// Quote-escaping style for Cypher string literals.
#[derive(Clone, Copy)]
#[cfg(any(feature = "crustdb", feature = "neo4j", feature = "falkordb"))]
pub enum CypherEscapeStyle {
    /// Escape single quotes by doubling them: `'` -> `''` (CrustDB)
    DoubleQuote,
    /// Escape single quotes with backslash: `'` -> `\'` (Neo4j, FalkorDB)
    Backslash,
}

/// Convert a JSON object to Cypher property syntax (e.g., `{key: value, ...}`).
#[cfg(any(feature = "crustdb", feature = "neo4j", feature = "falkordb"))]
pub fn json_to_cypher_props(value: &JsonValue, style: CypherEscapeStyle) -> String {
    let obj = match value.as_object() {
        Some(o) => o,
        None => return "{}".to_string(),
    };

    let pairs: Vec<String> = obj
        .iter()
        .filter_map(|(k, v)| {
            let val_str = json_value_to_cypher(v, style)?;
            Some(format!("{}: {}", k, val_str))
        })
        .collect();

    format!("{{{}}}", pairs.join(", "))
}

/// Convert a JSON value to a Cypher literal string.
#[cfg(any(feature = "crustdb", feature = "neo4j", feature = "falkordb"))]
pub fn json_value_to_cypher(value: &JsonValue, style: CypherEscapeStyle) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(b) => Some(b.to_string()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::String(s) => {
            let escaped = match style {
                CypherEscapeStyle::DoubleQuote => s.replace('\'', "''"),
                CypherEscapeStyle::Backslash => s.replace('\\', "\\\\").replace('\'', "\\'"),
            };
            Some(format!("'{}'", escaped))
        }
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr
                .iter()
                .filter_map(|v| json_value_to_cypher(v, style))
                .collect();
            Some(format!("[{}]", items.join(", ")))
        }
        JsonValue::Object(_) => {
            // Skip nested objects - Cypher doesn't support them directly
            None
        }
    }
}

/// An relationship stored in the database.
#[derive(Clone, Debug, Default)]
pub struct DbEdge {
    pub source: String,
    pub target: String,
    pub rel_type: String,
    pub properties: JsonValue,
    /// Optional type hint for source node (for creating placeholders)
    pub source_type: Option<String>,
    /// Optional type hint for target node (for creating placeholders)
    pub target_type: Option<String>,
}

/// Detailed statistics about the database.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DetailedStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub users: usize,
    pub computers: usize,
    pub groups: usize,
    pub domains: usize,
    pub ous: usize,
    pub gpos: usize,
    /// Database file size in bytes (CrustDB only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_size_bytes: Option<usize>,
    /// Number of cached queries (CrustDB only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_entries: Option<usize>,
    /// Total size of cached queries in bytes (CrustDB only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_size_bytes: Option<usize>,
}

/// Security insight for a well-known principal reachability.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ReachabilityInsight {
    pub principal_name: String,
    pub principal_id: Option<String>,
    pub reachable_count: usize,
}

/// Well-known principals to check for reachability in security insights.
///
/// Format: (display_name, SID_pattern)
/// - Patterns starting with '-' are domain-relative SID suffixes
/// - Other patterns are exact well-known SIDs
#[allow(dead_code)]
pub const WELL_KNOWN_PRINCIPALS: &[(&str, &str)] = &[
    ("Everyone", "S-1-1-0"),
    ("Authenticated Users", "S-1-5-11"),
    ("Domain Users", "-513"),
    ("Domain Computers", "-515"),
];

/// SID suffix for Domain Admins group.
#[allow(dead_code)]
pub const DOMAIN_ADMIN_SID_SUFFIX: &str = "-512";

/// Security insights computed from the graph.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SecurityInsights {
    /// Users who have a path to Domain Admins
    pub effective_da_count: usize,
    /// Users who are direct or transitive members of Domain Admins
    pub real_da_count: usize,
    /// Ratio of effective DAs to real DAs
    pub da_ratio: f64,
    /// Total users in the database
    pub total_users: usize,
    /// Percentage of users that are effective DAs
    pub effective_da_percentage: f64,
    /// Objects reachable from well-known principals
    pub reachability: Vec<ReachabilityInsight>,
    /// Users with paths to Domain Admins (for export)
    pub effective_das: Vec<(String, String, usize)>,
    /// Users who are members of Domain Admins (for export)
    pub real_das: Vec<(String, String)>,
}

impl SecurityInsights {
    /// Create SecurityInsights with computed ratios from raw counts.
    ///
    /// This helper reduces duplication across backend implementations.
    pub fn from_counts(
        total_users: usize,
        real_das: Vec<(String, String)>,
        effective_das: Vec<(String, String, usize)>,
        reachability: Vec<ReachabilityInsight>,
    ) -> Self {
        let real_da_count = real_das.len();
        let effective_da_count = effective_das.len();

        let da_ratio = if real_da_count > 0 {
            effective_da_count as f64 / real_da_count as f64
        } else {
            0.0
        };

        let effective_da_percentage = if total_users > 0 {
            (effective_da_count as f64 / total_users as f64) * 100.0
        } else {
            0.0
        };

        Self {
            effective_da_count,
            real_da_count,
            da_ratio,
            total_users,
            effective_da_percentage,
            reachability,
            effective_das,
            real_das,
        }
    }
}

/// A single choke point relationship identified by betweenness centrality.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ChokePoint {
    /// Source node object ID
    pub source_id: String,
    /// Source node name
    pub source_name: String,
    /// Source node label/type
    pub source_label: String,
    /// Target node object ID
    pub target_id: String,
    /// Target node name
    pub target_name: String,
    /// Target node label/type
    pub target_label: String,
    /// Relationship type (e.g., "MemberOf", "GenericAll")
    pub rel_type: String,
    /// Betweenness centrality score (higher = more paths pass through)
    pub betweenness: f64,
    /// Tier of the source node (0 = most critical, 3 = default)
    pub source_tier: i64,
}

/// Labels considered domain/infrastructure objects for filtering.
const DOMAIN_OBJECT_LABELS: &[&str] = &[
    "Domain",
    "OU",
    "GPO",
    "Container",
    "CertTemplate",
    "EnterpriseCA",
    "RootCA",
    "AIACA",
    "NTAuthStore",
];

impl ChokePoint {
    /// Whether the source is an "expected" high-centrality node (tier 0 or domain object).
    pub fn is_expected_source(&self) -> bool {
        self.source_tier == 0 || DOMAIN_OBJECT_LABELS.contains(&self.source_label.as_str())
    }
}

/// Response containing choke point analysis results.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ChokePointsResponse {
    /// Top choke point relationships, sorted by betweenness (highest first)
    pub choke_points: Vec<ChokePoint>,
    /// Top choke points where source is neither tier 0 nor a domain object
    pub unexpected_choke_points: Vec<ChokePoint>,
    /// Total number of relationships analyzed
    pub total_edges: usize,
    /// Total number of nodes in the graph
    pub total_nodes: usize,
}

/// A row from the query history table (owned version for reads).
#[derive(Clone, Debug)]
pub struct QueryHistoryRow {
    pub id: String,
    pub name: String,
    pub query: String,
    pub timestamp: i64,
    pub result_count: Option<i64>,
    pub status: String,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    /// Whether this is a background query (auto-fired, not user-initiated).
    /// Background queries should be ignored when using "back" navigation.
    pub background: bool,
}

/// A new query history entry (borrowed version for inserts).
#[derive(Clone, Debug)]
pub struct NewQueryHistoryEntry<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub query: &'a str,
    pub timestamp: i64,
    pub result_count: Option<i64>,
    pub status: &'a str,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub error: Option<&'a str>,
    pub background: bool,
}

/// Database error type.
#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(feature = "crustdb")]
impl From<crustdb::Error> for DbError {
    fn from(e: crustdb::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "neo4j")]
impl From<neo4rs::Error> for DbError {
    fn from(e: neo4rs::Error) -> Self {
        DbError::Database(e.to_string())
    }
}

#[cfg(feature = "falkordb")]
impl From<falkordb::FalkorDBError> for DbError {
    fn from(e: falkordb::FalkorDBError) -> Self {
        DbError::Database(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DbError>;

/// Normalize BloodHound type name to standard format.
/// This ensures consistent labeling regardless of case in source data.
pub fn normalize_node_type(data_type: &str) -> String {
    match data_type.to_lowercase().as_str() {
        "users" | "user" => "User",
        "groups" | "group" => "Group",
        "computers" | "computer" => "Computer",
        "domains" | "domain" => "Domain",
        "gpos" | "gpo" => "GPO",
        "ous" | "ou" => "OU",
        "containers" | "container" => "Container",
        "certtemplates" | "certtemplate" => "CertTemplate",
        "enterprisecas" | "enterpriseca" => "EnterpriseCA",
        "rootcas" | "rootca" => "RootCA",
        "aiacas" | "aiaca" => "AIACA",
        "ntauthstores" | "ntauthstore" => "NTAuthStore",
        _ => "Base",
    }
    .to_string()
}
