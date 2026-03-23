//! API request and response types.

use crate::db::{DbError, DbNode};
use crate::graph::FullGraph;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tracing::error;

// ============================================================================
// API Error Type
// ============================================================================

/// API error type with automatic response conversion.
#[derive(Debug)]
pub enum ApiError {
    /// Database operation failed
    Database(DbError),
    /// Invalid request from client
    BadRequest(String),
    /// Requested resource not found
    NotFound(String),
    /// Not connected to a database
    NotConnected,
    /// Internal server error
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Database(e) => write!(f, "Database error: {e}"),
            ApiError::BadRequest(msg) => write!(f, "Bad request: {msg}"),
            ApiError::NotFound(msg) => write!(f, "Not found: {msg}"),
            ApiError::NotConnected => write!(f, "Not connected to a database"),
            ApiError::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<DbError> for ApiError {
    fn from(e: DbError) -> Self {
        error!(error = %e, "Database error");
        ApiError::Database(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Database(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::NotConnected => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Not connected to a database".to_string(),
            ),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        (status, message).into_response()
    }
}

// ============================================================================
// Query Tracking Types
// ============================================================================

/// Status of a running or completed query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Running,
    Completed,
    Failed,
    Aborted,
}

impl std::fmt::Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStatus::Running => write!(f, "running"),
            QueryStatus::Completed => write!(f, "completed"),
            QueryStatus::Failed => write!(f, "failed"),
            QueryStatus::Aborted => write!(f, "aborted"),
        }
    }
}

/// Progress update for a running query.
#[derive(Debug, Clone, Serialize)]
pub struct QueryProgress {
    pub query_id: String,
    pub status: QueryStatus,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub result_count: Option<i64>,
    pub error: Option<String>,
    /// Query results (only populated when status is Completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<JsonValue>,
    /// Extracted graph (only populated when status is Completed and extract_graph was true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<FullGraph>,
}

// ============================================================================
// Database Connection Types
// ============================================================================

/// Database status response.
#[derive(Serialize)]
pub struct DatabaseStatus {
    pub connected: bool,
    pub database_type: Option<String>,
}

/// Supported database info.
#[derive(Serialize)]
pub struct SupportedDatabase {
    pub id: &'static str,
    pub name: &'static str,
    pub connection_type: &'static str,
}

/// Database connect request.
#[derive(Deserialize)]
pub struct ConnectRequest {
    pub url: String,
}

// ============================================================================
// Query Activity Types
// ============================================================================

/// Query activity update (number of active queries changed).
#[derive(Debug, Clone, Serialize)]
pub struct QueryActivity {
    /// Number of currently active queries.
    pub active: usize,
}

// ============================================================================
// Search Types
// ============================================================================

/// Search query parameters.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    20
}

// ============================================================================
// Node Types
// ============================================================================

/// Node connection counts response.
#[derive(Serialize)]
pub struct NodeCounts {
    pub incoming: usize,
    pub outgoing: usize,
    #[serde(rename = "adminTo")]
    pub admin_to: usize,
    #[serde(rename = "memberOf")]
    pub member_of: usize,
    pub members: usize,
}

/// Node security status response.
#[derive(Serialize)]
pub struct NodeStatus {
    /// Is the node owned by the attacker
    pub owned: bool,
    /// Is the node disabled (account disabled in AD)
    #[serde(rename = "isDisabled")]
    pub is_disabled: bool,
    /// Is the node a member of Enterprise Admins (SID -519)
    #[serde(rename = "isEnterpriseAdmin")]
    pub is_enterprise_admin: bool,
    /// Is the node a member of Domain Admins (SID -512)
    #[serde(rename = "isDomainAdmin")]
    pub is_domain_admin: bool,
    /// Tier level (0 = most critical, 3 = default)
    pub tier: i64,
    /// Does the node have a path to a tier-0 target
    #[serde(rename = "hasPathToHighTier")]
    pub has_path_to_high_tier: bool,
    /// Number of hops to the nearest tier-0 target (if hasPathToHighTier)
    #[serde(rename = "pathLength", skip_serializing_if = "Option::is_none")]
    pub path_length: Option<usize>,
}

// ============================================================================
// Tier Types
// ============================================================================

/// Request body for batch-setting tier on filtered nodes.
#[derive(Debug, Deserialize)]
pub struct BatchSetTierRequest {
    /// Tier value to assign (0-3)
    pub tier: i64,
    /// Node type filter (e.g., "User", "Group", "Computer"). Empty = all types.
    #[serde(default)]
    pub node_type: Option<String>,
    /// Regex filter applied to node name. Empty = no filter.
    #[serde(default)]
    pub name_regex: Option<String>,
    /// Assign to all (transitive) members of this group
    #[serde(default)]
    pub group_id: Option<String>,
    /// Assign to all objects contained in this OU (recursive)
    #[serde(default)]
    pub ou_id: Option<String>,
    /// Assign to an explicit list of node IDs (e.g., visible nodes from graph)
    #[serde(default)]
    pub node_ids: Option<Vec<String>>,
}

/// Response for batch tier update.
#[derive(Debug, Serialize)]
pub struct BatchSetTierResponse {
    /// Number of nodes updated
    pub updated: usize,
}

/// Response for compute-effective-tiers endpoint.
#[derive(Debug, Serialize)]
pub struct ComputeEffectiveTiersResponse {
    /// Number of nodes whose effective tier was computed
    pub computed: usize,
    /// Number of nodes where effective_tier < assigned tier (violations)
    pub violations: usize,
}

// ============================================================================
// Tier Violation Types
// ============================================================================

/// A single tier boundary violation category.
#[derive(Debug, Serialize, Clone)]
pub struct TierViolationCategory {
    /// Source zone tier (the lower-privilege side)
    pub source_zone: i64,
    /// Target zone tier (the higher-privilege side)
    pub target_zone: i64,
    /// Number of direct relationships crossing this tier boundary
    pub count: usize,
    /// Sample of violating edges (source_id, target_id, rel_type), capped
    pub edges: Vec<TierViolationEdge>,
}

/// A single violating edge.
#[derive(Debug, Serialize, Clone)]
pub struct TierViolationEdge {
    pub source_id: String,
    pub target_id: String,
    pub rel_type: String,
}

/// Response for the tier violations analysis.
#[derive(Debug, Serialize)]
pub struct TierViolationsResponse {
    pub violations: Vec<TierViolationCategory>,
    /// Total nodes in graph
    pub total_nodes: usize,
    /// Total edges in graph
    pub total_edges: usize,
}

// ============================================================================
// Path Types
// ============================================================================

/// Path query parameters.
#[derive(Debug, Deserialize)]
pub struct PathParams {
    pub from: String,
    pub to: String,
}

/// Path step in the response.
#[derive(Serialize)]
pub struct PathStep {
    pub node: DbNode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rel_type: Option<String>,
}

/// Path response with full graph for rendering.
#[derive(Serialize)]
pub struct PathResponse {
    pub found: bool,
    pub path: Vec<PathStep>,
    pub graph: FullGraph,
}

/// Query parameters for paths to Domain Admins.
#[derive(Debug, Deserialize)]
pub struct PathsToDaParams {
    /// Comma-separated list of relationship types to exclude
    #[serde(default)]
    pub exclude: String,
}

/// Response item for paths to Domain Admins query.
#[derive(Serialize)]
pub struct PathsToDaEntry {
    pub id: String,
    #[serde(rename = "type")]
    pub label: String,
    pub name: String,
    pub hops: usize,
}

/// Response for paths to Domain Admins query.
#[derive(Serialize)]
pub struct PathsToDaResponse {
    pub count: usize,
    pub entries: Vec<PathsToDaEntry>,
}

// ============================================================================
// Node/Relationship Mutation Types
// ============================================================================

/// Request body for adding a node.
#[derive(Deserialize)]
pub struct AddNodeRequest {
    pub id: String,
    pub name: String,
    pub label: String,
    #[serde(default)]
    pub properties: JsonValue,
}

/// Request body for adding an relationship.
#[derive(Deserialize)]
pub struct AddEdgeRequest {
    pub source: String,
    pub target: String,
    pub rel_type: String,
    #[serde(default)]
    pub properties: JsonValue,
}

// ============================================================================
// Query Types
// ============================================================================

/// Custom query request body.
#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    /// If true, try to extract a graph from the query results
    #[serde(default)]
    pub extract_graph: bool,
    /// Query language (optional, defaults to backend's default)
    #[serde(default)]
    pub language: Option<String>,
    /// If true, mark as background query (excluded from back navigation)
    #[serde(default)]
    pub background: bool,
    /// If true, wait for query completion instead of returning async mode.
    /// Useful for programmatic clients that always want inline results.
    #[serde(default)]
    pub sync: bool,
}

/// Response when starting a query.
/// Can be either sync (results inline) or async (query_id for progress subscription).
#[derive(Serialize)]
#[serde(tag = "mode")]
pub enum QueryStartResponse {
    /// Query completed synchronously - results are inline.
    #[serde(rename = "sync")]
    Sync {
        query_id: String,
        duration_ms: u64,
        result_count: Option<i64>,
        results: Option<JsonValue>,
        graph: Option<FullGraph>,
    },
    /// Query is running asynchronously - subscribe to progress events.
    #[serde(rename = "async")]
    Async { query_id: String },
}

// ============================================================================
// Query History Types
// ============================================================================

/// Query history entry.
#[derive(Serialize)]
pub struct QueryHistoryEntry {
    pub id: String,
    pub name: String,
    pub query: String,
    pub timestamp: i64,
    pub result_count: Option<i64>,
    pub status: QueryStatus,
    pub started_at: i64,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    /// Whether this is a background query (auto-fired, not user-initiated).
    pub background: bool,
}

/// Query history response with pagination.
#[derive(Serialize)]
pub struct QueryHistoryResponse {
    pub entries: Vec<QueryHistoryEntry>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Query history pagination params.
#[derive(Debug, Deserialize)]
pub struct HistoryParams {
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
}

fn default_page() -> usize {
    1
}

fn default_per_page() -> usize {
    20
}

/// Add query history request.
#[derive(Debug, Deserialize)]
pub struct AddHistoryRequest {
    pub name: String,
    pub query: String,
    pub result_count: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub background: bool,
}

// ============================================================================
// Generate Data Types
// ============================================================================

/// Data generation size preset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GenerateSize {
    Small,
    Medium,
    Large,
}

/// Request to generate sample data.
#[derive(Debug, Deserialize)]
pub struct GenerateRequest {
    pub size: GenerateSize,
}

/// Response after generating sample data.
#[derive(Debug, Serialize)]
pub struct GenerateResponse {
    pub nodes: usize,
    pub relationships: usize,
}

// ============================================================================
// File Browser Types
// ============================================================================

/// File browser request params.
#[derive(Debug, Deserialize)]
pub struct BrowseParams {
    /// Directory path to browse (defaults to home directory)
    pub path: Option<String>,
}

/// A file or directory entry.
#[derive(Debug, Serialize)]
pub struct BrowseEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
}

/// File browser response.
#[derive(Debug, Serialize)]
pub struct BrowseResponse {
    /// Current directory path
    pub current: String,
    /// Parent directory path (None if at root)
    pub parent: Option<String>,
    /// Entries in the directory
    pub entries: Vec<BrowseEntry>,
}
