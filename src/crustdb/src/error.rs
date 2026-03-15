//! Error types for CrustDB.

use thiserror::Error;

/// Result type alias for CrustDB operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for CrustDB operations.
#[derive(Error, Debug)]
pub enum Error {
    /// SQLite error.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Cypher parsing error.
    #[error("Cypher parse error: {0}")]
    Parse(String),

    /// Cypher semantic error.
    #[error("Cypher error: {0}")]
    Cypher(String),

    /// Node not found.
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// Relationship not found.
    #[error("Relationship not found: {0}")]
    RelationshipNotFound(String),

    /// Invalid property value.
    #[error("Invalid property: {0}")]
    InvalidProperty(String),

    /// Schema error.
    #[error("Schema error: {0}")]
    Schema(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Query exceeded a resource limit (e.g., intermediate binding count).
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Attempted a write operation on a read-only database.
    #[error("Database is read-only")]
    ReadOnly,

    /// Internal error (e.g., lock poisoned).
    #[error("Internal error: {0}")]
    Internal(String),
}
