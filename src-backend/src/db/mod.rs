//! Database module for graph storage.
//!
//! Supports multiple backends:
//! - CozoDB (Datalog-based)
//! - KuzuDB (Cypher-based) - currently active

pub mod cozo;
pub mod kuzu;

pub use cozo::{DbEdge, DbError, DbNode, DetailedStats, SecurityInsights};

// Use KuzuDatabase as the active backend
pub use kuzu::KuzuDatabase as GraphDatabase;
