//! Database module for graph storage using CozoDB.

mod cozo;

pub use cozo::{
    DbEdge, DbError, DbNode, DetailedStats, GraphDatabase, ReachabilityInsight, SecurityInsights,
};
