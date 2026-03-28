//! CrustDB-backed graph database for storing AD graph data.
//!
//! Uses Cypher queries for graph operations via the embedded crustdb engine.

mod algorithms;
mod backend_impl;
mod connections;
mod edges;
mod insights;
mod nodes;
mod query;
#[cfg(test)]
mod tests;

use crustdb::{Database, EntityCacheConfig};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use super::types::{DbError, DetailedStats, Result};

/// A graph database backed by CrustDB.
///
/// Database handles its own thread-safety internally via Mutex.
/// For concurrent queries, a connection pool would be needed.
#[derive(Clone)]
pub struct CrustDatabase {
    db: Arc<Database>,
}

impl CrustDatabase {
    /// Create or open a database at the given path.
    ///
    /// If `enable_caching` is true, query results for read-only queries will be cached
    /// and automatically invalidated when data changes.
    pub fn new<P: AsRef<Path>>(path: P, enable_caching: bool) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!(path = %path_str, caching = %enable_caching, "Opening CrustDB");

        let mut db = Database::open(&path_str).map_err(|e| DbError::Database(e.to_string()))?;
        db.set_caching(enable_caching);
        // Enable entity cache for faster BFS/shortest path traversals
        db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));
        // Cap intermediate bindings to prevent OOM on explosive queries
        db.set_max_intermediate_bindings(Some(5_000_000));

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        info!("CrustDB initialized successfully");
        Ok(instance)
    }

    /// Create an in-memory database (useful for testing).
    pub fn in_memory() -> Result<Self> {
        debug!("Creating in-memory CrustDB");
        let mut db = Database::in_memory().map_err(|e| DbError::Database(e.to_string()))?;
        db.set_caching(true); // Enable caching by default for tests too
                              // Enable entity cache for faster BFS/shortest path traversals
        db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));
        // Cap intermediate bindings to prevent OOM on explosive queries
        db.set_max_intermediate_bindings(Some(5_000_000));

        let instance = Self { db: Arc::new(db) };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Initialize the schema by creating indexes and base structures.
    fn init_schema(&self) -> Result<()> {
        debug!("Initializing CrustDB schema");
        // CrustDB auto-creates nodes/relationships on first use

        // Create property indexes for commonly queried fields
        // These significantly speed up node lookups by objectid and name
        self.db
            .create_property_index("objectid")
            .map_err(|e| DbError::Database(e.to_string()))?;
        self.db
            .create_property_index("name")
            .map_err(|e| DbError::Database(e.to_string()))?;

        debug!("Property indexes created for objectid and name");
        Ok(())
    }

    /// Execute a Cypher query and return the raw result.
    pub(crate) fn execute(&self, query: &str) -> Result<crustdb::QueryResult> {
        self.db
            .execute(query)
            .map_err(|e| DbError::Database(e.to_string()))
    }

    /// Clear all data from the database.
    pub fn clear(&self) -> Result<()> {
        info!("Clearing all data from CrustDB");
        self.db
            .clear()
            .map_err(|e| DbError::Database(e.to_string()))?;
        debug!("Database cleared");
        Ok(())
    }

    /// Get node and relationship counts.
    ///
    /// Uses efficient SQL via CrustDB's stats() method instead of Cypher queries.
    pub fn get_stats(&self) -> Result<(usize, usize)> {
        let stats = self
            .db
            .stats()
            .map_err(|e| DbError::Database(e.to_string()))?;
        Ok((stats.node_count, stats.relationship_count))
    }

    /// Get detailed stats including counts by node type.
    ///
    /// Uses efficient SQL queries via get_label_counts() instead of
    /// multiple Cypher queries, reducing ~5 seconds to ~50ms.
    pub fn get_detailed_stats(&self) -> Result<DetailedStats> {
        // Get basic stats (2 fast SQL queries)
        let stats = self
            .db
            .stats()
            .map_err(|e| DbError::Database(e.to_string()))?;

        // Get label counts excluding placeholder nodes (created during
        // relationship import for referenced-but-not-yet-imported nodes).
        let label_counts = self
            .db
            .get_label_counts_excluding("placeholder")
            .map_err(|e| DbError::Database(e.to_string()))?;

        // Get database size and cache stats
        let database_size = self
            .db
            .database_size()
            .map_err(|e| DbError::Database(e.to_string()))?;
        let cache_stats = self
            .db
            .cache_stats()
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(DetailedStats {
            total_nodes: stats.node_count,
            total_edges: stats.relationship_count,
            users: label_counts.get("User").copied().unwrap_or(0),
            computers: label_counts.get("Computer").copied().unwrap_or(0),
            groups: label_counts.get("Group").copied().unwrap_or(0),
            domains: label_counts.get("Domain").copied().unwrap_or(0),
            ous: label_counts.get("OU").copied().unwrap_or(0),
            gpos: label_counts.get("GPO").copied().unwrap_or(0),
            database_size_bytes: Some(database_size),
            cache_entries: Some(cache_stats.entry_count),
            cache_size_bytes: Some(cache_stats.total_size_bytes),
        })
    }

    /// Helper to extract string value from result row.
    pub(crate) fn get_string_value(
        &self,
        values: &std::collections::HashMap<String, crustdb::ResultValue>,
        key: &str,
    ) -> String {
        values
            .get(key)
            .and_then(|v| match v {
                crustdb::ResultValue::Property(crustdb::PropertyValue::String(s)) => {
                    Some(s.clone())
                }
                crustdb::ResultValue::Property(crustdb::PropertyValue::Integer(n)) => {
                    Some(n.to_string())
                }
                _ => None,
            })
            .unwrap_or_default()
    }

    // Choke points: uses default DatabaseBackend::get_choke_points() which loads
    // all nodes/edges once and runs Brandes' algorithm in-memory via algorithms.rs.
    // The previous CrustDB-specific override ran per-edge Cypher queries to resolve
    // node metadata, causing O(E) query overhead.
}
