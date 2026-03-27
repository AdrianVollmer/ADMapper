use crate::error::{Error, Result};
use crate::storage::{CacheStats, EntityCache, EntityCacheConfig, EntityCacheStats};

use crate::query::executor::ResourceLimits;

impl super::Database {
    /// Enable or disable query caching.
    ///
    /// When enabled, read-only query results are cached and subsequent
    /// executions of the same query will return cached results. The cache
    /// is automatically invalidated when data is modified.
    pub fn set_caching(&mut self, enabled: bool) {
        self.caching_enabled = enabled;
    }

    /// Check if caching is enabled.
    pub fn caching_enabled(&self) -> bool {
        self.caching_enabled
    }

    /// Clear the query cache.
    pub fn clear_cache(&self) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.clear_query_cache()
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> Result<CacheStats> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.cache_stats()
    }

    /// Configure the entity cache for nodes and relationships.
    ///
    /// The entity cache reduces SQLite lookups during graph traversals (BFS, shortest path)
    /// by caching recently accessed nodes and relationships in memory.
    ///
    /// # Arguments
    /// * `config` - Cache configuration specifying capacity for nodes and relationships.
    ///   Use `EntityCacheConfig::disabled()` to turn off caching.
    ///   Use `EntityCacheConfig::with_capacity(n)` for n entries each.
    ///
    /// # Example
    /// ```ignore
    /// // Enable caching with 10,000 entries each for nodes and relationships
    /// db.set_entity_cache(EntityCacheConfig::with_capacity(10_000));
    ///
    /// // Disable caching
    /// db.set_entity_cache(EntityCacheConfig::disabled());
    /// ```
    pub fn set_entity_cache(&self, config: EntityCacheConfig) {
        let mut cache = self.entity_cache.lock().unwrap();
        *cache = EntityCache::new(config);
    }

    /// Set the maximum number of intermediate bindings allowed per query.
    ///
    /// This acts as a circuit breaker to prevent out-of-memory conditions on
    /// queries that produce explosive intermediate results (cross joins, deep
    /// variable-length path traversals, etc.).
    ///
    /// `None` means unlimited (default). A reasonable starting point for
    /// production use is 1_000_000.
    pub fn set_max_intermediate_bindings(&mut self, limit: Option<usize>) {
        self.max_intermediate_bindings = limit;
    }

    /// Set the maximum BFS frontier entries allowed per query.
    ///
    /// This prevents out-of-memory conditions on shortestPath and
    /// variable-length expand queries over dense graphs. When the BFS
    /// queue exceeds this limit, the query returns an error instead of
    /// consuming all available memory.
    ///
    /// `None` means unlimited (default). A reasonable starting point for
    /// production use is 2_000_000.
    pub fn set_max_frontier_entries(&mut self, limit: Option<usize>) {
        self.max_frontier_entries = limit;
    }

    pub(crate) fn resource_limits(&self) -> ResourceLimits {
        ResourceLimits {
            max_bindings: self.max_intermediate_bindings,
            max_frontier_entries: self.max_frontier_entries,
        }
    }

    /// Get statistics about the entity cache.
    pub fn entity_cache_stats(&self) -> EntityCacheStats {
        let cache = self.entity_cache.lock().unwrap();
        cache.stats()
    }

    /// Clear the entity cache.
    pub fn clear_entity_cache(&self) {
        let mut cache = self.entity_cache.lock().unwrap();
        cache.clear();
    }
}
