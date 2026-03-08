//! LRU cache for nodes and relationships to reduce SQLite lookups during traversals.
//!
//! This cache significantly improves performance for BFS/DFS operations by avoiding
//! repeated SQLite queries for the same nodes and relationships. It's especially
//! effective for shortest path queries where the same entities are accessed multiple
//! times during path reconstruction.

use crate::graph::{Node, Relationship};
use lru::LruCache;
use std::num::NonZeroUsize;

/// Default cache capacity (number of entries per cache type).
pub const DEFAULT_CACHE_CAPACITY: usize = 10_000;

/// Configuration for the entity cache.
#[derive(Debug, Clone, Copy)]
pub struct EntityCacheConfig {
    /// Maximum number of nodes to cache. Set to 0 to disable node caching.
    pub node_capacity: usize,
    /// Maximum number of relationships to cache. Set to 0 to disable relationship caching.
    pub relationship_capacity: usize,
}

impl Default for EntityCacheConfig {
    fn default() -> Self {
        Self {
            node_capacity: DEFAULT_CACHE_CAPACITY,
            relationship_capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}

impl EntityCacheConfig {
    /// Create a config with the same capacity for both nodes and relationships.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            node_capacity: capacity,
            relationship_capacity: capacity,
        }
    }

    /// Create a disabled cache config (capacity 0).
    pub fn disabled() -> Self {
        Self {
            node_capacity: 0,
            relationship_capacity: 0,
        }
    }
}

/// LRU cache for nodes and relationships.
///
/// This cache is designed to be passed to query execution functions to avoid
/// repeated database lookups for the same entities during graph traversals.
pub struct EntityCache {
    nodes: Option<LruCache<i64, Node>>,
    relationships: Option<LruCache<i64, Relationship>>,
    /// Statistics
    node_hits: usize,
    node_misses: usize,
    relationship_hits: usize,
    relationship_misses: usize,
}

/// Statistics about cache usage.
#[derive(Debug, Clone, Copy, Default)]
pub struct EntityCacheStats {
    /// Number of nodes currently in cache.
    pub node_count: usize,
    /// Maximum node capacity.
    pub node_capacity: usize,
    /// Number of node cache hits.
    pub node_hits: usize,
    /// Number of node cache misses.
    pub node_misses: usize,
    /// Number of relationships currently in cache.
    pub relationship_count: usize,
    /// Maximum relationship capacity.
    pub relationship_capacity: usize,
    /// Number of relationship cache hits.
    pub relationship_hits: usize,
    /// Number of relationship cache misses.
    pub relationship_misses: usize,
}

impl EntityCacheStats {
    /// Calculate node hit rate (0.0 to 1.0).
    pub fn node_hit_rate(&self) -> f64 {
        let total = self.node_hits + self.node_misses;
        if total == 0 {
            0.0
        } else {
            self.node_hits as f64 / total as f64
        }
    }

    /// Calculate relationship hit rate (0.0 to 1.0).
    pub fn relationship_hit_rate(&self) -> f64 {
        let total = self.relationship_hits + self.relationship_misses;
        if total == 0 {
            0.0
        } else {
            self.relationship_hits as f64 / total as f64
        }
    }
}

impl EntityCache {
    /// Create a new entity cache with the given configuration.
    pub fn new(config: EntityCacheConfig) -> Self {
        let nodes = NonZeroUsize::new(config.node_capacity).map(LruCache::new);
        let relationships = NonZeroUsize::new(config.relationship_capacity).map(LruCache::new);

        Self {
            nodes,
            relationships,
            node_hits: 0,
            node_misses: 0,
            relationship_hits: 0,
            relationship_misses: 0,
        }
    }

    /// Create a cache with default capacity.
    pub fn with_default_capacity() -> Self {
        Self::new(EntityCacheConfig::default())
    }

    /// Create a cache with the specified capacity for both nodes and relationships.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(EntityCacheConfig::with_capacity(capacity))
    }

    /// Check if node caching is enabled.
    pub fn nodes_enabled(&self) -> bool {
        self.nodes.is_some()
    }

    /// Check if relationship caching is enabled.
    pub fn relationships_enabled(&self) -> bool {
        self.relationships.is_some()
    }

    /// Get a node from the cache.
    pub fn get_node(&mut self, id: i64) -> Option<&Node> {
        if let Some(ref mut cache) = self.nodes {
            if let Some(node) = cache.get(&id) {
                self.node_hits += 1;
                Some(node)
            } else {
                self.node_misses += 1;
                None
            }
        } else {
            None
        }
    }

    /// Insert a node into the cache.
    pub fn insert_node(&mut self, node: Node) {
        if let Some(ref mut cache) = self.nodes {
            cache.put(node.id, node);
        }
    }

    /// Get a relationship from the cache.
    pub fn get_relationship(&mut self, id: i64) -> Option<&Relationship> {
        if let Some(ref mut cache) = self.relationships {
            if let Some(rel) = cache.get(&id) {
                self.relationship_hits += 1;
                Some(rel)
            } else {
                self.relationship_misses += 1;
                None
            }
        } else {
            None
        }
    }

    /// Insert a relationship into the cache.
    pub fn insert_relationship(&mut self, rel: Relationship) {
        if let Some(ref mut cache) = self.relationships {
            cache.put(rel.id, rel);
        }
    }

    /// Clear all cached entries.
    pub fn clear(&mut self) {
        if let Some(ref mut cache) = self.nodes {
            cache.clear();
        }
        if let Some(ref mut cache) = self.relationships {
            cache.clear();
        }
        self.node_hits = 0;
        self.node_misses = 0;
        self.relationship_hits = 0;
        self.relationship_misses = 0;
    }

    /// Get cache statistics.
    pub fn stats(&self) -> EntityCacheStats {
        EntityCacheStats {
            node_count: self.nodes.as_ref().map(|c| c.len()).unwrap_or(0),
            node_capacity: self.nodes.as_ref().map(|c| c.cap().get()).unwrap_or(0),
            node_hits: self.node_hits,
            node_misses: self.node_misses,
            relationship_count: self.relationships.as_ref().map(|c| c.len()).unwrap_or(0),
            relationship_capacity: self
                .relationships
                .as_ref()
                .map(|c| c.cap().get())
                .unwrap_or(0),
            relationship_hits: self.relationship_hits,
            relationship_misses: self.relationship_misses,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_node(id: i64) -> Node {
        Node {
            id,
            labels: vec!["Test".to_string()],
            properties: HashMap::new(),
        }
    }

    fn make_relationship(id: i64) -> Relationship {
        Relationship {
            id,
            source: 1,
            target: 2,
            rel_type: "KNOWS".to_string(),
            properties: HashMap::new(),
        }
    }

    #[test]
    fn test_node_cache_hit_miss() {
        let mut cache = EntityCache::with_capacity(10);

        // Miss
        assert!(cache.get_node(1).is_none());
        assert_eq!(cache.stats().node_misses, 1);

        // Insert
        cache.insert_node(make_node(1));

        // Hit
        assert!(cache.get_node(1).is_some());
        assert_eq!(cache.stats().node_hits, 1);
    }

    #[test]
    fn test_relationship_cache_hit_miss() {
        let mut cache = EntityCache::with_capacity(10);

        // Miss
        assert!(cache.get_relationship(1).is_none());
        assert_eq!(cache.stats().relationship_misses, 1);

        // Insert
        cache.insert_relationship(make_relationship(1));

        // Hit
        assert!(cache.get_relationship(1).is_some());
        assert_eq!(cache.stats().relationship_hits, 1);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = EntityCache::with_capacity(2);

        cache.insert_node(make_node(1));
        cache.insert_node(make_node(2));
        cache.insert_node(make_node(3)); // Should evict node 1

        assert!(cache.get_node(1).is_none());
        assert!(cache.get_node(2).is_some());
        assert!(cache.get_node(3).is_some());
    }

    #[test]
    fn test_disabled_cache() {
        let mut cache = EntityCache::new(EntityCacheConfig::disabled());

        assert!(!cache.nodes_enabled());
        assert!(!cache.relationships_enabled());

        cache.insert_node(make_node(1));
        assert!(cache.get_node(1).is_none());
    }

    #[test]
    fn test_hit_rate() {
        let mut cache = EntityCache::with_capacity(10);

        cache.insert_node(make_node(1));
        cache.get_node(1); // hit
        cache.get_node(1); // hit
        cache.get_node(2); // miss

        let stats = cache.stats();
        assert_eq!(stats.node_hits, 2);
        assert_eq!(stats.node_misses, 1);
        assert!((stats.node_hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_clear() {
        let mut cache = EntityCache::with_capacity(10);

        cache.insert_node(make_node(1));
        cache.insert_relationship(make_relationship(1));
        cache.get_node(1);

        cache.clear();

        assert!(cache.get_node(1).is_none());
        assert!(cache.get_relationship(1).is_none());
        assert_eq!(cache.stats().node_hits, 0);
    }
}
