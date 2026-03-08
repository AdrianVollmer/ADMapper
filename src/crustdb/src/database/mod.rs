mod history;
mod import;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query;
use crate::storage::{CacheStats, EntityCache, EntityCacheConfig, EntityCacheStats, SqliteStorage};

use crate::query::executor::algorithms::RelationshipBetweenness;
use crate::query::QueryResult;

/// Default number of read connections in the pool.
const DEFAULT_READ_POOL_SIZE: usize = 4;

/// Main database handle.
///
/// Uses a connection pool for concurrent read queries:
/// - One write connection protected by a Mutex (writes are serialized)
/// - N read connections for concurrent read queries (round-robin selection)
///
/// WAL mode allows readers and writers to proceed concurrently at the SQLite level.
pub struct Database {
    /// Primary write connection (also used for reads when pool is exhausted or in-memory).
    pub(crate) write_conn: Mutex<SqliteStorage>,
    /// Pool of read-only connections for concurrent queries.
    /// Empty for in-memory databases (each connection would be separate DB).
    pub(crate) read_pool: Vec<Mutex<SqliteStorage>>,
    /// Round-robin index for selecting read connections.
    pub(crate) read_index: AtomicUsize,
    /// Path to database file (None for in-memory).
    pub(crate) db_path: Option<PathBuf>,
    /// Whether query caching is enabled (disabled by default).
    pub(crate) caching_enabled: bool,
    /// Entity cache for nodes and relationships (reduces SQLite lookups during traversals).
    /// Protected by a Mutex for thread-safe access.
    pub(crate) entity_cache: Mutex<EntityCache>,
}

impl Database {
    /// Open or create a database at the given path.
    ///
    /// Creates a connection pool with one write connection and multiple read
    /// connections for concurrent query execution.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_pool_size(path, DEFAULT_READ_POOL_SIZE)
    }

    /// Open or create a database with a specific read pool size.
    ///
    /// - `pool_size = 0`: No read pool, all queries use the write connection
    /// - `pool_size > 0`: Creates N read-only connections for concurrent reads
    pub fn open_with_pool_size<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();

        // Primary write connection
        let write_storage = SqliteStorage::open(&path_buf)?;

        // Create read pool
        let read_pool = (0..pool_size)
            .map(|_| SqliteStorage::open_read_only(&path_buf).map(Mutex::new))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            write_conn: Mutex::new(write_storage),
            read_pool,
            read_index: AtomicUsize::new(0),
            db_path: Some(path_buf),
            caching_enabled: false,
            entity_cache: Mutex::new(EntityCache::new(EntityCacheConfig::disabled())),
        })
    }

    /// Create an in-memory database.
    ///
    /// In-memory databases cannot use connection pooling because each connection
    /// would create a separate database. All queries use the single connection.
    pub fn in_memory() -> Result<Self> {
        let storage = SqliteStorage::in_memory()?;
        Ok(Self {
            write_conn: Mutex::new(storage),
            read_pool: Vec::new(), // No pooling for in-memory
            read_index: AtomicUsize::new(0),
            db_path: None,
            caching_enabled: false,
            entity_cache: Mutex::new(EntityCache::new(EntityCacheConfig::disabled())),
        })
    }

    /// Get a read connection from the pool (round-robin).
    /// Falls back to write connection if pool is empty.
    pub(crate) fn get_read_storage(&self) -> std::sync::MutexGuard<'_, SqliteStorage> {
        if self.read_pool.is_empty() {
            // No pool (in-memory or pool_size=0), use write connection
            self.write_conn.lock().unwrap()
        } else {
            // Round-robin selection from pool
            let idx = self.read_index.fetch_add(1, Ordering::Relaxed) % self.read_pool.len();
            self.read_pool[idx].lock().unwrap()
        }
    }

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

    /// Create an index on a JSON property for faster lookups.
    ///
    /// This creates a SQLite expression index on `json_extract(properties, '$.property')`,
    /// which significantly speeds up queries that filter nodes by this property.
    ///
    /// Common properties to index: `object_id`, `name`, etc.
    ///
    /// # Example
    /// ```ignore
    /// db.create_property_index("object_id")?;
    /// // Now queries like MATCH (n {object_id: '...'}) will use the index
    /// ```
    pub fn create_property_index(&self, property: &str) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.create_property_index(property)
    }

    /// Drop an index on a JSON property.
    ///
    /// Returns Ok(true) if the index existed and was dropped,
    /// Ok(false) if the index didn't exist.
    pub fn drop_property_index(&self, property: &str) -> Result<bool> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.drop_property_index(property)
    }

    /// List all property indexes that have been created.
    pub fn list_property_indexes(&self) -> Result<Vec<String>> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.list_property_indexes()
    }

    /// Check if a property index exists.
    pub fn has_property_index(&self, property: &str) -> Result<bool> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.has_property_index(property)
    }

    /// Execute a Cypher query.
    ///
    /// Read-only queries (MATCH without SET/DELETE) are executed on a pooled
    /// read connection for better concurrency. Write queries use the primary
    /// write connection.
    ///
    /// If entity caching is enabled (via `set_entity_cache`), nodes and relationships
    /// are cached during traversals to reduce SQLite lookups.
    pub fn execute(&self, query: &str) -> Result<QueryResult> {
        let statement = query::parser::parse(query)?;

        if statement.is_read_only() {
            // Use read connection from pool for query execution
            let read_storage = self.get_read_storage();

            // Handle caching for read-only queries
            if self.caching_enabled {
                let ast_json = serde_json::to_string(&statement)
                    .map_err(|e| Error::Internal(format!("Failed to serialize AST: {}", e)))?;
                let query_hash = compute_hash(&ast_json);

                // Check cache (can read from read-only connection)
                if let Some(cached_bytes) = read_storage.get_cached_result(&query_hash)? {
                    if let Ok(cached_result) = serde_json::from_slice(&cached_bytes) {
                        return Ok(cached_result);
                    }
                }

                // Execute on read connection with entity cache
                let result = {
                    let mut entity_cache = self.entity_cache.lock().unwrap();
                    let cache_ref =
                        if entity_cache.nodes_enabled() || entity_cache.relationships_enabled() {
                            Some(&mut *entity_cache)
                        } else {
                            None
                        };
                    query::executor::execute_with_cache(&statement, &read_storage, cache_ref)?
                };

                // Drop read_storage before acquiring write lock to avoid deadlock
                // (in-memory databases use write_conn for reads since there's no pool)
                drop(read_storage);

                // Cache via write connection (cache writes need write access)
                let result_bytes = serde_json::to_vec(&result)
                    .map_err(|e| Error::Internal(format!("Failed to serialize result: {}", e)))?;
                if let Ok(write_storage) = self.write_conn.lock() {
                    // Best-effort caching - don't fail if we can't cache
                    let _ = write_storage.cache_result(&query_hash, &ast_json, &result_bytes);
                }
                Ok(result)
            } else {
                // Execute with entity cache (if enabled)
                let mut entity_cache = self.entity_cache.lock().unwrap();
                let cache_ref =
                    if entity_cache.nodes_enabled() || entity_cache.relationships_enabled() {
                        Some(&mut *entity_cache)
                    } else {
                        None
                    };
                query::executor::execute_with_cache(&statement, &read_storage, cache_ref)
            }
        } else {
            // Write queries use the write connection
            let storage = self
                .write_conn
                .lock()
                .map_err(|e| Error::Internal(e.to_string()))?;

            // Clear entity cache on write operations (data changed)
            {
                let mut entity_cache = self.entity_cache.lock().unwrap();
                entity_cache.clear();
            }

            query::executor::execute_with_cache(&statement, &storage, None)
        }
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.stats()
    }

    /// Get database file size in bytes.
    ///
    /// Returns the total size of the database file (page_count * page_size).
    /// Returns 0 for in-memory databases.
    pub fn database_size(&self) -> Result<usize> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.database_size()
    }

    /// Clear all data from the database.
    /// This is much faster than using Cypher DELETE queries.
    pub fn clear(&self) -> Result<()> {
        let storage = self
            .write_conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        storage.clear()
    }

    /// Get a node by its ID.
    pub fn get_node(&self, node_id: i64) -> Result<Option<Node>> {
        let storage = self.get_read_storage();
        storage.get_node(node_id)
    }

    /// Get a relationship by its ID.
    ///
    /// Useful for resolving relationship IDs returned by algorithms like relationship betweenness.
    pub fn get_relationship(&self, rel_id: i64) -> Result<Option<Relationship>> {
        let storage = self.get_read_storage();
        storage.get_relationship(rel_id)
    }

    /// Get all distinct relationship types.
    ///
    /// Uses direct SQL query on the normalized rel_types table for O(distinct_types)
    /// performance instead of O(relationships) via Cypher MATCH.
    pub fn get_all_relationship_types(&self) -> Result<Vec<String>> {
        let storage = self.get_read_storage();
        storage.get_all_relationship_types()
    }

    /// Get all distinct node labels.
    ///
    /// Uses direct SQL query on the normalized node_labels table for O(distinct_labels)
    /// performance instead of O(nodes) via Cypher MATCH.
    pub fn get_all_labels(&self) -> Result<Vec<String>> {
        let storage = self.get_read_storage();
        storage.get_all_labels()
    }

    /// Compute relationship betweenness centrality for the graph.
    ///
    /// Relationship betweenness centrality measures how many shortest paths pass through
    /// each relationship. Relationships with high betweenness are "choke points" - removing
    /// them would disrupt many paths through the graph.
    ///
    /// This is useful for Active Directory security analysis to identify:
    /// - Critical permissions that enable many attack paths
    /// - High-impact remediation targets
    /// - Structural vulnerabilities in the permission graph
    ///
    /// Results are cached and automatically invalidated when graph data changes.
    ///
    /// # Arguments
    ///
    /// * `rel_types` - Optional filter to only consider specific relationship types
    ///   (e.g., `Some(&["MemberOf", "GenericAll"])`)
    /// * `directed` - Whether to treat relationships as directed (true) or undirected (false).
    ///   For AD graphs, directed is usually appropriate since permissions are directional.
    ///
    /// # Returns
    ///
    /// A `RelationshipBetweenness` struct containing:
    /// - `scores`: HashMap of relationship ID to betweenness score
    /// - `nodes_processed`: Number of nodes in the graph
    /// - `relationships_count`: Number of relationships analyzed
    ///
    /// Use `result.top_k(10)` to get the top 10 relationships by betweenness.
    ///
    /// # Complexity
    ///
    /// O(V * E) where V is the number of nodes and E is the number of relationships.
    /// For large graphs, this may take significant time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let db = Database::open("graph.db")?;
    /// let result = db.relationship_betweenness_centrality(None, true)?;
    ///
    /// // Get top 10 choke points
    /// for (rel_id, score) in result.top_k(10) {
    ///     println!("Relationship {} has betweenness {}", rel_id, score);
    /// }
    /// ```
    pub fn relationship_betweenness_centrality(
        &self,
        rel_types: Option<&[&str]>,
        directed: bool,
    ) -> Result<RelationshipBetweenness> {
        let read_storage = self.get_read_storage();

        // Generate cache key based on algorithm parameters
        let cache_key = format!(
            "algo:relationship_betweenness:directed={}:types={}",
            directed,
            rel_types
                .map(|t| t.join(","))
                .unwrap_or_else(|| "all".to_string())
        );
        let cache_hash = compute_hash(&cache_key);

        // Check cache
        if let Some(cached_bytes) = read_storage.get_cached_result(&cache_hash)? {
            if let Ok(cached_result) = serde_json::from_slice(&cached_bytes) {
                return Ok(cached_result);
            }
        }

        // Compute (expensive)
        let result = query::executor::algorithms::relationship_betweenness_centrality(
            &read_storage,
            rel_types,
            directed,
        )?;

        // Cache the result
        let result_bytes = serde_json::to_vec(&result)
            .map_err(|e| Error::Internal(format!("Failed to serialize result: {}", e)))?;
        if let Ok(write_storage) = self.write_conn.lock() {
            // Best-effort caching - don't fail if we can't cache
            let _ = write_storage.cache_result(&cache_hash, &cache_key, &result_bytes);
        }

        Ok(result)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Checkpoint WAL and close connections gracefully.
        // This ensures WAL files are merged back into the main database file.
        if self.db_path.is_some() {
            if let Ok(storage) = self.write_conn.lock() {
                // PRAGMA wal_checkpoint(TRUNCATE) merges WAL into main DB and truncates WAL file
                let _ = storage.checkpoint();
            }
        }
        // Read pool connections will be dropped automatically.
        // SqliteStorage drops its Connection which closes the SQLite handle.
    }
}

/// Compute a hash of the given string for use as a cache key.
pub(crate) fn compute_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// A row from the query history (owned version for reads).
#[derive(Debug, Clone)]
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
    pub background: bool,
}

/// A new query history entry (borrowed version for inserts).
#[derive(Debug, Clone)]
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

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Total number of nodes.
    pub node_count: usize,
    /// Total number of relationships.
    pub relationship_count: usize,
    /// Number of distinct node labels.
    pub label_count: usize,
    /// Number of distinct relationship types.
    pub relationship_type_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::PropertyValue;
    use crate::query::ResultValue;

    #[test]
    fn test_database_create_single_node() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.label_count, 1);
    }

    #[test]
    fn test_database_create_relationship() {
        let db = Database::in_memory().unwrap();

        let result = db.execute(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})"
        ).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.relationship_count, 1);
        assert_eq!(stats.relationship_type_count, 1);
    }

    #[test]
    fn test_match_create_relationship_between_existing_nodes() {
        let db = Database::in_memory().unwrap();

        // Create two nodes separately
        db.execute("CREATE (:Group {object_id: 'G1', name: 'Group1'})")
            .unwrap();
        db.execute("CREATE (:Group {object_id: 'G2', name: 'Group2'})")
            .unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.relationship_count, 0);

        // MATCH...CREATE should create a relationship between existing nodes, not new ones
        let result = db
            .execute(
                "MATCH (a:Group {object_id: 'G1'}), (b:Group {object_id: 'G2'}) \
                 CREATE (a)-[:MemberOf]->(b)",
            )
            .unwrap();

        assert_eq!(
            result.stats.nodes_created, 0,
            "MATCH...CREATE should not create new nodes"
        );
        assert_eq!(
            result.stats.relationships_created, 1,
            "MATCH...CREATE should create 1 relationship"
        );

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2, "Node count should remain 2");
        assert_eq!(stats.relationship_count, 1, "Should have 1 relationship");

        // Verify the relationship connects the right nodes
        let verify = db
            .execute("MATCH (a:Group {object_id: 'G1'})-[:MemberOf]->(b:Group) RETURN b.object_id")
            .unwrap();
        assert_eq!(verify.rows.len(), 1);
    }

    #[test]
    fn test_database_multiple_creates() {
        let db = Database::in_memory().unwrap();

        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_database_complex_pattern() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 3);
        assert_eq!(result.stats.relationships_created, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.relationship_count, 2);
        assert_eq!(stats.relationship_type_count, 2); // KNOWS, WORKS_AT
    }

    #[test]
    fn test_database_syntax_error() {
        let db = Database::in_memory().unwrap();

        let result = db.execute("CREATE n:Person");
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_insert_nodes() {
        let db = Database::in_memory().unwrap();

        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "object_id": "acme-3"}),
            ),
        ];

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 3);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_batch_insert_relationships() {
        let db = Database::in_memory().unwrap();

        // Create nodes first
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "object_id": "acme-3"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(node_ids.len(), 3);

        // Create relationships using node IDs
        let relationships = vec![
            (
                node_ids[0],
                node_ids[1],
                "KNOWS".to_string(),
                serde_json::json!({"since": 2020}),
            ),
            (
                node_ids[0],
                node_ids[2],
                "WORKS_AT".to_string(),
                serde_json::json!({}),
            ),
        ];

        let rel_ids = db.insert_relationships_batch(&relationships).unwrap();
        assert_eq!(rel_ids.len(), 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.relationship_count, 2);
        assert_eq!(stats.relationship_type_count, 2);
    }

    #[test]
    fn test_property_index() {
        let db = Database::in_memory().unwrap();

        // Create nodes with object_id property
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "object_id": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "object_id": "bob-2"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();

        // Build property index
        let index = db.build_property_index("object_id").unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(index.get("alice-1"), Some(&node_ids[0]));
        assert_eq!(index.get("bob-2"), Some(&node_ids[1]));

        // Find node by property
        let found = db.find_node_by_property("object_id", "alice-1").unwrap();
        assert_eq!(found, Some(node_ids[0]));

        let not_found = db.find_node_by_property("object_id", "nobody").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_create_property_index() {
        let db = Database::in_memory().unwrap();

        // Create nodes first
        db.execute("CREATE (n:Person {object_id: 'p1', name: 'Alice'})")
            .unwrap();
        db.execute("CREATE (n:Person {object_id: 'p2', name: 'Bob'})")
            .unwrap();

        // Initially no property indexes
        assert!(db.list_property_indexes().unwrap().is_empty());

        // Create index on object_id
        db.create_property_index("object_id").unwrap();
        assert!(db.has_property_index("object_id").unwrap());

        // Index should be listed
        let indexes = db.list_property_indexes().unwrap();
        assert_eq!(indexes, vec!["object_id"]);

        // Queries using the indexed property should still work correctly
        let result = db
            .execute("MATCH (n {object_id: 'p1'}) RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Drop the index
        assert!(db.drop_property_index("object_id").unwrap());
        assert!(!db.has_property_index("object_id").unwrap());

        // Query still works after dropping index
        let result = db
            .execute("MATCH (n {object_id: 'p2'}) RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_batch_insert_large() {
        let db = Database::in_memory().unwrap();

        // Create 1000 nodes in a batch
        let nodes: Vec<_> = (0..1000)
            .map(|i| {
                (
                    vec!["TestNode".to_string()],
                    serde_json::json!({"id": i, "object_id": format!("node-{}", i)}),
                )
            })
            .collect();

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 1000);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1000);
    }

    #[test]
    fn test_upsert_nodes_batch() {
        let db = Database::in_memory().unwrap();

        // Create a placeholder node (like an orphan from relationship import)
        let placeholder = vec![(
            vec!["Base".to_string()],
            serde_json::json!({
                "object_id": "test-user-1",
                "name": "test-user-1",
                "placeholder": true
            }),
        )];
        let ids1 = db.upsert_nodes_batch(&placeholder).unwrap();
        assert_eq!(ids1.len(), 1);

        // Upsert with full data - should merge properties
        let full_data = vec![(
            vec!["User".to_string()],
            serde_json::json!({
                "object_id": "test-user-1",
                "name": "alice@corp.local",
                "enabled": true,
                "department": "Engineering"
            }),
        )];
        let ids2 = db.upsert_nodes_batch(&full_data).unwrap();
        assert_eq!(ids2.len(), 1);

        // Should be the same node
        assert_eq!(ids1[0], ids2[0]);

        // Only one node should exist
        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1);

        // Verify via Cypher query that properties were merged
        let result = db
            .execute("MATCH (n {object_id: 'test-user-1'}) RETURN n.name, n.enabled, n.department")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_get_or_create_node_by_object_id() {
        let db = Database::in_memory().unwrap();

        // Create an orphan node
        let id1 = db
            .get_or_create_node_by_object_id("orphan-1", "User")
            .unwrap();
        assert!(id1 > 0);

        // Same object_id should return same ID
        let id2 = db
            .get_or_create_node_by_object_id("orphan-1", "Computer")
            .unwrap();
        assert_eq!(id1, id2);

        // Different object_id should create new node
        let id3 = db
            .get_or_create_node_by_object_id("orphan-2", "User")
            .unwrap();
        assert_ne!(id1, id3);

        // Should have 2 nodes
        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
    }

    #[test]
    fn test_count_aggregate() {
        let db = Database::in_memory().unwrap();

        // Create some nodes
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        // Count all nodes
        let result = db.execute("MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(result.rows.len(), 1, "Should return single row");

        // Extract count
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 3, "Should count 3 nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }

        // Count by label
        let result = db.execute("MATCH (n:Person) RETURN count(n)").unwrap();
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 Person nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }

    #[test]
    fn test_count_relationships() {
        let db = Database::in_memory().unwrap();

        // Create nodes with relationships
        db.execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        db.execute("CREATE (c:Person)-[:WORKS_AT]->(d:Company)")
            .unwrap();

        // Count all relationships
        let result = db.execute("MATCH ()-[r]->() RETURN count(r)").unwrap();
        assert_eq!(result.rows.len(), 1);

        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 relationships");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }

    #[test]
    fn test_query_history_api() {
        let db = Database::in_memory().unwrap();

        // Add a query to history
        db.add_query_history(NewQueryHistoryEntry {
            id: "test-id-1",
            name: "Test Query",
            query: "MATCH (n) RETURN n",
            timestamp: 1700000000,
            result_count: Some(42),
            status: "completed",
            started_at: 1700000000,
            duration_ms: Some(150),
            error: None,
            background: false,
        })
        .unwrap();

        // Get query history
        let (rows, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "test-id-1");
        assert_eq!(rows[0].name, "Test Query");
        assert_eq!(rows[0].result_count, Some(42));
        assert_eq!(rows[0].duration_ms, Some(150));

        // Update status
        db.update_query_status("test-id-1", "archived", Some(200), Some(50), None)
            .unwrap();

        let (rows, _) = db.get_query_history(10, 0).unwrap();
        assert_eq!(rows[0].status, "archived");
        assert_eq!(rows[0].duration_ms, Some(200));
        assert_eq!(rows[0].result_count, Some(50));

        // Delete query
        db.delete_query_history("test-id-1").unwrap();
        let (rows, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
        assert!(rows.is_empty());

        // Add another and clear all
        db.add_query_history(NewQueryHistoryEntry {
            id: "test-id-2",
            name: "Another",
            query: "MATCH (n) RETURN n",
            timestamp: 1700000001,
            result_count: None,
            status: "pending",
            started_at: 1700000001,
            duration_ms: None,
            error: None,
            background: true, // background query
        })
        .unwrap();
        db.clear_query_history().unwrap();
        let (_, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_caching_disabled_by_default() {
        let db = Database::in_memory().unwrap();
        assert!(!db.caching_enabled());

        // Execute a query - should not be cached
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();

        // Cache should be empty
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_query_caching_basic() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);
        assert!(db.caching_enabled());

        // Create some data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Execute a read-only query
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 2);

        // Check cache has one entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Execute the same query again - should hit cache
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 2);

        // Cache should still have one entry (not doubled)
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Results should be equivalent
        assert_eq!(result1.columns, result2.columns);
        assert_eq!(result1.rows.len(), result2.rows.len());
    }

    #[test]
    fn test_cache_invalidation_on_insert() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 1);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Insert new data - should invalidate cache
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // Execute query again - should get fresh result
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 2);
    }

    #[test]
    fn test_cache_invalidation_on_update() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.age").unwrap();
        assert_eq!(result1.rows.len(), 1);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Update data - should invalidate cache
        db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
            .unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_cache_invalidation_on_delete() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 2);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Delete data - should invalidate cache
        db.execute("MATCH (n:Person {name: 'Bob'}) DELETE n")
            .unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // Execute query again - should get fresh result
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 1);
    }

    #[test]
    fn test_write_queries_not_cached() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // CREATE is not cached
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // MATCH with SET is not cached (not read-only)
        db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 30")
            .unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // MATCH with DELETE is not cached
        db.execute("CREATE (n:Temp {x: 1})").unwrap();
        db.execute("MATCH (n:Temp) DELETE n").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // But pure MATCH RETURN is cached
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_clear_cache() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create data and cache a query
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();

        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Manually clear cache
        db.clear_cache().unwrap();

        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        // Create a file-backed database to test connection pooling
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_concurrent.db");
        let db = Arc::new(Database::open(&db_path).unwrap());

        // Create some test data
        db.execute("CREATE (n:Person {name: 'Alice', id: 1})")
            .unwrap();
        db.execute("CREATE (n:Person {name: 'Bob', id: 2})")
            .unwrap();
        db.execute("CREATE (n:Person {name: 'Charlie', id: 3})")
            .unwrap();

        // Spawn multiple threads to read concurrently
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..10 {
                        let result = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
                        assert_eq!(result.rows.len(), 3);
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data integrity
        let result = db.execute("MATCH (n:Person) RETURN count(n)").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_read_pool_with_custom_size() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_pool.db");

        // Create with pool size 2
        let db = Database::open_with_pool_size(&db_path, 2).unwrap();

        db.execute("CREATE (n:Test {x: 1})").unwrap();
        let result = db.execute("MATCH (n:Test) RETURN n.x").unwrap();
        assert_eq!(result.rows.len(), 1);

        // Pool size 0 should also work (no read pool)
        let db2 = Database::open_with_pool_size(&db_path, 0).unwrap();
        let result = db2.execute("MATCH (n:Test) RETURN n.x").unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}
