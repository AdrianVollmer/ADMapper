use crate::error::{Error, Result};
use crate::query;
use crate::query::QueryResult;

use super::compute_hash;

impl super::Database {
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

        if !statement.is_read_only() && self.read_only {
            return Err(Error::ReadOnly);
        }

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
                    query::executor::execute_with_cache(
                        &statement,
                        &read_storage,
                        cache_ref,
                        self.resource_limits(),
                    )?
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
                query::executor::execute_with_cache(
                    &statement,
                    &read_storage,
                    cache_ref,
                    self.resource_limits(),
                )
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

            query::executor::execute_with_cache(&statement, &storage, None, self.resource_limits())
        }
    }
}
