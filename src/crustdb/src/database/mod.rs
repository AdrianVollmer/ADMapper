mod algorithms;
mod config;
mod data_access;
mod execution;
mod history;
mod import;
mod indexes;
mod tests;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::storage::{AdjacencyCache, EntityCache, EntityCacheConfig, SqliteStorage};

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
    /// Whether this database is opened in read-only mode.
    /// Write queries will be rejected with `Error::ReadOnly`.
    pub(crate) read_only: bool,
    /// Whether query caching is enabled (disabled by default).
    pub(crate) caching_enabled: bool,
    /// Entity cache for nodes and relationships (reduces SQLite lookups during traversals).
    /// Protected by a Mutex for thread-safe access.
    pub(crate) entity_cache: Mutex<EntityCache>,
    /// Maximum intermediate bindings allowed per query (memory safeguard).
    /// None means unlimited. Set via `set_max_intermediate_bindings`.
    pub(crate) max_intermediate_bindings: Option<usize>,
    /// Maximum BFS frontier entries allowed per query (memory safeguard).
    /// None means unlimited. Set via `set_max_frontier_entries`.
    pub(crate) max_frontier_entries: Option<usize>,
    /// In-memory adjacency list cache for fast graph traversals.
    /// Lazily built on first read query after invalidation.
    /// Wrapped in Arc so it can be shared with query execution contexts.
    pub(crate) adjacency_cache: RwLock<Option<Arc<AdjacencyCache>>>,
    /// Whether the adjacency cache needs rebuilding (set on mutations).
    pub(crate) adjacency_dirty: AtomicBool,
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
            read_only: false,
            caching_enabled: false,
            entity_cache: Mutex::new(EntityCache::new(EntityCacheConfig::disabled())),
            max_intermediate_bindings: None,
            max_frontier_entries: None,
            adjacency_cache: RwLock::new(None),
            adjacency_dirty: AtomicBool::new(true),
        })
    }

    /// Open an existing database in read-only mode.
    ///
    /// All connections are read-only. Write queries will be rejected with
    /// `Error::ReadOnly`. The database file must already exist.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_read_only_with_pool_size(path, DEFAULT_READ_POOL_SIZE)
    }

    /// Open an existing database in read-only mode with a specific pool size.
    pub fn open_read_only_with_pool_size<P: AsRef<Path>>(
        path: P,
        pool_size: usize,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();

        // All connections are read-only - no write connection needed.
        // We still need one "primary" connection for stats/metadata queries.
        let primary = SqliteStorage::open_read_only(&path_buf)?;

        let read_pool = (0..pool_size)
            .map(|_| SqliteStorage::open_read_only(&path_buf).map(Mutex::new))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            write_conn: Mutex::new(primary),
            read_pool,
            read_index: AtomicUsize::new(0),
            db_path: Some(path_buf),
            read_only: true,
            caching_enabled: false,
            entity_cache: Mutex::new(EntityCache::new(EntityCacheConfig::disabled())),
            max_intermediate_bindings: None,
            max_frontier_entries: None,
            adjacency_cache: RwLock::new(None),
            adjacency_dirty: AtomicBool::new(true),
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
            read_only: false,
            caching_enabled: false,
            entity_cache: Mutex::new(EntityCache::new(EntityCacheConfig::disabled())),
            max_intermediate_bindings: None,
            max_frontier_entries: None,
            adjacency_cache: RwLock::new(None),
            adjacency_dirty: AtomicBool::new(true),
        })
    }

    /// Returns true if this database was opened in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
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

    /// Return an error if the database is read-only.
    fn require_writable(&self) -> Result<()> {
        if self.read_only {
            Err(Error::ReadOnly)
        } else {
            Ok(())
        }
    }

    /// Mark the adjacency cache as dirty (needs rebuild).
    /// Called after any graph mutation.
    pub(crate) fn invalidate_adjacency_cache(&self) {
        self.adjacency_dirty.store(true, Ordering::Release);
    }

    /// Get the adjacency cache, rebuilding it if necessary.
    ///
    /// Returns an `Arc` clone of the cache that can be passed to query
    /// execution contexts without holding the lock.
    pub fn ensure_adjacency_cache(&self) -> Result<Arc<AdjacencyCache>> {
        // Fast path: cache is valid
        if !self.adjacency_dirty.load(Ordering::Acquire) {
            let guard = self.adjacency_cache.read().unwrap();
            if let Some(ref arc) = *guard {
                return Ok(Arc::clone(arc));
            }
            // Cache was None despite not being dirty — fall through to rebuild
            drop(guard);
        }

        // Slow path: rebuild the cache
        let mut write_guard = self.adjacency_cache.write().unwrap();
        // Double-check after acquiring write lock (another thread may have rebuilt)
        if !self.adjacency_dirty.load(Ordering::Acquire) {
            if let Some(ref arc) = *write_guard {
                return Ok(Arc::clone(arc));
            }
        }

        let storage = self.get_read_storage();
        let cache = Arc::new(AdjacencyCache::build(&storage)?);
        drop(storage);
        let result = Arc::clone(&cache);
        *write_guard = Some(cache);
        self.adjacency_dirty.store(false, Ordering::Release);

        Ok(result)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Checkpoint WAL and close connections gracefully.
        // This ensures WAL files are merged back into the main database file.
        // Skip for read-only databases (checkpoint requires write access).
        if self.db_path.is_some() && !self.read_only {
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
