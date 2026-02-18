# CrustDB Connection Pooling for Concurrent Queries

## Problem

Currently, CrustDB uses a single database connection protected by a `Mutex`. This means all queries are serialized - only one query can execute at a time, even though SQLite supports concurrent reads with WAL mode.

When users run multiple queries (e.g., via the query panel), they are queued and execute one at a time rather than concurrently.

## Why RwLock Doesn't Work

SQLite's `rusqlite::Connection` is `Send` but not `Sync`. This means:
- `Mutex<Connection>` is `Sync` (via exclusive access)
- `RwLock<Connection>` is NOT `Sync` (would need `T: Send + Sync`)

So we can't simply switch to `RwLock` to allow concurrent reads.

## Proposed Solution: Connection Pool

Implement a connection pool that maintains multiple SQLite connections to the same database file. SQLite with WAL mode allows:
- Multiple concurrent readers
- One writer at a time (blocks readers during write)

### Architecture

```
                    ┌─────────────────┐
                    │  CrustDatabase  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ ConnectionPool  │
                    │  (e.g., r2d2)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼────┐          ┌────▼────┐          ┌────▼────┐
   │  Conn 1 │          │  Conn 2 │          │  Conn 3 │
   │ (reader)│          │ (reader)│          │ (writer)│
   └─────────┘          └─────────┘          └─────────┘
```

### Implementation Options

#### Option 1: Use r2d2-sqlite (Recommended)
```rust
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub struct SqliteStorage {
    pool: Pool<SqliteConnectionManager>,
}

impl SqliteStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::builder()
            .max_size(4)  // Tune based on expected concurrency
            .build(manager)?;

        // Enable WAL mode on first connection
        let conn = pool.get()?;
        conn.execute_batch("PRAGMA journal_mode=WAL")?;

        Ok(Self { pool })
    }

    pub fn execute(&self, query: &str) -> Result<...> {
        let conn = self.pool.get()?;
        // Execute query
    }
}
```

#### Option 2: Manual Pool
Implement a simple pool using `crossbeam` or `parking_lot`:
- Vec of connections behind a Mutex
- Checkout/checkin pattern
- Less dependency but more code

### Changes Required

1. **Add dependency**: `r2d2 = "0.8"` and `r2d2_sqlite = "0.24"`

2. **Refactor SqliteStorage** (`src/crustdb/src/storage.rs`):
   - Replace `conn: Connection` with `pool: Pool<SqliteConnectionManager>`
   - Update all methods to get connection from pool
   - Enable WAL mode on initialization

3. **Remove internal Mutex** (`src/crustdb/src/lib.rs`):
   - Change `storage: Mutex<SqliteStorage>` to `storage: SqliteStorage`
   - Pool handles concurrency internally

4. **Update backend wrapper** (`src/backend/src/db/crustdb.rs`):
   - Can use `Arc<Database>` directly (already done)
   - Pool is thread-safe

### Configuration

```rust
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: u32,
    /// Minimum number of idle connections
    pub min_idle: Option<u32>,
    /// Connection timeout
    pub connection_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 4,
            min_idle: Some(1),
            connection_timeout: Duration::from_secs(30),
        }
    }
}
```

### Testing

1. Run multiple concurrent MATCH queries and verify they execute in parallel
2. Benchmark: single query vs N concurrent queries
3. Verify write operations still work correctly (serialized by SQLite)
4. Test pool exhaustion behavior (all connections busy)

## Benefits

- Concurrent read queries (common case)
- Better utilization of multi-core systems
- Improved perceived responsiveness
- Foundation for future scalability

## Considerations

- Pool size tuning (too many = wasted resources, too few = contention)
- WAL mode increases disk usage slightly
- Need to handle pool exhaustion gracefully
- Connection lifecycle management

## Related

- SQLite WAL mode: https://www.sqlite.org/wal.html
- r2d2-sqlite: https://crates.io/crates/r2d2_sqlite
