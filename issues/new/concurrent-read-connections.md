# Concurrent Read Connections via Connection Pool

## Background

Issue #6 from the crustdb code review identified that `Mutex<SqliteStorage>` serializes all database access, causing contention under concurrent load.

The initial recommendation to use `RwLock<SqliteStorage>` won't work because `rusqlite::Connection` is `Send` but not `Sync` - the connection cannot be shared across threads even with a read lock.

## Foundation Already in Place

The following groundwork has been completed:

### 1. WAL Mode (`storage.rs:init_schema()`)

```rust
self.conn.execute_batch("PRAGMA journal_mode = WAL;")?;
self.conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
```

WAL (Write-Ahead Logging) allows readers and writers to proceed concurrently at the SQLite level:
- Readers don't block writers
- Writers don't block readers
- Only writers block other writers

### 2. Query Classification (`parser.rs:Statement::is_read_only()`)

```rust
impl Statement {
    pub fn is_read_only(&self) -> bool {
        match self {
            Statement::Match(m) => m.set_clause.is_none() && m.delete_clause.is_none(),
            Statement::Create(_) | Statement::Merge(_) | Statement::Delete(_) | Statement::Set(_) => false,
        }
    }
}
```

This allows the database layer to distinguish read-only queries from mutations at parse time.

## Proposed Implementation

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Database                          │
├─────────────────────────────────────────────────────┤
│  write_conn: Mutex<SqliteStorage>  (1 connection)   │
│  read_pool:  Pool<SqliteStorage>   (N connections)  │
└─────────────────────────────────────────────────────┘
```

- **Write connection**: Single connection protected by `Mutex`, used for all mutating queries
- **Read pool**: Pool of N read-only connections, used for concurrent read queries

### Option A: Use `r2d2` Connection Pool

```toml
[dependencies]
r2d2 = "0.8"
r2d2_sqlite = "0.24"
```

```rust
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;

pub struct Database {
    write_conn: Mutex<SqliteStorage>,
    read_pool: Pool<SqliteConnectionManager>,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        // Write connection (existing)
        let write_conn = Mutex::new(SqliteStorage::open(path)?);

        // Read pool
        let manager = SqliteConnectionManager::file(path)
            .with_flags(OpenFlags::SQLITE_OPEN_READ_ONLY);
        let read_pool = Pool::builder()
            .max_size(4)  // Tune based on workload
            .build(manager)?;

        Ok(Self { write_conn, read_pool })
    }

    pub fn query(&self, cypher: &str) -> Result<QueryResult> {
        let statement = parser::parse(cypher)?;

        if statement.is_read_only() {
            let conn = self.read_pool.get()?;
            // Execute with read connection
        } else {
            let conn = self.write_conn.lock().unwrap();
            // Execute with write connection
        }
    }
}
```

### Option B: Manual Connection Pool (No Dependencies)

```rust
pub struct Database {
    write_conn: Mutex<SqliteStorage>,
    read_conns: Vec<Mutex<SqliteStorage>>,
    read_index: AtomicUsize,
}

impl Database {
    pub fn open(path: &str, read_pool_size: usize) -> Result<Self> {
        let write_conn = Mutex::new(SqliteStorage::open(path)?);

        let read_conns = (0..read_pool_size)
            .map(|_| {
                let storage = SqliteStorage::open_read_only(path)?;
                Ok(Mutex::new(storage))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            write_conn,
            read_conns,
            read_index: AtomicUsize::new(0),
        })
    }

    fn get_read_conn(&self) -> MutexGuard<SqliteStorage> {
        // Round-robin selection
        let idx = self.read_index.fetch_add(1, Ordering::Relaxed) % self.read_conns.len();
        self.read_conns[idx].lock().unwrap()
    }
}
```

### Required Changes to SqliteStorage

Add a read-only open mode:

```rust
impl SqliteStorage {
    pub fn open_read_only(path: &str) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        // Skip schema init - read-only connection
        Ok(Self { conn })
    }
}
```

## Implementation Steps

1. **Add `open_read_only()` to SqliteStorage**
   - Open connection with `SQLITE_OPEN_READ_ONLY` flag
   - Skip schema initialization (assume schema exists)

2. **Choose pool implementation**
   - Option A (`r2d2`): More features, handles connection health checks
   - Option B (manual): No dependencies, simpler, sufficient for most cases

3. **Modify `Database` struct**
   - Add read pool alongside existing write connection
   - Update `query()` to route based on `is_read_only()`

4. **Add configuration**
   - Pool size (default: number of CPUs or 4)
   - Optional: disable pooling for embedded/single-threaded use

5. **Testing**
   - Concurrent read benchmark
   - Mixed read/write workload
   - Connection exhaustion handling

## Considerations

### Connection Limits
SQLite has a practical limit on concurrent connections. The pool size should be tuned:
- Too few: Contention on read pool
- Too many: File descriptor exhaustion, diminishing returns

Recommended: Start with 4, benchmark, adjust.

### Read-Your-Writes Consistency
With separate read connections, a write followed by a read might not see the write immediately (WAL checkpoint lag). Options:
- Force checkpoint after writes (performance cost)
- Route post-write reads to write connection temporarily
- Accept eventual consistency for reads

### In-Memory Databases
For `:memory:` databases, connection pooling won't work (each connection is a separate database). Detect and fall back to single-connection mode.

### Transactions
Read connections cannot be used for transactions that might write. The `is_read_only()` check handles this at the statement level, but explicit transactions need consideration.

## See Also

- `issues/new/code-review-crustdb.md` - Issue #6 (original problem)
- `src/crustdb/src/storage.rs` - WAL mode implementation
- `src/crustdb/src/query/parser.rs` - `Statement::is_read_only()`
