//! Database schema creation and migrations.

use crate::error::Result;

use super::SqliteStorage;

/// Current schema version.
pub const SCHEMA_VERSION: i32 = 6;

impl SqliteStorage {
    /// Initialize the database schema.
    pub(crate) fn init_schema(&self) -> Result<()> {
        // Enable WAL mode for better concurrency.
        // WAL allows readers and writers to proceed concurrently - readers don't block
        // writers and writers don't block readers. Only writers block other writers.
        // This significantly reduces contention in multi-threaded scenarios.
        self.conn.execute_batch("PRAGMA journal_mode = WAL;")?;

        // Set busy timeout to 5 seconds. When the database is locked, SQLite will
        // retry for up to this duration before returning SQLITE_BUSY.
        self.conn.execute_batch("PRAGMA busy_timeout = 5000;")?;

        // Enable foreign keys
        self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        // Check current schema version
        let version = self.get_schema_version();

        if version == 0 {
            // Fresh database - create the current schema directly
            self.create_current_schema()?;
        } else if version < SCHEMA_VERSION {
            // Run migrations
            self.migrate(version)?;
        }

        Ok(())
    }

    /// Get current schema version (0 if no schema exists).
    fn get_schema_version(&self) -> i32 {
        self.conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Create the current schema directly for fresh databases.
    ///
    /// This creates all tables, indexes, and triggers at the current schema version,
    /// avoiding unnecessary migrations on empty tables. For upgrading existing
    /// databases from older versions, see [`Self::migrate`].
    fn create_current_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Metadata table for schema versioning
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO meta (key, value) VALUES ('schema_version', '6');

            -- Normalized node labels
            CREATE TABLE node_labels (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            -- Normalized relationship types
            CREATE TABLE rel_types (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            -- Nodes table
            -- Note: UNIQUE constraint on objectid automatically creates an index
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                objectid TEXT UNIQUE,
                properties TEXT NOT NULL DEFAULT '{}'
            );

            -- Node to label mapping (many-to-many)
            CREATE TABLE node_label_map (
                node_id INTEGER NOT NULL,
                label_id INTEGER NOT NULL,
                PRIMARY KEY (node_id, label_id),
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (label_id) REFERENCES node_labels(id) ON DELETE RESTRICT
            );
            CREATE INDEX idx_node_label_map_label ON node_label_map(label_id);
            CREATE INDEX idx_node_label_map_node ON node_label_map(node_id);

            -- Edges table
            CREATE TABLE relationships (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                type_id INTEGER NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (type_id) REFERENCES rel_types(id) ON DELETE RESTRICT
            );
            CREATE INDEX idx_edges_source ON relationships(source_id);
            CREATE INDEX idx_edges_target ON relationships(target_id);
            CREATE INDEX idx_edges_type ON relationships(type_id);
            CREATE INDEX idx_edges_source_type ON relationships(source_id, type_id);
            CREATE INDEX idx_edges_target_type ON relationships(target_id, type_id);

            -- Query history table for storing executed queries
            CREATE TABLE query_history (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                query TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                result_count INTEGER,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                duration_ms INTEGER,
                error TEXT,
                background INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX idx_query_history_timestamp ON query_history(timestamp DESC);

            -- Query cache table
            CREATE TABLE query_cache (
                query_hash TEXT PRIMARY KEY,
                query_ast TEXT NOT NULL,
                result BLOB NOT NULL,
                created_at INTEGER NOT NULL
            );

            -- Invalidation triggers (clear all cache on any data change)
            CREATE TRIGGER cache_invalidate_nodes_insert AFTER INSERT ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_nodes_update AFTER UPDATE ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_nodes_delete AFTER DELETE ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_edges_insert AFTER INSERT ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_edges_update AFTER UPDATE ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_edges_delete AFTER DELETE ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_labels_insert AFTER INSERT ON node_label_map
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER cache_invalidate_labels_delete AFTER DELETE ON node_label_map
                BEGIN DELETE FROM query_cache; END;
            "#,
        )?;
        Ok(())
    }

    /// Run migrations from old_version to current.
    fn migrate(&self, old_version: i32) -> Result<()> {
        if old_version < 2 {
            self.migrate_v1_to_v2()?;
        }
        if old_version < 3 {
            self.migrate_v2_to_v3()?;
        }
        if old_version < 4 {
            self.migrate_v3_to_v4()?;
        }
        if old_version < 5 {
            self.migrate_v4_to_v5()?;
        }
        if old_version < 6 {
            self.migrate_v5_to_v6()?;
        }
        Ok(())
    }

    /// Migration from v1 to v2: Add query_history table.
    fn migrate_v1_to_v2(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Query history table for storing executed queries
            CREATE TABLE IF NOT EXISTS query_history (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                query TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                result_count INTEGER,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                duration_ms INTEGER,
                error TEXT,
                background INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_query_history_timestamp ON query_history(timestamp DESC);

            -- Update schema version
            UPDATE meta SET value = '2' WHERE key = 'schema_version';
            "#,
        )?;
        Ok(())
    }

    /// Migration from v2 to v3: Add query cache table and invalidation triggers.
    fn migrate_v2_to_v3(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Query cache table
            CREATE TABLE IF NOT EXISTS query_cache (
                query_hash TEXT PRIMARY KEY,
                query_ast TEXT NOT NULL,
                result BLOB NOT NULL,
                created_at INTEGER NOT NULL
            );

            -- Invalidation triggers (clear all cache on any data change)
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_nodes_insert AFTER INSERT ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_nodes_update AFTER UPDATE ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_nodes_delete AFTER DELETE ON nodes
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_edges_insert AFTER INSERT ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_edges_update AFTER UPDATE ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_edges_delete AFTER DELETE ON relationships
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_labels_insert AFTER INSERT ON node_label_map
                BEGIN DELETE FROM query_cache; END;
            CREATE TRIGGER IF NOT EXISTS cache_invalidate_labels_delete AFTER DELETE ON node_label_map
                BEGIN DELETE FROM query_cache; END;

            -- Update schema version
            UPDATE meta SET value = '3' WHERE key = 'schema_version';
            "#,
        )?;
        Ok(())
    }

    /// Migration from v3 to v4: Convert JSON TEXT to JSONB for better query performance.
    ///
    /// JSONB stores JSON in a binary format that doesn't need re-parsing on each
    /// json_extract() call. This provides 2-3x speedup for property queries.
    fn migrate_v3_to_v4(&self) -> Result<()> {
        // Convert existing JSON text to JSONB binary format
        // SQLite's jsonb() function converts JSON text to JSONB blob
        // json_extract() works transparently on both formats
        self.conn.execute_batch(
            r#"
            -- Convert nodes properties from JSON text to JSONB
            UPDATE nodes SET properties = jsonb(properties) WHERE properties IS NOT NULL;

            -- Convert relationships properties from JSON text to JSONB
            UPDATE relationships SET properties = jsonb(properties) WHERE properties IS NOT NULL;

            -- Update schema version
            UPDATE meta SET value = '4' WHERE key = 'schema_version';
            "#,
        )?;
        Ok(())
    }

    /// Migration from v4 to v5: Add background column to query_history.
    fn migrate_v4_to_v5(&self) -> Result<()> {
        // Check if background column already exists (added in updated v1_to_v2 for new databases)
        let has_background: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('query_history') WHERE name = 'background'",
                [],
                |row| row.get::<_, i32>(0),
            )
            .map(|c| c > 0)
            .unwrap_or(false);

        if !has_background {
            self.conn.execute(
                "ALTER TABLE query_history ADD COLUMN background INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        self.conn.execute(
            "UPDATE meta SET value = '5' WHERE key = 'schema_version'",
            [],
        )?;
        Ok(())
    }

    /// Migration from v5 to v6: Add dedicated objectid column to nodes table.
    ///
    /// This provides a proper indexed column for node identity lookups instead of
    /// relying on JSON property extraction. The UNIQUE constraint on objectid
    /// automatically creates an index, so no explicit index is needed.
    fn migrate_v5_to_v6(&self) -> Result<()> {
        // Check if objectid column already exists
        let has_objectid: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('nodes') WHERE name = 'objectid'",
                [],
                |row| row.get::<_, i32>(0),
            )
            .map(|c| c > 0)
            .unwrap_or(false);

        if !has_objectid {
            // Add the objectid column
            self.conn
                .execute("ALTER TABLE nodes ADD COLUMN objectid TEXT UNIQUE", [])?;

            // Populate from existing JSON properties
            self.conn.execute(
                "UPDATE nodes SET objectid = json_extract(properties, '$.objectid') WHERE objectid IS NULL",
                [],
            )?;
        }

        // Drop the redundant explicit index if it exists
        // (UNIQUE constraint already provides an index)
        self.conn
            .execute_batch("DROP INDEX IF EXISTS idx_nodes_objectid;")?;

        self.conn.execute(
            "UPDATE meta SET value = '6' WHERE key = 'schema_version'",
            [],
        )?;
        Ok(())
    }
}
