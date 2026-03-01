//! SQLite storage backend for the graph database.

use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue, Relationship};
use crate::{DatabaseStats, NewQueryHistoryEntry, QueryHistoryRow};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::path::Path;

/// Current schema version.
const SCHEMA_VERSION: i32 = 6;

/// Validate a property name to prevent JSON path injection.
///
/// Property names must contain only alphanumeric characters and underscores,
/// and must not be empty. This prevents injection attacks in JSON path expressions.
fn validate_property_name(property: &str) -> Result<()> {
    if property.is_empty() {
        return Err(Error::InvalidProperty(
            "Property name cannot be empty".to_string(),
        ));
    }
    if !property
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::InvalidProperty(format!(
            "Property name '{}' contains invalid characters (only alphanumeric and underscore allowed)",
            property
        )));
    }
    Ok(())
}

/// SQLite-based storage backend.
pub struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Create an in-memory database.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Open an existing database in read-only mode.
    ///
    /// This is used for read pool connections. The schema is assumed to exist
    /// (created by the primary write connection). Read-only connections can
    /// execute queries concurrently without blocking each other or the writer.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        use rusqlite::OpenFlags;
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        // Set busy timeout for read connections too
        conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
        Ok(Self { conn })
    }

    /// Initialize the database schema.
    fn init_schema(&self) -> Result<()> {
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
            // Fresh database - create schema
            self.create_schema_v1()?;
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

    /// Create the initial schema (v1).
    fn create_schema_v1(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Metadata table for schema versioning
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO meta (key, value) VALUES ('schema_version', '3');

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
            -- Note: UNIQUE constraint on object_id automatically creates an index
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                object_id TEXT UNIQUE,
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

    /// Migration from v5 to v6: Add dedicated object_id column to nodes table.
    ///
    /// This provides a proper indexed column for node identity lookups instead of
    /// relying on JSON property extraction. The UNIQUE constraint on object_id
    /// automatically creates an index, so no explicit index is needed.
    fn migrate_v5_to_v6(&self) -> Result<()> {
        // Check if object_id column already exists
        let has_object_id: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('nodes') WHERE name = 'object_id'",
                [],
                |row| row.get::<_, i32>(0),
            )
            .map(|c| c > 0)
            .unwrap_or(false);

        if !has_object_id {
            // Add the object_id column
            self.conn
                .execute("ALTER TABLE nodes ADD COLUMN object_id TEXT UNIQUE", [])?;

            // Populate from existing JSON properties
            self.conn.execute(
                "UPDATE nodes SET object_id = json_extract(properties, '$.object_id') WHERE object_id IS NULL",
                [],
            )?;
        }

        // Drop the redundant explicit index if it exists
        // (UNIQUE constraint already provides an index)
        self.conn
            .execute_batch("DROP INDEX IF EXISTS idx_nodes_object_id;")?;

        self.conn.execute(
            "UPDATE meta SET value = '6' WHERE key = 'schema_version'",
            [],
        )?;
        Ok(())
    }

    /// Get or create a node label ID.
    fn get_or_create_label(&self, label: &str) -> Result<i64> {
        // Try to get existing
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM node_labels WHERE name = ?1",
                params![label],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create new
        self.conn
            .execute("INSERT INTO node_labels (name) VALUES (?1)", params![label])?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get or create an relationship type ID.
    fn get_or_create_edge_type(&self, rel_type: &str) -> Result<i64> {
        // Try to get existing
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM rel_types WHERE name = ?1",
                params![rel_type],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create new
        self.conn.execute(
            "INSERT INTO rel_types (name) VALUES (?1)",
            params![rel_type],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert a node into the database.
    pub fn insert_node(&self, labels: &[String], properties: &serde_json::Value) -> Result<i64> {
        let props_json = serde_json::to_string(properties)?;
        // Extract object_id from properties for the dedicated column
        let object_id = properties
            .get("object_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        self.conn.execute(
            "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))",
            params![object_id, props_json],
        )?;
        let node_id = self.conn.last_insert_rowid();

        for label in labels {
            let label_id = self.get_or_create_label(label)?;
            self.conn.execute(
                "INSERT INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
                params![node_id, label_id],
            )?;
        }

        Ok(node_id)
    }

    /// Insert an relationship into the database.
    pub fn insert_edge(
        &self,
        source_id: i64,
        target_id: i64,
        rel_type: &str,
        properties: &serde_json::Value,
    ) -> Result<i64> {
        let type_id = self.get_or_create_edge_type(rel_type)?;
        let props_json = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT INTO relationships (source_id, target_id, type_id, properties) VALUES (?1, ?2, ?3, jsonb(?4))",
            params![source_id, target_id, type_id, props_json],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert multiple nodes in a single transaction.
    ///
    /// Returns a vector of the created node IDs in the same order as the input.
    pub fn insert_nodes_batch(
        &mut self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut node_ids = Vec::with_capacity(nodes.len());

        // Pre-collect all unique labels and create them
        let mut label_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (labels, _) in nodes {
            for label in labels {
                if !label_cache.contains_key(label) {
                    let label_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM node_labels WHERE name = ?1",
                            params![label],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let label_id = match label_id {
                        Some(id) => id,
                        None => {
                            tx.execute(
                                "INSERT INTO node_labels (name) VALUES (?1)",
                                params![label],
                            )?;
                            tx.last_insert_rowid()
                        }
                    };
                    label_cache.insert(label.clone(), label_id);
                }
            }
        }

        // Insert nodes using prepared statement
        {
            let mut node_stmt =
                tx.prepare("INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))")?;
            let mut label_stmt =
                tx.prepare("INSERT INTO node_label_map (node_id, label_id) VALUES (?1, ?2)")?;

            for (labels, properties) in nodes {
                let props_json = serde_json::to_string(properties)?;
                // Extract object_id from properties for the dedicated column
                let object_id = properties
                    .get("object_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                node_stmt.execute(params![object_id, props_json])?;
                let node_id = tx.last_insert_rowid();
                node_ids.push(node_id);

                for label in labels {
                    if let Some(&label_id) = label_cache.get(label) {
                        label_stmt.execute(params![node_id, label_id])?;
                    }
                }
            }
        }

        tx.commit()?;
        Ok(node_ids)
    }

    /// Upsert multiple nodes in a single transaction.
    ///
    /// If a node with the same object_id already exists, its properties are merged
    /// (new properties are added, existing properties are updated) rather than
    /// replaced entirely. Labels are also merged.
    ///
    /// Returns a vector of the node IDs (internal SQLite IDs) in the same order as the input.
    pub fn upsert_nodes_batch(
        &mut self,
        nodes: &[(Vec<String>, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut node_ids = Vec::with_capacity(nodes.len());

        // Pre-collect all unique labels and create them
        let mut label_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (labels, _) in nodes {
            for label in labels {
                if !label_cache.contains_key(label) {
                    let label_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM node_labels WHERE name = ?1",
                            params![label],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let label_id = match label_id {
                        Some(id) => id,
                        None => {
                            tx.execute(
                                "INSERT INTO node_labels (name) VALUES (?1)",
                                params![label],
                            )?;
                            tx.last_insert_rowid()
                        }
                    };
                    label_cache.insert(label.clone(), label_id);
                }
            }
        }

        // Upsert nodes using prepared statements
        // json_patch merges the new properties into the existing ones
        {
            let mut upsert_stmt = tx.prepare(
                "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))
                 ON CONFLICT(object_id) DO UPDATE SET
                   properties = jsonb(json_patch(json(properties), json(?2)))",
            )?;
            let mut get_id_stmt = tx.prepare("SELECT id FROM nodes WHERE object_id = ?1")?;
            let mut label_stmt = tx.prepare(
                "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            )?;

            for (labels, properties) in nodes {
                let props_json = serde_json::to_string(properties)?;
                // Extract object_id from properties for the dedicated column
                let object_id = properties
                    .get("object_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if let Some(ref oid) = object_id {
                    upsert_stmt.execute(params![oid, props_json])?;
                    // Get the node ID (either newly inserted or existing)
                    let node_id: i64 = get_id_stmt.query_row(params![oid], |row| row.get(0))?;
                    node_ids.push(node_id);

                    // Merge labels (INSERT OR IGNORE handles duplicates)
                    for label in labels {
                        if let Some(&label_id) = label_cache.get(label) {
                            label_stmt.execute(params![node_id, label_id])?;
                        }
                    }
                } else {
                    // No object_id, fall back to regular insert
                    tx.execute(
                        "INSERT INTO nodes (object_id, properties) VALUES (NULL, jsonb(?1))",
                        params![props_json],
                    )?;
                    let node_id = tx.last_insert_rowid();
                    node_ids.push(node_id);

                    for label in labels {
                        if let Some(&label_id) = label_cache.get(label) {
                            label_stmt.execute(params![node_id, label_id])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(node_ids)
    }

    /// Get or create a node by object_id, returning its internal ID.
    ///
    /// If the node exists, returns its ID without modifying it.
    /// If it doesn't exist, creates an orphan node with just the object_id
    /// and the specified label, ready to be upserted later with full properties.
    pub fn get_or_create_node_by_object_id(&self, object_id: &str, label: &str) -> Result<i64> {
        // Try to find existing node
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT id FROM nodes WHERE object_id = ?1",
                params![object_id],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }

        // Create orphan node with minimal properties
        let props = serde_json::json!({
            "object_id": object_id,
            "name": object_id,
            "placeholder": true
        });
        let props_json = serde_json::to_string(&props)?;

        self.conn.execute(
            "INSERT INTO nodes (object_id, properties) VALUES (?1, jsonb(?2))",
            params![object_id, props_json],
        )?;
        let node_id = self.conn.last_insert_rowid();

        // Add label
        let label_id = self.get_or_create_label(label)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            params![node_id, label_id],
        )?;

        Ok(node_id)
    }

    /// Insert multiple relationships in a single transaction.
    ///
    /// Each relationship is specified as (source_id, target_id, rel_type, properties).
    /// Returns a vector of the created relationship IDs in the same order as the input.
    pub fn insert_edges_batch(
        &mut self,
        relationships: &[(i64, i64, String, serde_json::Value)],
    ) -> Result<Vec<i64>> {
        if relationships.is_empty() {
            return Ok(Vec::new());
        }

        let tx = self.conn.transaction()?;
        let mut edge_ids = Vec::with_capacity(relationships.len());

        // Pre-collect all unique relationship types and create them
        let mut type_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (_, _, rel_type, _) in relationships {
            if !type_cache.contains_key(rel_type) {
                let type_id: Option<i64> = tx
                    .query_row(
                        "SELECT id FROM rel_types WHERE name = ?1",
                        params![rel_type],
                        |row| row.get(0),
                    )
                    .optional()?;
                let type_id = match type_id {
                    Some(id) => id,
                    None => {
                        tx.execute(
                            "INSERT INTO rel_types (name) VALUES (?1)",
                            params![rel_type],
                        )?;
                        tx.last_insert_rowid()
                    }
                };
                type_cache.insert(rel_type.clone(), type_id);
            }
        }

        // Insert relationships using prepared statement
        {
            let mut edge_stmt = tx.prepare(
                "INSERT INTO relationships (source_id, target_id, type_id, properties) VALUES (?1, ?2, ?3, jsonb(?4))",
            )?;

            for (source_id, target_id, rel_type, properties) in relationships {
                let props_json = serde_json::to_string(properties)?;
                let type_id = type_cache.get(rel_type).copied().unwrap_or(0);
                edge_stmt.execute(params![source_id, target_id, type_id, props_json])?;
                edge_ids.push(tx.last_insert_rowid());
            }
        }

        tx.commit()?;
        Ok(edge_ids)
    }

    /// Find a node ID by a property value.
    ///
    /// Searches for nodes where the JSON properties contain the specified key-value pair.
    /// Property names must contain only alphanumeric characters and underscores.
    /// Optimized path for object_id which uses a dedicated indexed column.
    pub fn find_node_by_property(&self, property: &str, value: &str) -> Result<Option<i64>> {
        validate_property_name(property)?;

        // Use the dedicated object_id column for faster lookups
        let query = if property == "object_id" {
            "SELECT id FROM nodes WHERE object_id = ?1 LIMIT 1".to_string()
        } else {
            format!(
                "SELECT id FROM nodes WHERE json_extract(properties, '$.{}') = ?1 LIMIT 1",
                property
            )
        };
        let result: Option<i64> = self
            .conn
            .query_row(&query, params![value], |row| row.get(0))
            .optional()?;
        Ok(result)
    }

    /// Find nodes by property value with optional label filter.
    ///
    /// Uses indexed property lookup when available (via `create_property_index`).
    /// Property names must contain only alphanumeric characters and underscores.
    pub fn find_nodes_by_property(
        &self,
        property: &str,
        value: &serde_json::Value,
        labels: &[String],
        limit: Option<u64>,
    ) -> Result<Vec<Node>> {
        validate_property_name(property)?;

        // Convert JSON value to rusqlite Value with correct type for comparison
        let sql_value: rusqlite::types::Value = match value {
            serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    rusqlite::types::Value::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    rusqlite::types::Value::Real(f)
                } else {
                    return Ok(Vec::new());
                }
            }
            serde_json::Value::Bool(b) => {
                // SQLite stores booleans as integers (0/1)
                rusqlite::types::Value::Integer(if *b { 1 } else { 0 })
            }
            serde_json::Value::Null => rusqlite::types::Value::Null,
            _ => return Ok(Vec::new()), // Arrays/objects not supported for index lookup
        };

        let limit_clause = limit.map(|n| format!(" LIMIT {}", n)).unwrap_or_default();

        let sql = if labels.is_empty() {
            // No label filter - just property lookup
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM nodes n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 WHERE json_extract(n.properties, '$.{}') = ?1
                 GROUP BY n.id{}",
                property, limit_clause
            )
        } else {
            // With label filter - use subquery for efficiency
            let label_placeholders: Vec<String> =
                (2..=labels.len() + 1).map(|i| format!("?{}", i)).collect();
            format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(all_labels.name) as labels
                 FROM (
                     SELECT DISTINCT nodes.id, nodes.properties
                     FROM nodes
                     JOIN node_label_map nlm ON nodes.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE json_extract(nodes.properties, '$.{}') = ?1
                       AND nl.name IN ({})
                     {}
                 ) AS n
                 LEFT JOIN node_label_map nlm2 ON n.id = nlm2.node_id
                 LEFT JOIN node_labels all_labels ON nlm2.label_id = all_labels.id
                 GROUP BY n.id, n.properties",
                property,
                label_placeholders.join(", "),
                limit_clause
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;

        if labels.is_empty() {
            self.collect_nodes_from_stmt(&mut stmt, [&sql_value as &dyn rusqlite::ToSql])
        } else {
            // Build dynamic params: [sql_value, label1, label2, ...]
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            param_values.push(Box::new(sql_value));
            for label in labels {
                param_values.push(Box::new(label.clone()));
            }
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();
            self.collect_nodes_from_stmt(&mut stmt, params_refs.as_slice())
        }
    }

    /// Build an index of property values to node IDs for efficient batch lookups.
    ///
    /// Returns a HashMap from property value to node ID.
    /// Property names must contain only alphanumeric characters and underscores.
    /// Optimized path for object_id which uses a dedicated indexed column.
    pub fn build_property_index(
        &self,
        property: &str,
    ) -> Result<std::collections::HashMap<String, i64>> {
        validate_property_name(property)?;

        // Use the dedicated object_id column for faster lookups
        let query = if property == "object_id" {
            "SELECT id, object_id FROM nodes WHERE object_id IS NOT NULL".to_string()
        } else {
            format!(
                "SELECT id, json_extract(properties, '$.{}') FROM nodes WHERE json_extract(properties, '$.{}') IS NOT NULL",
                property, property
            )
        };
        let mut stmt = self.conn.prepare(&query)?;
        let mut index = std::collections::HashMap::new();

        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let value: String = row.get(1)?;
            Ok((id, value))
        })?;

        for row in rows {
            let (id, value) = row?;
            index.insert(value, id);
        }

        Ok(index)
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: i64) -> Result<Option<Node>> {
        // Use json() to convert JSONB blob back to JSON text
        let node: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT id, json(properties) FROM nodes WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let Some((id, props_json)) = node else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        // Get labels via join
        let mut label_stmt = self.conn.prepare_cached(
            "SELECT nl.name FROM node_labels nl
             JOIN node_label_map nlm ON nl.id = nlm.label_id
             WHERE nlm.node_id = ?1",
        )?;
        let labels: Vec<String> = label_stmt
            .query_map(params![id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Some(Node {
            id,
            labels,
            properties,
        }))
    }

    /// Get an relationship by ID.
    pub fn get_edge(&self, id: i64) -> Result<Option<Relationship>> {
        // Use json() to convert JSONB blob back to JSON text
        let relationship: Option<(i64, i64, i64, String, String)> = self
            .conn
            .query_row(
                "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
                 FROM relationships e
                 JOIN rel_types et ON e.type_id = et.id
                 WHERE e.id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((id, source, target, rel_type, props_json)) = relationship else {
            return Ok(None);
        };

        let properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&props_json)?;

        Ok(Some(Relationship {
            id,
            source,
            target,
            rel_type,
            properties,
        }))
    }

    /// Delete a node and its associated relationships.
    pub fn delete_node(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Delete an relationship.
    pub fn delete_edge(&self, id: i64) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM relationships WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    /// Check if a node has any connected relationships.
    pub fn has_edges(&self, node_id: i64) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE source_id = ?1 OR target_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Update a single property on a node.
    pub fn update_node_property(
        &self,
        node_id: i64,
        property: &str,
        value: &PropertyValue,
    ) -> Result<bool> {
        // Get current properties (use json() to convert JSONB to text)
        let current: Option<String> = self
            .conn
            .query_row(
                "SELECT json(properties) FROM nodes WHERE id = ?1",
                params![node_id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(current_json) = current else {
            return Ok(false); // Node doesn't exist
        };

        // Parse, update, and serialize
        let mut properties: std::collections::HashMap<String, PropertyValue> =
            serde_json::from_str(&current_json)?;
        properties.insert(property.to_string(), value.clone());
        let new_json = serde_json::to_string(&properties)?;

        let affected = self.conn.execute(
            "UPDATE nodes SET properties = jsonb(?1) WHERE id = ?2",
            params![new_json, node_id],
        )?;
        Ok(affected > 0)
    }

    /// Add a label to a node.
    pub fn add_node_label(&self, node_id: i64, label: &str) -> Result<bool> {
        // Check if node exists
        let exists: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM nodes WHERE id = ?1",
                params![node_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        if !exists {
            return Ok(false);
        }

        let label_id = self.get_or_create_label(label)?;

        // Try to insert (ignore if already exists)
        self.conn.execute(
            "INSERT OR IGNORE INTO node_label_map (node_id, label_id) VALUES (?1, ?2)",
            params![node_id, label_id],
        )?;
        Ok(true)
    }

    /// Scan all nodes in the database.
    pub fn scan_all_nodes(&self) -> Result<Vec<Node>> {
        self.get_all_nodes_limit(None)
    }

    /// Find nodes by label.
    pub fn find_nodes_by_label(&self, label: &str) -> Result<Vec<Node>> {
        self.find_nodes_by_label_limit(label, None)
    }

    /// Find relationships by type.
    pub fn find_edges_by_type(&self, rel_type: &str) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             WHERE et.name = ?1",
        )?;

        self.collect_edges_from_stmt(&mut stmt, params![rel_type])
    }

    /// Scan all relationships in the database.
    pub fn scan_all_edges(&self) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id",
        )?;

        self.collect_edges_from_stmt(&mut stmt, [])
    }

    /// Helper: collect relationships from a prepared statement that returns
    /// (id, source_id, target_id, rel_type, properties).
    fn collect_edges_from_stmt<P: rusqlite::Params>(
        &self,
        stmt: &mut rusqlite::Statement,
        params: P,
    ) -> Result<Vec<Relationship>> {
        let rows = stmt.query_map(params, |row| {
            let id: i64 = row.get(0)?;
            let source: i64 = row.get(1)?;
            let target: i64 = row.get(2)?;
            let rel_type: String = row.get(3)?;
            let properties_json: String = row.get(4)?;
            Ok((id, source, target, rel_type, properties_json))
        })?;

        let mut relationships = Vec::new();
        for row_result in rows {
            let (id, source, target, rel_type, properties_json) = row_result?;

            let properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&properties_json)?;

            relationships.push(Relationship {
                id,
                source,
                target,
                rel_type,
                properties,
            });
        }

        Ok(relationships)
    }

    /// Count all nodes.
    pub fn count_nodes(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Count nodes with a specific label.
    pub fn count_nodes_by_label(&self, label: &str) -> Result<u64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM nodes n
             JOIN node_label_map nlm ON n.id = nlm.node_id
             JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE nl.name = ?1",
            params![label],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Count all relationships.
    pub fn count_edges(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Count relationships with a specific type.
    pub fn count_edges_by_type(&self, rel_type: &str) -> Result<u64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             WHERE et.name = ?1",
            params![rel_type],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Find nodes by label with optional limit.
    pub fn find_nodes_by_label_limit(&self, label: &str, limit: Option<u64>) -> Result<Vec<Node>> {
        // Use subquery to limit nodes BEFORE joining for all labels
        // This ensures we only process N nodes instead of all matching nodes
        // Use json() to convert JSONB to text for deserialization
        let sql = match limit {
            Some(n) => format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM (
                     SELECT DISTINCT nodes.id, nodes.properties
                     FROM nodes
                     JOIN node_label_map nlm ON nodes.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE nl.name = ?1
                     LIMIT {}
                 ) AS n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 GROUP BY n.id, n.properties",
                n
            ),
            None => "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                     FROM nodes n
                     JOIN node_label_map nlm ON n.id = nlm.node_id
                     JOIN node_labels nl ON nlm.label_id = nl.id
                     WHERE n.id IN (
                         SELECT DISTINCT nlm2.node_id FROM node_label_map nlm2
                         JOIN node_labels nl2 ON nlm2.label_id = nl2.id
                         WHERE nl2.name = ?1
                     )
                     GROUP BY n.id"
                .to_string(),
        };

        let mut stmt = self.conn.prepare(&sql)?;
        self.collect_nodes_from_stmt(&mut stmt, params![label])
    }

    /// Get all nodes with optional limit.
    pub fn get_all_nodes_limit(&self, limit: Option<u64>) -> Result<Vec<Node>> {
        // Use subquery to limit nodes BEFORE joining for labels
        // This ensures we only process N nodes instead of all nodes
        // Use json() to convert JSONB to text for deserialization
        let sql = match limit {
            Some(n) => format!(
                "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                 FROM (SELECT id, properties FROM nodes LIMIT {}) AS n
                 LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                 LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                 GROUP BY n.id, n.properties",
                n
            ),
            None => "SELECT n.id, json(n.properties), GROUP_CONCAT(nl.name) as labels
                     FROM nodes n
                     LEFT JOIN node_label_map nlm ON n.id = nlm.node_id
                     LEFT JOIN node_labels nl ON nlm.label_id = nl.id
                     GROUP BY n.id"
                .to_string(),
        };

        let mut stmt = self.conn.prepare(&sql)?;
        self.collect_nodes_from_stmt(&mut stmt, [])
    }

    /// Helper: collect nodes from a prepared statement that returns (id, properties, labels).
    fn collect_nodes_from_stmt<P: rusqlite::Params>(
        &self,
        stmt: &mut rusqlite::Statement,
        params: P,
    ) -> Result<Vec<Node>> {
        let rows = stmt.query_map(params, |row| {
            let id: i64 = row.get(0)?;
            let properties_json: String = row.get(1)?;
            let labels_concat: Option<String> = row.get(2)?;
            Ok((id, properties_json, labels_concat))
        })?;

        let mut nodes = Vec::new();
        for row_result in rows {
            let (id, properties_json, labels_concat) = row_result?;

            let properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&properties_json)?;

            let labels: Vec<String> = labels_concat
                .map(|s| s.split(',').map(|l| l.to_string()).collect())
                .unwrap_or_default();

            nodes.push(Node {
                id,
                labels,
                properties,
            });
        }

        Ok(nodes)
    }

    /// Find outgoing relationships from a node.
    pub fn find_outgoing_edges(&self, node_id: i64) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             WHERE e.source_id = ?1",
        )?;

        self.collect_edges_from_stmt(&mut stmt, params![node_id])
    }

    /// Find incoming relationships to a node.
    pub fn find_incoming_edges(&self, node_id: i64) -> Result<Vec<Relationship>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.id, e.source_id, e.target_id, et.name, json(e.properties)
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             WHERE e.target_id = ?1",
        )?;

        self.collect_edges_from_stmt(&mut stmt, params![node_id])
    }

    /// Count outgoing relationships from a node.
    pub fn count_outgoing_edges(&self, node_id: i64) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE source_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count incoming relationships to a node.
    pub fn count_incoming_edges(&self, node_id: i64) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships WHERE target_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count incoming relationships to a node by object_id.
    /// Uses the dedicated object_id column for efficient indexed lookup.
    pub fn count_incoming_edges_by_object_id(&self, object_id: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships e \
             JOIN nodes n ON e.target_id = n.id \
             WHERE n.object_id = ?1",
            params![object_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count outgoing relationships from a node by object_id.
    /// Uses the dedicated object_id column for efficient indexed lookup.
    pub fn count_outgoing_edges_by_object_id(&self, object_id: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relationships e \
             JOIN nodes n ON e.source_id = n.id \
             WHERE n.object_id = ?1",
            params![object_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Get all relationships for a node by object_id (both incoming and outgoing).
    /// Returns (source_object_id, target_object_id, rel_type) tuples.
    /// Uses the dedicated object_id column for efficient indexed lookup.
    pub fn get_node_edges_by_object_id(
        &self,
        object_id: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let mut relationships = Vec::new();

        // Query for relationships where node is source or target, using dedicated object_id column
        let mut stmt = self.conn.prepare_cached(
            "SELECT
                src.object_id AS src_id,
                tgt.object_id AS tgt_id,
                et.name AS rel_type
             FROM relationships e
             JOIN nodes src ON e.source_id = src.id
             JOIN nodes tgt ON e.target_id = tgt.id
             JOIN rel_types et ON e.type_id = et.id
             WHERE src.object_id = ?1
                OR tgt.object_id = ?1",
        )?;

        let rows = stmt.query_map(params![object_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        for row in rows {
            relationships.push(row?);
        }

        Ok(relationships)
    }

    /// Get incoming connections to a node by object_id.
    ///
    /// Returns all nodes that have relationships pointing TO the specified node,
    /// along with those relationships. This uses direct SQL with the object_id index
    /// for optimal performance, avoiding full node scans.
    ///
    /// Returns (Vec<Node>, Vec<Relationship>) where nodes are the source nodes of
    /// incoming relationships, and relationships are the relationships.
    pub fn get_incoming_connections_by_object_id(
        &self,
        object_id: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        // First find the target node's internal ID using the dedicated object_id column
        let target_id: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM nodes WHERE object_id = ?1",
                params![object_id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(target_id) = target_id else {
            return Ok((Vec::new(), Vec::new()));
        };

        // Get incoming relationships and source nodes in a single query
        let mut stmt = self.conn.prepare_cached(
            "SELECT
                e.id AS rel_id,
                e.source_id,
                e.target_id,
                et.name AS rel_type,
                json(e.properties) AS edge_props,
                src.id AS src_node_id,
                json(src.properties) AS src_props,
                GROUP_CONCAT(DISTINCT nl.name) AS src_labels
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             JOIN nodes src ON e.source_id = src.id
             LEFT JOIN node_label_map nlm ON src.id = nlm.node_id
             LEFT JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE e.target_id = ?1
             GROUP BY e.id, src.id",
        )?;

        let mut nodes_map: std::collections::HashMap<i64, Node> = std::collections::HashMap::new();
        let mut relationships = Vec::new();

        let rows = stmt.query_map(params![target_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,            // rel_id
                row.get::<_, i64>(1)?,            // source_id
                row.get::<_, i64>(2)?,            // target_id
                row.get::<_, String>(3)?,         // rel_type
                row.get::<_, String>(4)?,         // edge_props
                row.get::<_, i64>(5)?,            // src_node_id
                row.get::<_, String>(6)?,         // src_props
                row.get::<_, Option<String>>(7)?, // src_labels
            ))
        })?;

        for row_result in rows {
            let (
                rel_id,
                source_id,
                target_id_row,
                rel_type,
                edge_props,
                src_node_id,
                src_props,
                src_labels,
            ) = row_result?;

            // Add relationship
            let edge_properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&edge_props)?;
            relationships.push(Relationship {
                id: rel_id,
                source: source_id,
                target: target_id_row,
                rel_type,
                properties: edge_properties,
            });

            // Add source node if not already present
            if let std::collections::hash_map::Entry::Vacant(e) = nodes_map.entry(src_node_id) {
                let properties: std::collections::HashMap<String, PropertyValue> =
                    serde_json::from_str(&src_props)?;
                let labels: Vec<String> = src_labels
                    .map(|s| s.split(',').map(|l| l.to_string()).collect())
                    .unwrap_or_default();
                e.insert(Node {
                    id: src_node_id,
                    labels,
                    properties,
                });
            }
        }

        // Also fetch and add the target node itself
        if let Some(target_node) = self.get_node(target_id)? {
            nodes_map.insert(target_id, target_node);
        }

        Ok((nodes_map.into_values().collect(), relationships))
    }

    /// Get outgoing connections from a node by object_id.
    ///
    /// Returns all nodes that the specified node has relationships pointing TO,
    /// along with those relationships. This uses direct SQL with the object_id index
    /// for optimal performance.
    ///
    /// Returns (Vec<Node>, Vec<Relationship>) where nodes are the target nodes of
    /// outgoing relationships, and relationships are the relationships.
    pub fn get_outgoing_connections_by_object_id(
        &self,
        object_id: &str,
    ) -> Result<(Vec<Node>, Vec<Relationship>)> {
        // First find the source node's internal ID using the dedicated object_id column
        let source_id: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM nodes WHERE object_id = ?1",
                params![object_id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(source_id) = source_id else {
            return Ok((Vec::new(), Vec::new()));
        };

        // Get outgoing relationships and target nodes in a single query
        let mut stmt = self.conn.prepare_cached(
            "SELECT
                e.id AS rel_id,
                e.source_id,
                e.target_id,
                et.name AS rel_type,
                json(e.properties) AS edge_props,
                tgt.id AS tgt_node_id,
                json(tgt.properties) AS tgt_props,
                GROUP_CONCAT(DISTINCT nl.name) AS tgt_labels
             FROM relationships e
             JOIN rel_types et ON e.type_id = et.id
             JOIN nodes tgt ON e.target_id = tgt.id
             LEFT JOIN node_label_map nlm ON tgt.id = nlm.node_id
             LEFT JOIN node_labels nl ON nlm.label_id = nl.id
             WHERE e.source_id = ?1
             GROUP BY e.id, tgt.id",
        )?;

        let mut nodes_map: std::collections::HashMap<i64, Node> = std::collections::HashMap::new();
        let mut relationships = Vec::new();

        let rows = stmt.query_map(params![source_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,            // rel_id
                row.get::<_, i64>(1)?,            // source_id
                row.get::<_, i64>(2)?,            // target_id
                row.get::<_, String>(3)?,         // rel_type
                row.get::<_, String>(4)?,         // edge_props
                row.get::<_, i64>(5)?,            // tgt_node_id
                row.get::<_, String>(6)?,         // tgt_props
                row.get::<_, Option<String>>(7)?, // tgt_labels
            ))
        })?;

        for row_result in rows {
            let (
                rel_id,
                source_id_row,
                target_id,
                rel_type,
                edge_props,
                tgt_node_id,
                tgt_props,
                tgt_labels,
            ) = row_result?;

            // Add relationship
            let edge_properties: std::collections::HashMap<String, PropertyValue> =
                serde_json::from_str(&edge_props)?;
            relationships.push(Relationship {
                id: rel_id,
                source: source_id_row,
                target: target_id,
                rel_type,
                properties: edge_properties,
            });

            // Add target node if not already present
            if let std::collections::hash_map::Entry::Vacant(e) = nodes_map.entry(tgt_node_id) {
                let properties: std::collections::HashMap<String, PropertyValue> =
                    serde_json::from_str(&tgt_props)?;
                let labels: Vec<String> = tgt_labels
                    .map(|s| s.split(',').map(|l| l.to_string()).collect())
                    .unwrap_or_default();
                e.insert(Node {
                    id: tgt_node_id,
                    labels,
                    properties,
                });
            }
        }

        // Also fetch and add the source node itself
        if let Some(source_node) = self.get_node(source_id)? {
            nodes_map.insert(source_id, source_node);
        }

        Ok((nodes_map.into_values().collect(), relationships))
    }

    /// Get database statistics.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let node_count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;

        let edge_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))?;

        let label_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM node_labels", [], |row| row.get(0))?;

        let edge_type_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM rel_types", [], |row| row.get(0))?;

        Ok(DatabaseStats {
            node_count,
            edge_count,
            label_count,
            edge_type_count,
        })
    }

    /// Get database file size in bytes (page_count * page_size).
    pub fn database_size(&self) -> Result<usize> {
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok((page_count * page_size) as usize)
    }

    /// Clear all data from the database (nodes, relationships, labels, types).
    /// This is much faster than deleting via Cypher queries.
    pub fn clear(&self) -> Result<()> {
        // Delete in order respecting foreign key relationships
        self.conn.execute("DELETE FROM node_label_map", [])?;
        self.conn.execute("DELETE FROM relationships", [])?;
        self.conn.execute("DELETE FROM nodes", [])?;
        self.conn.execute("DELETE FROM rel_types", [])?;
        self.conn.execute("DELETE FROM node_labels", [])?;
        Ok(())
    }

    /// Checkpoint the WAL file, merging it into the main database file.
    ///
    /// This is called during graceful shutdown to ensure WAL files are cleaned up.
    /// Uses TRUNCATE mode which merges WAL and then truncates it to zero size.
    pub fn checkpoint(&self) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    /// Begin a transaction.
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.conn.transaction()?)
    }

    /// Get all node labels.
    pub fn get_all_labels(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM node_labels ORDER BY name")?;
        let labels: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(labels)
    }

    /// Get all relationship types.
    pub fn get_all_edge_types(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM rel_types ORDER BY name")?;
        let types: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(types)
    }

    /// Get counts for all node labels in a single query.
    /// Returns a HashMap of label name to count.
    pub fn get_label_counts(&self) -> Result<std::collections::HashMap<String, usize>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT nl.name, COUNT(*) as cnt
             FROM node_labels nl
             JOIN node_label_map nlm ON nl.id = nlm.label_id
             GROUP BY nl.id, nl.name",
        )?;

        let mut counts = std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, usize>(1)?))
        })?;

        for row in rows {
            let (label, count) = row?;
            counts.insert(label, count);
        }

        Ok(counts)
    }

    // ========================================================================
    // Query History Methods
    // ========================================================================

    /// Add a query to the history.
    pub fn add_query_history(&self, entry: NewQueryHistoryEntry<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO query_history
             (id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                entry.id,
                entry.name,
                entry.query,
                entry.timestamp,
                entry.result_count,
                entry.status,
                entry.started_at,
                entry.duration_ms.map(|d| d as i64),
                entry.error,
                entry.background
            ],
        )?;
        Ok(())
    }

    /// Update the status of a query in history.
    pub fn update_query_status(
        &self,
        id: &str,
        status: &str,
        duration_ms: Option<u64>,
        result_count: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE query_history
             SET status = ?2, duration_ms = ?3, result_count = ?4, error = ?5
             WHERE id = ?1",
            params![
                id,
                status,
                duration_ms.map(|d| d as i64),
                result_count,
                error
            ],
        )?;
        Ok(())
    }

    /// Get query history with pagination.
    /// Returns (rows, total_count).
    pub fn get_query_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<QueryHistoryRow>, usize)> {
        // Get total count
        let total: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM query_history", [], |row| row.get(0))?;

        // Get paginated results
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, name, query, timestamp, result_count, status, started_at, duration_ms, error, background
             FROM query_history
             ORDER BY timestamp DESC
             LIMIT ?1 OFFSET ?2",
        )?;

        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(QueryHistoryRow {
                id: row.get(0)?,
                name: row.get(1)?,
                query: row.get(2)?,
                timestamp: row.get(3)?,
                result_count: row.get(4)?,
                status: row.get(5)?,
                started_at: row.get(6)?,
                duration_ms: row.get::<_, Option<i64>>(7)?.map(|d| d as u64),
                error: row.get(8)?,
                background: row.get::<_, i64>(9).map(|v| v != 0).unwrap_or(false),
            })
        })?;

        let history: Vec<QueryHistoryRow> = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((history, total))
    }

    /// Delete a query from history.
    pub fn delete_query_history(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM query_history WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Clear all query history.
    pub fn clear_query_history(&self) -> Result<()> {
        self.conn.execute("DELETE FROM query_history", [])?;
        Ok(())
    }

    // ========================================================================
    // Query Cache Methods
    // ========================================================================

    /// Get a cached query result by hash.
    pub fn get_cached_result(&self, query_hash: &str) -> Result<Option<Vec<u8>>> {
        let result: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT result FROM query_cache WHERE query_hash = ?1",
                params![query_hash],
                |row| row.get(0),
            )
            .optional()?;
        Ok(result)
    }

    /// Store a query result in the cache.
    pub fn cache_result(&self, query_hash: &str, ast_json: &str, result: &[u8]) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT OR REPLACE INTO query_cache (query_hash, query_ast, result, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![query_hash, ast_json, result, now],
        )?;
        Ok(())
    }

    /// Clear the query cache manually.
    pub fn clear_query_cache(&self) -> Result<()> {
        self.conn.execute("DELETE FROM query_cache", [])?;
        Ok(())
    }

    /// Get cache statistics (entry count, total size).
    pub fn cache_stats(&self) -> Result<CacheStats> {
        let entry_count: usize =
            self.conn
                .query_row("SELECT COUNT(*) FROM query_cache", [], |row| row.get(0))?;

        let total_size_bytes: usize = self.conn.query_row(
            "SELECT COALESCE(SUM(LENGTH(result)), 0) FROM query_cache",
            [],
            |row| row.get(0),
        )?;

        Ok(CacheStats {
            entry_count,
            total_size_bytes,
        })
    }

    // ========================================================================
    // Property Index Methods
    // ========================================================================

    /// Create an index on a JSON property for faster lookups.
    ///
    /// This creates a SQLite expression index on `json_extract(properties, '$.property')`,
    /// which significantly speeds up queries that filter nodes by this property.
    ///
    /// The index name follows the pattern `idx_nodes_prop_{property}`.
    /// If the index already exists, this is a no-op.
    pub fn create_property_index(&self, property: &str) -> Result<()> {
        validate_property_name(property)?;

        let index_name = format!("idx_nodes_prop_{}", property);
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON nodes(json_extract(properties, '$.{}'))",
            index_name, property
        );

        self.conn.execute(&sql, [])?;
        Ok(())
    }

    /// Drop an index on a JSON property.
    ///
    /// Returns Ok(true) if the index existed and was dropped,
    /// Ok(false) if the index didn't exist.
    pub fn drop_property_index(&self, property: &str) -> Result<bool> {
        validate_property_name(property)?;

        let index_name = format!("idx_nodes_prop_{}", property);

        // Check if index exists
        let exists: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![&index_name],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        if exists {
            let sql = format!("DROP INDEX {}", index_name);
            self.conn.execute(&sql, [])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all property indexes that have been created.
    ///
    /// Returns a list of property names that have indexes.
    pub fn list_property_indexes(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT name FROM sqlite_master
             WHERE type = 'index' AND name LIKE 'idx_nodes_prop_%'",
        )?;

        let indexes: Vec<String> = stmt
            .query_map([], |row| {
                let name: String = row.get(0)?;
                // Strip the prefix to get the property name
                Ok(name
                    .strip_prefix("idx_nodes_prop_")
                    .unwrap_or(&name)
                    .to_string())
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(indexes)
    }

    /// Check if a property index exists.
    pub fn has_property_index(&self, property: &str) -> Result<bool> {
        validate_property_name(property)?;

        let index_name = format!("idx_nodes_prop_{}", property);
        let exists: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![&index_name],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        Ok(exists)
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached entries.
    pub entry_count: usize,
    /// Total size of cached results in bytes.
    pub total_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_query_node() {
        let storage = SqliteStorage::in_memory().unwrap();

        let props = serde_json::json!({"name": "Alice", "age": 30});
        let node_id = storage
            .insert_node(&["Person".to_string()], &props)
            .unwrap();

        let node = storage.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.id, node_id);
        assert!(node.has_label("Person"));
        assert_eq!(
            node.get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_create_node_with_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();

        let props = serde_json::json!({"name": "Charlie"});
        let node_id = storage
            .insert_node(&["Person".to_string(), "Actor".to_string()], &props)
            .unwrap();

        let node = storage.get_node(node_id).unwrap().unwrap();
        assert!(node.has_label("Person"));
        assert!(node.has_label("Actor"));
    }

    #[test]
    fn test_create_and_query_edge() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();

        let rel_id = storage
            .insert_edge(
                alice_id,
                bob_id,
                "KNOWS",
                &serde_json::json!({"since": 2020}),
            )
            .unwrap();

        let relationship = storage.get_edge(rel_id).unwrap().unwrap();
        assert_eq!(relationship.source, alice_id);
        assert_eq!(relationship.target, bob_id);
        assert_eq!(relationship.rel_type, "KNOWS");
    }

    #[test]
    fn test_find_nodes_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();
        storage
            .insert_node(
                &["Company".to_string()],
                &serde_json::json!({"name": "Acme"}),
            )
            .unwrap();

        let people = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(people.len(), 2);

        let companies = storage.find_nodes_by_label("Company").unwrap();
        assert_eq!(companies.len(), 1);
    }

    #[test]
    fn test_find_edges_by_type() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let acme_id = storage
            .insert_node(&["Company".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, acme_id, "WORKS_AT", &serde_json::json!({}))
            .unwrap();

        let knows_edges = storage.find_edges_by_type("KNOWS").unwrap();
        assert_eq!(knows_edges.len(), 1);

        let works_at_edges = storage.find_edges_by_type("WORKS_AT").unwrap();
        assert_eq!(works_at_edges.len(), 1);
    }

    #[test]
    fn test_outgoing_incoming_edges() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let charlie_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, charlie_id, "KNOWS", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(bob_id, alice_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        let alice_outgoing = storage.find_outgoing_edges(alice_id).unwrap();
        assert_eq!(alice_outgoing.len(), 2);

        let alice_incoming = storage.find_incoming_edges(alice_id).unwrap();
        assert_eq!(alice_incoming.len(), 1);
    }

    #[test]
    fn test_stats() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        storage
            .insert_node(&["Company".to_string()], &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.label_count, 2);
        assert_eq!(stats.edge_type_count, 1);
    }

    #[test]
    fn test_delete_node_cascades() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();
        let bob_id = storage
            .insert_node(&["Person".to_string()], &serde_json::json!({}))
            .unwrap();

        let rel_id = storage
            .insert_edge(alice_id, bob_id, "KNOWS", &serde_json::json!({}))
            .unwrap();

        // Delete alice - should cascade delete the relationship
        storage.delete_node(alice_id).unwrap();

        assert!(storage.get_node(alice_id).unwrap().is_none());
        assert!(storage.get_edge(rel_id).unwrap().is_none());
        assert!(storage.get_node(bob_id).unwrap().is_some());
    }

    #[test]
    fn test_get_all_labels_and_types() {
        let storage = SqliteStorage::in_memory().unwrap();

        let alice_id = storage
            .insert_node(
                &["Person".to_string(), "Actor".to_string()],
                &serde_json::json!({}),
            )
            .unwrap();
        let movie_id = storage
            .insert_node(&["Movie".to_string()], &serde_json::json!({}))
            .unwrap();

        storage
            .insert_edge(alice_id, movie_id, "ACTED_IN", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(alice_id, movie_id, "DIRECTED", &serde_json::json!({}))
            .unwrap();

        let labels = storage.get_all_labels().unwrap();
        assert_eq!(labels, vec!["Actor", "Movie", "Person"]);

        let types = storage.get_all_edge_types().unwrap();
        assert_eq!(types, vec!["ACTED_IN", "DIRECTED"]);
    }

    #[test]
    fn test_get_label_counts() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Insert nodes with various labels
        storage
            .insert_node(
                &["Person".to_string()],
                &serde_json::json!({"name": "Alice"}),
            )
            .unwrap();
        storage
            .insert_node(&["Person".to_string()], &serde_json::json!({"name": "Bob"}))
            .unwrap();
        storage
            .insert_node(
                &["Company".to_string()],
                &serde_json::json!({"name": "Acme"}),
            )
            .unwrap();
        storage
            .insert_node(&["User".to_string()], &serde_json::json!({"name": "User1"}))
            .unwrap();

        let counts = storage.get_label_counts().unwrap();

        assert_eq!(counts.get("Person"), Some(&2));
        assert_eq!(counts.get("Company"), Some(&1));
        assert_eq!(counts.get("User"), Some(&1));
        assert_eq!(counts.get("Unknown"), None);
    }

    #[test]
    fn test_property_name_validation() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with a valid property
        let props = serde_json::json!({"object_id": "test123"});
        storage.insert_node(&["Test".to_string()], &props).unwrap();

        // Valid property names should work
        assert!(storage
            .find_node_by_property("object_id", "test123")
            .is_ok());
        assert!(storage.find_node_by_property("valid_name", "value").is_ok());
        assert!(storage.find_node_by_property("name123", "value").is_ok());

        // Invalid property names should be rejected
        assert!(storage.find_node_by_property("", "value").is_err());
        assert!(storage.find_node_by_property("name.path", "value").is_err());
        assert!(storage.find_node_by_property("name'--", "value").is_err());
        assert!(storage.find_node_by_property("name)", "value").is_err());
        assert!(storage.find_node_by_property("name$", "value").is_err());
        assert!(storage
            .find_node_by_property("name space", "value")
            .is_err());

        // Same validation for build_property_index
        assert!(storage.build_property_index("object_id").is_ok());
        assert!(storage.build_property_index("").is_err());
        assert!(storage.build_property_index("name'--").is_err());
    }

    #[test]
    fn test_property_indexes() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Initially no property indexes
        let indexes = storage.list_property_indexes().unwrap();
        assert!(indexes.is_empty());

        // Create an index
        storage.create_property_index("object_id").unwrap();
        assert!(storage.has_property_index("object_id").unwrap());

        // List should show it
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes, vec!["object_id"]);

        // Creating same index again is a no-op
        storage.create_property_index("object_id").unwrap();
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes.len(), 1);

        // Create another index
        storage.create_property_index("name").unwrap();
        let indexes = storage.list_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.contains(&"object_id".to_string()));
        assert!(indexes.contains(&"name".to_string()));

        // Drop an index
        assert!(storage.drop_property_index("object_id").unwrap());
        assert!(!storage.has_property_index("object_id").unwrap());

        // Drop non-existent index returns false
        assert!(!storage.drop_property_index("object_id").unwrap());

        // Invalid property names should be rejected
        assert!(storage.create_property_index("").is_err());
        assert!(storage.create_property_index("name'--").is_err());
    }

    #[test]
    fn test_upsert_nodes_batch_merges_properties() {
        let mut storage = SqliteStorage::in_memory().unwrap();

        // Insert an orphan/placeholder node with minimal properties
        let orphans = vec![(
            vec!["Base".to_string()],
            serde_json::json!({
                "object_id": "S-1-5-21-TEST",
                "name": "S-1-5-21-TEST",
                "placeholder": true
            }),
        )];
        let ids1 = storage.upsert_nodes_batch(&orphans).unwrap();
        assert_eq!(ids1.len(), 1);

        // Now upsert the full node data - should merge, not overwrite
        let full_nodes = vec![(
            vec!["User".to_string()],
            serde_json::json!({
                "object_id": "S-1-5-21-TEST",
                "name": "testuser@corp.local",
                "enabled": true,
                "email": "test@example.com"
            }),
        )];
        let ids2 = storage.upsert_nodes_batch(&full_nodes).unwrap();
        assert_eq!(ids2.len(), 1);

        // Should be the same node (same ID)
        assert_eq!(ids1[0], ids2[0]);

        // Verify properties were merged
        let node = storage.get_node(ids1[0]).unwrap().unwrap();

        // New properties should be present
        assert_eq!(
            node.properties.get("name"),
            Some(&serde_json::json!("testuser@corp.local"))
                .map(|v| serde_json::from_value(v.clone()).unwrap())
                .as_ref()
        );
        assert_eq!(
            node.properties.get("enabled"),
            Some(&crate::graph::PropertyValue::Bool(true))
        );
        assert_eq!(
            node.properties.get("email"),
            Some(&crate::graph::PropertyValue::String(
                "test@example.com".to_string()
            ))
        );

        // Original placeholder property should still be there (json_patch merges)
        // Note: json_patch replaces values when keys conflict, so "name" is updated
        // but "placeholder" from the original should remain

        // Labels should be merged (both Base and User)
        assert!(node.has_label("User"));
        // Base label should also be present due to INSERT OR IGNORE
        assert!(node.has_label("Base"));
    }

    #[test]
    fn test_get_or_create_node_by_object_id() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create an orphan node
        let id1 = storage
            .get_or_create_node_by_object_id("S-1-5-21-ORPHAN", "User")
            .unwrap();
        assert!(id1 > 0);

        // Getting the same node again should return the same ID
        let id2 = storage
            .get_or_create_node_by_object_id("S-1-5-21-ORPHAN", "Computer")
            .unwrap();
        assert_eq!(id1, id2);

        // Verify the node was created with placeholder properties
        let node = storage.get_node(id1).unwrap().unwrap();
        assert_eq!(
            node.properties.get("object_id"),
            Some(&crate::graph::PropertyValue::String(
                "S-1-5-21-ORPHAN".to_string()
            ))
        );
        assert_eq!(
            node.properties.get("placeholder"),
            Some(&crate::graph::PropertyValue::Bool(true))
        );
        assert!(node.has_label("User"));
    }

    #[test]
    fn test_query_history_crud() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Add a query
        storage
            .add_query_history(NewQueryHistoryEntry {
                id: "q1",
                name: "Test Query",
                query: "MATCH (n) RETURN n",
                timestamp: 1700000000,
                result_count: Some(10),
                status: "completed",
                started_at: 1700000000,
                duration_ms: Some(100),
                error: None,
                background: false,
            })
            .unwrap();

        // Get query history
        let (rows, total) = storage.get_query_history(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "q1");
        assert_eq!(rows[0].name, "Test Query");
        assert_eq!(rows[0].query, "MATCH (n) RETURN n");
        assert_eq!(rows[0].result_count, Some(10));
        assert_eq!(rows[0].status, "completed");
        assert_eq!(rows[0].duration_ms, Some(100));

        // Add another query
        storage
            .add_query_history(NewQueryHistoryEntry {
                id: "q2",
                name: "Failed Query",
                query: "INVALID",
                timestamp: 1700000001,
                result_count: None,
                status: "error",
                started_at: 1700000001,
                duration_ms: Some(50),
                error: Some("Parse error"),
                background: false,
            })
            .unwrap();

        let (rows, total) = storage.get_query_history(10, 0).unwrap();
        assert_eq!(total, 2);
        // Should be ordered by timestamp DESC
        assert_eq!(rows[0].id, "q2");
        assert_eq!(rows[1].id, "q1");

        // Update status
        storage
            .update_query_status("q2", "completed", Some(200), Some(5), None)
            .unwrap();

        let (rows, _) = storage.get_query_history(10, 0).unwrap();
        let q2 = rows.iter().find(|r| r.id == "q2").unwrap();
        assert_eq!(q2.status, "completed");
        assert_eq!(q2.duration_ms, Some(200));
        assert_eq!(q2.result_count, Some(5));

        // Delete one query
        storage.delete_query_history("q1").unwrap();
        let (rows, total) = storage.get_query_history(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows[0].id, "q2");

        // Clear all
        storage.clear_query_history().unwrap();
        let (rows, total) = storage.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
        assert!(rows.is_empty());
    }
}
