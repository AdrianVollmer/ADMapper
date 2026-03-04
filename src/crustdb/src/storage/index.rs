//! Property index management.

use crate::error::Result;
use rusqlite::{params, OptionalExtension};

use super::{validate_property_name, SqliteStorage};

impl SqliteStorage {
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
