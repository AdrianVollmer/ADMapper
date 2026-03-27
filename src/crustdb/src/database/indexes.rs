use crate::error::{Error, Result};

impl super::Database {
    /// Create an index on a JSON property for faster lookups.
    ///
    /// This creates a SQLite expression index on `json_extract(properties, '$.property')`,
    /// which significantly speeds up queries that filter nodes by this property.
    ///
    /// Common properties to index: `objectid`, `name`, etc.
    ///
    /// # Example
    /// ```ignore
    /// db.create_property_index("objectid")?;
    /// // Now queries like MATCH (n {objectid: '...'}) will use the index
    /// ```
    pub fn create_property_index(&self, property: &str) -> Result<()> {
        self.require_writable()?;
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
        self.require_writable()?;
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
}
