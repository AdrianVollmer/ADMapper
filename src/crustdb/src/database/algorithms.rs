use crate::error::{Error, Result};
use crate::query;
use crate::query::executor::algorithms::RelationshipBetweenness;

use super::compute_hash;

impl super::Database {
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
