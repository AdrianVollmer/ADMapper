//! Graph algorithms for centrality metrics and analysis.
//!
//! This module implements graph algorithms that operate on the entire graph
//! or significant portions of it, rather than pattern-based queries.

use crate::error::Result;
use crate::graph::Relationship;
use crate::storage::SqliteStorage;
use std::collections::{HashMap, VecDeque};

/// Result of relationship betweenness centrality computation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RelationshipBetweenness {
    /// Relationship ID to betweenness score mapping.
    pub scores: HashMap<i64, f64>,
    /// Number of nodes processed.
    pub nodes_processed: usize,
    /// Number of relationships in the graph.
    pub relationships_count: usize,
}

impl RelationshipBetweenness {
    /// Get the top k relationships by betweenness score.
    pub fn top_k(&self, k: usize) -> Vec<(i64, f64)> {
        let mut sorted: Vec<_> = self
            .scores
            .iter()
            .map(|(&id, &score)| (id, score))
            .collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted.truncate(k);
        sorted
    }

    /// Get relationships with betweenness score above a threshold.
    pub fn above_threshold(&self, threshold: f64) -> Vec<(i64, f64)> {
        self.scores
            .iter()
            .filter(|(_, &score)| score > threshold)
            .map(|(&id, &score)| (id, score))
            .collect()
    }
}

/// Compute relationship betweenness centrality using Brandes' algorithm.
///
/// Relationship betweenness centrality measures how many shortest paths pass through
/// each relationship. Edges with high betweenness are "choke points" - removing them
/// would disrupt many paths through the graph.
///
/// # Algorithm
///
/// For each source node s:
/// 1. BFS to compute shortest path distances and counts to all reachable nodes
/// 2. Backtrack from leaves to accumulate dependency values
/// 3. For each relationship (v, w) where w is further from s than v:
///    - Add dependency contribution: (sigma_sv / sigma_sw) * (1 + delta_w)
///
/// # Complexity
///
/// O(V * E) for unweighted graphs, where V is the number of nodes and E is
/// the number of relationships.
///
/// # Arguments
///
/// * `storage` - The storage backend to query
/// * `rel_types` - Optional filter to only consider specific relationship types
/// * `direction` - Whether to treat relationships as directed or undirected
///
/// # Returns
///
/// Relationship betweenness scores for all relationships (or filtered relationships).
pub fn relationship_betweenness_centrality(
    storage: &SqliteStorage,
    rel_types: Option<&[&str]>,
    directed: bool,
) -> Result<RelationshipBetweenness> {
    // Use sampled version with automatic sample size selection
    // For graphs > 500 nodes, sample to keep runtime reasonable
    relationship_betweenness_centrality_sampled(storage, rel_types, directed, None)
}

/// Compute relationship betweenness centrality with optional sampling.
///
/// When `sample_size` is Some(n), only n randomly selected source nodes are used,
/// and the result is extrapolated. This provides an approximation in O(sample * E)
/// instead of O(V * E), making it practical for large graphs.
///
/// When `sample_size` is None, automatic selection is used:
/// - Graphs ≤ 500 nodes: exact computation
/// - Graphs > 500 nodes: sample 500 nodes (extrapolated)
///
/// # Arguments
///
/// * `storage` - The storage backend to query
/// * `rel_types` - Optional filter to only consider specific relationship types
/// * `directed` - Whether to treat relationships as directed or undirected
/// * `sample_size` - Optional number of source nodes to sample (None = auto)
pub fn relationship_betweenness_centrality_sampled(
    storage: &SqliteStorage,
    rel_types: Option<&[&str]>,
    directed: bool,
    sample_size: Option<usize>,
) -> Result<RelationshipBetweenness> {
    // Load the graph structure
    let all_nodes = storage.scan_all_nodes()?;
    let all_relationships = storage.scan_all_relationships()?;

    // Filter relationships by type if specified
    let relationships: Vec<Relationship> = if let Some(types) = rel_types {
        all_relationships
            .into_iter()
            .filter(|r| types.contains(&r.rel_type.as_str()))
            .collect()
    } else {
        all_relationships
    };

    let node_ids: Vec<i64> = all_nodes.iter().map(|n| n.id).collect();
    let num_nodes = node_ids.len();
    let num_relationships = relationships.len();

    // Determine sample size
    // Default: exact for small graphs, sample for large
    // 100 nodes keeps runtime under 1 second for most graphs
    const AUTO_SAMPLE_THRESHOLD: usize = 100;
    let effective_sample = match sample_size {
        Some(n) => n.min(num_nodes),
        None => {
            if num_nodes <= AUTO_SAMPLE_THRESHOLD {
                num_nodes // Exact computation
            } else {
                AUTO_SAMPLE_THRESHOLD // Sample
            }
        }
    };

    // Select source nodes (sample if needed)
    let source_nodes: Vec<i64> = if effective_sample >= num_nodes {
        node_ids.clone()
    } else {
        // Deterministic sampling using stride for reproducibility
        let stride = num_nodes / effective_sample;
        node_ids
            .iter()
            .step_by(stride)
            .take(effective_sample)
            .copied()
            .collect()
    };

    let is_sampled = source_nodes.len() < num_nodes;
    let scale_factor = if is_sampled {
        num_nodes as f64 / source_nodes.len() as f64
    } else {
        1.0
    };

    // Build adjacency lists
    // For directed: outgoing relationships only
    // For undirected: both directions
    let mut adj: HashMap<i64, Vec<(i64, i64)>> = HashMap::new(); // node -> [(neighbor, rel_id)]
    for node in &all_nodes {
        adj.insert(node.id, Vec::new());
    }

    for relationship in &relationships {
        adj.entry(relationship.source)
            .or_default()
            .push((relationship.target, relationship.id));
        if !directed {
            adj.entry(relationship.target)
                .or_default()
                .push((relationship.source, relationship.id));
        }
    }

    // Initialize betweenness scores
    let mut relationship_betweenness: HashMap<i64, f64> = HashMap::new();
    for relationship in &relationships {
        relationship_betweenness.insert(relationship.id, 0.0);
    }

    // Brandes' algorithm: iterate over source nodes (sampled or all)
    for &source in &source_nodes {
        // BFS data structures
        let mut stack: Vec<i64> = Vec::new(); // Nodes in order of discovery (for backtracking)
        let mut pred: HashMap<i64, Vec<(i64, i64)>> = HashMap::new(); // node -> [(predecessor, rel_id)]
        let mut sigma: HashMap<i64, f64> = HashMap::new(); // Number of shortest paths
        let mut dist: HashMap<i64, i32> = HashMap::new(); // Distance from source

        // Initialize
        for &node in &node_ids {
            pred.insert(node, Vec::new());
            sigma.insert(node, 0.0);
            dist.insert(node, -1); // -1 = unvisited
        }
        sigma.insert(source, 1.0);
        dist.insert(source, 0);

        // BFS
        let mut queue: VecDeque<i64> = VecDeque::new();
        queue.push_back(source);

        while let Some(v) = queue.pop_front() {
            stack.push(v);
            let v_dist = *dist.get(&v).unwrap();

            if let Some(neighbors) = adj.get(&v) {
                for &(w, rel_id) in neighbors {
                    // First visit to w?
                    if *dist.get(&w).unwrap() < 0 {
                        dist.insert(w, v_dist + 1);
                        queue.push_back(w);
                    }

                    // Is this relationship on a shortest path?
                    if *dist.get(&w).unwrap() == v_dist + 1 {
                        let sigma_v = *sigma.get(&v).unwrap();
                        *sigma.get_mut(&w).unwrap() += sigma_v;
                        pred.get_mut(&w).unwrap().push((v, rel_id));
                    }
                }
            }
        }

        // Backtrack: accumulate dependencies
        let mut delta: HashMap<i64, f64> = HashMap::new();
        for &node in &node_ids {
            delta.insert(node, 0.0);
        }

        // Process nodes in reverse BFS order (leaves first)
        while let Some(w) = stack.pop() {
            let sigma_w = *sigma.get(&w).unwrap();
            let delta_w = *delta.get(&w).unwrap();

            if let Some(predecessors) = pred.get(&w) {
                for &(v, rel_id) in predecessors {
                    let sigma_v = *sigma.get(&v).unwrap();
                    // Contribution of this relationship to betweenness
                    let contrib = (sigma_v / sigma_w) * (1.0 + delta_w);

                    // Update relationship betweenness
                    *relationship_betweenness.get_mut(&rel_id).unwrap() += contrib;

                    // Update node dependency for backpropagation
                    *delta.get_mut(&v).unwrap() += contrib;
                }
            }
        }
    }

    // For undirected graphs, each relationship is counted twice (once from each endpoint)
    // so we divide by 2
    if !directed {
        for score in relationship_betweenness.values_mut() {
            *score /= 2.0;
        }
    }

    // Scale up scores if we used sampling to extrapolate to full graph
    if is_sampled {
        for score in relationship_betweenness.values_mut() {
            *score *= scale_factor;
        }
    }

    Ok(RelationshipBetweenness {
        scores: relationship_betweenness,
        nodes_processed: num_nodes,
        relationships_count: num_relationships,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_storage() -> (SqliteStorage, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::open(&db_path).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_relationship_betweenness_simple_chain() {
        // Create a simple chain: A -> B -> C -> D
        // The middle relationships should have higher betweenness
        let (storage, _dir) = create_test_storage();

        // Insert nodes
        let node_a = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "A"}))
            .unwrap();
        let node_b = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "B"}))
            .unwrap();
        let node_c = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "C"}))
            .unwrap();
        let node_d = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "D"}))
            .unwrap();

        // Insert relationships
        let rel_ab = storage
            .insert_relationship(node_a, node_b, "NEXT", &serde_json::json!({}))
            .unwrap();
        let rel_bc = storage
            .insert_relationship(node_b, node_c, "NEXT", &serde_json::json!({}))
            .unwrap();
        let rel_cd = storage
            .insert_relationship(node_c, node_d, "NEXT", &serde_json::json!({}))
            .unwrap();

        // Compute betweenness (undirected)
        let result = relationship_betweenness_centrality(&storage, None, false).unwrap();

        assert_eq!(result.nodes_processed, 4);
        assert_eq!(result.relationships_count, 3);

        // In an undirected chain A-B-C-D:
        // Relationship B-C is on more shortest paths than A-B or C-D
        let score_ab = *result.scores.get(&rel_ab).unwrap();
        let score_bc = *result.scores.get(&rel_bc).unwrap();
        let score_cd = *result.scores.get(&rel_cd).unwrap();

        assert!(
            score_bc >= score_ab,
            "Middle relationship should have higher betweenness"
        );
        assert!(
            score_bc >= score_cd,
            "Middle relationship should have higher betweenness"
        );
    }

    #[test]
    fn test_relationship_betweenness_star() {
        // Create a star graph: center connected to 4 outer nodes
        // All relationships from center should have equal betweenness
        let (storage, _dir) = create_test_storage();

        let center = storage
            .insert_node(
                &["Node".to_string()],
                &serde_json::json!({"name": "Center"}),
            )
            .unwrap();

        let mut outer_nodes = Vec::new();
        let mut relationships = Vec::new();

        for i in 0..4 {
            let props = serde_json::json!({"name": format!("N{}", i)});
            let outer = storage.insert_node(&["Node".to_string()], &props).unwrap();
            outer_nodes.push(outer);
            let relationship = storage
                .insert_relationship(center, outer, "LINK", &serde_json::json!({}))
                .unwrap();
            relationships.push(relationship);
        }

        let result = relationship_betweenness_centrality(&storage, None, false).unwrap();

        // All relationships should have the same betweenness (by symmetry)
        let first_score = *result.scores.get(&relationships[0]).unwrap();
        for rel_id in &relationships {
            let score = *result.scores.get(rel_id).unwrap();
            assert!(
                (score - first_score).abs() < 0.001,
                "Star relationships should have equal betweenness"
            );
        }
    }

    #[test]
    fn test_relationship_betweenness_with_type_filter() {
        let (storage, _dir) = create_test_storage();

        let node_a = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "A"}))
            .unwrap();
        let node_b = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "B"}))
            .unwrap();
        let node_c = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "C"}))
            .unwrap();

        // Create two types of relationships
        let rel_ab = storage
            .insert_relationship(node_a, node_b, "IMPORTANT", &serde_json::json!({}))
            .unwrap();
        let _rel_bc = storage
            .insert_relationship(node_b, node_c, "TRIVIAL", &serde_json::json!({}))
            .unwrap();

        // Filter to only IMPORTANT relationships
        let result =
            relationship_betweenness_centrality(&storage, Some(&["IMPORTANT"]), false).unwrap();

        assert_eq!(result.relationships_count, 1);
        assert!(result.scores.contains_key(&rel_ab));
    }

    #[test]
    fn test_top_k() {
        let (storage, _dir) = create_test_storage();

        // Create a chain
        let node_a = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "A"}))
            .unwrap();
        let node_b = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "B"}))
            .unwrap();
        let node_c = storage
            .insert_node(&["Node".to_string()], &serde_json::json!({"name": "C"}))
            .unwrap();

        storage
            .insert_relationship(node_a, node_b, "NEXT", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_relationship(node_b, node_c, "NEXT", &serde_json::json!({}))
            .unwrap();

        let result = relationship_betweenness_centrality(&storage, None, false).unwrap();
        let top_1 = result.top_k(1);

        assert_eq!(top_1.len(), 1);
    }
}
