//! Graph algorithms for centrality metrics and analysis.
//!
//! This module implements graph algorithms that operate on the entire graph
//! or significant portions of it, rather than pattern-based queries.

use crate::error::Result;
use crate::graph::Edge;
use crate::storage::SqliteStorage;
use std::collections::{HashMap, VecDeque};

/// Result of edge betweenness centrality computation.
#[derive(Debug, Clone)]
pub struct EdgeBetweenness {
    /// Edge ID to betweenness score mapping.
    pub scores: HashMap<i64, f64>,
    /// Number of nodes processed.
    pub nodes_processed: usize,
    /// Number of edges in the graph.
    pub edges_count: usize,
}

impl EdgeBetweenness {
    /// Get the top k edges by betweenness score.
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

    /// Get edges with betweenness score above a threshold.
    pub fn above_threshold(&self, threshold: f64) -> Vec<(i64, f64)> {
        self.scores
            .iter()
            .filter(|(_, &score)| score > threshold)
            .map(|(&id, &score)| (id, score))
            .collect()
    }
}

/// Compute edge betweenness centrality using Brandes' algorithm.
///
/// Edge betweenness centrality measures how many shortest paths pass through
/// each edge. Edges with high betweenness are "choke points" - removing them
/// would disrupt many paths through the graph.
///
/// # Algorithm
///
/// For each source node s:
/// 1. BFS to compute shortest path distances and counts to all reachable nodes
/// 2. Backtrack from leaves to accumulate dependency values
/// 3. For each edge (v, w) where w is further from s than v:
///    - Add dependency contribution: (sigma_sv / sigma_sw) * (1 + delta_w)
///
/// # Complexity
///
/// O(V * E) for unweighted graphs, where V is the number of nodes and E is
/// the number of edges.
///
/// # Arguments
///
/// * `storage` - The storage backend to query
/// * `edge_types` - Optional filter to only consider specific edge types
/// * `direction` - Whether to treat edges as directed or undirected
///
/// # Returns
///
/// Edge betweenness scores for all edges (or filtered edges).
pub fn edge_betweenness_centrality(
    storage: &SqliteStorage,
    edge_types: Option<&[&str]>,
    directed: bool,
) -> Result<EdgeBetweenness> {
    // Load the graph structure
    let all_nodes = storage.scan_all_nodes()?;
    let all_edges = storage.scan_all_edges()?;

    // Filter edges by type if specified
    let edges: Vec<Edge> = if let Some(types) = edge_types {
        all_edges
            .into_iter()
            .filter(|e| types.contains(&e.edge_type.as_str()))
            .collect()
    } else {
        all_edges
    };

    let node_ids: Vec<i64> = all_nodes.iter().map(|n| n.id).collect();
    let num_nodes = node_ids.len();
    let num_edges = edges.len();

    // Build adjacency lists
    // For directed: outgoing edges only
    // For undirected: both directions
    let mut adj: HashMap<i64, Vec<(i64, i64)>> = HashMap::new(); // node -> [(neighbor, edge_id)]
    for node in &all_nodes {
        adj.insert(node.id, Vec::new());
    }

    for edge in &edges {
        adj.entry(edge.source)
            .or_default()
            .push((edge.target, edge.id));
        if !directed {
            adj.entry(edge.target)
                .or_default()
                .push((edge.source, edge.id));
        }
    }

    // Initialize betweenness scores
    let mut edge_betweenness: HashMap<i64, f64> = HashMap::new();
    for edge in &edges {
        edge_betweenness.insert(edge.id, 0.0);
    }

    // Brandes' algorithm: iterate over all source nodes
    for &source in &node_ids {
        // BFS data structures
        let mut stack: Vec<i64> = Vec::new(); // Nodes in order of discovery (for backtracking)
        let mut pred: HashMap<i64, Vec<(i64, i64)>> = HashMap::new(); // node -> [(predecessor, edge_id)]
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
                for &(w, edge_id) in neighbors {
                    // First visit to w?
                    if *dist.get(&w).unwrap() < 0 {
                        dist.insert(w, v_dist + 1);
                        queue.push_back(w);
                    }

                    // Is this edge on a shortest path?
                    if *dist.get(&w).unwrap() == v_dist + 1 {
                        let sigma_v = *sigma.get(&v).unwrap();
                        *sigma.get_mut(&w).unwrap() += sigma_v;
                        pred.get_mut(&w).unwrap().push((v, edge_id));
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
                for &(v, edge_id) in predecessors {
                    let sigma_v = *sigma.get(&v).unwrap();
                    // Contribution of this edge to betweenness
                    let contrib = (sigma_v / sigma_w) * (1.0 + delta_w);

                    // Update edge betweenness
                    *edge_betweenness.get_mut(&edge_id).unwrap() += contrib;

                    // Update node dependency for backpropagation
                    *delta.get_mut(&v).unwrap() += contrib;
                }
            }
        }
    }

    // For undirected graphs, each edge is counted twice (once from each endpoint)
    // so we divide by 2
    if !directed {
        for score in edge_betweenness.values_mut() {
            *score /= 2.0;
        }
    }

    Ok(EdgeBetweenness {
        scores: edge_betweenness,
        nodes_processed: num_nodes,
        edges_count: num_edges,
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
    fn test_edge_betweenness_simple_chain() {
        // Create a simple chain: A -> B -> C -> D
        // The middle edges should have higher betweenness
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

        // Insert edges
        let edge_ab = storage
            .insert_edge(node_a, node_b, "NEXT", &serde_json::json!({}))
            .unwrap();
        let edge_bc = storage
            .insert_edge(node_b, node_c, "NEXT", &serde_json::json!({}))
            .unwrap();
        let edge_cd = storage
            .insert_edge(node_c, node_d, "NEXT", &serde_json::json!({}))
            .unwrap();

        // Compute betweenness (undirected)
        let result = edge_betweenness_centrality(&storage, None, false).unwrap();

        assert_eq!(result.nodes_processed, 4);
        assert_eq!(result.edges_count, 3);

        // In an undirected chain A-B-C-D:
        // Edge B-C is on more shortest paths than A-B or C-D
        let score_ab = *result.scores.get(&edge_ab).unwrap();
        let score_bc = *result.scores.get(&edge_bc).unwrap();
        let score_cd = *result.scores.get(&edge_cd).unwrap();

        assert!(
            score_bc >= score_ab,
            "Middle edge should have higher betweenness"
        );
        assert!(
            score_bc >= score_cd,
            "Middle edge should have higher betweenness"
        );
    }

    #[test]
    fn test_edge_betweenness_star() {
        // Create a star graph: center connected to 4 outer nodes
        // All edges from center should have equal betweenness
        let (storage, _dir) = create_test_storage();

        let center = storage
            .insert_node(
                &["Node".to_string()],
                &serde_json::json!({"name": "Center"}),
            )
            .unwrap();

        let mut outer_nodes = Vec::new();
        let mut edges = Vec::new();

        for i in 0..4 {
            let props = serde_json::json!({"name": format!("N{}", i)});
            let outer = storage.insert_node(&["Node".to_string()], &props).unwrap();
            outer_nodes.push(outer);
            let edge = storage
                .insert_edge(center, outer, "LINK", &serde_json::json!({}))
                .unwrap();
            edges.push(edge);
        }

        let result = edge_betweenness_centrality(&storage, None, false).unwrap();

        // All edges should have the same betweenness (by symmetry)
        let first_score = *result.scores.get(&edges[0]).unwrap();
        for edge_id in &edges {
            let score = *result.scores.get(edge_id).unwrap();
            assert!(
                (score - first_score).abs() < 0.001,
                "Star edges should have equal betweenness"
            );
        }
    }

    #[test]
    fn test_edge_betweenness_with_type_filter() {
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

        // Create two types of edges
        let edge_ab = storage
            .insert_edge(node_a, node_b, "IMPORTANT", &serde_json::json!({}))
            .unwrap();
        let _edge_bc = storage
            .insert_edge(node_b, node_c, "TRIVIAL", &serde_json::json!({}))
            .unwrap();

        // Filter to only IMPORTANT edges
        let result = edge_betweenness_centrality(&storage, Some(&["IMPORTANT"]), false).unwrap();

        assert_eq!(result.edges_count, 1);
        assert!(result.scores.contains_key(&edge_ab));
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
            .insert_edge(node_a, node_b, "NEXT", &serde_json::json!({}))
            .unwrap();
        storage
            .insert_edge(node_b, node_c, "NEXT", &serde_json::json!({}))
            .unwrap();

        let result = edge_betweenness_centrality(&storage, None, false).unwrap();
        let top_1 = result.top_k(1);

        assert_eq!(top_1.len(), 1);
    }
}
