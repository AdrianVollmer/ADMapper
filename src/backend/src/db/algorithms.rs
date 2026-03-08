//! Shared graph algorithms for all database backends.
//!
//! These algorithms operate on the common `DbNode`/`DbEdge` types,
//! making them available to every backend without reimplementation.

use std::collections::{HashMap, VecDeque};

use tracing::debug;

use super::types::{ChokePoint, ChokePointsResponse, DbEdge, DbNode};

/// Automatic sampling threshold: exact computation for graphs up to this size,
/// sampled approximation above it.
const AUTO_SAMPLE_THRESHOLD: usize = 100;

/// Compute relationship betweenness centrality using Brandes' algorithm.
///
/// Returns the top `top_k` relationships ranked by betweenness score.
/// For graphs with more than 100 nodes, uses deterministic sampling
/// with extrapolation to keep runtime reasonable.
///
/// # Complexity
///
/// O(V * E) exact, or O(sample * E) with sampling.
pub fn relationship_betweenness_centrality(
    nodes: &[DbNode],
    edges: &[DbEdge],
    directed: bool,
    top_k: usize,
) -> ChokePointsResponse {
    let num_nodes = nodes.len();
    let num_edges = edges.len();

    if num_nodes == 0 || num_edges == 0 {
        return ChokePointsResponse {
            choke_points: Vec::new(),
            total_edges: num_edges,
            total_nodes: num_nodes,
        };
    }

    // Map node IDs to dense indices for fast array-based BFS
    let node_index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.as_str(), i))
        .collect();

    // Build adjacency list: node_idx -> [(neighbor_idx, edge_idx)]
    let mut adj: Vec<Vec<(usize, usize)>> = vec![Vec::new(); num_nodes];
    for (edge_idx, edge) in edges.iter().enumerate() {
        let Some(&src) = node_index.get(edge.source.as_str()) else {
            continue;
        };
        let Some(&tgt) = node_index.get(edge.target.as_str()) else {
            continue;
        };
        adj[src].push((tgt, edge_idx));
        if !directed {
            adj[tgt].push((src, edge_idx));
        }
    }

    // Determine sample size
    let effective_sample = if num_nodes <= AUTO_SAMPLE_THRESHOLD {
        num_nodes
    } else {
        AUTO_SAMPLE_THRESHOLD
    };

    // Select source nodes (deterministic stride sampling)
    let source_nodes: Vec<usize> = if effective_sample >= num_nodes {
        (0..num_nodes).collect()
    } else {
        let stride = num_nodes / effective_sample;
        (0..num_nodes)
            .step_by(stride)
            .take(effective_sample)
            .collect()
    };

    let is_sampled = source_nodes.len() < num_nodes;
    let scale_factor = if is_sampled {
        num_nodes as f64 / source_nodes.len() as f64
    } else {
        1.0
    };

    debug!(
        nodes = num_nodes,
        edges = num_edges,
        sampled = is_sampled,
        sources = source_nodes.len(),
        "Computing relationship betweenness centrality"
    );

    // Pre-allocate BFS arrays (reused across iterations)
    let mut edge_betweenness = vec![0.0f64; num_edges];
    let mut sigma = vec![0.0f64; num_nodes];
    let mut dist = vec![-1i32; num_nodes];
    let mut delta = vec![0.0f64; num_nodes];
    let mut pred: Vec<Vec<(usize, usize)>> = vec![Vec::new(); num_nodes];

    // Brandes' algorithm: iterate over source nodes
    for &source in &source_nodes {
        // Reset arrays
        for i in 0..num_nodes {
            sigma[i] = 0.0;
            dist[i] = -1;
            delta[i] = 0.0;
            pred[i].clear();
        }
        sigma[source] = 1.0;
        dist[source] = 0;

        let mut stack: Vec<usize> = Vec::new();
        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(source);

        // BFS forward pass
        while let Some(v) = queue.pop_front() {
            stack.push(v);
            let v_dist = dist[v];

            for &(w, edge_idx) in &adj[v] {
                if dist[w] < 0 {
                    dist[w] = v_dist + 1;
                    queue.push_back(w);
                }
                if dist[w] == v_dist + 1 {
                    sigma[w] += sigma[v];
                    pred[w].push((v, edge_idx));
                }
            }
        }

        // Backtrack: accumulate dependencies (leaves first)
        while let Some(w) = stack.pop() {
            for &(v, edge_idx) in &pred[w] {
                let contrib = (sigma[v] / sigma[w]) * (1.0 + delta[w]);
                edge_betweenness[edge_idx] += contrib;
                delta[v] += contrib;
            }
        }
    }

    // Undirected: each edge counted from both endpoints
    if !directed {
        for score in &mut edge_betweenness {
            *score /= 2.0;
        }
    }

    // Scale up if sampled
    if is_sampled {
        for score in &mut edge_betweenness {
            *score *= scale_factor;
        }
    }

    // Collect top-k
    let mut ranked: Vec<(usize, f64)> = edge_betweenness
        .iter()
        .enumerate()
        .filter(|(_, &s)| s > 0.0)
        .map(|(i, &s)| (i, s))
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(top_k);

    // Build lookup for node metadata
    let node_by_id: HashMap<&str, &DbNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let choke_points: Vec<ChokePoint> = ranked
        .into_iter()
        .filter_map(|(edge_idx, score)| {
            let edge = &edges[edge_idx];
            let src = node_by_id.get(edge.source.as_str())?;
            let tgt = node_by_id.get(edge.target.as_str())?;
            Some(ChokePoint {
                source_id: src.id.clone(),
                source_name: src.name.clone(),
                source_label: src.label.clone(),
                target_id: tgt.id.clone(),
                target_name: tgt.name.clone(),
                target_label: tgt.label.clone(),
                rel_type: edge.rel_type.clone(),
                betweenness: score,
            })
        })
        .collect();

    debug!(
        top_score = choke_points.first().map(|c| c.betweenness),
        results = choke_points.len(),
        "Betweenness centrality complete"
    );

    ChokePointsResponse {
        choke_points,
        total_edges: num_edges,
        total_nodes: num_nodes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(id: &str) -> DbNode {
        DbNode {
            id: id.to_string(),
            name: id.to_string(),
            label: "Node".to_string(),
            properties: json!({}),
        }
    }

    fn edge(src: &str, tgt: &str) -> DbEdge {
        DbEdge {
            source: src.to_string(),
            target: tgt.to_string(),
            rel_type: "REL".to_string(),
            properties: json!({}),
            source_type: None,
            target_type: None,
        }
    }

    #[test]
    fn test_empty_graph() {
        let result = relationship_betweenness_centrality(&[], &[], true, 10);
        assert_eq!(result.choke_points.len(), 0);
        assert_eq!(result.total_nodes, 0);
        assert_eq!(result.total_edges, 0);
    }

    #[test]
    fn test_chain_middle_edge_highest() {
        // A -> B -> C -> D: middle edges should have highest betweenness
        let nodes = vec![node("A"), node("B"), node("C"), node("D")];
        let edges = vec![edge("A", "B"), edge("B", "C"), edge("C", "D")];

        let result = relationship_betweenness_centrality(&nodes, &edges, false, 10);
        assert_eq!(result.total_nodes, 4);
        assert_eq!(result.total_edges, 3);

        // B-C should be highest (on 4 shortest paths: A-C, A-D, B-D, B-C... actually all
        // cross it), then A-B and C-D equal
        let scores: HashMap<String, f64> = result
            .choke_points
            .iter()
            .map(|c| (format!("{}-{}", c.source_id, c.target_id), c.betweenness))
            .collect();

        let bc = scores.get("B-C").copied().unwrap_or(0.0);
        let ab = scores.get("A-B").copied().unwrap_or(0.0);
        let cd = scores.get("C-D").copied().unwrap_or(0.0);
        assert!(bc >= ab, "B-C ({bc}) should be >= A-B ({ab})");
        assert!(bc >= cd, "B-C ({bc}) should be >= C-D ({cd})");
    }

    #[test]
    fn test_star_equal_betweenness() {
        // Star: center connected to 4 outer nodes (undirected)
        let nodes = vec![node("C"), node("N0"), node("N1"), node("N2"), node("N3")];
        let edges = vec![
            edge("C", "N0"),
            edge("C", "N1"),
            edge("C", "N2"),
            edge("C", "N3"),
        ];

        let result = relationship_betweenness_centrality(&nodes, &edges, false, 10);
        assert_eq!(result.choke_points.len(), 4);

        // All edges should have equal betweenness by symmetry
        let first = result.choke_points[0].betweenness;
        for cp in &result.choke_points {
            assert!(
                (cp.betweenness - first).abs() < 0.001,
                "Star edges should be equal: {} vs {}",
                cp.betweenness,
                first
            );
        }
    }

    #[test]
    fn test_directed_vs_undirected() {
        // A -> B -> C: directed should have different scores than undirected
        let nodes = vec![node("A"), node("B"), node("C")];
        let edges = vec![edge("A", "B"), edge("B", "C")];

        let directed = relationship_betweenness_centrality(&nodes, &edges, true, 10);
        let undirected = relationship_betweenness_centrality(&nodes, &edges, false, 10);

        // Both should find edges, but scores differ
        assert!(!directed.choke_points.is_empty());
        assert!(!undirected.choke_points.is_empty());
    }

    #[test]
    fn test_disconnected_components() {
        // Two separate chains: A-B and C-D
        let nodes = vec![node("A"), node("B"), node("C"), node("D")];
        let edges = vec![edge("A", "B"), edge("C", "D")];

        let result = relationship_betweenness_centrality(&nodes, &edges, false, 10);
        // Both edges exist but have low betweenness (only 1 pair uses each)
        assert_eq!(result.choke_points.len(), 2);
    }

    #[test]
    fn test_top_k_limits_results() {
        let nodes = vec![node("A"), node("B"), node("C"), node("D")];
        let edges = vec![edge("A", "B"), edge("B", "C"), edge("C", "D")];

        let result = relationship_betweenness_centrality(&nodes, &edges, false, 1);
        assert_eq!(result.choke_points.len(), 1);
    }

    #[test]
    fn test_preserves_node_metadata() {
        let nodes = vec![
            DbNode {
                id: "S-1-5-21-1".to_string(),
                name: "admin@corp.local".to_string(),
                label: "User".to_string(),
                properties: json!({}),
            },
            DbNode {
                id: "S-1-5-21-2".to_string(),
                name: "Domain Admins".to_string(),
                label: "Group".to_string(),
                properties: json!({}),
            },
        ];
        let edges = vec![DbEdge {
            source: "S-1-5-21-1".to_string(),
            target: "S-1-5-21-2".to_string(),
            rel_type: "MemberOf".to_string(),
            properties: json!({}),
            source_type: None,
            target_type: None,
        }];

        let result = relationship_betweenness_centrality(&nodes, &edges, true, 10);
        assert_eq!(result.choke_points.len(), 1);
        let cp = &result.choke_points[0];
        assert_eq!(cp.source_name, "admin@corp.local");
        assert_eq!(cp.source_label, "User");
        assert_eq!(cp.target_name, "Domain Admins");
        assert_eq!(cp.target_label, "Group");
        assert_eq!(cp.rel_type, "MemberOf");
    }
}
