//! Sugiyama hierarchical graph layout algorithm.
//!
//! Implements the classic four-phase pipeline:
//! 1. Cycle removal (DFS back-edge reversal)
//! 2. Layer assignment (longest path from sources)
//! 3. Crossing minimization (barycenter sweeps)
//! 4. Coordinate assignment (Brandes-Kopf)
//!
//! Reference: Sugiyama, Tagawa & Toda (1981); Brandes & Kopf (2001).

mod coordinate;
mod crossing;
mod graph;
mod layering;

use coordinate::{assign_coordinates, CoordConfig};
use crossing::minimize_crossings;
use graph::DagGraph;
use layering::{assign_layers, build_layered_graph};

/// Configuration for the Sugiyama layout.
pub struct SugiyamaConfig {
    /// Minimum horizontal gap between adjacent nodes in a layer.
    pub node_sep: f32,
    /// Vertical gap between adjacent layers.
    pub layer_sep: f32,
    /// Number of barycenter sweep iterations for crossing minimization.
    pub crossing_iterations: usize,
}

impl Default for SugiyamaConfig {
    fn default() -> Self {
        SugiyamaConfig {
            node_sep: 1.5,
            layer_sep: 2.0,
            crossing_iterations: 24,
        }
    }
}

/// Compute a top-to-bottom hierarchical layout.
///
/// Returns `(x, y)` coordinates for each of the `n` input nodes (indices
/// 0..n). The caller is responsible for applying direction transforms and
/// normalization.
///
/// `sort_keys` (optional): one string per real node used as tiebreaker when
/// the barycenter heuristic cannot distinguish node order. Typically
/// `"{label}\t{name}"` so nodes are grouped by type, then sorted by name.
pub fn layout(
    n: usize,
    edges: &[[usize; 2]],
    config: &SugiyamaConfig,
    sort_keys: Option<&[String]>,
) -> Vec<(f32, f32)> {
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![(0.0, 0.0)];
    }

    // Phase 1: build DAG and remove cycles
    let mut dag = DagGraph::new(n, edges);
    dag.remove_cycles();

    // Phase 2: layer assignment
    let layers = assign_layers(&dag);

    // Phase 3: insert virtual nodes for long edges
    let mut layered = build_layered_graph(&dag, &layers);

    // Phase 4: crossing minimization
    minimize_crossings(&mut layered, config.crossing_iterations, sort_keys);

    // Phase 5: coordinate assignment (Brandes-Kopf)
    let coord_config = CoordConfig {
        node_sep: config.node_sep,
        layer_sep: config.layer_sep,
    };
    let all_coords = assign_coordinates(&layered, &coord_config);

    // Return only real node coordinates (strip virtual nodes)
    all_coords[..n].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_graph() {
        let coords = layout(0, &[], &SugiyamaConfig::default(), None);
        assert!(coords.is_empty());
    }

    #[test]
    fn single_node_graph() {
        let coords = layout(1, &[], &SugiyamaConfig::default(), None);
        assert_eq!(coords, vec![(0.0, 0.0)]);
    }

    #[test]
    fn chain() {
        let coords = layout(
            4,
            &[[0, 1], [1, 2], [2, 3]],
            &SugiyamaConfig::default(),
            None,
        );
        assert_eq!(coords.len(), 4);
        // Y should strictly increase along the chain
        for i in 0..3 {
            assert!(
                coords[i + 1].1 > coords[i].1,
                "y[{}]={} should be > y[{}]={}",
                i + 1,
                coords[i + 1].1,
                i,
                coords[i].1
            );
        }
    }

    #[test]
    fn no_duplicate_positions() {
        let edges: &[[usize; 2]] = &[[0, 2], [0, 3], [1, 3], [1, 4], [2, 5], [3, 5], [4, 5]];
        let coords = layout(6, edges, &SugiyamaConfig::default(), None);
        for i in 0..coords.len() {
            for j in (i + 1)..coords.len() {
                let dx = (coords[i].0 - coords[j].0).abs();
                let dy = (coords[i].1 - coords[j].1).abs();
                assert!(
                    dx > 0.01 || dy > 0.01,
                    "nodes {i} and {j} overlap at ({}, {})",
                    coords[i].0,
                    coords[i].1
                );
            }
        }
    }

    #[test]
    fn graph_with_cycle() {
        // A->B->C->A — cycle should be broken, layout should still work
        let coords = layout(
            3,
            &[[0, 1], [1, 2], [2, 0]],
            &SugiyamaConfig::default(),
            None,
        );
        assert_eq!(coords.len(), 3);
    }

    #[test]
    fn disconnected_components() {
        // Two separate chains: 0->1 and 2->3
        let coords = layout(4, &[[0, 1], [2, 3]], &SugiyamaConfig::default(), None);
        assert_eq!(coords.len(), 4);
        // Each chain should have increasing y
        assert!(coords[1].1 > coords[0].1);
        assert!(coords[3].1 > coords[2].1);
    }

    /// Bipartite-like graph mimicking AD computers → groups.
    /// All nodes must have distinct, finite positions.
    #[test]
    fn bipartite_ad_graph() {
        // 8 computers, 4 groups
        let edges: &[[usize; 2]] = &[
            [0, 8],
            [0, 9],
            [1, 8],
            [1, 10],
            [2, 9],
            [2, 10],
            [3, 8],
            [3, 9],
            [3, 11],
            [4, 10],
            [4, 11],
            [5, 8],
            [5, 11],
            [6, 9],
            [6, 10],
            [7, 8],
            [7, 9],
            [7, 10],
        ];
        let config = SugiyamaConfig::default();
        let coords = layout(12, edges, &config, None);
        assert_eq!(coords.len(), 12);

        for (i, &(x, y)) in coords.iter().enumerate() {
            assert!(x.is_finite(), "node {i} x is not finite: {x}");
            assert!(y.is_finite(), "node {i} y is not finite: {y}");
        }

        // No two nodes should overlap
        for i in 0..coords.len() {
            for j in (i + 1)..coords.len() {
                let dx = (coords[i].0 - coords[j].0).abs();
                let dy = (coords[i].1 - coords[j].1).abs();
                assert!(
                    dx > 0.01 || dy > 0.01,
                    "nodes {i} and {j} overlap at ({:.3}, {:.3}) vs ({:.3}, {:.3})",
                    coords[i].0,
                    coords[i].1,
                    coords[j].0,
                    coords[j].1,
                );
            }
        }
    }

    /// Nodes on the same layer must all be spaced at least node_sep apart.
    #[test]
    fn same_layer_separation() {
        // Three roots, each pointing to a shared sink
        let edges: &[[usize; 2]] = &[[0, 3], [1, 3], [2, 3]];
        let config = SugiyamaConfig::default();
        let coords = layout(4, edges, &config, None);

        // Nodes 0,1,2 are on layer 0 — check pairwise x distance
        let mut xs: Vec<f32> = coords[..3].iter().map(|c| c.0).collect();
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for i in 1..xs.len() {
            assert!(
                xs[i] - xs[i - 1] >= config.node_sep - 0.01,
                "same-layer nodes too close: {:.3} and {:.3}",
                xs[i - 1],
                xs[i],
            );
        }
    }

    /// Many-to-many edges: 10 sources each connecting to 3 of 5 targets.
    /// Exercises crossing minimization and coordinate assignment at scale.
    #[test]
    fn many_to_many_no_overlap() {
        let edges: &[[usize; 2]] = &[
            [0, 10],
            [0, 11],
            [0, 12],
            [1, 10],
            [1, 13],
            [1, 14],
            [2, 11],
            [2, 12],
            [2, 13],
            [3, 10],
            [3, 12],
            [3, 14],
            [4, 11],
            [4, 13],
            [4, 14],
            [5, 10],
            [5, 11],
            [5, 14],
            [6, 12],
            [6, 13],
            [6, 14],
            [7, 10],
            [7, 11],
            [7, 12],
            [8, 11],
            [8, 13],
            [8, 14],
            [9, 10],
            [9, 12],
            [9, 13],
        ];
        let coords = layout(15, edges, &SugiyamaConfig::default(), None);
        assert_eq!(coords.len(), 15);

        for i in 0..coords.len() {
            for j in (i + 1)..coords.len() {
                let dx = (coords[i].0 - coords[j].0).abs();
                let dy = (coords[i].1 - coords[j].1).abs();
                assert!(
                    dx > 0.01 || dy > 0.01,
                    "nodes {i} and {j} overlap at ({:.3}, {:.3}) vs ({:.3}, {:.3})",
                    coords[i].0,
                    coords[i].1,
                    coords[j].0,
                    coords[j].1,
                );
            }
        }
    }

    /// Three-layer DAG: sources → middle → sinks.
    #[test]
    fn three_layer_dag() {
        let edges: &[[usize; 2]] = &[
            [0, 3],
            [0, 4],
            [1, 4],
            [1, 5],
            [2, 3],
            [2, 5],
            [3, 6],
            [4, 6],
            [4, 7],
            [5, 7],
        ];
        let config = SugiyamaConfig::default();
        let coords = layout(8, edges, &config, None);
        assert_eq!(coords.len(), 8);

        // Layer 0: {0,1,2}, Layer 1: {3,4,5}, Layer 2: {6,7}
        // Each layer's y values should be equal within the layer
        let y0 = coords[0].1;
        assert!((coords[1].1 - y0).abs() < 0.01);
        assert!((coords[2].1 - y0).abs() < 0.01);

        let y1 = coords[3].1;
        assert!((coords[4].1 - y1).abs() < 0.01);
        assert!((coords[5].1 - y1).abs() < 0.01);
        assert!(y1 > y0, "layer 1 should be below layer 0");

        let y2 = coords[6].1;
        assert!((coords[7].1 - y2).abs() < 0.01);
        assert!(y2 > y1, "layer 2 should be below layer 1");

        // No overlaps
        for i in 0..coords.len() {
            for j in (i + 1)..coords.len() {
                let dx = (coords[i].0 - coords[j].0).abs();
                let dy = (coords[i].1 - coords[j].1).abs();
                assert!(
                    dx > 0.01 || dy > 0.01,
                    "nodes {i} and {j} overlap at ({:.3}, {:.3})",
                    coords[i].0,
                    coords[i].1,
                );
            }
        }
    }

    /// Reproduce the hierarchical2.png graph structure: computers → groups.
    #[test]
    fn hierarchical_screenshot_graph() {
        // 6 computers (0-5) → 7 groups (6-12)
        let edges: &[[usize; 2]] = &[
            [0, 6],
            [0, 7], // EXTDC01 → 2 groups
            [1, 8], // EXTRODC01 → 1 group
            [2, 9],
            [2, 10],
            [2, 11], // GHOST-DC01 → 3 groups
            [3, 10],
            [3, 11], // RODC01 → 2 groups
            [4, 11],
            [4, 12], // DC01 → 2 groups
            [5, 12], // DC02 → 1 group
        ];
        let config = SugiyamaConfig::default();
        let coords = layout(13, edges, &config, None);
        assert_eq!(coords.len(), 13);

        // All finite
        for (i, &(x, y)) in coords.iter().enumerate() {
            assert!(x.is_finite() && y.is_finite(), "node {i}: ({x}, {y})");
        }

        // Computers (0-5) should be on layer 0, groups (6-12) on layer 1
        // So all computers share a y, all groups share a different y
        let comp_y = coords[0].1;
        for i in 1..6 {
            assert!(
                (coords[i].1 - comp_y).abs() < 0.01,
                "computer {i} y={:.3} != expected {:.3}",
                coords[i].1,
                comp_y,
            );
        }
        let group_y = coords[6].1;
        for i in 7..13 {
            assert!(
                (coords[i].1 - group_y).abs() < 0.01,
                "group {i} y={:.3} != expected {:.3}",
                coords[i].1,
                group_y,
            );
        }

        // No overlapping nodes
        for i in 0..13 {
            for j in (i + 1)..13 {
                let dx = (coords[i].0 - coords[j].0).abs();
                let dy = (coords[i].1 - coords[j].1).abs();
                assert!(
                    dx > 0.01 || dy > 0.01,
                    "nodes {i} and {j} overlap at ({:.3}, {:.3})",
                    coords[i].0,
                    coords[i].1,
                );
            }
        }

        // Computers (layer 0) should be spaced at least node_sep apart
        let mut comp_xs: Vec<f32> = (0..6).map(|i| coords[i].0).collect();
        comp_xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for i in 1..comp_xs.len() {
            assert!(
                comp_xs[i] - comp_xs[i - 1] >= config.node_sep - 0.01,
                "computers too close: {:.3} and {:.3}",
                comp_xs[i - 1],
                comp_xs[i],
            );
        }

        // Groups (layer 1) should be spaced at least node_sep apart
        let mut group_xs: Vec<f32> = (6..13).map(|i| coords[i].0).collect();
        group_xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for i in 1..group_xs.len() {
            assert!(
                group_xs[i] - group_xs[i - 1] >= config.node_sep - 0.01,
                "groups too close: {:.3} and {:.3}",
                group_xs[i - 1],
                group_xs[i],
            );
        }
    }

    /// Verify coordinate ranges are reasonable (not all bunched in a tiny area).
    #[test]
    fn coordinate_range_reasonable() {
        let edges: &[[usize; 2]] = &[
            [0, 5],
            [0, 6],
            [1, 6],
            [1, 7],
            [2, 5],
            [2, 7],
            [3, 5],
            [3, 6],
            [3, 7],
            [4, 6],
            [4, 7],
        ];
        let coords = layout(8, edges, &SugiyamaConfig::default(), None);
        let xs: Vec<f32> = coords.iter().map(|c| c.0).collect();
        let ys: Vec<f32> = coords.iter().map(|c| c.1).collect();

        let x_range = xs.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - xs.iter().cloned().fold(f32::INFINITY, f32::min);
        let y_range = ys.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - ys.iter().cloned().fold(f32::INFINITY, f32::min);

        // Both ranges should be > 1 (not degenerate)
        assert!(x_range > 1.0, "x_range too small: {x_range}");
        assert!(y_range > 1.0, "y_range too small: {y_range}");
    }

    #[test]
    fn wide_graph_spacing() {
        // Root -> 5 children — children should all be spaced apart
        let edges: &[[usize; 2]] = &[[0, 1], [0, 2], [0, 3], [0, 4], [0, 5]];
        let config = SugiyamaConfig::default();
        let coords = layout(6, edges, &config, None);

        // All children (1..6) are on the same layer
        let mut child_xs: Vec<f32> = coords[1..].iter().map(|c| c.0).collect();
        child_xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for i in 1..child_xs.len() {
            assert!(
                child_xs[i] - child_xs[i - 1] >= config.node_sep - 0.01,
                "children too close: {} and {}",
                child_xs[i - 1],
                child_xs[i]
            );
        }
    }

    /// Sort keys should order children by (label, name) when barycenters
    /// are equal (all share the same single parent).
    #[test]
    fn sort_keys_tiebreak_ordering() {
        // Root (0) -> 5 children (1-5), all same barycenter
        let edges: &[[usize; 2]] = &[[0, 1], [0, 2], [0, 3], [0, 4], [0, 5]];
        let keys: Vec<String> = vec![
            "Root\troot".into(),       // 0
            "OU\tZebra".into(),        // 1
            "Container\tAlpha".into(), // 2
            "OU\tAlpha".into(),        // 3
            "Domain\tBeta".into(),     // 4
            "Container\tBeta".into(),  // 5
        ];
        let config = SugiyamaConfig::default();
        let coords = layout(6, edges, &config, Some(&keys));

        // Children should be ordered: Container/Alpha(2), Container/Beta(5),
        // Domain/Beta(4), OU/Alpha(3), OU/Zebra(1)
        let expected_order = [2, 5, 4, 3, 1];
        let mut children: Vec<(usize, f32)> = (1..=5).map(|i| (i, coords[i].0)).collect();
        children.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let actual_order: Vec<usize> = children.iter().map(|c| c.0).collect();
        assert_eq!(
            actual_order, expected_order,
            "children should be sorted by (label, name); got {actual_order:?}"
        );
    }
}
