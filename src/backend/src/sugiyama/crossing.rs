//! Crossing minimization using barycenter heuristic with sweep iterations.

use super::layering::LayeredGraph;

/// Minimize edge crossings by reordering nodes within layers.
///
/// Uses alternating forward/backward barycenter sweeps, keeping the
/// ordering with the fewest crossings seen so far.
///
/// `sort_keys` (optional): one entry per real node, used as tiebreaker
/// when two nodes share the same barycenter value.
pub(crate) fn minimize_crossings(
    graph: &mut LayeredGraph,
    max_iterations: usize,
    sort_keys: Option<&[String]>,
) {
    let n_layers = graph.layers.len();
    if n_layers <= 1 {
        return;
    }

    let total = graph.layer_of.len();
    let mut pos = vec![0usize; total];

    // Pre-sort layers by sort key so the initial order reflects the
    // desired tiebreaking even if the loop exits early (0 crossings).
    if let Some(keys) = sort_keys {
        for layer in &mut graph.layers {
            layer.sort_by(|&a, &b| keys.get(a).cmp(&keys.get(b)));
        }
    }

    rebuild_positions(&graph.layers, &mut pos);

    let mut best_crossings = count_all_crossings(&graph.layers, &graph.out_adj, &pos);
    let mut best_order = graph.layers.clone();

    for _ in 0..max_iterations {
        // Forward sweep (top to bottom): order by barycenter of predecessors
        for l in 1..n_layers {
            order_by_barycenter(&mut graph.layers[l], &graph.in_adj, &pos, sort_keys);
            update_positions(&graph.layers[l], &mut pos);
        }

        // Backward sweep (bottom to top): order by barycenter of successors
        for l in (0..n_layers - 1).rev() {
            order_by_barycenter(&mut graph.layers[l], &graph.out_adj, &pos, sort_keys);
            update_positions(&graph.layers[l], &mut pos);
        }

        let c = count_all_crossings(&graph.layers, &graph.out_adj, &pos);
        if c < best_crossings {
            best_crossings = c;
            best_order.clone_from(&graph.layers);
        }
        if best_crossings == 0 {
            break;
        }
    }

    graph.layers = best_order;
    rebuild_positions(&graph.layers, &mut pos);
}

/// Sort a single layer by the barycenter of each node's neighbors.
/// For equal barycenters, `sort_keys` is used as tiebreaker.
fn order_by_barycenter(
    layer: &mut [usize],
    adj: &[Vec<usize>],
    pos: &[usize],
    sort_keys: Option<&[String]>,
) {
    let mut scored: Vec<(usize, f64)> = layer
        .iter()
        .map(|&node| {
            let neighbors = &adj[node];
            if neighbors.is_empty() {
                (node, pos[node] as f64)
            } else {
                let sum: f64 = neighbors.iter().map(|&nb| pos[nb] as f64).sum();
                (node, sum / neighbors.len() as f64)
            }
        })
        .collect();

    scored.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                // Tiebreaker: sort by key (label\tname) when barycenters match
                let ka = sort_keys.and_then(|k| k.get(a.0));
                let kb = sort_keys.and_then(|k| k.get(b.0));
                ka.cmp(&kb)
            })
    });

    for (i, (node, _)) in scored.into_iter().enumerate() {
        layer[i] = node;
    }
}

fn update_positions(layer: &[usize], pos: &mut [usize]) {
    for (p, &node) in layer.iter().enumerate() {
        pos[node] = p;
    }
}

fn rebuild_positions(layers: &[Vec<usize>], pos: &mut [usize]) {
    for layer in layers {
        for (p, &node) in layer.iter().enumerate() {
            pos[node] = p;
        }
    }
}

/// Total edge crossings across all adjacent layer pairs.
fn count_all_crossings(layers: &[Vec<usize>], out_adj: &[Vec<usize>], pos: &[usize]) -> usize {
    layers
        .iter()
        .take(layers.len().saturating_sub(1))
        .map(|layer| count_crossings_between(layer, out_adj, pos))
        .sum()
}

/// Count crossings between a layer and the one below it.
///
/// Two edges (u1,v1) and (u2,v2) cross iff the upper positions and lower
/// positions are in opposite order. Uses merge-sort inversion counting
/// for O(E log E) performance.
fn count_crossings_between(upper: &[usize], out_adj: &[Vec<usize>], pos: &[usize]) -> usize {
    // Collect edges as (upper_pos, lower_pos), sorted by upper_pos
    let mut edges: Vec<(usize, usize)> = Vec::new();
    for &u in upper {
        for &v in &out_adj[u] {
            edges.push((pos[u], pos[v]));
        }
    }
    edges.sort();

    // Count inversions in the lower-position sequence
    let lower: Vec<usize> = edges.iter().map(|&(_, lp)| lp).collect();
    merge_sort_count(&mut lower.clone())
}

/// Count inversions via merge sort — O(n log n).
fn merge_sort_count(arr: &mut [usize]) -> usize {
    let n = arr.len();
    if n <= 1 {
        return 0;
    }
    let mid = n / 2;
    let mut left = arr[..mid].to_vec();
    let mut right = arr[mid..].to_vec();

    let mut count = merge_sort_count(&mut left) + merge_sort_count(&mut right);

    let (mut i, mut j, mut k) = (0, 0, 0);
    while i < left.len() && j < right.len() {
        if left[i] <= right[j] {
            arr[k] = left[i];
            i += 1;
        } else {
            arr[k] = right[j];
            count += left.len() - i;
            j += 1;
        }
        k += 1;
    }
    arr[k..k + left.len() - i].copy_from_slice(&left[i..]);
    arr[k + left.len() - i..].copy_from_slice(&right[j..]);

    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inversion_count() {
        assert_eq!(merge_sort_count(&mut []), 0);
        assert_eq!(merge_sort_count(&mut [1]), 0);
        assert_eq!(merge_sort_count(&mut [1, 2, 3]), 0);
        assert_eq!(merge_sort_count(&mut [3, 2, 1]), 3);
        assert_eq!(merge_sort_count(&mut [2, 1, 3]), 1);
    }

    #[test]
    fn known_crossing_count() {
        // Layer 0: [A(0), B(1)]  Layer 1: [C(2), D(3)]
        // Edges: A->D, B->C  — these cross
        use crate::sugiyama::graph::DagGraph;
        use crate::sugiyama::layering::{assign_layers, build_layered_graph};

        let g = DagGraph::new(4, &[[0, 3], [1, 2]]);
        let layers = assign_layers(&g);
        let lg = build_layered_graph(&g, &layers);

        let mut pos = vec![0usize; lg.layer_of.len()];
        rebuild_positions(&lg.layers, &mut pos);
        let c = count_all_crossings(&lg.layers, &lg.out_adj, &pos);
        assert_eq!(c, 1);
    }

    #[test]
    fn minimization_reduces_crossings() {
        use crate::sugiyama::graph::DagGraph;
        use crate::sugiyama::layering::{assign_layers, build_layered_graph};

        // Build a graph where initial ordering has crossings
        // A->D, B->C (crossing), A->C, B->D
        let g = DagGraph::new(4, &[[0, 3], [1, 2], [0, 2], [1, 3]]);
        let layers = assign_layers(&g);
        let mut lg = build_layered_graph(&g, &layers);

        let mut pos = vec![0usize; lg.layer_of.len()];
        rebuild_positions(&lg.layers, &mut pos);
        let before = count_all_crossings(&lg.layers, &lg.out_adj, &pos);

        minimize_crossings(&mut lg, 12, None);

        rebuild_positions(&lg.layers, &mut pos);
        let after = count_all_crossings(&lg.layers, &lg.out_adj, &pos);
        assert!(after <= before);
    }

    /// Bipartite graph matching hierarchical6.png structure: computers → groups.
    /// After minimization, crossings should be zero or near-zero.
    #[test]
    fn bipartite_zero_crossings() {
        use crate::sugiyama::graph::DagGraph;
        use crate::sugiyama::layering::{assign_layers, build_layered_graph};

        // 6 computers (0-5) → 7 groups (6-12), many-to-many
        let edges: &[[usize; 2]] = &[
            [0, 6],
            [0, 7],
            [1, 8],
            [2, 9],
            [2, 10],
            [2, 11],
            [3, 10],
            [3, 11],
            [4, 11],
            [4, 12],
            [5, 12],
        ];
        let g = DagGraph::new(13, edges);
        let layers = assign_layers(&g);
        let mut lg = build_layered_graph(&g, &layers);

        minimize_crossings(&mut lg, 24, None);

        let mut pos = vec![0usize; lg.layer_of.len()];
        rebuild_positions(&lg.layers, &mut pos);
        let crossings = count_all_crossings(&lg.layers, &lg.out_adj, &pos);
        // Nodes 2 and 3 both connect to 10 and 11, forming K_{2,2} which
        // has exactly 1 unavoidable crossing in any layered drawing.
        assert!(
            crossings <= 1,
            "expected at most 1 crossing (K_{{2,2}} minimum), got {crossings}"
        );
    }
}
