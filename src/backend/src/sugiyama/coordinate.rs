//! Brandes-Kopf coordinate assignment for Sugiyama layout.
//!
//! Implements the algorithm from "Fast and Simple Horizontal Coordinate
//! Assignment" (Brandes & Kopf, 2001). Four alignment passes (up-left,
//! up-right, down-left, down-right) are balanced by taking the median
//! x-coordinate for each node.

use super::layering::LayeredGraph;
use std::collections::HashSet;

/// Layout spacing parameters.
pub(crate) struct CoordConfig {
    /// Minimum horizontal distance between adjacent nodes in a layer.
    pub node_sep: f32,
    /// Vertical distance between adjacent layers.
    pub layer_sep: f32,
}

impl Default for CoordConfig {
    fn default() -> Self {
        CoordConfig {
            node_sep: 1.5,
            layer_sep: 2.0,
        }
    }
}

/// Assign (x, y) coordinates to all nodes. Only the first `n_real` entries
/// in the returned vec correspond to real nodes.
pub(crate) fn assign_coordinates(graph: &LayeredGraph, config: &CoordConfig) -> Vec<(f32, f32)> {
    let n = graph.layer_of.len();
    if n == 0 {
        return Vec::new();
    }

    let pos = build_pos_lookup(graph);
    let conflicts = mark_type1_conflicts(graph, &pos);

    // Four alignment passes
    let dirs: [(VDir, HDir); 4] = [
        (VDir::Down, HDir::Left),
        (VDir::Down, HDir::Right),
        (VDir::Up, HDir::Left),
        (VDir::Up, HDir::Right),
    ];

    let mut all_x: Vec<Vec<f32>> = Vec::with_capacity(4);
    for &(vdir, hdir) in &dirs {
        let (root, align) = vertical_alignment(graph, &pos, &conflicts, vdir, hdir);
        let x = horizontal_compaction(graph, &pos, &root, &align, config.node_sep, hdir);
        all_x.push(x);
    }

    // Normalize each assignment so min = 0
    for xs in &mut all_x {
        let min = xs.iter().copied().fold(f32::INFINITY, f32::min);
        if min.is_finite() {
            for x in xs.iter_mut() {
                *x -= min;
            }
        }
    }

    // Align left assignments to left edge (min=0, already done above) and
    // right assignments to the right edge. This prevents symmetric collapse.
    let max_width = all_x
        .iter()
        .map(|xs| {
            let max = xs.iter().copied().fold(f32::NEG_INFINITY, f32::max);
            let min = xs.iter().copied().fold(f32::INFINITY, f32::min);
            max - min
        })
        .fold(0.0_f32, f32::max);

    // Indices 1 and 3 are the right-aligned passes — anchor them to the right
    for &idx in &[1, 3] {
        let cur_max = all_x[idx]
            .iter()
            .copied()
            .fold(f32::NEG_INFINITY, f32::max);
        let shift = max_width - cur_max;
        for x in &mut all_x[idx] {
            *x += shift;
        }
    }

    // Balance: median of 4 (average of middle two)
    let mut result = Vec::with_capacity(n);
    for (i, layer) in graph.layer_of.iter().enumerate().take(n) {
        let mut vals = [all_x[0][i], all_x[1][i], all_x[2][i], all_x[3][i]];
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let x = (vals[1] + vals[2]) / 2.0;
        let y = *layer as f32 * config.layer_sep;
        result.push((x, y));
    }

    // Post-process: enforce minimum spacing within each layer, preserving
    // the crossing-minimized order.
    enforce_layer_spacing(&mut result, graph, config.node_sep);

    // Refine: move nodes toward the barycenter of their neighbors.
    // B-K aligns to medians which doesn't produce center-of-gravity
    // placement. This pass fixes that while keeping the crossing-minimized
    // ordering.
    barycenter_refine(&mut result, graph, config.node_sep);

    result
}

// ---------------------------------------------------------------------------
// Post-processing
// ---------------------------------------------------------------------------

/// Push apart nodes that are too close within the same layer, preserving
/// the crossing-minimized order and the centre of mass.
fn enforce_layer_spacing(
    coords: &mut [(f32, f32)],
    graph: &LayeredGraph,
    node_sep: f32,
) {
    for layer in &graph.layers {
        preserve_order_spacing(layer, coords, node_sep);
    }
}

/// Enforce minimum spacing within a single layer, keeping the given
/// left-to-right order (from crossing minimization) and re-centring
/// around the original centre of mass.
fn preserve_order_spacing(layer: &[usize], coords: &mut [(f32, f32)], node_sep: f32) {
    if layer.len() <= 1 {
        return;
    }

    let center: f32 = layer.iter().map(|&n| coords[n].0).sum::<f32>() / layer.len() as f32;

    // Push apart left-to-right in the crossing-minimized order
    for i in 1..layer.len() {
        let min_x = coords[layer[i - 1]].0 + node_sep;
        if coords[layer[i]].0 < min_x {
            coords[layer[i]].0 = min_x;
        }
    }

    // Re-centre around original centre of mass
    let new_center: f32 = layer.iter().map(|&n| coords[n].0).sum::<f32>() / layer.len() as f32;
    let shift = center - new_center;
    for &n in layer {
        coords[n].0 += shift;
    }
}

/// Move nodes toward the barycenter (center of gravity) of their
/// neighbours using alternating forward/backward sweeps. The
/// crossing-minimized ordering within each layer is preserved.
fn barycenter_refine(coords: &mut [(f32, f32)], graph: &LayeredGraph, node_sep: f32) {
    let n_layers = graph.layers.len();
    if n_layers <= 1 {
        return;
    }

    for _ in 0..12 {
        // Forward: place each node at the average x of its predecessors
        for l in 1..n_layers {
            for &node in &graph.layers[l] {
                let preds = &graph.in_adj[node];
                if !preds.is_empty() {
                    coords[node].0 = preds.iter().map(|&p| coords[p].0).sum::<f32>()
                        / preds.len() as f32;
                }
            }
            preserve_order_spacing(&graph.layers[l], coords, node_sep);
        }

        // Backward: place each node at the average x of its successors
        for l in (0..n_layers - 1).rev() {
            for &node in &graph.layers[l] {
                let succs = &graph.out_adj[node];
                if !succs.is_empty() {
                    coords[node].0 = succs.iter().map(|&c| coords[c].0).sum::<f32>()
                        / succs.len() as f32;
                }
            }
            preserve_order_spacing(&graph.layers[l], coords, node_sep);
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum VDir {
    /// Process from layer 0 downward (use predecessors for alignment).
    Down,
    /// Process from last layer upward (use successors for alignment).
    Up,
}

#[derive(Clone, Copy, PartialEq)]
enum HDir {
    Left,
    Right,
}

/// Set of edges involved in type-1 conflicts, stored as (source, target).
type ConflictSet = HashSet<(usize, usize)>;

// ---------------------------------------------------------------------------
// Position lookup
// ---------------------------------------------------------------------------

fn build_pos_lookup(graph: &LayeredGraph) -> Vec<usize> {
    let mut pos = vec![0usize; graph.layer_of.len()];
    for layer in &graph.layers {
        for (p, &node) in layer.iter().enumerate() {
            pos[node] = p;
        }
    }
    pos
}

// ---------------------------------------------------------------------------
// Type-1 conflict detection
// ---------------------------------------------------------------------------

/// Mark non-inner segments that cross an inner segment (edge between two
/// virtual nodes). These edges must not be used for alignment.
fn mark_type1_conflicts(graph: &LayeredGraph, pos: &[usize]) -> ConflictSet {
    let mut conflicts = ConflictSet::new();
    let n_layers = graph.layers.len();

    for l in 0..n_layers.saturating_sub(1) {
        // Edges between layer l and l+1 as (src, tgt, is_inner)
        let mut segments: Vec<(usize, usize, bool)> = Vec::new();
        for &u in &graph.layers[l] {
            for &v in &graph.out_adj[u] {
                if graph.layer_of[v] == l + 1 {
                    let inner = graph.is_virtual[u] && graph.is_virtual[v];
                    segments.push((u, v, inner));
                }
            }
        }

        // Check each inner segment against each non-inner segment
        for i in 0..segments.len() {
            for j in (i + 1)..segments.len() {
                let (u1, v1, inner1) = segments[i];
                let (u2, v2, inner2) = segments[j];
                if inner1 == inner2 {
                    continue;
                }
                let crosses = (pos[u1] < pos[u2]) != (pos[v1] < pos[v2]);
                if crosses {
                    if !inner1 {
                        conflicts.insert((u1, v1));
                    }
                    if !inner2 {
                        conflicts.insert((u2, v2));
                    }
                }
            }
        }
    }

    conflicts
}

// ---------------------------------------------------------------------------
// Vertical alignment
// ---------------------------------------------------------------------------

/// Build root[] and align[] arrays for one of the four direction combos.
///
/// `root[v]` = topmost node in v's alignment block.
/// `align[v]` = next node in the circular chain (align[last] = root).
fn vertical_alignment(
    graph: &LayeredGraph,
    pos: &[usize],
    conflicts: &ConflictSet,
    vdir: VDir,
    hdir: HDir,
) -> (Vec<usize>, Vec<usize>) {
    let n = graph.layer_of.len();
    let n_layers = graph.layers.len();
    let mut root: Vec<usize> = (0..n).collect();
    let mut align: Vec<usize> = (0..n).collect();

    let layer_order: Vec<usize> = match vdir {
        VDir::Down => (1..n_layers).collect(),
        VDir::Up => (0..n_layers.saturating_sub(1)).rev().collect(),
    };

    for l in layer_order {
        let layer = &graph.layers[l];

        let adj_l = match vdir {
            VDir::Down => l - 1,
            VDir::Up => l + 1,
        };

        // Track the boundary position to avoid crossing alignments
        let mut r: i32 = match hdir {
            HDir::Left => -1,
            HDir::Right => graph.layers.get(adj_l).map_or(0, |lay| lay.len()) as i32,
        };

        let indices: Vec<usize> = match hdir {
            HDir::Left => (0..layer.len()).collect(),
            HDir::Right => (0..layer.len()).rev().collect(),
        };

        for idx in indices {
            let v = layer[idx];

            let neighbors: &[usize] = match vdir {
                VDir::Down => &graph.in_adj[v],
                VDir::Up => &graph.out_adj[v],
            };

            let mut sorted_nb: Vec<usize> = neighbors
                .iter()
                .filter(|&&nb| graph.layer_of[nb] == adj_l)
                .copied()
                .collect();
            sorted_nb.sort_by_key(|&nb| pos[nb]);

            let d = sorted_nb.len();
            if d == 0 {
                continue;
            }

            // Median candidate(s)
            let medians = median_indices(d, hdir);

            for m in medians {
                if align[v] != v {
                    break;
                }

                let u = sorted_nb[m];

                // Check for type-1 conflict (use canonical edge direction)
                let edge = match vdir {
                    VDir::Down => (u, v),
                    VDir::Up => (v, u),
                };
                if conflicts.contains(&edge) {
                    continue;
                }

                let u_pos = pos[u] as i32;
                let ok = match hdir {
                    HDir::Left => u_pos > r,
                    HDir::Right => u_pos < r,
                };

                if ok {
                    align[u] = v;
                    root[v] = root[u];
                    align[v] = root[v];
                    r = u_pos;
                }
            }
        }
    }

    (root, align)
}

/// Return the median index/indices for a neighbor list of size `d`.
fn median_indices(d: usize, hdir: HDir) -> Vec<usize> {
    let m_left = (d - 1) / 2;
    let m_right = d / 2;
    if m_left == m_right {
        vec![m_left]
    } else {
        match hdir {
            HDir::Left => vec![m_left, m_right],
            HDir::Right => vec![m_right, m_left],
        }
    }
}

// ---------------------------------------------------------------------------
// Horizontal compaction
// ---------------------------------------------------------------------------

/// Assign x-coordinates by placing aligned blocks, respecting min spacing.
fn horizontal_compaction(
    graph: &LayeredGraph,
    pos: &[usize],
    root: &[usize],
    align: &[usize],
    node_sep: f32,
    hdir: HDir,
) -> Vec<f32> {
    let n = graph.layer_of.len();
    let mut sink: Vec<usize> = (0..n).collect();
    let mut shift = vec![f32::NAN; n];
    let mut x = vec![f32::NAN; n];

    // Initialize shift sentinel
    match hdir {
        HDir::Left => shift.fill(f32::INFINITY),
        HDir::Right => shift.fill(f32::NEG_INFINITY),
    }

    // Place every block root
    for v in 0..n {
        if root[v] == v {
            place_block(
                v, graph, pos, root, align, &mut sink, &mut shift, &mut x, node_sep, hdir,
            );
        }
    }

    // Apply class shifts
    for v in 0..n {
        x[v] = x[root[v]];
        let s = shift[sink[root[v]]];
        match hdir {
            HDir::Left => {
                if s < f32::INFINITY {
                    x[v] += s;
                }
            }
            HDir::Right => {
                if s > f32::NEG_INFINITY {
                    x[v] += s;
                }
            }
        }
    }

    // Mirror right assignments so all directions share the same orientation
    if hdir == HDir::Right {
        for xi in &mut x {
            *xi = -*xi;
        }
    }

    x
}

/// Recursively place a block and all blocks it depends on.
#[allow(clippy::too_many_arguments)]
fn place_block(
    v: usize,
    graph: &LayeredGraph,
    pos: &[usize],
    root: &[usize],
    align: &[usize],
    sink: &mut [usize],
    shift: &mut [f32],
    x: &mut [f32],
    node_sep: f32,
    hdir: HDir,
) {
    if !x[v].is_nan() {
        return;
    }
    x[v] = 0.0;

    let mut w = v;
    loop {
        let layer = &graph.layers[graph.layer_of[w]];
        let p = pos[w];

        let neighbor_idx = match hdir {
            HDir::Left => p.checked_sub(1),
            HDir::Right => {
                if p + 1 < layer.len() {
                    Some(p + 1)
                } else {
                    None
                }
            }
        };

        if let Some(np) = neighbor_idx {
            let u = layer[np];
            let u_root = root[u];
            place_block(
                u_root, graph, pos, root, align, sink, shift, x, node_sep, hdir,
            );

            if sink[v] == v {
                sink[v] = sink[u_root];
            }

            if sink[v] != sink[u_root] {
                match hdir {
                    HDir::Left => {
                        shift[sink[u_root]] =
                            shift[sink[u_root]].min(x[v] - x[u_root] - node_sep);
                    }
                    HDir::Right => {
                        shift[sink[u_root]] =
                            shift[sink[u_root]].max(x[v] - x[u_root] + node_sep);
                    }
                }
            } else {
                match hdir {
                    HDir::Left => {
                        x[v] = x[v].max(x[u_root] + node_sep);
                    }
                    HDir::Right => {
                        x[v] = x[v].min(x[u_root] - node_sep);
                    }
                }
            }
        }

        w = align[w];
        if w == v {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sugiyama::graph::DagGraph;
    use crate::sugiyama::layering::{assign_layers, build_layered_graph};

    fn make_layered(n: usize, edges: &[[usize; 2]]) -> LayeredGraph {
        let g = DagGraph::new(n, edges);
        let layers = assign_layers(&g);
        build_layered_graph(&g, &layers)
    }

    #[test]
    fn single_node() {
        let lg = make_layered(1, &[]);
        let config = CoordConfig::default();
        let coords = assign_coordinates(&lg, &config);
        assert_eq!(coords.len(), 1);
    }

    #[test]
    fn chain_spacing() {
        let lg = make_layered(3, &[[0, 1], [1, 2]]);
        let config = CoordConfig::default();
        let coords = assign_coordinates(&lg, &config);

        // All three should share the same x (single chain, no branching)
        let eps = 0.01;
        assert!((coords[0].0 - coords[1].0).abs() < eps);
        assert!((coords[1].0 - coords[2].0).abs() < eps);

        // Y should increase with layer
        assert!(coords[1].1 > coords[0].1);
        assert!(coords[2].1 > coords[1].1);
    }

    #[test]
    fn diamond_no_overlap() {
        // A->{B,C}->D
        let lg = make_layered(4, &[[0, 1], [0, 2], [1, 3], [2, 3]]);
        let config = CoordConfig::default();
        let coords = assign_coordinates(&lg, &config);

        // B and C are on the same layer and must not overlap
        let b = &coords[1];
        let c = &coords[2];
        assert!(
            (b.0 - c.0).abs() >= config.node_sep - 0.01,
            "B and C too close: {} vs {}",
            b.0,
            c.0
        );
    }

    #[test]
    fn symmetric_graph_roughly_centered() {
        // A -> B, A -> C  (B and C on same layer)
        let lg = make_layered(3, &[[0, 1], [0, 2]]);
        let config = CoordConfig::default();
        let coords = assign_coordinates(&lg, &config);

        // A should be roughly centered between B and C
        let a_x = coords[0].0;
        let mid = (coords[1].0 + coords[2].0) / 2.0;
        assert!(
            (a_x - mid).abs() < config.node_sep,
            "root not centered: a={a_x}, mid={mid}"
        );
    }

    /// Targets should sit at the barycenter (average x) of their parents.
    #[test]
    fn barycenter_positioning() {
        // 6 sources (0-5), 3 targets (6-8) — mimics hierarchical5.png
        // target 6: parents 0,1,2     (top third)
        // target 7: parents 2,3       (middle)
        // target 8: parents 3,4,5     (bottom third)
        let edges: &[[usize; 2]] = &[
            [0, 6],
            [1, 6],
            [2, 6],
            [2, 7],
            [3, 7],
            [3, 8],
            [4, 8],
            [5, 8],
        ];
        let g = DagGraph::new(9, edges);
        let layers = assign_layers(&g);
        let mut lg = build_layered_graph(&g, &layers);
        crate::sugiyama::crossing::minimize_crossings(&mut lg, 24, None);
        let config = CoordConfig::default();
        let coords = assign_coordinates(&lg, &config);

        // Each target's x should be close to the average x of its parents
        for &(target, ref parents) in &[(6, vec![0, 1, 2]), (7, vec![2, 3]), (8, vec![3, 4, 5])] {
            let avg_parent_x =
                parents.iter().map(|&p| coords[p].0).sum::<f32>() / parents.len() as f32;
            let diff = (coords[target].0 - avg_parent_x).abs();
            assert!(
                diff < config.node_sep + 0.1,
                "target {target}: x={:.2}, parent avg={:.2}, diff={:.2}",
                coords[target].0,
                avg_parent_x,
                diff,
            );
        }
    }
}
