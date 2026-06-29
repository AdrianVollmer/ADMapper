//! Server-side graph layout.
//!
//! All layout algorithms operate on simple index-based adjacency lists —
//! no external graph library needed.
//!
//! The force-directed layout is based on the Fruchterman-Reingold implementation
//! from visgraph by Raoul Luque, licensed under MIT OR Apache-2.0.
//! Source: <https://github.com/raoulluque/visgraph>
//!
//! Hierarchical: Sugiyama layered layout with Brandes-Kopf coordinate
//! assignment.

use crate::api::types::{
    ApiError, LayoutAlgorithm, LayoutDirection, LayoutRequest, LayoutResponse, NodePosition,
};
use axum::Json;

const DEFAULT_ITERATIONS: u32 = 300;
const DEFAULT_TEMPERATURE: f32 = 0.1;
const TARGET_SIZE: f32 = 800.0;

// ============================================================================
// HTTP handler
// ============================================================================

pub async fn graph_layout(
    Json(req): Json<LayoutRequest>,
) -> Result<Json<LayoutResponse>, ApiError> {
    let node_count = req.nodes.len();

    if node_count == 0 {
        return Ok(Json(LayoutResponse {
            positions: Vec::new(),
        }));
    }

    for edge in &req.edges {
        if edge[0] >= node_count || edge[1] >= node_count {
            return Err(ApiError::BadRequest(format!(
                "Edge index out of bounds: [{}, {}] (node count: {node_count})",
                edge[0], edge[1]
            )));
        }
    }

    let positions = tokio::task::spawn_blocking(move || compute_layout(&req))
        .await
        .map_err(|e| ApiError::Internal(format!("Layout task failed: {e}")))?;

    Ok(Json(LayoutResponse { positions }))
}

pub fn compute_layout(req: &LayoutRequest) -> Vec<NodePosition> {
    match req.algorithm {
        LayoutAlgorithm::ForceDirected => {
            let iterations = req.iterations.unwrap_or(DEFAULT_ITERATIONS);
            let temperature = req.temperature.unwrap_or(DEFAULT_TEMPERATURE);
            force_directed(&req.nodes, &req.edges, iterations, temperature)
        }
        LayoutAlgorithm::Hierarchical => {
            let direction = req.direction.unwrap_or(LayoutDirection::LeftToRight);
            hierarchical(
                &req.nodes,
                &req.edges,
                direction,
                req.node_labels.as_deref(),
            )
        }
        LayoutAlgorithm::Circular => circular(&req.nodes),
        LayoutAlgorithm::Grid => grid(&req.nodes),
        LayoutAlgorithm::Lattice => lattice(&req.nodes),
        LayoutAlgorithm::Radial => radial(
            &req.nodes,
            &req.edges,
            req.node_labels.as_deref(),
        ),
    }
}

// ============================================================================
// Force-directed (Fruchterman-Reingold)
// ============================================================================

/// Fruchterman-Reingold force-directed layout.
///
/// Originally based on visgraph by Raoul Luque (MIT OR Apache-2.0),
/// substantially reworked:
///   - proper cooling schedule (exponential decay from a large initial temp)
///   - weak gravity toward the centroid to prevent cluster drift
///   - component-aware: disconnected components are laid out independently
///     and then packed using skyline bin-packing
fn force_directed(
    node_ids: &[String],
    edges: &[[usize; 2]],
    iterations: u32,
    _initial_temperature: f32,
) -> Vec<NodePosition> {
    let n = node_ids.len();
    if n == 0 {
        return Vec::new();
    }

    // Find connected components via union-find.
    let components = connected_components(n, edges);

    // If there's only one component, lay it out directly.
    if components.len() == 1 {
        let positions = fr_single_component(n, edges, iterations);
        return assemble_positions(node_ids, &[positions]);
    }

    // Lay out each component independently, then pack them.
    let mut component_positions = Vec::with_capacity(components.len());
    for comp in &components {
        let cn = comp.len();
        let global_to_local: std::collections::HashMap<usize, usize> =
            comp.iter().enumerate().map(|(li, &gi)| (gi, li)).collect();
        let local_edges: Vec<[usize; 2]> = edges
            .iter()
            .filter_map(|e| {
                let a = global_to_local.get(&e[0])?;
                let b = global_to_local.get(&e[1])?;
                Some([*a, *b])
            })
            .collect();
        component_positions.push(fr_single_component(cn, &local_edges, iterations));
    }

    // Compute bounding boxes for each component.
    let bboxes: Vec<(f32, f32)> = component_positions
        .iter()
        .map(|cpos| {
            let (mut min_x, mut max_x) = (f32::INFINITY, f32::NEG_INFINITY);
            let (mut min_y, mut max_y) = (f32::INFINITY, f32::NEG_INFINITY);
            for &(cx, cy) in cpos {
                min_x = min_x.min(cx);
                max_x = max_x.max(cx);
                min_y = min_y.min(cy);
                max_y = max_y.max(cy);
            }
            ((max_x - min_x).max(0.0), (max_y - min_y).max(0.0))
        })
        .collect();

    // Skyline-pack the bounding boxes.
    let padding = 0.3;
    let origins = skyline_pack(&bboxes, padding);

    // Place each component at its packed origin.
    let mut all_pos = vec![(0.0_f32, 0.0_f32); n];
    for (ci, comp) in components.iter().enumerate() {
        let cpos = &component_positions[ci];
        let (ox, oy) = origins[ci];
        // Shift component so its local min is at the packed origin.
        let (mut min_x, mut min_y) = (f32::INFINITY, f32::INFINITY);
        for &(cx, cy) in cpos {
            min_x = min_x.min(cx);
            min_y = min_y.min(cy);
        }
        for (li, &gi) in comp.iter().enumerate() {
            all_pos[gi].0 = cpos[li].0 - min_x + ox;
            all_pos[gi].1 = cpos[li].1 - min_y + oy;
        }
    }

    let positions = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodePosition {
            id: id.clone(),
            x: all_pos[i].0,
            y: all_pos[i].1,
        })
        .collect();

    normalize(positions)
}

/// Skyline bin-packing: place rectangles into a compact, roughly square area.
///
/// Takes a slice of (width, height) bounding boxes and padding between them.
/// Returns the (x, y) origin for each rectangle's top-left corner.
///
/// Algorithm: maintain a "skyline" -- a sequence of (x, y) segments where y
/// is the height of already-placed content at position x.  For each rectangle
/// (largest-area first), find the skyline position where the top of the placed
/// rectangle is lowest, place it, and raise the skyline.
fn skyline_pack(bboxes: &[(f32, f32)], padding: f32) -> Vec<(f32, f32)> {
    let n = bboxes.len();
    if n == 0 {
        return Vec::new();
    }

    // Padded sizes.
    let padded: Vec<(f32, f32)> = bboxes
        .iter()
        .map(|&(w, h)| (w + padding, h + padding))
        .collect();

    // Sort by area descending; place large components first.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        let area_a = padded[a].0 * padded[a].1;
        let area_b = padded[b].0 * padded[b].1;
        area_b.partial_cmp(&area_a).unwrap()
    });

    // Estimate bin width: aim for a roughly square total area.
    let total_area: f32 = padded.iter().map(|(w, h)| w * h).sum();
    let max_w = padded.iter().map(|(w, _)| *w).fold(0.0_f32, f32::max);
    let bin_width = total_area.sqrt().max(max_w);

    // Skyline segments: each (x_start, height).  The segment at index i
    // covers [segments[i].x, segments[i+1].x) at the given height.
    // The last segment implicitly extends to bin_width.
    let mut sky: Vec<(f32, f32)> = vec![(0.0, 0.0)];

    let mut origins = vec![(0.0_f32, 0.0_f32); n];

    for &idx in &order {
        let (rw, rh) = padded[idx];

        // Find the skyline position that minimises the top of the placed rect.
        let mut best_si = 0;
        let mut best_y = f32::INFINITY;

        'outer: for si in 0..sky.len() {
            let x0 = sky[si].0;
            if x0 + rw > bin_width + 0.001 {
                break;
            }
            // Max height across all segments the rect would overlap.
            let mut max_h = 0.0_f32;
            for sj in si..sky.len() {
                if sky[sj].0 >= x0 + rw - 0.001 {
                    break;
                }
                max_h = max_h.max(sky[sj].1);
                // If this is already worse than our best, skip.
                if max_h >= best_y {
                    continue 'outer;
                }
            }
            if max_h < best_y {
                best_y = max_h;
                best_si = si;
            }
        }

        let best_x = sky[best_si].0;
        origins[idx] = (best_x, best_y);

        // Update skyline: raise [best_x, best_x + rw) to best_y + rh.
        let new_top = best_y + rh;
        let rx_end = best_x + rw;

        // Collect the height of the segment that covers rx_end (we'll need
        // it if we're splitting a segment).
        let mut tail_h = sky.last().unwrap().1;
        for s in &sky {
            if s.0 <= rx_end + 0.001 {
                tail_h = s.1;
            }
        }

        // Build a new skyline:
        //   1. Keep segments entirely before best_x
        //   2. Insert (best_x, new_top)
        //   3. Insert (rx_end, tail_h) if rx_end < bin_width
        //   4. Keep segments starting at or after rx_end
        let mut new_sky: Vec<(f32, f32)> = Vec::with_capacity(sky.len() + 2);

        // Segments before the rect.
        for s in &sky {
            if s.0 < best_x - 0.001 {
                new_sky.push(*s);
            }
        }

        // The rect's raised segment.
        new_sky.push((best_x, new_top));

        // Segment after the rect (restore previous height).
        if rx_end < bin_width - 0.001 {
            new_sky.push((rx_end, tail_h));
        }

        // Segments after the rect's range.
        for s in &sky {
            if s.0 >= rx_end + 0.001 {
                new_sky.push(*s);
            }
        }

        // Merge adjacent segments with the same height.
        sky.clear();
        for seg in &new_sky {
            if let Some(last) = sky.last() {
                if (last.1 - seg.1).abs() < 0.001 {
                    continue;
                }
            }
            sky.push(*seg);
        }
    }

    origins
}

/// Lay out a single connected component using Fruchterman-Reingold.
fn fr_single_component(
    n: usize,
    edges: &[[usize; 2]],
    iterations: u32,
) -> Vec<(f32, f32)> {
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![(0.0, 0.0)];
    }

    let mut pos = vec![(0.0_f32, 0.0_f32); n];
    let mut rng = fastrand::Rng::new();
    for p in &mut pos {
        *p = (rng.f32() - 0.5, rng.f32() - 0.5);
    }

    // Ideal spring length: area = n, side = sqrt(n), so k = sqrt(area/n) = 1.
    // Using k proportional to 1/sqrt(n) keeps nodes from piling up in big graphs.
    let k = (1.0_f32 / n as f32).sqrt();
    let k2 = k * k;
    let clip = k * 0.01;

    // Cooling: start hot (large displacement allowed), decay exponentially.
    // Initial temperature proportional to layout area so nodes can traverse it.
    let t0 = k * (n as f32).sqrt(); // ~ side length of ideal layout
    let t_min = k * 0.01;

    // Gravity: weak pull toward centroid to prevent drift.
    // Stronger for larger graphs where drift is more pronounced.
    let gravity = 0.05 * k;

    for iteration in 0..iterations {
        let mut disp = vec![(0.0_f32, 0.0_f32); n];

        // Repulsive forces (all pairs)
        for i in 0..n {
            for j in (i + 1)..n {
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist = (dx * dx + dy * dy).sqrt().max(clip);
                let force = k2 / dist;
                let fx = (dx / dist) * force;
                let fy = (dy / dist) * force;
                disp[i].0 += fx;
                disp[i].1 += fy;
                disp[j].0 -= fx;
                disp[j].1 -= fy;
            }
        }

        // Attractive forces (edges)
        for edge in edges {
            let (s, t) = (edge[0], edge[1]);
            let dx = pos[s].0 - pos[t].0;
            let dy = pos[s].1 - pos[t].1;
            let dist = (dx * dx + dy * dy).sqrt().max(clip);
            let force = dist * dist / k;
            let fx = (dx / dist) * force;
            let fy = (dy / dist) * force;
            disp[s].0 -= fx;
            disp[s].1 -= fy;
            disp[t].0 += fx;
            disp[t].1 += fy;
        }

        // Gravity: pull toward centroid
        let (mut cx, mut cy) = (0.0_f32, 0.0_f32);
        for p in &pos {
            cx += p.0;
            cy += p.1;
        }
        cx /= n as f32;
        cy /= n as f32;
        for i in 0..n {
            disp[i].0 -= (pos[i].0 - cx) * gravity;
            disp[i].1 -= (pos[i].1 - cy) * gravity;
        }

        // Apply displacement with temperature-limited step size.
        // Exponential cooling: t = t0 * decay^iteration
        let progress = iteration as f32 / iterations as f32;
        let temp = (t0 * (t_min / t0).powf(progress)).max(t_min);

        for i in 0..n {
            let len = (disp[i].0 * disp[i].0 + disp[i].1 * disp[i].1).sqrt();
            if len > 0.0 {
                let limited = len.min(temp);
                pos[i].0 += (disp[i].0 / len) * limited;
                pos[i].1 += (disp[i].1 / len) * limited;
            }
        }
    }

    pos
}

/// Find connected components via union-find.  Returns a vec of components,
/// each being a sorted vec of global node indices.
fn connected_components(n: usize, edges: &[[usize; 2]]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]]; // path halving
            x = parent[x];
        }
        x
    }

    for e in edges {
        let a = find(&mut parent, e[0]);
        let b = find(&mut parent, e[1]);
        if a != b {
            parent[a] = b;
        }
    }

    let mut groups: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().push(i);
    }

    // Sort components by size descending (largest first).
    let mut comps: Vec<Vec<usize>> = groups.into_values().collect();
    comps.sort_by(|a, b| b.len().cmp(&a.len()));
    comps
}

/// Build the final NodePosition vec from per-component position arrays,
/// mapping local indices back to global ones.
fn assemble_positions(node_ids: &[String], component_positions: &[Vec<(f32, f32)>]) -> Vec<NodePosition> {
    // Single component: indices are 1:1.
    let positions = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodePosition {
            id: id.clone(),
            x: component_positions[0][i].0,
            y: component_positions[0][i].1,
        })
        .collect();
    normalize(positions)
}

// ============================================================================
// Hierarchical (center-of-gravity)
// ============================================================================

/// Sugiyama hierarchical layout with Brandes-Kopf coordinate assignment.
///
/// Each connected component is laid out independently with its own Sugiyama
/// pass, then components are packed together using skyline bin-packing.
/// This prevents unrelated components from introducing edge crossings and
/// eliminates dead space from shared layering.
fn hierarchical(
    node_ids: &[String],
    edges: &[[usize; 2]],
    direction: LayoutDirection,
    node_labels: Option<&[String]>,
) -> Vec<NodePosition> {
    let n = node_ids.len();
    if n == 0 {
        return Vec::new();
    }

    let components = connected_components(n, edges);
    let config = crate::sugiyama::SugiyamaConfig::default();

    // Lay out each component independently.
    let mut component_coords: Vec<Vec<(f32, f32)>> = Vec::with_capacity(components.len());
    for comp in &components {
        let cn = comp.len();
        let global_to_local: std::collections::HashMap<usize, usize> =
            comp.iter().enumerate().map(|(li, &gi)| (gi, li)).collect();
        let local_edges: Vec<[usize; 2]> = edges
            .iter()
            .filter_map(|e| {
                let a = global_to_local.get(&e[0])?;
                let b = global_to_local.get(&e[1])?;
                Some([*a, *b])
            })
            .collect();

        // Build local sort keys.
        let local_sort_keys: Option<Vec<String>> = node_labels.map(|labels| {
            comp.iter()
                .map(|&gi| {
                    let name = &node_ids[gi];
                    let label = labels.get(gi).map(|s| s.as_str()).unwrap_or("");
                    format!("{label}\t{name}")
                })
                .collect()
        });

        let coords =
            crate::sugiyama::layout(cn, &local_edges, &config, local_sort_keys.as_deref());
        component_coords.push(coords);
    }

    // If only one component, skip packing.
    if components.len() == 1 {
        let positions = apply_direction(node_ids, &component_coords[0], direction);
        return normalize(positions);
    }

    // Stack components along Sugiyama's x-axis (the across/non-flow axis).
    // After apply_direction this becomes the non-flow axis in final output:
    //   T->B / B->T: final x (horizontal stacking)
    //   L->R / R->L: final y (vertical stacking)
    let padding = config.node_sep;
    let mut all_coords = vec![(0.0_f32, 0.0_f32); n];
    let mut x_offset = 0.0_f32;

    for (ci, comp) in components.iter().enumerate() {
        let coords = &component_coords[ci];
        let (mut min_x, mut min_y) = (f32::INFINITY, f32::INFINITY);
        let mut max_x = f32::NEG_INFINITY;
        for &(x, y) in coords {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
        }
        for (li, &gi) in comp.iter().enumerate() {
            all_coords[gi].0 = coords[li].0 - min_x + x_offset;
            all_coords[gi].1 = coords[li].1 - min_y;
        }
        x_offset += (max_x - min_x).max(0.0) + padding;
    }

    let positions = apply_direction(node_ids, &all_coords, direction);
    normalize(positions)
}

/// Apply direction transform to Sugiyama coordinates (top-to-bottom → desired).
fn apply_direction(
    node_ids: &[String],
    coords: &[(f32, f32)],
    direction: LayoutDirection,
) -> Vec<NodePosition> {
    node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let (sx, sy) = coords[i];
            let (x, y) = match direction {
                LayoutDirection::TopToBottom => (sx, sy),
                LayoutDirection::BottomToTop => (sx, -sy),
                LayoutDirection::LeftToRight => (sy, sx),
                LayoutDirection::RightToLeft => (-sy, sx),
            };
            NodePosition {
                id: id.clone(),
                x,
                y,
            }
        })
        .collect()
}

// ============================================================================
// Radial (hierarchical layers on concentric circles)
// ============================================================================

/// Radial layout: uses hierarchical layering, then arranges each layer on a
/// concentric circle.  The innermost layer (deepest in the DAG) goes to the
/// center; if it contains a single node, that node sits at the exact center.
///
/// Node ordering within each ring reuses the Sugiyama crossing minimization,
/// then angular positions are refined to minimize the angle between edges and
/// the radial direction (edges should point toward/away from the center).
fn radial(
    node_ids: &[String],
    edges: &[[usize; 2]],
    node_labels: Option<&[String]>,
) -> Vec<NodePosition> {
    let n = node_ids.len();
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![NodePosition {
            id: node_ids[0].clone(),
            x: 0.0,
            y: 0.0,
        }];
    }

    // Build sort keys for crossing minimization tiebreaking.
    let sort_keys: Option<Vec<String>> = node_labels.map(|labels| {
        node_ids
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let label = labels.get(i).map(|s| s.as_str()).unwrap_or("");
                format!("{label}\t{name}")
            })
            .collect()
    });

    // Get layers with crossing-minimized ordering.
    let ordered_layers =
        crate::sugiyama::get_ordered_layers(n, edges, sort_keys.as_deref());

    if ordered_layers.is_empty() {
        return node_ids
            .iter()
            .map(|id| NodePosition {
                id: id.clone(),
                x: 0.0,
                y: 0.0,
            })
            .collect();
    }

    // Reverse layers so the deepest layer (sinks) is innermost (index 0).
    let layers: Vec<&Vec<usize>> = ordered_layers.iter().rev().collect();
    let n_layers = layers.len();

    // Assign angular positions per ring.
    let mut angle_of = vec![0.0_f32; n];
    let mut radius_of = vec![0.0_f32; n];

    // Radius spacing: innermost ring has radius proportional to its node count
    // to avoid crowding.  Each subsequent ring is spaced to give nodes roughly
    // equal angular separation.
    let ring_sep = 2.5_f32; // base separation between rings

    for (li, layer) in layers.iter().enumerate() {
        let count = layer.len();

        let radius = if li == 0 && count == 1 {
            // Single center node: radius = 0
            0.0
        } else if li == 0 {
            // Innermost ring with multiple nodes
            ring_sep
        } else {
            // Outer rings: space proportionally
            ring_sep * (li as f32 + 1.0)
        };

        if count == 1 {
            angle_of[layer[0]] = 0.0;
            radius_of[layer[0]] = radius;
        } else {
            // Distribute evenly around the circle in the crossing-minimized order.
            let step = std::f32::consts::TAU / count as f32;
            for (pos, &node) in layer.iter().enumerate() {
                angle_of[node] = step * pos as f32;
                radius_of[node] = radius;
            }
        }
    }

    // Refinement: adjust angular positions to minimize deviation from radial
    // edge alignment.  For each node on ring > 0, compute the ideal angle as
    // the circular mean of its connected neighbors on inner rings, then blend
    // with the current position.
    //
    // Build adjacency for quick lookup.
    let mut neighbors: Vec<Vec<usize>> = vec![vec![]; n];
    for e in edges {
        if e[0] < n && e[1] < n {
            neighbors[e[0]].push(e[1]);
            neighbors[e[1]].push(e[0]);
        }
    }

    // Layer index for each node (0 = innermost after reversal).
    let mut layer_idx = vec![0usize; n];
    for (li, layer) in layers.iter().enumerate() {
        for &node in *layer {
            layer_idx[node] = li;
        }
    }

    // Several passes of radial refinement.
    for _pass in 0..10 {
        for li in 0..n_layers {
            let layer = &layers[li];
            if layer.len() <= 1 {
                continue;
            }

            // For each node, compute ideal angle from inner neighbors.
            let mut ideal_angles: Vec<Option<f32>> = Vec::with_capacity(layer.len());
            for &node in layer.iter() {
                let inner_nbrs: Vec<usize> = neighbors[node]
                    .iter()
                    .filter(|&&nb| layer_idx[nb] < li)
                    .copied()
                    .collect();

                if inner_nbrs.is_empty() {
                    ideal_angles.push(None);
                    continue;
                }

                // Circular mean of neighbor angles.
                let (mut sin_sum, mut cos_sum) = (0.0_f32, 0.0_f32);
                for &nb in &inner_nbrs {
                    sin_sum += angle_of[nb].sin();
                    cos_sum += angle_of[nb].cos();
                }
                let ideal = sin_sum.atan2(cos_sum);
                ideal_angles.push(Some(ideal));
            }

            // Sort nodes by their ideal angle (nodes without inner neighbors
            // keep their relative position among the others).
            let mut indexed: Vec<(usize, f32)> = layer
                .iter()
                .enumerate()
                .map(|(pos, &node)| {
                    let sort_angle = ideal_angles[pos].unwrap_or(angle_of[node]);
                    (node, sort_angle)
                })
                .collect();

            indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            // Re-distribute evenly in the sorted order.
            let count = indexed.len();
            let step = std::f32::consts::TAU / count as f32;

            // Anchor: the first node's ideal angle sets the rotation of the ring
            // so the ring as a whole faces toward the inner neighbors.
            let base_angle = indexed[0].1;
            for (pos, (node, _)) in indexed.iter().enumerate() {
                angle_of[*node] = base_angle + step * pos as f32;
            }
        }
    }

    // Convert polar to cartesian.
    let positions: Vec<NodePosition> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let r = radius_of[i];
            let a = angle_of[i];
            NodePosition {
                id: id.clone(),
                x: r * a.cos(),
                y: r * a.sin(),
            }
        })
        .collect();

    normalize(positions)
}

// ============================================================================
// Circular
// ============================================================================

fn circular(node_ids: &[String]) -> Vec<NodePosition> {
    let n = node_ids.len();
    node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let angle = 2.0 * std::f32::consts::PI * i as f32 / n.max(1) as f32;
            NodePosition {
                id: id.clone(),
                x: angle.cos() * TARGET_SIZE,
                y: angle.sin() * TARGET_SIZE,
            }
        })
        .collect()
}

// ============================================================================
// Grid
// ============================================================================

fn grid(node_ids: &[String]) -> Vec<NodePosition> {
    let n = node_ids.len();
    let cols = (n as f32).sqrt().ceil() as usize;
    let rows = n.div_ceil(cols);
    let sx = if cols > 1 {
        (TARGET_SIZE * 2.0) / (cols - 1) as f32
    } else {
        0.0
    };
    let sy = if rows > 1 {
        (TARGET_SIZE * 2.0) / (rows - 1) as f32
    } else {
        0.0
    };

    node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodePosition {
            id: id.clone(),
            x: -TARGET_SIZE + (i % cols) as f32 * sx,
            y: -TARGET_SIZE + (i / cols) as f32 * sy,
        })
        .collect()
}

// ============================================================================
// Lattice (tilted grid)
// ============================================================================

fn lattice(node_ids: &[String]) -> Vec<NodePosition> {
    let n = node_ids.len();
    let cols = (n as f32).sqrt().ceil() as usize;
    let spacing = 180.0_f32;
    let angle: f32 = 0.5_f32.atan(); // 26.57°
    let (cos, sin) = (angle.cos(), angle.sin());

    let rows = n.div_ceil(cols);
    let start_x = -((cols.saturating_sub(1)) as f32 * spacing) / 2.0;
    let start_y = -((rows.saturating_sub(1)) as f32 * spacing) / 2.0;

    let mut positions: Vec<NodePosition> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let gx = start_x + (i % cols) as f32 * spacing;
            let gy = start_y + (i / cols) as f32 * spacing;
            NodePosition {
                id: id.clone(),
                x: gx * cos - gy * sin,
                y: gx * sin + gy * cos,
            }
        })
        .collect();

    // Scale to fit
    let max_abs = positions
        .iter()
        .flat_map(|p| [p.x.abs(), p.y.abs()])
        .fold(0.0_f32, f32::max);
    if max_abs > 0.0 {
        let scale = TARGET_SIZE / max_abs;
        for p in &mut positions {
            p.x *= scale;
            p.y *= scale;
        }
    }
    positions
}

// ============================================================================
// Shared normalization
// ============================================================================

/// Center at origin, scale each axis independently to fill [-TARGET_SIZE, TARGET_SIZE].
fn normalize(mut positions: Vec<NodePosition>) -> Vec<NodePosition> {
    if positions.len() <= 1 {
        for p in &mut positions {
            p.x = 0.0;
            p.y = 0.0;
        }
        return positions;
    }

    let (mut min_x, mut max_x) = (f32::INFINITY, f32::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f32::INFINITY, f32::NEG_INFINITY);
    for p in &positions {
        min_x = min_x.min(p.x);
        max_x = max_x.max(p.x);
        min_y = min_y.min(p.y);
        max_y = max_y.max(p.y);
    }

    let range_x = (max_x - min_x).max(f32::EPSILON);
    let range_y = (max_y - min_y).max(f32::EPSILON);
    let cx = (min_x + max_x) / 2.0;
    let cy = (min_y + max_y) / 2.0;
    let sx = (TARGET_SIZE * 2.0) / range_x;
    let sy = (TARGET_SIZE * 2.0) / range_y;

    for p in &mut positions {
        p.x = (p.x - cx) * sx;
        p.y = (p.y - cy) * sy;
    }
    positions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(n: usize, edges: &[[usize; 2]]) -> LayoutRequest {
        LayoutRequest {
            nodes: (0..n).map(|i| format!("node_{i}")).collect(),
            edges: edges.to_vec(),
            algorithm: LayoutAlgorithm::Hierarchical,
            direction: Some(LayoutDirection::LeftToRight),
            iterations: None,
            node_labels: None,
            temperature: None,
        }
    }

    #[test]
    fn full_pipeline_bipartite() {
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
        let req = make_request(13, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 13);

        // All positions finite and within TARGET_SIZE bounds
        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "node {i} ({}) non-finite: ({}, {})",
                p.id,
                p.x,
                p.y,
            );
            assert!(
                p.x.abs() <= TARGET_SIZE + 1.0 && p.y.abs() <= TARGET_SIZE + 1.0,
                "node {i} out of bounds: ({}, {})",
                p.x,
                p.y,
            );
        }

        // No two nodes should overlap: at least 1 pixel apart
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "nodes {i} and {j} overlap (dist={dist:.2}): ({:.1},{:.1}) vs ({:.1},{:.1})",
                    positions[i].x,
                    positions[i].y,
                    positions[j].x,
                    positions[j].y,
                );
            }
        }
    }

    #[test]
    fn full_pipeline_large_graph() {
        // 20 sources → 8 targets, dense connections
        let edges: &[[usize; 2]] = &[
            [0, 20],
            [0, 21],
            [1, 21],
            [1, 22],
            [2, 20],
            [2, 23],
            [3, 22],
            [3, 24],
            [4, 20],
            [4, 25],
            [5, 21],
            [5, 23],
            [6, 22],
            [6, 26],
            [7, 24],
            [7, 27],
            [8, 20],
            [8, 21],
            [9, 23],
            [9, 25],
            [10, 26],
            [10, 27],
            [11, 20],
            [11, 24],
            [12, 21],
            [12, 22],
            [13, 25],
            [13, 26],
            [14, 27],
            [14, 20],
            [15, 23],
            [15, 24],
            [16, 21],
            [16, 25],
            [17, 22],
            [17, 27],
            [18, 20],
            [18, 26],
            [19, 23],
            [19, 24],
        ];
        let req = make_request(28, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 28);

        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "node {i} non-finite: ({}, {})",
                p.x,
                p.y,
            );
        }

        // No overlaps (use Euclidean distance > 10 after normalization to ±800)
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "nodes {i} and {j} too close (dist={dist:.1}): ({:.1},{:.1}) vs ({:.1},{:.1})",
                    positions[i].x,
                    positions[i].y,
                    positions[j].x,
                    positions[j].y,
                );
            }
        }
    }

    /// Graph with many isolated nodes (no edges) alongside connected nodes.
    #[test]
    fn full_pipeline_isolated_nodes() {
        // Nodes 0-4 are connected, nodes 5-14 are isolated
        let edges: &[[usize; 2]] = &[[0, 1], [1, 2], [2, 3], [3, 4]];
        let req = make_request(15, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 15);
        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "node {i} non-finite: ({}, {})",
                p.x,
                p.y,
            );
        }

        // No overlaps
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "nodes {i} and {j} too close (dist={dist:.1}): ({:.1},{:.1}) vs ({:.1},{:.1})",
                    positions[i].x,
                    positions[i].y,
                    positions[j].x,
                    positions[j].y,
                );
            }
        }
    }

    /// All nodes isolated (no edges at all).
    #[test]
    fn full_pipeline_no_edges() {
        let req = make_request(10, &[]);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 10);
        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "node {i} non-finite: ({}, {})",
                p.x,
                p.y,
            );
        }

        // No overlaps
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "nodes {i} and {j} too close (dist={dist:.1}): ({:.1},{:.1}) vs ({:.1},{:.1})",
                    positions[i].x,
                    positions[i].y,
                    positions[j].x,
                    positions[j].y,
                );
            }
        }
    }

    /// The positions should span a significant range (not all bunched together).
    #[test]
    fn full_pipeline_spread() {
        let edges: &[[usize; 2]] = &[[0, 5], [1, 5], [2, 6], [3, 6], [4, 7], [5, 7], [6, 7]];
        let req = make_request(8, edges);
        let positions = compute_layout(&req);

        let xs: Vec<f32> = positions.iter().map(|p| p.x).collect();
        let ys: Vec<f32> = positions.iter().map(|p| p.y).collect();
        let x_range = xs.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - xs.iter().cloned().fold(f32::INFINITY, f32::min);
        let y_range = ys.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - ys.iter().cloned().fold(f32::INFINITY, f32::min);

        // After normalize, ranges should be close to 2*TARGET_SIZE (1600)
        assert!(
            x_range > 100.0,
            "x range too small: {x_range} (positions: {xs:?})"
        );
        assert!(
            y_range > 100.0,
            "y range too small: {y_range} (positions: {ys:?})"
        );
    }

    /// Real graph from hierarchical.json with actual node names and types.
    /// Verifies barycenters are exact and graph isn't squashed.
    #[test]
    fn hierarchical_json_barycenters() {
        // Node names in hierarchical.json order
        let node_names: Vec<String> = vec![
            "WEBSERVERSCCM@CONTOSO.INT",
            "SUB-CA-CONTOSO-INT@CONTOSO.INT",
            "CONTOSOSIGNATUREDOCUMENTSFORSAP@CONTOSO.INT",
            "WEBSERVER1YEAR2048@CONTOSO.INT",
            "WEBSERVER3YEARS@CONTOSO.INT",
            "WEBSERVERS4B2@CONTOSO.INT",
            "WEBSERVER1YEARARV@CONTOSO.INT",
            "WEBSERVER1YEAR@CONTOSO.INT",
            "WEBSERVER1YEARSCSP@CONTOSO.INT",
            "WEBSERVER2YEARSARV@CONTOSO.INT",
            "WEBSERVER3YEARSARV@CONTOSO.INT",
            "WEBSERVEREXPORT@CONTOSO.INT",
            "SUBCA5YEARS@CONTOSO.INT",
            "ROOT-CA-CONTOSO.INT@CONTOSO.INT",
            "SUBORDINATECERTIFICATIONAUTHORITY15YEARS@CONTOSO.INT",
            "SCCMWEBSERVERCERTIFICATE@CONTOSO.INT",
            "SUB-CA-VPN-CONTOSO-INT@CONTOSO.INT",
            "USERSAP@CONTOSO.INT",
            "SCCMCLIENTCERTIFICATEMANUAL@CONTOSO.INT",
            "KEYRECOVERYAGENTCERTEP@CONTOSO.INT",
            "CERTEP CONTOSO SERVICE CA@CONTOSO.INT",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let node_labels: Vec<String> = vec![
            "CertTemplate", "EnterpriseCA", "CertTemplate", "CertTemplate",
            "CertTemplate", "CertTemplate", "CertTemplate", "CertTemplate",
            "CertTemplate", "CertTemplate", "CertTemplate", "CertTemplate",
            "CertTemplate", "EnterpriseCA", "CertTemplate", "CertTemplate",
            "EnterpriseCA", "CertTemplate", "CertTemplate", "CertTemplate",
            "EnterpriseCA",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let edges: Vec<[usize; 2]> = vec![
            [19, 20], [2, 1], [9, 1], [12, 13], [0, 1], [5, 1],
            [10, 1], [6, 1], [4, 1], [15, 16], [7, 1], [14, 13],
            [17, 16], [3, 1], [11, 1], [8, 1], [18, 16],
        ];
        let req = LayoutRequest {
            nodes: node_names,
            edges,
            algorithm: LayoutAlgorithm::Hierarchical,
            direction: Some(LayoutDirection::LeftToRight),
            iterations: None,
            node_labels: Some(node_labels),
            temperature: None,
        };
        let pos = compute_layout(&req);

        let targets = [1usize, 13, 16, 20];
        let names = ["SUB-CA", "ROOT-CA", "SUB-CA-VPN", "CERTEP"];
        eprintln!("\n=== Screen positions (LTR, with labels) ===");
        for (i, p) in pos.iter().enumerate() {
            let label = targets.iter().position(|&t| t == i)
                .map(|j| names[j]).unwrap_or("src");
            eprintln!("  node {i:2} ({label:>10}): x={:7.1}, y={:7.1}", p.x, p.y);
        }

        // Targets at exact barycenters of their parents' screen Y.
        let parent_map: [(usize, &[usize]); 4] = [
            (1, &[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
            (13, &[12, 14]),
            (16, &[15, 17, 18]),
            (20, &[19]),
        ];
        eprintln!("\n=== Barycenter check ===");
        for (target, parents) in &parent_map {
            let avg = parents.iter().map(|&p| pos[p].y).sum::<f32>() / parents.len() as f32;
            let diff = (pos[*target].y - avg).abs();
            let name = names[targets.iter().position(|&t| t == *target).unwrap()];
            eprintln!("  {name:>10}: y={:.1}, parent_avg={:.1}, diff={diff:.1}",
                pos[*target].y, avg);
        }

        // Not squashed.
        let x_range = pos.iter().map(|p| p.x).fold(f32::NEG_INFINITY, f32::max)
            - pos.iter().map(|p| p.x).fold(f32::INFINITY, f32::min);
        let y_range = pos.iter().map(|p| p.y).fold(f32::NEG_INFINITY, f32::max)
            - pos.iter().map(|p| p.y).fold(f32::INFINITY, f32::min);
        assert!(x_range > TARGET_SIZE, "squashed horizontally: {x_range:.0}");
        assert!(y_range > TARGET_SIZE, "squashed vertically: {y_range:.0}");
    }

    // ── Force-directed specific tests ────────────────────────────────────

    fn make_fd_request(n: usize, edges: &[[usize; 2]]) -> LayoutRequest {
        LayoutRequest {
            nodes: (0..n).map(|i| format!("node_{i}")).collect(),
            edges: edges.to_vec(),
            algorithm: LayoutAlgorithm::ForceDirected,
            direction: None,
            iterations: None,
            node_labels: None,
            temperature: None,
        }
    }

    /// Helper: assert all positions finite, in bounds, and no overlaps.
    fn assert_valid_layout(positions: &[NodePosition], label: &str) {
        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "{label}: node {i} non-finite: ({}, {})",
                p.x,
                p.y,
            );
            assert!(
                p.x.abs() <= TARGET_SIZE + 1.0 && p.y.abs() <= TARGET_SIZE + 1.0,
                "{label}: node {i} out of bounds: ({}, {})",
                p.x,
                p.y,
            );
        }
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "{label}: nodes {i} and {j} too close (dist={dist:.1})",
                );
            }
        }
    }

    #[test]
    fn fd_star_graph() {
        // Hub + 8 spokes
        let edges: &[[usize; 2]] = &[
            [0, 1], [0, 2], [0, 3], [0, 4],
            [0, 5], [0, 6], [0, 7], [0, 8],
        ];
        let req = make_fd_request(9, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 9);
        assert_valid_layout(&positions, "fd_star");
    }

    #[test]
    fn fd_disconnected_components() {
        // Two triangles with no connection between them
        let edges: &[[usize; 2]] = &[
            [0, 1], [1, 2], [2, 0], // component A
            [3, 4], [4, 5], [5, 3], // component B
        ];
        let req = make_fd_request(6, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 6);
        assert_valid_layout(&positions, "fd_disconnected");
    }

    #[test]
    fn fd_no_edges() {
        let req = make_fd_request(10, &[]);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 10);
        assert_valid_layout(&positions, "fd_no_edges");
    }

    #[test]
    fn fd_single_node() {
        let req = make_fd_request(1, &[]);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 1);
        assert!(positions[0].x.is_finite() && positions[0].y.is_finite());
    }

    #[test]
    fn skyline_pack_basic() {
        // Three rectangles: one big, two small.
        let bboxes = vec![(4.0, 3.0), (1.0, 1.0), (1.0, 1.0)];
        let origins = skyline_pack(&bboxes, 0.0);

        // All origins should be finite and non-negative.
        for (i, &(x, y)) in origins.iter().enumerate() {
            assert!(x >= 0.0 && y >= 0.0, "rect {i} has negative origin: ({x}, {y})");
            assert!(x.is_finite() && y.is_finite(), "rect {i} non-finite: ({x}, {y})");
        }

        // No two rectangles should overlap.
        for i in 0..bboxes.len() {
            for j in (i + 1)..bboxes.len() {
                let (x1, y1) = origins[i];
                let (w1, h1) = bboxes[i];
                let (x2, y2) = origins[j];
                let (w2, h2) = bboxes[j];
                let overlap_x = x1 < x2 + w2 && x2 < x1 + w1;
                let overlap_y = y1 < y2 + h2 && y2 < y1 + h1;
                assert!(
                    !(overlap_x && overlap_y),
                    "rects {i} and {j} overlap: ({x1},{y1},{w1},{h1}) vs ({x2},{y2},{w2},{h2})",
                );
            }
        }
    }

    #[test]
    fn skyline_pack_many_equal() {
        // 9 equal squares should pack into roughly a 3x3 grid.
        let bboxes = vec![(1.0, 1.0); 9];
        let origins = skyline_pack(&bboxes, 0.0);

        let max_x = origins.iter().map(|o| o.0 + 1.0).fold(0.0_f32, f32::max);
        let max_y = origins.iter().map(|o| o.1 + 1.0).fold(0.0_f32, f32::max);

        // Should be roughly square, not a long strip.
        let aspect = max_x.max(max_y) / max_x.min(max_y);
        assert!(
            aspect < 2.0,
            "9 equal squares packed with bad aspect ratio: {max_x} x {max_y} (ratio {aspect:.1})"
        );
    }

    #[test]
    fn fd_spread() {
        let edges: &[[usize; 2]] = &[[0, 5], [1, 5], [2, 6], [3, 6], [4, 7], [5, 7], [6, 7]];
        let req = make_fd_request(8, edges);
        let positions = compute_layout(&req);

        let xs: Vec<f32> = positions.iter().map(|p| p.x).collect();
        let ys: Vec<f32> = positions.iter().map(|p| p.y).collect();
        let x_range = xs.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - xs.iter().cloned().fold(f32::INFINITY, f32::min);
        let y_range = ys.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
            - ys.iter().cloned().fold(f32::INFINITY, f32::min);

        assert!(
            x_range > 100.0,
            "fd x range too small: {x_range} (positions: {xs:?})"
        );
        assert!(
            y_range > 100.0,
            "fd y range too small: {y_range} (positions: {ys:?})"
        );
    }

    // ── Radial layout tests ──────────────────────────────────────────────

    fn make_radial_request(n: usize, edges: &[[usize; 2]]) -> LayoutRequest {
        LayoutRequest {
            nodes: (0..n).map(|i| format!("node_{i}")).collect(),
            edges: edges.to_vec(),
            algorithm: LayoutAlgorithm::Radial,
            direction: None,
            iterations: None,
            node_labels: None,
            temperature: None,
        }
    }

    #[test]
    fn radial_star() {
        let edges: &[[usize; 2]] = &[
            [0, 1], [0, 2], [0, 3], [0, 4],
            [0, 5], [0, 6], [0, 7], [0, 8],
        ];
        let req = make_radial_request(9, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 9);
        assert_valid_layout(&positions, "radial_star");
    }

    #[test]
    fn radial_three_layer_dag() {
        // Sources -> middle -> sinks
        let edges: &[[usize; 2]] = &[
            [0, 3], [0, 4], [1, 4], [1, 5],
            [2, 3], [2, 5], [3, 6], [4, 6],
            [4, 7], [5, 7],
        ];
        let req = make_radial_request(8, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 8);
        assert_valid_layout(&positions, "radial_3layer");

        // Sinks (6, 7) should be closer to center than sources (0, 1, 2).
        let sink_dist: f32 = [6, 7]
            .iter()
            .map(|&i| (positions[i].x.powi(2) + positions[i].y.powi(2)).sqrt())
            .sum::<f32>()
            / 2.0;
        let source_dist: f32 = [0, 1, 2]
            .iter()
            .map(|&i| (positions[i].x.powi(2) + positions[i].y.powi(2)).sqrt())
            .sum::<f32>()
            / 3.0;
        assert!(
            sink_dist < source_dist,
            "sinks (avg dist {sink_dist:.0}) should be closer to center than sources ({source_dist:.0})"
        );
    }

    #[test]
    fn radial_single_sink_at_center() {
        // Diamond: two sources -> one middle -> one sink
        let edges: &[[usize; 2]] = &[[0, 2], [1, 2], [2, 3]];
        let req = make_radial_request(4, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 4);

        // The sink (node 3) is the only node in its layer, so it should
        // be at/near the center (smallest distance from origin).
        let dists: Vec<f32> = positions
            .iter()
            .map(|p| (p.x.powi(2) + p.y.powi(2)).sqrt())
            .collect();
        let sink_dist = dists[3];
        let min_other = dists[..3].iter().cloned().fold(f32::INFINITY, f32::min);
        assert!(
            sink_dist < min_other,
            "sink should be closest to center: sink={sink_dist:.0}, min_other={min_other:.0}"
        );
    }

    #[test]
    fn radial_no_edges() {
        let req = make_radial_request(10, &[]);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 10);
        assert_valid_layout(&positions, "radial_no_edges");
    }

    #[test]
    fn radial_single_node() {
        let req = make_radial_request(1, &[]);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 1);
        assert!(positions[0].x.is_finite() && positions[0].y.is_finite());
    }

    #[test]
    fn hier_component_stacking_direction() {
        // Two disconnected chains: 0->1->2 and 3->4->5
        let edges: &[[usize; 2]] = &[[0, 1], [1, 2], [3, 4], [4, 5]];

        // L->R: components should stack VERTICALLY (differ in y, similar x range)
        let req_lr = LayoutRequest {
            nodes: (0..6).map(|i| format!("n{i}")).collect(),
            edges: edges.to_vec(),
            algorithm: LayoutAlgorithm::Hierarchical,
            direction: Some(LayoutDirection::LeftToRight),
            iterations: None,
            node_labels: None,
            temperature: None,
        };
        let pos_lr = compute_layout(&req_lr);

        let a_cy_lr = (pos_lr[0].y + pos_lr[1].y + pos_lr[2].y) / 3.0;
        let b_cy_lr = (pos_lr[3].y + pos_lr[4].y + pos_lr[5].y) / 3.0;
        let a_cx_lr = (pos_lr[0].x + pos_lr[1].x + pos_lr[2].x) / 3.0;
        let b_cx_lr = (pos_lr[3].x + pos_lr[4].x + pos_lr[5].x) / 3.0;

        let y_sep_lr = (a_cy_lr - b_cy_lr).abs();
        let x_sep_lr = (a_cx_lr - b_cx_lr).abs();

        eprintln!("L->R: A=({:.0},{:.0}), B=({:.0},{:.0}), x_sep={:.0}, y_sep={:.0}",
            a_cx_lr, a_cy_lr, b_cx_lr, b_cy_lr, x_sep_lr, y_sep_lr);

        assert!(
            y_sep_lr > x_sep_lr,
            "L->R: components should stack vertically: y_sep={y_sep_lr:.0} > x_sep={x_sep_lr:.0}"
        );

        // T->B: components should stack HORIZONTALLY (differ in x, similar y range)
        let req_tb = LayoutRequest {
            nodes: (0..6).map(|i| format!("n{i}")).collect(),
            edges: edges.to_vec(),
            algorithm: LayoutAlgorithm::Hierarchical,
            direction: Some(LayoutDirection::TopToBottom),
            iterations: None,
            node_labels: None,
            temperature: None,
        };
        let pos_tb = compute_layout(&req_tb);

        let a_cy_tb = (pos_tb[0].y + pos_tb[1].y + pos_tb[2].y) / 3.0;
        let b_cy_tb = (pos_tb[3].y + pos_tb[4].y + pos_tb[5].y) / 3.0;
        let a_cx_tb = (pos_tb[0].x + pos_tb[1].x + pos_tb[2].x) / 3.0;
        let b_cx_tb = (pos_tb[3].x + pos_tb[4].x + pos_tb[5].x) / 3.0;

        let y_sep_tb = (a_cy_tb - b_cy_tb).abs();
        let x_sep_tb = (a_cx_tb - b_cx_tb).abs();

        eprintln!("T->B: A=({:.0},{:.0}), B=({:.0},{:.0}), x_sep={:.0}, y_sep={:.0}",
            a_cx_tb, a_cy_tb, b_cx_tb, b_cy_tb, x_sep_tb, y_sep_tb);

        assert!(
            x_sep_tb > y_sep_tb,
            "T->B: components should stack horizontally: x_sep={x_sep_tb:.0} > y_sep={y_sep_tb:.0}"
        );
    }
}
