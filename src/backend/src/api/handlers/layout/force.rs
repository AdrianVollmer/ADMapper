//! Force-directed (Fruchterman-Reingold) layout.
//!
//! Originally based on visgraph by Raoul Luque (MIT OR Apache-2.0),
//! substantially reworked:
//!   - proper cooling schedule (exponential decay from a large initial temp)
//!   - weak gravity toward the centroid to prevent cluster drift
//!   - component-aware: disconnected components are laid out independently
//!     and then packed using skyline bin-packing

use super::{normalize, NodePosition};

/// Fruchterman-Reingold force-directed layout.
pub(crate) fn force_directed(
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
    let mut packed: Vec<Vec<(f32, f32)>> = Vec::with_capacity(components.len());
    for (ci, cpos) in component_positions.iter().enumerate() {
        let (ox, oy) = origins[ci];
        let (mut min_x, mut min_y) = (f32::INFINITY, f32::INFINITY);
        for &(cx, cy) in cpos {
            min_x = min_x.min(cx);
            min_y = min_y.min(cy);
        }
        let shifted: Vec<(f32, f32)> = cpos
            .iter()
            .map(|&(cx, cy)| (cx - min_x + ox, cy - min_y + oy))
            .collect();
        packed.push(shifted);
    }

    // Build global position array mapping back from local indices.
    let mut global_pos = vec![(0.0_f32, 0.0_f32); n];
    for (ci, comp) in components.iter().enumerate() {
        for (li, &gi) in comp.iter().enumerate() {
            global_pos[gi] = packed[ci][li];
        }
    }

    let positions: Vec<NodePosition> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodePosition {
            id: id.clone(),
            x: global_pos[i].0,
            y: global_pos[i].1,
        })
        .collect();
    normalize(positions)
}

// ── Skyline bin-packing ─────────────────────────────────────────────────────

/// Pack axis-aligned rectangles into a roughly-square area.
///
/// Returns origin `(x, y)` for each rectangle (top-left corner).
///
/// Uses the Skyline Bottom-Left (SBL) algorithm: a 1D height-map tracks the
/// lowest available y for each x-column. Each new rectangle is placed at the
/// position along the skyline that results in the smallest wasted area.
///
/// The total width is clamped to `sqrt(total_area) * 1.3` so the packing
/// tends toward a square aspect ratio rather than a long strip.
pub(crate) fn skyline_pack(bboxes: &[(f32, f32)], padding: f32) -> Vec<(f32, f32)> {
    if bboxes.is_empty() {
        return Vec::new();
    }
    if bboxes.len() == 1 {
        return vec![(0.0, 0.0)];
    }

    // Padded dimensions.
    let padded: Vec<(f32, f32)> = bboxes
        .iter()
        .map(|&(w, h)| (w + padding, h + padding))
        .collect();

    // Sort indices by height descending (tall rectangles first).
    let mut order: Vec<usize> = (0..padded.len()).collect();
    order.sort_by(|&a, &b| {
        padded[b]
            .1
            .partial_cmp(&padded[a].1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Target width for roughly square packing.
    let total_area: f32 = padded.iter().map(|(w, h)| w * h).sum();
    let target_width = total_area.sqrt() * 1.3;

    // Discretize into columns for the skyline.
    let col_width = padded
        .iter()
        .map(|(w, _)| *w)
        .fold(f32::INFINITY, f32::min)
        .max(0.01);
    let n_cols = (target_width / col_width).ceil() as usize;
    let mut skyline = vec![0.0_f32; n_cols];

    let mut origins = vec![(0.0_f32, 0.0_f32); bboxes.len()];

    for &idx in &order {
        let (rw, rh) = padded[idx];
        let span = ((rw / col_width).ceil() as usize).max(1).min(n_cols);

        // Find the position along the skyline with the lowest max height.
        let mut best_col = 0;
        let mut best_y = f32::INFINITY;
        for start in 0..=(n_cols - span) {
            let max_h = skyline[start..start + span]
                .iter()
                .copied()
                .fold(0.0_f32, f32::max);
            if max_h < best_y {
                best_y = max_h;
                best_col = start;
            }
        }

        let x = best_col as f32 * col_width;
        let y = best_y;
        origins[idx] = (x, y);

        // Update skyline.
        for h in skyline
            .iter_mut()
            .take((best_col + span).min(n_cols))
            .skip(best_col)
        {
            *h = y + rh;
        }
    }

    origins
}

// ── Single-component FR ─────────────────────────────────────────────────────

fn fr_single_component(n: usize, edges: &[[usize; 2]], iterations: u32) -> Vec<(f32, f32)> {
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![(0.0, 0.0)];
    }

    // Initial layout: place on a circle so the starting state isn't degenerate.
    let mut pos: Vec<(f32, f32)> = (0..n)
        .map(|i| {
            let angle = 2.0 * std::f32::consts::PI * i as f32 / n as f32;
            (angle.cos() * 10.0, angle.sin() * 10.0)
        })
        .collect();

    // FR parameters.
    let area = (n as f32) * 100.0;
    let k = (area / n as f32).sqrt(); // ideal spring length
    let k_sq = k * k;

    // Cooling: start hot, decay exponentially.
    let t_init = k * (n as f32).sqrt();
    let t_final = k * 0.01;
    let decay = (t_final / t_init).powf(1.0 / iterations.max(1) as f32);

    let mut temperature = t_init;

    for _iter in 0..iterations {
        // Repulsive forces (all pairs).
        let mut disp = vec![(0.0_f32, 0.0_f32); n];
        for i in 0..n {
            for j in (i + 1)..n {
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist_sq = (dx * dx + dy * dy).max(0.01);
                let force = k_sq / dist_sq.sqrt();
                let fx = dx / dist_sq.sqrt() * force;
                let fy = dy / dist_sq.sqrt() * force;
                disp[i].0 += fx;
                disp[i].1 += fy;
                disp[j].0 -= fx;
                disp[j].1 -= fy;
            }
        }

        // Attractive forces (edges).
        for e in edges {
            let (i, j) = (e[0], e[1]);
            if i >= n || j >= n {
                continue;
            }
            let dx = pos[i].0 - pos[j].0;
            let dy = pos[i].1 - pos[j].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let force = dist * dist / k;
            let fx = dx / dist * force;
            let fy = dy / dist * force;
            disp[i].0 -= fx;
            disp[i].1 -= fy;
            disp[j].0 += fx;
            disp[j].1 += fy;
        }

        // Gravity toward centroid (prevents drift).
        let cx = pos.iter().map(|p| p.0).sum::<f32>() / n as f32;
        let cy = pos.iter().map(|p| p.1).sum::<f32>() / n as f32;
        let gravity = 0.1 * temperature / t_init;
        for i in 0..n {
            disp[i].0 -= (pos[i].0 - cx) * gravity;
            disp[i].1 -= (pos[i].1 - cy) * gravity;
        }

        // Apply displacement, capped by temperature.
        for i in 0..n {
            let dx = disp[i].0;
            let dy = disp[i].1;
            let len = (dx * dx + dy * dy).sqrt().max(0.01);
            let capped = len.min(temperature);
            pos[i].0 += dx / len * capped;
            pos[i].1 += dy / len * capped;
        }

        temperature *= decay;
    }

    pos
}

// ── Connected components (union-find) ───────────────────────────────────────

pub(crate) fn connected_components(n: usize, edges: &[[usize; 2]]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..n).collect();
    let mut rank = vec![0u8; n];

    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while parent[root] != root {
            root = parent[root];
        }
        // Path compression
        let mut cur = i;
        while parent[cur] != root {
            let next = parent[cur];
            parent[cur] = root;
            cur = next;
        }
        root
    }

    for e in edges {
        if e[0] >= n || e[1] >= n {
            continue;
        }
        let ra = find(&mut parent, e[0]);
        let rb = find(&mut parent, e[1]);
        if ra != rb {
            if rank[ra] < rank[rb] {
                parent[ra] = rb;
            } else if rank[ra] > rank[rb] {
                parent[rb] = ra;
            } else {
                parent[rb] = ra;
                rank[ra] += 1;
            }
        }
    }

    let mut comp_map: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    for i in 0..n {
        comp_map.entry(find(&mut parent, i)).or_default().push(i);
    }

    let mut components: Vec<Vec<usize>> = comp_map.into_values().collect();
    components.sort_by_key(|c| std::cmp::Reverse(c.len()));
    components
}

fn assemble_positions(
    node_ids: &[String],
    component_positions: &[Vec<(f32, f32)>],
) -> Vec<NodePosition> {
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;

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
        let edges: &[[usize; 2]] = &[
            [0, 1],
            [0, 2],
            [0, 3],
            [0, 4],
            [0, 5],
            [0, 6],
            [0, 7],
            [0, 8],
        ];
        let req = make_fd_request(9, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 9);
        assert_valid_layout(&positions, "fd_star");
    }

    #[test]
    fn fd_disconnected_components() {
        let edges: &[[usize; 2]] = &[
            [0, 1],
            [1, 2],
            [2, 0], // component A
            [3, 4],
            [4, 5],
            [5, 3], // component B
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
        let bboxes = vec![(4.0, 3.0), (1.0, 1.0), (1.0, 1.0)];
        let origins = skyline_pack(&bboxes, 0.0);

        for (i, &(x, y)) in origins.iter().enumerate() {
            assert!(
                x >= 0.0 && y >= 0.0,
                "rect {i} has negative origin: ({x}, {y})"
            );
            assert!(
                x.is_finite() && y.is_finite(),
                "rect {i} non-finite: ({x}, {y})"
            );
        }

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
        let bboxes = vec![(1.0, 1.0); 9];
        let origins = skyline_pack(&bboxes, 0.0);

        let max_x = origins.iter().map(|o| o.0 + 1.0).fold(0.0_f32, f32::max);
        let max_y = origins.iter().map(|o| o.1 + 1.0).fold(0.0_f32, f32::max);

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

        assert!(x_range > 100.0, "fd x range too small: {x_range}");
        assert!(y_range > 100.0, "fd y range too small: {y_range}");
    }
}
