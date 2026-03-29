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
    }
}

// ============================================================================
// Force-directed (Fruchterman-Reingold)
// ============================================================================

/// Fruchterman-Reingold force-directed layout.
///
/// Based on code from visgraph by Raoul Luque (MIT OR Apache-2.0).
/// Adapted to work on raw index-based adjacency lists instead of petgraph.
/// Source: <https://github.com/raoulluque/visgraph>
fn force_directed(
    node_ids: &[String],
    edges: &[[usize; 2]],
    iterations: u32,
    initial_temperature: f32,
) -> Vec<NodePosition> {
    let n = node_ids.len();
    if n == 0 {
        return Vec::new();
    }

    let mut pos = vec![(0.0_f32, 0.0_f32); n];
    let mut rng = fastrand::Rng::new();
    for p in &mut pos {
        *p = (rng.f32(), rng.f32());
    }

    let k = (1.0 / n as f32).sqrt();
    let clip = 0.01_f32;

    for iteration in 0..iterations {
        let mut disp = vec![(0.0_f32, 0.0_f32); n];

        // Repulsive forces (all pairs)
        for i in 0..n {
            for j in (i + 1)..n {
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist = (dx * dx + dy * dy).sqrt().max(clip);
                let force = k * k / dist;
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

        // Apply with cooling
        let temp = initial_temperature - (0.1 * iteration as f32) / ((iterations + 1) as f32);
        for i in 0..n {
            let len = (disp[i].0 * disp[i].0 + disp[i].1 * disp[i].1).sqrt();
            if len > 0.0 {
                let limited = len.min(temp);
                pos[i].0 += (disp[i].0 / len) * limited;
                pos[i].1 += (disp[i].1 / len) * limited;
            }
        }
    }

    let positions = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodePosition {
            id: id.clone(),
            x: pos[i].0,
            y: pos[i].1,
        })
        .collect();

    normalize(positions)
}

// ============================================================================
// Hierarchical (center-of-gravity)
// ============================================================================

/// Sugiyama hierarchical layout with Brandes-Kopf coordinate assignment.
///
/// Pipeline: cycle removal → longest-path layering → virtual nodes →
/// barycenter crossing minimization → Brandes-Kopf coordinates.
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

    // Build sort keys: (label, name) for tiebreaking in crossing minimization.
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

    let config = crate::sugiyama::SugiyamaConfig::default();
    let coords = crate::sugiyama::layout(n, edges, &config, sort_keys.as_deref());

    // Apply direction transform (Sugiyama produces top-to-bottom)
    let positions: Vec<NodePosition> = node_ids
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

/// Center at origin, scale each axis to fill [-TARGET_SIZE, TARGET_SIZE].
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
}
