//! Server-side graph layout.
//!
//! All layout algorithms operate on simple index-based adjacency lists --
//! no external graph library needed.
//!
//! The force-directed layout is based on the Fruchterman-Reingold implementation
//! from visgraph by Raoul Luque, licensed under MIT OR Apache-2.0.
//! Source: <https://github.com/raoulluque/visgraph>
//!
//! Hierarchical: Sugiyama layered layout with Brandes-Kopf coordinate
//! assignment.

mod force;
mod hierarchical;
mod radial;
mod simple;

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
            force::force_directed(&req.nodes, &req.edges, iterations, temperature)
        }
        LayoutAlgorithm::Hierarchical => {
            let direction = req.direction.unwrap_or(LayoutDirection::LeftToRight);
            hierarchical::hierarchical(
                &req.nodes,
                &req.edges,
                direction,
                req.node_labels.as_deref(),
            )
        }
        LayoutAlgorithm::Circular => simple::circular(&req.nodes),
        LayoutAlgorithm::Grid => simple::grid(&req.nodes),
        LayoutAlgorithm::Lattice => simple::lattice(&req.nodes),
        LayoutAlgorithm::Radial => {
            radial::radial(&req.nodes, &req.edges, req.node_labels.as_deref())
        }
    }
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
