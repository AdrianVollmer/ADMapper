//! Simple layout algorithms: circular, grid, lattice.

use super::{NodePosition, TARGET_SIZE};

pub(crate) fn circular(node_ids: &[String]) -> Vec<NodePosition> {
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

pub(crate) fn grid(node_ids: &[String]) -> Vec<NodePosition> {
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

pub(crate) fn lattice(node_ids: &[String]) -> Vec<NodePosition> {
    let n = node_ids.len();
    let cols = (n as f32).sqrt().ceil() as usize;
    let spacing = 180.0_f32;
    let angle: f32 = 0.5_f32.atan(); // 26.57deg
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
