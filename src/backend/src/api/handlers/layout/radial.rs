//! Radial layout: uses hierarchical layering, then arranges each layer on a
//! concentric circle.
//!
//! The innermost layer (deepest in the DAG) goes to the center; if it contains
//! a single node, that node sits at the exact center.
//!
//! Node ordering within each ring reuses the Sugiyama crossing minimization,
//! then angular positions are refined to minimize the angle between edges and
//! the radial direction (edges should point toward/away from the center).

use super::{normalize, NodePosition, TARGET_SIZE};

pub(crate) fn radial(
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
    let ring_sep = 2.5_f32;

    for (li, layer) in layers.iter().enumerate() {
        let count = layer.len();

        let radius = if li == 0 && count == 1 {
            0.0
        } else if li == 0 {
            ring_sep
        } else {
            ring_sep * (li as f32 + 1.0)
        };

        if count == 1 {
            angle_of[layer[0]] = 0.0;
            radius_of[layer[0]] = radius;
        } else {
            let step = std::f32::consts::TAU / count as f32;
            for (pos, &node) in layer.iter().enumerate() {
                angle_of[node] = step * pos as f32;
                radius_of[node] = radius;
            }
        }
    }

    // Refinement: adjust angular positions to minimize deviation from radial
    // edge alignment.
    let mut neighbors: Vec<Vec<usize>> = vec![vec![]; n];
    for e in edges {
        if e[0] < n && e[1] < n {
            neighbors[e[0]].push(e[1]);
            neighbors[e[1]].push(e[0]);
        }
    }

    let mut layer_idx = vec![0usize; n];
    for (li, layer) in layers.iter().enumerate() {
        for &node in *layer {
            layer_idx[node] = li;
        }
    }

    for _pass in 0..10 {
        for li in 0..n_layers {
            let layer = &layers[li];
            if layer.len() <= 1 {
                continue;
            }

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

                let (mut sin_sum, mut cos_sum) = (0.0_f32, 0.0_f32);
                for &nb in &inner_nbrs {
                    sin_sum += angle_of[nb].sin();
                    cos_sum += angle_of[nb].cos();
                }
                ideal_angles.push(Some(sin_sum.atan2(cos_sum)));
            }

            let mut indexed: Vec<(usize, f32)> = layer
                .iter()
                .enumerate()
                .map(|(pos, &node)| {
                    let sort_angle = ideal_angles[pos].unwrap_or(angle_of[node]);
                    (node, sort_angle)
                })
                .collect();

            indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let count = indexed.len();
            let step = std::f32::consts::TAU / count as f32;
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::api::types::LayoutAlgorithm;

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

    fn assert_valid_layout(positions: &[NodePosition], label: &str) {
        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "{label}: node {i} non-finite: ({}, {})", p.x, p.y,
            );
            assert!(
                p.x.abs() <= TARGET_SIZE + 1.0 && p.y.abs() <= TARGET_SIZE + 1.0,
                "{label}: node {i} out of bounds: ({}, {})", p.x, p.y,
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
        let edges: &[[usize; 2]] = &[
            [0, 3], [0, 4], [1, 4], [1, 5],
            [2, 3], [2, 5], [3, 6], [4, 6],
            [4, 7], [5, 7],
        ];
        let req = make_radial_request(8, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 8);
        assert_valid_layout(&positions, "radial_3layer");

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
        let edges: &[[usize; 2]] = &[[0, 2], [1, 2], [2, 3]];
        let req = make_radial_request(4, edges);
        let positions = compute_layout(&req);
        assert_eq!(positions.len(), 4);

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
}
