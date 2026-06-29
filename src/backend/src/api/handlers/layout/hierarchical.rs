//! Sugiyama hierarchical layout with Brandes-Kopf coordinate assignment.
//!
//! Each connected component is laid out independently with its own Sugiyama
//! pass, then components are packed together using skyline bin-packing.

use super::force::connected_components;
use super::{normalize, NodePosition};
use crate::api::types::LayoutDirection;

/// Sugiyama hierarchical layout.
///
/// Each connected component is laid out independently with its own Sugiyama
/// pass, then components are packed together along the non-flow axis.
/// This prevents unrelated components from introducing edge crossings and
/// eliminates dead space from shared layering.
pub(crate) fn hierarchical(
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

/// Apply direction transform to Sugiyama coordinates (top-to-bottom -> desired).
pub(crate) fn apply_direction(
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::api::types::LayoutAlgorithm;

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
            [0, 6], [0, 7], [1, 8], [2, 9], [2, 10], [2, 11],
            [3, 10], [3, 11], [4, 11], [4, 12], [5, 12],
        ];
        let req = make_request(13, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 13);

        for (i, p) in positions.iter().enumerate() {
            assert!(
                p.x.is_finite() && p.y.is_finite(),
                "node {i} ({}) non-finite: ({}, {})", p.id, p.x, p.y,
            );
            assert!(
                p.x.abs() <= TARGET_SIZE + 1.0 && p.y.abs() <= TARGET_SIZE + 1.0,
                "node {i} out of bounds: ({}, {})", p.x, p.y,
            );
        }

        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(
                    dist > 1.0,
                    "nodes {i} and {j} overlap (dist={dist:.2}): ({:.1},{:.1}) vs ({:.1},{:.1})",
                    positions[i].x, positions[i].y, positions[j].x, positions[j].y,
                );
            }
        }
    }

    #[test]
    fn full_pipeline_large_graph() {
        let edges: &[[usize; 2]] = &[
            [0, 20], [0, 21], [1, 21], [1, 22], [2, 20], [2, 23],
            [3, 22], [3, 24], [4, 20], [4, 25], [5, 21], [5, 23],
            [6, 22], [6, 26], [7, 24], [7, 27], [8, 20], [8, 21],
            [9, 23], [9, 25], [10, 26], [10, 27], [11, 20], [11, 24],
            [12, 21], [12, 22], [13, 25], [13, 26], [14, 27], [14, 20],
            [15, 23], [15, 24], [16, 21], [16, 25], [17, 22], [17, 27],
            [18, 20], [18, 26], [19, 23], [19, 24],
        ];
        let req = make_request(28, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 28);
        for (i, p) in positions.iter().enumerate() {
            assert!(p.x.is_finite() && p.y.is_finite(), "node {i} non-finite");
        }
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(dist > 1.0, "nodes {i} and {j} too close (dist={dist:.1})");
            }
        }
    }

    #[test]
    fn full_pipeline_isolated_nodes() {
        let edges: &[[usize; 2]] = &[[0, 1], [1, 2], [2, 3], [3, 4]];
        let req = make_request(15, edges);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 15);
        for (i, p) in positions.iter().enumerate() {
            assert!(p.x.is_finite() && p.y.is_finite(), "node {i} non-finite");
        }
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(dist > 1.0, "nodes {i} and {j} too close (dist={dist:.1})");
            }
        }
    }

    #[test]
    fn full_pipeline_no_edges() {
        let req = make_request(10, &[]);
        let positions = compute_layout(&req);

        assert_eq!(positions.len(), 10);
        for (i, p) in positions.iter().enumerate() {
            assert!(p.x.is_finite() && p.y.is_finite(), "node {i} non-finite");
        }
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dist = ((positions[i].x - positions[j].x).powi(2)
                    + (positions[i].y - positions[j].y).powi(2))
                .sqrt();
                assert!(dist > 1.0, "nodes {i} and {j} too close (dist={dist:.1})");
            }
        }
    }

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

        assert!(x_range > 100.0, "x range too small: {x_range}");
        assert!(y_range > 100.0, "y range too small: {y_range}");
    }

    #[test]
    fn hierarchical_json_barycenters() {
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

        let x_range = pos.iter().map(|p| p.x).fold(f32::NEG_INFINITY, f32::max)
            - pos.iter().map(|p| p.x).fold(f32::INFINITY, f32::min);
        let y_range = pos.iter().map(|p| p.y).fold(f32::NEG_INFINITY, f32::max)
            - pos.iter().map(|p| p.y).fold(f32::INFINITY, f32::min);
        assert!(x_range > TARGET_SIZE, "squashed horizontally: {x_range:.0}");
        assert!(y_range > TARGET_SIZE, "squashed vertically: {y_range:.0}");
    }

    #[test]
    fn hier_component_stacking_direction() {
        let edges: &[[usize; 2]] = &[[0, 1], [1, 2], [3, 4], [4, 5]];

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

        assert!(
            y_sep_lr > x_sep_lr,
            "L->R: components should stack vertically: y_sep={y_sep_lr:.0} > x_sep={x_sep_lr:.0}"
        );

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

        assert!(
            x_sep_tb > y_sep_tb,
            "T->B: components should stack horizontally: x_sep={x_sep_tb:.0} > y_sep={y_sep_tb:.0}"
        );
    }
}
