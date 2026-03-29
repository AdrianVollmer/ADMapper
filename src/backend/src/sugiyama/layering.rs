//! Layer assignment and virtual node insertion for Sugiyama layout.

use super::graph::DagGraph;

/// Extended graph with layer information and virtual nodes for long edges.
pub(crate) struct LayeredGraph {
    /// Nodes in each layer, in left-to-right order.
    pub layers: Vec<Vec<usize>>,
    /// Layer index for each node (real and virtual).
    pub layer_of: Vec<usize>,
    /// Number of real (non-virtual) nodes. Indices 0..n_real are real.
    pub n_real: usize,
    /// Outgoing adjacency (extended with virtual nodes).
    pub out_adj: Vec<Vec<usize>>,
    /// Incoming adjacency (extended with virtual nodes).
    pub in_adj: Vec<Vec<usize>>,
    /// Whether a node is virtual (dummy node for long edge routing).
    pub is_virtual: Vec<bool>,
}

/// Assign layers using longest-path from sources.
///
/// For every edge s -> t: `layer[t] >= layer[s] + 1`.
/// Roots (no incoming edges) get layer 0.
pub(crate) fn assign_layers(graph: &DagGraph) -> Vec<usize> {
    // Kahn's algorithm for topological sort
    let mut in_degree: Vec<usize> = graph.in_adj.iter().map(|v| v.len()).collect();
    let mut queue: std::collections::VecDeque<usize> =
        (0..graph.n).filter(|&i| in_degree[i] == 0).collect();
    let mut topo = Vec::with_capacity(graph.n);

    while let Some(node) = queue.pop_front() {
        topo.push(node);
        for &target in &graph.out_adj[node] {
            in_degree[target] -= 1;
            if in_degree[target] == 0 {
                queue.push_back(target);
            }
        }
    }

    // Defensive: if some nodes weren't reached (shouldn't happen after cycle
    // removal), add them at the end.
    if topo.len() < graph.n {
        let in_topo: std::collections::HashSet<usize> = topo.iter().copied().collect();
        for i in 0..graph.n {
            if !in_topo.contains(&i) {
                topo.push(i);
            }
        }
    }

    // Longest-path layering: process in topological order.
    let mut layer = vec![0usize; graph.n];
    for &node in &topo {
        for &pred in &graph.in_adj[node] {
            layer[node] = layer[node].max(layer[pred] + 1);
        }
    }

    layer
}

/// Build the layered graph, inserting virtual nodes for edges that span
/// more than one layer.
pub(crate) fn build_layered_graph(graph: &DagGraph, layer_of_real: &[usize]) -> LayeredGraph {
    let n_real = graph.n;
    let mut layer_of = layer_of_real.to_vec();
    let mut out_adj: Vec<Vec<usize>> = graph.out_adj.clone();
    let mut in_adj: Vec<Vec<usize>> = graph.in_adj.clone();
    let mut is_virtual = vec![false; n_real];

    // Collect edges spanning more than one layer.
    let mut long_edges = Vec::new();
    for s in 0..n_real {
        for &t in &graph.out_adj[s] {
            if layer_of[t] - layer_of[s] > 1 {
                long_edges.push((s, t));
            }
        }
    }

    for (s, t) in long_edges {
        // Remove original edge
        out_adj[s].retain(|&x| x != t);
        in_adj[t].retain(|&x| x != s);

        // Insert virtual nodes at each intermediate layer
        let mut prev = s;
        for l in (layer_of[s] + 1)..layer_of[t] {
            let vnode = layer_of.len();
            layer_of.push(l);
            is_virtual.push(true);
            out_adj.push(Vec::new());
            in_adj.push(Vec::new());

            out_adj[prev].push(vnode);
            in_adj[vnode].push(prev);
            prev = vnode;
        }
        out_adj[prev].push(t);
        in_adj[t].push(prev);
    }

    // Build layer lists
    let max_layer = layer_of.iter().copied().max().unwrap_or(0);
    let mut layers = vec![vec![]; max_layer + 1];
    for (i, &l) in layer_of.iter().enumerate() {
        layers[l].push(i);
    }

    LayeredGraph {
        layers,
        layer_of,
        n_real,
        out_adj,
        in_adj,
        is_virtual,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_layers() {
        let g = DagGraph::new(3, &[[0, 1], [1, 2]]);
        let layers = assign_layers(&g);
        assert_eq!(layers, vec![0, 1, 2]);
    }

    #[test]
    fn diamond_layers() {
        // A -> B, A -> C, B -> D, C -> D
        let g = DagGraph::new(4, &[[0, 1], [0, 2], [1, 3], [2, 3]]);
        let layers = assign_layers(&g);
        assert_eq!(layers[0], 0);
        assert_eq!(layers[1], 1);
        assert_eq!(layers[2], 1);
        assert_eq!(layers[3], 2);
    }

    #[test]
    fn virtual_nodes_inserted() {
        // A(0) -> B(1) -> C(2), A(0) -> C(2)  — edge A->C spans 2 layers
        let g = DagGraph::new(3, &[[0, 1], [1, 2], [0, 2]]);
        let layers = assign_layers(&g);
        let lg = build_layered_graph(&g, &layers);

        // One virtual node should be inserted for the A->C edge
        assert_eq!(lg.n_real, 3);
        assert!(lg.layer_of.len() > 3, "virtual nodes should be added");
        assert!(lg.is_virtual[3], "node 3 should be virtual");
        assert_eq!(lg.layer_of[3], 1, "virtual node at layer 1");
    }

    #[test]
    fn no_virtual_nodes_for_adjacent_layers() {
        let g = DagGraph::new(2, &[[0, 1]]);
        let layers = assign_layers(&g);
        let lg = build_layered_graph(&g, &layers);
        assert_eq!(lg.layer_of.len(), 2, "no virtual nodes needed");
    }
}
