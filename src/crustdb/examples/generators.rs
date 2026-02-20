//! Synthetic graph topology generators for stress testing.
//!
//! Four topologies targeting different performance characteristics:
//! - Dense Cluster: near-clique for BFS explosion testing
//! - Long Chain: linear path for deep traversal testing
//! - Wide Fan-Out: high branching factor tree
//! - Power-Law: Barabási-Albert scale-free network

use std::collections::HashSet;

/// A generated graph ready for loading.
#[derive(Debug, Clone)]
pub struct GeneratedGraph {
    /// Nodes as (labels, properties JSON value).
    pub nodes: Vec<(Vec<String>, serde_json::Value)>,
    /// Edges as (source_idx, target_idx, type, properties).
    /// Indices refer to position in nodes vector.
    pub edges: Vec<(usize, usize, String, serde_json::Value)>,
}

impl GeneratedGraph {
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

/// Graph topology types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// Near-clique: every node connected to ~10% of others randomly.
    DenseCluster,
    /// Linear chain with optional shortcut edges.
    LongChain,
    /// Tree with high branching factor.
    WideFanOut,
    /// Barabási-Albert preferential attachment (scale-free).
    PowerLaw,
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Topology::DenseCluster => write!(f, "dense_cluster"),
            Topology::LongChain => write!(f, "long_chain"),
            Topology::WideFanOut => write!(f, "wide_fanout"),
            Topology::PowerLaw => write!(f, "power_law"),
        }
    }
}

impl std::str::FromStr for Topology {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dense_cluster" | "dense" | "cluster" => Ok(Topology::DenseCluster),
            "long_chain" | "chain" | "linear" => Ok(Topology::LongChain),
            "wide_fanout" | "fanout" | "tree" => Ok(Topology::WideFanOut),
            "power_law" | "powerlaw" | "barabasi" => Ok(Topology::PowerLaw),
            _ => Err(format!("Unknown topology: {}", s)),
        }
    }
}

/// Simple linear congruential generator for reproducible randomness.
/// Avoids external dependency for a benchmark.
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed.wrapping_add(1),
        }
    }

    fn next_u64(&mut self) -> u64 {
        // LCG parameters from Numerical Recipes
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn next_usize(&mut self, max: usize) -> usize {
        (self.next_u64() as usize) % max
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }
}

/// Generate a dense cluster graph (near-clique).
///
/// Each node is connected to approximately `density` fraction of all other nodes.
/// Default density is 0.1 (10%).
pub fn generate_dense_cluster(n: usize, density: Option<f64>) -> GeneratedGraph {
    let density = density.unwrap_or(0.1);
    let mut rng = SimpleRng::new(42);

    let nodes: Vec<_> = (0..n)
        .map(|i| {
            (
                vec!["Node".to_string()],
                serde_json::json!({
                    "id": i,
                    "value": rng.next_usize(1000) as i64
                }),
            )
        })
        .collect();

    let mut edges = Vec::new();
    for i in 0..n {
        for j in 0..n {
            if i != j && rng.next_f64() < density {
                edges.push((i, j, "CONNECTED".to_string(), serde_json::json!({})));
            }
        }
    }

    GeneratedGraph { nodes, edges }
}

/// Generate a long chain graph with optional shortcut edges.
///
/// Creates a linear path A→B→C→...→N, plus random shortcuts.
pub fn generate_long_chain(n: usize, shortcuts: usize) -> GeneratedGraph {
    let mut rng = SimpleRng::new(42);

    let nodes: Vec<_> = (0..n)
        .map(|i| {
            (
                vec!["Node".to_string()],
                serde_json::json!({
                    "id": i,
                    "value": rng.next_usize(1000) as i64
                }),
            )
        })
        .collect();

    // Chain edges
    let mut edges: Vec<_> = (0..n - 1)
        .map(|i| (i, i + 1, "NEXT".to_string(), serde_json::json!({})))
        .collect();

    // Add random shortcut edges
    let mut shortcut_set: HashSet<(usize, usize)> = HashSet::new();
    while shortcut_set.len() < shortcuts && shortcut_set.len() < n * (n - 1) / 2 {
        let from = rng.next_usize(n);
        let to = rng.next_usize(n);
        if from != to && !shortcut_set.contains(&(from, to)) {
            shortcut_set.insert((from, to));
            edges.push((from, to, "SHORTCUT".to_string(), serde_json::json!({})));
        }
    }

    GeneratedGraph { nodes, edges }
}

/// Generate a wide fan-out tree.
///
/// Creates a tree with the specified branching factor and depth.
/// Total nodes = 1 + branching + branching^2 + ... + branching^depth
pub fn generate_wide_fanout(branching: usize, depth: usize) -> GeneratedGraph {
    let mut rng = SimpleRng::new(42);
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    // BFS to build the tree
    let mut current_level = vec![0usize]; // Start with root
    nodes.push((
        vec!["Node".to_string()],
        serde_json::json!({
            "id": 0,
            "value": rng.next_usize(1000) as i64,
            "depth": 0
        }),
    ));

    for d in 0..depth {
        let mut next_level = Vec::new();
        for &parent_idx in &current_level {
            for _ in 0..branching {
                let child_idx = nodes.len();
                nodes.push((
                    vec!["Node".to_string()],
                    serde_json::json!({
                        "id": child_idx,
                        "value": rng.next_usize(1000) as i64,
                        "depth": d + 1
                    }),
                ));
                edges.push((
                    parent_idx,
                    child_idx,
                    "CHILD".to_string(),
                    serde_json::json!({}),
                ));
                next_level.push(child_idx);
            }
        }
        current_level = next_level;
    }

    GeneratedGraph { nodes, edges }
}

/// Generate a power-law (Barabási-Albert) graph.
///
/// Uses preferential attachment: new nodes connect to existing nodes
/// with probability proportional to their degree.
/// `m` is the number of edges each new node creates.
pub fn generate_power_law(n: usize, m: usize) -> GeneratedGraph {
    let mut rng = SimpleRng::new(42);

    // Start with a small complete graph of m+1 nodes
    let initial_nodes = m + 1;
    let mut nodes: Vec<_> = (0..initial_nodes)
        .map(|i| {
            (
                vec!["Node".to_string()],
                serde_json::json!({
                    "id": i,
                    "value": rng.next_usize(1000) as i64
                }),
            )
        })
        .collect();

    // Initial complete graph edges
    let mut edges: Vec<(usize, usize, String, serde_json::Value)> = Vec::new();
    for i in 0..initial_nodes {
        for j in (i + 1)..initial_nodes {
            edges.push((i, j, "LINKED".to_string(), serde_json::json!({})));
        }
    }

    // Track degree for preferential attachment
    let mut degrees: Vec<usize> = vec![initial_nodes - 1; initial_nodes];
    let mut total_degree: usize = degrees.iter().sum();

    // Add remaining nodes with preferential attachment
    while nodes.len() < n {
        let new_idx = nodes.len();
        nodes.push((
            vec!["Node".to_string()],
            serde_json::json!({
                "id": new_idx,
                "value": rng.next_usize(1000) as i64
            }),
        ));
        degrees.push(0);

        // Connect to m existing nodes with probability proportional to degree
        let mut connected: HashSet<usize> = HashSet::new();
        let mut attempts = 0;
        while connected.len() < m && connected.len() < new_idx && attempts < 1000 {
            attempts += 1;

            // Pick a random node with probability proportional to degree
            let target_sum = rng.next_usize(total_degree.max(1));
            let mut cumsum = 0;
            let mut target = 0;
            for (i, &deg) in degrees.iter().enumerate().take(new_idx) {
                cumsum += deg;
                if cumsum > target_sum {
                    target = i;
                    break;
                }
            }

            if !connected.contains(&target) {
                connected.insert(target);
                edges.push((new_idx, target, "LINKED".to_string(), serde_json::json!({})));
                degrees[new_idx] += 1;
                degrees[target] += 1;
                total_degree += 2;
            }
        }
    }

    GeneratedGraph { nodes, edges }
}

/// Generate a graph of the specified topology and scale.
pub fn generate(topology: Topology, scale: usize) -> GeneratedGraph {
    match topology {
        Topology::DenseCluster => generate_dense_cluster(scale, None),
        Topology::LongChain => {
            // Add ~1% shortcuts for variety
            let shortcuts = (scale / 100).max(10);
            generate_long_chain(scale, shortcuts)
        }
        Topology::WideFanOut => {
            // Calculate depth to get approximately `scale` nodes with branching=100
            // Total = sum of branching^i for i in 0..depth
            // For branching=100: depth=2 gives 10101 nodes, depth=1 gives 101 nodes
            let (branching, depth) = if scale <= 100 {
                (scale.saturating_sub(1).max(1), 1)
            } else if scale <= 10_000 {
                (100, 2)
            } else if scale <= 1_000_000 {
                (100, 3) // ~1,010,101 nodes
            } else {
                (100, 4)
            };
            generate_wide_fanout(branching, depth)
        }
        Topology::PowerLaw => {
            // m=3 gives realistic power-law with moderate density
            generate_power_law(scale, 3)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dense_cluster() {
        let graph = generate_dense_cluster(100, Some(0.1));
        assert_eq!(graph.node_count(), 100);
        // With 10% density, expect ~100*99*0.1 = 990 edges (but random)
        assert!(graph.edge_count() > 500 && graph.edge_count() < 1500);
    }

    #[test]
    fn test_long_chain() {
        let graph = generate_long_chain(100, 10);
        assert_eq!(graph.node_count(), 100);
        assert_eq!(graph.edge_count(), 99 + 10); // Chain + shortcuts
    }

    #[test]
    fn test_wide_fanout() {
        let graph = generate_wide_fanout(10, 2);
        // 1 root + 10 children + 100 grandchildren = 111 nodes
        assert_eq!(graph.node_count(), 111);
        // Each non-leaf has 10 children = 11 * 10 = 110 edges
        assert_eq!(graph.edge_count(), 110);
    }

    #[test]
    fn test_power_law() {
        let graph = generate_power_law(100, 3);
        assert_eq!(graph.node_count(), 100);
        // Initial complete graph of 4 nodes = 6 edges
        // Then 96 nodes each adding ~3 edges = ~288 edges
        // Total ~294 edges (may vary due to collision avoidance)
        assert!(graph.edge_count() > 250 && graph.edge_count() < 350);
    }
}
