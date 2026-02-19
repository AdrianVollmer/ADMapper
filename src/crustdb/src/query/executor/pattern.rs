//! Pattern matching execution for MATCH queries.

use crate::error::{Error, Result};
use crate::graph::{Edge, Node, PropertyValue};
use crate::query::parser::{
    Direction, Expression, Literal, NodePattern, Pattern, PatternElement, RelQuantifier,
    RelationshipPattern,
};
use crate::storage::SqliteStorage;
use std::collections::{HashMap, HashSet, VecDeque};

use super::eval::floats_equal;
use super::{Binding, Path, PathConstraints};

// =============================================================================
// Configuration Constants
// =============================================================================

/// Default maximum path length for unbounded traversals.
///
/// Used when queries specify open-ended patterns like `(a)-[:REL*]->(b)` or
/// `(a)-[:REL]-+(b)` without an explicit upper bound. This prevents infinite
/// loops and memory exhaustion on cyclic or very deep graphs.
///
/// The value 10000 allows traversing reasonably large graphs while providing
/// a safety limit. For queries that need deeper traversal, use explicit bounds
/// like `(a)-[:REL*1..50000]->(b)`.
pub const DEFAULT_MAX_PATH_DEPTH: usize = 10000;

/// State for BFS traversal with path tracking.
#[derive(Clone)]
struct TraversalState {
    node_id: i64,
    /// Node IDs in the path (including current).
    path_nodes: Vec<i64>,
    /// Edges traversed to reach this state.
    path_edges: Vec<Edge>,
}

/// Check if pattern is a single node pattern.
pub fn is_single_node_pattern(pattern: &Pattern) -> bool {
    pattern.elements.len() == 1 && matches!(pattern.elements[0], PatternElement::Node(_))
}

/// Check if pattern is a single-hop relationship pattern (node-rel-node) without variable length.
pub fn is_single_hop_pattern(pattern: &Pattern) -> bool {
    if pattern.elements.len() != 3 {
        return false;
    }
    if !matches!(pattern.elements[0], PatternElement::Node(_)) {
        return false;
    }
    if !matches!(pattern.elements[2], PatternElement::Node(_)) {
        return false;
    }
    // Check that the relationship has no variable length
    match &pattern.elements[1] {
        PatternElement::Relationship(rel) => rel.length.is_none(),
        _ => false,
    }
}

/// Check if pattern is a variable-length relationship pattern (node-[*..]-node).
pub fn is_variable_length_pattern(pattern: &Pattern) -> bool {
    if pattern.elements.len() != 3 {
        return false;
    }
    if !matches!(pattern.elements[0], PatternElement::Node(_)) {
        return false;
    }
    if !matches!(pattern.elements[2], PatternElement::Node(_)) {
        return false;
    }
    // Check that the relationship has variable length
    match &pattern.elements[1] {
        PatternElement::Relationship(rel) => rel.length.is_some(),
        _ => false,
    }
}

/// Check if pattern is a multi-hop pattern (node-rel-node-rel-node...).
pub fn is_multi_hop_pattern(pattern: &Pattern) -> bool {
    // Must have at least 5 elements (node-rel-node-rel-node)
    if pattern.elements.len() < 5 {
        return false;
    }
    // Must have odd number of elements (alternating node-rel-node...)
    if pattern.elements.len() % 2 == 0 {
        return false;
    }
    // Check alternating pattern: node, rel, node, rel, node, ...
    for (i, elem) in pattern.elements.iter().enumerate() {
        if i % 2 == 0 {
            // Even indices should be nodes
            if !matches!(elem, PatternElement::Node(_)) {
                return false;
            }
        } else {
            // Odd indices should be relationships without variable length
            match elem {
                PatternElement::Relationship(rel) => {
                    if rel.length.is_some() {
                        return false; // Variable length not supported in multi-hop yet
                    }
                }
                _ => return false,
            }
        }
    }
    true
}

/// Check if pattern is a shortest path pattern (has shortest_k or quantifier).
pub fn is_shortest_path_pattern(pattern: &Pattern) -> bool {
    // Check if SHORTEST k is specified
    if pattern.shortest_k.is_some() {
        return true;
    }

    // Check if any relationship has a quantifier (+, *)
    for elem in &pattern.elements {
        if let PatternElement::Relationship(rel) = elem {
            if rel.quantifier.is_some() {
                return true;
            }
        }
    }

    false
}

/// Extract source and target variable names from a shortest path pattern.
pub fn get_path_endpoint_vars(pattern: &Pattern) -> (String, String) {
    let source_var = pattern
        .elements
        .first()
        .and_then(|e| match e {
            PatternElement::Node(n) => n.variable.clone(),
            _ => None,
        })
        .unwrap_or_else(|| "_src".to_string());

    let target_var = pattern
        .elements
        .last()
        .and_then(|e| match e {
            PatternElement::Node(n) => n.variable.clone(),
            _ => None,
        })
        .unwrap_or_else(|| "_tgt".to_string());

    (source_var, target_var)
}

/// Execute a shortest path pattern using BFS.
///
/// The `constraints` parameter enables predicate pushdown: when the WHERE clause
/// specifies specific source/target node IDs (e.g., `src.id = 0 AND dst.id = 24`),
/// we can filter nodes BEFORE BFS starts, enabling proper early termination.
pub fn execute_shortest_path_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
    constraints: &PathConstraints,
) -> Result<Vec<Binding>> {
    // Extract pattern components (must be node-rel-node for now)
    if pattern.elements.len() != 3 {
        return Err(Error::Cypher(
            "SHORTEST path requires a simple (a)-[r]->(b) pattern".into(),
        ));
    }

    let (source_pattern, rel_pattern, target_pattern) = match (
        &pattern.elements[0],
        &pattern.elements[1],
        &pattern.elements[2],
    ) {
        (PatternElement::Node(s), PatternElement::Relationship(r), PatternElement::Node(t)) => {
            (s, r, t)
        }
        _ => return Err(Error::Cypher("Invalid shortest path pattern".into())),
    };

    let source_var = source_pattern.variable.as_deref().unwrap_or("_src");
    let target_var = target_pattern.variable.as_deref().unwrap_or("_tgt");
    let path_var = pattern.path_variable.as_deref();

    // Determine min/max hops based on quantifier
    let (min_hops, max_hops) = match &rel_pattern.quantifier {
        Some(RelQuantifier::OneOrMore) => (1usize, DEFAULT_MAX_PATH_DEPTH),
        Some(RelQuantifier::ZeroOrMore) => (0usize, DEFAULT_MAX_PATH_DEPTH),
        None => {
            // Check if there's a length spec
            if let Some(ref len) = rel_pattern.length {
                (
                    len.min.unwrap_or(1) as usize,
                    len.max.unwrap_or(DEFAULT_MAX_PATH_DEPTH as u32) as usize,
                )
            } else {
                (1, DEFAULT_MAX_PATH_DEPTH) // Default: one or more
            }
        }
    };

    // BFS state
    #[derive(Clone, Debug)]
    struct PathState {
        node_id: i64,
        path_nodes: Vec<i64>,
        path_edges: Vec<i64>,
    }

    // For collecting shortest paths
    #[derive(Debug)]
    struct PathResult {
        length: usize,
        path_nodes: Vec<i64>,
        path_edges: Vec<i64>,
        source_node: Node,
        target_node: Node,
    }

    // Scan source nodes and apply pushed-down constraints
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;
    let source_nodes: Vec<Node> = if constraints.source_props.is_empty() {
        source_nodes
    } else {
        // Filter to only nodes matching ALL WHERE clause property constraints
        source_nodes
            .into_iter()
            .filter(|n| {
                constraints.source_props.iter().all(|(prop, values)| {
                    n.properties.get(prop).is_some_and(|pv| values.contains(pv))
                })
            })
            .collect()
    };

    // Scan target nodes and apply pushed-down constraints
    let target_nodes = scan_nodes(target_pattern, storage)?;
    let target_nodes = filter_by_properties(target_nodes, target_pattern)?;
    let target_nodes: Vec<Node> = if constraints.target_props.is_empty() {
        target_nodes
    } else {
        // Filter to only nodes matching ALL WHERE clause property constraints
        target_nodes
            .into_iter()
            .filter(|n| {
                constraints.target_props.iter().all(|(prop, values)| {
                    n.properties.get(prop).is_some_and(|pv| values.contains(pv))
                })
            })
            .collect()
    };
    let target_ids: HashSet<i64> = target_nodes.iter().map(|n| n.id).collect();
    let target_map: HashMap<i64, Node> = target_nodes.into_iter().map(|n| (n.id, n)).collect();

    let mut all_results: Vec<PathResult> = Vec::new();
    let k = pattern.shortest_k.unwrap_or(1) as usize;

    // Optimization: for SHORTEST 1 with specific target, use simple BFS with visited set
    // This is O(V+E) instead of exponential in the number of paths
    let use_fast_bfs = k == 1 && target_ids.len() == 1;

    // BFS from each source node to find shortest paths
    for source_node in source_nodes {
        // For fast single-target BFS, use a visited set to avoid exponential path enumeration
        let mut visited: HashSet<i64> = HashSet::new();
        let mut found_paths: Vec<PathResult> = Vec::new();
        let mut queue: VecDeque<PathState> = VecDeque::new();
        let mut shortest_found: Option<usize> = None;

        queue.push_back(PathState {
            node_id: source_node.id,
            path_nodes: vec![source_node.id],
            path_edges: vec![],
        });
        visited.insert(source_node.id);

        // BFS level by level to ensure shortest paths first
        while let Some(state) = queue.pop_front() {
            let current_depth = state.path_edges.len();

            // Early termination: if we've found enough paths, stop
            if found_paths.len() >= k {
                break;
            }

            // Early termination: if we've found paths at a shorter depth, skip deeper paths
            if let Some(shortest) = shortest_found {
                if current_depth > shortest {
                    break;
                }
            }

            // Check if current depth exceeds max
            if current_depth > max_hops {
                continue;
            }

            // Check if we reached a target node at valid depth
            if current_depth >= min_hops && target_ids.contains(&state.node_id) {
                if state.node_id != source_node.id || current_depth > 0 {
                    // Valid path found
                    if let Some(target_node) = target_map.get(&state.node_id) {
                        found_paths.push(PathResult {
                            length: current_depth,
                            path_nodes: state.path_nodes.clone(),
                            path_edges: state.path_edges.clone(),
                            source_node: source_node.clone(),
                            target_node: target_node.clone(),
                        });
                        if shortest_found.is_none() {
                            shortest_found = Some(current_depth);
                        }
                        // For fast BFS with single target, stop immediately
                        if use_fast_bfs {
                            break;
                        }
                    }
                }
            }

            // Don't expand if we've reached max depth
            if current_depth >= max_hops {
                continue;
            }

            // Don't expand states that are at the shortest path depth
            if let Some(shortest) = shortest_found {
                if current_depth >= shortest {
                    continue;
                }
            }

            // Expand to neighbors
            let edges = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(state.node_id)?,
                Direction::Incoming => storage.find_incoming_edges(state.node_id)?,
                Direction::Both => {
                    let mut edges = storage.find_outgoing_edges(state.node_id)?;
                    edges.extend(storage.find_incoming_edges(state.node_id)?);
                    edges
                }
            };

            // Filter edges by type if specified
            let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
                edges
            } else {
                edges
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.edge_type))
                    .collect()
            };

            for edge in edges {
                // Determine the next node
                let next_node_id = match rel_pattern.direction {
                    Direction::Outgoing => edge.target,
                    Direction::Incoming => edge.source,
                    Direction::Both => {
                        if edge.source == state.node_id {
                            edge.target
                        } else {
                            edge.source
                        }
                    }
                };

                // For fast BFS: skip if already visited (globally)
                // For k>1 BFS: only avoid cycles within the same path
                if use_fast_bfs {
                    if visited.contains(&next_node_id) {
                        continue;
                    }
                    visited.insert(next_node_id);
                } else if state.path_nodes.contains(&next_node_id) {
                    continue;
                }

                let mut new_path_nodes = state.path_nodes.clone();
                new_path_nodes.push(next_node_id);

                let mut new_path_edges = state.path_edges.clone();
                new_path_edges.push(edge.id);

                queue.push_back(PathState {
                    node_id: next_node_id,
                    path_nodes: new_path_nodes,
                    path_edges: new_path_edges,
                });
            }
        }

        all_results.extend(found_paths);
    }

    // Sort results by path length (shortest first)
    all_results.sort_by_key(|r| r.length);

    // Convert to bindings
    let mut bindings: Vec<Binding> = Vec::new();
    for result in all_results {
        let mut binding = Binding::new()
            .with_node(source_var, result.source_node)
            .with_node(target_var, result.target_node);

        if let Some(pv) = path_var {
            // Fetch full node objects for path
            let mut path_nodes: Vec<Node> = Vec::new();
            for &nid in &result.path_nodes {
                if let Some(node) = storage.get_node(nid)? {
                    path_nodes.push(node);
                }
            }
            // Fetch full edge objects for path
            let mut path_edges: Vec<Edge> = Vec::new();
            for &eid in &result.path_edges {
                if let Some(edge) = storage.get_edge(eid)? {
                    path_edges.push(edge);
                }
            }
            binding = binding.with_path(
                pv,
                Path {
                    nodes: path_nodes,
                    edges: path_edges,
                },
            );
        }

        bindings.push(binding);
    }

    Ok(bindings)
}

/// Execute a single-node pattern match.
pub fn execute_single_node_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
    limit: Option<u64>,
) -> Result<Vec<Binding>> {
    let node_pattern = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected node pattern".into())),
    };

    let variable = node_pattern.variable.as_deref().unwrap_or("_");

    // Scan and filter nodes (with optional SQL-level limit)
    let nodes = scan_nodes_with_limit(node_pattern, storage, limit)?;
    let nodes = filter_by_properties(nodes, node_pattern)?;

    // Convert to bindings
    let bindings = nodes
        .into_iter()
        .map(|node| Binding::new().with_node(variable, node))
        .collect();

    Ok(bindings)
}

/// Execute a single-hop relationship pattern match.
pub fn execute_single_hop_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    // Extract pattern components
    let (source_pattern, rel_pattern, target_pattern) = match (
        &pattern.elements[0],
        &pattern.elements[1],
        &pattern.elements[2],
    ) {
        (PatternElement::Node(s), PatternElement::Relationship(r), PatternElement::Node(t)) => {
            (s, r, t)
        }
        _ => return Err(Error::Cypher("Invalid single-hop pattern".into())),
    };

    let source_var = source_pattern.variable.as_deref().unwrap_or("_src");
    let rel_var = rel_pattern.variable.as_deref();
    let target_var = target_pattern.variable.as_deref().unwrap_or("_tgt");
    let path_var = pattern.path_variable.as_deref();

    // Scan source nodes
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;

    let mut bindings = Vec::new();

    // For each source node, find matching relationships and targets
    for source_node in source_nodes {
        let edges = match rel_pattern.direction {
            Direction::Outgoing => storage.find_outgoing_edges(source_node.id)?,
            Direction::Incoming => storage.find_incoming_edges(source_node.id)?,
            Direction::Both => {
                let mut edges = storage.find_outgoing_edges(source_node.id)?;
                edges.extend(storage.find_incoming_edges(source_node.id)?);
                edges
            }
        };

        // Filter edges by type if specified
        let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
            edges
        } else {
            edges
                .into_iter()
                .filter(|e| rel_pattern.types.contains(&e.edge_type))
                .collect()
        };

        // Filter edges by properties if specified
        let edges = filter_edges_by_properties(edges, rel_pattern)?;

        // For each matching edge, get the target node and check if it matches
        for edge in edges {
            // Determine the actual target node based on direction
            let target_id = match rel_pattern.direction {
                Direction::Outgoing => edge.target,
                Direction::Incoming => edge.source,
                Direction::Both => {
                    if edge.source == source_node.id {
                        edge.target
                    } else {
                        edge.source
                    }
                }
            };

            // Get the target node
            let target_node = match storage.get_node(target_id)? {
                Some(n) => n,
                None => continue,
            };

            // Check if target node matches the target pattern
            if !node_matches_pattern(&target_node, target_pattern) {
                continue;
            }

            // Create binding for this match
            let mut binding = Binding::new()
                .with_node(source_var, source_node.clone())
                .with_node(target_var, target_node.clone());

            if let Some(rv) = rel_var {
                binding = binding.with_edge(rv, edge.clone());
            }

            // Add path variable if specified
            if let Some(pv) = path_var {
                let path = Path {
                    nodes: vec![source_node.clone(), target_node.clone()],
                    edges: vec![edge.clone()],
                };
                binding = binding.with_path(pv, path);
            }

            bindings.push(binding);
        }
    }

    Ok(bindings)
}

/// Execute a multi-hop pattern match (e.g., (a)-[r1]->(b)-[r2]->(c)).
pub fn execute_multi_hop_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    let path_var = pattern.path_variable.as_deref();

    // Extract all nodes and relationships from the pattern
    let mut node_patterns: Vec<&NodePattern> = Vec::new();
    let mut rel_patterns: Vec<&RelationshipPattern> = Vec::new();

    for (i, elem) in pattern.elements.iter().enumerate() {
        if i % 2 == 0 {
            match elem {
                PatternElement::Node(np) => node_patterns.push(np),
                _ => return Err(Error::Cypher("Invalid multi-hop pattern".into())),
            }
        } else {
            match elem {
                PatternElement::Relationship(rp) => rel_patterns.push(rp),
                _ => return Err(Error::Cypher("Invalid multi-hop pattern".into())),
            }
        }
    }

    // Start with the first node pattern
    let first_node_pattern = node_patterns[0];
    let first_var = first_node_pattern.variable.as_deref().unwrap_or("_n0");

    let initial_nodes = scan_nodes(first_node_pattern, storage)?;
    let initial_nodes = filter_by_properties(initial_nodes, first_node_pattern)?;

    // Initialize bindings with first node
    let mut current_bindings: Vec<(Binding, Vec<i64>, Vec<i64>)> = initial_nodes
        .into_iter()
        .map(|node| {
            let node_id = node.id;
            let binding = Binding::new().with_node(first_var, node);
            (binding, vec![node_id], vec![])
        })
        .collect();

    // Process each hop
    for hop_idx in 0..rel_patterns.len() {
        let rel_pattern = rel_patterns[hop_idx];
        let target_pattern = node_patterns[hop_idx + 1];

        let rel_var = rel_pattern.variable.as_deref();
        let default_target_var = format!("_n{}", hop_idx + 1);
        let target_var = target_pattern
            .variable
            .as_deref()
            .unwrap_or(&default_target_var);

        let mut next_bindings: Vec<(Binding, Vec<i64>, Vec<i64>)> = Vec::new();

        for (binding, path_nodes, path_edges) in current_bindings {
            // Get the last node in the path
            let last_node_id = *path_nodes.last().unwrap();

            // Find edges from the last node
            let edges = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(last_node_id)?,
                Direction::Incoming => storage.find_incoming_edges(last_node_id)?,
                Direction::Both => {
                    let mut edges = storage.find_outgoing_edges(last_node_id)?;
                    edges.extend(storage.find_incoming_edges(last_node_id)?);
                    edges
                }
            };

            // Filter edges by type
            let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
                edges
            } else {
                edges
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.edge_type))
                    .collect()
            };

            // Filter edges by properties
            let edges = filter_edges_by_properties(edges, rel_pattern)?;

            for edge in edges {
                // Determine the target node
                let target_id = match rel_pattern.direction {
                    Direction::Outgoing => edge.target,
                    Direction::Incoming => edge.source,
                    Direction::Both => {
                        if edge.source == last_node_id {
                            edge.target
                        } else {
                            edge.source
                        }
                    }
                };

                // Get target node
                let target_node = match storage.get_node(target_id)? {
                    Some(n) => n,
                    None => continue,
                };

                // Check if target matches pattern
                if !node_matches_pattern(&target_node, target_pattern) {
                    continue;
                }

                // Create new binding
                let mut new_binding = binding.clone().with_node(target_var, target_node);

                if let Some(rv) = rel_var {
                    new_binding = new_binding.with_edge(rv, edge.clone());
                }

                // Update path
                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(target_id);
                let mut new_path_edges = path_edges.clone();
                new_path_edges.push(edge.id);

                next_bindings.push((new_binding, new_path_nodes, new_path_edges));
            }
        }

        current_bindings = next_bindings;
    }

    // Convert to final bindings with optional path variable
    let mut bindings = Vec::new();
    for (mut binding, path_node_ids, path_edge_ids) in current_bindings {
        if let Some(pv) = path_var {
            // Fetch full node objects
            let mut path_nodes: Vec<Node> = Vec::new();
            for &nid in &path_node_ids {
                if let Some(node) = storage.get_node(nid)? {
                    path_nodes.push(node);
                }
            }
            // Fetch full edge objects
            let mut path_edges: Vec<Edge> = Vec::new();
            for &eid in &path_edge_ids {
                if let Some(edge) = storage.get_edge(eid)? {
                    path_edges.push(edge);
                }
            }
            let path = Path {
                nodes: path_nodes,
                edges: path_edges,
            };
            binding = binding.with_path(pv, path);
        }
        bindings.push(binding);
    }

    Ok(bindings)
}

/// Execute a variable-length relationship pattern match using BFS.
pub fn execute_variable_length_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    // Extract pattern components
    let (source_pattern, rel_pattern, target_pattern) = match (
        &pattern.elements[0],
        &pattern.elements[1],
        &pattern.elements[2],
    ) {
        (PatternElement::Node(s), PatternElement::Relationship(r), PatternElement::Node(t)) => {
            (s, r, t)
        }
        _ => return Err(Error::Cypher("Invalid variable-length pattern".into())),
    };

    let source_var = source_pattern.variable.as_deref().unwrap_or("_src");
    let target_var = target_pattern.variable.as_deref().unwrap_or("_tgt");
    let rel_var = rel_pattern.variable.as_deref();
    let path_var = pattern.path_variable.as_deref();

    // Get length constraints
    let length_spec = rel_pattern
        .length
        .as_ref()
        .ok_or_else(|| Error::Cypher("Variable-length pattern requires length spec".into()))?;

    let min_depth = length_spec.min.unwrap_or(1) as usize;
    let max_depth = length_spec.max.unwrap_or(DEFAULT_MAX_PATH_DEPTH as u32) as usize;

    // Scan source nodes
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;

    let mut bindings = Vec::new();

    // BFS from each source node
    for source_node in source_nodes {
        let mut queue: VecDeque<TraversalState> = VecDeque::new();
        // Track visited nodes per path to avoid cycles within a single path
        let mut found_targets: HashSet<i64> = HashSet::new();

        queue.push_back(TraversalState {
            node_id: source_node.id,
            path_nodes: vec![source_node.id],
            path_edges: vec![],
        });

        while let Some(state) = queue.pop_front() {
            let depth = state.path_edges.len();

            // If we've reached a valid depth, check if current node matches target pattern
            if depth >= min_depth && depth <= max_depth {
                if let Some(target_node) = storage.get_node(state.node_id)? {
                    if node_matches_pattern(&target_node, target_pattern) {
                        // Avoid duplicate results for same source-target pair
                        if !found_targets.contains(&state.node_id)
                            && state.node_id != source_node.id
                        {
                            found_targets.insert(state.node_id);

                            let mut binding = Binding::new()
                                .with_node(source_var, source_node.clone())
                                .with_node(target_var, target_node);

                            // Add path variable if requested
                            if let Some(pv) = path_var {
                                // Fetch full node objects for the path
                                let mut path_nodes_full: Vec<Node> = Vec::new();
                                for &nid in &state.path_nodes {
                                    if let Some(node) = storage.get_node(nid)? {
                                        path_nodes_full.push(node);
                                    }
                                }
                                binding = binding.with_path(
                                    pv,
                                    Path {
                                        nodes: path_nodes_full,
                                        edges: state.path_edges.clone(),
                                    },
                                );
                            }

                            // Add edge list if relationship variable is specified
                            if let Some(rv) = rel_var {
                                binding = binding.with_edge_list(rv, state.path_edges.clone());
                            }

                            bindings.push(binding);
                        }
                    }
                }
            }

            // Don't expand beyond max depth
            if depth >= max_depth {
                continue;
            }

            // Get edges from current node
            let edges = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(state.node_id)?,
                Direction::Incoming => storage.find_incoming_edges(state.node_id)?,
                Direction::Both => {
                    let mut edges = storage.find_outgoing_edges(state.node_id)?;
                    edges.extend(storage.find_incoming_edges(state.node_id)?);
                    edges
                }
            };

            // Filter by relationship type if specified
            let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
                edges
            } else {
                edges
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.edge_type))
                    .collect()
            };

            // Add neighbors to queue
            for edge in edges {
                let next_id = match rel_pattern.direction {
                    Direction::Outgoing => edge.target,
                    Direction::Incoming => edge.source,
                    Direction::Both => {
                        if edge.source == state.node_id {
                            edge.target
                        } else {
                            edge.source
                        }
                    }
                };

                // Avoid cycles within the same path
                if state.path_nodes.contains(&next_id) {
                    continue;
                }

                let mut new_path_nodes = state.path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_edges = state.path_edges.clone();
                new_path_edges.push(edge);

                queue.push_back(TraversalState {
                    node_id: next_id,
                    path_nodes: new_path_nodes,
                    path_edges: new_path_edges,
                });
            }
        }
    }

    Ok(bindings)
}

// =============================================================================
// Pattern Matching Helpers
// =============================================================================

/// Check if a node matches a node pattern (labels and properties).
pub fn node_matches_pattern(node: &Node, pattern: &NodePattern) -> bool {
    // Check labels: each label group is AND'd, alternatives within a group are OR'd
    for label_group in &pattern.labels {
        // Node must have at least one label from this group
        let has_any = label_group.iter().any(|label| node.has_label(label));
        if !has_any {
            return false;
        }
    }

    // Check properties
    if let Some(Expression::Map(entries)) = &pattern.properties {
        for (key, value) in entries {
            match node.get(key) {
                Some(node_value) => {
                    if !property_matches(node_value, value) {
                        return false;
                    }
                }
                None => {
                    if !matches!(value, Expression::Literal(Literal::Null)) {
                        return false;
                    }
                }
            }
        }
    }

    true
}

/// Filter edges by properties specified in the relationship pattern.
pub fn filter_edges_by_properties(
    edges: Vec<Edge>,
    pattern: &RelationshipPattern,
) -> Result<Vec<Edge>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(edges);
    };

    let required_props = match props_expr {
        Expression::Map(entries) => entries,
        _ => {
            return Err(Error::Cypher(
                "Relationship properties must be a map".into(),
            ))
        }
    };

    if required_props.is_empty() {
        return Ok(edges);
    }

    let filtered: Vec<Edge> = edges
        .into_iter()
        .filter(|edge| {
            required_props
                .iter()
                .all(|(key, value)| match edge.properties.get(key) {
                    Some(edge_value) => property_matches(edge_value, value),
                    None => matches!(value, Expression::Literal(Literal::Null)),
                })
        })
        .collect();

    Ok(filtered)
}

/// Scan nodes from storage based on the node pattern.
/// This implements the Node Scan and Label Filter operators.
pub fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - scan all nodes
        storage.scan_all_nodes()
    } else {
        // For efficiency, use first label from first group for initial scan
        // Then filter using full label expression
        let first_group = &pattern.labels[0];
        if first_group.is_empty() {
            return storage.scan_all_nodes();
        }

        // If first group has alternatives (OR), we need to scan for each and merge
        let mut all_nodes = Vec::new();
        let mut seen_ids = HashSet::new();

        for label in first_group {
            let nodes = storage.find_nodes_by_label(label)?;
            for node in nodes {
                if seen_ids.insert(node.id) {
                    all_nodes.push(node);
                }
            }
        }

        // Filter to match full label expression (handles AND of multiple groups)
        if pattern.labels.len() > 1 {
            all_nodes.retain(|node| {
                pattern
                    .labels
                    .iter()
                    .all(|group| group.iter().any(|label| node.has_label(label)))
            });
        }

        Ok(all_nodes)
    }
}

/// Scan nodes with optional SQL-level LIMIT pushdown.
/// Only applies limit when we have a simple label pattern (no OR, no AND of multiple groups).
pub fn scan_nodes_with_limit(
    pattern: &NodePattern,
    storage: &SqliteStorage,
    limit: Option<u64>,
) -> Result<Vec<Node>> {
    // Can only push limit for simple patterns
    let can_push_limit = limit.is_some()
        && pattern.properties.is_none()
        && (pattern.labels.is_empty()
            || (pattern.labels.len() == 1 && pattern.labels[0].len() == 1));

    if can_push_limit {
        let limit = limit.unwrap();
        if pattern.labels.is_empty() {
            storage.get_all_nodes_limit(Some(limit))
        } else {
            let label = &pattern.labels[0][0];
            storage.find_nodes_by_label_limit(label, Some(limit))
        }
    } else {
        // Fall back to regular scan (no limit pushdown)
        scan_nodes(pattern, storage)
    }
}

/// Filter nodes by properties specified in the pattern.
/// This implements the Property Filter operator.
pub fn filter_by_properties(nodes: Vec<Node>, pattern: &NodePattern) -> Result<Vec<Node>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(nodes);
    };

    // Properties should be a Map expression
    let required_props = match props_expr {
        Expression::Map(entries) => entries,
        _ => return Err(Error::Cypher("Pattern properties must be a map".into())),
    };

    if required_props.is_empty() {
        return Ok(nodes);
    }

    let filtered: Vec<Node> = nodes
        .into_iter()
        .filter(|node| {
            required_props.iter().all(|(key, value)| {
                match node.get(key) {
                    Some(node_value) => property_matches(node_value, value),
                    None => {
                        // Check if required value is null
                        matches!(value, Expression::Literal(Literal::Null))
                    }
                }
            })
        })
        .collect();

    Ok(filtered)
}

/// Check if a node's property value matches an expression.
fn property_matches(node_value: &PropertyValue, expr: &Expression) -> bool {
    match expr {
        Expression::Literal(lit) => literal_matches_property(lit, node_value),
        _ => false, // Complex expressions not supported in property matching
    }
}

/// Check if a literal matches a property value.
fn literal_matches_property(lit: &Literal, prop: &PropertyValue) -> bool {
    match (lit, prop) {
        (Literal::Null, _) => false, // NULL never equals anything
        (Literal::Boolean(a), PropertyValue::Bool(b)) => a == b,
        (Literal::Integer(a), PropertyValue::Integer(b)) => a == b,
        (Literal::Float(a), PropertyValue::Float(b)) => floats_equal(*a, *b),
        (Literal::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type comparisons
        (Literal::Integer(a), PropertyValue::Float(b)) => floats_equal(*a as f64, *b),
        (Literal::Float(a), PropertyValue::Integer(b)) => floats_equal(*a, *b as f64),
        _ => false,
    }
}
