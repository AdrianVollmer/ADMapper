//! Pattern matching execution for MATCH queries.

use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue, Relationship};
use crate::query::parser::{
    Direction, Expression, Literal, NodePattern, Pattern, PatternElement, RelationshipPattern,
};
use crate::storage::SqliteStorage;
use std::collections::{HashMap, HashSet, VecDeque};

use super::create::literal_to_json;
use super::eval::floats_equal;
use super::{Binding, Path, PathConstraints};

// =============================================================================
// Configuration Constants
// =============================================================================

/// Default maximum path length for unbounded traversals.
///
/// Used when queries specify open-ended patterns like `(a)-[:REL*]->(b)` or
/// `(a)-[:REL*1..]->(b)` without an explicit upper bound. This prevents infinite
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
    path_edges: Vec<Relationship>,
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
    if pattern.elements.len().is_multiple_of(2) {
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

/// Check if pattern is a shortest path pattern.
///
/// Note: In openCypher 9, shortest paths are expressed using shortestPath() and
/// allShortestPaths() functions, which are parsed as Expression variants. This
/// function checks if a pattern has variable-length relationships that would be
/// suitable for shortest path queries.
pub fn is_shortest_path_pattern(pattern: &Pattern) -> bool {
    // A pattern is suitable for shortest path if it has a variable-length relationship
    for elem in &pattern.elements {
        if let PatternElement::Relationship(rel) = elem {
            if rel.length.is_some() {
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
///
/// The `all_paths` parameter controls whether to return just the single shortest path
/// (shortestPath) or all paths of the shortest length (allShortestPaths).
pub fn execute_shortest_path_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
    constraints: &PathConstraints,
    all_paths: bool,
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

    // Determine min/max hops from variable-length spec
    let (min_hops, max_hops) = if let Some(ref len) = rel_pattern.length {
        (
            len.min.unwrap_or(1) as usize,
            len.max.unwrap_or(DEFAULT_MAX_PATH_DEPTH as u32) as usize,
        )
    } else {
        (1, DEFAULT_MAX_PATH_DEPTH) // Default: one or more
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
    // shortestPath returns 1 path, allShortestPaths returns all paths of shortest length
    let k = if all_paths { usize::MAX } else { 1 };

    // Optimization: for shortestPath with specific target, use simple BFS with visited set
    // This is O(V+E) instead of exponential in the number of paths
    let use_fast_bfs = !all_paths && target_ids.len() == 1;

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
            #[allow(clippy::collapsible_if)]
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
            let relationships = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(state.node_id)?,
                Direction::Incoming => storage.find_incoming_edges(state.node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_edges(state.node_id)?;
                    relationships.extend(storage.find_incoming_edges(state.node_id)?);
                    relationships
                }
            };

            // Filter relationships by type if specified
            let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
                relationships
            } else {
                relationships
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.rel_type))
                    .collect()
            };

            for relationship in relationships {
                // Determine the next node
                let next_node_id = match rel_pattern.direction {
                    Direction::Outgoing => relationship.target,
                    Direction::Incoming => relationship.source,
                    Direction::Both => {
                        if relationship.source == state.node_id {
                            relationship.target
                        } else {
                            relationship.source
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
                new_path_edges.push(relationship.id);

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
            // Fetch full relationship objects for path
            let mut path_edges: Vec<Relationship> = Vec::new();
            for &eid in &result.path_edges {
                if let Some(relationship) = storage.get_edge(eid)? {
                    path_edges.push(relationship);
                }
            }
            binding = binding.with_path(
                pv,
                Path {
                    nodes: path_nodes,
                    relationships: path_edges,
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
/// Check if a node pattern has filters (labels or properties).
/// Used to determine selectivity for query optimization.
fn pattern_has_filters(pattern: &NodePattern) -> bool {
    !pattern.labels.is_empty() || pattern.properties.is_some()
}

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

    // Optimization: flip traversal direction when target is more selective than source.
    // This avoids scanning all nodes when only the target has filters.
    // For example: MATCH (a)-[r]->(b {id: '...'}) should start from b and find incoming relationships.
    let source_has_filters = pattern_has_filters(source_pattern);
    let target_has_filters = pattern_has_filters(target_pattern);
    let should_flip = !source_has_filters && target_has_filters;

    if should_flip {
        return execute_single_hop_pattern_flipped(
            source_pattern,
            rel_pattern,
            target_pattern,
            source_var,
            rel_var,
            target_var,
            path_var,
            storage,
        );
    }

    // Scan source nodes
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;

    let mut bindings = Vec::new();

    // For each source node, find matching relationships and targets
    for source_node in source_nodes {
        let relationships = match rel_pattern.direction {
            Direction::Outgoing => storage.find_outgoing_edges(source_node.id)?,
            Direction::Incoming => storage.find_incoming_edges(source_node.id)?,
            Direction::Both => {
                let mut relationships = storage.find_outgoing_edges(source_node.id)?;
                relationships.extend(storage.find_incoming_edges(source_node.id)?);
                relationships
            }
        };

        // Filter relationships by type if specified
        let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
            relationships
        } else {
            relationships
                .into_iter()
                .filter(|e| rel_pattern.types.contains(&e.rel_type))
                .collect()
        };

        // Filter relationships by properties if specified
        let relationships = filter_edges_by_properties(relationships, rel_pattern)?;

        // For each matching relationship, get the target node and check if it matches
        for relationship in relationships {
            // Determine the actual target node based on direction
            let target_id = match rel_pattern.direction {
                Direction::Outgoing => relationship.target,
                Direction::Incoming => relationship.source,
                Direction::Both => {
                    if relationship.source == source_node.id {
                        relationship.target
                    } else {
                        relationship.source
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
                binding = binding.with_edge(rv, relationship.clone());
            }

            // Add path variable if specified
            if let Some(pv) = path_var {
                let path = Path {
                    nodes: vec![source_node.clone(), target_node.clone()],
                    relationships: vec![relationship.clone()],
                };
                binding = binding.with_path(pv, path);
            }

            bindings.push(binding);
        }
    }

    Ok(bindings)
}

/// Execute single-hop pattern with flipped traversal direction.
/// Called when the target node is more selective than the source node.
/// Instead of scanning all source nodes, we scan the (filtered) target nodes
/// and traverse relationships in the reverse direction.
#[allow(clippy::too_many_arguments)]
fn execute_single_hop_pattern_flipped(
    source_pattern: &NodePattern,
    rel_pattern: &RelationshipPattern,
    target_pattern: &NodePattern,
    source_var: &str,
    rel_var: Option<&str>,
    target_var: &str,
    path_var: Option<&str>,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    // Scan the MORE selective target nodes first
    let target_nodes = scan_nodes(target_pattern, storage)?;
    let target_nodes = filter_by_properties(target_nodes, target_pattern)?;

    let mut bindings = Vec::new();

    // For each target node, find relationships in the REVERSE direction
    for target_node in target_nodes {
        // Flip the relationship lookup direction:
        // - Original Outgoing (a)->(b): now find incoming relationships to b
        // - Original Incoming (a)<-(b): now find outgoing relationships from b
        // - Original Both: still need both directions
        let relationships = match rel_pattern.direction {
            Direction::Outgoing => storage.find_incoming_edges(target_node.id)?,
            Direction::Incoming => storage.find_outgoing_edges(target_node.id)?,
            Direction::Both => {
                let mut relationships = storage.find_outgoing_edges(target_node.id)?;
                relationships.extend(storage.find_incoming_edges(target_node.id)?);
                relationships
            }
        };

        // Filter relationships by type if specified
        let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
            relationships
        } else {
            relationships
                .into_iter()
                .filter(|e| rel_pattern.types.contains(&e.rel_type))
                .collect()
        };

        // Filter relationships by properties if specified
        let relationships = filter_edges_by_properties(relationships, rel_pattern)?;

        // For each matching relationship, get the source node (we're traversing backwards)
        for relationship in relationships {
            // Determine the actual source node based on original direction
            // (reversed from the normal case since we're traversing backwards)
            let source_id = match rel_pattern.direction {
                Direction::Outgoing => relationship.source, // In flipped: relationship points TO target, so source is relationship.source
                Direction::Incoming => relationship.target, // In flipped: relationship points FROM target, so source is relationship.target
                Direction::Both => {
                    if relationship.target == target_node.id {
                        relationship.source
                    } else {
                        relationship.target
                    }
                }
            };

            // Get the source node
            let source_node = match storage.get_node(source_id)? {
                Some(n) => n,
                None => continue,
            };

            // Check if source node matches the source pattern
            // (source_pattern may still have label filters even if no properties)
            if !node_matches_pattern(&source_node, source_pattern) {
                continue;
            }

            // Create binding for this match (same variable names as original)
            let mut binding = Binding::new()
                .with_node(source_var, source_node.clone())
                .with_node(target_var, target_node.clone());

            if let Some(rv) = rel_var {
                binding = binding.with_edge(rv, relationship.clone());
            }

            // Add path variable if specified
            if let Some(pv) = path_var {
                let path = Path {
                    nodes: vec![source_node.clone(), target_node.clone()],
                    relationships: vec![relationship.clone()],
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

            // Find relationships from the last node
            let relationships = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(last_node_id)?,
                Direction::Incoming => storage.find_incoming_edges(last_node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_edges(last_node_id)?;
                    relationships.extend(storage.find_incoming_edges(last_node_id)?);
                    relationships
                }
            };

            // Filter relationships by type
            let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
                relationships
            } else {
                relationships
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.rel_type))
                    .collect()
            };

            // Filter relationships by properties
            let relationships = filter_edges_by_properties(relationships, rel_pattern)?;

            for relationship in relationships {
                // Determine the target node
                let target_id = match rel_pattern.direction {
                    Direction::Outgoing => relationship.target,
                    Direction::Incoming => relationship.source,
                    Direction::Both => {
                        if relationship.source == last_node_id {
                            relationship.target
                        } else {
                            relationship.source
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
                    new_binding = new_binding.with_edge(rv, relationship.clone());
                }

                // Update path
                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(target_id);
                let mut new_path_edges = path_edges.clone();
                new_path_edges.push(relationship.id);

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
            // Fetch full relationship objects
            let mut path_edges: Vec<Relationship> = Vec::new();
            for &eid in &path_edge_ids {
                if let Some(relationship) = storage.get_edge(eid)? {
                    path_edges.push(relationship);
                }
            }
            let path = Path {
                nodes: path_nodes,
                relationships: path_edges,
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
                                        relationships: state.path_edges.clone(),
                                    },
                                );
                            }

                            // Add relationship list if relationship variable is specified
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

            // Get relationships from current node
            let relationships = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(state.node_id)?,
                Direction::Incoming => storage.find_incoming_edges(state.node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_edges(state.node_id)?;
                    relationships.extend(storage.find_incoming_edges(state.node_id)?);
                    relationships
                }
            };

            // Filter by relationship type if specified
            let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
                relationships
            } else {
                relationships
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.rel_type))
                    .collect()
            };

            // Add neighbors to queue
            for relationship in relationships {
                let next_id = match rel_pattern.direction {
                    Direction::Outgoing => relationship.target,
                    Direction::Incoming => relationship.source,
                    Direction::Both => {
                        if relationship.source == state.node_id {
                            relationship.target
                        } else {
                            relationship.source
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
                new_path_edges.push(relationship);

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

/// Filter relationships by properties specified in the relationship pattern.
pub fn filter_edges_by_properties(
    relationships: Vec<Relationship>,
    pattern: &RelationshipPattern,
) -> Result<Vec<Relationship>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(relationships);
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
        return Ok(relationships);
    }

    let filtered: Vec<Relationship> = relationships
        .into_iter()
        .filter(|relationship| {
            required_props
                .iter()
                .all(|(key, value)| match relationship.properties.get(key) {
                    Some(edge_value) => property_matches(edge_value, value),
                    None => matches!(value, Expression::Literal(Literal::Null)),
                })
        })
        .collect();

    Ok(filtered)
}

/// Scan nodes from storage based on the node pattern.
/// This implements the Node Scan and Label Filter operators.
///
/// When the pattern has no labels but has properties with an available index,
/// uses index lookup instead of full scan for O(1) vs O(N) performance.
pub fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - try property index lookup first
        if let Some(Expression::Map(props)) = &pattern.properties {
            // Try to find an indexed property we can use
            for (key, value) in props {
                if let Ok(true) = storage.has_property_index(key) {
                    if let Expression::Literal(lit) = value {
                        let json_value = literal_to_json(lit)?;
                        // Use property index lookup
                        // Other properties will be filtered by filter_by_properties() later
                        return storage.find_nodes_by_property(key, &json_value, &[], None);
                    }
                }
            }
        }
        // No indexed property found - fall back to full scan
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
/// Only applies limit when we have a simple label pattern (no OR, no AND of multiple groups),
/// or when using an indexed property lookup.
pub fn scan_nodes_with_limit(
    pattern: &NodePattern,
    storage: &SqliteStorage,
    limit: Option<u64>,
) -> Result<Vec<Node>> {
    // Try property index lookup with limit if no labels but has indexed property
    if pattern.labels.is_empty() {
        if let Some(Expression::Map(props)) = &pattern.properties {
            for (key, value) in props {
                if let Ok(true) = storage.has_property_index(key) {
                    if let Expression::Literal(lit) = value {
                        let json_value = literal_to_json(lit)?;
                        // Use property index lookup with limit
                        return storage.find_nodes_by_property(key, &json_value, &[], limit);
                    }
                }
            }
        }
    }

    // Can only push limit for simple patterns without properties
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
