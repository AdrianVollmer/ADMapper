//! Shortest path pattern execution using BFS.

use crate::error::{Error, Result};
use crate::graph::Node;
use crate::query::parser::{Direction, Pattern, PatternElement};
use crate::storage::SqliteStorage;
use std::collections::{HashMap, HashSet, VecDeque};

use super::super::{Binding, Path, PathConstraints};
use super::matching::{filter_by_properties, scan_nodes};
use super::DEFAULT_MAX_PATH_DEPTH;

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
        path_rels: Vec<i64>,
    }

    // For collecting shortest paths
    #[derive(Debug)]
    struct PathResult {
        length: usize,
        path_nodes: Vec<i64>,
        path_rels: Vec<i64>,
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
            path_rels: vec![],
        });
        visited.insert(source_node.id);

        // BFS level by level to ensure shortest paths first
        while let Some(state) = queue.pop_front() {
            let current_depth = state.path_rels.len();

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
                            path_rels: state.path_rels.clone(),
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
                Direction::Outgoing => storage.find_outgoing_relationships(state.node_id)?,
                Direction::Incoming => storage.find_incoming_relationships(state.node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_relationships(state.node_id)?;
                    relationships.extend(storage.find_incoming_relationships(state.node_id)?);
                    relationships
                }
            };

            // Filter relationships by type if specified
            let relationships: Vec<_> = if rel_pattern.types.is_empty() {
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

                let mut new_path_rels = state.path_rels.clone();
                new_path_rels.push(relationship.id);

                queue.push_back(PathState {
                    node_id: next_node_id,
                    path_nodes: new_path_nodes,
                    path_rels: new_path_rels,
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
            let mut path_rels: Vec<_> = Vec::new();
            for &eid in &result.path_rels {
                if let Some(relationship) = storage.get_relationship(eid)? {
                    path_rels.push(relationship);
                }
            }
            binding = binding.with_path(
                pv,
                Path {
                    nodes: path_nodes,
                    relationships: path_rels,
                },
            );
        }

        bindings.push(binding);
    }

    Ok(bindings)
}
