use super::{
    get_node_cached, get_relationship_cached, Binding, ExecutionContext, ExecutionResult, Path,
};
use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query::operators::{ExpandRequest, VariableLengthExpandRequest};
use crate::query::planner::{ExpandDirection, TargetPropertyFilter};
use crate::storage::{EntityCache, SqliteStorage};
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::trace;

pub(super) fn execute_expand(
    bindings: Vec<Binding>,
    req: &ExpandRequest<'_>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
    ctx: &mut ExecutionContext,
) -> Result<ExecutionResult> {
    let mut result = Vec::new();
    let limit = req.limit.map(|l| l as usize);

    'outer: for binding in bindings {
        let source_node = binding
            .get_node(req.source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", req.source_variable)))?;

        let relationships = get_relationships(source_node.id, req.direction, storage)?;
        let relationships = filter_relationships_by_type(relationships, req.types);

        for relationship in relationships {
            let target_id = get_target_id(&relationship, source_node.id, req.direction);
            let target_node = match get_node_cached(target_id, storage, cache.as_deref_mut())? {
                Some(n) => n,
                None => continue,
            };

            // Check target labels
            if !req.target_labels.is_empty()
                && !req.target_labels.iter().any(|l| target_node.has_label(l))
            {
                continue;
            }

            let mut new_binding = binding
                .clone()
                .with_node(req.target_variable, target_node.clone());

            if let Some(rv) = req.rel_variable {
                new_binding = new_binding.with_relationship(rv, relationship.clone());
            }

            // Bind the path if path_variable is set
            if let Some(pv) = req.path_variable {
                new_binding = new_binding.with_path(
                    pv,
                    Path {
                        nodes: vec![source_node.clone(), target_node],
                        relationships: vec![relationship],
                    },
                );
            }

            result.push(new_binding);
            ctx.track_bindings(1)?;

            // Early termination when limit is reached
            if let Some(lim) = limit {
                if result.len() >= lim {
                    break 'outer;
                }
            }
        }
    }

    Ok(ExecutionResult::Bindings(result))
}

/// Resolve a target property filter to matching nodes.
/// This enables early termination during BFS by pre-computing valid targets.
pub(super) fn resolve_target_property_filter(
    filter: &TargetPropertyFilter,
    target_labels: &[String],
    storage: &SqliteStorage,
) -> Result<Vec<Node>> {
    match filter {
        TargetPropertyFilter::Eq { property, value } => {
            storage.find_nodes_by_property(property, value, target_labels, None)
        }
        TargetPropertyFilter::EndsWith { property, suffix } => {
            storage.find_nodes_by_property_suffix(property, suffix, target_labels)
        }
        TargetPropertyFilter::StartsWith { property, prefix } => {
            storage.find_nodes_by_property_prefix(property, prefix, target_labels)
        }
        TargetPropertyFilter::Contains {
            property,
            substring,
        } => storage.find_nodes_by_property_contains(property, substring, target_labels),
    }
}

pub(super) fn execute_variable_length_expand(
    bindings: Vec<Binding>,
    req: &VariableLengthExpandRequest<'_>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
    ctx: &mut ExecutionContext,
) -> Result<ExecutionResult> {
    trace!(
        source = req.source_variable,
        target = req.target_variable,
        min_hops = req.min_hops,
        max_hops = req.max_hops,
        types = ?req.types,
        direction = ?req.direction,
        bindings = bindings.len(),
        "variable_length_expand: starting"
    );

    let mut result = Vec::new();

    // Resolve target_property_filter to node IDs for early termination
    let filter_resolved_ids: Option<HashSet<i64>> = if let Some(filter) = req.target_property_filter
    {
        let nodes = resolve_target_property_filter(filter, req.target_labels, storage)?;
        if nodes.is_empty() {
            // No matching targets exist - can return early
            return Ok(ExecutionResult::Bindings(result));
        }
        Some(nodes.into_iter().map(|n| n.id).collect())
    } else {
        None
    };

    // Combine explicit target_ids with filter-resolved IDs
    let target_id_set: Option<HashSet<i64>> = match (req.target_ids, &filter_resolved_ids) {
        (Some(ids), Some(filter_ids)) => {
            // Intersection: node must be in both sets
            let explicit: HashSet<i64> = ids.iter().copied().collect();
            Some(explicit.intersection(filter_ids).copied().collect())
        }
        (Some(ids), None) => Some(ids.iter().copied().collect()),
        (None, Some(filter_ids)) => Some(filter_ids.clone()),
        (None, None) => None,
    };

    let limit = req.limit.map(|l| l as usize);

    for binding in bindings {
        // Early termination: check if we've reached the limit
        if let Some(lim) = limit {
            if result.len() >= lim {
                break;
            }
        }

        let source_node = binding
            .get_node(req.source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", req.source_variable)))?;

        // BFS traversal with global visited set to prevent exponential explosion.
        // Without this, dense graphs would explore the same node via every possible path,
        // leading to O(relationships^depth) complexity instead of O(V+E).
        let mut queue: VecDeque<(i64, Vec<i64>, Vec<Relationship>)> = VecDeque::new();
        let mut visited: HashSet<i64> = HashSet::new();
        let mut prev_depth: u32 = 0;

        queue.push_back((source_node.id, vec![source_node.id], Vec::new()));
        visited.insert(source_node.id);

        trace!(
            source_id = source_node.id,
            "variable_length_expand: starting BFS from source"
        );

        'bfs: while let Some((current_id, path_nodes, path_rels)) = queue.pop_front() {
            let depth = path_rels.len() as u32;

            if depth > prev_depth {
                trace!(
                    depth,
                    queue_len = queue.len(),
                    visited = visited.len(),
                    results = result.len(),
                    "variable_length_expand: advancing to next depth"
                );
                prev_depth = depth;
            }

            // Early termination: check if we've reached the limit
            if let Some(lim) = limit {
                if result.len() >= lim {
                    break 'bfs;
                }
            }

            // Check if we've reached a valid target
            if depth >= req.min_hops && depth <= req.max_hops && current_id != source_node.id {
                // If target_ids is set, only consider nodes in that set
                let matches_target_ids = match &target_id_set {
                    Some(ids) => ids.contains(&current_id),
                    None => true,
                };

                if matches_target_ids {
                    if let Some(target_node) =
                        get_node_cached(current_id, storage, cache.as_deref_mut())?
                    {
                        // Check target labels
                        let matches_labels = req.target_labels.is_empty()
                            || req.target_labels.iter().any(|l| target_node.has_label(l));

                        if matches_labels {
                            let mut new_binding =
                                binding.clone().with_node(req.target_variable, target_node);

                            if let Some(pv) = req.path_variable {
                                // Build full path
                                let mut nodes = Vec::new();
                                for &nid in &path_nodes {
                                    if let Some(n) =
                                        get_node_cached(nid, storage, cache.as_deref_mut())?
                                    {
                                        nodes.push(n);
                                    }
                                }
                                new_binding = new_binding.with_path(
                                    pv,
                                    Path {
                                        nodes,
                                        relationships: path_rels.clone(),
                                    },
                                );
                            }

                            if let Some(rv) = req.rel_variable {
                                new_binding =
                                    new_binding.with_relationship_list(rv, path_rels.clone());
                            }

                            result.push(new_binding);
                            ctx.track_bindings(1)?;

                            // Early termination after finding a match if limit is 1
                            if let Some(lim) = limit {
                                if result.len() >= lim {
                                    break 'bfs;
                                }
                            }
                        }
                    }
                }
            }

            // Don't expand beyond max depth
            if depth >= req.max_hops {
                continue;
            }

            // Expand to neighbors
            let relationships = get_relationships(current_id, req.direction, storage)?;
            let relationships = filter_relationships_by_type(relationships, req.types);

            for relationship in relationships {
                let next_id = get_target_id(&relationship, current_id, req.direction);

                // Skip already visited nodes (global deduplication)
                if visited.contains(&next_id) {
                    continue;
                }
                visited.insert(next_id);

                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_rels = path_rels.clone();
                new_path_rels.push(relationship);

                queue.push_back((next_id, new_path_nodes, new_path_rels));
                ctx.check_frontier(queue.len())?;
            }
        }

        trace!(
            source_id = source_node.id,
            visited = visited.len(),
            results = result.len(),
            "variable_length_expand: finished BFS from source"
        );
    }

    trace!(
        total_results = result.len(),
        "variable_length_expand: complete"
    );

    Ok(ExecutionResult::Bindings(result))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_shortest_path(
    bindings: Vec<Binding>,
    source_variable: &str,
    target_variable: &str,
    target_labels: &[String],
    path_variable: Option<&str>,
    types: &[String],
    direction: ExpandDirection,
    min_hops: u32,
    max_hops: u32,
    _k: u32,
    target_property_filter: Option<&TargetPropertyFilter>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
    ctx: &mut ExecutionContext,
) -> Result<ExecutionResult> {
    trace!(
        source = source_variable,
        target = target_variable,
        min_hops,
        max_hops,
        types = ?types,
        direction = ?direction,
        bindings = bindings.len(),
        target_labels = ?target_labels,
        has_target_filter = target_property_filter.is_some(),
        "shortest_path: starting"
    );

    let mut result = Vec::new();

    // Resolve target property filter to matching node IDs for early termination.
    // Supports Eq, EndsWith, StartsWith, Contains via SQL pushdown.
    let filter_resolved_ids: Option<HashSet<i64>> = if let Some(filter) = target_property_filter {
        let nodes = resolve_target_property_filter(filter, target_labels, storage)?;
        trace!(
            resolved_targets = nodes.len(),
            "shortest_path: resolved target filter to node IDs"
        );
        if nodes.is_empty() {
            return Ok(ExecutionResult::Bindings(result));
        }
        Some(nodes.into_iter().map(|n| n.id).collect())
    } else {
        None
    };

    // Pre-scan target nodes if we have label constraints (and no filter-resolved IDs)
    let target_ids: Option<HashSet<i64>> =
        if filter_resolved_ids.is_none() && !target_labels.is_empty() {
            let mut ids = HashSet::new();
            for label in target_labels {
                for node in storage.find_nodes_by_label(label)? {
                    ids.insert(node.id);
                }
            }
            Some(ids)
        } else {
            None
        };

    if let Some(ref ids) = target_ids {
        trace!(
            candidate_targets = ids.len(),
            "shortest_path: pre-scanned target nodes by label"
        );
    }

    for binding in bindings {
        let source_node = binding
            .get_node(source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", source_variable)))?;

        // BFS using parent pointers instead of cloning path vectors.
        // Each queue entry is just (node_id, depth) — O(1) per entry.
        // The parent map records how we reached each node for path reconstruction.
        let mut parent: HashMap<i64, (i64, i64)> = HashMap::new(); // child -> (parent, rel_id)
        let mut visited: HashSet<i64> = HashSet::new();
        let mut found_targets: Vec<i64> = Vec::new();
        let mut prev_depth: u32 = 0;

        let mut queue: VecDeque<(i64, u32)> = VecDeque::new();
        queue.push_back((source_node.id, 0));
        visited.insert(source_node.id);

        trace!(
            source_id = source_node.id,
            "shortest_path: starting BFS from source"
        );

        while let Some((current_id, depth)) = queue.pop_front() {
            if depth > max_hops {
                continue;
            }

            if depth > prev_depth {
                trace!(
                    depth,
                    queue_len = queue.len(),
                    visited = visited.len(),
                    found = found_targets.len(),
                    "shortest_path: advancing to next depth"
                );
                prev_depth = depth;
            }

            // Check if we reached a valid target
            if depth >= min_hops && current_id != source_node.id {
                let is_target = if let Some(ref ids) = filter_resolved_ids {
                    ids.contains(&current_id)
                } else if let Some(ref ids) = target_ids {
                    ids.contains(&current_id)
                } else {
                    true
                };

                if is_target {
                    found_targets.push(current_id);
                    // Early termination: if filter resolves to exactly 1 target
                    if filter_resolved_ids
                        .as_ref()
                        .is_some_and(|ids| ids.len() == 1)
                    {
                        break;
                    }
                }
            }

            // Expand
            let relationships = get_relationships(current_id, direction, storage)?;
            let relationships = filter_relationships_by_type(relationships, types);

            for relationship in relationships {
                let next_id = get_target_id(&relationship, current_id, direction);

                if visited.contains(&next_id) {
                    continue;
                }
                visited.insert(next_id);
                parent.insert(next_id, (current_id, relationship.id));

                queue.push_back((next_id, depth + 1));
                ctx.check_frontier(queue.len())?;
            }
        }

        trace!(
            source_id = source_node.id,
            visited = visited.len(),
            found = found_targets.len(),
            final_queue_len = queue.len(),
            parent_map_len = parent.len(),
            "shortest_path: BFS complete from source"
        );

        // Reconstruct paths from parent pointers and convert to bindings
        for target_id in found_targets {
            let target_node = match get_node_cached(target_id, storage, cache.as_deref_mut())? {
                Some(n) => n,
                None => continue,
            };
            let mut new_binding = binding.clone().with_node(target_variable, target_node);

            if let Some(pv) = path_variable {
                let (path_node_ids, path_rel_ids) =
                    reconstruct_path(source_node.id, target_id, &parent);

                let mut nodes = Vec::with_capacity(path_node_ids.len());
                for &nid in &path_node_ids {
                    if let Some(n) = get_node_cached(nid, storage, cache.as_deref_mut())? {
                        nodes.push(n);
                    }
                }
                let mut relationships = Vec::with_capacity(path_rel_ids.len());
                for &eid in &path_rel_ids {
                    if let Some(e) = get_relationship_cached(eid, storage, cache.as_deref_mut())? {
                        relationships.push(e);
                    }
                }
                new_binding = new_binding.with_path(
                    pv,
                    Path {
                        nodes,
                        relationships,
                    },
                );
            }

            result.push(new_binding);
            ctx.track_bindings(1)?;
        }
    }

    trace!(total_results = result.len(), "shortest_path: complete");

    Ok(ExecutionResult::Bindings(result))
}

// =============================================================================
// Helper Functions for Traversal
// =============================================================================

pub(super) fn get_relationships(
    node_id: i64,
    direction: ExpandDirection,
    storage: &SqliteStorage,
) -> Result<Vec<Relationship>> {
    match direction {
        ExpandDirection::Outgoing => storage.find_outgoing_relationships(node_id),
        ExpandDirection::Incoming => storage.find_incoming_relationships(node_id),
        ExpandDirection::Both => {
            let mut relationships = storage.find_outgoing_relationships(node_id)?;
            relationships.extend(storage.find_incoming_relationships(node_id)?);
            Ok(relationships)
        }
    }
}

pub(super) fn filter_relationships_by_type(
    relationships: Vec<Relationship>,
    types: &[String],
) -> Vec<Relationship> {
    if types.is_empty() {
        relationships
    } else {
        relationships
            .into_iter()
            .filter(|e| types.contains(&e.rel_type))
            .collect()
    }
}

pub(super) fn get_target_id(
    relationship: &Relationship,
    from_id: i64,
    direction: ExpandDirection,
) -> i64 {
    match direction {
        ExpandDirection::Outgoing => relationship.target,
        ExpandDirection::Incoming => relationship.source,
        ExpandDirection::Both => {
            if relationship.source == from_id {
                relationship.target
            } else {
                relationship.source
            }
        }
    }
}

/// Reconstruct a path from parent pointers.
///
/// Walks backward from `target` to `source` using the parent map,
/// then reverses to get source-to-target order.
/// Returns (node_ids, relationship_ids) for the path.
fn reconstruct_path(
    source: i64,
    target: i64,
    parent: &HashMap<i64, (i64, i64)>,
) -> (Vec<i64>, Vec<i64>) {
    let mut node_ids = vec![target];
    let mut rel_ids = Vec::new();
    let mut current = target;
    while current != source {
        if let Some(&(parent_id, rel_id)) = parent.get(&current) {
            rel_ids.push(rel_id);
            node_ids.push(parent_id);
            current = parent_id;
        } else {
            break;
        }
    }
    node_ids.reverse();
    rel_ids.reverse();
    (node_ids, rel_ids)
}
