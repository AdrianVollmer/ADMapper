use super::{get_node_cached, get_relationship_cached, Binding, ExecutionResult, Path};
use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query::operators::{ExpandRequest, VariableLengthExpandRequest};
use crate::query::planner::{ExpandDirection, TargetPropertyFilter};
use crate::storage::{EntityCache, SqliteStorage};
use std::collections::{HashSet, VecDeque};

pub(super) fn execute_expand(
    bindings: Vec<Binding>,
    req: &ExpandRequest<'_>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
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
) -> Result<ExecutionResult> {
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

        queue.push_back((source_node.id, vec![source_node.id], Vec::new()));
        visited.insert(source_node.id);

        'bfs: while let Some((current_id, path_nodes, path_rels)) = queue.pop_front() {
            let depth = path_rels.len() as u32;

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
            }
        }
    }

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
    target_property_filter: Option<(String, serde_json::Value)>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
) -> Result<ExecutionResult> {
    let mut result = Vec::new();

    // If we have a specific target property filter, look up the target node directly
    // This enables early termination when we find this specific node
    let specific_target_id: Option<i64> =
        if let Some((ref prop, ref value)) = target_property_filter {
            // Look up node(s) matching the property filter
            let nodes = storage.find_nodes_by_property(prop, value, target_labels, Some(1))?;
            nodes.first().map(|n| n.id)
        } else {
            None
        };

    // Pre-scan target nodes if we have label constraints (and no specific target)
    let target_ids: Option<HashSet<i64>> =
        if specific_target_id.is_none() && !target_labels.is_empty() {
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

    for binding in bindings {
        let source_node = binding
            .get_node(source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", source_variable)))?;

        // BFS for shortest paths
        let mut visited: HashSet<i64> = HashSet::new();
        let mut found_paths: Vec<(Node, Vec<i64>, Vec<i64>)> = Vec::new();
        let mut found_specific_target = false;

        let mut queue: VecDeque<(i64, Vec<i64>, Vec<i64>)> = VecDeque::new();
        queue.push_back((source_node.id, vec![source_node.id], Vec::new()));
        visited.insert(source_node.id);

        while let Some((current_id, path_nodes, path_rel_ids)) = queue.pop_front() {
            let depth = path_rel_ids.len() as u32;

            // Stop BFS at max depth
            if depth > max_hops {
                continue;
            }

            // Early termination: if we found the specific target, stop exploring
            if found_specific_target {
                break;
            }

            // Check if we reached a valid target
            if depth >= min_hops && current_id != source_node.id {
                let is_target = if let Some(specific_id) = specific_target_id {
                    // We have a specific target - check if this is it
                    current_id == specific_id
                } else {
                    // Check against label-based target set
                    target_ids
                        .as_ref()
                        .map(|ids| ids.contains(&current_id))
                        .unwrap_or(true)
                };

                if is_target {
                    if let Some(target_node) =
                        get_node_cached(current_id, storage, cache.as_deref_mut())?
                    {
                        found_paths.push((target_node, path_nodes.clone(), path_rel_ids.clone()));
                        // If we have a specific target, we found it - can terminate early
                        if specific_target_id.is_some() {
                            found_specific_target = true;
                        }
                    }
                }
            }

            // Don't expand if we already found our specific target
            if found_specific_target {
                break;
            }

            // Expand
            let relationships = get_relationships(current_id, direction, storage)?;
            let relationships = filter_relationships_by_type(relationships, types);

            for relationship in relationships {
                let next_id = get_target_id(&relationship, current_id, direction);

                // Use global visited set for efficiency when we only need shortest paths
                // (all paths at shortest depth are equally valid)
                if visited.contains(&next_id) {
                    continue;
                }
                visited.insert(next_id);

                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_rel_ids = path_rel_ids.clone();
                new_path_rel_ids.push(relationship.id);

                queue.push_back((next_id, new_path_nodes, new_path_rel_ids));
            }
        }

        // Convert found paths to bindings
        for (target_node, path_node_ids, path_rel_ids) in found_paths {
            let mut new_binding = binding.clone().with_node(target_variable, target_node);

            if let Some(pv) = path_variable {
                let mut nodes = Vec::new();
                for &nid in &path_node_ids {
                    if let Some(n) = get_node_cached(nid, storage, cache.as_deref_mut())? {
                        nodes.push(n);
                    }
                }
                let mut relationships = Vec::new();
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
        }
    }

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
