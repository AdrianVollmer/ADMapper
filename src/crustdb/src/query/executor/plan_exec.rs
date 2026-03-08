//! Plan executor - interprets and executes query plans.
//!
//! This module takes a `QueryPlan` from the planner and executes it
//! against the storage backend, producing a `QueryResult`.

use super::{Binding, Path};
use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue, Relationship};
use crate::query::operators::{ExpandRequest, VariableLengthExpandRequest};
use crate::query::planner::{
    AggregateFunction, CreateNode, CreateRelationship, ExpandDirection, FilterPredicate, PlanExpr,
    PlanLiteral, PlanOperator, ProjectColumn, QueryPlan, SetOperation, TargetPropertyFilter,
};
use crate::query::{PathNode, PathRelationship, QueryResult, QueryStats, ResultValue, Row};
use crate::storage::{EntityCache, SqliteStorage};
use std::collections::{HashMap, HashSet, VecDeque};

// =============================================================================
// Cached Storage Access
// =============================================================================

/// Get a node, checking the cache first if available.
#[inline]
fn get_node_cached(
    id: i64,
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
) -> Result<Option<Node>> {
    if let Some(c) = cache {
        if let Some(node) = c.get_node(id) {
            return Ok(Some(node.clone()));
        }
        // Cache miss - fetch from storage and cache
        if let Some(node) = storage.get_node(id)? {
            c.insert_node(node.clone());
            return Ok(Some(node));
        }
        Ok(None)
    } else {
        storage.get_node(id)
    }
}

/// Get a relationship, checking the cache first if available.
#[inline]
fn get_relationship_cached(
    id: i64,
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
) -> Result<Option<Relationship>> {
    if let Some(c) = cache {
        if let Some(rel) = c.get_relationship(id) {
            return Ok(Some(rel.clone()));
        }
        // Cache miss - fetch from storage and cache
        if let Some(rel) = storage.get_relationship(id)? {
            c.insert_relationship(rel.clone());
            return Ok(Some(rel));
        }
        Ok(None)
    } else {
        storage.get_relationship(id)
    }
}

// =============================================================================
// Main Entry Point
// =============================================================================

/// Execute a query plan against storage with an optional entity cache.
pub fn execute_plan(
    plan: &QueryPlan,
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
) -> Result<QueryResult> {
    let mut stats = QueryStats::default();
    let start = std::time::Instant::now();

    // Execute the plan tree
    let execution_result = execute_operator(&plan.root, storage, &mut stats, cache)?;

    // Convert to QueryResult
    let result = match execution_result {
        ExecutionResult::Bindings(_bindings) => {
            // No RETURN clause - empty result
            QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                stats,
            }
        }
        ExecutionResult::Rows { columns, rows } => QueryResult {
            columns,
            rows,
            stats,
        },
    };

    let mut result = result;
    result.stats.execution_time_ms = start.elapsed().as_millis() as u64;
    Ok(result)
}

/// Internal execution result - either bindings (for intermediate steps) or final rows.
enum ExecutionResult {
    Bindings(Vec<Binding>),
    Rows {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}

// =============================================================================
// Operator Execution
// =============================================================================

fn execute_operator(
    op: &PlanOperator,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
    mut cache: Option<&mut EntityCache>,
) -> Result<ExecutionResult> {
    match op {
        PlanOperator::Empty => Ok(ExecutionResult::Bindings(Vec::new())),

        PlanOperator::ProduceRow => Ok(ExecutionResult::Bindings(vec![Binding::new()])),

        PlanOperator::NodeScan {
            variable,
            label_groups,
            limit,
            property_filter,
        } => execute_node_scan(
            variable,
            label_groups,
            *limit,
            property_filter.clone(),
            storage,
        ),

        PlanOperator::Expand {
            source,
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
        } => {
            let bindings =
                execute_operator_to_bindings(source, storage, stats, cache.as_deref_mut())?;
            let request = ExpandRequest {
                source_variable,
                rel_variable: rel_variable.as_deref(),
                target_variable,
                target_labels,
                path_variable: path_variable.as_deref(),
                types,
                direction: *direction,
            };
            execute_expand(bindings, &request, storage, cache)
        }

        PlanOperator::VariableLengthExpand {
            source,
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            target_ids,
            limit,
            target_property_filter,
        } => {
            let bindings =
                execute_operator_to_bindings(source, storage, stats, cache.as_deref_mut())?;
            let request = VariableLengthExpandRequest {
                source_variable,
                rel_variable: rel_variable.as_deref(),
                target_variable,
                target_labels,
                path_variable: path_variable.as_deref(),
                types,
                direction: *direction,
                min_hops: *min_hops,
                max_hops: *max_hops,
                target_ids: target_ids.as_deref(),
                limit: *limit,
                target_property_filter: target_property_filter.as_ref(),
            };
            execute_variable_length_expand(bindings, &request, storage, cache)
        }

        PlanOperator::ShortestPath {
            source,
            source_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            k,
            target_property_filter,
        } => {
            let bindings =
                execute_operator_to_bindings(source, storage, stats, cache.as_deref_mut())?;
            execute_shortest_path(
                bindings,
                source_variable,
                target_variable,
                target_labels,
                path_variable.as_deref(),
                types,
                *direction,
                *min_hops,
                *max_hops,
                *k,
                target_property_filter.clone(),
                storage,
                cache,
            )
        }

        PlanOperator::Filter { source, predicate } => {
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            let filtered = filter_bindings(bindings, predicate)?;
            Ok(ExecutionResult::Bindings(filtered))
        }

        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            execute_project(bindings, columns, *distinct, storage)
        }

        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            execute_aggregate(bindings, group_by, aggregates, storage)
        }

        PlanOperator::CountPushdown { label, alias } => {
            execute_count_pushdown(label.as_deref(), alias, storage)
        }

        PlanOperator::RelationshipTypesScan { alias } => {
            execute_relationship_types_scan(alias, storage)
        }

        PlanOperator::Limit { source, count } => {
            // Limit can work on either Bindings or Rows
            match execute_operator(source, storage, stats, cache)? {
                ExecutionResult::Bindings(mut bindings) => {
                    bindings.truncate(*count as usize);
                    Ok(ExecutionResult::Bindings(bindings))
                }
                ExecutionResult::Rows { columns, mut rows } => {
                    rows.truncate(*count as usize);
                    Ok(ExecutionResult::Rows { columns, rows })
                }
            }
        }

        PlanOperator::Skip { source, count } => {
            // Skip can work on either Bindings or Rows
            match execute_operator(source, storage, stats, cache)? {
                ExecutionResult::Bindings(bindings) => {
                    let skipped: Vec<_> = bindings.into_iter().skip(*count as usize).collect();
                    Ok(ExecutionResult::Bindings(skipped))
                }
                ExecutionResult::Rows { columns, rows } => {
                    let skipped: Vec<_> = rows.into_iter().skip(*count as usize).collect();
                    Ok(ExecutionResult::Rows {
                        columns,
                        rows: skipped,
                    })
                }
            }
        }

        PlanOperator::Create {
            source,
            nodes,
            relationships,
        } => execute_create(
            source.as_deref(),
            nodes,
            relationships,
            storage,
            stats,
            cache,
        ),

        PlanOperator::SetProperties { source, sets } => {
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            execute_set_properties(&bindings, sets, storage, stats)?;
            Ok(ExecutionResult::Bindings(bindings))
        }

        PlanOperator::Delete {
            source,
            variables,
            detach,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            execute_delete(&bindings, variables, *detach, storage, stats)?;
            Ok(ExecutionResult::Bindings(Vec::new()))
        }

        PlanOperator::Sort { source, keys: _ } => {
            // TODO: Implement sorting
            let bindings = execute_operator_to_bindings(source, storage, stats, cache)?;
            Ok(ExecutionResult::Bindings(bindings))
        }

        PlanOperator::RelationshipScan { .. } => {
            Err(Error::Cypher("RelationshipScan not implemented".into()))
        }
    }
}

/// Execute an operator and expect bindings (not final rows).
fn execute_operator_to_bindings(
    op: &PlanOperator,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
    cache: Option<&mut EntityCache>,
) -> Result<Vec<Binding>> {
    match execute_operator(op, storage, stats, cache)? {
        ExecutionResult::Bindings(b) => Ok(b),
        ExecutionResult::Rows { .. } => {
            // This shouldn't happen in a well-formed plan
            Err(Error::Internal("Expected bindings, got rows".into()))
        }
    }
}

// =============================================================================
// Scan Operators
// =============================================================================

fn execute_node_scan(
    variable: &str,
    label_groups: &[Vec<String>],
    limit: Option<u64>,
    property_filter: Option<(String, serde_json::Value)>,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    // label_groups structure:
    // - Each inner Vec is OR'd (alternatives)
    // - Outer Vec is AND'd (all groups must match)
    // Example: :Person|Company → [["Person", "Company"]]
    // Example: :Person:Actor|Director → [["Person"], ["Actor", "Director"]]

    // If we have a property filter, use indexed lookup (much faster)
    let nodes = if let Some((prop, value)) = property_filter {
        // Flatten labels for property lookup (handles simple single-label case)
        let flat_labels: Vec<String> = label_groups.iter().flatten().cloned().collect();
        storage.find_nodes_by_property(&prop, &value, &flat_labels, limit)?
    } else if label_groups.is_empty() || label_groups.iter().all(|g| g.is_empty()) {
        // No label filter - scan all nodes
        storage.get_all_nodes_limit(limit)?
    } else if label_groups.len() == 1 && label_groups[0].len() == 1 {
        // Simple single label case - use index
        storage.find_nodes_by_label_limit(&label_groups[0][0], limit)?
    } else if label_groups.len() == 1 && label_groups[0].len() > 1 {
        // Single group with OR alternatives (e.g., :Person|Company)
        // Scan for each label and merge, avoiding duplicates
        let mut seen_ids = std::collections::HashSet::new();
        let mut all_nodes = Vec::new();
        for label in &label_groups[0] {
            for node in storage.find_nodes_by_label(label)? {
                if seen_ids.insert(node.id) {
                    all_nodes.push(node);
                }
            }
        }
        if let Some(lim) = limit {
            all_nodes.truncate(lim as usize);
        }
        all_nodes
    } else {
        // Multiple groups - use first label from first group for initial scan, then filter
        let first_group = &label_groups[0];
        let first_label = first_group.first().map(String::as_str).unwrap_or("");

        let mut nodes = if first_label.is_empty() {
            storage.get_all_nodes_limit(None)?
        } else {
            storage.find_nodes_by_label(first_label)?
        };

        // Filter: for each group, node must have at least one matching label
        nodes.retain(|n: &crate::graph::Node| {
            label_groups.iter().all(|group| {
                // Node must have at least one label from this group
                group.is_empty() || group.iter().any(|label| n.has_label(label))
            })
        });

        if let Some(lim) = limit {
            nodes.truncate(lim as usize);
        }
        nodes
    };

    let bindings = nodes
        .into_iter()
        .map(|node| Binding::new().with_node(variable, node))
        .collect();

    Ok(ExecutionResult::Bindings(bindings))
}

// =============================================================================
// Expand Operators
// =============================================================================

fn execute_expand(
    bindings: Vec<Binding>,
    req: &ExpandRequest<'_>,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
) -> Result<ExecutionResult> {
    let mut result = Vec::new();

    for binding in bindings {
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
        }
    }

    Ok(ExecutionResult::Bindings(result))
}

/// Resolve a target property filter to matching nodes.
/// This enables early termination during BFS by pre-computing valid targets.
fn resolve_target_property_filter(
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

fn execute_variable_length_expand(
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
fn execute_shortest_path(
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

fn get_relationships(
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

fn filter_relationships_by_type(
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

fn get_target_id(relationship: &Relationship, from_id: i64, direction: ExpandDirection) -> i64 {
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

// =============================================================================
// Filter Operator
// =============================================================================

fn filter_bindings(bindings: Vec<Binding>, predicate: &FilterPredicate) -> Result<Vec<Binding>> {
    let mut result = Vec::new();
    for binding in bindings {
        if evaluate_predicate(predicate, &binding)? {
            result.push(binding);
        }
    }
    Ok(result)
}

fn evaluate_predicate(predicate: &FilterPredicate, binding: &Binding) -> Result<bool> {
    match predicate {
        FilterPredicate::True => Ok(true),

        FilterPredicate::Eq { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(values_equal(&l, &r))
        }

        FilterPredicate::Ne { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(!values_equal(&l, &r))
        }

        FilterPredicate::Lt { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(compare_values(&l, &r).map(|c| c < 0).unwrap_or(false))
        }

        FilterPredicate::Le { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(compare_values(&l, &r).map(|c| c <= 0).unwrap_or(false))
        }

        FilterPredicate::Gt { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(compare_values(&l, &r).map(|c| c > 0).unwrap_or(false))
        }

        FilterPredicate::Ge { left, right } => {
            let l = evaluate_expr(left, binding)?;
            let r = evaluate_expr(right, binding)?;
            Ok(compare_values(&l, &r).map(|c| c >= 0).unwrap_or(false))
        }

        FilterPredicate::And { left, right } => {
            Ok(evaluate_predicate(left, binding)? && evaluate_predicate(right, binding)?)
        }

        FilterPredicate::Or { left, right } => {
            Ok(evaluate_predicate(left, binding)? || evaluate_predicate(right, binding)?)
        }

        FilterPredicate::Not { inner } => Ok(!evaluate_predicate(inner, binding)?),

        FilterPredicate::IsNull { expr } => {
            let v = evaluate_expr(expr, binding)?;
            Ok(matches!(v, EvalValue::Null))
        }

        FilterPredicate::IsNotNull { expr } => {
            let v = evaluate_expr(expr, binding)?;
            Ok(!matches!(v, EvalValue::Null))
        }

        FilterPredicate::StartsWith { expr, prefix } => {
            let v = evaluate_expr(expr, binding)?;
            if let EvalValue::String(s) = v {
                Ok(s.starts_with(prefix))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::EndsWith { expr, suffix } => {
            let v = evaluate_expr(expr, binding)?;
            if let EvalValue::String(s) = v {
                Ok(s.ends_with(suffix))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::Contains { expr, substring } => {
            let v = evaluate_expr(expr, binding)?;
            if let EvalValue::String(s) = v {
                Ok(s.contains(substring))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::Regex { expr, pattern } => {
            let v = evaluate_expr(expr, binding)?;
            if let EvalValue::String(s) = v {
                let re = regex::Regex::new(pattern).map_err(|e| Error::Cypher(e.to_string()))?;
                Ok(re.is_match(&s))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::HasLabel { variable, label } => {
            if let Some(node) = binding.get_node(variable) {
                Ok(node.has_label(label))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::In { expr, list } => {
            let v = evaluate_expr(expr, binding)?;
            for item in list {
                let item_v = evaluate_expr(item, binding)?;
                if values_equal(&v, &item_v) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

// =============================================================================
// Expression Evaluation
// =============================================================================

#[derive(Debug, Clone)]
enum EvalValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<EvalValue>),
    Node(Node),
    Relationship(Relationship),
    Path(Path),
}

fn evaluate_expr(expr: &PlanExpr, binding: &Binding) -> Result<EvalValue> {
    match expr {
        PlanExpr::Literal(lit) => Ok(match lit {
            PlanLiteral::Null => EvalValue::Null,
            PlanLiteral::Bool(b) => EvalValue::Bool(*b),
            PlanLiteral::Int(i) => EvalValue::Int(*i),
            PlanLiteral::Float(f) => EvalValue::Float(*f),
            PlanLiteral::String(s) => EvalValue::String(s.clone()),
        }),

        PlanExpr::Variable(v) => {
            if let Some(node) = binding.get_node(v) {
                Ok(EvalValue::Node(node.clone()))
            } else if let Some(relationship) = binding.get_relationship(v) {
                Ok(EvalValue::Relationship(relationship.clone()))
            } else if let Some(path) = binding.get_path(v) {
                Ok(EvalValue::Path(path.clone()))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::Property { variable, property } => {
            if let Some(node) = binding.get_node(variable) {
                Ok(property_to_eval_value(node.properties.get(property)))
            } else if let Some(relationship) = binding.get_relationship(variable) {
                Ok(property_to_eval_value(
                    relationship.properties.get(property),
                ))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::PathLength { path_variable } => {
            if let Some(path) = binding.get_path(path_variable) {
                Ok(EvalValue::Int(path.relationships.len() as i64))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::Function { name, args } => {
            // Handle common functions
            let upper = name.to_uppercase();
            match upper.as_str() {
                "ID" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        match v {
                            EvalValue::Node(n) => Ok(EvalValue::Int(n.id)),
                            EvalValue::Relationship(e) => Ok(EvalValue::Int(e.id)),
                            _ => Ok(EvalValue::Null),
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TYPE" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::Relationship(e) = v {
                            Ok(EvalValue::String(e.rel_type))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "LABELS" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::Node(n) = v {
                            let labels: Vec<EvalValue> =
                                n.labels.into_iter().map(EvalValue::String).collect();
                            Ok(EvalValue::List(labels))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TOLOWER" | "LOWER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::String(s) = v {
                            Ok(EvalValue::String(s.to_lowercase()))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TOUPPER" | "UPPER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::String(s) = v {
                            Ok(EvalValue::String(s.to_uppercase()))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                _ => Ok(EvalValue::Null), // Unknown function
            }
        }
    }
}

fn property_to_eval_value(prop: Option<&PropertyValue>) -> EvalValue {
    match prop {
        None => EvalValue::Null,
        Some(PropertyValue::Null) => EvalValue::Null,
        Some(PropertyValue::Bool(b)) => EvalValue::Bool(*b),
        Some(PropertyValue::Integer(i)) => EvalValue::Int(*i),
        Some(PropertyValue::Float(f)) => EvalValue::Float(*f),
        Some(PropertyValue::String(s)) => EvalValue::String(s.clone()),
        Some(PropertyValue::List(items)) => {
            let values: Vec<EvalValue> = items
                .iter()
                .map(|p| property_to_eval_value(Some(p)))
                .collect();
            EvalValue::List(values)
        }
        Some(PropertyValue::Map(_)) => {
            // Maps are not currently supported as eval values
            EvalValue::Null
        }
    }
}

fn values_equal(a: &EvalValue, b: &EvalValue) -> bool {
    match (a, b) {
        (EvalValue::Null, EvalValue::Null) => false, // NULL != NULL in Cypher
        (EvalValue::Bool(x), EvalValue::Bool(y)) => x == y,
        (EvalValue::Int(x), EvalValue::Int(y)) => x == y,
        (EvalValue::Float(x), EvalValue::Float(y)) => (x - y).abs() < f64::EPSILON,
        (EvalValue::Int(x), EvalValue::Float(y)) | (EvalValue::Float(y), EvalValue::Int(x)) => {
            (*x as f64 - y).abs() < f64::EPSILON
        }
        (EvalValue::String(x), EvalValue::String(y)) => x == y,
        _ => false,
    }
}

fn compare_values(a: &EvalValue, b: &EvalValue) -> Option<i32> {
    match (a, b) {
        (EvalValue::Int(x), EvalValue::Int(y)) => Some(x.cmp(y) as i32),
        (EvalValue::Float(x), EvalValue::Float(y)) => {
            if x < y {
                Some(-1)
            } else if x > y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::Int(x), EvalValue::Float(y)) => {
            let xf = *x as f64;
            if xf < *y {
                Some(-1)
            } else if xf > *y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::Float(x), EvalValue::Int(y)) => {
            let yf = *y as f64;
            if *x < yf {
                Some(-1)
            } else if *x > yf {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::String(x), EvalValue::String(y)) => Some(x.cmp(y) as i32),
        _ => None,
    }
}

// =============================================================================
// Project and Aggregate Operators
// =============================================================================

fn execute_project(
    bindings: Vec<Binding>,
    columns: &[ProjectColumn],
    distinct: bool,
    _storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let column_names: Vec<String> = columns.iter().map(|c| c.alias.clone()).collect();
    let mut rows = Vec::new();

    for binding in bindings {
        let mut values = HashMap::new();
        for col in columns {
            let value = evaluate_expr(&col.expr, &binding)?;
            values.insert(col.alias.clone(), eval_to_result_value(value));
        }
        rows.push(Row { values });
    }

    if distinct {
        // Simple deduplication based on string representation
        let mut seen = HashSet::new();
        let mut unique_rows = Vec::new();
        for row in rows {
            let key = format!("{:?}", row.values);
            if seen.insert(key) {
                unique_rows.push(row);
            }
        }
        rows = unique_rows;
    }

    Ok(ExecutionResult::Rows {
        columns: column_names,
        rows,
    })
}

fn execute_aggregate(
    bindings: Vec<Binding>,
    group_by: &[ProjectColumn],
    aggregates: &[crate::query::planner::AggregateColumn],
    _storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    // If no GROUP BY, treat all rows as one group
    if group_by.is_empty() {
        let mut values = HashMap::new();

        for agg in aggregates {
            let result = compute_aggregate(&agg.function, &bindings)?;
            values.insert(agg.alias.clone(), result);
        }

        let columns: Vec<String> = aggregates.iter().map(|a| a.alias.clone()).collect();
        return Ok(ExecutionResult::Rows {
            columns,
            rows: vec![Row { values }],
        });
    }

    // Group by implementation
    let mut groups: HashMap<String, Vec<Binding>> = HashMap::new();

    for binding in bindings {
        let mut key_parts = Vec::new();
        for col in group_by {
            let v = evaluate_expr(&col.expr, &binding)?;
            key_parts.push(format!("{:?}", v));
        }
        let key = key_parts.join("|");
        groups.entry(key).or_default().push(binding);
    }

    let mut columns: Vec<String> = group_by.iter().map(|c| c.alias.clone()).collect();
    columns.extend(aggregates.iter().map(|a| a.alias.clone()));

    let mut rows = Vec::new();
    for (_, group_bindings) in groups {
        let first = &group_bindings[0];
        let mut values = HashMap::new();

        // Add GROUP BY columns
        for col in group_by {
            let v = evaluate_expr(&col.expr, first)?;
            values.insert(col.alias.clone(), eval_to_result_value(v));
        }

        // Add aggregates
        for agg in aggregates {
            let result = compute_aggregate(&agg.function, &group_bindings)?;
            values.insert(agg.alias.clone(), result);
        }

        rows.push(Row { values });
    }

    Ok(ExecutionResult::Rows { columns, rows })
}

fn compute_aggregate(func: &AggregateFunction, bindings: &[Binding]) -> Result<ResultValue> {
    match func {
        AggregateFunction::Count(arg) => {
            let count = if let Some(expr) = arg {
                bindings
                    .iter()
                    .filter(|b| !matches!(evaluate_expr(expr, b), Ok(EvalValue::Null)))
                    .count()
            } else {
                bindings.len()
            };
            Ok(ResultValue::Property(PropertyValue::Integer(count as i64)))
        }

        AggregateFunction::Sum(expr) => {
            let mut sum = 0.0;
            let mut is_int = true;
            for b in bindings {
                match evaluate_expr(expr, b)? {
                    EvalValue::Int(i) => sum += i as f64,
                    EvalValue::Float(f) => {
                        sum += f;
                        is_int = false;
                    }
                    _ => {}
                }
            }
            if is_int {
                Ok(ResultValue::Property(PropertyValue::Integer(sum as i64)))
            } else {
                Ok(ResultValue::Property(PropertyValue::Float(sum)))
            }
        }

        AggregateFunction::Avg(expr) => {
            let mut sum = 0.0;
            let mut count = 0;
            for b in bindings {
                match evaluate_expr(expr, b)? {
                    EvalValue::Int(i) => {
                        sum += i as f64;
                        count += 1;
                    }
                    EvalValue::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Ok(ResultValue::Property(PropertyValue::Float(
                    sum / count as f64,
                )))
            } else {
                Ok(ResultValue::Property(PropertyValue::Null))
            }
        }

        AggregateFunction::Min(expr) => {
            let mut min: Option<EvalValue> = None;
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, EvalValue::Null) {
                    min = Some(match min {
                        None => v,
                        Some(m) => {
                            if compare_values(&v, &m).map(|c| c < 0).unwrap_or(false) {
                                v
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            Ok(eval_to_result_value(min.unwrap_or(EvalValue::Null)))
        }

        AggregateFunction::Max(expr) => {
            let mut max: Option<EvalValue> = None;
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, EvalValue::Null) {
                    max = Some(match max {
                        None => v,
                        Some(m) => {
                            if compare_values(&v, &m).map(|c| c > 0).unwrap_or(false) {
                                v
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            Ok(eval_to_result_value(max.unwrap_or(EvalValue::Null)))
        }

        AggregateFunction::Collect(expr) => {
            let mut items = Vec::new();
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, EvalValue::Null) {
                    items.push(eval_to_property_value(v));
                }
            }
            Ok(ResultValue::Property(PropertyValue::List(items)))
        }
    }
}

fn execute_count_pushdown(
    label: Option<&str>,
    alias: &str,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let count = if let Some(l) = label {
        storage.count_nodes_by_label(l)?
    } else {
        storage.count_nodes()?
    };

    let mut values = HashMap::new();
    values.insert(
        alias.to_string(),
        ResultValue::Property(PropertyValue::Integer(count as i64)),
    );

    Ok(ExecutionResult::Rows {
        columns: vec![alias.to_string()],
        rows: vec![Row { values }],
    })
}

/// Execute relationship types scan - returns all distinct relationship types.
///
/// This is O(distinct_types) instead of O(relationships) because it queries the
/// normalized rel_types table directly rather than scanning all relationships.
fn execute_relationship_types_scan(
    alias: &str,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let rel_types = storage.get_all_relationship_types()?;

    let rows: Vec<Row> = rel_types
        .into_iter()
        .map(|type_name| {
            let mut values = HashMap::new();
            values.insert(
                alias.to_string(),
                ResultValue::Property(PropertyValue::String(type_name)),
            );
            Row { values }
        })
        .collect();

    Ok(ExecutionResult::Rows {
        columns: vec![alias.to_string()],
        rows,
    })
}

// =============================================================================
// Mutation Operators
// =============================================================================

fn execute_create(
    source: Option<&PlanOperator>,
    nodes: &[CreateNode],
    relationships: &[CreateRelationship],
    storage: &SqliteStorage,
    stats: &mut QueryStats,
    _cache: Option<&mut EntityCache>,
) -> Result<ExecutionResult> {
    // Build nodes first, tracking variable -> id mapping
    let mut var_to_id: HashMap<String, i64> = HashMap::new();

    for create_node in nodes {
        let props = plan_properties_to_json(&create_node.properties)?;
        let node_id = storage.insert_node(&create_node.labels, &props)?;

        stats.nodes_created += 1;
        stats.labels_added += create_node.labels.len();
        stats.properties_set += create_node.properties.len();

        if let Some(ref var) = create_node.variable {
            var_to_id.insert(var.clone(), node_id);
        }
    }

    // Create relationships using variable name lookup
    for create_rel in relationships {
        let source_id = var_to_id.get(&create_rel.source).ok_or_else(|| {
            Error::Cypher(format!("Unknown source variable: {}", create_rel.source))
        })?;
        let target_id = var_to_id.get(&create_rel.target).ok_or_else(|| {
            Error::Cypher(format!("Unknown target variable: {}", create_rel.target))
        })?;
        let props = plan_properties_to_json(&create_rel.properties)?;

        storage.insert_relationship(*source_id, *target_id, &create_rel.rel_type, &props)?;
        stats.relationships_created += 1;
        stats.properties_set += create_rel.properties.len();
    }

    // Return empty result for CREATE
    if source.is_some() {
        // If there's a source (MATCH ... CREATE), pass through
        Ok(ExecutionResult::Bindings(Vec::new()))
    } else {
        Ok(ExecutionResult::Bindings(Vec::new()))
    }
}

fn execute_set_properties(
    bindings: &[Binding],
    sets: &[SetOperation],
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for set_op in sets {
            match set_op {
                SetOperation::Property {
                    variable,
                    property,
                    value,
                } => {
                    if let Some(node) = binding.get_node(variable) {
                        let val = evaluate_expr(value, binding)?;
                        let prop_val = eval_to_property_value(val);
                        storage.update_node_property(node.id, property, &prop_val)?;
                        stats.properties_set += 1;
                    }
                }
                SetOperation::AddLabel { variable, label } => {
                    if let Some(node) = binding.get_node(variable) {
                        storage.add_node_label(node.id, label)?;
                        stats.labels_added += 1;
                    }
                }
                SetOperation::RemoveLabel { .. } => {
                    // Not implemented yet
                }
            }
        }
    }
    Ok(())
}

fn execute_delete(
    bindings: &[Binding],
    variables: &[String],
    detach: bool,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for var in variables {
            if let Some(node) = binding.get_node(var) {
                // Check for relationships if not DETACH DELETE
                if !detach && storage.has_relationships(node.id)? {
                    return Err(Error::Cypher(
                        "Cannot delete node with relationships. Use DETACH DELETE.".into(),
                    ));
                }
                storage.delete_node(node.id)?;
                stats.nodes_deleted += 1;
            } else if let Some(relationship) = binding.get_relationship(var) {
                storage.delete_relationship(relationship.id)?;
                stats.relationships_deleted += 1;
            }
        }
    }
    Ok(())
}

// =============================================================================
// Value Conversion Utilities
// =============================================================================

fn eval_to_result_value(v: EvalValue) -> ResultValue {
    match v {
        EvalValue::Null => ResultValue::Property(PropertyValue::Null),
        EvalValue::Bool(b) => ResultValue::Property(PropertyValue::Bool(b)),
        EvalValue::Int(i) => ResultValue::Property(PropertyValue::Integer(i)),
        EvalValue::Float(f) => ResultValue::Property(PropertyValue::Float(f)),
        EvalValue::String(s) => ResultValue::Property(PropertyValue::String(s)),
        EvalValue::List(items) => {
            let props: Vec<PropertyValue> = items.into_iter().map(eval_to_property_value).collect();
            ResultValue::Property(PropertyValue::List(props))
        }
        EvalValue::Node(n) => ResultValue::Node {
            id: n.id,
            labels: n.labels,
            properties: n.properties,
        },
        EvalValue::Relationship(e) => ResultValue::Relationship {
            id: e.id,
            source: e.source,
            target: e.target,
            rel_type: e.rel_type,
            properties: e.properties,
        },
        EvalValue::Path(p) => ResultValue::Path {
            nodes: p
                .nodes
                .into_iter()
                .map(|n| PathNode {
                    id: n.id,
                    labels: n.labels,
                    properties: n.properties,
                })
                .collect(),
            relationships: p
                .relationships
                .into_iter()
                .map(|e| PathRelationship {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    rel_type: e.rel_type,
                    properties: e.properties,
                })
                .collect(),
        },
    }
}

fn eval_to_property_value(v: EvalValue) -> PropertyValue {
    match v {
        EvalValue::Null => PropertyValue::Null,
        EvalValue::Bool(b) => PropertyValue::Bool(b),
        EvalValue::Int(i) => PropertyValue::Integer(i),
        EvalValue::Float(f) => PropertyValue::Float(f),
        EvalValue::String(s) => PropertyValue::String(s),
        EvalValue::List(items) => {
            PropertyValue::List(items.into_iter().map(eval_to_property_value).collect())
        }
        // Nodes/relationships/paths can't be converted to property values
        _ => PropertyValue::Null,
    }
}

fn plan_properties_to_json(props: &[(String, PlanExpr)]) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, expr) in props {
        let value = match expr {
            PlanExpr::Literal(lit) => match lit {
                PlanLiteral::Null => serde_json::Value::Null,
                PlanLiteral::Bool(b) => serde_json::Value::Bool(*b),
                PlanLiteral::Int(i) => serde_json::Value::Number((*i).into()),
                PlanLiteral::Float(f) => serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                PlanLiteral::String(s) => serde_json::Value::String(s.clone()),
            },
            _ => {
                return Err(Error::Cypher(
                    "Only literal values supported in CREATE properties".into(),
                ))
            }
        };
        map.insert(key.clone(), value);
    }
    Ok(serde_json::Value::Object(map))
}
