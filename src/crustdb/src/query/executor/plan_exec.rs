//! Plan executor - interprets and executes query plans.
//!
//! This module takes a `QueryPlan` from the planner and executes it
//! against the storage backend, producing a `QueryResult`.

use super::{Binding, Path};
use crate::error::{Error, Result};
use crate::graph::{Edge, Node, PropertyValue};
use crate::query::planner::{
    AggregateFunction, CreateEdge, CreateNode, ExpandDirection, FilterPredicate, PlanExpr,
    PlanLiteral, PlanOperator, ProjectColumn, QueryPlan, SetOperation,
};
use crate::query::{PathEdge, PathNode, QueryResult, QueryStats, ResultValue, Row};
use crate::storage::SqliteStorage;
use std::collections::{HashMap, HashSet, VecDeque};

// =============================================================================
// Main Entry Point
// =============================================================================

/// Execute a query plan against storage.
pub fn execute_plan(plan: &QueryPlan, storage: &SqliteStorage) -> Result<QueryResult> {
    let mut stats = QueryStats::default();
    let start = std::time::Instant::now();

    // Execute the plan tree
    let execution_result = execute_operator(&plan.root, storage, &mut stats)?;

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
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_expand(
                bindings,
                source_variable,
                rel_variable.as_deref(),
                target_variable,
                target_labels,
                path_variable.as_deref(),
                types,
                *direction,
                storage,
            )
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
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_variable_length_expand(
                bindings,
                source_variable,
                rel_variable.as_deref(),
                target_variable,
                target_labels,
                path_variable.as_deref(),
                types,
                *direction,
                *min_hops,
                *max_hops,
                storage,
            )
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
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
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
            )
        }

        PlanOperator::Filter { source, predicate } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            let filtered = filter_bindings(bindings, predicate)?;
            Ok(ExecutionResult::Bindings(filtered))
        }

        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_project(bindings, columns, *distinct, storage)
        }

        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_aggregate(bindings, group_by, aggregates, storage)
        }

        PlanOperator::CountPushdown { label, alias } => {
            execute_count_pushdown(label.as_deref(), alias, storage)
        }

        PlanOperator::Limit { source, count } => {
            // Limit can work on either Bindings or Rows
            match execute_operator(source, storage, stats)? {
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
            match execute_operator(source, storage, stats)? {
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
            edges,
        } => execute_create(source.as_deref(), nodes, edges, storage, stats),

        PlanOperator::SetProperties { source, sets } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_set_properties(&bindings, sets, storage, stats)?;
            Ok(ExecutionResult::Bindings(bindings))
        }

        PlanOperator::Delete {
            source,
            variables,
            detach,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            execute_delete(&bindings, variables, *detach, storage, stats)?;
            Ok(ExecutionResult::Bindings(Vec::new()))
        }

        PlanOperator::Sort { source, keys: _ } => {
            // TODO: Implement sorting
            let bindings = execute_operator_to_bindings(source, storage, stats)?;
            Ok(ExecutionResult::Bindings(bindings))
        }

        PlanOperator::EdgeScan { .. } => Err(Error::Cypher("EdgeScan not implemented".into())),
    }
}

/// Execute an operator and expect bindings (not final rows).
fn execute_operator_to_bindings(
    op: &PlanOperator,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<Vec<Binding>> {
    match execute_operator(op, storage, stats)? {
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

#[allow(clippy::too_many_arguments)]
fn execute_expand(
    bindings: Vec<Binding>,
    source_variable: &str,
    rel_variable: Option<&str>,
    target_variable: &str,
    target_labels: &[String],
    path_variable: Option<&str>,
    types: &[String],
    direction: ExpandDirection,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let mut result = Vec::new();

    for binding in bindings {
        let source_node = binding
            .get_node(source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", source_variable)))?;

        let edges = get_edges(source_node.id, direction, storage)?;
        let edges = filter_edges_by_type(edges, types);

        for edge in edges {
            let target_id = get_target_id(&edge, source_node.id, direction);
            let target_node = match storage.get_node(target_id)? {
                Some(n) => n,
                None => continue,
            };

            // Check target labels
            if !target_labels.is_empty() && !target_labels.iter().any(|l| target_node.has_label(l))
            {
                continue;
            }

            let mut new_binding = binding
                .clone()
                .with_node(target_variable, target_node.clone());

            if let Some(rv) = rel_variable {
                new_binding = new_binding.with_edge(rv, edge.clone());
            }

            // Bind the path if path_variable is set
            if let Some(pv) = path_variable {
                new_binding = new_binding.with_path(
                    pv,
                    Path {
                        nodes: vec![source_node.clone(), target_node],
                        edges: vec![edge],
                    },
                );
            }

            result.push(new_binding);
        }
    }

    Ok(ExecutionResult::Bindings(result))
}

#[allow(clippy::too_many_arguments)]
fn execute_variable_length_expand(
    bindings: Vec<Binding>,
    source_variable: &str,
    rel_variable: Option<&str>,
    target_variable: &str,
    target_labels: &[String],
    path_variable: Option<&str>,
    types: &[String],
    direction: ExpandDirection,
    min_hops: u32,
    max_hops: u32,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let mut result = Vec::new();

    for binding in bindings {
        let source_node = binding
            .get_node(source_variable)
            .ok_or_else(|| Error::Cypher(format!("Variable {} not bound", source_variable)))?;

        // BFS traversal with global visited set to prevent exponential explosion.
        // Without this, dense graphs would explore the same node via every possible path,
        // leading to O(edges^depth) complexity instead of O(V+E).
        let mut queue: VecDeque<(i64, Vec<i64>, Vec<Edge>)> = VecDeque::new();
        let mut visited: HashSet<i64> = HashSet::new();

        queue.push_back((source_node.id, vec![source_node.id], Vec::new()));
        visited.insert(source_node.id);

        while let Some((current_id, path_nodes, path_edges)) = queue.pop_front() {
            let depth = path_edges.len() as u32;

            // Check if we've reached a valid target
            if depth >= min_hops && depth <= max_hops && current_id != source_node.id {
                if let Some(target_node) = storage.get_node(current_id)? {
                    // Check target labels
                    let matches_labels = target_labels.is_empty()
                        || target_labels.iter().any(|l| target_node.has_label(l));

                    if matches_labels {
                        let mut new_binding =
                            binding.clone().with_node(target_variable, target_node);

                        if let Some(pv) = path_variable {
                            // Build full path
                            let mut nodes = Vec::new();
                            for &nid in &path_nodes {
                                if let Some(n) = storage.get_node(nid)? {
                                    nodes.push(n);
                                }
                            }
                            new_binding = new_binding.with_path(
                                pv,
                                Path {
                                    nodes,
                                    edges: path_edges.clone(),
                                },
                            );
                        }

                        if let Some(rv) = rel_variable {
                            new_binding = new_binding.with_edge_list(rv, path_edges.clone());
                        }

                        result.push(new_binding);
                    }
                }
            }

            // Don't expand beyond max depth
            if depth >= max_hops {
                continue;
            }

            // Expand to neighbors
            let edges = get_edges(current_id, direction, storage)?;
            let edges = filter_edges_by_type(edges, types);

            for edge in edges {
                let next_id = get_target_id(&edge, current_id, direction);

                // Skip already visited nodes (global deduplication)
                if visited.contains(&next_id) {
                    continue;
                }
                visited.insert(next_id);

                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_edges = path_edges.clone();
                new_path_edges.push(edge);

                queue.push_back((next_id, new_path_nodes, new_path_edges));
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

        while let Some((current_id, path_nodes, path_edge_ids)) = queue.pop_front() {
            let depth = path_edge_ids.len() as u32;

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
                    if let Some(target_node) = storage.get_node(current_id)? {
                        found_paths.push((target_node, path_nodes.clone(), path_edge_ids.clone()));
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
            let edges = get_edges(current_id, direction, storage)?;
            let edges = filter_edges_by_type(edges, types);

            for edge in edges {
                let next_id = get_target_id(&edge, current_id, direction);

                // Use global visited set for efficiency when we only need shortest paths
                // (all paths at shortest depth are equally valid)
                if visited.contains(&next_id) {
                    continue;
                }
                visited.insert(next_id);

                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_edge_ids = path_edge_ids.clone();
                new_path_edge_ids.push(edge.id);

                queue.push_back((next_id, new_path_nodes, new_path_edge_ids));
            }
        }

        // Convert found paths to bindings
        for (target_node, path_node_ids, path_edge_ids) in found_paths {
            let mut new_binding = binding.clone().with_node(target_variable, target_node);

            if let Some(pv) = path_variable {
                let mut nodes = Vec::new();
                for &nid in &path_node_ids {
                    if let Some(n) = storage.get_node(nid)? {
                        nodes.push(n);
                    }
                }
                let mut edges = Vec::new();
                for &eid in &path_edge_ids {
                    if let Some(e) = storage.get_edge(eid)? {
                        edges.push(e);
                    }
                }
                new_binding = new_binding.with_path(pv, Path { nodes, edges });
            }

            result.push(new_binding);
        }
    }

    Ok(ExecutionResult::Bindings(result))
}

// =============================================================================
// Helper Functions for Traversal
// =============================================================================

fn get_edges(
    node_id: i64,
    direction: ExpandDirection,
    storage: &SqliteStorage,
) -> Result<Vec<Edge>> {
    match direction {
        ExpandDirection::Outgoing => storage.find_outgoing_edges(node_id),
        ExpandDirection::Incoming => storage.find_incoming_edges(node_id),
        ExpandDirection::Both => {
            let mut edges = storage.find_outgoing_edges(node_id)?;
            edges.extend(storage.find_incoming_edges(node_id)?);
            Ok(edges)
        }
    }
}

fn filter_edges_by_type(edges: Vec<Edge>, types: &[String]) -> Vec<Edge> {
    if types.is_empty() {
        edges
    } else {
        edges
            .into_iter()
            .filter(|e| types.contains(&e.edge_type))
            .collect()
    }
}

fn get_target_id(edge: &Edge, from_id: i64, direction: ExpandDirection) -> i64 {
    match direction {
        ExpandDirection::Outgoing => edge.target,
        ExpandDirection::Incoming => edge.source,
        ExpandDirection::Both => {
            if edge.source == from_id {
                edge.target
            } else {
                edge.source
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
    Edge(Edge),
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
            } else if let Some(edge) = binding.get_edge(v) {
                Ok(EvalValue::Edge(edge.clone()))
            } else if let Some(path) = binding.get_path(v) {
                Ok(EvalValue::Path(path.clone()))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::Property { variable, property } => {
            if let Some(node) = binding.get_node(variable) {
                Ok(property_to_eval_value(node.properties.get(property)))
            } else if let Some(edge) = binding.get_edge(variable) {
                Ok(property_to_eval_value(edge.properties.get(property)))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::PathLength { path_variable } => {
            if let Some(path) = binding.get_path(path_variable) {
                Ok(EvalValue::Int(path.edges.len() as i64))
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
                            EvalValue::Edge(e) => Ok(EvalValue::Int(e.id)),
                            _ => Ok(EvalValue::Null),
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TYPE" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::Edge(e) = v {
                            Ok(EvalValue::String(e.edge_type))
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

// =============================================================================
// Mutation Operators
// =============================================================================

fn execute_create(
    source: Option<&PlanOperator>,
    nodes: &[CreateNode],
    edges: &[CreateEdge],
    storage: &SqliteStorage,
    stats: &mut QueryStats,
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

    // Create edges using variable name lookup
    for create_edge in edges {
        let source_id = var_to_id.get(&create_edge.source).ok_or_else(|| {
            Error::Cypher(format!("Unknown source variable: {}", create_edge.source))
        })?;
        let target_id = var_to_id.get(&create_edge.target).ok_or_else(|| {
            Error::Cypher(format!("Unknown target variable: {}", create_edge.target))
        })?;
        let props = plan_properties_to_json(&create_edge.properties)?;

        storage.insert_edge(*source_id, *target_id, &create_edge.edge_type, &props)?;
        stats.relationships_created += 1;
        stats.properties_set += create_edge.properties.len();
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
                // Check for edges if not DETACH DELETE
                if !detach && storage.has_edges(node.id)? {
                    return Err(Error::Cypher(
                        "Cannot delete node with relationships. Use DETACH DELETE.".into(),
                    ));
                }
                storage.delete_node(node.id)?;
                stats.nodes_deleted += 1;
            } else if let Some(edge) = binding.get_edge(var) {
                storage.delete_edge(edge.id)?;
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
        EvalValue::Edge(e) => ResultValue::Edge {
            id: e.id,
            source: e.source,
            target: e.target,
            edge_type: e.edge_type,
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
            edges: p
                .edges
                .into_iter()
                .map(|e| PathEdge {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    edge_type: e.edge_type,
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
        // Nodes/edges/paths can't be converted to property values
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
