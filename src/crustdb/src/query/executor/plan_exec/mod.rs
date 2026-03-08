//! Plan executor - interprets and executes query plans.
//!
//! This module takes a `QueryPlan` from the planner and executes it
//! against the storage backend, producing a `QueryResult`.

mod convert;
mod eval;
mod expand;
mod filter;
mod mutate;
mod project;
mod scan;

use convert::*;
use eval::*;
use expand::*;
use filter::*;
use mutate::*;
use project::*;
use scan::*;

use super::{Binding, Path};
use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query::operators::{ExpandRequest, VariableLengthExpandRequest};
use crate::query::planner::{PlanOperator, QueryPlan};
use crate::query::{QueryResult, QueryStats, Row};
use crate::storage::{EntityCache, SqliteStorage};

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

        PlanOperator::Expand(ref p) => {
            let bindings =
                execute_operator_to_bindings(&p.source, storage, stats, cache.as_deref_mut())?;
            let request = ExpandRequest {
                source_variable: &p.source_variable,
                rel_variable: p.rel_variable.as_deref(),
                target_variable: &p.target_variable,
                target_labels: &p.target_labels,
                path_variable: p.path_variable.as_deref(),
                types: &p.types,
                direction: p.direction,
                limit: p.limit,
            };
            execute_expand(bindings, &request, storage, cache)
        }

        PlanOperator::VariableLengthExpand(ref p) => {
            let bindings =
                execute_operator_to_bindings(&p.source, storage, stats, cache.as_deref_mut())?;
            let request = VariableLengthExpandRequest {
                source_variable: &p.source_variable,
                rel_variable: p.rel_variable.as_deref(),
                target_variable: &p.target_variable,
                target_labels: &p.target_labels,
                path_variable: p.path_variable.as_deref(),
                types: &p.types,
                direction: p.direction,
                min_hops: p.min_hops,
                max_hops: p.max_hops,
                target_ids: p.target_ids.as_deref(),
                limit: p.limit,
                target_property_filter: p.target_property_filter.as_ref(),
            };
            execute_variable_length_expand(bindings, &request, storage, cache)
        }

        PlanOperator::ShortestPath(ref p) => {
            let bindings =
                execute_operator_to_bindings(&p.source, storage, stats, cache.as_deref_mut())?;
            execute_shortest_path(
                bindings,
                &p.source_variable,
                &p.target_variable,
                &p.target_labels,
                p.path_variable.as_deref(),
                &p.types,
                p.direction,
                p.min_hops,
                p.max_hops,
                p.k,
                p.target_property_filter.clone(),
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

        PlanOperator::RelationshipCountPushdown { rel_type, alias } => {
            execute_relationship_count_pushdown(rel_type.as_deref(), alias, storage)
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

        PlanOperator::CrossJoin { left, right } => {
            let left_bindings =
                execute_operator_to_bindings(left, storage, stats, cache.as_deref_mut())?;
            let right_bindings = execute_operator_to_bindings(right, storage, stats, cache)?;
            let mut result = Vec::with_capacity(left_bindings.len() * right_bindings.len());
            for lb in &left_bindings {
                for rb in &right_bindings {
                    result.push(lb.merge(rb));
                }
            }
            Ok(ExecutionResult::Bindings(result))
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
