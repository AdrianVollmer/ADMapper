//! Plan executor - interprets and executes query plans.
//!
//! This module takes a `QueryPlan` from the planner and executes it
//! against the storage backend, producing a `QueryResult`.

mod convert;
pub(crate) mod eval;
mod expand;
pub(crate) mod filter;
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
use tracing::warn;

use super::{Binding, Path};
use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query::operators::{ExpandRequest, VariableLengthExpandRequest};
use crate::query::planner::{PlanOperator, QueryPlan};
use crate::query::{QueryResult, QueryStats, Row};
use crate::storage::{EntityCache, SqliteStorage};

// =============================================================================
// Execution Context
// =============================================================================

/// Execution context threaded through all operators during query execution.
///
/// Carries mutable query statistics and resource limits. The `max_bindings`
/// limit acts as a circuit breaker to prevent out-of-memory conditions on
/// queries that produce explosive intermediate results (e.g., large cross
/// joins or deep variable-length path traversals).
/// Default BFS frontier limit (2 million entries ≈ 24 MB of queue memory).
const DEFAULT_MAX_FRONTIER_ENTRIES: usize = 2_000_000;

/// Resource limits for query execution to prevent OOM conditions.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum number of intermediate bindings allowed. None = unlimited.
    pub max_bindings: Option<usize>,
    /// Maximum BFS frontier entries allowed. Defaults to 2M.
    /// Set to `None` to disable (not recommended).
    pub max_frontier_entries: Option<usize>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_bindings: None,
            max_frontier_entries: Some(DEFAULT_MAX_FRONTIER_ENTRIES),
        }
    }
}

pub(crate) struct ExecutionContext {
    pub stats: QueryStats,
    limits: ResourceLimits,
    /// Running count of bindings produced so far.
    bindings_produced: usize,
}

impl ExecutionContext {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            stats: QueryStats::default(),
            limits,
            bindings_produced: 0,
        }
    }

    /// Record newly produced bindings and check the limit.
    /// Call after pushing to a result vec.
    pub fn track_bindings(&mut self, count: usize) -> Result<()> {
        self.bindings_produced += count;
        if let Some(max) = self.limits.max_bindings {
            if self.bindings_produced > max {
                return Err(Error::ResourceLimit(format!(
                    "query produced more than {} intermediate results; \
                     simplify the query or increase the limit",
                    max
                )));
            }
        }
        Ok(())
    }

    /// Check whether the BFS frontier has exceeded the allowed size.
    /// Call after pushing entries to a BFS queue.
    pub fn check_frontier(&self, queue_len: usize) -> Result<()> {
        if let Some(max) = self.limits.max_frontier_entries {
            if queue_len > max {
                return Err(Error::ResourceLimit(format!(
                    "BFS frontier exceeded {} entries; \
                     reduce path depth or add filters to narrow the search",
                    max
                )));
            }
        }
        Ok(())
    }
}

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
    limits: ResourceLimits,
) -> Result<QueryResult> {
    let mut ctx = ExecutionContext::new(limits);
    let start = std::time::Instant::now();

    // Execute the plan tree
    let execution_result = execute_operator(&plan.root, storage, &mut ctx, cache)?;

    // Convert to QueryResult
    let mut result = match execution_result {
        ExecutionResult::Bindings(_bindings) => {
            // No RETURN clause - empty result
            QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                stats: ctx.stats,
            }
        }
        ExecutionResult::Rows { columns, rows } => QueryResult {
            columns,
            rows,
            stats: ctx.stats,
        },
    };

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
    ctx: &mut ExecutionContext,
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
                execute_operator_to_bindings(&p.source, storage, ctx, cache.as_deref_mut())?;
            let request = ExpandRequest {
                source_variable: &p.source_variable,
                rel_variable: p.rel_variable.as_deref(),
                target_variable: &p.target_variable,
                target_labels: &p.target_labels,
                path_variable: p.path_variable.as_deref(),
                types: &p.types,
                direction: p.direction,
                limit: p.limit,
                target_property_filter: p.target_property_filter.as_ref(),
            };
            execute_expand(bindings, &request, storage, cache, ctx)
        }

        PlanOperator::VariableLengthExpand(ref p) => {
            let bindings =
                execute_operator_to_bindings(&p.source, storage, ctx, cache.as_deref_mut())?;
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
            execute_variable_length_expand(bindings, &request, storage, cache, ctx)
        }

        PlanOperator::ShortestPath(ref p) => {
            let bindings =
                execute_operator_to_bindings(&p.source, storage, ctx, cache.as_deref_mut())?;
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
                p.target_property_filter.as_ref(),
                p.limit,
                storage,
                cache,
                ctx,
            )
        }

        PlanOperator::Filter { source, predicate } => {
            let bindings = execute_operator_to_bindings(source, storage, ctx, cache)?;
            let filtered = filter_bindings(bindings, predicate)?;
            Ok(ExecutionResult::Bindings(filtered))
        }

        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, ctx, cache)?;
            execute_project(bindings, columns, *distinct, storage)
        }

        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, ctx, cache)?;
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
            match execute_operator(source, storage, ctx, cache)? {
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
            match execute_operator(source, storage, ctx, cache)? {
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
                execute_operator_to_bindings(left, storage, ctx, cache.as_deref_mut())?;
            let right_bindings = execute_operator_to_bindings(right, storage, ctx, cache)?;
            let product_size = left_bindings.len() * right_bindings.len();
            ctx.track_bindings(product_size)?;
            let mut result = Vec::with_capacity(product_size);
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
        } => execute_create(source.as_deref(), nodes, relationships, storage, cache, ctx),

        PlanOperator::SetProperties { source, sets } => {
            let bindings = execute_operator_to_bindings(source, storage, ctx, cache)?;
            execute_set_properties(&bindings, sets, storage, &mut ctx.stats)?;
            Ok(ExecutionResult::Bindings(bindings))
        }

        PlanOperator::Delete {
            source,
            variables,
            detach,
        } => {
            let bindings = execute_operator_to_bindings(source, storage, ctx, cache)?;
            execute_delete(&bindings, variables, *detach, storage, &mut ctx.stats)?;
            Ok(ExecutionResult::Bindings(Vec::new()))
        }

        PlanOperator::Sort { source, keys } => {
            // Sort operates on Rows (after projection)
            match execute_operator(source, storage, ctx, cache)? {
                ExecutionResult::Rows { columns, mut rows } => {
                    rows.sort_by(|a, b| {
                        for key in keys {
                            let av = a.get(&key.column);
                            let bv = b.get(&key.column);
                            let cmp = compare_result_values(av, bv);
                            let cmp = if key.descending { cmp.reverse() } else { cmp };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    Ok(ExecutionResult::Rows { columns, rows })
                }
                ExecutionResult::Bindings(bindings) => {
                    // Sort on bindings is a no-op (shouldn't happen in well-formed plans)
                    Ok(ExecutionResult::Bindings(bindings))
                }
            }
        }
    }
}

/// Compare two optional ResultValues for sorting.
/// NULL values sort last (after all non-NULL values).
fn compare_result_values(
    a: Option<&crate::query::ResultValue>,
    b: Option<&crate::query::ResultValue>,
) -> std::cmp::Ordering {
    use crate::graph::PropertyValue;
    use crate::query::ResultValue;

    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(a), Some(b)) => match (a, b) {
            (ResultValue::Property(pa), ResultValue::Property(pb)) => match (pa, pb) {
                (PropertyValue::Integer(x), PropertyValue::Integer(y)) => x.cmp(y),
                (PropertyValue::Float(x), PropertyValue::Float(y)) => {
                    x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                }
                (PropertyValue::Integer(x), PropertyValue::Float(y)) => (*x as f64)
                    .partial_cmp(y)
                    .unwrap_or(std::cmp::Ordering::Equal),
                (PropertyValue::Float(x), PropertyValue::Integer(y)) => x
                    .partial_cmp(&(*y as f64))
                    .unwrap_or(std::cmp::Ordering::Equal),
                (PropertyValue::String(x), PropertyValue::String(y)) => x.cmp(y),
                (PropertyValue::Bool(x), PropertyValue::Bool(y)) => x.cmp(y),
                (PropertyValue::Null, PropertyValue::Null) => std::cmp::Ordering::Equal,
                (PropertyValue::Null, _) => std::cmp::Ordering::Greater,
                (_, PropertyValue::Null) => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            },
            _ => std::cmp::Ordering::Equal,
        },
    }
}

/// Execute an operator and expect bindings (not final rows).
fn execute_operator_to_bindings(
    op: &PlanOperator,
    storage: &SqliteStorage,
    ctx: &mut ExecutionContext,
    cache: Option<&mut EntityCache>,
) -> Result<Vec<Binding>> {
    match execute_operator(op, storage, ctx, cache)? {
        ExecutionResult::Bindings(b) => Ok(b),
        ExecutionResult::Rows { .. } => {
            // This shouldn't happen in a well-formed plan
            Err(Error::Internal("Expected bindings, got rows".into()))
        }
    }
}

/// Execute a plan and return raw bindings (for WITH clause support).
pub fn execute_plan_bindings(
    plan: &QueryPlan,
    storage: &SqliteStorage,
    ctx: &mut ExecutionContext,
    cache: Option<&mut EntityCache>,
) -> Result<Vec<Binding>> {
    execute_operator_to_bindings(&plan.root, storage, ctx, cache)
}

/// Execute a plan using provided bindings as the source.
///
/// Replaces the leaf operator's output with the given bindings, allowing
/// the rest of the plan (Project, Aggregate, Sort, etc.) to process them.
pub fn execute_plan_on_bindings(
    plan: &QueryPlan,
    bindings: Vec<Binding>,
    storage: &SqliteStorage,
    ctx: &mut ExecutionContext,
    cache: Option<&mut EntityCache>,
) -> Result<QueryResult> {
    let result = execute_operator_on_bindings(&plan.root, bindings, storage, ctx, cache)?;
    match result {
        ExecutionResult::Bindings(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            stats: ctx.stats.clone(),
        }),
        ExecutionResult::Rows { columns, rows } => Ok(QueryResult {
            columns,
            rows,
            stats: ctx.stats.clone(),
        }),
    }
}

/// Execute an operator, injecting provided bindings at leaf positions.
fn execute_operator_on_bindings(
    op: &PlanOperator,
    bindings: Vec<Binding>,
    storage: &SqliteStorage,
    ctx: &mut ExecutionContext,
    cache: Option<&mut EntityCache>,
) -> Result<ExecutionResult> {
    match op {
        // Leaf operators get replaced by the provided bindings
        PlanOperator::Empty | PlanOperator::ProduceRow | PlanOperator::NodeScan { .. } => {
            Ok(ExecutionResult::Bindings(bindings))
        }

        // Operators that process their source - recurse with bindings
        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            let inner_bindings =
                execute_on_bindings_to_bindings(source, bindings, storage, ctx, cache)?;
            execute_project(inner_bindings, columns, *distinct, storage)
        }

        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let inner_bindings =
                execute_on_bindings_to_bindings(source, bindings, storage, ctx, cache)?;
            execute_aggregate(inner_bindings, group_by, aggregates, storage)
        }

        PlanOperator::Sort { source, keys } => {
            let inner = execute_operator_on_bindings(source, bindings, storage, ctx, cache)?;
            match inner {
                ExecutionResult::Rows { columns, mut rows } => {
                    rows.sort_by(|a, b| {
                        for key in keys {
                            let av = a.get(&key.column);
                            let bv = b.get(&key.column);
                            let cmp = compare_result_values(av, bv);
                            let cmp = if key.descending { cmp.reverse() } else { cmp };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    Ok(ExecutionResult::Rows { columns, rows })
                }
                other => Ok(other),
            }
        }

        PlanOperator::Limit { source, count } => {
            match execute_operator_on_bindings(source, bindings, storage, ctx, cache)? {
                ExecutionResult::Bindings(mut b) => {
                    b.truncate(*count as usize);
                    Ok(ExecutionResult::Bindings(b))
                }
                ExecutionResult::Rows { columns, mut rows } => {
                    rows.truncate(*count as usize);
                    Ok(ExecutionResult::Rows { columns, rows })
                }
            }
        }

        PlanOperator::Skip { source, count } => {
            match execute_operator_on_bindings(source, bindings, storage, ctx, cache)? {
                ExecutionResult::Bindings(b) => {
                    let skipped: Vec<_> = b.into_iter().skip(*count as usize).collect();
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

        PlanOperator::Filter { source, predicate } => {
            let inner_bindings =
                execute_on_bindings_to_bindings(source, bindings, storage, ctx, cache)?;
            let filtered = filter_bindings(inner_bindings, predicate)?;
            Ok(ExecutionResult::Bindings(filtered))
        }

        // Fallback: execute normally (ignoring provided bindings).
        // This drops the caller-provided bindings which may produce wrong
        // results if the caller expected them to be threaded through.
        _ => {
            warn!(
                operator = op.variant_name(),
                num_bindings = bindings.len(),
                "execute_operator_on_bindings: operator does not support injected bindings; \
                 falling back to normal execution (bindings dropped)"
            );
            execute_operator(op, storage, ctx, cache)
        }
    }
}

/// Helper: execute on bindings and expect bindings back.
fn execute_on_bindings_to_bindings(
    op: &PlanOperator,
    bindings: Vec<Binding>,
    storage: &SqliteStorage,
    ctx: &mut ExecutionContext,
    cache: Option<&mut EntityCache>,
) -> Result<Vec<Binding>> {
    match execute_operator_on_bindings(op, bindings, storage, ctx, cache)? {
        ExecutionResult::Bindings(b) => Ok(b),
        ExecutionResult::Rows { .. } => Err(Error::Internal("Expected bindings, got rows".into())),
    }
}
