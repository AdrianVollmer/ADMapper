//! Query planner - converts AST to execution plan.
//!
//! The planner takes a parsed Cypher AST and produces an optimized
//! execution plan that can be run by the executor.

use super::ast::{
    BinaryOperator, Expression, Literal, MatchClause, OrderByItem, Pattern, PatternElement,
    ReturnClause, Statement,
};
use crate::error::{Error, Result};

// Re-export plan types for backwards compatibility
pub use super::operators::{
    AggregateColumn, AggregateFunction, CreateNode, CreateRelationship, ExpandDirection,
    ExpandParams, FilterPredicate, PlanExpr, PlanLiteral, PlanOperator, ProjectColumn, QueryPlan,
    SetOperation, ShortestPathParams, SortKey, TargetPropertyFilter, VarLenExpandParams,
};

mod create;
mod expression;
mod match_plan;
mod optimizer;

use create::*;
use expression::*;
use match_plan::*;

pub use optimizer::optimize;

// =============================================================================
// Plan Generation
// =============================================================================

/// Plan a parsed statement.
pub fn plan(statement: &Statement) -> Result<QueryPlan> {
    let root = match statement {
        Statement::Create(create) => plan_create(create)?,
        Statement::Match(match_clause) => plan_match(match_clause)?,
        Statement::Return(return_clause) => plan_standalone_return(return_clause)?,
        Statement::Delete(_) => {
            return Err(Error::Cypher("Standalone DELETE not supported".into()));
        }
        Statement::Set(_) => {
            return Err(Error::Cypher("Standalone SET not supported".into()));
        }
        Statement::Merge(_) => {
            return Err(Error::Cypher("MERGE not yet supported".into()));
        }
        Statement::UnionAll(_) | Statement::Union(_) => {
            return Err(Error::Internal(
                "UNION should be handled at the executor level, not the planner".into(),
            ));
        }
    };

    Ok(QueryPlan { root })
}

/// Plan a standalone RETURN statement (e.g., RETURN 1, RETURN "hello").
pub(super) fn plan_standalone_return(return_clause: &ReturnClause) -> Result<PlanOperator> {
    // Use ProduceRow to create a single empty row, then project the expressions
    plan_return(PlanOperator::ProduceRow, return_clause)
}

/// Plan a RETURN clause into projection, aggregation, and pagination.
pub(super) fn plan_return(
    source: PlanOperator,
    return_clause: &ReturnClause,
) -> Result<PlanOperator> {
    let mut plan = source;

    // Check for aggregates
    let has_aggregates = return_clause
        .items
        .iter()
        .any(|item| is_aggregate_expression(&item.expression));

    if has_aggregates {
        // Separate aggregate and non-aggregate columns
        let mut group_by = Vec::new();
        let mut aggregates = Vec::new();

        for item in &return_clause.items {
            let alias = item
                .alias
                .clone()
                .unwrap_or_else(|| format_expression(&item.expression));

            if let Some(agg) = try_extract_aggregate(&item.expression)? {
                aggregates.push(AggregateColumn {
                    function: agg,
                    alias,
                });
            } else {
                let expr = plan_expression(&item.expression)?;
                group_by.push(ProjectColumn { expr, alias });
            }
        }

        plan = PlanOperator::Aggregate {
            source: Box::new(plan),
            group_by,
            aggregates,
        };
    } else {
        // Simple projection
        let columns: Result<Vec<_>> = return_clause
            .items
            .iter()
            .map(|item| {
                let expr = plan_expression(&item.expression)?;
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format_expression(&item.expression));
                Ok(ProjectColumn { expr, alias })
            })
            .collect();

        plan = PlanOperator::Project {
            source: Box::new(plan),
            columns: columns?,
            distinct: return_clause.distinct,
        };
    }

    // Add ORDER BY (must come after projection, before SKIP/LIMIT)
    if let Some(ref order_by) = return_clause.order_by {
        let keys = plan_order_by(order_by, return_clause)?;
        plan = PlanOperator::Sort {
            source: Box::new(plan),
            keys,
        };
    }

    // Add SKIP
    if let Some(skip) = return_clause.skip {
        plan = PlanOperator::Skip {
            source: Box::new(plan),
            count: skip,
        };
    }

    // Add LIMIT
    if let Some(limit) = return_clause.limit {
        plan = PlanOperator::Limit {
            source: Box::new(plan),
            count: limit,
        };
    }

    Ok(plan)
}

/// Plan a SET clause.
pub(super) fn plan_set_clause(set_clause: &super::parser::SetClause) -> Result<Vec<SetOperation>> {
    let mut ops = Vec::new();

    for item in &set_clause.items {
        match item {
            super::parser::SetItem::Property {
                variable,
                property,
                value,
            } => {
                ops.push(SetOperation::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                    value: plan_expression(value)?,
                });
            }
            super::parser::SetItem::Labels { variable, labels } => {
                for label in labels {
                    ops.push(SetOperation::AddLabel {
                        variable: variable.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
    }

    Ok(ops)
}

/// Plan ORDER BY items into sort keys.
///
/// Resolves each ORDER BY expression to a projected column name by:
/// 1. Checking if it matches a RETURN alias (e.g., `ORDER BY name` when `RETURN n.name AS name`)
/// 2. Formatting the expression and matching against auto-generated column names
fn plan_order_by(order_by: &[OrderByItem], return_clause: &ReturnClause) -> Result<Vec<SortKey>> {
    order_by
        .iter()
        .map(|item| {
            let expr_str = format_expression(&item.expression);

            // First, check if it matches a RETURN alias directly
            let column = return_clause
                .items
                .iter()
                .find_map(|ri| {
                    // Match explicit alias
                    if let Some(ref alias) = ri.alias {
                        if alias == &expr_str {
                            return Some(alias.clone());
                        }
                    }
                    // Match expression text (auto-generated alias)
                    let ri_expr = format_expression(&ri.expression);
                    if ri_expr == expr_str {
                        return Some(ri.alias.clone().unwrap_or(ri_expr));
                    }
                    None
                })
                .ok_or_else(|| {
                    Error::Cypher(format!(
                        "ORDER BY expression '{}' not found in RETURN clause",
                        expr_str
                    ))
                })?;

            Ok(SortKey {
                column,
                descending: item.descending,
            })
        })
        .collect()
}
