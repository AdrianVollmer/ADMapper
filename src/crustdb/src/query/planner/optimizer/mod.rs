//! Query plan optimization passes.

mod cardinality;
mod filter;
mod helpers;
mod pushdown;

#[cfg(test)]
mod tests;

use super::{AggregateFunction, PlanExpr, PlanOperator, QueryPlan};

use cardinality::estimate_cardinality;
use helpers::is_relationship_pattern_with_var;
use pushdown::{
    push_filter_into_expand, push_filter_into_shortest_path, push_filter_into_var_len_expand,
};

/// Try to push a LIMIT into a single operator.
///
/// Handles NodeScan (by setting its limit) and expand-like operators (Expand,
/// VariableLengthExpand, ShortestPath) that have no limit yet. Returns `Ok` with
/// the modified operator on success, or `Err` with the original operator unchanged
/// so the caller can fall back to keeping LIMIT on top.
fn try_push_limit(source: PlanOperator, count: u64) -> Result<PlanOperator, Box<PlanOperator>> {
    match source {
        PlanOperator::NodeScan {
            variable,
            label_groups,
            limit: None,
            property_filter,
        } => Ok(PlanOperator::NodeScan {
            variable,
            label_groups,
            limit: Some(count),
            property_filter,
        }),
        PlanOperator::Expand(mut p) if p.limit.is_none() => {
            p.source = Box::new(optimize_operator(*p.source));
            p.limit = Some(count);
            Ok(PlanOperator::Expand(p))
        }
        PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
            p.source = Box::new(optimize_operator(*p.source));
            p.limit = Some(count);
            Ok(PlanOperator::VariableLengthExpand(p))
        }
        PlanOperator::ShortestPath(mut p) if p.limit.is_none() => {
            p.source = Box::new(optimize_operator(*p.source));
            p.limit = Some(count);
            Ok(PlanOperator::ShortestPath(p))
        }
        other => Err(Box::new(other)),
    }
}

/// Push a LIMIT through a Project into the project's source operator.
///
/// First tries `try_push_limit` directly. For Filter sources, optimizes the filter
/// first (which may eliminate it via predicate pushdown), then retries the limit push.
/// Returns the final operator tree with the LIMIT placed as deep as possible.
fn push_limit_through_project(
    project_source: PlanOperator,
    columns: Vec<super::ProjectColumn>,
    count: u64,
) -> PlanOperator {
    // Special case: Filter sources need optimization first to see if the filter
    // can be eliminated, which then allows the limit to be pushed deeper.
    if matches!(project_source, PlanOperator::Filter { .. }) {
        let optimized = optimize_operator(project_source);
        return match optimized {
            // Filter survived optimization — predicates could not be fully pushed
            // down, so we must NOT push LIMIT past the filter (it may discard
            // rows, causing early termination with too few results).
            PlanOperator::Filter { source, predicate } => PlanOperator::Limit {
                source: Box::new(PlanOperator::Project {
                    source: Box::new(PlanOperator::Filter { source, predicate }),
                    columns,
                    distinct: false,
                }),
                count,
            },
            // Filter was eliminated — try pushing limit into whatever remains.
            other => match try_push_limit(other, count) {
                Ok(op) => PlanOperator::Project {
                    source: Box::new(op),
                    columns,
                    distinct: false,
                },
                Err(inner) => PlanOperator::Limit {
                    source: Box::new(PlanOperator::Project {
                        source: inner,
                        columns,
                        distinct: false,
                    }),
                    count,
                },
            },
        };
    }

    // General case: try to push limit directly into the source.
    match try_push_limit(project_source, count) {
        Ok(op) => PlanOperator::Project {
            source: Box::new(op),
            columns,
            distinct: false,
        },
        Err(other) => PlanOperator::Limit {
            source: Box::new(PlanOperator::Project {
                source: Box::new(optimize_operator(*other)),
                columns,
                distinct: false,
            }),
            count,
        },
    }
}

/// Apply optimization passes to a query plan.
pub fn optimize(plan: QueryPlan) -> QueryPlan {
    let root = optimize_operator(plan.root);
    QueryPlan { root }
}

fn optimize_operator(op: PlanOperator) -> PlanOperator {
    match op {
        // Optimize COUNT pushdown for simple patterns
        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            // Check for COUNT(*) or COUNT(n) with no grouping
            if group_by.is_empty() && aggregates.len() == 1 {
                if let AggregateFunction::Count(ref arg) = aggregates[0].function {
                    // Check if source is a simple NodeScan without filters or property pushdown
                    if let PlanOperator::NodeScan {
                        ref label_groups,
                        limit: None,
                        property_filter: None,
                        ..
                    } = *source
                    {
                        // Can push COUNT to SQL only for simple single label
                        // Flatten to check - OR labels can't be pushed down easily
                        let flat_labels: Vec<String> =
                            label_groups.iter().flatten().cloned().collect();
                        let label = if flat_labels.is_empty() {
                            None
                        } else if flat_labels.len() == 1 {
                            Some(flat_labels[0].clone())
                        } else {
                            // Multiple labels - can't push down easily
                            return PlanOperator::Aggregate {
                                source: Box::new(optimize_operator(*source)),
                                group_by,
                                aggregates,
                            };
                        };

                        // Only optimize COUNT(*) or COUNT(var) without DISTINCT
                        if arg.is_none() || matches!(arg, Some(PlanExpr::Variable(_))) {
                            return PlanOperator::CountPushdown {
                                label,
                                alias: aggregates[0].alias.clone(),
                            };
                        }
                    }

                    // Check for COUNT(r) on Expand (relationship count pushdown)
                    // Pattern: Aggregate(count(r)) -> Expand(types, NodeScan(no filter))
                    if let PlanOperator::Expand(ref p) = *source {
                        // Only push down if no target label filter and source is unfiltered NodeScan
                        if p.target_labels.is_empty() {
                            if let PlanOperator::NodeScan {
                                label_groups: ref scan_labels,
                                limit: None,
                                property_filter: None,
                                ..
                            } = *p.source
                            {
                                if scan_labels.is_empty() {
                                    let rel_type = if p.types.len() == 1 {
                                        Some(p.types[0].clone())
                                    } else if p.types.is_empty() {
                                        None
                                    } else {
                                        // Multiple types - can't push down easily
                                        return PlanOperator::Aggregate {
                                            source: Box::new(optimize_operator(*source)),
                                            group_by,
                                            aggregates,
                                        };
                                    };
                                    return PlanOperator::RelationshipCountPushdown {
                                        rel_type,
                                        alias: aggregates[0].alias.clone(),
                                    };
                                }
                            }
                        }
                    }
                }
            }

            PlanOperator::Aggregate {
                source: Box::new(optimize_operator(*source)),
                group_by,
                aggregates,
            }
        }

        // Optimize LIMIT pushdown — try to push the limit into the source
        // operator to enable early termination deeper in the plan tree.
        PlanOperator::Limit { source, count } => {
            match *source {
                // LIMIT on Project (non-DISTINCT) can be pushed through
                PlanOperator::Project {
                    source: project_source,
                    columns,
                    distinct: false,
                } => push_limit_through_project(*project_source, columns, count),

                // LIMIT on NodeScan / Expand / VarLenExpand / ShortestPath
                source => match try_push_limit(source, count) {
                    Ok(op) => op,
                    Err(inner) => PlanOperator::Limit {
                        source: Box::new(optimize_operator(*inner)),
                        count,
                    },
                },
            }
        }

        // Optimize Filter: try to push predicates into underlying operators
        PlanOperator::Filter { source, predicate } => match *source {
            PlanOperator::VariableLengthExpand(p) => push_filter_into_var_len_expand(p, predicate),
            PlanOperator::ShortestPath(p) => push_filter_into_shortest_path(p, predicate),
            PlanOperator::Expand(p) => push_filter_into_expand(p, predicate),
            other => PlanOperator::Filter {
                source: Box::new(optimize_operator(other)),
                predicate,
            },
        },
        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            // Optimize: RETURN DISTINCT type(r) -> RelationshipTypesScan
            // Pattern: Project(distinct=true) over Expand with single column type(r)
            if distinct && columns.len() == 1 {
                if let PlanExpr::Function { name, args } = &columns[0].expr {
                    if name.to_uppercase() == "TYPE" && args.len() == 1 {
                        if let PlanExpr::Variable(rel_var) = &args[0] {
                            // Check if source involves a relationship variable matching rel_var
                            if is_relationship_pattern_with_var(&source, rel_var) {
                                return PlanOperator::RelationshipTypesScan {
                                    alias: columns[0].alias.clone(),
                                };
                            }
                        }
                    }
                }
            }

            PlanOperator::Project {
                source: Box::new(optimize_operator(*source)),
                columns,
                distinct,
            }
        }
        PlanOperator::Skip { source, count } => PlanOperator::Skip {
            source: Box::new(optimize_operator(*source)),
            count,
        },
        PlanOperator::Expand(mut p) => {
            p.source = Box::new(optimize_operator(*p.source));
            PlanOperator::Expand(p)
        }
        PlanOperator::VariableLengthExpand(mut p) => {
            p.source = Box::new(optimize_operator(*p.source));
            PlanOperator::VariableLengthExpand(p)
        }
        PlanOperator::ShortestPath(mut p) => {
            p.source = Box::new(optimize_operator(*p.source));
            PlanOperator::ShortestPath(p)
        }
        PlanOperator::SetProperties { source, sets } => PlanOperator::SetProperties {
            source: Box::new(optimize_operator(*source)),
            sets,
        },
        PlanOperator::Delete {
            source,
            variables,
            detach,
        } => PlanOperator::Delete {
            source: Box::new(optimize_operator(*source)),
            variables,
            detach,
        },
        PlanOperator::Create {
            source,
            nodes,
            relationships,
        } => PlanOperator::Create {
            source: source.map(|s| Box::new(optimize_operator(*s))),
            nodes,
            relationships,
        },

        PlanOperator::CrossJoin { left, right } => {
            let left_opt = optimize_operator(*left);
            let right_opt = optimize_operator(*right);
            let left_card = estimate_cardinality(&left_opt);
            let right_card = estimate_cardinality(&right_opt);
            // Place smaller-estimated side as left (outer loop) so that
            // subsequent filters can eliminate rows earlier.
            if left_card <= right_card {
                PlanOperator::CrossJoin {
                    left: Box::new(left_opt),
                    right: Box::new(right_opt),
                }
            } else {
                PlanOperator::CrossJoin {
                    left: Box::new(right_opt),
                    right: Box::new(left_opt),
                }
            }
        }

        // Leaf operators - no optimization needed
        other => other,
    }
}
