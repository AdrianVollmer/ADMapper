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

        // Optimize LIMIT pushdown for simple node scans
        PlanOperator::Limit { source, count } => {
            match *source {
                // LIMIT on NodeScan can be pushed down
                PlanOperator::NodeScan {
                    variable,
                    label_groups,
                    limit: None,
                    property_filter,
                } => PlanOperator::NodeScan {
                    variable,
                    label_groups,
                    limit: Some(count),
                    property_filter,
                },

                // LIMIT on Expand can be pushed down for early termination
                PlanOperator::Expand(mut p) if p.limit.is_none() => {
                    p.source = Box::new(optimize_operator(*p.source));
                    p.limit = Some(count);
                    PlanOperator::Expand(p)
                }

                // LIMIT on VariableLengthExpand can be pushed down for early termination
                PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
                    p.source = Box::new(optimize_operator(*p.source));
                    p.limit = Some(count);
                    PlanOperator::VariableLengthExpand(p)
                }

                // LIMIT on ShortestPath can be pushed down for early termination
                PlanOperator::ShortestPath(mut p) if p.limit.is_none() => {
                    p.source = Box::new(optimize_operator(*p.source));
                    p.limit = Some(count);
                    PlanOperator::ShortestPath(p)
                }

                // LIMIT on Project can be pushed through to inner operators
                PlanOperator::Project {
                    source: project_source,
                    columns,
                    distinct: false,
                } => {
                    match *project_source {
                        // Push through to NodeScan
                        PlanOperator::NodeScan {
                            variable,
                            label_groups,
                            limit: None,
                            property_filter,
                        } => PlanOperator::Project {
                            source: Box::new(PlanOperator::NodeScan {
                                variable,
                                label_groups,
                                limit: Some(count),
                                property_filter,
                            }),
                            columns,
                            distinct: false,
                        },
                        // Push through to Expand
                        PlanOperator::Expand(mut p) if p.limit.is_none() => {
                            p.source = Box::new(optimize_operator(*p.source));
                            p.limit = Some(count);
                            PlanOperator::Project {
                                source: Box::new(PlanOperator::Expand(p)),
                                columns,
                                distinct: false,
                            }
                        }
                        // Push through to VariableLengthExpand
                        PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
                            p.source = Box::new(optimize_operator(*p.source));
                            p.limit = Some(count);
                            PlanOperator::Project {
                                source: Box::new(PlanOperator::VariableLengthExpand(p)),
                                columns,
                                distinct: false,
                            }
                        }
                        // Push through to ShortestPath
                        PlanOperator::ShortestPath(mut p) if p.limit.is_none() => {
                            p.source = Box::new(optimize_operator(*p.source));
                            p.limit = Some(count);
                            PlanOperator::Project {
                                source: Box::new(PlanOperator::ShortestPath(p)),
                                columns,
                                distinct: false,
                            }
                        }
                        // Push through Filter -> expand/shortest-path
                        // This handles: MATCH (a)-[*]->(b) WHERE ... RETURN ... LIMIT n
                        // First optimize the Filter (which may push target predicates),
                        // then push LIMIT into the result
                        PlanOperator::Filter {
                            source: filter_source,
                            predicate,
                        } => {
                            // Optimize the Filter first to allow target predicate pushdown
                            let optimized_filter = optimize_operator(PlanOperator::Filter {
                                source: filter_source,
                                predicate,
                            });

                            // Now check if we can push LIMIT into the result
                            match optimized_filter {
                                // Filter was kept, check what's inside
                                PlanOperator::Filter {
                                    source: opt_filter_source,
                                    predicate: opt_predicate,
                                } => {
                                    // A Filter remains above the expand, meaning
                                    // some predicates could not be pushed down.
                                    // We must NOT push LIMIT into the expand because
                                    // the Filter may discard results, causing the
                                    // expand to terminate early with too few results.
                                    // Keep LIMIT on top instead.
                                    PlanOperator::Limit {
                                        source: Box::new(PlanOperator::Project {
                                            source: Box::new(PlanOperator::Filter {
                                                source: opt_filter_source,
                                                predicate: opt_predicate,
                                            }),
                                            columns,
                                            distinct: false,
                                        }),
                                        count,
                                    }
                                }
                                // Filter was optimized away (predicate fully pushed),
                                // check if we got an expand-like operator
                                PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
                                    p.limit = Some(count);
                                    PlanOperator::Project {
                                        source: Box::new(PlanOperator::VariableLengthExpand(p)),
                                        columns,
                                        distinct: false,
                                    }
                                }
                                PlanOperator::Expand(mut p) if p.limit.is_none() => {
                                    p.limit = Some(count);
                                    PlanOperator::Project {
                                        source: Box::new(PlanOperator::Expand(p)),
                                        columns,
                                        distinct: false,
                                    }
                                }
                                PlanOperator::ShortestPath(mut p) if p.limit.is_none() => {
                                    p.limit = Some(count);
                                    PlanOperator::Project {
                                        source: Box::new(PlanOperator::ShortestPath(p)),
                                        columns,
                                        distinct: false,
                                    }
                                }
                                // Something else, keep LIMIT on top
                                other => PlanOperator::Limit {
                                    source: Box::new(PlanOperator::Project {
                                        source: Box::new(other),
                                        columns,
                                        distinct: false,
                                    }),
                                    count,
                                },
                            }
                        }
                        // Default: keep LIMIT on top
                        other => PlanOperator::Limit {
                            source: Box::new(PlanOperator::Project {
                                source: Box::new(optimize_operator(other)),
                                columns,
                                distinct: false,
                            }),
                            count,
                        },
                    }
                }

                other => PlanOperator::Limit {
                    source: Box::new(optimize_operator(other)),
                    count,
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
