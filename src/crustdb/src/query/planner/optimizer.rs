//! Query plan optimization passes.

use super::{
    AggregateFunction, FilterPredicate, PlanExpr, PlanLiteral, PlanOperator, QueryPlan,
    ShortestPathParams, TargetPropertyFilter, VarLenExpandParams,
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
                        // Push through Filter -> VariableLengthExpand
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
                                // check if we got VariableLengthExpand
                                PlanOperator::VariableLengthExpand(mut p) if p.limit.is_none() => {
                                    p.limit = Some(count);
                                    PlanOperator::Project {
                                        source: Box::new(PlanOperator::VariableLengthExpand(p)),
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

        PlanOperator::CrossJoin { left, right } => PlanOperator::CrossJoin {
            left: Box::new(optimize_operator(*left)),
            right: Box::new(optimize_operator(*right)),
        },

        // Leaf operators - no optimization needed
        other => other,
    }
}

/// Check if a plan involves a relationship pattern that binds the given variable.
///
/// This is used to detect patterns like `MATCH ()-[r]->() RETURN DISTINCT type(r)`
/// where we can optimize to use RelationshipTypesScan instead of scanning all relationships.
fn is_relationship_pattern_with_var(op: &PlanOperator, rel_var: &str) -> bool {
    match op {
        PlanOperator::Expand(p) => p.rel_variable.as_deref() == Some(rel_var),
        PlanOperator::VariableLengthExpand(p) => p.rel_variable.as_deref() == Some(rel_var),
        PlanOperator::Filter { source, .. } => is_relationship_pattern_with_var(source, rel_var),
        _ => false,
    }
}

/// Extract a target property filter from a predicate, if possible.
///
/// Looks for simple property conditions on the target variable that can be
/// pushed into VariableLengthExpand for early termination during BFS.
///
/// Supported patterns:
/// - `target.property = value` -> Eq filter
/// - `target.property ENDS WITH 'suffix'` -> EndsWith filter
/// - `target.property STARTS WITH 'prefix'` -> StartsWith filter
/// - `target.property CONTAINS 'substring'` -> Contains filter
///
/// Returns (pushed_filter, remaining_predicate) where remaining_predicate
/// is None if the entire predicate was pushed.
fn extract_target_property_filter(
    predicate: &FilterPredicate,
    target_variable: &str,
) -> Option<(TargetPropertyFilter, Option<FilterPredicate>)> {
    match predicate {
        // Simple equality: target.property = value
        FilterPredicate::Eq { left, right } => {
            if let PlanExpr::Property { variable, property } = left {
                if variable == target_variable {
                    if let PlanExpr::Literal(PlanLiteral::String(s)) = right {
                        return Some((
                            TargetPropertyFilter::Eq {
                                property: property.clone(),
                                value: serde_json::Value::String(s.clone()),
                            },
                            None,
                        ));
                    }
                    if let PlanExpr::Literal(PlanLiteral::Int(i)) = right {
                        return Some((
                            TargetPropertyFilter::Eq {
                                property: property.clone(),
                                value: serde_json::Value::Number((*i).into()),
                            },
                            None,
                        ));
                    }
                    if let PlanExpr::Literal(PlanLiteral::Bool(b)) = right {
                        return Some((
                            TargetPropertyFilter::Eq {
                                property: property.clone(),
                                value: serde_json::Value::Bool(*b),
                            },
                            None,
                        ));
                    }
                }
            }
            None
        }

        // ENDS WITH: target.property ENDS WITH 'suffix'
        FilterPredicate::EndsWith { expr, suffix } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::EndsWith {
                            property: property.clone(),
                            suffix: suffix.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // STARTS WITH: target.property STARTS WITH 'prefix'
        FilterPredicate::StartsWith { expr, prefix } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::StartsWith {
                            property: property.clone(),
                            prefix: prefix.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // CONTAINS: target.property CONTAINS 'substring'
        FilterPredicate::Contains { expr, substring } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::Contains {
                            property: property.clone(),
                            substring: substring.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // AND: try to extract from either side
        FilterPredicate::And { left, right } => {
            // Try left side first
            if let Some((filter, remaining_left)) =
                extract_target_property_filter(left, target_variable)
            {
                // Combine remaining left (if any) with right
                let remaining = match remaining_left {
                    Some(rem_left) => Some(FilterPredicate::And {
                        left: Box::new(rem_left),
                        right: right.clone(),
                    }),
                    None => Some((**right).clone()),
                };
                return Some((filter, remaining));
            }
            // Try right side
            if let Some((filter, remaining_right)) =
                extract_target_property_filter(right, target_variable)
            {
                // Combine left with remaining right (if any)
                let remaining = match remaining_right {
                    Some(rem_right) => Some(FilterPredicate::And {
                        left: left.clone(),
                        right: Box::new(rem_right),
                    }),
                    None => Some((**left).clone()),
                };
                return Some((filter, remaining));
            }
            None
        }

        // Other predicates can't be pushed
        _ => None,
    }
}

/// Extract a simple source property equality from a predicate.
///
/// Given `source_variable = "a"`, matches patterns like `a.prop = 'value'`
/// and returns `(property, value, remaining_predicate)`.
///
/// This allows pushing source filters into the NodeScan below a
/// VariableLengthExpand, dramatically reducing BFS work.
fn extract_source_property_filter(
    predicate: &FilterPredicate,
    source_variable: &str,
) -> Option<((String, serde_json::Value), Option<FilterPredicate>)> {
    match predicate {
        FilterPredicate::Eq { left, right } => {
            if let PlanExpr::Property { variable, property } = left {
                if variable == source_variable {
                    let value = match right {
                        PlanExpr::Literal(PlanLiteral::String(s)) => {
                            Some(serde_json::Value::String(s.clone()))
                        }
                        PlanExpr::Literal(PlanLiteral::Int(i)) => {
                            Some(serde_json::Value::Number((*i).into()))
                        }
                        PlanExpr::Literal(PlanLiteral::Bool(b)) => {
                            Some(serde_json::Value::Bool(*b))
                        }
                        _ => None,
                    };
                    if let Some(val) = value {
                        return Some(((property.clone(), val), None));
                    }
                }
            }
            None
        }

        FilterPredicate::And { left, right } => {
            if let Some((filter, remaining_left)) =
                extract_source_property_filter(left, source_variable)
            {
                let remaining = match remaining_left {
                    Some(rem_left) => Some(FilterPredicate::And {
                        left: Box::new(rem_left),
                        right: right.clone(),
                    }),
                    None => Some((**right).clone()),
                };
                return Some((filter, remaining));
            }
            if let Some((filter, remaining_right)) =
                extract_source_property_filter(right, source_variable)
            {
                let remaining = match remaining_right {
                    Some(rem_right) => Some(FilterPredicate::And {
                        left: left.clone(),
                        right: Box::new(rem_right),
                    }),
                    None => Some((**left).clone()),
                };
                return Some((filter, remaining));
            }
            None
        }

        _ => None,
    }
}

/// Push filter predicates from a WHERE clause into a VariableLengthExpand.
///
/// Target predicates (on the target variable) become `target_property_filter`
/// for early BFS termination. Source predicates go into the NodeScan for
/// reduced starting set.
fn push_filter_into_var_len_expand(
    mut p: VarLenExpandParams,
    predicate: FilterPredicate,
) -> PlanOperator {
    if p.target_property_filter.is_none() {
        if let Some((pushed_filter, remaining_predicate)) =
            extract_target_property_filter(&predicate, &p.target_variable)
        {
            p.source = Box::new(optimize_operator(*p.source));
            p.target_property_filter = Some(pushed_filter);
            let expand = PlanOperator::VariableLengthExpand(p);
            return wrap_with_remaining(expand, remaining_predicate);
        }
    }
    // Couldn't push target predicate (or already has one), try source pushdown
    p.source = Box::new(optimize_operator(*p.source));
    let expand = PlanOperator::VariableLengthExpand(p);
    let (pushed, leftover) = try_push_source_filter_into_node_scan(predicate, expand);
    wrap_with_filter(pushed, leftover)
}

/// Push filter predicates from a WHERE clause into a ShortestPath.
///
/// Target predicates (on the target variable) become `target_property_filter`
/// for early BFS termination. Source predicates go into the NodeScan for
/// reduced starting set.
fn push_filter_into_shortest_path(
    mut p: ShortestPathParams,
    predicate: FilterPredicate,
) -> PlanOperator {
    if p.target_property_filter.is_none() {
        if let Some((pushed_filter, remaining_predicate)) =
            extract_target_property_filter(&predicate, &p.target_variable)
        {
            p.source = Box::new(optimize_operator(*p.source));
            p.target_property_filter = Some(pushed_filter);
            let sp = PlanOperator::ShortestPath(p);
            return wrap_with_remaining(sp, remaining_predicate);
        }
    }
    // Couldn't push target predicate (or already has one), try source pushdown
    p.source = Box::new(optimize_operator(*p.source));
    let sp = PlanOperator::ShortestPath(p);
    let (pushed, leftover) = try_push_source_filter_into_node_scan(predicate, sp);
    wrap_with_filter(pushed, leftover)
}

/// After pushing a target filter, handle remaining predicates:
/// try source pushdown, then wrap leftovers in a Filter.
fn wrap_with_remaining(operator: PlanOperator, remaining: Option<FilterPredicate>) -> PlanOperator {
    if let Some(remaining_pred) = remaining {
        let (pushed, leftover) = try_push_source_filter_into_node_scan(remaining_pred, operator);
        wrap_with_filter(pushed, leftover)
    } else {
        operator
    }
}

/// Wrap an operator in a Filter if there's a leftover predicate.
fn wrap_with_filter(operator: PlanOperator, leftover: Option<FilterPredicate>) -> PlanOperator {
    if let Some(pred) = leftover {
        PlanOperator::Filter {
            source: Box::new(operator),
            predicate: pred,
        }
    } else {
        operator
    }
}

/// Try to push a source property equality filter into the NodeScan that feeds
/// a VariableLengthExpand or ShortestPath.
///
/// Transforms:
///   Filter(a.prop = 'X') -> Expand(source: NodeScan(no filter))
/// Into:
///   Expand(source: NodeScan(property_filter: (prop, 'X')))
///
/// Returns the (possibly modified) operator and any remaining predicate.
fn try_push_source_filter_into_node_scan(
    predicate: FilterPredicate,
    operator: PlanOperator,
) -> (PlanOperator, Option<FilterPredicate>) {
    match operator {
        PlanOperator::VariableLengthExpand(mut p) => {
            if let Some(result) =
                try_push_into_inner_scan(&predicate, &p.source_variable, &mut p.source)
            {
                return (PlanOperator::VariableLengthExpand(p), result);
            }
            (PlanOperator::VariableLengthExpand(p), Some(predicate))
        }
        PlanOperator::ShortestPath(mut p) => {
            if let Some(result) =
                try_push_into_inner_scan(&predicate, &p.source_variable, &mut p.source)
            {
                return (PlanOperator::ShortestPath(p), result);
            }
            (PlanOperator::ShortestPath(p), Some(predicate))
        }
        other => (other, Some(predicate)),
    }
}

/// Try to push a source property filter into a NodeScan that is the inner
/// source of an expand operator. Returns `Some(remaining)` on success.
fn try_push_into_inner_scan(
    predicate: &FilterPredicate,
    source_variable: &str,
    source: &mut Box<PlanOperator>,
) -> Option<Option<FilterPredicate>> {
    if let PlanOperator::NodeScan {
        property_filter: ref mut pf @ None,
        ..
    } = **source
    {
        if let Some((prop_filter, remaining)) =
            extract_source_property_filter(predicate, source_variable)
        {
            *pf = Some(prop_filter);
            return Some(remaining);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::query::parser::parse;

    fn plan_query(query: &str) -> QueryPlan {
        let stmt = parse(query).expect("parse failed");
        let plan = plan(&stmt).expect("plan failed");
        optimize(plan)
    }

    #[test]
    fn test_plan_simple_match() {
        let plan = plan_query("MATCH (n:Person) RETURN n");
        // Should be: Project -> NodeScan
        assert!(matches!(plan.root, PlanOperator::Project { .. }));
    }

    #[test]
    fn test_plan_count_pushdown() {
        let plan = plan_query("MATCH (n:Person) RETURN count(n)");
        // Should be optimized to CountPushdown
        assert!(matches!(plan.root, PlanOperator::CountPushdown { .. }));
    }

    #[test]
    fn test_plan_with_where() {
        let plan = plan_query("MATCH (n:Person) WHERE n.age > 30 RETURN n");
        // Should be: Project -> Filter -> NodeScan
        if let PlanOperator::Project { source, .. } = plan.root {
            assert!(matches!(*source, PlanOperator::Filter { .. }));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_limit_pushdown() {
        let plan = plan_query("MATCH (n:Person) RETURN n LIMIT 10");
        // Should be: Project -> NodeScan(limit=10)
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::NodeScan { limit, .. } = *source {
                assert_eq!(limit, Some(10));
            } else {
                panic!("Expected NodeScan");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_single_hop() {
        let plan = plan_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
        // Should be: Project -> Expand -> NodeScan
        if let PlanOperator::Project { source, .. } = plan.root {
            assert!(matches!(*source, PlanOperator::Expand(_)));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_limit_pushdown() {
        let plan = plan_query("MATCH (a)-[*1..5]->(b) RETURN b LIMIT 1");
        // Should have limit pushed into VariableLengthExpand
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand(ref p) = *source {
                assert_eq!(
                    p.limit,
                    Some(1),
                    "LIMIT should be pushed into VariableLengthExpand"
                );
            } else {
                panic!("Expected VariableLengthExpand");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_filter_pushdown() {
        let plan = plan_query("MATCH (a)-[*1..5]->(b) WHERE b.name ENDS WITH 'admin' RETURN b");
        // Should have target_property_filter pushed into VariableLengthExpand
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand(ref p) = *source {
                assert!(
                    p.target_property_filter.is_some(),
                    "ENDS WITH predicate should be pushed into VariableLengthExpand"
                );
                if let Some(TargetPropertyFilter::EndsWith {
                    ref property,
                    ref suffix,
                }) = p.target_property_filter
                {
                    assert_eq!(property, "name");
                    assert_eq!(suffix, "admin");
                } else {
                    panic!("Expected EndsWith filter");
                }
            } else {
                panic!("Expected VariableLengthExpand");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_limit_through_filter() {
        // When both source and target predicates from a WHERE clause can be
        // pushed down (source into NodeScan, target into VariableLengthExpand),
        // the Filter is eliminated entirely and LIMIT can be pushed into the
        // expand for early BFS termination.
        // Structure: Project -> VariableLengthExpand(limit=1, target_filter, source: NodeScan(prop_filter))
        let plan = plan_query(
            "MATCH p = (a)-[*1..20]->(b) WHERE a.name = 'test' AND b.id ENDS WITH '-519' RETURN length(p) LIMIT 1",
        );
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand(ref p) = *source {
                assert_eq!(
                    p.limit,
                    Some(1),
                    "LIMIT should be pushed into VariableLengthExpand when Filter is eliminated"
                );
                assert!(
                    p.target_property_filter.is_some(),
                    "Target property filter should be pushed"
                );
                if let PlanOperator::NodeScan {
                    ref property_filter,
                    ..
                } = *p.source
                {
                    assert!(
                        property_filter.is_some(),
                        "Source property filter should be pushed into NodeScan"
                    );
                } else {
                    panic!("Expected NodeScan under VariableLengthExpand");
                }
            } else {
                panic!(
                    "Expected VariableLengthExpand under Project, got {:?}",
                    source
                );
            }
        } else {
            panic!("Expected Project at root, got {:?}", plan.root);
        }
    }

    #[test]
    fn test_plan_source_filter_pushdown_to_nodescan() {
        // Source equality predicates from WHERE should be pushed into
        // the NodeScan below VariableLengthExpand.
        let plan =
            plan_query("MATCH (a)-[*1..20]->(b) WHERE a.objectid = 'USER_0' RETURN b.objectid");
        // Plan should be: Project -> VariableLengthExpand(source: NodeScan(prop_filter))
        // No Filter should remain.
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand(ref p) = *source {
                if let PlanOperator::NodeScan {
                    ref property_filter,
                    ..
                } = *p.source
                {
                    assert!(
                        property_filter.is_some(),
                        "Source predicate should be pushed into NodeScan"
                    );
                    let (ref prop, ref val) = property_filter.as_ref().unwrap();
                    assert_eq!(prop, "objectid");
                    assert_eq!(*val, serde_json::Value::String("USER_0".to_string()));
                } else {
                    panic!("Expected NodeScan under VariableLengthExpand");
                }
            } else {
                panic!("Expected VariableLengthExpand under Project");
            }
        } else {
            panic!("Expected Project at root");
        }
    }

    #[test]
    fn test_plan_boolean_target_filter_pushdown() {
        // Boolean target property filters should be pushed into VariableLengthExpand
        let plan =
            plan_query("MATCH (a)-[*1..20]->(b) WHERE b.is_highvalue = true RETURN b.objectid");
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand(ref p) = *source {
                assert!(
                    p.target_property_filter.is_some(),
                    "Boolean target property filter should be pushed into VariableLengthExpand"
                );
                if let Some(TargetPropertyFilter::Eq {
                    ref property,
                    ref value,
                }) = p.target_property_filter
                {
                    assert_eq!(property, "is_highvalue");
                    assert_eq!(*value, serde_json::Value::Bool(true));
                } else {
                    panic!("Expected Eq filter");
                }
            } else {
                panic!("Expected VariableLengthExpand, got {:?}", source);
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_relationship_count_pushdown() {
        // MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1
        // should produce: Limit(1, RelationshipCountPushdown)
        let plan = plan_query("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1");
        if let PlanOperator::Limit { source, count } = plan.root {
            assert_eq!(count, 1);
            assert!(
                matches!(*source, PlanOperator::RelationshipCountPushdown { .. }),
                "Expected RelationshipCountPushdown, got {:?}",
                source
            );
        } else {
            panic!("Expected Limit at root, got {:?}", plan.root);
        }
    }

    #[test]
    fn test_plan_expand_limit_pushdown() {
        // MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5
        // should push LIMIT into Expand
        let plan = plan_query("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5");
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::Expand(ref p) = *source {
                assert_eq!(p.limit, Some(5), "LIMIT 5 should be pushed into Expand");
            } else {
                panic!("Expected Expand under Project, got {:?}", source);
            }
        } else {
            panic!("Expected Project at root, got {:?}", plan.root);
        }
    }

    // =========================================================================
    // ShortestPath filter pushdown tests
    // =========================================================================

    #[test]
    fn test_shortest_path_ends_with_filter_pushdown() {
        let plan = plan_query(
            "MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE b.name ENDS WITH '-512' RETURN p",
        );
        // Should push ENDS WITH into ShortestPath.target_property_filter
        fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
            match op {
                PlanOperator::ShortestPath(p) => Some(p),
                PlanOperator::Project { source, .. } => find_shortest_path(source),
                PlanOperator::Filter { source, .. } => find_shortest_path(source),
                _ => None,
            }
        }
        let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
        assert!(
            sp.target_property_filter.is_some(),
            "ENDS WITH should be pushed into ShortestPath"
        );
        if let Some(TargetPropertyFilter::EndsWith {
            ref property,
            ref suffix,
        }) = sp.target_property_filter
        {
            assert_eq!(property, "name");
            assert_eq!(suffix, "-512");
        } else {
            panic!(
                "Expected EndsWith filter, got {:?}",
                sp.target_property_filter
            );
        }
    }

    #[test]
    fn test_shortest_path_eq_filter_pushdown() {
        let plan =
            plan_query("MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE b.id = 42 RETURN p");
        fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
            match op {
                PlanOperator::ShortestPath(p) => Some(p),
                PlanOperator::Project { source, .. } => find_shortest_path(source),
                PlanOperator::Filter { source, .. } => find_shortest_path(source),
                _ => None,
            }
        }
        let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
        assert!(
            sp.target_property_filter.is_some(),
            "Eq predicate should be pushed into ShortestPath"
        );
        matches!(
            sp.target_property_filter,
            Some(TargetPropertyFilter::Eq { .. })
        );
    }

    #[test]
    fn test_shortest_path_source_filter_pushdown() {
        let plan = plan_query(
            "MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE a.name = 'Alice' RETURN p",
        );
        fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
            match op {
                PlanOperator::ShortestPath(p) => Some(p),
                PlanOperator::Project { source, .. } => find_shortest_path(source),
                PlanOperator::Filter { source, .. } => find_shortest_path(source),
                _ => None,
            }
        }
        let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
        // Source filter should be pushed into the NodeScan under ShortestPath
        if let PlanOperator::NodeScan {
            ref property_filter,
            ..
        } = *sp.source
        {
            assert!(
                property_filter.is_some(),
                "Source predicate should be pushed into NodeScan"
            );
        } else {
            panic!("Expected NodeScan under ShortestPath, got {:?}", sp.source);
        }
    }

    #[test]
    fn test_shortest_path_both_filters_pushdown() {
        // Both source and target filters should be pushed down simultaneously
        let plan = plan_query(
            "MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE a.name = 'Alice' AND b.name ENDS WITH '-512' RETURN p",
        );
        fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
            match op {
                PlanOperator::ShortestPath(p) => Some(p),
                PlanOperator::Project { source, .. } => find_shortest_path(source),
                PlanOperator::Filter { source, .. } => find_shortest_path(source),
                _ => None,
            }
        }
        let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
        assert!(
            sp.target_property_filter.is_some(),
            "Target filter should be pushed into ShortestPath"
        );
        if let PlanOperator::NodeScan {
            ref property_filter,
            ..
        } = *sp.source
        {
            assert!(
                property_filter.is_some(),
                "Source filter should be pushed into NodeScan"
            );
        } else {
            panic!("Expected NodeScan under ShortestPath");
        }
    }
}
