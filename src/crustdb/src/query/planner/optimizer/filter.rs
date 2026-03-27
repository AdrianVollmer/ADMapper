//! Filter extraction: target and source property filters from predicates.

use super::super::{FilterPredicate, PlanExpr, PlanLiteral, TargetPropertyFilter};

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
pub(super) fn extract_target_property_filter(
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
pub(super) fn extract_source_property_filter(
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
