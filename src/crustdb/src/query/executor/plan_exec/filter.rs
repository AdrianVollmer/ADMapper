use super::{compare_values, evaluate_expr, values_equal, Binding};
use crate::error::Result;
use crate::graph::PropertyValue;
use crate::query::ast::ListPredicateKind;
use crate::query::planner::FilterPredicate;

pub(super) fn filter_bindings(
    bindings: Vec<Binding>,
    predicate: &FilterPredicate,
) -> Result<Vec<Binding>> {
    let mut result = Vec::new();
    for binding in bindings {
        if evaluate_predicate(predicate, &binding)? {
            result.push(binding);
        }
    }
    Ok(result)
}

/// Public interface for evaluating predicates (used by WITH WHERE).
pub(crate) fn evaluate_predicate_on(
    predicate: &FilterPredicate,
    binding: &Binding,
) -> Result<bool> {
    evaluate_predicate(predicate, binding)
}

pub(super) fn evaluate_predicate(predicate: &FilterPredicate, binding: &Binding) -> Result<bool> {
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
            Ok(matches!(v, PropertyValue::Null))
        }

        FilterPredicate::IsNotNull { expr } => {
            let v = evaluate_expr(expr, binding)?;
            Ok(!matches!(v, PropertyValue::Null))
        }

        FilterPredicate::StartsWith { expr, prefix } => {
            let v = evaluate_expr(expr, binding)?;
            if let PropertyValue::String(s) = v {
                Ok(s.starts_with(prefix))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::EndsWith { expr, suffix } => {
            let v = evaluate_expr(expr, binding)?;
            if let PropertyValue::String(s) = v {
                Ok(s.ends_with(suffix))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::Contains { expr, substring } => {
            let v = evaluate_expr(expr, binding)?;
            if let PropertyValue::String(s) = v {
                Ok(s.contains(substring))
            } else {
                Ok(false)
            }
        }

        FilterPredicate::Regex { expr, pattern } => {
            let v = evaluate_expr(expr, binding)?;
            if let PropertyValue::String(s) = v {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| crate::error::Error::Cypher(e.to_string()))?;
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

        FilterPredicate::ListPredicate {
            kind,
            variable,
            list,
            filter,
        } => {
            // Resolve the list: typically a relationship list from variable-length path
            let items = resolve_list_items(list, binding)?;
            let mut match_count: usize = 0;

            for item in &items {
                let matches = match item {
                    ListItemRef::Relationship(rel) => {
                        if let Some(pred) = filter {
                            let inner = binding.clone().with_relationship(variable, (*rel).clone());
                            evaluate_predicate(pred, &inner)?
                        } else {
                            true
                        }
                    }
                };

                if matches {
                    match_count += 1;
                }

                // Short-circuit
                match kind {
                    ListPredicateKind::Any if matches => return Ok(true),
                    ListPredicateKind::None if matches => return Ok(false),
                    ListPredicateKind::All if !matches => return Ok(false),
                    _ => {}
                }
            }

            Ok(match kind {
                ListPredicateKind::All => true,
                ListPredicateKind::Any => false,
                ListPredicateKind::None => true,
                ListPredicateKind::Single => match_count == 1,
            })
        }
    }
}

enum ListItemRef<'a> {
    Relationship(&'a crate::graph::Relationship),
}

/// Resolve a PlanExpr to a list of items for list predicate evaluation.
fn resolve_list_items<'a>(
    list_expr: &crate::query::planner::PlanExpr,
    binding: &'a Binding,
) -> Result<Vec<ListItemRef<'a>>> {
    if let crate::query::planner::PlanExpr::Variable(var_name) = list_expr {
        if let Some(rel_list) = binding.get_relationship_list(var_name) {
            return Ok(rel_list.iter().map(ListItemRef::Relationship).collect());
        }
    }
    // If not a relationship list variable, return empty
    Ok(Vec::new())
}
