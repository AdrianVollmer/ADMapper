use super::{compare_values, evaluate_expr, values_equal, Binding, EvalValue};
use crate::error::Result;
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
    }
}
