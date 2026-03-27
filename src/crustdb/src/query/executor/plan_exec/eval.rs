use super::filter::evaluate_predicate;
use super::Binding;
use crate::error::Result;
use crate::graph::PropertyValue;
use crate::query::planner::{CaseWhen, PlanExpr, PlanLiteral};

pub(super) fn evaluate_expr(expr: &PlanExpr, binding: &Binding) -> Result<PropertyValue> {
    match expr {
        PlanExpr::Literal(lit) => Ok(match lit {
            PlanLiteral::Null => PropertyValue::Null,
            PlanLiteral::Bool(b) => PropertyValue::Bool(*b),
            PlanLiteral::Int(i) => PropertyValue::Integer(*i),
            PlanLiteral::Float(f) => PropertyValue::Float(*f),
            PlanLiteral::String(s) => PropertyValue::String(s.clone()),
        }),

        PlanExpr::Variable(v) => {
            if let Some(node) = binding.get_node(v) {
                Ok(PropertyValue::Node(node.clone()))
            } else if let Some(relationship) = binding.get_relationship(v) {
                Ok(PropertyValue::Relationship(relationship.clone()))
            } else if let Some(path) = binding.get_path(v) {
                Ok(PropertyValue::Path(path.clone()))
            } else if let Some(scalar) = binding.get_scalar(v) {
                Ok(scalar.clone())
            } else {
                Ok(PropertyValue::Null)
            }
        }

        PlanExpr::Property { variable, property } => {
            if let Some(node) = binding.get_node(variable) {
                Ok(node
                    .properties
                    .get(property)
                    .cloned()
                    .unwrap_or(PropertyValue::Null))
            } else if let Some(relationship) = binding.get_relationship(variable) {
                Ok(relationship
                    .properties
                    .get(property)
                    .cloned()
                    .unwrap_or(PropertyValue::Null))
            } else {
                Ok(PropertyValue::Null)
            }
        }

        PlanExpr::PathLength { path_variable } => {
            if let Some(path) = binding.get_path(path_variable) {
                Ok(PropertyValue::Integer(path.relationships.len() as i64))
            } else {
                Ok(PropertyValue::Null)
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
                            PropertyValue::Node(n) => Ok(PropertyValue::Integer(n.id)),
                            PropertyValue::Relationship(e) => Ok(PropertyValue::Integer(e.id)),
                            _ => Ok(PropertyValue::Null),
                        }
                    } else {
                        Ok(PropertyValue::Null)
                    }
                }
                "TYPE" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let PropertyValue::Relationship(e) = v {
                            Ok(PropertyValue::String(e.rel_type))
                        } else {
                            Ok(PropertyValue::Null)
                        }
                    } else {
                        Ok(PropertyValue::Null)
                    }
                }
                "LABELS" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let PropertyValue::Node(n) = v {
                            let labels: Vec<PropertyValue> =
                                n.labels.into_iter().map(PropertyValue::String).collect();
                            Ok(PropertyValue::List(labels))
                        } else {
                            Ok(PropertyValue::Null)
                        }
                    } else {
                        Ok(PropertyValue::Null)
                    }
                }
                "TOLOWER" | "LOWER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let PropertyValue::String(s) = v {
                            Ok(PropertyValue::String(s.to_lowercase()))
                        } else {
                            Ok(PropertyValue::Null)
                        }
                    } else {
                        Ok(PropertyValue::Null)
                    }
                }
                "TOUPPER" | "UPPER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let PropertyValue::String(s) = v {
                            Ok(PropertyValue::String(s.to_uppercase()))
                        } else {
                            Ok(PropertyValue::Null)
                        }
                    } else {
                        Ok(PropertyValue::Null)
                    }
                }
                _ => Ok(PropertyValue::Null), // Unknown function
            }
        }

        PlanExpr::Case {
            operand,
            whens,
            else_,
        } => {
            let operand_val = operand
                .as_ref()
                .map(|e| evaluate_expr(e, binding))
                .transpose()?;

            for (when, then) in whens {
                let matched = match when {
                    CaseWhen::Predicate(pred) => evaluate_predicate(pred, binding)?,
                    CaseWhen::Value(val_expr) => {
                        let val = evaluate_expr(val_expr, binding)?;
                        match &operand_val {
                            Some(op) => values_equal(op, &val),
                            None => false,
                        }
                    }
                };
                if matched {
                    return evaluate_expr(then, binding);
                }
            }

            // No WHEN matched - return ELSE or NULL
            match else_ {
                Some(e) => evaluate_expr(e, binding),
                None => Ok(PropertyValue::Null),
            }
        }
    }
}

/// Evaluate a plan expression against a binding and return a PropertyValue.
/// Public interface for WITH clause projection.
pub(crate) fn evaluate_expr_pub(
    expr: &PlanExpr,
    binding: &Binding,
) -> crate::error::Result<PropertyValue> {
    evaluate_expr(expr, binding)
}

pub(super) fn values_equal(a: &PropertyValue, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Null, PropertyValue::Null) => false, // NULL != NULL in Cypher
        (PropertyValue::Bool(x), PropertyValue::Bool(y)) => x == y,
        (PropertyValue::Integer(x), PropertyValue::Integer(y)) => x == y,
        (PropertyValue::Float(x), PropertyValue::Float(y)) => (x - y).abs() < f64::EPSILON,
        (PropertyValue::Integer(x), PropertyValue::Float(y))
        | (PropertyValue::Float(y), PropertyValue::Integer(x)) => {
            (*x as f64 - y).abs() < f64::EPSILON
        }
        (PropertyValue::String(x), PropertyValue::String(y)) => x == y,
        _ => false,
    }
}

pub(super) fn compare_values(a: &PropertyValue, b: &PropertyValue) -> Option<i32> {
    match (a, b) {
        (PropertyValue::Integer(x), PropertyValue::Integer(y)) => Some(x.cmp(y) as i32),
        (PropertyValue::Float(x), PropertyValue::Float(y)) => {
            if x < y {
                Some(-1)
            } else if x > y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (PropertyValue::Integer(x), PropertyValue::Float(y)) => {
            let xf = *x as f64;
            if xf < *y {
                Some(-1)
            } else if xf > *y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (PropertyValue::Float(x), PropertyValue::Integer(y)) => {
            let yf = *y as f64;
            if *x < yf {
                Some(-1)
            } else if *x > yf {
                Some(1)
            } else {
                Some(0)
            }
        }
        (PropertyValue::String(x), PropertyValue::String(y)) => Some(x.cmp(y) as i32),
        _ => None,
    }
}
