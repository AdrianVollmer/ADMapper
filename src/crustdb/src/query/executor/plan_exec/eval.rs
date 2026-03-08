use super::{Binding, Path};
use crate::error::Result;
use crate::graph::{Node, PropertyValue, Relationship};
use crate::query::planner::{PlanExpr, PlanLiteral};

#[derive(Debug, Clone)]
pub(super) enum EvalValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<EvalValue>),
    Node(Node),
    Relationship(Relationship),
    Path(Path),
}

pub(super) fn evaluate_expr(expr: &PlanExpr, binding: &Binding) -> Result<EvalValue> {
    match expr {
        PlanExpr::Literal(lit) => Ok(match lit {
            PlanLiteral::Null => EvalValue::Null,
            PlanLiteral::Bool(b) => EvalValue::Bool(*b),
            PlanLiteral::Int(i) => EvalValue::Int(*i),
            PlanLiteral::Float(f) => EvalValue::Float(*f),
            PlanLiteral::String(s) => EvalValue::String(s.clone()),
        }),

        PlanExpr::Variable(v) => {
            if let Some(node) = binding.get_node(v) {
                Ok(EvalValue::Node(node.clone()))
            } else if let Some(relationship) = binding.get_relationship(v) {
                Ok(EvalValue::Relationship(relationship.clone()))
            } else if let Some(path) = binding.get_path(v) {
                Ok(EvalValue::Path(path.clone()))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::Property { variable, property } => {
            if let Some(node) = binding.get_node(variable) {
                Ok(property_to_eval_value(node.properties.get(property)))
            } else if let Some(relationship) = binding.get_relationship(variable) {
                Ok(property_to_eval_value(
                    relationship.properties.get(property),
                ))
            } else {
                Ok(EvalValue::Null)
            }
        }

        PlanExpr::PathLength { path_variable } => {
            if let Some(path) = binding.get_path(path_variable) {
                Ok(EvalValue::Int(path.relationships.len() as i64))
            } else {
                Ok(EvalValue::Null)
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
                            EvalValue::Node(n) => Ok(EvalValue::Int(n.id)),
                            EvalValue::Relationship(e) => Ok(EvalValue::Int(e.id)),
                            _ => Ok(EvalValue::Null),
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TYPE" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::Relationship(e) = v {
                            Ok(EvalValue::String(e.rel_type))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "LABELS" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::Node(n) = v {
                            let labels: Vec<EvalValue> =
                                n.labels.into_iter().map(EvalValue::String).collect();
                            Ok(EvalValue::List(labels))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TOLOWER" | "LOWER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::String(s) = v {
                            Ok(EvalValue::String(s.to_lowercase()))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                "TOUPPER" | "UPPER" => {
                    if args.len() == 1 {
                        let v = evaluate_expr(&args[0], binding)?;
                        if let EvalValue::String(s) = v {
                            Ok(EvalValue::String(s.to_uppercase()))
                        } else {
                            Ok(EvalValue::Null)
                        }
                    } else {
                        Ok(EvalValue::Null)
                    }
                }
                _ => Ok(EvalValue::Null), // Unknown function
            }
        }
    }
}

pub(super) fn property_to_eval_value(prop: Option<&PropertyValue>) -> EvalValue {
    match prop {
        None => EvalValue::Null,
        Some(PropertyValue::Null) => EvalValue::Null,
        Some(PropertyValue::Bool(b)) => EvalValue::Bool(*b),
        Some(PropertyValue::Integer(i)) => EvalValue::Int(*i),
        Some(PropertyValue::Float(f)) => EvalValue::Float(*f),
        Some(PropertyValue::String(s)) => EvalValue::String(s.clone()),
        Some(PropertyValue::List(items)) => {
            let values: Vec<EvalValue> = items
                .iter()
                .map(|p| property_to_eval_value(Some(p)))
                .collect();
            EvalValue::List(values)
        }
        Some(PropertyValue::Map(_)) => {
            // Maps are not currently supported as eval values
            EvalValue::Null
        }
    }
}

pub(super) fn values_equal(a: &EvalValue, b: &EvalValue) -> bool {
    match (a, b) {
        (EvalValue::Null, EvalValue::Null) => false, // NULL != NULL in Cypher
        (EvalValue::Bool(x), EvalValue::Bool(y)) => x == y,
        (EvalValue::Int(x), EvalValue::Int(y)) => x == y,
        (EvalValue::Float(x), EvalValue::Float(y)) => (x - y).abs() < f64::EPSILON,
        (EvalValue::Int(x), EvalValue::Float(y)) | (EvalValue::Float(y), EvalValue::Int(x)) => {
            (*x as f64 - y).abs() < f64::EPSILON
        }
        (EvalValue::String(x), EvalValue::String(y)) => x == y,
        _ => false,
    }
}

pub(super) fn compare_values(a: &EvalValue, b: &EvalValue) -> Option<i32> {
    match (a, b) {
        (EvalValue::Int(x), EvalValue::Int(y)) => Some(x.cmp(y) as i32),
        (EvalValue::Float(x), EvalValue::Float(y)) => {
            if x < y {
                Some(-1)
            } else if x > y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::Int(x), EvalValue::Float(y)) => {
            let xf = *x as f64;
            if xf < *y {
                Some(-1)
            } else if xf > *y {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::Float(x), EvalValue::Int(y)) => {
            let yf = *y as f64;
            if *x < yf {
                Some(-1)
            } else if *x > yf {
                Some(1)
            } else {
                Some(0)
            }
        }
        (EvalValue::String(x), EvalValue::String(y)) => Some(x.cmp(y) as i32),
        _ => None,
    }
}
