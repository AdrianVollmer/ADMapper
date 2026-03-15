//! Expression planning - converts AST expressions to plan expressions and predicates.

use super::{AggregateFunction, CaseWhen, FilterPredicate};
use super::{BinaryOperator, Error, Expression, Literal, PlanExpr, PlanLiteral, Result};

/// Convert AST expression to plan expression.
pub(crate) fn plan_expression(expr: &Expression) -> Result<PlanExpr> {
    match expr {
        Expression::Literal(lit) => Ok(PlanExpr::Literal(plan_literal(lit))),
        Expression::Variable(v) => Ok(PlanExpr::Variable(v.clone())),
        Expression::Property { base, property } => {
            if let Expression::Variable(var) = base.as_ref() {
                Ok(PlanExpr::Property {
                    variable: var.clone(),
                    property: property.clone(),
                })
            } else {
                Err(Error::Cypher(
                    "Complex property access not supported".into(),
                ))
            }
        }
        Expression::FunctionCall { name, args } => {
            // Handle length(path) specially
            if name.to_lowercase() == "length" && args.len() == 1 {
                if let Expression::Variable(v) = &args[0] {
                    return Ok(PlanExpr::PathLength {
                        path_variable: v.clone(),
                    });
                }
            }

            let planned_args: Result<Vec<_>> = args.iter().map(plan_expression).collect();
            Ok(PlanExpr::Function {
                name: name.clone(),
                args: planned_args?,
            })
        }
        Expression::Case {
            operand,
            whens,
            else_,
        } => {
            let plan_operand = operand
                .as_ref()
                .map(|e| plan_expression(e).map(Box::new))
                .transpose()?;

            let plan_whens: Result<Vec<_>> = whens
                .iter()
                .map(|(when_expr, then_expr)| {
                    let when = if plan_operand.is_some() {
                        // Simple CASE: WHEN values compared by equality
                        CaseWhen::Value(plan_expression(when_expr)?)
                    } else {
                        // Searched CASE: WHEN predicates
                        CaseWhen::Predicate(plan_expression_as_predicate(when_expr)?)
                    };
                    let then = plan_expression(then_expr)?;
                    Ok((when, then))
                })
                .collect();

            let plan_else = else_
                .as_ref()
                .map(|e| plan_expression(e).map(Box::new))
                .transpose()?;

            Ok(PlanExpr::Case {
                operand: plan_operand,
                whens: plan_whens?,
                else_: plan_else,
            })
        }
        _ => Err(Error::Cypher(format!(
            "Expression type not supported in plan: {:?}",
            expr
        ))),
    }
}

/// Convert expression to filter predicate.
pub(crate) fn plan_expression_as_predicate(expr: &Expression) -> Result<FilterPredicate> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(FilterPredicate::And {
                left: Box::new(plan_expression_as_predicate(left)?),
                right: Box::new(plan_expression_as_predicate(right)?),
            }),
            BinaryOperator::Or => Ok(FilterPredicate::Or {
                left: Box::new(plan_expression_as_predicate(left)?),
                right: Box::new(plan_expression_as_predicate(right)?),
            }),
            BinaryOperator::Eq => Ok(FilterPredicate::Eq {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::Ne => Ok(FilterPredicate::Ne {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::Lt => Ok(FilterPredicate::Lt {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::Le => Ok(FilterPredicate::Le {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::Gt => Ok(FilterPredicate::Gt {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::Ge => Ok(FilterPredicate::Ge {
                left: plan_expression(left)?,
                right: plan_expression(right)?,
            }),
            BinaryOperator::StartsWith => {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    Ok(FilterPredicate::StartsWith {
                        expr: plan_expression(left)?,
                        prefix: s.clone(),
                    })
                } else {
                    Err(Error::Cypher("STARTS WITH requires string literal".into()))
                }
            }
            BinaryOperator::EndsWith => {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    Ok(FilterPredicate::EndsWith {
                        expr: plan_expression(left)?,
                        suffix: s.clone(),
                    })
                } else {
                    Err(Error::Cypher("ENDS WITH requires string literal".into()))
                }
            }
            BinaryOperator::Contains => {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    Ok(FilterPredicate::Contains {
                        expr: plan_expression(left)?,
                        substring: s.clone(),
                    })
                } else {
                    Err(Error::Cypher("CONTAINS requires string literal".into()))
                }
            }
            BinaryOperator::RegexMatch => {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    Ok(FilterPredicate::Regex {
                        expr: plan_expression(left)?,
                        pattern: s.clone(),
                    })
                } else {
                    Err(Error::Cypher("=~ requires string literal pattern".into()))
                }
            }
            BinaryOperator::In => {
                if let Expression::List(items) = right.as_ref() {
                    let list: Result<Vec<PlanExpr>> = items.iter().map(plan_expression).collect();
                    Ok(FilterPredicate::In {
                        expr: plan_expression(left)?,
                        list: list?,
                    })
                } else {
                    Err(Error::Cypher("IN requires a list on the right side".into()))
                }
            }
            _ => Err(Error::Cypher(format!("Operator {:?} not supported", op))),
        },
        Expression::UnaryOp { op, operand } => match op {
            super::super::parser::UnaryOperator::Not => Ok(FilterPredicate::Not {
                inner: Box::new(plan_expression_as_predicate(operand)?),
            }),
            super::super::parser::UnaryOperator::IsNull => Ok(FilterPredicate::IsNull {
                expr: plan_expression(operand)?,
            }),
            super::super::parser::UnaryOperator::IsNotNull => Ok(FilterPredicate::IsNotNull {
                expr: plan_expression(operand)?,
            }),
            super::super::parser::UnaryOperator::Neg => {
                Err(Error::Cypher("Negation not supported as predicate".into()))
            }
        },
        Expression::ListPredicate {
            kind,
            variable,
            list,
            filter,
        } => {
            let planned_filter = match filter {
                Some(f) => Some(Box::new(plan_expression_as_predicate(f)?)),
                None => None,
            };
            Ok(FilterPredicate::ListPredicate {
                kind: *kind,
                variable: variable.clone(),
                list: plan_expression(list)?,
                filter: planned_filter,
            })
        }
        Expression::Literal(Literal::Boolean(true)) => Ok(FilterPredicate::True),
        _ => Err(Error::Cypher(format!(
            "Expression not supported as predicate: {:?}",
            expr
        ))),
    }
}

/// Extract a simple property filter for pushdown (single key-value with literal value).
/// Returns Some((property_name, value)) if the properties are a simple {key: literal}.
pub(super) fn extract_simple_property_filter(
    props: &Expression,
) -> Option<(String, serde_json::Value)> {
    if let Expression::Map(entries) = props {
        // Only push down single property filters for now
        if entries.len() == 1 {
            let (key, value) = entries.iter().next()?;
            // Only push down literal values (not expressions)
            let json_value = match value {
                Expression::Literal(Literal::String(s)) => serde_json::Value::String(s.clone()),
                Expression::Literal(Literal::Integer(n)) => serde_json::Value::Number((*n).into()),
                Expression::Literal(Literal::Float(f)) => serde_json::json!(*f),
                Expression::Literal(Literal::Boolean(b)) => serde_json::Value::Bool(*b),
                Expression::Literal(Literal::Null) => serde_json::Value::Null,
                _ => return None, // Complex expression, can't push down
            };
            return Some((key.clone(), json_value));
        }
    }
    None
}

/// Plan inline properties (e.g., `{name: 'Alice'}`) as a filter predicate.
pub(super) fn plan_inline_properties(
    variable: &str,
    props: &Expression,
) -> Result<FilterPredicate> {
    if let Expression::Map(entries) = props {
        if entries.is_empty() {
            return Ok(FilterPredicate::True);
        }

        let mut predicates: Vec<FilterPredicate> = Vec::new();

        for (key, value) in entries {
            let left = PlanExpr::Property {
                variable: variable.to_string(),
                property: key.clone(),
            };
            let right = plan_expression(value)?;
            predicates.push(FilterPredicate::Eq { left, right });
        }

        // Combine with AND
        let mut result = predicates.pop().unwrap();
        while let Some(pred) = predicates.pop() {
            result = FilterPredicate::And {
                left: Box::new(pred),
                right: Box::new(result),
            };
        }

        Ok(result)
    } else {
        Err(Error::Cypher("Inline properties must be a map".into()))
    }
}

/// Plan properties for CREATE clause.
pub(super) fn plan_properties(props: &Option<Expression>) -> Result<Vec<(String, PlanExpr)>> {
    let Some(props) = props else {
        return Ok(Vec::new());
    };

    if let Expression::Map(entries) = props {
        entries
            .iter()
            .map(|(key, value)| {
                let expr = plan_expression(value)?;
                Ok((key.clone(), expr))
            })
            .collect()
    } else {
        Err(Error::Cypher("Properties must be a map".into()))
    }
}

pub(super) fn plan_literal(lit: &Literal) -> PlanLiteral {
    match lit {
        Literal::Null => PlanLiteral::Null,
        Literal::Boolean(b) => PlanLiteral::Bool(*b),
        Literal::Integer(i) => PlanLiteral::Int(*i),
        Literal::Float(f) => PlanLiteral::Float(*f),
        Literal::String(s) => PlanLiteral::String(s.clone()),
    }
}

// =============================================================================
// Aggregate Detection
// =============================================================================

pub(crate) fn is_aggregate_expression(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall { name, .. } => {
            let upper = name.to_uppercase();
            matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT"
            )
        }
        _ => false,
    }
}

pub(super) fn try_extract_aggregate(expr: &Expression) -> Result<Option<AggregateFunction>> {
    if let Expression::FunctionCall { name, args } = expr {
        let upper = name.to_uppercase();
        match upper.as_str() {
            "COUNT" => {
                let arg = if args.is_empty() {
                    None
                } else {
                    Some(plan_expression(&args[0])?)
                };
                Ok(Some(AggregateFunction::Count(arg)))
            }
            "SUM" => {
                if args.len() != 1 {
                    return Err(Error::Cypher("SUM requires exactly one argument".into()));
                }
                Ok(Some(AggregateFunction::Sum(plan_expression(&args[0])?)))
            }
            "AVG" => {
                if args.len() != 1 {
                    return Err(Error::Cypher("AVG requires exactly one argument".into()));
                }
                Ok(Some(AggregateFunction::Avg(plan_expression(&args[0])?)))
            }
            "MIN" => {
                if args.len() != 1 {
                    return Err(Error::Cypher("MIN requires exactly one argument".into()));
                }
                Ok(Some(AggregateFunction::Min(plan_expression(&args[0])?)))
            }
            "MAX" => {
                if args.len() != 1 {
                    return Err(Error::Cypher("MAX requires exactly one argument".into()));
                }
                Ok(Some(AggregateFunction::Max(plan_expression(&args[0])?)))
            }
            "COLLECT" => {
                if args.len() != 1 {
                    return Err(Error::Cypher(
                        "COLLECT requires exactly one argument".into(),
                    ));
                }
                Ok(Some(AggregateFunction::Collect(plan_expression(&args[0])?)))
            }
            _ => Ok(None),
        }
    } else {
        Ok(None)
    }
}

/// Format expression for default alias.
pub(crate) fn format_expression(expr: &Expression) -> String {
    match expr {
        Expression::Variable(v) => v.clone(),
        Expression::Property { base, property } => {
            format!("{}.{}", format_expression(base), property)
        }
        Expression::FunctionCall { name, args } => {
            let args_str: Vec<String> = args.iter().map(format_expression).collect();
            format!("{}({})", name, args_str.join(", "))
        }
        Expression::Literal(lit) => match lit {
            Literal::Null => "NULL".to_string(),
            Literal::Boolean(b) => b.to_string(),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::String(s) => format!("'{}'", s),
        },
        _ => "expr".to_string(),
    }
}
