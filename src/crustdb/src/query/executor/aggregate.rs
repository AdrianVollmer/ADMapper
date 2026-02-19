//! Aggregate function evaluation (COUNT, SUM, AVG, etc.).

use crate::error::{Error, Result};
use crate::graph::PropertyValue;
use crate::query::parser::{Expression, ReturnClause};
use crate::query::ResultValue;

use super::eval::evaluate_expression_with_bindings;
use super::Binding;

/// Check if an expression is an aggregate function (count, sum, avg, etc.)
pub fn is_aggregate_function(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall { name, .. } => {
            matches!(
                name.to_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT"
            )
        }
        _ => false,
    }
}

/// Check if any return item contains an aggregate function.
pub fn has_aggregate_functions(return_clause: &ReturnClause) -> bool {
    return_clause
        .items
        .iter()
        .any(|item| is_aggregate_function(&item.expression))
}

/// Evaluate aggregate function over all bindings.
pub fn evaluate_aggregate(expr: &Expression, bindings: &[Binding]) -> Result<ResultValue> {
    if let Expression::FunctionCall { name, args } = expr {
        let name_upper = name.to_uppercase();
        match name_upper.as_str() {
            "COUNT" => {
                if args.is_empty() {
                    // count(*) - count all rows
                    Ok(ResultValue::Property(PropertyValue::Integer(
                        bindings.len() as i64,
                    )))
                } else if let Expression::Variable(var_name) = &args[0] {
                    // count(n) - count rows where variable exists
                    let count = bindings
                        .iter()
                        .filter(|b| {
                            b.nodes.contains_key(var_name) || b.edges.contains_key(var_name)
                        })
                        .count();
                    Ok(ResultValue::Property(PropertyValue::Integer(count as i64)))
                } else {
                    // count(expr) - count non-null values
                    let count = bindings
                        .iter()
                        .filter(|b| {
                            evaluate_expression_with_bindings(&args[0], b)
                                .map(|v| !matches!(v, PropertyValue::Null))
                                .unwrap_or(false)
                        })
                        .count();
                    Ok(ResultValue::Property(PropertyValue::Integer(count as i64)))
                }
            }
            "COLLECT" => {
                if args.is_empty() {
                    return Err(Error::Cypher("collect() requires an argument".into()));
                }
                let values: Vec<PropertyValue> = bindings
                    .iter()
                    .filter_map(|b| evaluate_expression_with_bindings(&args[0], b).ok())
                    .filter(|v| !matches!(v, PropertyValue::Null))
                    .collect();
                Ok(ResultValue::Property(PropertyValue::List(values)))
            }
            _ => Err(Error::Cypher(format!(
                "Aggregate function {} not yet implemented",
                name
            ))),
        }
    } else {
        Err(Error::Cypher("Expected aggregate function".into()))
    }
}
