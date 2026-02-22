//! Result building from bindings.

use crate::error::{Error, Result};
use crate::graph::PropertyValue;
use crate::query::parser::{Expression, ReturnClause};
use crate::query::{PathEdge, PathNode, QueryResult, QueryStats, ResultValue, Row};
use std::collections::HashMap;

use super::aggregate::{evaluate_aggregate, has_aggregate_functions, is_aggregate_function};
use super::eval::{
    evaluate_expression_with_bindings, evaluate_function_call_with_bindings,
    literal_to_property_value,
};
use super::Binding;

/// Build query result from bindings.
pub fn build_match_result_from_bindings(
    bindings: Vec<Binding>,
    return_clause: &ReturnClause,
    _stats: &mut QueryStats,
) -> Result<QueryResult> {
    // Build column names
    let columns: Vec<String> = return_clause
        .items
        .iter()
        .map(|item| {
            if let Some(ref alias) = item.alias {
                alias.clone()
            } else {
                expr_to_column_name_generic(&item.expression)
            }
        })
        .collect();

    // Check if we have aggregate functions - if so, return single aggregated row
    if has_aggregate_functions(return_clause) {
        let mut values = HashMap::new();

        for (i, item) in return_clause.items.iter().enumerate() {
            let column_name = &columns[i];
            let value = if is_aggregate_function(&item.expression) {
                evaluate_aggregate(&item.expression, &bindings)?
            } else if bindings.is_empty() {
                ResultValue::Property(PropertyValue::Null)
            } else {
                evaluate_return_item_with_bindings(&item.expression, &bindings[0])?
            };
            values.insert(column_name.clone(), value);
        }

        return Ok(QueryResult {
            columns,
            rows: vec![Row { values }],
            stats: QueryStats::default(),
        });
    }

    // Build rows (non-aggregate case)
    let mut rows = Vec::with_capacity(bindings.len());

    for binding in bindings {
        let mut values = HashMap::new();

        for (i, item) in return_clause.items.iter().enumerate() {
            let column_name = &columns[i];
            let value = evaluate_return_item_with_bindings(&item.expression, &binding)?;
            values.insert(column_name.clone(), value);
        }

        rows.push(Row { values });
    }

    // Apply SKIP
    if let Some(skip_count) = return_clause.skip {
        let skip = skip_count as usize;
        if skip >= rows.len() {
            rows.clear();
        } else {
            rows.drain(..skip);
        }
    }

    // Apply LIMIT
    if let Some(limit_count) = return_clause.limit {
        rows.truncate(limit_count as usize);
    }

    Ok(QueryResult {
        columns,
        rows,
        stats: QueryStats::default(),
    })
}

/// Convert expression to column name.
fn expr_to_column_name_generic(expr: &Expression) -> String {
    match expr {
        Expression::Variable(name) => name.clone(),
        Expression::Property { base, property } => {
            let base_name = expr_to_column_name_generic(base);
            format!("{}.{}", base_name, property)
        }
        Expression::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                let arg_str = args
                    .iter()
                    .map(expr_to_column_name_generic)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, arg_str)
            }
        }
        _ => "expr".to_string(),
    }
}

/// Evaluate return item expression with bindings.
pub fn evaluate_return_item_with_bindings(
    expr: &Expression,
    binding: &Binding,
) -> Result<ResultValue> {
    match expr {
        Expression::Variable(name) => {
            if let Some(node) = binding.get_node(name) {
                Ok(ResultValue::Node {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                })
            } else if let Some(edge) = binding.get_edge(name) {
                Ok(ResultValue::Edge {
                    id: edge.id,
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type.clone(),
                    properties: edge.properties.clone(),
                })
            } else if let Some(path) = binding.get_path(name) {
                Ok(ResultValue::Path {
                    nodes: path
                        .nodes
                        .iter()
                        .map(|n| PathNode {
                            id: n.id,
                            labels: n.labels.clone(),
                            properties: n.properties.clone(),
                        })
                        .collect(),
                    edges: path
                        .edges
                        .iter()
                        .map(|e| PathEdge {
                            id: e.id,
                            source: e.source,
                            target: e.target,
                            edge_type: e.edge_type.clone(),
                            properties: e.properties.clone(),
                        })
                        .collect(),
                })
            } else if let Some(edge_list) = binding.get_edge_list(name) {
                let list: Vec<PropertyValue> = edge_list
                    .iter()
                    .map(|e| PropertyValue::Map(e.properties.clone()))
                    .collect();
                Ok(ResultValue::Property(PropertyValue::List(list)))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }
        Expression::Property { base, property } => {
            if let Expression::Variable(base_name) = base.as_ref() {
                if let Some(node) = binding.get_node(base_name) {
                    let value = node.get(property).cloned().unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
                if let Some(edge) = binding.get_edge(base_name) {
                    let value = edge
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
            }
            Err(Error::Cypher("Property access on unknown variable".into()))
        }
        Expression::Literal(lit) => {
            let prop_value = literal_to_property_value(lit);
            Ok(ResultValue::Property(prop_value))
        }
        Expression::FunctionCall { name, args } => {
            let name_upper = name.to_uppercase();
            match name_upper.as_str() {
                "LENGTH" => {
                    if args.len() != 1 {
                        return Err(Error::Cypher("length() requires 1 argument".into()));
                    }
                    if let Expression::Variable(var_name) = &args[0] {
                        if let Some(path) = binding.get_path(var_name) {
                            return Ok(ResultValue::Property(PropertyValue::Integer(
                                path.edges.len() as i64,
                            )));
                        }
                    }
                    let val = evaluate_expression_with_bindings(&args[0], binding)?;
                    match val {
                        PropertyValue::String(s) => Ok(ResultValue::Property(
                            PropertyValue::Integer(s.len() as i64),
                        )),
                        PropertyValue::List(l) => Ok(ResultValue::Property(
                            PropertyValue::Integer(l.len() as i64),
                        )),
                        PropertyValue::Null => Ok(ResultValue::Property(PropertyValue::Null)),
                        _ => Ok(ResultValue::Property(PropertyValue::Null)),
                    }
                }
                "NODES" => {
                    if args.len() != 1 {
                        return Err(Error::Cypher("nodes() requires 1 argument".into()));
                    }
                    if let Expression::Variable(var_name) = &args[0] {
                        if let Some(path) = binding.get_path(var_name) {
                            let list: Vec<PropertyValue> = path
                                .nodes
                                .iter()
                                .map(|n| PropertyValue::Integer(n.id))
                                .collect();
                            return Ok(ResultValue::Property(PropertyValue::List(list)));
                        }
                    }
                    Err(Error::Cypher("nodes() requires a path argument".into()))
                }
                "RELATIONSHIPS" | "RELS" => {
                    if args.len() != 1 {
                        return Err(Error::Cypher("relationships() requires 1 argument".into()));
                    }
                    if let Expression::Variable(var_name) = &args[0] {
                        if let Some(path) = binding.get_path(var_name) {
                            let list: Vec<PropertyValue> = path
                                .edges
                                .iter()
                                .map(|e| PropertyValue::Integer(e.id))
                                .collect();
                            return Ok(ResultValue::Property(PropertyValue::List(list)));
                        }
                    }
                    Err(Error::Cypher(
                        "relationships() requires a path argument".into(),
                    ))
                }
                _ => {
                    let val = evaluate_function_call_with_bindings(name, args, binding)?;
                    Ok(ResultValue::Property(val))
                }
            }
        }
        _ => Err(Error::Cypher(
            "Complex expressions in RETURN not yet supported".into(),
        )),
    }
}
