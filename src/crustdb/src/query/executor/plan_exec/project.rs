use super::{compare_values, eval_to_result_value, evaluate_expr, Binding, ExecutionResult};
use crate::error::Result;
use crate::graph::PropertyValue;
use crate::query::planner::{AggregateFunction, ProjectColumn};
use crate::query::{ResultValue, Row};
use crate::storage::SqliteStorage;
use std::collections::{HashMap, HashSet};

pub(super) fn execute_project(
    bindings: Vec<Binding>,
    columns: &[ProjectColumn],
    distinct: bool,
    _storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let column_names: Vec<String> = columns.iter().map(|c| c.alias.clone()).collect();
    let mut rows = Vec::new();

    for binding in bindings {
        let mut values = HashMap::new();
        for col in columns {
            let value = evaluate_expr(&col.expr, &binding)?;
            values.insert(col.alias.clone(), eval_to_result_value(value));
        }
        rows.push(Row { values });
    }

    if distinct {
        let mut seen: HashSet<Vec<(String, ResultValue)>> = HashSet::new();
        let mut unique_rows = Vec::new();
        for row in rows {
            let mut key: Vec<(String, ResultValue)> = row.values.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            key.sort_by(|(a, _), (b, _)| a.cmp(b));
            if seen.insert(key) {
                unique_rows.push(row);
            }
        }
        rows = unique_rows;
    }

    Ok(ExecutionResult::Rows {
        columns: column_names,
        rows,
    })
}

pub(super) fn execute_aggregate(
    bindings: Vec<Binding>,
    group_by: &[ProjectColumn],
    aggregates: &[crate::query::planner::AggregateColumn],
    _storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    // If no GROUP BY, treat all rows as one group
    if group_by.is_empty() {
        let mut values = HashMap::new();

        for agg in aggregates {
            let result = compute_aggregate(&agg.function, &bindings)?;
            values.insert(agg.alias.clone(), result);
        }

        let columns: Vec<String> = aggregates.iter().map(|a| a.alias.clone()).collect();
        return Ok(ExecutionResult::Rows {
            columns,
            rows: vec![Row { values }],
        });
    }

    // Group by implementation
    let mut groups: HashMap<String, Vec<Binding>> = HashMap::new();

    for binding in bindings {
        let mut key_parts = Vec::new();
        for col in group_by {
            let v = evaluate_expr(&col.expr, &binding)?;
            key_parts.push(format!("{:?}", v));
        }
        let key = key_parts.join("|");
        groups.entry(key).or_default().push(binding);
    }

    let mut columns: Vec<String> = group_by.iter().map(|c| c.alias.clone()).collect();
    columns.extend(aggregates.iter().map(|a| a.alias.clone()));

    let mut rows = Vec::new();
    for (_, group_bindings) in groups {
        let first = &group_bindings[0];
        let mut values = HashMap::new();

        // Add GROUP BY columns
        for col in group_by {
            let v = evaluate_expr(&col.expr, first)?;
            values.insert(col.alias.clone(), eval_to_result_value(v));
        }

        // Add aggregates
        for agg in aggregates {
            let result = compute_aggregate(&agg.function, &group_bindings)?;
            values.insert(agg.alias.clone(), result);
        }

        rows.push(Row { values });
    }

    Ok(ExecutionResult::Rows { columns, rows })
}

pub(super) fn compute_aggregate(
    func: &AggregateFunction,
    bindings: &[Binding],
) -> Result<ResultValue> {
    match func {
        AggregateFunction::Count(arg) => {
            let count = if let Some(expr) = arg {
                bindings
                    .iter()
                    .filter(|b| !matches!(evaluate_expr(expr, b), Ok(PropertyValue::Null)))
                    .count()
            } else {
                bindings.len()
            };
            Ok(ResultValue::Property(PropertyValue::Integer(count as i64)))
        }

        AggregateFunction::Sum(expr) => {
            let mut sum = 0.0;
            let mut is_int = true;
            for b in bindings {
                match evaluate_expr(expr, b)? {
                    PropertyValue::Integer(i) => sum += i as f64,
                    PropertyValue::Float(f) => {
                        sum += f;
                        is_int = false;
                    }
                    _ => {}
                }
            }
            if is_int {
                Ok(ResultValue::Property(PropertyValue::Integer(sum as i64)))
            } else {
                Ok(ResultValue::Property(PropertyValue::Float(sum)))
            }
        }

        AggregateFunction::Avg(expr) => {
            let mut sum = 0.0;
            let mut count = 0;
            for b in bindings {
                match evaluate_expr(expr, b)? {
                    PropertyValue::Integer(i) => {
                        sum += i as f64;
                        count += 1;
                    }
                    PropertyValue::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Ok(ResultValue::Property(PropertyValue::Float(
                    sum / count as f64,
                )))
            } else {
                Ok(ResultValue::Property(PropertyValue::Null))
            }
        }

        AggregateFunction::Min(expr) => {
            let mut min: Option<PropertyValue> = None;
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, PropertyValue::Null) {
                    min = Some(match min {
                        None => v,
                        Some(m) => {
                            if compare_values(&v, &m).map(|c| c < 0).unwrap_or(false) {
                                v
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            Ok(eval_to_result_value(min.unwrap_or(PropertyValue::Null)))
        }

        AggregateFunction::Max(expr) => {
            let mut max: Option<PropertyValue> = None;
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, PropertyValue::Null) {
                    max = Some(match max {
                        None => v,
                        Some(m) => {
                            if compare_values(&v, &m).map(|c| c > 0).unwrap_or(false) {
                                v
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            Ok(eval_to_result_value(max.unwrap_or(PropertyValue::Null)))
        }

        AggregateFunction::Collect(expr) => {
            let mut items = Vec::new();
            for b in bindings {
                let v = evaluate_expr(expr, b)?;
                if !matches!(v, PropertyValue::Null) {
                    items.push(v);
                }
            }
            Ok(ResultValue::Property(PropertyValue::List(items)))
        }
    }
}

pub(super) fn execute_count_pushdown(
    label: Option<&str>,
    alias: &str,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let count = if let Some(l) = label {
        storage.count_nodes_by_label(l)?
    } else {
        storage.count_nodes()?
    };

    let mut values = HashMap::new();
    values.insert(
        alias.to_string(),
        ResultValue::Property(PropertyValue::Integer(count as i64)),
    );

    Ok(ExecutionResult::Rows {
        columns: vec![alias.to_string()],
        rows: vec![Row { values }],
    })
}

/// Execute relationship count pushdown - uses SQL COUNT instead of expanding.
pub(super) fn execute_relationship_count_pushdown(
    rel_type: Option<&str>,
    alias: &str,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let count = if let Some(t) = rel_type {
        storage.count_relationships_by_type(t)?
    } else {
        storage.count_relationships()?
    };

    let mut values = HashMap::new();
    values.insert(
        alias.to_string(),
        ResultValue::Property(PropertyValue::Integer(count as i64)),
    );

    Ok(ExecutionResult::Rows {
        columns: vec![alias.to_string()],
        rows: vec![Row { values }],
    })
}

/// Execute relationship types scan - returns all distinct relationship types.
///
/// This is O(distinct_types) instead of O(relationships) because it queries the
/// normalized rel_types table directly rather than scanning all relationships.
pub(super) fn execute_relationship_types_scan(
    alias: &str,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    let rel_types = storage.get_all_relationship_types()?;

    let rows: Vec<Row> = rel_types
        .into_iter()
        .map(|type_name| {
            let mut values = HashMap::new();
            values.insert(
                alias.to_string(),
                ResultValue::Property(PropertyValue::String(type_name)),
            );
            Row { values }
        })
        .collect();

    Ok(ExecutionResult::Rows {
        columns: vec![alias.to_string()],
        rows,
    })
}
