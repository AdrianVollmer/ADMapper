//! Expression evaluation for WHERE clauses and RETURN items.

use crate::error::{Error, Result};
use crate::graph::PropertyValue;
use crate::query::parser::{BinaryOperator, Expression, Literal, UnaryOperator};

use super::Binding;

// =============================================================================
// Configuration Constants
// =============================================================================

/// Maximum compiled size for user-provided regex patterns (in bytes).
///
/// This limit prevents denial-of-service attacks from pathological regex
/// patterns that could cause excessive memory usage or catastrophic
/// backtracking during compilation.
///
/// 256KB is generous for most legitimate use cases while providing protection
/// against malicious patterns.
const REGEX_SIZE_LIMIT: usize = 256 * 1024;

/// Relative tolerance for float comparison.
///
/// Using a relative tolerance handles both small and large values correctly.
/// For values near zero, the absolute difference must be small. For large values,
/// the relative difference must be small.
const FLOAT_RELATIVE_TOLERANCE: f64 = 1e-10;

/// Compare two floats for approximate equality.
///
/// Uses relative comparison to handle both small and large values correctly.
/// `f64::EPSILON` alone is too strict for large values (e.g., comparing
/// 1e15 with 1e15 + 1 would fail even though they're effectively equal
/// for most practical purposes).
#[inline]
pub(super) fn floats_equal(a: f64, b: f64) -> bool {
    let diff = (a - b).abs();
    let max_abs = a.abs().max(b.abs());

    if max_abs < 1.0 {
        // For small values, use absolute comparison
        diff < FLOAT_RELATIVE_TOLERANCE
    } else {
        // For larger values, use relative comparison
        diff < max_abs * FLOAT_RELATIVE_TOLERANCE
    }
}

/// Filter bindings by WHERE clause predicate.
pub fn filter_bindings_by_where(
    bindings: Vec<Binding>,
    predicate: &Expression,
) -> Result<Vec<Binding>> {
    let mut filtered = Vec::new();

    for binding in bindings {
        let result = evaluate_expression_with_bindings(predicate, &binding)?;
        if is_truthy(&result) {
            filtered.push(binding);
        }
    }

    Ok(filtered)
}

/// Evaluate an expression using bindings (supports multiple variables).
pub fn evaluate_expression_with_bindings(
    expr: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    match expr {
        Expression::Literal(lit) => Ok(literal_to_property_value(lit)),

        Expression::Variable(name) => {
            if let Some(node) = binding.nodes.get(name) {
                Ok(PropertyValue::Map(node.properties.clone()))
            } else if let Some(edge) = binding.edges.get(name) {
                Ok(PropertyValue::Map(edge.properties.clone()))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }

        Expression::Property { base, property } => {
            if let Expression::Variable(base_name) = base.as_ref() {
                if let Some(node) = binding.nodes.get(base_name) {
                    return Ok(node.get(property).cloned().unwrap_or(PropertyValue::Null));
                }
                if let Some(edge) = binding.edges.get(base_name) {
                    return Ok(edge
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(PropertyValue::Null));
                }
            }
            Err(Error::Cypher("Property access on unknown variable".into()))
        }

        Expression::BinaryOp { left, op, right } => {
            evaluate_binary_op_with_bindings(left, *op, right, binding)
        }

        Expression::UnaryOp { op, operand } => {
            evaluate_unary_op_with_bindings(*op, operand, binding)
        }

        Expression::FunctionCall { name, args } => {
            evaluate_function_call_with_bindings(name, args, binding)
        }

        Expression::List(items) => {
            let values: Result<Vec<_>> = items
                .iter()
                .map(|item| evaluate_expression_with_bindings(item, binding))
                .collect();
            Ok(PropertyValue::List(values?))
        }

        _ => Err(Error::Cypher("Expression type not supported".into())),
    }
}

/// Evaluate binary operation with bindings.
fn evaluate_binary_op_with_bindings(
    left: &Expression,
    op: BinaryOperator,
    right: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    // Short-circuit for logical operators
    match op {
        BinaryOperator::And => {
            let left_val = evaluate_expression_with_bindings(left, binding)?;
            if !is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(false));
            }
            let right_val = evaluate_expression_with_bindings(right, binding)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        BinaryOperator::Or => {
            let left_val = evaluate_expression_with_bindings(left, binding)?;
            if is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(true));
            }
            let right_val = evaluate_expression_with_bindings(right, binding)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        _ => {}
    }

    let left_val = evaluate_expression_with_bindings(left, binding)?;
    let right_val = evaluate_expression_with_bindings(right, binding)?;

    match op {
        BinaryOperator::Eq => Ok(PropertyValue::Bool(values_equal(&left_val, &right_val))),
        BinaryOperator::Ne => Ok(PropertyValue::Bool(!values_equal(&left_val, &right_val))),
        BinaryOperator::Lt => {
            compare_values(&left_val, &right_val, |ord| ord == std::cmp::Ordering::Less)
        }
        BinaryOperator::Le => compare_values(&left_val, &right_val, |ord| {
            ord != std::cmp::Ordering::Greater
        }),
        BinaryOperator::Gt => compare_values(&left_val, &right_val, |ord| {
            ord == std::cmp::Ordering::Greater
        }),
        BinaryOperator::Ge => {
            compare_values(&left_val, &right_val, |ord| ord != std::cmp::Ordering::Less)
        }
        BinaryOperator::StartsWith => {
            string_predicate(&left_val, &right_val, |s, p| s.starts_with(p))
        }
        BinaryOperator::EndsWith => string_predicate(&left_val, &right_val, |s, p| s.ends_with(p)),
        BinaryOperator::Contains => string_predicate(&left_val, &right_val, |s, p| s.contains(p)),
        BinaryOperator::RegexMatch => match (&left_val, &right_val) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                // Use RegexBuilder with size limit to prevent DoS from pathological patterns
                match regex::RegexBuilder::new(pattern)
                    .size_limit(REGEX_SIZE_LIMIT)
                    .build()
                {
                    Ok(re) => Ok(PropertyValue::Bool(re.is_match(text))),
                    Err(e) => Err(Error::Cypher(format!("Invalid regex: {}", e))),
                }
            }
            _ => Ok(PropertyValue::Null),
        },
        BinaryOperator::In => {
            if let PropertyValue::List(list) = &right_val {
                Ok(PropertyValue::Bool(
                    list.iter().any(|v| values_equal(&left_val, v)),
                ))
            } else {
                Ok(PropertyValue::Null)
            }
        }
        BinaryOperator::Add => arithmetic_op(&left_val, &right_val, |a, b| a + b, |a, b| a + b),
        BinaryOperator::Sub => arithmetic_op(&left_val, &right_val, |a, b| a - b, |a, b| a - b),
        BinaryOperator::Mul => arithmetic_op(&left_val, &right_val, |a, b| a * b, |a, b| a * b),
        BinaryOperator::Div => arithmetic_op(&left_val, &right_val, |a, b| a / b, |a, b| a / b),
        BinaryOperator::Mod => arithmetic_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b),
        BinaryOperator::Pow => match (&left_val, &right_val) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Float((*a as f64).powf(*b as f64)))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                Ok(PropertyValue::Float(a.powf(*b)))
            }
            _ => Ok(PropertyValue::Null),
        },
        BinaryOperator::Xor => {
            let l = is_truthy(&left_val);
            let r = is_truthy(&right_val);
            Ok(PropertyValue::Bool(l ^ r))
        }
        BinaryOperator::And | BinaryOperator::Or => unreachable!(),
    }
}

/// Evaluate unary operation with bindings.
fn evaluate_unary_op_with_bindings(
    op: UnaryOperator,
    operand: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    let val = evaluate_expression_with_bindings(operand, binding)?;

    match op {
        UnaryOperator::Not => Ok(PropertyValue::Bool(!is_truthy(&val))),
        UnaryOperator::Neg => match val {
            PropertyValue::Integer(n) => Ok(PropertyValue::Integer(-n)),
            PropertyValue::Float(f) => Ok(PropertyValue::Float(-f)),
            _ => Ok(PropertyValue::Null),
        },
        UnaryOperator::IsNull => Ok(PropertyValue::Bool(matches!(val, PropertyValue::Null))),
        UnaryOperator::IsNotNull => Ok(PropertyValue::Bool(!matches!(val, PropertyValue::Null))),
    }
}

/// Evaluate function call with bindings.
pub fn evaluate_function_call_with_bindings(
    name: &str,
    args: &[Expression],
    binding: &Binding,
) -> Result<PropertyValue> {
    let name_upper = name.to_uppercase();

    match name_upper.as_str() {
        "COALESCE" => {
            for arg in args {
                let val = evaluate_expression_with_bindings(arg, binding)?;
                if !matches!(val, PropertyValue::Null) {
                    return Ok(val);
                }
            }
            Ok(PropertyValue::Null)
        }
        "SIZE" | "LENGTH" => {
            if args.len() != 1 {
                return Err(Error::Cypher("size() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::Integer(s.len() as i64)),
                PropertyValue::List(l) => Ok(PropertyValue::Integer(l.len() as i64)),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOLOWER" | "LOWER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toLower() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_lowercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOUPPER" | "UPPER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toUpper() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_uppercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TYPE" => {
            if args.len() != 1 {
                return Err(Error::Cypher("type() requires 1 argument".into()));
            }
            if let Expression::Variable(var_name) = &args[0] {
                if let Some(edge) = binding.edges.get(var_name) {
                    return Ok(PropertyValue::String(edge.edge_type.clone()));
                }
            }
            Ok(PropertyValue::Null)
        }
        "ID" => {
            if args.len() != 1 {
                return Err(Error::Cypher("id() requires 1 argument".into()));
            }
            if let Expression::Variable(var_name) = &args[0] {
                if let Some(node) = binding.nodes.get(var_name) {
                    return Ok(PropertyValue::Integer(node.id));
                }
                if let Some(edge) = binding.edges.get(var_name) {
                    return Ok(PropertyValue::Integer(edge.id));
                }
            }
            Ok(PropertyValue::Null)
        }
        "LABELS" => {
            if args.len() != 1 {
                return Err(Error::Cypher("labels() requires 1 argument".into()));
            }
            if let Expression::Variable(var_name) = &args[0] {
                if let Some(node) = binding.nodes.get(var_name) {
                    let labels: Vec<PropertyValue> = node
                        .labels
                        .iter()
                        .map(|l| PropertyValue::String(l.clone()))
                        .collect();
                    return Ok(PropertyValue::List(labels));
                }
            }
            Ok(PropertyValue::Null)
        }
        _ => Err(Error::Cypher(format!("Unknown function: {}", name))),
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert a literal to a PropertyValue.
pub fn literal_to_property_value(lit: &Literal) -> PropertyValue {
    match lit {
        Literal::Null => PropertyValue::Null,
        Literal::Boolean(b) => PropertyValue::Bool(*b),
        Literal::Integer(n) => PropertyValue::Integer(*n),
        Literal::Float(f) => PropertyValue::Float(*f),
        Literal::String(s) => PropertyValue::String(s.clone()),
    }
}

/// Check if two PropertyValues are equal.
pub fn values_equal(a: &PropertyValue, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => false,
        (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a == b,
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a == b,
        (PropertyValue::Float(a), PropertyValue::Float(b)) => floats_equal(*a, *b),
        (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => floats_equal(*a as f64, *b),
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => floats_equal(*a, *b as f64),
        (PropertyValue::List(a), PropertyValue::List(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal(x, y))
        }
        _ => false,
    }
}

/// Compare two values and apply a comparison function.
fn compare_values<F>(a: &PropertyValue, b: &PropertyValue, cmp: F) -> Result<PropertyValue>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    let ordering = match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => {
            return Ok(PropertyValue::Null);
        }
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        _ => return Ok(PropertyValue::Null),
    };

    Ok(PropertyValue::Bool(cmp(ordering)))
}

/// Apply a string predicate.
fn string_predicate<F>(a: &PropertyValue, b: &PropertyValue, pred: F) -> Result<PropertyValue>
where
    F: Fn(&str, &str) -> bool,
{
    match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
        (PropertyValue::String(s), PropertyValue::String(p)) => Ok(PropertyValue::Bool(pred(s, p))),
        _ => Ok(PropertyValue::Null),
    }
}

/// Apply an arithmetic operation.
fn arithmetic_op<F, G>(
    a: &PropertyValue,
    b: &PropertyValue,
    int_op: F,
    float_op: G,
) -> Result<PropertyValue>
where
    F: Fn(i64, i64) -> i64,
    G: Fn(f64, f64) -> f64,
{
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
            Ok(PropertyValue::Integer(int_op(*a, *b)))
        }
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            Ok(PropertyValue::Float(float_op(*a, *b)))
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
            Ok(PropertyValue::Float(float_op(*a as f64, *b)))
        }
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
            Ok(PropertyValue::Float(float_op(*a, *b as f64)))
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => {
            Ok(PropertyValue::String(format!("{}{}", a, b)))
        }
        _ => Ok(PropertyValue::Null),
    }
}

/// Check if a PropertyValue is truthy.
pub fn is_truthy(val: &PropertyValue) -> bool {
    match val {
        PropertyValue::Null => false,
        PropertyValue::Bool(b) => *b,
        PropertyValue::Integer(n) => *n != 0,
        PropertyValue::Float(f) => *f != 0.0,
        PropertyValue::String(s) => !s.is_empty(),
        PropertyValue::List(l) => !l.is_empty(),
        PropertyValue::Map(m) => !m.is_empty(),
    }
}

/// Check if a node's property value matches an expression.
pub fn property_matches(node_value: &PropertyValue, expr: &Expression) -> bool {
    match expr {
        Expression::Literal(lit) => literal_matches_property(lit, node_value),
        _ => false,
    }
}

/// Check if a literal matches a property value.
pub fn literal_matches_property(lit: &Literal, prop: &PropertyValue) -> bool {
    match (lit, prop) {
        (Literal::Null, _) => false,
        (Literal::Boolean(a), PropertyValue::Bool(b)) => a == b,
        (Literal::Integer(a), PropertyValue::Integer(b)) => a == b,
        (Literal::Float(a), PropertyValue::Float(b)) => floats_equal(*a, *b),
        (Literal::String(a), PropertyValue::String(b)) => a == b,
        (Literal::Integer(a), PropertyValue::Float(b)) => floats_equal(*a as f64, *b),
        (Literal::Float(a), PropertyValue::Integer(b)) => floats_equal(*a, *b as f64),
        _ => false,
    }
}
