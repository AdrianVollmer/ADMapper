//! Query executor - runs parsed statements against the storage backend.

use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue};
use crate::storage::SqliteStorage;
use super::{QueryResult, QueryStats, Row, ResultValue};
use super::parser::{
    Statement, CreateClause, MatchClause, Pattern, PatternElement, NodePattern,
    RelationshipPattern, Expression, Literal, Direction, ReturnClause,
};
use std::collections::HashMap;
use std::time::Instant;

/// Execute a parsed statement against the storage.
pub fn execute(statement: &Statement, storage: &SqliteStorage) -> Result<QueryResult> {
    let start = Instant::now();

    let mut stats = QueryStats::default();

    let result = match statement {
        Statement::Create(create) => {
            execute_create(create, storage, &mut stats)?;
            QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                stats,
            }
        }
        Statement::Match(match_clause) => {
            execute_match(match_clause, storage, &mut stats)?
        }
        Statement::Delete(_) => {
            return Err(Error::Cypher("DELETE execution not yet implemented".into()));
        }
        Statement::Set(_) => {
            return Err(Error::Cypher("SET execution not yet implemented".into()));
        }
        Statement::Merge(_) => {
            return Err(Error::Cypher("MERGE execution not yet implemented".into()));
        }
    };

    let mut result = result;
    result.stats.execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(result)
}

/// Execute a CREATE statement.
fn execute_create(
    create: &CreateClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    // Track variable bindings: variable name -> node ID
    let mut bindings: HashMap<String, i64> = HashMap::new();

    // Process pattern elements in order
    // The pattern alternates: Node, Relationship, Node, Relationship, Node, ...
    let elements = &create.pattern.elements;
    let mut i = 0;

    while i < elements.len() {
        match &elements[i] {
            PatternElement::Node(node_pattern) => {
                // Check if this variable is already bound
                let node_id = if let Some(ref var) = node_pattern.variable {
                    if let Some(&existing_id) = bindings.get(var) {
                        // Variable already bound - use existing node
                        existing_id
                    } else {
                        // Create new node and bind it
                        let id = create_node(node_pattern, storage, stats)?;
                        bindings.insert(var.clone(), id);
                        id
                    }
                } else {
                    // Anonymous node - always create
                    create_node(node_pattern, storage, stats)?
                };

                // Store for relationship lookup (even if we didn't create it)
                let _ = node_id;

                i += 1;
            }
            PatternElement::Relationship(rel_pattern) => {
                // A relationship must be between two nodes
                // The previous element should have been a node (source)
                // The next element should be a node (target)

                if i == 0 {
                    return Err(Error::Cypher("Relationship pattern must follow a node".into()));
                }
                if i + 1 >= elements.len() {
                    return Err(Error::Cypher("Relationship pattern must be followed by a node".into()));
                }

                // Get source node ID from previous node
                let source_id = get_node_id(&elements[i - 1], &bindings)?;

                // Get or create the target node
                let target_node = match &elements[i + 1] {
                    PatternElement::Node(np) => np,
                    _ => return Err(Error::Cypher("Relationship must be followed by a node".into())),
                };

                let target_id = if let Some(ref var) = target_node.variable {
                    if let Some(&existing_id) = bindings.get(var) {
                        // Variable already bound - use existing node
                        existing_id
                    } else {
                        // Create new node and bind it
                        let id = create_node(target_node, storage, stats)?;
                        bindings.insert(var.clone(), id);
                        id
                    }
                } else {
                    // Anonymous node - always create
                    create_node(target_node, storage, stats)?
                };

                // Create the relationship
                create_relationship(rel_pattern, source_id, target_id, storage, stats)?;

                // Skip the relationship and the target node (we already processed it)
                i += 2;
            }
        }
    }

    Ok(())
}

// =============================================================================
// MATCH Execution
// =============================================================================

/// Execute a MATCH statement.
fn execute_match(
    match_clause: &MatchClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<QueryResult> {
    // For now, we only support simple single-node patterns: MATCH (n) or MATCH (n:Label)
    // Get the first node pattern from the MATCH
    let node_pattern = get_single_node_pattern(&match_clause.pattern)?;

    // Scan nodes based on the pattern
    let nodes = scan_nodes(node_pattern, storage)?;

    // Filter by properties if specified in the pattern
    let nodes = filter_by_properties(nodes, node_pattern)?;

    // Filter by WHERE clause if present
    let variable = node_pattern.variable.as_deref().unwrap_or("_");
    let nodes = if let Some(ref where_clause) = match_clause.where_clause {
        filter_by_where(nodes, variable, &where_clause.predicate)?
    } else {
        nodes
    };

    // Build result based on RETURN clause
    let return_clause = match_clause.return_clause.as_ref()
        .ok_or_else(|| Error::Cypher("MATCH requires RETURN clause".into()))?;

    build_match_result(nodes, variable, return_clause, stats)
}

/// Extract a single node pattern from a MATCH pattern.
/// For M3, we only support simple single-node patterns.
fn get_single_node_pattern(pattern: &Pattern) -> Result<&NodePattern> {
    if pattern.elements.len() != 1 {
        return Err(Error::Cypher(
            "Only single-node MATCH patterns are supported (M3)".into()
        ));
    }

    match &pattern.elements[0] {
        PatternElement::Node(np) => Ok(np),
        PatternElement::Relationship(_) => {
            Err(Error::Cypher("MATCH pattern must start with a node".into()))
        }
    }
}

/// Scan nodes from storage based on the node pattern.
/// This implements the Node Scan and Label Filter operators.
fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - scan all nodes
        storage.scan_all_nodes()
    } else {
        // Filter by first label (we can AND multiple labels later)
        let label = &pattern.labels[0];
        let mut nodes = storage.find_nodes_by_label(label)?;

        // If multiple labels, filter to only nodes that have ALL labels
        if pattern.labels.len() > 1 {
            nodes.retain(|node| {
                pattern.labels.iter().all(|l| node.has_label(l))
            });
        }

        Ok(nodes)
    }
}

/// Filter nodes by properties specified in the pattern.
/// This implements the Property Filter operator.
fn filter_by_properties(nodes: Vec<Node>, pattern: &NodePattern) -> Result<Vec<Node>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(nodes);
    };

    // Properties should be a Map expression
    let required_props = match props_expr {
        Expression::Map(entries) => entries,
        _ => return Err(Error::Cypher("Pattern properties must be a map".into())),
    };

    if required_props.is_empty() {
        return Ok(nodes);
    }

    let filtered: Vec<Node> = nodes
        .into_iter()
        .filter(|node| {
            required_props.iter().all(|(key, value)| {
                match node.get(key) {
                    Some(node_value) => property_matches(node_value, value),
                    None => {
                        // Check if required value is null
                        matches!(value, Expression::Literal(Literal::Null))
                    }
                }
            })
        })
        .collect();

    Ok(filtered)
}

/// Check if a node's property value matches an expression.
fn property_matches(node_value: &PropertyValue, expr: &Expression) -> bool {
    match expr {
        Expression::Literal(lit) => literal_matches_property(lit, node_value),
        _ => false, // Complex expressions not supported in property matching
    }
}

/// Check if a literal matches a property value.
fn literal_matches_property(lit: &Literal, prop: &PropertyValue) -> bool {
    match (lit, prop) {
        (Literal::Null, _) => false, // NULL never equals anything
        (Literal::Boolean(a), PropertyValue::Bool(b)) => a == b,
        (Literal::Integer(a), PropertyValue::Integer(b)) => a == b,
        (Literal::Float(a), PropertyValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Literal::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type comparisons
        (Literal::Integer(a), PropertyValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (Literal::Float(a), PropertyValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        _ => false,
    }
}

// =============================================================================
// WHERE Clause Evaluation (M4)
// =============================================================================

use super::parser::BinaryOperator;

/// Filter nodes by WHERE clause predicate.
fn filter_by_where(
    nodes: Vec<Node>,
    variable: &str,
    predicate: &Expression,
) -> Result<Vec<Node>> {
    let mut filtered = Vec::new();

    for node in nodes {
        let result = evaluate_expression(predicate, variable, &node)?;
        if is_truthy(&result) {
            filtered.push(node);
        }
    }

    Ok(filtered)
}

/// Evaluate an expression in the context of a node binding.
fn evaluate_expression(
    expr: &Expression,
    variable: &str,
    node: &Node,
) -> Result<PropertyValue> {
    match expr {
        Expression::Literal(lit) => Ok(literal_to_property_value(lit)),

        Expression::Variable(name) => {
            if name == variable {
                // Return the node as a map of properties
                Ok(PropertyValue::Map(node.properties.clone()))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }

        Expression::Property { base, property } => {
            if let Expression::Variable(base_name) = base.as_ref() {
                if base_name == variable {
                    return Ok(node.get(property).cloned().unwrap_or(PropertyValue::Null));
                }
            }
            Err(Error::Cypher("Property access on non-variable not supported".into()))
        }

        Expression::BinaryOp { left, op, right } => {
            evaluate_binary_op(left, *op, right, variable, node)
        }

        Expression::UnaryOp { op, operand } => {
            evaluate_unary_op(*op, operand, variable, node)
        }

        Expression::FunctionCall { name, args } => {
            evaluate_function_call(name, args, variable, node)
        }

        Expression::List(items) => {
            let values: Result<Vec<_>> = items.iter()
                .map(|item| evaluate_expression(item, variable, node))
                .collect();
            Ok(PropertyValue::List(values?))
        }

        _ => Err(Error::Cypher("Expression type not supported in WHERE clause".into())),
    }
}

/// Convert a literal to a PropertyValue.
fn literal_to_property_value(lit: &Literal) -> PropertyValue {
    match lit {
        Literal::Null => PropertyValue::Null,
        Literal::Boolean(b) => PropertyValue::Bool(*b),
        Literal::Integer(n) => PropertyValue::Integer(*n),
        Literal::Float(f) => PropertyValue::Float(*f),
        Literal::String(s) => PropertyValue::String(s.clone()),
    }
}

/// Evaluate a binary operation.
fn evaluate_binary_op(
    left: &Expression,
    op: BinaryOperator,
    right: &Expression,
    variable: &str,
    node: &Node,
) -> Result<PropertyValue> {
    // For logical operators, we can short-circuit
    match op {
        BinaryOperator::And => {
            let left_val = evaluate_expression(left, variable, node)?;
            if !is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(false));
            }
            let right_val = evaluate_expression(right, variable, node)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        BinaryOperator::Or => {
            let left_val = evaluate_expression(left, variable, node)?;
            if is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(true));
            }
            let right_val = evaluate_expression(right, variable, node)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        BinaryOperator::Xor => {
            let left_val = evaluate_expression(left, variable, node)?;
            let right_val = evaluate_expression(right, variable, node)?;
            let l = is_truthy(&left_val);
            let r = is_truthy(&right_val);
            return Ok(PropertyValue::Bool(l ^ r));
        }
        _ => {}
    }

    let left_val = evaluate_expression(left, variable, node)?;
    let right_val = evaluate_expression(right, variable, node)?;

    match op {
        // Comparison operators
        BinaryOperator::Eq => Ok(PropertyValue::Bool(values_equal(&left_val, &right_val))),
        BinaryOperator::Ne => Ok(PropertyValue::Bool(!values_equal(&left_val, &right_val))),
        BinaryOperator::Lt => compare_values(&left_val, &right_val, |ord| ord == std::cmp::Ordering::Less),
        BinaryOperator::Le => compare_values(&left_val, &right_val, |ord| ord != std::cmp::Ordering::Greater),
        BinaryOperator::Gt => compare_values(&left_val, &right_val, |ord| ord == std::cmp::Ordering::Greater),
        BinaryOperator::Ge => compare_values(&left_val, &right_val, |ord| ord != std::cmp::Ordering::Less),

        // String operators
        BinaryOperator::StartsWith => string_predicate(&left_val, &right_val, |s, p| s.starts_with(p)),
        BinaryOperator::EndsWith => string_predicate(&left_val, &right_val, |s, p| s.ends_with(p)),
        BinaryOperator::Contains => string_predicate(&left_val, &right_val, |s, p| s.contains(p)),

        // IN operator
        BinaryOperator::In => {
            if let PropertyValue::List(list) = &right_val {
                Ok(PropertyValue::Bool(list.iter().any(|v| values_equal(&left_val, v))))
            } else {
                Ok(PropertyValue::Null) // IN on non-list is null
            }
        }

        // Arithmetic operators
        BinaryOperator::Add => arithmetic_op(&left_val, &right_val, |a, b| a + b, |a, b| a + b),
        BinaryOperator::Sub => arithmetic_op(&left_val, &right_val, |a, b| a - b, |a, b| a - b),
        BinaryOperator::Mul => arithmetic_op(&left_val, &right_val, |a, b| a * b, |a, b| a * b),
        BinaryOperator::Div => {
            // Division by zero check
            match &right_val {
                PropertyValue::Integer(0) => {
                    return Err(Error::Cypher("Division by zero".into()));
                }
                PropertyValue::Float(f) if *f == 0.0 => {
                    return Err(Error::Cypher("Division by zero".into()));
                }
                _ => {}
            }
            arithmetic_op(&left_val, &right_val, |a, b| a / b, |a, b| a / b)
        }
        BinaryOperator::Mod => arithmetic_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b),
        BinaryOperator::Pow => {
            match (&left_val, &right_val) {
                (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                    Ok(PropertyValue::Float((*a as f64).powf(*b as f64)))
                }
                (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
                    Ok(PropertyValue::Float(a.powf(*b as f64)))
                }
                (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
                    Ok(PropertyValue::Float((*a as f64).powf(*b)))
                }
                (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                    Ok(PropertyValue::Float(a.powf(*b)))
                }
                _ => Ok(PropertyValue::Null),
            }
        }

        // Already handled above
        BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
            unreachable!()
        }

        BinaryOperator::RegexMatch => {
            match (&left_val, &right_val) {
                (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
                (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                    match regex::Regex::new(pattern) {
                        Ok(re) => Ok(PropertyValue::Bool(re.is_match(text))),
                        Err(e) => Err(Error::Cypher(format!("Invalid regex pattern: {}", e))),
                    }
                }
                _ => Ok(PropertyValue::Null), // Non-string types return NULL
            }
        }
    }
}

/// Check if two PropertyValues are equal.
fn values_equal(a: &PropertyValue, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => false, // NULL never equals anything
        (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a == b,
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a == b,
        (PropertyValue::Float(a), PropertyValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type numeric comparisons
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        // List equality
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
            return Ok(PropertyValue::Null); // Comparisons with NULL return NULL
        }
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        _ => return Ok(PropertyValue::Null), // Incompatible types
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
        (PropertyValue::String(s), PropertyValue::String(p)) => {
            Ok(PropertyValue::Bool(pred(s, p)))
        }
        _ => Ok(PropertyValue::Null), // Non-string types return NULL
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
        // String concatenation with +
        (PropertyValue::String(a), PropertyValue::String(b)) => {
            Ok(PropertyValue::String(format!("{}{}", a, b)))
        }
        _ => Ok(PropertyValue::Null),
    }
}

/// Evaluate a unary operation.
fn evaluate_unary_op(
    op: super::parser::UnaryOperator,
    operand: &Expression,
    variable: &str,
    node: &Node,
) -> Result<PropertyValue> {
    let val = evaluate_expression(operand, variable, node)?;

    match op {
        super::parser::UnaryOperator::Not => {
            Ok(PropertyValue::Bool(!is_truthy(&val)))
        }
        super::parser::UnaryOperator::Neg => {
            match val {
                PropertyValue::Integer(n) => Ok(PropertyValue::Integer(-n)),
                PropertyValue::Float(f) => Ok(PropertyValue::Float(-f)),
                _ => Ok(PropertyValue::Null),
            }
        }
        super::parser::UnaryOperator::IsNull => {
            Ok(PropertyValue::Bool(matches!(val, PropertyValue::Null)))
        }
        super::parser::UnaryOperator::IsNotNull => {
            Ok(PropertyValue::Bool(!matches!(val, PropertyValue::Null)))
        }
    }
}

/// Evaluate a function call.
fn evaluate_function_call(
    name: &str,
    args: &[Expression],
    variable: &str,
    node: &Node,
) -> Result<PropertyValue> {
    let name_upper = name.to_uppercase();

    match name_upper.as_str() {
        // String functions
        "TOLOWER" | "LOWER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toLower() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_lowercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Err(Error::Cypher("toLower() requires a string argument".into())),
            }
        }
        "TOUPPER" | "UPPER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toUpper() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_uppercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Err(Error::Cypher("toUpper() requires a string argument".into())),
            }
        }
        "TRIM" => {
            if args.len() != 1 {
                return Err(Error::Cypher("trim() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.trim().to_string())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Err(Error::Cypher("trim() requires a string argument".into())),
            }
        }
        "SIZE" | "LENGTH" => {
            if args.len() != 1 {
                return Err(Error::Cypher("size() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::Integer(s.len() as i64)),
                PropertyValue::List(l) => Ok(PropertyValue::Integer(l.len() as i64)),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Err(Error::Cypher("size() requires a string or list argument".into())),
            }
        }
        "SUBSTRING" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(Error::Cypher("substring() requires 2 or 3 arguments".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            let start = evaluate_expression(&args[1], variable, node)?;
            let len = if args.len() == 3 {
                Some(evaluate_expression(&args[2], variable, node)?)
            } else {
                None
            };

            match (val, start, len) {
                (PropertyValue::String(s), PropertyValue::Integer(start), None) => {
                    let start = start.max(0) as usize;
                    if start >= s.len() {
                        Ok(PropertyValue::String(String::new()))
                    } else {
                        Ok(PropertyValue::String(s[start..].to_string()))
                    }
                }
                (PropertyValue::String(s), PropertyValue::Integer(start), Some(PropertyValue::Integer(len))) => {
                    let start = start.max(0) as usize;
                    let len = len.max(0) as usize;
                    if start >= s.len() {
                        Ok(PropertyValue::String(String::new()))
                    } else {
                        let end = (start + len).min(s.len());
                        Ok(PropertyValue::String(s[start..end].to_string()))
                    }
                }
                (PropertyValue::Null, _, _) | (_, PropertyValue::Null, _) => Ok(PropertyValue::Null),
                _ => Err(Error::Cypher("substring() requires string and integer arguments".into())),
            }
        }

        // Type coercion
        "TOINTEGER" | "TOINT" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toInteger() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::Integer(n) => Ok(PropertyValue::Integer(n)),
                PropertyValue::Float(f) => Ok(PropertyValue::Integer(f as i64)),
                PropertyValue::String(s) => {
                    s.parse::<i64>()
                        .map(PropertyValue::Integer)
                        .unwrap_or(PropertyValue::Null)
                        .pipe(Ok)
                }
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOFLOAT" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toFloat() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::Integer(n) => Ok(PropertyValue::Float(n as f64)),
                PropertyValue::Float(f) => Ok(PropertyValue::Float(f)),
                PropertyValue::String(s) => {
                    s.parse::<f64>()
                        .map(PropertyValue::Float)
                        .unwrap_or(PropertyValue::Null)
                        .pipe(Ok)
                }
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOSTRING" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toString() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s)),
                PropertyValue::Integer(n) => Ok(PropertyValue::String(n.to_string())),
                PropertyValue::Float(f) => Ok(PropertyValue::String(f.to_string())),
                PropertyValue::Bool(b) => Ok(PropertyValue::String(b.to_string())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }

        // Math functions
        "ABS" => {
            if args.len() != 1 {
                return Err(Error::Cypher("abs() requires exactly 1 argument".into()));
            }
            let val = evaluate_expression(&args[0], variable, node)?;
            match val {
                PropertyValue::Integer(n) => Ok(PropertyValue::Integer(n.abs())),
                PropertyValue::Float(f) => Ok(PropertyValue::Float(f.abs())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }

        // Null handling
        "COALESCE" => {
            for arg in args {
                let val = evaluate_expression(arg, variable, node)?;
                if !matches!(val, PropertyValue::Null) {
                    return Ok(val);
                }
            }
            Ok(PropertyValue::Null)
        }

        _ => Err(Error::Cypher(format!("Unknown function: {}", name))),
    }
}

/// Helper trait for pipe operations.
trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

impl<T> Pipe for T {}

/// Check if a PropertyValue is truthy.
fn is_truthy(val: &PropertyValue) -> bool {
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

/// Build the query result based on the RETURN clause.
/// This implements the Projection operator.
fn build_match_result(
    nodes: Vec<Node>,
    variable: &str,
    return_clause: &ReturnClause,
    _stats: &mut QueryStats,
) -> Result<QueryResult> {
    // Build column names from return items
    let columns: Vec<String> = return_clause.items.iter().map(|item| {
        if let Some(ref alias) = item.alias {
            alias.clone()
        } else {
            expr_to_column_name(&item.expression, variable)
        }
    }).collect();

    // Build rows
    let mut rows = Vec::with_capacity(nodes.len());

    for node in nodes {
        let mut values = HashMap::new();

        for (i, item) in return_clause.items.iter().enumerate() {
            let column_name = &columns[i];
            let value = evaluate_return_item(&item.expression, variable, &node)?;
            values.insert(column_name.clone(), value);
        }

        rows.push(Row { values });
    }

    Ok(QueryResult {
        columns,
        rows,
        stats: QueryStats::default(),
    })
}

/// Convert an expression to a column name.
fn expr_to_column_name(expr: &Expression, default_var: &str) -> String {
    match expr {
        Expression::Variable(name) => name.clone(),
        Expression::Property { base, property } => {
            let base_name = expr_to_column_name(base, default_var);
            format!("{}.{}", base_name, property)
        }
        Expression::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                let arg_str = args.iter()
                    .map(|a| expr_to_column_name(a, default_var))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, arg_str)
            }
        }
        _ => default_var.to_string(),
    }
}

/// Evaluate a return item expression for a given node.
fn evaluate_return_item(
    expr: &Expression,
    variable: &str,
    node: &Node,
) -> Result<ResultValue> {
    match expr {
        Expression::Variable(name) => {
            if name == variable || name == "*" {
                // Return the whole node
                Ok(ResultValue::Node {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                })
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }
        Expression::Property { base, property } => {
            // Check if base is our variable
            if let Expression::Variable(base_name) = base.as_ref() {
                if base_name == variable {
                    // Return the property value
                    let value = node.get(property).cloned()
                        .unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
            }
            Err(Error::Cypher(format!("Cannot access property on non-variable")))
        }
        Expression::Literal(lit) => {
            // Return the literal value
            let prop_value = literal_to_property(lit)?;
            Ok(ResultValue::Property(prop_value))
        }
        _ => Err(Error::Cypher("Complex expressions in RETURN not yet supported".into())),
    }
}

/// Convert a literal to a PropertyValue.
fn literal_to_property(lit: &Literal) -> Result<PropertyValue> {
    Ok(match lit {
        Literal::Null => PropertyValue::Null,
        Literal::Boolean(b) => PropertyValue::Bool(*b),
        Literal::Integer(n) => PropertyValue::Integer(*n),
        Literal::Float(f) => PropertyValue::Float(*f),
        Literal::String(s) => PropertyValue::String(s.clone()),
    })
}

// =============================================================================
// CREATE Helpers
// =============================================================================

/// Get the ID of a node from a pattern element.
fn get_node_id(element: &PatternElement, bindings: &HashMap<String, i64>) -> Result<i64> {
    match element {
        PatternElement::Node(np) => {
            if let Some(ref var) = np.variable {
                if let Some(&id) = bindings.get(var) {
                    return Ok(id);
                }
            }
            // Anonymous node or unbound variable
            Err(Error::Cypher("Cannot reference unbound node in relationship".into()))
        }
        PatternElement::Relationship(_) => {
            Err(Error::Cypher("Expected node, found relationship".into()))
        }
    }
}

/// Create a node from a node pattern.
fn create_node(
    pattern: &NodePattern,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<i64> {
    let labels: Vec<String> = pattern.labels.clone();

    let properties = match &pattern.properties {
        Some(expr) => expression_to_json(expr)?,
        None => serde_json::json!({}),
    };

    let node_id = storage.insert_node(&labels, &properties)?;

    stats.nodes_created += 1;
    stats.labels_added += labels.len();

    // Count properties set
    if let serde_json::Value::Object(map) = &properties {
        stats.properties_set += map.len();
    }

    Ok(node_id)
}

/// Create a relationship from a relationship pattern.
fn create_relationship(
    pattern: &RelationshipPattern,
    source_id: i64,
    target_id: i64,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<i64> {
    // Get the relationship type
    let edge_type = pattern.types.first()
        .ok_or_else(|| Error::Cypher("Relationship must have a type".into()))?;

    let properties = match &pattern.properties {
        Some(expr) => expression_to_json(expr)?,
        None => serde_json::json!({}),
    };

    // Handle direction
    let (actual_source, actual_target) = match pattern.direction {
        Direction::Outgoing => (source_id, target_id),
        Direction::Incoming => (target_id, source_id),
        Direction::Both => {
            // Undirected in CREATE typically means outgoing
            (source_id, target_id)
        }
    };

    let edge_id = storage.insert_edge(actual_source, actual_target, edge_type, &properties)?;

    stats.relationships_created += 1;

    // Count properties set
    if let serde_json::Value::Object(map) = &properties {
        stats.properties_set += map.len();
    }

    Ok(edge_id)
}

/// Convert an AST Expression to a JSON value.
fn expression_to_json(expr: &Expression) -> Result<serde_json::Value> {
    match expr {
        Expression::Literal(lit) => literal_to_json(lit),
        Expression::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries {
                map.insert(key.clone(), expression_to_json(value)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Expression::List(items) => {
            let arr: Result<Vec<_>> = items.iter().map(expression_to_json).collect();
            Ok(serde_json::Value::Array(arr?))
        }
        Expression::Variable(name) => {
            // In CREATE context, variables in properties are not supported
            Err(Error::Cypher(format!("Cannot use variable '{}' in CREATE properties", name)))
        }
        Expression::Parameter(name) => {
            // Parameters not yet supported
            Err(Error::Cypher(format!("Parameters not yet supported: ${}", name)))
        }
        _ => Err(Error::Cypher("Complex expressions not supported in CREATE properties".into())),
    }
}

/// Convert a literal to a JSON value.
fn literal_to_json(lit: &Literal) -> Result<serde_json::Value> {
    Ok(match lit {
        Literal::Null => serde_json::Value::Null,
        Literal::Boolean(b) => serde_json::Value::Bool(*b),
        Literal::Integer(n) => serde_json::Value::Number((*n).into()),
        Literal::Float(f) => {
            serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .ok_or_else(|| Error::Cypher(format!("Invalid float value: {}", f)))?
        }
        Literal::String(s) => serde_json::Value::String(s.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse;

    #[test]
    fn test_create_single_node() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.labels_added, 1);
        assert_eq!(result.stats.properties_set, 2);

        // Verify node was created
        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 1);
    }

    #[test]
    fn test_create_node_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.labels_added, 2);
    }

    #[test]
    fn test_create_two_nodes_with_relationship() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        // Verify in storage
        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
    }

    #[test]
    fn test_create_relationship_with_properties() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.properties_set, 1); // just 'since' on the relationship
    }

    #[test]
    fn test_create_chain_pattern() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 3);
        assert_eq!(result.stats.relationships_created, 2);

        let stats = storage.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 2);
    }

    #[test]
    fn test_create_node_no_properties() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Person)").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 0);
    }

    #[test]
    fn test_create_with_null_property() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Person {name: 'Alice', nickname: null})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);
    }

    #[test]
    fn test_create_with_boolean_property() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Task {done: true})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);

        // Verify node was created correctly
        let nodes = storage.find_nodes_by_label("Task").unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_create_with_float_property() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (n:Measurement {value: 3.14})").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
    }

    #[test]
    fn test_create_incoming_relationship() {
        let storage = SqliteStorage::in_memory().unwrap();
        let stmt = parse("CREATE (a:Person)<-[:FOLLOWS]-(b:Person)").unwrap();

        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        // The edge should go from b to a (because of incoming direction)
        let edges = storage.find_edges_by_type("FOLLOWS").unwrap();
        assert_eq!(edges.len(), 1);
    }

    // =========================================================================
    // MATCH Tests
    // =========================================================================

    #[test]
    fn test_match_all_nodes() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes first
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(), &storage).unwrap();

        // Match all nodes
        let result = execute(&parse("MATCH (n) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.columns, vec!["n"]);
    }

    #[test]
    fn test_match_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(), &storage).unwrap();

        // Match only Person nodes
        let result = execute(&parse("MATCH (n:Person) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_match_by_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with varying labels
        execute(&parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person:Director {name: 'Oliver'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Match only Person+Actor nodes
        let result = execute(&parse("MATCH (n:Person:Actor) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_by_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        // Match by name property
        let result = execute(&parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_return_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();

        // Match and return specific property
        let result = execute(&parse("MATCH (n:Person) RETURN n.name").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns, vec!["n.name"]);

        // Check the value
        let row = &result.rows[0];
        let name = row.values.get("n.name").unwrap();
        assert!(matches!(name, ResultValue::Property(PropertyValue::String(s)) if s == "Alice"));
    }

    #[test]
    fn test_match_return_multiple_properties() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();

        // Match and return multiple properties
        let result = execute(&parse("MATCH (n:Person) RETURN n.name, n.age").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.contains(&"n.name".to_string()));
        assert!(result.columns.contains(&"n.age".to_string()));
    }

    #[test]
    fn test_match_empty_result() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with one label
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Match with non-existent label
        let result = execute(&parse("MATCH (n:Movie) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_property_not_found() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with different properties
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        // Match by property that doesn't match any node
        let result = execute(&parse("MATCH (n:Person {name: 'Charlie'}) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_return_missing_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node without 'age' property
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Return a property that doesn't exist
        let result = execute(&parse("MATCH (n:Person) RETURN n.age").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);

        // Should return null for missing property
        let row = &result.rows[0];
        let age = row.values.get("n.age").unwrap();
        assert!(matches!(age, ResultValue::Property(PropertyValue::Null)));
    }

    // =========================================================================
    // WHERE Tests (M4)
    // =========================================================================

    #[test]
    fn test_where_comparison_greater_than() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.age > 28 RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_where_comparison_less_than_or_equal() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.age <= 25 RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_equality() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_where_inequality() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name <> 'Alice' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_and() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.age >= 25 AND n.age <= 30 RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_where_or() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Charlie' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_where_not() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE NOT n.name = 'Alice' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_starts_with() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Adam'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Adam
    }

    #[test]
    fn test_where_ends_with() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Grace'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name ENDS WITH 'ce' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Grace
    }

    #[test]
    fn test_where_contains() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Alicia'})").unwrap(), &storage).unwrap();

        let result = execute(&parse("MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Alicia
    }

    #[test]
    fn test_where_is_null() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap(); // No age

        let result = execute(&parse("MATCH (n:Person) WHERE n.age IS NULL RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_is_not_null() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap(); // No age

        let result = execute(&parse("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Alice only
    }

    #[test]
    fn test_where_complex_expression() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        // (age > 25 AND age < 35) OR name = 'Charlie'
        let result = execute(&parse("MATCH (n:Person) WHERE (n.age > 25 AND n.age < 35) OR n.name = 'Charlie' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice (30) and Charlie
    }

    #[test]
    fn test_where_regex_match() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Adam'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        // Match names starting with 'A'
        let result = execute(&parse("MATCH (n:Person) WHERE n.name =~ '^A.*' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Adam
    }

    #[test]
    fn test_where_regex_match_case_insensitive() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'bob'})").unwrap(), &storage).unwrap();

        // Case-insensitive match for 'alice'
        let result = execute(&parse("MATCH (n:Person) WHERE n.name =~ '(?i)alice' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_where_regex_match_digit_pattern() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(&parse("CREATE (n:Product {code: 'ABC123'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Product {code: 'XYZ789'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Product {code: 'NoDigits'})").unwrap(), &storage).unwrap();

        // Match codes containing digits
        let result = execute(&parse("MATCH (n:Product) WHERE n.code =~ '.*[0-9]+.*' RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // ABC123 and XYZ789
    }
}
