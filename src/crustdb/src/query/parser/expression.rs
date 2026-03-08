//! Expression-related builders for the Cypher parser.

use super::super::ast::{
    BinaryOperator, Expression, ListPredicateKind, Literal, Pattern, UnaryOperator,
};
use super::Rule;
use crate::error::{Error, Result};
use pest::iterators::Pair;

/// Build an expression from an Expression rule.
pub(super) fn build_expression(pair: Pair<Rule>) -> Result<Expression> {
    // Expression -> OrExpression -> XorExpression -> AndExpression -> ...
    // The pair might be Expression or OrExpression directly
    match pair.as_rule() {
        Rule::Expression => {
            // Descend into OrExpression
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::OrExpression {
                    return build_or_expression(inner);
                }
            }
            Err(Error::Parse("Expression requires OrExpression".into()))
        }
        Rule::OrExpression => build_or_expression(pair),
        _ => Err(Error::Parse(format!(
            "Unexpected rule in expression: {:?}",
            pair.as_rule()
        ))),
    }
}

/// Build an OR expression.
fn build_or_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut operands = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::XorExpression {
            operands.push(build_xor_expression(inner)?);
        }
    }

    if operands.is_empty() {
        return Err(Error::Parse("OR expression requires operands".into()));
    }

    let mut result = operands.remove(0);
    for operand in operands {
        result = Expression::BinaryOp {
            left: Box::new(result),
            op: BinaryOperator::Or,
            right: Box::new(operand),
        };
    }

    Ok(result)
}

/// Build an XOR expression.
fn build_xor_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut operands = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::AndExpression {
            operands.push(build_and_expression(inner)?);
        }
    }

    if operands.is_empty() {
        return Err(Error::Parse("XOR expression requires operands".into()));
    }

    let mut result = operands.remove(0);
    for operand in operands {
        result = Expression::BinaryOp {
            left: Box::new(result),
            op: BinaryOperator::Xor,
            right: Box::new(operand),
        };
    }

    Ok(result)
}

/// Build an AND expression.
fn build_and_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut operands = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::NotExpression {
            operands.push(build_not_expression(inner)?);
        }
    }

    if operands.is_empty() {
        return Err(Error::Parse("AND expression requires operands".into()));
    }

    let mut result = operands.remove(0);
    for operand in operands {
        result = Expression::BinaryOp {
            left: Box::new(result),
            op: BinaryOperator::And,
            right: Box::new(operand),
        };
    }

    Ok(result)
}

/// Build a NOT expression.
fn build_not_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut not_count = 0;
    let mut inner_expr = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NOT => not_count += 1,
            Rule::ComparisonExpression => {
                inner_expr = Some(build_comparison_expression(inner)?);
            }
            _ => {}
        }
    }

    let mut expr =
        inner_expr.ok_or_else(|| Error::Parse("NOT expression requires operand".into()))?;

    // Apply NOT operators (odd number means negate)
    for _ in 0..(not_count % 2) {
        expr = Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand: Box::new(expr),
        };
    }

    Ok(expr)
}

/// Build a comparison expression.
fn build_comparison_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut parts: Vec<Pair<Rule>> = pair.into_inner().collect();

    if parts.is_empty() {
        return Err(Error::Parse("Empty comparison expression".into()));
    }

    // First element should be AddOrSubtractExpression
    let first = parts.remove(0);
    let mut left = build_add_or_subtract_expression(first)?;

    // Process partial comparisons
    for part in parts {
        if part.as_rule() == Rule::PartialComparisonExpression {
            let (op, right) = build_partial_comparison(part)?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
    }

    Ok(left)
}

/// Build a partial comparison expression.
fn build_partial_comparison(pair: Pair<Rule>) -> Result<(BinaryOperator, Expression)> {
    let mut op = None;
    let mut right = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::EQ => op = Some(BinaryOperator::Eq),
            Rule::NE => op = Some(BinaryOperator::Ne),
            Rule::LT => op = Some(BinaryOperator::Lt),
            Rule::GT => op = Some(BinaryOperator::Gt),
            Rule::LE => op = Some(BinaryOperator::Le),
            Rule::GE => op = Some(BinaryOperator::Ge),
            Rule::REGEX_MATCH => op = Some(BinaryOperator::RegexMatch),
            Rule::AddOrSubtractExpression => {
                right = Some(build_add_or_subtract_expression(inner)?);
            }
            _ => {}
        }
    }

    let op = op.ok_or_else(|| Error::Parse("Comparison requires operator".into()))?;
    let right = right.ok_or_else(|| Error::Parse("Comparison requires right operand".into()))?;
    Ok((op, right))
}

/// Build an add/subtract expression.
fn build_add_or_subtract_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut parts: Vec<(Option<BinaryOperator>, Pair<Rule>)> = Vec::new();
    let mut pending_op = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::MultiplyDivideModuloExpression => {
                parts.push((pending_op.take(), inner));
            }
            Rule::ADD => pending_op = Some(BinaryOperator::Add),
            Rule::SUBTRACT => pending_op = Some(BinaryOperator::Sub),
            _ => {}
        }
    }

    if parts.is_empty() {
        return Err(Error::Parse("Empty add/subtract expression".into()));
    }

    let (_, first) = parts.remove(0);
    let mut result = build_multiply_divide_expression(first)?;

    for (op, part) in parts {
        let op = op.ok_or_else(|| Error::Parse("Binary expression requires operator".into()))?;
        let right = build_multiply_divide_expression(part)?;
        result = Expression::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }

    Ok(result)
}

/// Build a multiply/divide/modulo expression.
fn build_multiply_divide_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut parts: Vec<(Option<BinaryOperator>, Pair<Rule>)> = Vec::new();
    let mut pending_op = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PowerOfExpression => {
                parts.push((pending_op.take(), inner));
            }
            Rule::MULTIPLY => pending_op = Some(BinaryOperator::Mul),
            Rule::DIVIDE => pending_op = Some(BinaryOperator::Div),
            Rule::MODULO => pending_op = Some(BinaryOperator::Mod),
            _ => {}
        }
    }

    if parts.is_empty() {
        return Err(Error::Parse("Empty multiply/divide expression".into()));
    }

    let (_, first) = parts.remove(0);
    let mut result = build_power_expression(first)?;

    for (op, part) in parts {
        let op = op.ok_or_else(|| Error::Parse("Binary expression requires operator".into()))?;
        let right = build_power_expression(part)?;
        result = Expression::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }

    Ok(result)
}

/// Build a power expression.
fn build_power_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut parts: Vec<Pair<Rule>> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::UnaryAddOrSubtractExpression => {
                parts.push(inner);
            }
            Rule::POW => {}
            _ => {}
        }
    }

    if parts.is_empty() {
        return Err(Error::Parse("Empty power expression".into()));
    }

    // Power is right-associative
    let mut result = build_unary_add_subtract_expression(parts.pop().unwrap())?;
    while let Some(part) = parts.pop() {
        let left = build_unary_add_subtract_expression(part)?;
        result = Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Pow,
            right: Box::new(result),
        };
    }

    Ok(result)
}

/// Build a unary add/subtract expression.
fn build_unary_add_subtract_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut negate = false;
    let mut inner_expr = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SUBTRACT => negate = !negate,
            Rule::ADD => {}
            Rule::StringListNullOperatorExpression => {
                inner_expr = Some(build_string_list_null_expression(inner)?);
            }
            _ => {}
        }
    }

    let mut expr =
        inner_expr.ok_or_else(|| Error::Parse("Unary expression requires operand".into()))?;

    if negate {
        expr = Expression::UnaryOp {
            op: UnaryOperator::Neg,
            operand: Box::new(expr),
        };
    }

    Ok(expr)
}

/// Build a string/list/null operator expression.
pub(super) fn build_string_list_null_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut base = None;
    let mut operations: Vec<Pair<Rule>> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PropertyOrLabelsExpression => {
                base = Some(build_property_or_labels_expression(inner)?);
            }
            Rule::StringOperatorExpression
            | Rule::ListOperatorExpression
            | Rule::NullOperatorExpression => {
                operations.push(inner);
            }
            _ => {}
        }
    }

    let mut result =
        base.ok_or_else(|| Error::Parse("String/list/null expression requires base".into()))?;

    for op in operations {
        result = apply_string_list_null_operator(result, op)?;
    }

    Ok(result)
}

/// Apply a string/list/null operator to an expression.
pub(super) fn apply_string_list_null_operator(
    base: Expression,
    pair: Pair<Rule>,
) -> Result<Expression> {
    match pair.as_rule() {
        Rule::NullOperatorExpression => {
            let mut is_not = false;
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::NOT {
                    is_not = true;
                }
            }
            Ok(Expression::UnaryOp {
                op: if is_not {
                    UnaryOperator::IsNotNull
                } else {
                    UnaryOperator::IsNull
                },
                operand: Box::new(base),
            })
        }
        Rule::StringOperatorExpression => {
            let mut op = None;
            let mut operand = None;

            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::STARTS => op = Some(BinaryOperator::StartsWith),
                    Rule::ENDS => op = Some(BinaryOperator::EndsWith),
                    Rule::CONTAINS => op = Some(BinaryOperator::Contains),
                    Rule::PropertyOrLabelsExpression => {
                        operand = Some(build_property_or_labels_expression(inner)?);
                    }
                    _ => {}
                }
            }

            let op = op.ok_or_else(|| Error::Parse("String operator missing".into()))?;
            let operand =
                operand.ok_or_else(|| Error::Parse("String operator requires operand".into()))?;
            Ok(Expression::BinaryOp {
                left: Box::new(base),
                op,
                right: Box::new(operand),
            })
        }
        Rule::ListOperatorExpression => {
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::IN => {
                        // Continue to find the operand
                    }
                    Rule::PropertyOrLabelsExpression => {
                        let operand = build_property_or_labels_expression(inner)?;
                        return Ok(Expression::BinaryOp {
                            left: Box::new(base),
                            op: BinaryOperator::In,
                            right: Box::new(operand),
                        });
                    }
                    Rule::Expression => {
                        // List indexing [expr] or slicing [expr..expr]
                        // For now, return an error as we don't have List indexing in our AST
                        return Err(Error::Parse("List indexing not yet supported".into()));
                    }
                    _ => {}
                }
            }
            Err(Error::Parse("Invalid list operator".into()))
        }
        _ => Err(Error::Parse("Unknown string/list/null operator".into())),
    }
}

/// Build a property or labels expression.
pub(super) fn build_property_or_labels_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut base = None;
    let mut property_lookups: Vec<String> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Atom => {
                base = Some(build_atom(inner)?);
            }
            Rule::PropertyLookup => {
                for lookup_inner in inner.into_inner() {
                    if lookup_inner.as_rule() == Rule::PropertyKeyName {
                        property_lookups.push(extract_schema_name(lookup_inner)?);
                    }
                }
            }
            Rule::NodeLabels => {
                // Label check expression - not commonly used in expressions
                // Skip for now
            }
            _ => {}
        }
    }

    let mut result =
        base.ok_or_else(|| Error::Parse("Property expression requires atom".into()))?;

    // Apply property lookups
    for prop in property_lookups {
        result = Expression::Property {
            base: Box::new(result),
            property: prop,
        };
    }

    Ok(result)
}

/// Build an atom (literal, variable, function call, etc.).
pub(super) fn build_atom(pair: Pair<Rule>) -> Result<Expression> {
    let children: Vec<Pair<Rule>> = pair.into_inner().collect();

    for (i, inner) in children.iter().enumerate() {
        match inner.as_rule() {
            Rule::Literal => {
                return build_literal(inner.clone());
            }
            Rule::Variable => {
                return Ok(Expression::Variable(extract_variable(inner.clone())?));
            }
            Rule::Parameter => {
                return build_parameter(inner.clone());
            }
            Rule::FunctionInvocation => {
                return build_function_invocation(inner.clone());
            }
            Rule::ParenthesizedExpression => {
                for paren_inner in inner.clone().into_inner() {
                    if paren_inner.as_rule() == Rule::Expression {
                        return build_expression(paren_inner);
                    }
                }
            }
            Rule::CaseExpression => {
                return build_case_expression(inner.clone());
            }
            Rule::COUNT => {
                return Ok(Expression::FunctionCall {
                    name: "count".into(),
                    args: vec![Expression::Variable("*".into())],
                });
            }
            Rule::ListComprehension => {
                return Err(Error::Parse("List comprehension not yet supported".into()));
            }
            Rule::PatternComprehension => {
                return Err(Error::Parse(
                    "Pattern comprehension not yet supported".into(),
                ));
            }
            Rule::ShortestPathPattern => {
                return build_shortest_path_pattern(inner.clone());
            }
            Rule::ALL | Rule::ANY_ | Rule::NONE | Rule::SINGLE => {
                let kind = match inner.as_rule() {
                    Rule::ALL => ListPredicateKind::All,
                    Rule::ANY_ => ListPredicateKind::Any,
                    Rule::NONE => ListPredicateKind::None,
                    Rule::SINGLE => ListPredicateKind::Single,
                    _ => unreachable!(),
                };
                // FilterExpression is the next sibling
                let filter_expr = children
                    .get(i + 1)
                    .ok_or_else(|| Error::Parse("Missing FilterExpression".into()))?;
                return build_list_predicate(filter_expr.clone(), kind);
            }
            _ => {}
        }
    }
    Err(Error::Parse("Unknown atom type".into()))
}

/// Build a list predicate expression: ALL/ANY/NONE/SINGLE(var IN list WHERE pred).
///
/// Grammar: FilterExpression = { IdInColl ~ (SP? ~ Where)? }
///          IdInColl = { Variable ~ SP ~ IN ~ SP ~ Expression }
fn build_list_predicate(filter_pair: Pair<Rule>, kind: ListPredicateKind) -> Result<Expression> {
    let mut variable = None;
    let mut list_expr = None;
    let mut filter = None;

    for inner in filter_pair.into_inner() {
        match inner.as_rule() {
            Rule::IdInColl => {
                for id_inner in inner.into_inner() {
                    match id_inner.as_rule() {
                        Rule::Variable => {
                            variable = Some(extract_variable(id_inner)?);
                        }
                        Rule::Expression => {
                            list_expr = Some(build_expression(id_inner)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::Where => {
                for where_inner in inner.into_inner() {
                    if where_inner.as_rule() == Rule::Expression {
                        filter = Some(build_expression(where_inner)?);
                    }
                }
            }
            _ => {}
        }
    }

    let variable =
        variable.ok_or_else(|| Error::Parse("List predicate missing variable".into()))?;
    let list =
        list_expr.ok_or_else(|| Error::Parse("List predicate missing list expression".into()))?;

    Ok(Expression::ListPredicate {
        kind,
        variable,
        list: Box::new(list),
        filter: filter.map(Box::new),
    })
}

/// Build a shortestPath() or allShortestPaths() expression.
pub(super) fn build_shortest_path_pattern(pair: Pair<Rule>) -> Result<Expression> {
    let mut is_all = false;
    let mut pattern_element = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SHORTESTPATH => is_all = false,
            Rule::ALLSHORTESTPATHS => is_all = true,
            Rule::PatternElement => {
                let elements = super::build_pattern_element(inner)?;
                pattern_element = Some(Pattern {
                    elements,
                    path_variable: None,
                    shortest_path: None, // Not used for expression-based shortest paths
                });
            }
            _ => {}
        }
    }

    let pattern =
        pattern_element.ok_or_else(|| Error::Parse("shortestPath requires a pattern".into()))?;

    if is_all {
        Ok(Expression::AllShortestPaths(Box::new(pattern)))
    } else {
        Ok(Expression::ShortestPath(Box::new(pattern)))
    }
}

/// Build a literal value.
pub(super) fn build_literal(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NumberLiteral => {
                return build_number_literal(inner);
            }
            Rule::StringLiteral => {
                return Ok(Expression::Literal(Literal::String(parse_string_literal(
                    inner,
                )?)));
            }
            Rule::BooleanLiteral => {
                for bool_inner in inner.into_inner() {
                    match bool_inner.as_rule() {
                        Rule::TRUE => return Ok(Expression::Literal(Literal::Boolean(true))),
                        Rule::FALSE => return Ok(Expression::Literal(Literal::Boolean(false))),
                        _ => {}
                    }
                }
            }
            Rule::NULL => {
                return Ok(Expression::Literal(Literal::Null));
            }
            Rule::MapLiteral => {
                return build_map_literal(inner);
            }
            Rule::ListLiteral => {
                return build_list_literal(inner);
            }
            _ => {}
        }
    }
    Err(Error::Parse("Unknown literal type".into()))
}

/// Build a number literal.
pub(super) fn build_number_literal(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DoubleLiteral => {
                let s = inner.as_str();
                let n: f64 = s
                    .parse()
                    .map_err(|_| Error::Parse(format!("Invalid float: {}", s)))?;
                return Ok(Expression::Literal(Literal::Float(n)));
            }
            Rule::IntegerLiteral => {
                let n = parse_integer_literal(inner)?;
                return Ok(Expression::Literal(Literal::Integer(n)));
            }
            _ => {}
        }
    }
    Err(Error::Parse("Unknown number type".into()))
}

/// Parse an integer literal (decimal, hex, or octal).
pub(super) fn parse_integer_literal(pair: Pair<Rule>) -> Result<i64> {
    let pair_str = pair.as_str();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DecimalInteger => {
                let s = inner.as_str();
                return s
                    .parse()
                    .map_err(|_| Error::Parse(format!("Invalid integer: {}", s)));
            }
            Rule::HexInteger => {
                let s = inner.as_str();
                let hex_part = s
                    .strip_prefix("0x")
                    .or_else(|| s.strip_prefix("0X"))
                    .unwrap_or(s);
                return i64::from_str_radix(hex_part, 16)
                    .map_err(|_| Error::Parse(format!("Invalid hex integer: {}", s)));
            }
            Rule::OctalInteger => {
                let s = inner.as_str();
                return i64::from_str_radix(s.trim_start_matches('0'), 8)
                    .map_err(|_| Error::Parse(format!("Invalid octal integer: {}", s)));
            }
            _ => {}
        }
    }
    // Fallback: try parsing the whole string
    pair_str
        .parse()
        .map_err(|_| Error::Parse(format!("Invalid integer: {}", pair_str)))
}

/// Parse a string literal, handling escape sequences.
pub(super) fn parse_string_literal(pair: Pair<Rule>) -> Result<String> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::StringDoubleText | Rule::StringSingleText => {
                return Ok(unescape_string(inner.as_str()));
            }
            _ => {}
        }
    }
    Ok(String::new())
}

/// Unescape a string (handle \n, \t, etc.).
pub(super) fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some('b') => result.push('\u{0008}'),
                Some('f') => result.push('\u{000C}'),
                Some('u') | Some('U') => {
                    // Unicode escape - collect hex digits
                    let mut hex = String::new();
                    for _ in 0..4 {
                        if let Some(&c) = chars.peek() {
                            if c.is_ascii_hexdigit() {
                                hex.push(chars.next().unwrap());
                            }
                        }
                    }
                    if let Ok(code) = u32::from_str_radix(&hex, 16) {
                        if let Some(c) = char::from_u32(code) {
                            result.push(c);
                        }
                    }
                }
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Build a map literal.
pub(super) fn build_map_literal(pair: Pair<Rule>) -> Result<Expression> {
    let mut entries = Vec::new();
    let mut key = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PropertyKeyName => {
                key = Some(extract_schema_name(inner)?);
            }
            Rule::Expression => {
                if let Some(k) = key.take() {
                    let value = build_expression(inner)?;
                    entries.push((k, value));
                }
            }
            _ => {}
        }
    }

    Ok(Expression::Map(entries))
}

/// Build a list literal.
pub(super) fn build_list_literal(pair: Pair<Rule>) -> Result<Expression> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            elements.push(build_expression(inner)?);
        }
    }

    Ok(Expression::List(elements))
}

/// Build a parameter ($param).
pub(super) fn build_parameter(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SymbolicName => {
                return Ok(Expression::Parameter(inner.as_str().to_string()));
            }
            Rule::DecimalInteger => {
                return Ok(Expression::Parameter(inner.as_str().to_string()));
            }
            _ => {}
        }
    }
    Err(Error::Parse("Parameter requires name".into()))
}

/// Build a function invocation.
pub(super) fn build_function_invocation(pair: Pair<Rule>) -> Result<Expression> {
    let mut name = None;
    let mut args = Vec::new();
    let mut distinct = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::FunctionName => {
                name = Some(build_function_name(inner)?);
            }
            Rule::DISTINCT => distinct = true,
            Rule::Expression => {
                args.push(build_expression(inner)?);
            }
            _ => {}
        }
    }

    let mut name = name.ok_or_else(|| Error::Parse("Function requires name".into()))?;

    // If DISTINCT, prepend to name for aggregates
    if distinct {
        name = format!("{}$distinct", name);
    }

    Ok(Expression::FunctionCall { name, args })
}

/// Build a function name (with namespace).
pub(super) fn build_function_name(pair: Pair<Rule>) -> Result<String> {
    let mut parts = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Namespace => {
                for ns_inner in inner.into_inner() {
                    if ns_inner.as_rule() == Rule::SymbolicName {
                        parts.push(ns_inner.as_str().to_string());
                    }
                }
            }
            Rule::SymbolicName => {
                parts.push(inner.as_str().to_string());
            }
            _ => {}
        }
    }

    Ok(parts.join("."))
}

/// Build a CASE expression.
pub(super) fn build_case_expression(pair: Pair<Rule>) -> Result<Expression> {
    let mut operand: Option<Box<Expression>> = None;
    let mut whens = Vec::new();
    let mut else_expr = None;
    #[allow(unused_assignments)]
    let mut saw_first_expr = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::CaseAlternative => {
                saw_first_expr = true;
                let (when, then) = build_case_alternative(inner)?;
                whens.push((when, then));
            }
            Rule::Expression => {
                if !saw_first_expr && operand.is_none() && whens.is_empty() {
                    // This is the CASE operand (simple CASE)
                    operand = Some(Box::new(build_expression(inner)?));
                    saw_first_expr = true;
                } else {
                    // This is the ELSE expression
                    else_expr = Some(Box::new(build_expression(inner)?));
                }
            }
            _ => {}
        }
    }

    Ok(Expression::Case {
        operand,
        whens,
        else_: else_expr,
    })
}

/// Build a CASE alternative (WHEN ... THEN ...).
pub(super) fn build_case_alternative(pair: Pair<Rule>) -> Result<(Expression, Expression)> {
    let mut when_expr = None;
    let mut then_expr = None;
    let mut is_then = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::THEN => is_then = true,
            Rule::Expression => {
                let expr = build_expression(inner)?;
                if is_then {
                    then_expr = Some(expr);
                } else {
                    when_expr = Some(expr);
                }
            }
            _ => {}
        }
    }

    let when_expr =
        when_expr.ok_or_else(|| Error::Parse("CASE requires WHEN expression".into()))?;
    let then_expr =
        then_expr.ok_or_else(|| Error::Parse("CASE requires THEN expression".into()))?;
    Ok((when_expr, then_expr))
}

/// Extract a variable name.
pub(super) fn extract_variable(pair: Pair<Rule>) -> Result<String> {
    let pair_str = pair.as_str().to_string();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SymbolicName {
            return Ok(extract_symbolic_name(inner));
        }
    }
    Ok(pair_str)
}

/// Extract a schema name (label, property key, etc.).
pub(super) fn extract_schema_name(pair: Pair<Rule>) -> Result<String> {
    let pair_str = pair.as_str().to_string();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SymbolicName => {
                return Ok(extract_symbolic_name(inner));
            }
            Rule::ReservedWord => {
                return Ok(inner.as_str().to_string());
            }
            _ => {}
        }
    }
    Ok(pair_str)
}

/// Extract a symbolic name, handling escaped names.
pub(super) fn extract_symbolic_name(pair: Pair<Rule>) -> String {
    let pair_str = pair.as_str().to_string();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::UnescapedSymbolicName => {
                return inner.as_str().to_string();
            }
            Rule::EscapedSymbolicName => {
                // Remove backticks
                let s = inner.as_str();
                return s.trim_matches('`').to_string();
            }
            _ => {}
        }
    }
    pair_str
}
