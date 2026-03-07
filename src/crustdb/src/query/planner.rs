//! Query planner - converts AST to execution plan.
//!
//! The planner takes a parsed Cypher AST and produces an optimized
//! execution plan that can be run by the executor.

use super::ast::{
    BinaryOperator, Expression, Literal, MatchClause, Pattern, PatternElement, ReturnClause,
    Statement,
};
use crate::error::{Error, Result};

// Re-export plan types for backwards compatibility
pub use super::operators::{
    AggregateColumn, AggregateFunction, CreateNode, CreateRelationship, ExpandDirection,
    FilterPredicate, PlanExpr, PlanLiteral, PlanOperator, ProjectColumn, QueryPlan, SetOperation,
    TargetPropertyFilter,
};

// =============================================================================
// Plan Generation
// =============================================================================

/// Plan a parsed statement.
pub fn plan(statement: &Statement) -> Result<QueryPlan> {
    let root = match statement {
        Statement::Create(create) => plan_create(create)?,
        Statement::Match(match_clause) => plan_match(match_clause)?,
        Statement::Return(return_clause) => plan_standalone_return(return_clause)?,
        Statement::Delete(_) => {
            return Err(Error::Cypher("Standalone DELETE not supported".into()));
        }
        Statement::Set(_) => {
            return Err(Error::Cypher("Standalone SET not supported".into()));
        }
        Statement::Merge(_) => {
            return Err(Error::Cypher("MERGE not yet supported".into()));
        }
    };

    Ok(QueryPlan { root })
}

/// Plan a CREATE statement.
fn plan_create(create: &super::parser::CreateClause) -> Result<PlanOperator> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut auto_var_counter = 0;
    let mut declared_vars: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Process pattern, tracking the previous node variable for relationship binding
    let mut prev_node_var: Option<String> = None;
    let mut pending_rel: Option<(super::parser::RelationshipPattern, String)> = None; // (rel_pattern, source_var)

    for element in &create.pattern.elements {
        match element {
            PatternElement::Node(np) => {
                // Assign a variable if not present
                let var = np.variable.clone().unwrap_or_else(|| {
                    auto_var_counter += 1;
                    format!("_auto_n{}", auto_var_counter)
                });

                // If there's a pending relationship, complete it now
                if let Some((rp, source_var)) = pending_rel.take() {
                    let rel_type = rp.types.first().cloned().unwrap_or_default();
                    let properties = plan_properties(&rp.properties)?;
                    let (source, target) = match rp.direction {
                        super::parser::Direction::Outgoing => (source_var, var.clone()),
                        super::parser::Direction::Incoming => (var.clone(), source_var),
                        super::parser::Direction::Both => (source_var, var.clone()), // Default to outgoing
                    };
                    relationships.push(CreateRelationship {
                        variable: rp.variable.clone(),
                        source,
                        target,
                        rel_type,
                        properties,
                    });
                }

                let labels: Vec<String> = np.labels.iter().flatten().cloned().collect();
                let properties = plan_properties(&np.properties)?;

                // Only create a node if it has labels/properties (declaration) or hasn't been declared yet
                // A node pattern like `(charlie)` appearing after `(charlie:Person {...})` is a reference
                let is_reference =
                    labels.is_empty() && properties.is_empty() && declared_vars.contains(&var);

                if !is_reference {
                    nodes.push(CreateNode {
                        variable: Some(var.clone()),
                        labels,
                        properties,
                    });
                    declared_vars.insert(var.clone());
                }

                prev_node_var = Some(var);
            }
            PatternElement::Relationship(rp) => {
                // Store the relationship to be completed when we see the next node
                let source_var = prev_node_var
                    .clone()
                    .ok_or_else(|| Error::Cypher("Relationship must follow a node".into()))?;
                pending_rel = Some((rp.clone(), source_var));
            }
        }
    }

    Ok(PlanOperator::Create {
        source: None,
        nodes,
        relationships,
    })
}

/// Plan a MATCH statement.
fn plan_match(match_clause: &MatchClause) -> Result<PlanOperator> {
    let pattern = &match_clause.pattern;

    // Start with pattern execution
    let mut plan = plan_pattern(pattern)?;

    // Add WHERE clause filter
    if let Some(ref where_clause) = match_clause.where_clause {
        let predicate = plan_expression_as_predicate(&where_clause.predicate)?;
        // Don't wrap with trivial True predicate
        if !matches!(predicate, FilterPredicate::True) {
            plan = PlanOperator::Filter {
                source: Box::new(plan),
                predicate,
            };
        }
    }

    // Add SET clause
    if let Some(ref set_clause) = match_clause.set_clause {
        let sets = plan_set_clause(set_clause)?;
        plan = PlanOperator::SetProperties {
            source: Box::new(plan),
            sets,
        };
    }

    // Add DELETE clause
    if let Some(ref delete_clause) = match_clause.delete_clause {
        // Extract variable names from delete expressions
        let variables: Vec<String> = delete_clause
            .expressions
            .iter()
            .filter_map(|expr| {
                if let Expression::Variable(v) = expr {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect();
        plan = PlanOperator::Delete {
            source: Box::new(plan),
            variables,
            detach: delete_clause.detach,
        };
    }

    // Add RETURN clause projection and pagination
    if let Some(ref return_clause) = match_clause.return_clause {
        plan = plan_return(plan, return_clause)?;
    }

    Ok(plan)
}

/// Plan a pattern into scan/expand operators.
fn plan_pattern(pattern: &Pattern) -> Result<PlanOperator> {
    if pattern.elements.is_empty() {
        return Ok(PlanOperator::Empty);
    }

    // Check if this is a shortest path pattern (shortestPath() or allShortestPaths())
    if let Some(mode) = pattern.shortest_path {
        let all_paths = mode == crate::query::parser::ShortestPathMode::All;
        return plan_shortest_path_pattern(pattern, all_paths);
    }

    // Start with first node
    let first_node = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Pattern must start with a node".into())),
    };

    let variable = first_node
        .variable
        .clone()
        .unwrap_or_else(|| "_n0".to_string());
    let label_groups: Vec<Vec<String>> = first_node.labels.clone();

    // Try to extract a simple property equality filter for pushdown
    let property_filter = first_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    let mut plan = PlanOperator::NodeScan {
        variable: variable.clone(),
        label_groups,
        limit: None,
        property_filter: property_filter.clone(),
    };

    // Add inline property filter if present and not already pushed down
    if let Some(ref props) = first_node.properties {
        if property_filter.is_none() {
            // Complex properties - use Filter operator
            let predicate = plan_inline_properties(&variable, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    // Process remaining pattern elements (relationships and nodes)
    let mut current_var = variable;
    let mut elem_idx = 1;

    while elem_idx < pattern.elements.len() {
        // Expect relationship
        let rel = match &pattern.elements[elem_idx] {
            PatternElement::Relationship(rp) => rp,
            _ => return Err(Error::Cypher("Expected relationship in pattern".into())),
        };

        elem_idx += 1;

        // Expect target node
        let target_node = match pattern.elements.get(elem_idx) {
            Some(PatternElement::Node(np)) => np,
            _ => {
                return Err(Error::Cypher(
                    "Expected target node after relationship".into(),
                ))
            }
        };

        elem_idx += 1;

        let target_var = target_node
            .variable
            .clone()
            .unwrap_or_else(|| format!("_n{}", elem_idx / 2));
        let target_labels: Vec<String> = target_node.labels.iter().flatten().cloned().collect();

        // Check if this is a variable-length relationship
        if let Some(ref length) = rel.length {
            let min_hops = length.min.unwrap_or(1);
            let max_hops = length.max.unwrap_or(100); // Reasonable default

            plan = PlanOperator::VariableLengthExpand {
                source: Box::new(plan),
                source_variable: current_var.clone(),
                rel_variable: rel.variable.clone(),
                target_variable: target_var.clone(),
                target_labels: target_labels.clone(),
                path_variable: pattern.path_variable.clone(),
                types: rel.types.clone(),
                direction: rel.direction.into(),
                min_hops,
                max_hops,
                target_ids: None,
                limit: None,
                target_property_filter: None, // Will be populated by predicate pushdown
            };
        } else {
            // Single-hop expand
            plan = PlanOperator::Expand {
                source: Box::new(plan),
                source_variable: current_var.clone(),
                rel_variable: rel.variable.clone(),
                target_variable: target_var.clone(),
                target_labels: target_labels.clone(),
                path_variable: pattern.path_variable.clone(),
                types: rel.types.clone(),
                direction: rel.direction.into(),
            };
        }

        // Add inline property filter for target node
        if let Some(ref props) = target_node.properties {
            let predicate = plan_inline_properties(&target_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }

        current_var = target_var;
    }

    Ok(plan)
}

/// Plan a shortest path pattern (used by shortestPath() expression).
/// This is now called when evaluating shortestPath expressions, not from pattern matching.
pub fn plan_shortest_path_pattern(pattern: &Pattern, all_paths: bool) -> Result<PlanOperator> {
    // shortestPath requires exactly node-rel-node
    if pattern.elements.len() != 3 {
        return Err(Error::Cypher(
            "shortestPath requires (a)-[*]->(b) pattern".into(),
        ));
    }

    let source_node = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected source node".into())),
    };

    let rel = match &pattern.elements[1] {
        PatternElement::Relationship(rp) => rp,
        _ => return Err(Error::Cypher("Expected relationship".into())),
    };

    let target_node = match &pattern.elements[2] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected target node".into())),
    };

    let source_var = source_node
        .variable
        .clone()
        .unwrap_or_else(|| "_src".to_string());
    let target_var = target_node
        .variable
        .clone()
        .unwrap_or_else(|| "_tgt".to_string());
    let source_label_groups: Vec<Vec<String>> = source_node.labels.clone();
    let target_labels: Vec<String> = target_node.labels.iter().flatten().cloned().collect();

    // Determine hop bounds from variable-length spec (e.g., [*1..5])
    let (min_hops, max_hops) = if let Some(ref length) = rel.length {
        (length.min.unwrap_or(1), length.max.unwrap_or(100))
    } else {
        // Default: variable length up to 100 hops
        (1, 100)
    };

    // Try to extract a simple property filter for pushdown
    let source_property_filter = source_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    let mut plan = PlanOperator::NodeScan {
        variable: source_var.clone(),
        label_groups: source_label_groups,
        limit: None,
        property_filter: source_property_filter.clone(),
    };

    // Add inline property filter for source if not pushed down
    if let Some(ref props) = source_node.properties {
        if source_property_filter.is_none() {
            let predicate = plan_inline_properties(&source_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    // Try to extract a simple property filter for target (for early BFS termination)
    let target_property_filter = target_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    // k=1 for shortestPath(), k=MAX for allShortestPaths() (all paths of shortest length)
    let k = if all_paths { u32::MAX } else { 1 };

    plan = PlanOperator::ShortestPath {
        source: Box::new(plan),
        source_variable: source_var,
        target_variable: target_var.clone(),
        target_labels,
        path_variable: pattern.path_variable.clone(),
        types: rel.types.clone(),
        direction: rel.direction.into(),
        min_hops,
        max_hops,
        k,
        target_property_filter: target_property_filter.clone(),
    };

    // Add inline property filter for target if not pushed down (complex expressions)
    if let Some(ref props) = target_node.properties {
        if target_property_filter.is_none() {
            let predicate = plan_inline_properties(&target_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    Ok(plan)
}

/// Plan a standalone RETURN statement (e.g., RETURN 1, RETURN "hello").
fn plan_standalone_return(return_clause: &ReturnClause) -> Result<PlanOperator> {
    // Use ProduceRow to create a single empty row, then project the expressions
    plan_return(PlanOperator::ProduceRow, return_clause)
}

/// Plan a RETURN clause into projection, aggregation, and pagination.
fn plan_return(source: PlanOperator, return_clause: &ReturnClause) -> Result<PlanOperator> {
    let mut plan = source;

    // Check for aggregates
    let has_aggregates = return_clause
        .items
        .iter()
        .any(|item| is_aggregate_expression(&item.expression));

    if has_aggregates {
        // Separate aggregate and non-aggregate columns
        let mut group_by = Vec::new();
        let mut aggregates = Vec::new();

        for item in &return_clause.items {
            let alias = item
                .alias
                .clone()
                .unwrap_or_else(|| format_expression(&item.expression));

            if let Some(agg) = try_extract_aggregate(&item.expression)? {
                aggregates.push(AggregateColumn {
                    function: agg,
                    alias,
                });
            } else {
                let expr = plan_expression(&item.expression)?;
                group_by.push(ProjectColumn { expr, alias });
            }
        }

        plan = PlanOperator::Aggregate {
            source: Box::new(plan),
            group_by,
            aggregates,
        };
    } else {
        // Simple projection
        let columns: Result<Vec<_>> = return_clause
            .items
            .iter()
            .map(|item| {
                let expr = plan_expression(&item.expression)?;
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format_expression(&item.expression));
                Ok(ProjectColumn { expr, alias })
            })
            .collect();

        plan = PlanOperator::Project {
            source: Box::new(plan),
            columns: columns?,
            distinct: return_clause.distinct,
        };
    }

    // Add SKIP
    if let Some(skip) = return_clause.skip {
        plan = PlanOperator::Skip {
            source: Box::new(plan),
            count: skip,
        };
    }

    // Add LIMIT
    if let Some(limit) = return_clause.limit {
        plan = PlanOperator::Limit {
            source: Box::new(plan),
            count: limit,
        };
    }

    Ok(plan)
}

/// Plan a SET clause.
fn plan_set_clause(set_clause: &super::parser::SetClause) -> Result<Vec<SetOperation>> {
    let mut ops = Vec::new();

    for item in &set_clause.items {
        match item {
            super::parser::SetItem::Property {
                variable,
                property,
                value,
            } => {
                ops.push(SetOperation::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                    value: plan_expression(value)?,
                });
            }
            super::parser::SetItem::Labels { variable, labels } => {
                for label in labels {
                    ops.push(SetOperation::AddLabel {
                        variable: variable.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
    }

    Ok(ops)
}

// =============================================================================
// Expression Planning
// =============================================================================

/// Convert AST expression to plan expression.
fn plan_expression(expr: &Expression) -> Result<PlanExpr> {
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
        _ => Err(Error::Cypher(format!(
            "Expression type not supported in plan: {:?}",
            expr
        ))),
    }
}

/// Convert expression to filter predicate.
fn plan_expression_as_predicate(expr: &Expression) -> Result<FilterPredicate> {
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
            super::parser::UnaryOperator::Not => Ok(FilterPredicate::Not {
                inner: Box::new(plan_expression_as_predicate(operand)?),
            }),
            super::parser::UnaryOperator::IsNull => Ok(FilterPredicate::IsNull {
                expr: plan_expression(operand)?,
            }),
            super::parser::UnaryOperator::IsNotNull => Ok(FilterPredicate::IsNotNull {
                expr: plan_expression(operand)?,
            }),
            super::parser::UnaryOperator::Neg => {
                Err(Error::Cypher("Negation not supported as predicate".into()))
            }
        },
        Expression::Literal(Literal::Boolean(true)) => Ok(FilterPredicate::True),
        _ => Err(Error::Cypher(format!(
            "Expression not supported as predicate: {:?}",
            expr
        ))),
    }
}

/// Extract a simple property filter for pushdown (single key-value with literal value).
/// Returns Some((property_name, value)) if the properties are a simple {key: literal}.
fn extract_simple_property_filter(props: &Expression) -> Option<(String, serde_json::Value)> {
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
fn plan_inline_properties(variable: &str, props: &Expression) -> Result<FilterPredicate> {
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
fn plan_properties(props: &Option<Expression>) -> Result<Vec<(String, PlanExpr)>> {
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

fn plan_literal(lit: &Literal) -> PlanLiteral {
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

fn is_aggregate_expression(expr: &Expression) -> bool {
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

fn try_extract_aggregate(expr: &Expression) -> Result<Option<AggregateFunction>> {
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
fn format_expression(expr: &Expression) -> String {
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

// =============================================================================
// Optimization Passes
// =============================================================================

/// Apply optimization passes to a query plan.
pub fn optimize(plan: QueryPlan) -> QueryPlan {
    let root = optimize_operator(plan.root);
    QueryPlan { root }
}

fn optimize_operator(op: PlanOperator) -> PlanOperator {
    match op {
        // Optimize COUNT pushdown for simple patterns
        PlanOperator::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            // Check for COUNT(*) or COUNT(n) with no grouping
            if group_by.is_empty() && aggregates.len() == 1 {
                if let AggregateFunction::Count(ref arg) = aggregates[0].function {
                    // Check if source is a simple NodeScan without filters or property pushdown
                    if let PlanOperator::NodeScan {
                        ref label_groups,
                        limit: None,
                        property_filter: None,
                        ..
                    } = *source
                    {
                        // Can push COUNT to SQL only for simple single label
                        // Flatten to check - OR labels can't be pushed down easily
                        let flat_labels: Vec<String> =
                            label_groups.iter().flatten().cloned().collect();
                        let label = if flat_labels.is_empty() {
                            None
                        } else if flat_labels.len() == 1 {
                            Some(flat_labels[0].clone())
                        } else {
                            // Multiple labels - can't push down easily
                            return PlanOperator::Aggregate {
                                source: Box::new(optimize_operator(*source)),
                                group_by,
                                aggregates,
                            };
                        };

                        // Only optimize COUNT(*) or COUNT(var) without DISTINCT
                        if arg.is_none() || matches!(arg, Some(PlanExpr::Variable(_))) {
                            return PlanOperator::CountPushdown {
                                label,
                                alias: aggregates[0].alias.clone(),
                            };
                        }
                    }
                }
            }

            PlanOperator::Aggregate {
                source: Box::new(optimize_operator(*source)),
                group_by,
                aggregates,
            }
        }

        // Optimize LIMIT pushdown for simple node scans
        PlanOperator::Limit { source, count } => {
            match *source {
                // LIMIT on NodeScan can be pushed down
                PlanOperator::NodeScan {
                    variable,
                    label_groups,
                    limit: None,
                    property_filter,
                } => PlanOperator::NodeScan {
                    variable,
                    label_groups,
                    limit: Some(count),
                    property_filter,
                },

                // LIMIT on VariableLengthExpand can be pushed down for early termination
                PlanOperator::VariableLengthExpand {
                    source: inner_source,
                    source_variable,
                    rel_variable,
                    target_variable,
                    target_labels,
                    path_variable,
                    types,
                    direction,
                    min_hops,
                    max_hops,
                    target_ids,
                    limit: None,
                    target_property_filter,
                } => PlanOperator::VariableLengthExpand {
                    source: Box::new(optimize_operator(*inner_source)),
                    source_variable,
                    rel_variable,
                    target_variable,
                    target_labels,
                    path_variable,
                    types,
                    direction,
                    min_hops,
                    max_hops,
                    target_ids,
                    limit: Some(count),
                    target_property_filter,
                },

                // LIMIT on Project can be pushed through to inner operators
                PlanOperator::Project {
                    source: project_source,
                    columns,
                    distinct: false,
                } => {
                    match *project_source {
                        // Push through to NodeScan
                        PlanOperator::NodeScan {
                            variable,
                            label_groups,
                            limit: None,
                            property_filter,
                        } => PlanOperator::Project {
                            source: Box::new(PlanOperator::NodeScan {
                                variable,
                                label_groups,
                                limit: Some(count),
                                property_filter,
                            }),
                            columns,
                            distinct: false,
                        },
                        // Push through to VariableLengthExpand
                        PlanOperator::VariableLengthExpand {
                            source: inner_source,
                            source_variable,
                            rel_variable,
                            target_variable,
                            target_labels,
                            path_variable,
                            types,
                            direction,
                            min_hops,
                            max_hops,
                            target_ids,
                            limit: None,
                            target_property_filter,
                        } => PlanOperator::Project {
                            source: Box::new(PlanOperator::VariableLengthExpand {
                                source: Box::new(optimize_operator(*inner_source)),
                                source_variable,
                                rel_variable,
                                target_variable,
                                target_labels,
                                path_variable,
                                types,
                                direction,
                                min_hops,
                                max_hops,
                                target_ids,
                                limit: Some(count),
                                target_property_filter,
                            }),
                            columns,
                            distinct: false,
                        },
                        // Push through Filter -> VariableLengthExpand
                        // This handles: MATCH (a)-[*]->(b) WHERE ... RETURN ... LIMIT n
                        // First optimize the Filter (which may push target predicates),
                        // then push LIMIT into the result
                        PlanOperator::Filter {
                            source: filter_source,
                            predicate,
                        } => {
                            // Optimize the Filter first to allow target predicate pushdown
                            let optimized_filter = optimize_operator(PlanOperator::Filter {
                                source: filter_source,
                                predicate,
                            });

                            // Now check if we can push LIMIT into the result
                            match optimized_filter {
                                // Filter was kept, check what's inside
                                PlanOperator::Filter {
                                    source: opt_filter_source,
                                    predicate: opt_predicate,
                                } => {
                                    if let PlanOperator::VariableLengthExpand {
                                        source: inner_source,
                                        source_variable,
                                        rel_variable,
                                        target_variable,
                                        target_labels,
                                        path_variable,
                                        types,
                                        direction,
                                        min_hops,
                                        max_hops,
                                        target_ids,
                                        limit: None,
                                        target_property_filter,
                                    } = *opt_filter_source
                                    {
                                        PlanOperator::Project {
                                            source: Box::new(PlanOperator::Filter {
                                                source: Box::new(
                                                    PlanOperator::VariableLengthExpand {
                                                        source: inner_source,
                                                        source_variable,
                                                        rel_variable,
                                                        target_variable,
                                                        target_labels,
                                                        path_variable,
                                                        types,
                                                        direction,
                                                        min_hops,
                                                        max_hops,
                                                        target_ids,
                                                        limit: Some(count),
                                                        target_property_filter,
                                                    },
                                                ),
                                                predicate: opt_predicate,
                                            }),
                                            columns,
                                            distinct: false,
                                        }
                                    } else {
                                        // Can't push further, keep LIMIT on top
                                        PlanOperator::Limit {
                                            source: Box::new(PlanOperator::Project {
                                                source: Box::new(PlanOperator::Filter {
                                                    source: opt_filter_source,
                                                    predicate: opt_predicate,
                                                }),
                                                columns,
                                                distinct: false,
                                            }),
                                            count,
                                        }
                                    }
                                }
                                // Filter was optimized away (predicate fully pushed),
                                // check if we got VariableLengthExpand
                                PlanOperator::VariableLengthExpand {
                                    source: inner_source,
                                    source_variable,
                                    rel_variable,
                                    target_variable,
                                    target_labels,
                                    path_variable,
                                    types,
                                    direction,
                                    min_hops,
                                    max_hops,
                                    target_ids,
                                    limit: None,
                                    target_property_filter,
                                } => PlanOperator::Project {
                                    source: Box::new(PlanOperator::VariableLengthExpand {
                                        source: inner_source,
                                        source_variable,
                                        rel_variable,
                                        target_variable,
                                        target_labels,
                                        path_variable,
                                        types,
                                        direction,
                                        min_hops,
                                        max_hops,
                                        target_ids,
                                        limit: Some(count),
                                        target_property_filter,
                                    }),
                                    columns,
                                    distinct: false,
                                },
                                // Something else, keep LIMIT on top
                                other => PlanOperator::Limit {
                                    source: Box::new(PlanOperator::Project {
                                        source: Box::new(other),
                                        columns,
                                        distinct: false,
                                    }),
                                    count,
                                },
                            }
                        }
                        // Default: keep LIMIT on top
                        other => PlanOperator::Limit {
                            source: Box::new(PlanOperator::Project {
                                source: Box::new(optimize_operator(other)),
                                columns,
                                distinct: false,
                            }),
                            count,
                        },
                    }
                }

                other => PlanOperator::Limit {
                    source: Box::new(optimize_operator(other)),
                    count,
                },
            }
        }

        // Optimize Filter: try to push predicates into underlying operators
        PlanOperator::Filter { source, predicate } => {
            // Try to push target predicates into VariableLengthExpand
            if let PlanOperator::VariableLengthExpand {
                source: inner_source,
                source_variable,
                rel_variable,
                target_variable,
                target_labels,
                path_variable,
                types,
                direction,
                min_hops,
                max_hops,
                target_ids,
                limit,
                target_property_filter: None, // Only if not already set
            } = *source
            {
                // Try to extract a pushable target predicate
                if let Some((pushed_filter, remaining_predicate)) =
                    extract_target_property_filter(&predicate, &target_variable)
                {
                    let optimized_expand = PlanOperator::VariableLengthExpand {
                        source: Box::new(optimize_operator(*inner_source)),
                        source_variable,
                        rel_variable,
                        target_variable,
                        target_labels,
                        path_variable,
                        types,
                        direction,
                        min_hops,
                        max_hops,
                        target_ids,
                        limit,
                        target_property_filter: Some(pushed_filter),
                    };

                    // If there's remaining predicate, wrap with Filter
                    if let Some(remaining) = remaining_predicate {
                        PlanOperator::Filter {
                            source: Box::new(optimized_expand),
                            predicate: remaining,
                        }
                    } else {
                        optimized_expand
                    }
                } else {
                    // Couldn't push predicate, just optimize recursively
                    PlanOperator::Filter {
                        source: Box::new(PlanOperator::VariableLengthExpand {
                            source: Box::new(optimize_operator(*inner_source)),
                            source_variable,
                            rel_variable,
                            target_variable,
                            target_labels,
                            path_variable,
                            types,
                            direction,
                            min_hops,
                            max_hops,
                            target_ids,
                            limit,
                            target_property_filter: None,
                        }),
                        predicate,
                    }
                }
            } else {
                PlanOperator::Filter {
                    source: Box::new(optimize_operator(*source)),
                    predicate,
                }
            }
        }
        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => {
            // Optimize: RETURN DISTINCT type(r) -> RelationshipTypesScan
            // Pattern: Project(distinct=true) over Expand with single column type(r)
            if distinct && columns.len() == 1 {
                if let PlanExpr::Function { name, args } = &columns[0].expr {
                    if name.to_uppercase() == "TYPE" && args.len() == 1 {
                        if let PlanExpr::Variable(rel_var) = &args[0] {
                            // Check if source involves a relationship variable matching rel_var
                            if is_relationship_pattern_with_var(&source, rel_var) {
                                return PlanOperator::RelationshipTypesScan {
                                    alias: columns[0].alias.clone(),
                                };
                            }
                        }
                    }
                }
            }

            PlanOperator::Project {
                source: Box::new(optimize_operator(*source)),
                columns,
                distinct,
            }
        }
        PlanOperator::Skip { source, count } => PlanOperator::Skip {
            source: Box::new(optimize_operator(*source)),
            count,
        },
        PlanOperator::Expand {
            source,
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
        } => PlanOperator::Expand {
            source: Box::new(optimize_operator(*source)),
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
        },
        PlanOperator::VariableLengthExpand {
            source,
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            target_ids,
            limit,
            target_property_filter,
        } => PlanOperator::VariableLengthExpand {
            source: Box::new(optimize_operator(*source)),
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            target_ids,
            limit,
            target_property_filter,
        },
        PlanOperator::ShortestPath {
            source,
            source_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            k,
            target_property_filter,
        } => PlanOperator::ShortestPath {
            source: Box::new(optimize_operator(*source)),
            source_variable,
            target_variable,
            target_labels,
            path_variable,
            types,
            direction,
            min_hops,
            max_hops,
            k,
            target_property_filter,
        },
        PlanOperator::SetProperties { source, sets } => PlanOperator::SetProperties {
            source: Box::new(optimize_operator(*source)),
            sets,
        },
        PlanOperator::Delete {
            source,
            variables,
            detach,
        } => PlanOperator::Delete {
            source: Box::new(optimize_operator(*source)),
            variables,
            detach,
        },
        PlanOperator::Create {
            source,
            nodes,
            relationships,
        } => PlanOperator::Create {
            source: source.map(|s| Box::new(optimize_operator(*s))),
            nodes,
            relationships,
        },

        // Leaf operators - no optimization needed
        other => other,
    }
}

/// Check if a plan involves a relationship pattern that binds the given variable.
///
/// This is used to detect patterns like `MATCH ()-[r]->() RETURN DISTINCT type(r)`
/// where we can optimize to use RelationshipTypesScan instead of scanning all relationships.
fn is_relationship_pattern_with_var(op: &PlanOperator, rel_var: &str) -> bool {
    match op {
        PlanOperator::Expand { rel_variable, .. } => rel_variable.as_deref() == Some(rel_var),
        PlanOperator::VariableLengthExpand { rel_variable, .. } => {
            rel_variable.as_deref() == Some(rel_var)
        }
        PlanOperator::Filter { source, .. } => is_relationship_pattern_with_var(source, rel_var),
        _ => false,
    }
}

/// Extract a target property filter from a predicate, if possible.
///
/// Looks for simple property conditions on the target variable that can be
/// pushed into VariableLengthExpand for early termination during BFS.
///
/// Supported patterns:
/// - `target.property = value` → Eq filter
/// - `target.property ENDS WITH 'suffix'` → EndsWith filter
/// - `target.property STARTS WITH 'prefix'` → StartsWith filter
/// - `target.property CONTAINS 'substring'` → Contains filter
///
/// Returns (pushed_filter, remaining_predicate) where remaining_predicate
/// is None if the entire predicate was pushed.
fn extract_target_property_filter(
    predicate: &FilterPredicate,
    target_variable: &str,
) -> Option<(TargetPropertyFilter, Option<FilterPredicate>)> {
    match predicate {
        // Simple equality: target.property = value
        FilterPredicate::Eq { left, right } => {
            if let PlanExpr::Property { variable, property } = left {
                if variable == target_variable {
                    if let PlanExpr::Literal(PlanLiteral::String(s)) = right {
                        return Some((
                            TargetPropertyFilter::Eq {
                                property: property.clone(),
                                value: serde_json::Value::String(s.clone()),
                            },
                            None,
                        ));
                    }
                    if let PlanExpr::Literal(PlanLiteral::Int(i)) = right {
                        return Some((
                            TargetPropertyFilter::Eq {
                                property: property.clone(),
                                value: serde_json::Value::Number((*i).into()),
                            },
                            None,
                        ));
                    }
                }
            }
            None
        }

        // ENDS WITH: target.property ENDS WITH 'suffix'
        FilterPredicate::EndsWith { expr, suffix } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::EndsWith {
                            property: property.clone(),
                            suffix: suffix.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // STARTS WITH: target.property STARTS WITH 'prefix'
        FilterPredicate::StartsWith { expr, prefix } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::StartsWith {
                            property: property.clone(),
                            prefix: prefix.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // CONTAINS: target.property CONTAINS 'substring'
        FilterPredicate::Contains { expr, substring } => {
            if let PlanExpr::Property { variable, property } = expr {
                if variable == target_variable {
                    return Some((
                        TargetPropertyFilter::Contains {
                            property: property.clone(),
                            substring: substring.clone(),
                        },
                        None,
                    ));
                }
            }
            None
        }

        // AND: try to extract from either side
        FilterPredicate::And { left, right } => {
            // Try left side first
            if let Some((filter, remaining_left)) =
                extract_target_property_filter(left, target_variable)
            {
                // Combine remaining left (if any) with right
                let remaining = match remaining_left {
                    Some(rem_left) => Some(FilterPredicate::And {
                        left: Box::new(rem_left),
                        right: right.clone(),
                    }),
                    None => Some((**right).clone()),
                };
                return Some((filter, remaining));
            }
            // Try right side
            if let Some((filter, remaining_right)) =
                extract_target_property_filter(right, target_variable)
            {
                // Combine left with remaining right (if any)
                let remaining = match remaining_right {
                    Some(rem_right) => Some(FilterPredicate::And {
                        left: left.clone(),
                        right: Box::new(rem_right),
                    }),
                    None => Some((**left).clone()),
                };
                return Some((filter, remaining));
            }
            None
        }

        // Other predicates can't be pushed
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse;

    fn plan_query(query: &str) -> QueryPlan {
        let stmt = parse(query).expect("parse failed");
        let plan = plan(&stmt).expect("plan failed");
        optimize(plan)
    }

    #[test]
    fn test_plan_simple_match() {
        let plan = plan_query("MATCH (n:Person) RETURN n");
        // Should be: Project -> NodeScan
        assert!(matches!(plan.root, PlanOperator::Project { .. }));
    }

    #[test]
    fn test_plan_count_pushdown() {
        let plan = plan_query("MATCH (n:Person) RETURN count(n)");
        // Should be optimized to CountPushdown
        assert!(matches!(plan.root, PlanOperator::CountPushdown { .. }));
    }

    #[test]
    fn test_plan_with_where() {
        let plan = plan_query("MATCH (n:Person) WHERE n.age > 30 RETURN n");
        // Should be: Project -> Filter -> NodeScan
        if let PlanOperator::Project { source, .. } = plan.root {
            assert!(matches!(*source, PlanOperator::Filter { .. }));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_limit_pushdown() {
        let plan = plan_query("MATCH (n:Person) RETURN n LIMIT 10");
        // Should be: Project -> NodeScan(limit=10)
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::NodeScan { limit, .. } = *source {
                assert_eq!(limit, Some(10));
            } else {
                panic!("Expected NodeScan");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_single_hop() {
        let plan = plan_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
        // Should be: Project -> Expand -> NodeScan
        if let PlanOperator::Project { source, .. } = plan.root {
            assert!(matches!(*source, PlanOperator::Expand { .. }));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_limit_pushdown() {
        let plan = plan_query("MATCH (a)-[*1..5]->(b) RETURN b LIMIT 1");
        // Should have limit pushed into VariableLengthExpand
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand { limit, .. } = *source {
                assert_eq!(
                    limit,
                    Some(1),
                    "LIMIT should be pushed into VariableLengthExpand"
                );
            } else {
                panic!("Expected VariableLengthExpand");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_filter_pushdown() {
        let plan = plan_query("MATCH (a)-[*1..5]->(b) WHERE b.name ENDS WITH 'admin' RETURN b");
        // Should have target_property_filter pushed into VariableLengthExpand
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::VariableLengthExpand {
                target_property_filter,
                ..
            } = *source
            {
                assert!(
                    target_property_filter.is_some(),
                    "ENDS WITH predicate should be pushed into VariableLengthExpand"
                );
                if let Some(TargetPropertyFilter::EndsWith { property, suffix }) =
                    target_property_filter
                {
                    assert_eq!(property, "name");
                    assert_eq!(suffix, "admin");
                } else {
                    panic!("Expected EndsWith filter");
                }
            } else {
                panic!("Expected VariableLengthExpand");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_plan_variable_length_limit_through_filter() {
        // This pattern is used by node_status: MATCH path with WHERE and LIMIT 1
        let plan = plan_query(
            "MATCH p = (a)-[*1..20]->(b) WHERE a.name = 'test' AND b.id ENDS WITH '-519' RETURN length(p) LIMIT 1",
        );
        // Should have limit pushed into VariableLengthExpand even through Filter
        // Structure: Project -> Filter -> VariableLengthExpand(limit=1)
        if let PlanOperator::Project { source, .. } = plan.root {
            if let PlanOperator::Filter {
                source: filter_source,
                ..
            } = *source
            {
                if let PlanOperator::VariableLengthExpand {
                    limit,
                    target_property_filter,
                    ..
                } = *filter_source
                {
                    assert_eq!(
                        limit,
                        Some(1),
                        "LIMIT should be pushed into VariableLengthExpand through Filter"
                    );
                    // Also verify target property filter was pushed
                    assert!(
                        target_property_filter.is_some(),
                        "Target property filter should be pushed"
                    );
                } else {
                    panic!(
                        "Expected VariableLengthExpand under Filter, got {:?}",
                        filter_source
                    );
                }
            } else {
                panic!("Expected Filter under Project, got {:?}", source);
            }
        } else {
            panic!("Expected Project");
        }
    }
}
