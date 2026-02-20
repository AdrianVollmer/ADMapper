//! Query planner - converts AST to execution plan.
//!
//! The planner takes a parsed Cypher AST and produces an optimized
//! execution plan that can be run by the executor.

use super::parser::{
    BinaryOperator, Direction, Expression, Literal, MatchClause, Pattern, PatternElement,
    ReturnClause, Statement,
};
use crate::error::{Error, Result};

// =============================================================================
// Plan Data Structures
// =============================================================================

/// A query execution plan.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Root operator of the plan.
    pub root: PlanOperator,
}

/// Plan operators form a tree representing the execution strategy.
#[derive(Debug, Clone)]
pub enum PlanOperator {
    /// Scan all nodes with optional label filter.
    NodeScan {
        variable: String,
        labels: Vec<String>,
        /// SQL-level limit pushdown for simple scans.
        limit: Option<u64>,
    },
    /// Scan all edges with optional type filter.
    #[allow(dead_code)]
    EdgeScan {
        variable: String,
        types: Vec<String>,
    },
    /// Expand from a node along relationships.
    Expand {
        source: Box<PlanOperator>,
        source_variable: String,
        rel_variable: Option<String>,
        target_variable: String,
        target_labels: Vec<String>,
        types: Vec<String>,
        direction: ExpandDirection,
    },
    /// Variable-length expand (BFS).
    VariableLengthExpand {
        source: Box<PlanOperator>,
        source_variable: String,
        rel_variable: Option<String>,
        target_variable: String,
        target_labels: Vec<String>,
        path_variable: Option<String>,
        types: Vec<String>,
        direction: ExpandDirection,
        min_hops: u32,
        max_hops: u32,
    },
    /// Shortest path search (BFS with k paths).
    ShortestPath {
        source: Box<PlanOperator>,
        source_variable: String,
        target_variable: String,
        target_labels: Vec<String>,
        path_variable: Option<String>,
        types: Vec<String>,
        direction: ExpandDirection,
        min_hops: u32,
        max_hops: u32,
        k: u32,
    },
    /// Filter rows by predicate.
    Filter {
        source: Box<PlanOperator>,
        predicate: FilterPredicate,
    },
    /// Project columns for RETURN clause.
    Project {
        source: Box<PlanOperator>,
        columns: Vec<ProjectColumn>,
        /// Whether this is a RETURN DISTINCT.
        distinct: bool,
    },
    /// Aggregate rows (GROUP BY with aggregation functions).
    Aggregate {
        source: Box<PlanOperator>,
        /// Columns that aren't aggregates (implicit GROUP BY).
        group_by: Vec<ProjectColumn>,
        /// Aggregate functions to compute.
        aggregates: Vec<AggregateColumn>,
    },
    /// SQL pushdown for COUNT(*) or COUNT(n) without grouping.
    CountPushdown {
        label: Option<String>,
        alias: String,
    },
    /// Sort rows.
    #[allow(dead_code)]
    Sort {
        source: Box<PlanOperator>,
        keys: Vec<SortKey>,
    },
    /// Limit number of rows.
    Limit {
        source: Box<PlanOperator>,
        count: u64,
    },
    /// Skip rows.
    Skip {
        source: Box<PlanOperator>,
        count: u64,
    },
    /// Create nodes/edges.
    Create {
        source: Option<Box<PlanOperator>>,
        nodes: Vec<CreateNode>,
        edges: Vec<CreateEdge>,
    },
    /// Delete nodes/edges.
    Delete {
        source: Box<PlanOperator>,
        variables: Vec<String>,
        detach: bool,
    },
    /// Set properties/labels.
    SetProperties {
        source: Box<PlanOperator>,
        sets: Vec<SetOperation>,
    },
    /// Empty result (no rows).
    Empty,
    /// Produce a single empty row (for standalone CREATE).
    ProduceRow,
}

/// Expand direction for traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpandDirection {
    Outgoing,
    Incoming,
    Both,
}

impl From<Direction> for ExpandDirection {
    fn from(d: Direction) -> Self {
        match d {
            Direction::Outgoing => ExpandDirection::Outgoing,
            Direction::Incoming => ExpandDirection::Incoming,
            Direction::Both => ExpandDirection::Both,
        }
    }
}

/// Filter predicate in execution plan.
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    Eq { left: PlanExpr, right: PlanExpr },
    Ne { left: PlanExpr, right: PlanExpr },
    Lt { left: PlanExpr, right: PlanExpr },
    Le { left: PlanExpr, right: PlanExpr },
    Gt { left: PlanExpr, right: PlanExpr },
    Ge { left: PlanExpr, right: PlanExpr },
    And { left: Box<FilterPredicate>, right: Box<FilterPredicate> },
    Or { left: Box<FilterPredicate>, right: Box<FilterPredicate> },
    Not { inner: Box<FilterPredicate> },
    IsNull { expr: PlanExpr },
    IsNotNull { expr: PlanExpr },
    StartsWith { expr: PlanExpr, prefix: String },
    EndsWith { expr: PlanExpr, suffix: String },
    Contains { expr: PlanExpr, substring: String },
    Regex { expr: PlanExpr, pattern: String },
    HasLabel { variable: String, label: String },
    /// Always true (for optimized-away predicates).
    True,
}

/// Expression in execution plan.
#[derive(Debug, Clone)]
pub enum PlanExpr {
    Literal(PlanLiteral),
    Variable(String),
    Property { variable: String, property: String },
    Function { name: String, args: Vec<PlanExpr> },
    PathLength { path_variable: String },
}

/// Literal value in plan.
#[derive(Debug, Clone)]
pub enum PlanLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

/// Column projection for RETURN.
#[derive(Debug, Clone)]
pub struct ProjectColumn {
    pub expr: PlanExpr,
    pub alias: String,
}

/// Aggregate column.
#[derive(Debug, Clone)]
pub struct AggregateColumn {
    pub function: AggregateFunction,
    pub alias: String,
}

/// Supported aggregate functions.
#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count(Option<PlanExpr>),
    Sum(PlanExpr),
    Avg(PlanExpr),
    Min(PlanExpr),
    Max(PlanExpr),
    Collect(PlanExpr),
}

/// Sort key.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: PlanExpr,
    pub descending: bool,
}

/// Node creation specification.
#[derive(Debug, Clone)]
pub struct CreateNode {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Edge creation specification.
#[derive(Debug, Clone)]
pub struct CreateEdge {
    pub variable: Option<String>,
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Set operation.
#[derive(Debug, Clone)]
pub enum SetOperation {
    Property {
        variable: String,
        property: String,
        value: PlanExpr,
    },
    AddLabel {
        variable: String,
        label: String,
    },
    #[allow(dead_code)]
    RemoveLabel {
        variable: String,
        label: String,
    },
}

// =============================================================================
// Plan Generation
// =============================================================================

/// Plan a parsed statement.
pub fn plan(statement: &Statement) -> Result<QueryPlan> {
    let root = match statement {
        Statement::Create(create) => plan_create(create)?,
        Statement::Match(match_clause) => plan_match(match_clause)?,
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
    let mut edges = Vec::new();
    let mut auto_var_counter = 0;

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
                    let edge_type = rp.types.first().cloned().unwrap_or_default();
                    let properties = plan_properties(&rp.properties)?;
                    let (source, target) = match rp.direction {
                        super::parser::Direction::Outgoing => (source_var, var.clone()),
                        super::parser::Direction::Incoming => (var.clone(), source_var),
                        super::parser::Direction::Both => (source_var, var.clone()), // Default to outgoing
                    };
                    edges.push(CreateEdge {
                        variable: rp.variable.clone(),
                        source,
                        target,
                        edge_type,
                        properties,
                    });
                }

                let labels: Vec<String> = np.labels.iter().flatten().cloned().collect();
                let properties = plan_properties(&np.properties)?;
                nodes.push(CreateNode {
                    variable: Some(var.clone()),
                    labels,
                    properties,
                });

                prev_node_var = Some(var);
            }
            PatternElement::Relationship(rp) => {
                // Store the relationship to be completed when we see the next node
                let source_var = prev_node_var.clone().ok_or_else(|| {
                    Error::Cypher("Relationship must follow a node".into())
                })?;
                pending_rel = Some((rp.clone(), source_var));
            }
        }
    }

    Ok(PlanOperator::Create {
        source: None,
        nodes,
        edges,
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

    // Check for SHORTEST path pattern
    if pattern.shortest_k.is_some() {
        return plan_shortest_path_pattern(pattern);
    }

    // Start with first node
    let first_node = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Pattern must start with a node".into())),
    };

    let variable = first_node.variable.clone().unwrap_or_else(|| "_n0".to_string());
    let labels: Vec<String> = first_node.labels.iter().flatten().cloned().collect();

    let mut plan = PlanOperator::NodeScan {
        variable: variable.clone(),
        labels,
        limit: None,
    };

    // Add inline property filter if present
    if let Some(ref props) = first_node.properties {
        let predicate = plan_inline_properties(&variable, props)?;
        if !matches!(predicate, FilterPredicate::True) {
            plan = PlanOperator::Filter {
                source: Box::new(plan),
                predicate,
            };
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
            _ => return Err(Error::Cypher("Expected target node after relationship".into())),
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
            };
        } else {
            // Single-hop expand
            plan = PlanOperator::Expand {
                source: Box::new(plan),
                source_variable: current_var.clone(),
                rel_variable: rel.variable.clone(),
                target_variable: target_var.clone(),
                target_labels: target_labels.clone(),
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

/// Plan a SHORTEST path pattern.
fn plan_shortest_path_pattern(pattern: &Pattern) -> Result<PlanOperator> {
    // SHORTEST requires exactly node-rel-node
    if pattern.elements.len() != 3 {
        return Err(Error::Cypher("SHORTEST requires (a)-[r]->(b) pattern".into()));
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

    let source_var = source_node.variable.clone().unwrap_or_else(|| "_src".to_string());
    let target_var = target_node.variable.clone().unwrap_or_else(|| "_tgt".to_string());
    let source_labels: Vec<String> = source_node.labels.iter().flatten().cloned().collect();
    let target_labels: Vec<String> = target_node.labels.iter().flatten().cloned().collect();

    // Determine hop bounds
    let (min_hops, max_hops) = if let Some(ref length) = rel.length {
        (length.min.unwrap_or(1), length.max.unwrap_or(100))
    } else if rel.quantifier.is_some() {
        (1, 100) // + or * quantifier
    } else {
        (1, 100)
    };

    let mut plan = PlanOperator::NodeScan {
        variable: source_var.clone(),
        labels: source_labels,
        limit: None,
    };

    // Add inline property filter for source
    if let Some(ref props) = source_node.properties {
        let predicate = plan_inline_properties(&source_var, props)?;
        if !matches!(predicate, FilterPredicate::True) {
            plan = PlanOperator::Filter {
                source: Box::new(plan),
                predicate,
            };
        }
    }

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
        k: pattern.shortest_k.unwrap_or(1),
    };

    // Add inline property filter for target (applied after BFS)
    if let Some(ref props) = target_node.properties {
        let predicate = plan_inline_properties(&target_var, props)?;
        if !matches!(predicate, FilterPredicate::True) {
            plan = PlanOperator::Filter {
                source: Box::new(plan),
                predicate,
            };
        }
    }

    Ok(plan)
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
                Err(Error::Cypher("Complex property access not supported".into()))
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
        Expression::BinaryOp { left, op, right } => {
            match op {
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
                _ => Err(Error::Cypher(format!("Operator {:?} not supported", op))),
            }
        }
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
            matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT")
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
                    return Err(Error::Cypher("COLLECT requires exactly one argument".into()));
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
                    // Check if source is a simple NodeScan without filters
                    if let PlanOperator::NodeScan {
                        ref labels,
                        limit: None,
                        ..
                    } = *source
                    {
                        // Can push COUNT to SQL
                        let label = if labels.is_empty() {
                            None
                        } else if labels.len() == 1 {
                            Some(labels[0].clone())
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
                    labels,
                    limit: None,
                } => PlanOperator::NodeScan {
                    variable,
                    labels,
                    limit: Some(count),
                },

                // LIMIT on Project(NodeScan) can be pushed through
                PlanOperator::Project {
                    source: inner_source,
                    columns,
                    distinct: false,
                } => {
                    if let PlanOperator::NodeScan {
                        variable,
                        labels,
                        limit: None,
                    } = *inner_source
                    {
                        PlanOperator::Project {
                            source: Box::new(PlanOperator::NodeScan {
                                variable,
                                labels,
                                limit: Some(count),
                            }),
                            columns,
                            distinct: false,
                        }
                    } else {
                        PlanOperator::Limit {
                            source: Box::new(PlanOperator::Project {
                                source: Box::new(optimize_operator(*inner_source)),
                                columns,
                                distinct: false,
                            }),
                            count,
                        }
                    }
                }

                other => PlanOperator::Limit {
                    source: Box::new(optimize_operator(other)),
                    count,
                },
            }
        }

        // Recursively optimize other operators
        PlanOperator::Filter { source, predicate } => PlanOperator::Filter {
            source: Box::new(optimize_operator(*source)),
            predicate,
        },
        PlanOperator::Project {
            source,
            columns,
            distinct,
        } => PlanOperator::Project {
            source: Box::new(optimize_operator(*source)),
            columns,
            distinct,
        },
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
            types,
            direction,
        } => PlanOperator::Expand {
            source: Box::new(optimize_operator(*source)),
            source_variable,
            rel_variable,
            target_variable,
            target_labels,
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
        PlanOperator::Create { source, nodes, edges } => PlanOperator::Create {
            source: source.map(|s| Box::new(optimize_operator(*s))),
            nodes,
            edges,
        },

        // Leaf operators - no optimization needed
        other => other,
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
}
