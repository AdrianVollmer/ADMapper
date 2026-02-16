//! Cypher query parser.
//!
//! Parses Cypher query strings into an Abstract Syntax Tree (AST).
//!
//! Uses a pest grammar based on the openCypher specification.

use crate::error::{Error, Result};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use pest_derive::Parser;

/// Pest parser for Cypher queries.
#[derive(Parser)]
#[grammar = "query/cypher.pest"]
pub struct CypherParser;

/// A parsed Cypher statement.
#[derive(Debug, Clone)]
pub enum Statement {
    /// MATCH ... RETURN query
    Match(MatchClause),
    /// CREATE statement
    Create(CreateClause),
    /// MERGE statement
    Merge(MergeClause),
    /// DELETE statement
    Delete(DeleteClause),
    /// SET statement
    Set(SetClause),
}

/// MATCH clause AST.
#[derive(Debug, Clone)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub where_clause: Option<WhereClause>,
    pub return_clause: Option<ReturnClause>,
    pub delete_clause: Option<DeleteClause>,
    pub set_clause: Option<SetClause>,
}

/// CREATE clause AST.
#[derive(Debug, Clone)]
pub struct CreateClause {
    pub pattern: Pattern,
}

/// MERGE clause AST.
#[derive(Debug, Clone)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create: Option<SetClause>,
    pub on_match: Option<SetClause>,
}

/// DELETE clause AST.
#[derive(Debug, Clone)]
pub struct DeleteClause {
    pub detach: bool,
    pub expressions: Vec<Expression>,
}

/// SET clause AST.
#[derive(Debug, Clone)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

/// A single SET item.
#[derive(Debug, Clone)]
pub enum SetItem {
    Property {
        variable: String,
        property: String,
        value: Expression,
    },
    Labels {
        variable: String,
        labels: Vec<String>,
    },
}

/// WHERE clause AST.
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub predicate: Expression,
}

/// RETURN clause AST.
#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

/// A single RETURN item.
#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// ORDER BY item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expression: Expression,
    pub descending: bool,
}

/// A graph pattern (nodes and relationships).
#[derive(Debug, Clone)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
    /// Path variable name for `p = (a)-[*]->(b)` syntax.
    pub path_variable: Option<String>,
}

/// An element in a pattern.
#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

/// A node pattern like (n:Label {prop: value}).
#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    /// Labels to match. Each inner Vec is OR'd (alternatives), outer Vec is AND'd.
    /// Example: `:Person:Actor|Director` → `[["Person"], ["Actor", "Director"]]`
    pub labels: Vec<Vec<String>>,
    pub properties: Option<Expression>,
}

/// A relationship pattern like -[r:TYPE]->
#[derive(Debug, Clone)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub properties: Option<Expression>,
    pub direction: Direction,
    pub length: Option<LengthSpec>,
}

/// Relationship direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing, // ->
    Incoming, // <-
    Both,     // --
}

/// Variable-length relationship specification.
#[derive(Debug, Clone)]
pub struct LengthSpec {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

/// Details extracted from a relationship pattern: (variable, types, length, properties).
type RelationshipDetail = (
    Option<String>,
    Vec<String>,
    Option<LengthSpec>,
    Option<Expression>,
);

/// An expression in the query.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Literal value
    Literal(Literal),
    /// Variable reference
    Variable(String),
    /// Property access: n.prop
    Property {
        base: Box<Expression>,
        property: String,
    },
    /// Function call: func(args)
    FunctionCall { name: String, args: Vec<Expression> },
    /// Binary operation: a + b
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation: NOT a
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
    /// List: [a, b, c]
    List(Vec<Expression>),
    /// Map: {a: 1, b: 2}
    Map(Vec<(String, Expression)>),
    /// CASE expression
    Case {
        operand: Option<Box<Expression>>,
        whens: Vec<(Expression, Expression)>,
        else_: Option<Box<Expression>>,
    },
    /// Parameter: $param
    Parameter(String),
}

/// A literal value.
#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    Xor,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    // String
    Contains,
    StartsWith,
    EndsWith,
    // Collection
    In,
    // Regex
    RegexMatch,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

/// Parse a Cypher query string into a statement.
pub fn parse(query: &str) -> Result<Statement> {
    let pairs =
        CypherParser::parse(Rule::Cypher, query).map_err(|e| Error::Parse(e.to_string()))?;

    build_ast(pairs)
}

/// Build AST from pest parse tree.
fn build_ast(pairs: Pairs<Rule>) -> Result<Statement> {
    for pair in pairs {
        match pair.as_rule() {
            Rule::Cypher => {
                return build_ast(pair.into_inner());
            }
            Rule::Statement => {
                return build_ast(pair.into_inner());
            }
            Rule::Query => {
                return build_ast(pair.into_inner());
            }
            Rule::RegularQuery => {
                return build_ast(pair.into_inner());
            }
            Rule::SingleQuery => {
                return build_ast(pair.into_inner());
            }
            Rule::SinglePartQuery => {
                return build_single_part_query(pair);
            }
            Rule::MultiPartQuery => {
                return Err(Error::Parse("Multi-part queries not yet supported".into()));
            }
            Rule::EOI => continue,
            _ => continue,
        }
    }
    Err(Error::Parse("Empty query".into()))
}

/// Build AST from a SinglePartQuery.
fn build_single_part_query(pair: Pair<Rule>) -> Result<Statement> {
    let mut reading_clauses = Vec::new();
    let mut updating_clauses = Vec::new();
    let mut return_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ReadingClause => {
                reading_clauses.push(inner);
            }
            Rule::UpdatingClause => {
                updating_clauses.push(inner);
            }
            Rule::Return => {
                return_clause = Some(build_return_clause(inner)?);
            }
            _ => {}
        }
    }

    // Extract DELETE and SET clauses from updating clauses
    let mut delete_clause = None;
    let mut set_clause = None;

    for updating in &updating_clauses {
        for inner in updating.clone().into_inner() {
            match inner.as_rule() {
                Rule::Create => {
                    // Standalone CREATE without MATCH
                    return build_create_statement(inner);
                }
                Rule::Merge => {
                    // Standalone MERGE
                    return build_merge_statement(inner);
                }
                Rule::Delete => {
                    delete_clause = Some(build_delete_clause(inner)?);
                }
                Rule::Set => {
                    set_clause = Some(build_set_clause(inner)?);
                }
                _ => {}
            }
        }
    }

    // Handle reading clauses (MATCH) with optional DELETE/SET
    if !reading_clauses.is_empty() {
        let first_reading = reading_clauses.into_iter().next().unwrap();
        for inner in first_reading.into_inner() {
            if inner.as_rule() == Rule::Match {
                return build_match_statement(inner, return_clause, delete_clause, set_clause);
            }
        }
    }

    // Standalone DELETE or SET without MATCH is not supported
    if delete_clause.is_some() || set_clause.is_some() {
        return Err(Error::Parse("DELETE and SET require a MATCH clause".into()));
    }

    Err(Error::Parse("Unsupported query type".into()))
}

/// Build a CREATE statement.
fn build_create_statement(pair: Pair<Rule>) -> Result<Statement> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Pattern {
            let pattern = build_pattern(inner)?;
            return Ok(Statement::Create(CreateClause { pattern }));
        }
    }
    Err(Error::Parse("CREATE requires a pattern".into()))
}

/// Build a MATCH statement.
fn build_match_statement(
    pair: Pair<Rule>,
    return_clause: Option<ReturnClause>,
    delete_clause: Option<DeleteClause>,
    set_clause: Option<SetClause>,
) -> Result<Statement> {
    let mut pattern = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Pattern => {
                pattern = Some(build_pattern(inner)?);
            }
            Rule::Where => {
                where_clause = Some(build_where_clause(inner)?);
            }
            _ => {}
        }
    }

    let pattern = pattern.ok_or_else(|| Error::Parse("MATCH requires a pattern".into()))?;
    Ok(Statement::Match(MatchClause {
        pattern,
        where_clause,
        return_clause,
        delete_clause,
        set_clause,
    }))
}

/// Build a MERGE statement.
fn build_merge_statement(pair: Pair<Rule>) -> Result<Statement> {
    let mut pattern = None;
    let mut on_create = None;
    let mut on_match = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PatternPart => {
                let (path_variable, elements) = build_pattern_part(inner)?;
                pattern = Some(Pattern {
                    elements,
                    path_variable,
                });
            }
            Rule::MergeAction => {
                let (is_create, set_clause) = build_merge_action(inner)?;
                if is_create {
                    on_create = Some(set_clause);
                } else {
                    on_match = Some(set_clause);
                }
            }
            _ => {}
        }
    }

    let pattern = pattern.ok_or_else(|| Error::Parse("MERGE requires a pattern".into()))?;
    Ok(Statement::Merge(MergeClause {
        pattern,
        on_create,
        on_match,
    }))
}

/// Build a merge action (ON CREATE SET / ON MATCH SET).
fn build_merge_action(pair: Pair<Rule>) -> Result<(bool, SetClause)> {
    let mut is_create = false;
    let mut set_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::CREATE => is_create = true,
            Rule::MATCH => is_create = false,
            Rule::Set => set_clause = Some(build_set_clause(inner)?),
            _ => {}
        }
    }

    let set_clause = set_clause.ok_or_else(|| Error::Parse("Merge action requires SET".into()))?;
    Ok((is_create, set_clause))
}

/// Build a DELETE clause.
fn build_delete_clause(pair: Pair<Rule>) -> Result<DeleteClause> {
    let mut detach = false;
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DETACH => detach = true,
            Rule::Expression => {
                expressions.push(build_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(DeleteClause {
        detach,
        expressions,
    })
}

/// Build a SET clause.
fn build_set_clause(pair: Pair<Rule>) -> Result<SetClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SetItem {
            items.push(build_set_item(inner)?);
        }
    }

    Ok(SetClause { items })
}

/// Build a single SET item.
fn build_set_item(pair: Pair<Rule>) -> Result<SetItem> {
    let mut parts: Vec<Pair<Rule>> = pair.into_inner().collect();

    // Check if this is a property expression (n.prop = value)
    if let Some(first) = parts.first() {
        if first.as_rule() == Rule::PropertyExpression {
            let prop_expr = parts.remove(0);
            let (variable, property) = build_property_expression_parts(prop_expr)?;

            // Find the value expression
            for part in parts {
                if part.as_rule() == Rule::Expression {
                    let value = build_expression(part)?;
                    return Ok(SetItem::Property {
                        variable,
                        property,
                        value,
                    });
                }
            }
        } else if first.as_rule() == Rule::Variable {
            let var = extract_variable(parts.remove(0))?;

            // Check if this is label assignment or value assignment
            for part in parts {
                match part.as_rule() {
                    Rule::NodeLabels => {
                        // Flatten label groups for SET (all labels are added)
                        let label_groups = build_node_labels(part)?;
                        let labels: Vec<String> = label_groups.into_iter().flatten().collect();
                        return Ok(SetItem::Labels {
                            variable: var,
                            labels,
                        });
                    }
                    Rule::Expression => {
                        // This is actually setting a variable to a value, treat as error for now
                        return Err(Error::Parse(
                            "Setting variable to expression not supported, use property access"
                                .into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    Err(Error::Parse("Invalid SET item".into()))
}

/// Extract variable and property from a PropertyExpression.
fn build_property_expression_parts(pair: Pair<Rule>) -> Result<(String, String)> {
    let mut variable = None;
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Atom => {
                // The atom should contain a Variable
                for atom_inner in inner.into_inner() {
                    if atom_inner.as_rule() == Rule::Variable {
                        variable = Some(extract_variable(atom_inner)?);
                    }
                }
            }
            Rule::PropertyLookup => {
                for lookup_inner in inner.into_inner() {
                    if lookup_inner.as_rule() == Rule::PropertyKeyName {
                        property = Some(extract_schema_name(lookup_inner)?);
                    }
                }
            }
            _ => {}
        }
    }

    let variable =
        variable.ok_or_else(|| Error::Parse("Property expression missing variable".into()))?;
    let property =
        property.ok_or_else(|| Error::Parse("Property expression missing property".into()))?;
    Ok((variable, property))
}

/// Build a WHERE clause.
fn build_where_clause(pair: Pair<Rule>) -> Result<WhereClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            let predicate = build_expression(inner)?;
            return Ok(WhereClause { predicate });
        }
    }
    Err(Error::Parse("WHERE requires an expression".into()))
}

/// Build a RETURN clause.
fn build_return_clause(pair: Pair<Rule>) -> Result<ReturnClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::ProjectionBody {
            return build_projection_body(inner);
        }
    }
    Err(Error::Parse("RETURN requires projection body".into()))
}

/// Build projection body (the part after RETURN/WITH).
fn build_projection_body(pair: Pair<Rule>) -> Result<ReturnClause> {
    let mut distinct = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DISTINCT => distinct = true,
            Rule::ProjectionItems => {
                items = build_projection_items(inner)?;
            }
            Rule::Order => {
                order_by = Some(build_order_by(inner)?);
            }
            Rule::Skip => {
                skip = Some(build_skip_or_limit(inner)?);
            }
            Rule::Limit => {
                limit = Some(build_skip_or_limit(inner)?);
            }
            _ => {}
        }
    }

    Ok(ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    })
}

/// Build projection items.
fn build_projection_items(pair: Pair<Rule>) -> Result<Vec<ReturnItem>> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::STAR => {
                // Return all - represented as a special expression
                items.push(ReturnItem {
                    expression: Expression::Variable("*".into()),
                    alias: None,
                });
            }
            Rule::ProjectionItem => {
                items.push(build_projection_item(inner)?);
            }
            _ => {}
        }
    }

    Ok(items)
}

/// Build a single projection item.
fn build_projection_item(pair: Pair<Rule>) -> Result<ReturnItem> {
    let mut expression = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Expression => {
                expression = Some(build_expression(inner)?);
            }
            Rule::Variable => {
                alias = Some(extract_variable(inner)?);
            }
            _ => {}
        }
    }

    let expression =
        expression.ok_or_else(|| Error::Parse("Projection item requires expression".into()))?;
    Ok(ReturnItem { expression, alias })
}

/// Build ORDER BY clause.
fn build_order_by(pair: Pair<Rule>) -> Result<Vec<OrderByItem>> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SortItem {
            items.push(build_sort_item(inner)?);
        }
    }

    Ok(items)
}

/// Build a single sort item.
fn build_sort_item(pair: Pair<Rule>) -> Result<OrderByItem> {
    let mut expression = None;
    let mut descending = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Expression => {
                expression = Some(build_expression(inner)?);
            }
            Rule::DESCENDING | Rule::DESC => descending = true,
            Rule::ASCENDING | Rule::ASC => descending = false,
            _ => {}
        }
    }

    let expression =
        expression.ok_or_else(|| Error::Parse("Sort item requires expression".into()))?;
    Ok(OrderByItem {
        expression,
        descending,
    })
}

/// Build SKIP or LIMIT value.
fn build_skip_or_limit(pair: Pair<Rule>) -> Result<u64> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            let expr = build_expression(inner)?;
            if let Expression::Literal(Literal::Integer(n)) = expr {
                return Ok(n as u64);
            }
            return Err(Error::Parse("SKIP/LIMIT requires integer literal".into()));
        }
    }
    Err(Error::Parse("SKIP/LIMIT requires expression".into()))
}

/// Build a pattern from a Pattern rule.
fn build_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut elements = Vec::new();
    let mut path_variable = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::PatternPart {
            let (path_var, part_elements) = build_pattern_part(inner)?;
            if path_var.is_some() {
                path_variable = path_var;
            }
            elements.extend(part_elements);
        }
    }

    Ok(Pattern {
        elements,
        path_variable,
    })
}

/// Build pattern elements from a PatternPart.
/// Returns (path_variable, elements).
fn build_pattern_part(pair: Pair<Rule>) -> Result<(Option<String>, Vec<PatternElement>)> {
    let mut path_variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::AnonymousPatternPart => {
                let elements = build_anonymous_pattern_part(inner)?;
                return Ok((path_variable, elements));
            }
            Rule::Variable => {
                // Named pattern (p = ...)
                path_variable = Some(extract_variable(inner)?);
            }
            _ => {}
        }
    }
    Err(Error::Parse(
        "PatternPart requires AnonymousPatternPart".into(),
    ))
}

/// Build pattern elements from an AnonymousPatternPart.
fn build_anonymous_pattern_part(pair: Pair<Rule>) -> Result<Vec<PatternElement>> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::PatternElement {
            return build_pattern_element(inner);
        }
    }
    Err(Error::Parse(
        "AnonymousPatternPart requires PatternElement".into(),
    ))
}

/// Build pattern elements from a PatternElement.
fn build_pattern_element(pair: Pair<Rule>) -> Result<Vec<PatternElement>> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NodePattern => {
                elements.push(PatternElement::Node(build_node_pattern(inner)?));
            }
            Rule::PatternElementChain => {
                let (rel, node) = build_pattern_element_chain(inner)?;
                elements.push(PatternElement::Relationship(rel));
                elements.push(PatternElement::Node(node));
            }
            Rule::PatternElement => {
                // Parenthesized pattern element
                let nested = build_pattern_element(inner)?;
                elements.extend(nested);
            }
            _ => {}
        }
    }

    Ok(elements)
}

/// Build a relationship and node from a PatternElementChain.
fn build_pattern_element_chain(pair: Pair<Rule>) -> Result<(RelationshipPattern, NodePattern)> {
    let mut rel = None;
    let mut node = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::RelationshipPattern => {
                rel = Some(build_relationship_pattern(inner)?);
            }
            Rule::NodePattern => {
                node = Some(build_node_pattern(inner)?);
            }
            _ => {}
        }
    }

    let rel = rel.ok_or_else(|| Error::Parse("Chain requires relationship".into()))?;
    let node = node.ok_or_else(|| Error::Parse("Chain requires node".into()))?;
    Ok((rel, node))
}

/// Build a NodePattern from a NodePattern rule.
fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Variable => {
                variable = Some(extract_variable(inner)?);
            }
            Rule::NodeLabels => {
                labels = build_node_labels(inner)?;
            }
            Rule::Properties => {
                properties = Some(build_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
    })
}

/// Build node labels from a NodeLabels rule.
/// Returns Vec<Vec<String>> where each inner Vec is OR'd (alternatives), outer Vec is AND'd.
fn build_node_labels(pair: Pair<Rule>) -> Result<Vec<Vec<String>>> {
    let mut label_groups = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::NodeLabel {
            let mut alternatives = Vec::new();
            for label_inner in inner.into_inner() {
                if label_inner.as_rule() == Rule::LabelName {
                    alternatives.push(extract_schema_name(label_inner)?);
                }
            }
            if !alternatives.is_empty() {
                label_groups.push(alternatives);
            }
        }
    }

    Ok(label_groups)
}

/// Build a RelationshipPattern from a RelationshipPattern rule.
fn build_relationship_pattern(pair: Pair<Rule>) -> Result<RelationshipPattern> {
    let mut variable = None;
    let mut types = Vec::new();
    let mut properties = None;
    let mut length = None;
    let mut has_left_arrow = false;
    let mut has_right_arrow = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::LeftArrowHead => has_left_arrow = true,
            Rule::RightArrowHead => has_right_arrow = true,
            Rule::RelationshipDetail => {
                let detail = build_relationship_detail(inner)?;
                variable = detail.0;
                types = detail.1;
                length = detail.2;
                properties = detail.3;
            }
            Rule::Dash => {}
            _ => {}
        }
    }

    let direction = match (has_left_arrow, has_right_arrow) {
        (false, true) => Direction::Outgoing, // -->
        (true, false) => Direction::Incoming, // <--
        (true, true) => Direction::Both,      // <-->
        (false, false) => Direction::Both,    // --
    };

    Ok(RelationshipPattern {
        variable,
        types,
        properties,
        direction,
        length,
    })
}

/// Build relationship detail (variable, types, range, properties).
fn build_relationship_detail(pair: Pair<Rule>) -> Result<RelationshipDetail> {
    let mut variable = None;
    let mut types = Vec::new();
    let mut length = None;
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Variable => {
                variable = Some(extract_variable(inner)?);
            }
            Rule::RelationshipTypes => {
                types = build_relationship_types(inner)?;
            }
            Rule::RangeLiteral => {
                length = Some(build_range_literal(inner)?);
            }
            Rule::Properties => {
                properties = Some(build_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok((variable, types, length, properties))
}

/// Build relationship types from a RelationshipTypes rule.
fn build_relationship_types(pair: Pair<Rule>) -> Result<Vec<String>> {
    let mut types = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::RelTypeName {
            types.push(extract_schema_name(inner)?);
        }
    }

    Ok(types)
}

/// Build a range literal (*1..3).
fn build_range_literal(pair: Pair<Rule>) -> Result<LengthSpec> {
    let mut min = None;
    let mut max = None;
    let mut saw_dots = false;
    let mut first_int = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::IntegerLiteral => {
                let n = parse_integer_literal(inner)? as u32;
                if !saw_dots && first_int {
                    min = Some(n);
                    first_int = false;
                } else {
                    max = Some(n);
                }
            }
            Rule::DOT_DOT => {
                saw_dots = true;
            }
            _ => {}
        }
    }

    // If we only have one number and no dots, it's an exact length
    if !saw_dots && min.is_some() {
        max = min;
    }

    Ok(LengthSpec { min, max })
}

/// Build properties from a Properties rule.
fn build_properties(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::MapLiteral => {
                return build_map_literal(inner);
            }
            Rule::Parameter => {
                return build_parameter(inner);
            }
            _ => {}
        }
    }
    Err(Error::Parse("Properties requires map or parameter".into()))
}

/// Build an expression from an Expression rule.
fn build_expression(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_string_list_null_expression(pair: Pair<Rule>) -> Result<Expression> {
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
fn apply_string_list_null_operator(base: Expression, pair: Pair<Rule>) -> Result<Expression> {
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
fn build_property_or_labels_expression(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_atom(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Literal => {
                return build_literal(inner);
            }
            Rule::Variable => {
                return Ok(Expression::Variable(extract_variable(inner)?));
            }
            Rule::Parameter => {
                return build_parameter(inner);
            }
            Rule::FunctionInvocation => {
                return build_function_invocation(inner);
            }
            Rule::ParenthesizedExpression => {
                for paren_inner in inner.into_inner() {
                    if paren_inner.as_rule() == Rule::Expression {
                        return build_expression(paren_inner);
                    }
                }
            }
            Rule::CaseExpression => {
                return build_case_expression(inner);
            }
            Rule::COUNT => {
                // count(*) - handled specially
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
            _ => {}
        }
    }
    Err(Error::Parse("Unknown atom type".into()))
}

/// Build a literal value.
fn build_literal(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_number_literal(pair: Pair<Rule>) -> Result<Expression> {
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
fn parse_integer_literal(pair: Pair<Rule>) -> Result<i64> {
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
fn parse_string_literal(pair: Pair<Rule>) -> Result<String> {
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
fn unescape_string(s: &str) -> String {
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
fn build_map_literal(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_list_literal(pair: Pair<Rule>) -> Result<Expression> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            elements.push(build_expression(inner)?);
        }
    }

    Ok(Expression::List(elements))
}

/// Build a parameter ($param).
fn build_parameter(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_function_invocation(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_function_name(pair: Pair<Rule>) -> Result<String> {
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
fn build_case_expression(pair: Pair<Rule>) -> Result<Expression> {
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
fn build_case_alternative(pair: Pair<Rule>) -> Result<(Expression, Expression)> {
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
fn extract_variable(pair: Pair<Rule>) -> Result<String> {
    let pair_str = pair.as_str().to_string();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SymbolicName {
            return Ok(extract_symbolic_name(inner));
        }
    }
    Ok(pair_str)
}

/// Extract a schema name (label, property key, etc.).
fn extract_schema_name(pair: Pair<Rule>) -> Result<String> {
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
fn extract_symbolic_name(pair: Pair<Rule>) -> String {
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

/// Check if a Cypher query is syntactically valid without building an AST.
pub fn validate(query: &str) -> Result<()> {
    CypherParser::parse(Rule::Cypher, query).map_err(|e| Error::Parse(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Syntax validation tests
    #[test]
    fn test_validate_simple_match() {
        assert!(validate("MATCH (n) RETURN n").is_ok());
    }

    #[test]
    fn test_validate_invalid_syntax() {
        assert!(validate("MATCH n RETURN").is_err());
    }

    // AST building tests for CREATE (M2)
    #[test]
    fn test_parse_create_single_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        match stmt {
            Statement::Create(create) => {
                assert_eq!(create.pattern.elements.len(), 1);
                match &create.pattern.elements[0] {
                    PatternElement::Node(node) => {
                        assert_eq!(node.variable, Some("n".to_string()));
                        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
                        assert!(node.properties.is_some());
                    }
                    _ => panic!("Expected node pattern"),
                }
            }
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_create_node_no_properties() {
        let stmt = parse("CREATE (n:Person)").unwrap();
        match stmt {
            Statement::Create(create) => {
                assert_eq!(create.pattern.elements.len(), 1);
                match &create.pattern.elements[0] {
                    PatternElement::Node(node) => {
                        assert_eq!(node.variable, Some("n".to_string()));
                        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
                        assert!(node.properties.is_none());
                    }
                    _ => panic!("Expected node pattern"),
                }
            }
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_create_node_multiple_labels() {
        let stmt = parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap();
        match stmt {
            Statement::Create(create) => match &create.pattern.elements[0] {
                PatternElement::Node(node) => {
                    assert_eq!(
                        node.labels,
                        vec![vec!["Person".to_string()], vec!["Actor".to_string()]]
                    );
                }
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_create_relationship() {
        let stmt =
            parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
        match stmt {
            Statement::Create(create) => {
                assert_eq!(create.pattern.elements.len(), 3);

                // First node
                match &create.pattern.elements[0] {
                    PatternElement::Node(node) => {
                        assert_eq!(node.variable, Some("a".to_string()));
                        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
                    }
                    _ => panic!("Expected node pattern"),
                }

                // Relationship
                match &create.pattern.elements[1] {
                    PatternElement::Relationship(rel) => {
                        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
                        assert_eq!(rel.direction, Direction::Outgoing);
                    }
                    _ => panic!("Expected relationship pattern"),
                }

                // Second node
                match &create.pattern.elements[2] {
                    PatternElement::Node(node) => {
                        assert_eq!(node.variable, Some("b".to_string()));
                    }
                    _ => panic!("Expected node pattern"),
                }
            }
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_create_relationship_with_properties() {
        let stmt = parse("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)").unwrap();
        match stmt {
            Statement::Create(create) => match &create.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    assert_eq!(rel.types, vec!["KNOWS".to_string()]);
                    assert!(rel.properties.is_some());
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_create_anonymous_node() {
        let stmt = parse("CREATE (:Person {name: 'Alice'})").unwrap();
        match stmt {
            Statement::Create(create) => match &create.pattern.elements[0] {
                PatternElement::Node(node) => {
                    assert_eq!(node.variable, None);
                    assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
                }
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_map_properties() {
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30, active: true})").unwrap();
        match stmt {
            Statement::Create(create) => match &create.pattern.elements[0] {
                PatternElement::Node(node) => match node.properties.as_ref().unwrap() {
                    Expression::Map(entries) => {
                        assert_eq!(entries.len(), 3);
                        assert_eq!(entries[0].0, "name");
                        assert_eq!(entries[1].0, "age");
                        assert_eq!(entries[2].0, "active");
                    }
                    _ => panic!("Expected map expression"),
                },
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_literal_values() {
        let stmt =
            parse("CREATE (n {str: 'hello', int: 42, float: 3.14, bool: true, null_val: null})")
                .unwrap();
        match stmt {
            Statement::Create(create) => match &create.pattern.elements[0] {
                PatternElement::Node(node) => match node.properties.as_ref().unwrap() {
                    Expression::Map(entries) => {
                        assert!(matches!(
                            entries[0].1,
                            Expression::Literal(Literal::String(_))
                        ));
                        assert!(matches!(
                            entries[1].1,
                            Expression::Literal(Literal::Integer(42))
                        ));
                        assert!(matches!(
                            entries[2].1,
                            Expression::Literal(Literal::Float(_))
                        ));
                        assert!(matches!(
                            entries[3].1,
                            Expression::Literal(Literal::Boolean(true))
                        ));
                        assert!(matches!(entries[4].1, Expression::Literal(Literal::Null)));
                    }
                    _ => panic!("Expected map expression"),
                },
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected CREATE statement"),
        }
    }

    // MATCH tests
    #[test]
    fn test_parse_simple_match() {
        let stmt = parse("MATCH (n) RETURN n").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.pattern.elements.len(), 1);
                assert!(m.return_clause.is_some());
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_match_with_where() {
        let stmt = parse("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert!(m.where_clause.is_some());
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_match_relationship() {
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.pattern.elements.len(), 3);
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    let length = rel.length.as_ref().unwrap();
                    assert_eq!(length.min, Some(1));
                    assert_eq!(length.max, Some(3));
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_return_with_alias() {
        let stmt = parse("MATCH (n:Person) RETURN n.name AS personName").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.unwrap();
                assert_eq!(ret.items[0].alias, Some("personName".to_string()));
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_order_by_limit() {
        let stmt = parse("MATCH (n:Person) RETURN n ORDER BY n.age DESC LIMIT 10").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.unwrap();
                assert!(ret.order_by.is_some());
                let order = ret.order_by.unwrap();
                assert!(order[0].descending);
                assert_eq!(ret.limit, Some(10));
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    // DELETE tests
    #[test]
    fn test_parse_delete() {
        let stmt = parse("MATCH (n:Person {name: 'Bob'}) DELETE n").unwrap();
        match stmt {
            Statement::Match(m) => {
                let del = m.delete_clause.expect("Expected delete clause");
                assert!(!del.detach);
                assert_eq!(del.expressions.len(), 1);
            }
            _ => panic!("Expected MATCH statement with DELETE clause"),
        }
    }

    #[test]
    fn test_parse_detach_delete() {
        let stmt = parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        match stmt {
            Statement::Match(m) => {
                let del = m.delete_clause.expect("Expected delete clause");
                assert!(del.detach);
            }
            _ => panic!("Expected MATCH statement with DELETE clause"),
        }
    }

    // SET tests
    #[test]
    fn test_parse_set_property() {
        let stmt = parse("MATCH (n:Person) SET n.age = 31").unwrap();
        match stmt {
            Statement::Match(m) => {
                let set = m.set_clause.expect("Expected set clause");
                assert_eq!(set.items.len(), 1);
                match &set.items[0] {
                    SetItem::Property {
                        variable, property, ..
                    } => {
                        assert_eq!(variable, "n");
                        assert_eq!(property, "age");
                    }
                    _ => panic!("Expected property set item"),
                }
            }
            _ => panic!("Expected MATCH statement with SET clause"),
        }
    }

    #[test]
    fn test_parse_set_label() {
        let stmt = parse("MATCH (n:Person) SET n:Employee").unwrap();
        match stmt {
            Statement::Match(m) => {
                let set = m.set_clause.expect("Expected set clause");
                assert_eq!(set.items.len(), 1);
                match &set.items[0] {
                    SetItem::Labels { variable, labels } => {
                        assert_eq!(variable, "n");
                        assert_eq!(labels, &["Employee"]);
                    }
                    _ => panic!("Expected label set item"),
                }
            }
            _ => panic!("Expected MATCH statement with SET clause"),
        }
    }

    #[test]
    fn test_parse_label_or() {
        let stmt = parse("MATCH (n:Person|Company) RETURN n").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[0] {
                PatternElement::Node(node) => {
                    // Single label group with two alternatives
                    assert_eq!(
                        node.labels,
                        vec![vec!["Person".to_string(), "Company".to_string()]]
                    );
                }
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_label_or_with_and() {
        let stmt = parse("MATCH (n:Person:Actor|Director) RETURN n").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[0] {
                PatternElement::Node(node) => {
                    // First group: just Person; second group: Actor or Director
                    assert_eq!(
                        node.labels,
                        vec![
                            vec!["Person".to_string()],
                            vec!["Actor".to_string(), "Director".to_string()]
                        ]
                    );
                }
                _ => panic!("Expected node pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    // Expression tests
    #[test]
    fn test_parse_comparison_operators() {
        let stmt = parse("MATCH (n) WHERE n.a = 1 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.a <> 1 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.a < 1 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.a > 1 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));
    }

    #[test]
    fn test_parse_logical_operators() {
        let stmt = parse("MATCH (n) WHERE n.a = 1 AND n.b = 2 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.a = 1 OR n.b = 2 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE NOT n.a = 1 RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));
    }

    #[test]
    fn test_parse_null_check() {
        let stmt = parse("MATCH (n) WHERE n.a IS NULL RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.a IS NOT NULL RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));
    }

    #[test]
    fn test_parse_string_operators() {
        let stmt = parse("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.name ENDS WITH 'z' RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));

        let stmt = parse("MATCH (n) WHERE n.name CONTAINS 'test' RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));
    }

    #[test]
    fn test_parse_function_call() {
        let stmt = parse("MATCH (n:Person) RETURN count(n)").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.unwrap();
                match &ret.items[0].expression {
                    Expression::FunctionCall { name, args } => {
                        assert_eq!(name, "count");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected function call"),
                }
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_case_insensitive_keywords() {
        assert!(parse("create (n:Person)").is_ok());
        assert!(parse("CREATE (n:Person)").is_ok());
        assert!(parse("CrEaTe (n:Person)").is_ok());
    }

    // M6: Variable-length pattern tests
    #[test]
    fn test_parse_variable_length_range() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.pattern.elements.len(), 3);
                match &m.pattern.elements[1] {
                    PatternElement::Relationship(rel) => {
                        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
                        let len = rel.length.as_ref().unwrap();
                        assert_eq!(len.min, Some(1));
                        assert_eq!(len.max, Some(3));
                    }
                    _ => panic!("Expected relationship pattern"),
                }
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_unbounded() {
        let stmt = parse("MATCH (a)-[:KNOWS*]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    let len = rel.length.as_ref().unwrap();
                    assert_eq!(len.min, None);
                    assert_eq!(len.max, None);
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_exact() {
        let stmt = parse("MATCH (a)-[:KNOWS*2]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    let len = rel.length.as_ref().unwrap();
                    assert_eq!(len.min, Some(2));
                    assert_eq!(len.max, Some(2));
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_min_only() {
        let stmt = parse("MATCH (a)-[:KNOWS*2..]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    let len = rel.length.as_ref().unwrap();
                    assert_eq!(len.min, Some(2));
                    assert_eq!(len.max, None);
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_max_only() {
        let stmt = parse("MATCH (a)-[*..3]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    let len = rel.length.as_ref().unwrap();
                    assert_eq!(len.min, None);
                    assert_eq!(len.max, Some(3));
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }
}
