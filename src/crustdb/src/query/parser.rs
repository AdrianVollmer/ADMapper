//! Cypher query parser.
//!
//! Parses Cypher query strings into an Abstract Syntax Tree (AST).

use crate::error::{Error, Result};

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
    Property { variable: String, property: String, value: Expression },
    Labels { variable: String, labels: Vec<String> },
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
    pub labels: Vec<String>,
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
    Outgoing,  // ->
    Incoming,  // <-
    Both,      // --
}

/// Variable-length relationship specification.
#[derive(Debug, Clone)]
pub struct LengthSpec {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

/// An expression in the query.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Literal value
    Literal(Literal),
    /// Variable reference
    Variable(String),
    /// Property access: n.prop
    Property { base: Box<Expression>, property: String },
    /// Function call: func(args)
    FunctionCall { name: String, args: Vec<Expression> },
    /// Binary operation: a + b
    BinaryOp { left: Box<Expression>, op: BinaryOperator, right: Box<Expression> },
    /// Unary operation: NOT a
    UnaryOp { op: UnaryOperator, operand: Box<Expression> },
    /// List: [a, b, c]
    List(Vec<Expression>),
    /// Map: {a: 1, b: 2}
    Map(Vec<(String, Expression)>),
    /// CASE expression
    Case { operand: Option<Box<Expression>>, whens: Vec<(Expression, Expression)>, else_: Option<Box<Expression>> },
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
    Eq, Ne, Lt, Le, Gt, Ge,
    // Logical
    And, Or, Xor,
    // Arithmetic
    Add, Sub, Mul, Div, Mod, Pow,
    // String
    Contains, StartsWith, EndsWith,
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
    // TODO: Implement Cypher parser
    let _ = query;
    Err(Error::Parse("Cypher parser not yet implemented".to_string()))
}
