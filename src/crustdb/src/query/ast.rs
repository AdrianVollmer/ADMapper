//! Cypher AST (Abstract Syntax Tree) types.
//!
//! This module contains all the type definitions for the Cypher query AST.
//! These types represent the structure of parsed Cypher queries.

use serde::{Deserialize, Serialize};

/// A parsed Cypher statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
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
    /// Standalone RETURN (e.g., RETURN 1, RETURN "hello")
    Return(ReturnClause),
    /// UNION ALL of multiple queries
    UnionAll(Vec<Statement>),
}

impl Statement {
    /// Returns true if this statement only reads data (no mutations).
    ///
    /// This is useful for determining whether a query can safely run without
    /// exclusive database access. Read-only queries can potentially run
    /// concurrently with other read-only queries.
    pub fn is_read_only(&self) -> bool {
        match self {
            Statement::Match(m) => {
                m.set_clause.is_none() && m.delete_clause.is_none() && m.create_clause.is_none()
            }
            Statement::Return(_) => true,
            Statement::UnionAll(queries) => queries.iter().all(|q| q.is_read_only()),
            Statement::Create(_)
            | Statement::Merge(_)
            | Statement::Delete(_)
            | Statement::Set(_) => false,
        }
    }
}

/// MATCH clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub where_clause: Option<WhereClause>,
    pub return_clause: Option<ReturnClause>,
    pub delete_clause: Option<DeleteClause>,
    pub set_clause: Option<SetClause>,
    pub create_clause: Option<CreateClause>,
}

/// CREATE clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClause {
    pub pattern: Pattern,
}

/// MERGE clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create: Option<SetClause>,
    pub on_match: Option<SetClause>,
}

/// DELETE clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteClause {
    pub detach: bool,
    pub expressions: Vec<Expression>,
}

/// SET clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

/// A single SET item.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhereClause {
    pub predicate: Expression,
}

/// RETURN clause AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

/// A single RETURN item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// ORDER BY item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expression: Expression,
    pub descending: bool,
}

/// Shortest path mode for openCypher 9 shortestPath() and allShortestPaths().
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShortestPathMode {
    /// shortestPath() - returns single shortest path
    Single,
    /// allShortestPaths() - returns all paths of shortest length
    All,
}

/// A graph pattern (nodes and relationships).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
    /// Path variable name for `p = (a)-[*]->(b)` syntax.
    pub path_variable: Option<String>,
    /// Shortest path mode when using shortestPath() or allShortestPaths().
    pub shortest_path: Option<ShortestPathMode>,
}

/// An element in a pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

/// A node pattern like (n:Label {prop: value}).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePattern {
    pub variable: Option<String>,
    /// Labels to match. Each inner Vec is OR'd (alternatives), outer Vec is AND'd.
    /// Example: `:Person:Actor|Director` → `[["Person"], ["Actor", "Director"]]`
    pub labels: Vec<Vec<String>>,
    pub properties: Option<Expression>,
}

/// A relationship pattern like -[r:TYPE]->
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub properties: Option<Expression>,
    pub direction: Direction,
    pub length: Option<LengthSpec>,
}

/// Relationship direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Outgoing, // ->
    Incoming, // <-
    Both,     // --
}

/// Variable-length relationship specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LengthSpec {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

/// An expression in the query.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// shortestPath((a)-[*]->(b)) - returns single shortest path
    ShortestPath(Box<Pattern>),
    /// allShortestPaths((a)-[*]->(b)) - returns all paths of shortest length
    AllShortestPaths(Box<Pattern>),
    /// List predicate: ALL(x IN list WHERE predicate),
    /// ANY(x IN list WHERE predicate), NONE(...), SINGLE(...)
    ListPredicate {
        kind: ListPredicateKind,
        variable: String,
        list: Box<Expression>,
        filter: Option<Box<Expression>>,
    },
}

/// Kind of list predicate function.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ListPredicateKind {
    All,
    Any,
    None,
    Single,
}

/// A literal value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}
