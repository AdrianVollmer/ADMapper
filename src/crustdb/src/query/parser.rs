//! Cypher query parser.
//!
//! Parses Cypher query strings into an Abstract Syntax Tree (AST).
//!
//! Uses a pest grammar based on the openCypher specification.

use crate::error::{Error, Result};
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
    let pairs = CypherParser::parse(Rule::Cypher, query)
        .map_err(|e| Error::Parse(e.to_string()))?;

    // TODO: Convert pest parse tree to AST
    // For now, we just verify parsing succeeds
    let _ = pairs;
    Err(Error::Parse("AST conversion not yet implemented".to_string()))
}

/// Check if a Cypher query is syntactically valid without building an AST.
pub fn validate(query: &str) -> Result<()> {
    CypherParser::parse(Rule::Cypher, query)
        .map_err(|e| Error::Parse(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        assert!(validate("MATCH (n) RETURN n").is_ok());
    }

    #[test]
    fn test_parse_match_with_label() {
        assert!(validate("MATCH (n:Person) RETURN n").is_ok());
    }

    #[test]
    fn test_parse_match_with_properties() {
        assert!(validate("MATCH (n:Person {name: 'Alice'}) RETURN n").is_ok());
    }

    #[test]
    fn test_parse_match_with_where() {
        assert!(validate("MATCH (n:Person) WHERE n.age > 30 RETURN n").is_ok());
    }

    #[test]
    fn test_parse_simple_create() {
        assert!(validate("CREATE (n:Person {name: 'Alice'})").is_ok());
    }

    #[test]
    fn test_parse_create_relationship() {
        assert!(validate("CREATE (a:Person)-[:KNOWS]->(b:Person)").is_ok());
    }

    #[test]
    fn test_parse_relationship_pattern() {
        assert!(validate("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").is_ok());
    }

    #[test]
    fn test_parse_variable_length_path() {
        assert!(validate("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").is_ok());
    }

    #[test]
    fn test_parse_set_property() {
        assert!(validate("MATCH (n:Person {name: 'Alice'}) SET n.age = 31").is_ok());
    }

    #[test]
    fn test_parse_delete() {
        assert!(validate("MATCH (n:Person {name: 'Bob'}) DELETE n").is_ok());
    }

    #[test]
    fn test_parse_detach_delete() {
        assert!(validate("MATCH (n:Person {name: 'Charlie'}) DETACH DELETE n").is_ok());
    }

    #[test]
    fn test_parse_invalid_syntax() {
        assert!(validate("MATCH n RETURN").is_err());
    }

    #[test]
    fn test_parse_aggregate() {
        assert!(validate("MATCH (n:Person) RETURN count(n)").is_ok());
    }

    #[test]
    fn test_parse_complex_query() {
        let query = r#"
            MATCH (charlie:Person {name: 'Charlie Sheen'})-[:ACTED_IN]->(movie:Movie)
            WHERE movie.year > 1980
            RETURN movie.title, movie.year
            ORDER BY movie.year DESC
            LIMIT 10
        "#;
        assert!(validate(query).is_ok());
    }
}
