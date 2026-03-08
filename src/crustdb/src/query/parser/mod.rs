//! Cypher query parser.
//!
//! Parses Cypher query strings into an Abstract Syntax Tree (AST).
//!
//! Uses a pest grammar based on the openCypher specification.

use crate::error::{Error, Result};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use pest_derive::Parser;

// Re-export AST types for backwards compatibility
pub use super::ast::{
    BinaryOperator, CreateClause, DeleteClause, Direction, Expression, LengthSpec,
    ListPredicateKind, Literal, NodePattern, Pattern, PatternElement, RelationshipPattern,
    ReturnClause, SetClause, SetItem, ShortestPathMode, Statement, UnaryOperator,
};

mod clause;
mod expression;
mod pattern;

use clause::*;
use expression::*;
use pattern::*;

/// Pest parser for Cypher queries.
#[derive(Parser)]
#[grammar = "query/cypher.pest"]
pub struct CypherParser;

/// Details extracted from a relationship pattern: (variable, types, length, properties).
type RelationshipDetail = (
    Option<String>,
    Vec<String>,
    Option<LengthSpec>,
    Option<Expression>,
);

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

    // Extract DELETE, SET, and CREATE clauses from updating clauses
    let mut delete_clause = None;
    let mut set_clause = None;
    let mut create_clause = None;
    let mut merge_pair = None;

    for updating in &updating_clauses {
        for inner in updating.clone().into_inner() {
            match inner.as_rule() {
                Rule::Create => {
                    create_clause = Some(inner);
                }
                Rule::Merge => {
                    merge_pair = Some(inner);
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

    // Handle reading clauses (MATCH) with optional updating clauses
    if !reading_clauses.is_empty() {
        let first_reading = reading_clauses.into_iter().next().unwrap();
        for inner in first_reading.into_inner() {
            if inner.as_rule() == Rule::Match {
                let create = create_clause
                    .map(|pair| build_create_clause(pair))
                    .transpose()?;
                return build_match_statement(
                    inner,
                    return_clause,
                    delete_clause,
                    set_clause,
                    create,
                );
            }
        }
    }

    // Standalone CREATE without MATCH
    if let Some(pair) = create_clause {
        return build_create_statement(pair);
    }

    // Standalone MERGE
    if let Some(pair) = merge_pair {
        return build_merge_statement(pair);
    }

    // Standalone DELETE or SET without MATCH is not supported
    if delete_clause.is_some() || set_clause.is_some() {
        return Err(Error::Parse("DELETE and SET require a MATCH clause".into()));
    }

    // Standalone RETURN (e.g., RETURN 1, RETURN "hello")
    if let Some(ret) = return_clause {
        return Ok(Statement::Return(ret));
    }

    Err(Error::Parse("Unsupported query type".into()))
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

    #[test]
    fn test_parse_shortest_path_function() {
        // openCypher 9 shortestPath() syntax
        let stmt =
            parse("MATCH (a:Person), (b:Person) RETURN shortestPath((a)-[:KNOWS*]->(b))").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.as_ref().expect("Expected RETURN clause");
                assert_eq!(ret.items.len(), 1);
                match &ret.items[0].expression {
                    Expression::ShortestPath(pattern) => {
                        assert_eq!(pattern.elements.len(), 3); // node-rel-node
                    }
                    _ => panic!("Expected ShortestPath expression"),
                }
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_all_shortest_paths_function() {
        // openCypher 9 allShortestPaths() syntax
        let stmt = parse("MATCH (a), (b) RETURN allShortestPaths((a)-[:KNOWS*1..5]->(b))").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.as_ref().expect("Expected RETURN clause");
                assert_eq!(ret.items.len(), 1);
                match &ret.items[0].expression {
                    Expression::AllShortestPaths(pattern) => {
                        assert_eq!(pattern.elements.len(), 3);
                    }
                    _ => panic!("Expected AllShortestPaths expression"),
                }
            }
            _ => panic!("Expected MATCH statement"),
        }
    }

    #[test]
    fn test_parse_variable_length_one_or_more() {
        // Variable length with range (openCypher 9 syntax using *1..)
        let stmt = parse("MATCH (a)-[:LINK*1..]->(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => match &m.pattern.elements[1] {
                PatternElement::Relationship(rel) => {
                    assert!(rel.length.is_some());
                    let len = rel.length.as_ref().unwrap();
                    assert_eq!(len.min, Some(1));
                    assert_eq!(len.max, None);
                }
                _ => panic!("Expected relationship pattern"),
            },
            _ => panic!("Expected MATCH statement"),
        }
    }

    // is_read_only() tests
    #[test]
    fn test_is_read_only_match() {
        let stmt = parse("MATCH (n:Person) RETURN n").unwrap();
        assert!(stmt.is_read_only());
    }

    #[test]
    fn test_is_read_only_match_with_where() {
        let stmt = parse("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap();
        assert!(stmt.is_read_only());
    }

    #[test]
    fn test_is_read_only_create() {
        let stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(!stmt.is_read_only());
    }

    #[test]
    fn test_is_read_only_match_create() {
        let stmt = parse("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)").unwrap();
        assert!(!stmt.is_read_only());
        // Should be parsed as Statement::Match with create_clause
        if let Statement::Match(m) = &stmt {
            assert!(m.create_clause.is_some());
        } else {
            panic!("Expected Statement::Match, got {:?}", stmt);
        }
    }

    #[test]
    fn test_is_read_only_match_with_set() {
        let stmt = parse("MATCH (n:Person) SET n.age = 31").unwrap();
        assert!(!stmt.is_read_only());
    }

    #[test]
    fn test_is_read_only_match_with_delete() {
        let stmt = parse("MATCH (n:Person) DELETE n").unwrap();
        assert!(!stmt.is_read_only());
    }

    #[test]
    fn test_is_read_only_merge() {
        let stmt = parse("MERGE (n:Person {name: 'Alice'})").unwrap();
        assert!(!stmt.is_read_only());
    }
}
