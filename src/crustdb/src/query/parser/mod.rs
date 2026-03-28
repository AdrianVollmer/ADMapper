//! Cypher query parser.
//!
//! Parses Cypher query strings into an Abstract Syntax Tree (AST).
//!
//! Uses a pest grammar based on the openCypher specification.

use crate::error::{Error, Result};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use pest_derive::Parser;

// Re-export AST types used by submodules
pub use super::ast::{
    CreateClause, Direction, Expression, LengthSpec, MatchClause, RelationshipPattern, SetClause,
    SetItem, ShortestPathMode, Statement, UnaryOperator,
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
                return build_regular_query(pair);
            }
            Rule::SingleQuery => {
                return build_ast(pair.into_inner());
            }
            Rule::SinglePartQuery => {
                return build_single_part_query(pair);
            }
            Rule::MultiPartQuery => {
                return build_multi_part_query(pair);
            }
            Rule::EOI => continue,
            _ => continue,
        }
    }
    Err(Error::Parse("expected at least one query statement".into()))
}

/// Build AST from a RegularQuery, which may contain UNION ALL clauses.
fn build_regular_query(pair: Pair<Rule>) -> Result<Statement> {
    let mut queries: Vec<Statement> = Vec::new();
    let mut all_unions = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SingleQuery => {
                queries.push(build_ast(inner.into_inner())?);
            }
            Rule::Union => {
                // Check if this is UNION ALL (not plain UNION)
                let inner_str = inner.as_str().to_uppercase();
                let is_all = inner_str.contains("ALL");
                if !is_all {
                    all_unions = false;
                }
                // Extract the SingleQuery from the Union rule
                for union_inner in inner.into_inner() {
                    if union_inner.as_rule() == Rule::SingleQuery {
                        queries.push(build_ast(union_inner.into_inner())?);
                    }
                }
            }
            _ => {}
        }
    }

    if queries.len() == 1 {
        // No UNION, just a single query
        Ok(queries.into_iter().next().unwrap())
    } else if all_unions {
        Ok(Statement::UnionAll(queries))
    } else {
        // Plain UNION (with deduplication)
        Ok(Statement::Union(queries))
    }
}

/// Build AST from a MultiPartQuery.
///
/// Grammar: `((ReadingClause*)  (UpdatingClause*) With)+ SinglePartQuery`
/// Each WITH-terminated segment becomes a WithStage. The final SinglePartQuery
/// becomes the terminating query.
fn build_multi_part_query(pair: Pair<Rule>) -> Result<Statement> {
    use super::ast::WithStage;

    let mut stages: Vec<WithStage> = Vec::new();
    let mut current_reading: Option<Pair<Rule>> = None;
    let mut final_query: Option<Statement> = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ReadingClause => {
                // Accumulate reading clauses - for now we only support one MATCH per stage
                for rc_inner in inner.into_inner() {
                    if rc_inner.as_rule() == Rule::Match {
                        current_reading = Some(rc_inner);
                    }
                }
            }
            Rule::UpdatingClause => {
                return Err(Error::Parse(
                    "expected supported clause, updating clauses before with not yet supported"
                        .into(),
                ));
            }
            Rule::With => {
                // Parse WITH clause: WITH ~ ProjectionBody ~ (SP? ~ Where)?
                let mut with_clause = None;
                let mut post_where = None;

                for with_inner in inner.into_inner() {
                    match with_inner.as_rule() {
                        Rule::ProjectionBody => {
                            with_clause = Some(build_projection_body(with_inner)?);
                        }
                        Rule::Where => {
                            post_where = Some(build_where_clause(with_inner)?.predicate);
                        }
                        _ => {}
                    }
                }

                let with_clause = with_clause.ok_or_else(|| {
                    Error::Parse("expected projection body in with clause".into())
                })?;

                // Build the MatchClause for this stage if there was a MATCH
                let match_clause = if let Some(match_pair) = current_reading.take() {
                    let mut pattern = None;
                    let mut where_clause = None;
                    for mc_inner in match_pair.into_inner() {
                        match mc_inner.as_rule() {
                            Rule::Pattern => {
                                pattern = Some(build_pattern(mc_inner)?);
                            }
                            Rule::Where => {
                                where_clause = Some(build_where_clause(mc_inner)?);
                            }
                            _ => {}
                        }
                    }
                    let pattern = pattern
                        .ok_or_else(|| Error::Parse("expected pattern in match clause".into()))?;
                    Some(MatchClause {
                        pattern,
                        where_clause,
                        return_clause: None,
                        delete_clause: None,
                        set_clause: None,
                        create_clause: None,
                    })
                } else {
                    None
                };

                stages.push(WithStage {
                    match_clause,
                    with_clause,
                    post_where,
                });
            }
            Rule::SinglePartQuery => {
                final_query = Some(build_single_part_query(inner)?);
            }
            _ => {}
        }
    }

    let final_query = final_query
        .ok_or_else(|| Error::Parse("expected final part in multi-part query".into()))?;

    Ok(Statement::Pipeline {
        stages,
        final_query: Box::new(final_query),
    })
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
        return Err(Error::Parse(
            "expected match clause before delete or set".into(),
        ));
    }

    // Standalone RETURN (e.g., RETURN 1, RETURN "hello")
    if let Some(ret) = return_clause {
        return Ok(Statement::Return(ret));
    }

    Err(Error::Parse("expected supported query type".into()))
}

/// Check if a Cypher query is syntactically valid without building an AST.
#[cfg(test)]
pub fn validate(query: &str) -> Result<()> {
    CypherParser::parse(Rule::Cypher, query).map_err(|e| Error::Parse(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::ast::{Literal, PatternElement};
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
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        assert_eq!(create.pattern.elements.len(), 1);
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        assert_eq!(node.variable, Some("n".to_string()));
        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
        assert!(node.properties.is_some());
    }

    #[test]
    fn test_parse_create_node_no_properties() {
        let stmt = parse("CREATE (n:Person)").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        assert_eq!(create.pattern.elements.len(), 1);
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        assert_eq!(node.variable, Some("n".to_string()));
        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
        assert!(node.properties.is_none());
    }

    #[test]
    fn test_parse_create_node_multiple_labels() {
        let stmt = parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        assert_eq!(
            node.labels,
            vec![vec!["Person".to_string()], vec!["Actor".to_string()]]
        );
    }

    #[test]
    fn test_parse_create_relationship() {
        let stmt =
            parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        assert_eq!(create.pattern.elements.len(), 3);

        // First node
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        assert_eq!(node.variable, Some("a".to_string()));
        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);

        // Relationship
        let PatternElement::Relationship(rel) = &create.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                create.pattern.elements[1]
            );
        };
        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
        assert_eq!(rel.direction, Direction::Outgoing);

        // Second node
        let PatternElement::Node(node) = &create.pattern.elements[2] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[2]
            );
        };
        assert_eq!(node.variable, Some("b".to_string()));
    }

    #[test]
    fn test_parse_create_relationship_with_properties() {
        let stmt = parse("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &create.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                create.pattern.elements[1]
            );
        };
        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
        assert!(rel.properties.is_some());
    }

    #[test]
    fn test_parse_create_anonymous_node() {
        let stmt = parse("CREATE (:Person {name: 'Alice'})").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        assert_eq!(node.variable, None);
        assert_eq!(node.labels, vec![vec!["Person".to_string()]]);
    }

    #[test]
    fn test_parse_map_properties() {
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30, active: true})").unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        let Expression::Map(entries) = node.properties.as_ref().unwrap() else {
            panic!("expected map expression, got {:?}", node.properties);
        };
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "name");
        assert_eq!(entries[1].0, "age");
        assert_eq!(entries[2].0, "active");
    }

    #[test]
    fn test_parse_literal_values() {
        let stmt =
            parse("CREATE (n {str: 'hello', int: 42, float: 3.14, bool: true, null_val: null})")
                .unwrap();
        let Statement::Create(create) = stmt else {
            panic!("expected CREATE statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &create.pattern.elements[0] else {
            panic!(
                "expected node pattern, got {:?}",
                create.pattern.elements[0]
            );
        };
        let Expression::Map(entries) = node.properties.as_ref().unwrap() else {
            panic!("expected map expression, got {:?}", node.properties);
        };
        assert!(
            matches!(entries[0].1, Expression::Literal(Literal::String(_))),
            "expected String literal, got {:?}",
            entries[0].1
        );
        assert!(
            matches!(entries[1].1, Expression::Literal(Literal::Integer(42))),
            "expected Integer(42), got {:?}",
            entries[1].1
        );
        assert!(
            matches!(entries[2].1, Expression::Literal(Literal::Float(_))),
            "expected Float literal, got {:?}",
            entries[2].1
        );
        assert!(
            matches!(entries[3].1, Expression::Literal(Literal::Boolean(true))),
            "expected Boolean(true), got {:?}",
            entries[3].1
        );
        assert!(
            matches!(entries[4].1, Expression::Literal(Literal::Null)),
            "expected Null, got {:?}",
            entries[4].1
        );
    }

    // MATCH tests
    #[test]
    fn test_parse_simple_match() {
        let stmt = parse("MATCH (n) RETURN n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        assert_eq!(m.pattern.elements.len(), 1);
        assert!(m.return_clause.is_some());
    }

    #[test]
    fn test_parse_match_with_where() {
        let stmt = parse("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        assert!(m.where_clause.is_some());
    }

    #[test]
    fn test_parse_match_relationship() {
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        assert_eq!(m.pattern.elements.len(), 3);
    }

    #[test]
    fn test_parse_variable_length_path() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        let length = rel.length.as_ref().unwrap();
        assert_eq!(length.min, Some(1));
        assert_eq!(length.max, Some(3));
    }

    #[test]
    fn test_parse_return_with_alias() {
        let stmt = parse("MATCH (n:Person) RETURN n.name AS personName").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let ret = m.return_clause.unwrap();
        assert_eq!(ret.items[0].alias, Some("personName".to_string()));
    }

    #[test]
    fn test_parse_order_by_limit() {
        let stmt = parse("MATCH (n:Person) RETURN n ORDER BY n.age DESC LIMIT 10").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let ret = m.return_clause.unwrap();
        assert!(ret.order_by.is_some());
        let order = ret.order_by.unwrap();
        assert!(order[0].descending);
        assert_eq!(ret.limit, Some(10));
    }

    // DELETE tests
    #[test]
    fn test_parse_delete() {
        let stmt = parse("MATCH (n:Person {name: 'Bob'}) DELETE n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!(
                "expected MATCH statement with DELETE clause, got {:?}",
                stmt
            );
        };
        let del = m.delete_clause.expect("expected delete clause");
        assert!(!del.detach);
        assert_eq!(del.expressions.len(), 1);
    }

    #[test]
    fn test_parse_detach_delete() {
        let stmt = parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!(
                "expected MATCH statement with DELETE clause, got {:?}",
                stmt
            );
        };
        let del = m.delete_clause.expect("expected delete clause");
        assert!(del.detach);
    }

    // SET tests
    #[test]
    fn test_parse_set_property() {
        let stmt = parse("MATCH (n:Person) SET n.age = 31").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement with SET clause, got {:?}", stmt);
        };
        let set = m.set_clause.expect("expected set clause");
        assert_eq!(set.items.len(), 1);
        let SetItem::Property {
            variable, property, ..
        } = &set.items[0]
        else {
            panic!("expected property set item, got {:?}", set.items[0]);
        };
        assert_eq!(variable, "n");
        assert_eq!(property, "age");
    }

    #[test]
    fn test_parse_set_label() {
        let stmt = parse("MATCH (n:Person) SET n:Employee").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement with SET clause, got {:?}", stmt);
        };
        let set = m.set_clause.expect("expected set clause");
        assert_eq!(set.items.len(), 1);
        let SetItem::Labels { variable, labels } = &set.items[0] else {
            panic!("expected label set item, got {:?}", set.items[0]);
        };
        assert_eq!(variable, "n");
        assert_eq!(labels, &["Employee"]);
    }

    #[test]
    fn test_parse_label_or() {
        let stmt = parse("MATCH (n:Person|Company) RETURN n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &m.pattern.elements[0] else {
            panic!("expected node pattern, got {:?}", m.pattern.elements[0]);
        };
        // Single label group with two alternatives
        assert_eq!(
            node.labels,
            vec![vec!["Person".to_string(), "Company".to_string()]]
        );
    }

    #[test]
    fn test_parse_label_or_with_and() {
        let stmt = parse("MATCH (n:Person:Actor|Director) RETURN n").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Node(node) = &m.pattern.elements[0] else {
            panic!("expected node pattern, got {:?}", m.pattern.elements[0]);
        };
        // First group: just Person; second group: Actor or Director
        assert_eq!(
            node.labels,
            vec![
                vec!["Person".to_string()],
                vec!["Actor".to_string(), "Director".to_string()]
            ]
        );
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
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let ret = m.return_clause.unwrap();
        let Expression::FunctionCall { name, args } = &ret.items[0].expression else {
            panic!("expected function call, got {:?}", ret.items[0].expression);
        };
        assert_eq!(name, "count");
        assert_eq!(args.len(), 1);
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
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        assert_eq!(m.pattern.elements.len(), 3);
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, Some(1));
        assert_eq!(len.max, Some(3));
    }

    #[test]
    fn test_parse_variable_length_unbounded() {
        let stmt = parse("MATCH (a)-[:KNOWS*]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, None);
        assert_eq!(len.max, None);
    }

    #[test]
    fn test_parse_variable_length_exact() {
        let stmt = parse("MATCH (a)-[:KNOWS*2]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, Some(2));
        assert_eq!(len.max, Some(2));
    }

    #[test]
    fn test_parse_variable_length_min_only() {
        let stmt = parse("MATCH (a)-[:KNOWS*2..]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, Some(2));
        assert_eq!(len.max, None);
    }

    #[test]
    fn test_parse_variable_length_max_only() {
        let stmt = parse("MATCH (a)-[*..3]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, None);
        assert_eq!(len.max, Some(3));
    }

    #[test]
    fn test_parse_shortest_path_function() {
        // openCypher 9 shortestPath() syntax
        let stmt =
            parse("MATCH (a:Person), (b:Person) RETURN shortestPath((a)-[:KNOWS*]->(b))").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let ret = m.return_clause.as_ref().expect("Expected RETURN clause");
        assert_eq!(ret.items.len(), 1);
        let Expression::ShortestPath(pattern) = &ret.items[0].expression else {
            panic!(
                "expected ShortestPath expression, got {:?}",
                ret.items[0].expression
            );
        };
        assert_eq!(pattern.elements.len(), 3); // node-rel-node
    }

    #[test]
    fn test_parse_all_shortest_paths_function() {
        // openCypher 9 allShortestPaths() syntax
        let stmt = parse("MATCH (a), (b) RETURN allShortestPaths((a)-[:KNOWS*1..5]->(b))").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let ret = m.return_clause.as_ref().expect("Expected RETURN clause");
        assert_eq!(ret.items.len(), 1);
        let Expression::AllShortestPaths(pattern) = &ret.items[0].expression else {
            panic!(
                "expected AllShortestPaths expression, got {:?}",
                ret.items[0].expression
            );
        };
        assert_eq!(pattern.elements.len(), 3);
    }

    #[test]
    fn test_parse_variable_length_one_or_more() {
        // Variable length with range (openCypher 9 syntax using *1..)
        let stmt = parse("MATCH (a)-[:LINK*1..]->(b) RETURN a, b").unwrap();
        let Statement::Match(m) = stmt else {
            panic!("expected MATCH statement, got {:?}", stmt);
        };
        let PatternElement::Relationship(rel) = &m.pattern.elements[1] else {
            panic!(
                "expected relationship pattern, got {:?}",
                m.pattern.elements[1]
            );
        };
        assert!(rel.length.is_some());
        let len = rel.length.as_ref().unwrap();
        assert_eq!(len.min, Some(1));
        assert_eq!(len.max, None);
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
        let Statement::Match(m) = &stmt else {
            panic!("expected Statement::Match, got {:?}", stmt);
        };
        assert!(m.create_clause.is_some());
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
