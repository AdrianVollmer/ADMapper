//! Query executor - runs parsed statements against the storage backend.

use crate::error::{Error, Result};
use crate::storage::SqliteStorage;
use super::{QueryResult, QueryStats};
use super::parser::{
    Statement, CreateClause, Pattern, PatternElement, NodePattern,
    RelationshipPattern, Expression, Literal, Direction,
};
use std::collections::HashMap;
use std::time::Instant;

/// Execute a parsed statement against the storage.
pub fn execute(statement: &Statement, storage: &SqliteStorage) -> Result<QueryResult> {
    let start = Instant::now();

    let mut stats = QueryStats::default();

    match statement {
        Statement::Create(create) => {
            execute_create(create, storage, &mut stats)?;
        }
        Statement::Match(_) => {
            return Err(Error::Cypher("MATCH execution not yet implemented".into()));
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
    }

    stats.execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        stats,
    })
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
                // Create the node
                let node_id = create_node(node_pattern, storage, stats)?;

                // Bind variable if present
                if let Some(ref var) = node_pattern.variable {
                    bindings.insert(var.clone(), node_id);
                }

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
                let source_id = get_last_node_id(&elements[..i], &bindings)?;

                // Create the target node
                let target_node = match &elements[i + 1] {
                    PatternElement::Node(np) => np,
                    _ => return Err(Error::Cypher("Relationship must be followed by a node".into())),
                };

                let target_id = create_node(target_node, storage, stats)?;

                // Bind target variable if present
                if let Some(ref var) = target_node.variable {
                    bindings.insert(var.clone(), target_id);
                }

                // Create the relationship
                create_relationship(rel_pattern, source_id, target_id, storage, stats)?;

                // Skip the relationship and the target node (we already processed it)
                i += 2;
            }
        }
    }

    Ok(())
}

/// Get the ID of the last node in a pattern slice.
fn get_last_node_id(elements: &[PatternElement], bindings: &HashMap<String, i64>) -> Result<i64> {
    // Find the last node in the slice
    for elem in elements.iter().rev() {
        if let PatternElement::Node(np) = elem {
            if let Some(ref var) = np.variable {
                if let Some(&id) = bindings.get(var) {
                    return Ok(id);
                }
            }
            // Anonymous node - this shouldn't happen in well-formed patterns
            // but we need to handle it
            return Err(Error::Cypher("Cannot reference anonymous node in relationship".into()));
        }
    }
    Err(Error::Cypher("No source node for relationship".into()))
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
}
