//! Query executor - runs parsed statements against the storage backend.

use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue};
use crate::storage::SqliteStorage;
use super::{QueryResult, QueryStats, Row, ResultValue};
use super::parser::{
    Statement, CreateClause, MatchClause, Pattern, PatternElement, NodePattern,
    RelationshipPattern, Expression, Literal, Direction, ReturnClause,
};
use std::collections::HashMap;
use std::time::Instant;

/// Execute a parsed statement against the storage.
pub fn execute(statement: &Statement, storage: &SqliteStorage) -> Result<QueryResult> {
    let start = Instant::now();

    let mut stats = QueryStats::default();

    let result = match statement {
        Statement::Create(create) => {
            execute_create(create, storage, &mut stats)?;
            QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                stats,
            }
        }
        Statement::Match(match_clause) => {
            execute_match(match_clause, storage, &mut stats)?
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
    };

    let mut result = result;
    result.stats.execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(result)
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
                // Check if this variable is already bound
                let node_id = if let Some(ref var) = node_pattern.variable {
                    if let Some(&existing_id) = bindings.get(var) {
                        // Variable already bound - use existing node
                        existing_id
                    } else {
                        // Create new node and bind it
                        let id = create_node(node_pattern, storage, stats)?;
                        bindings.insert(var.clone(), id);
                        id
                    }
                } else {
                    // Anonymous node - always create
                    create_node(node_pattern, storage, stats)?
                };

                // Store for relationship lookup (even if we didn't create it)
                let _ = node_id;

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
                let source_id = get_node_id(&elements[i - 1], &bindings)?;

                // Get or create the target node
                let target_node = match &elements[i + 1] {
                    PatternElement::Node(np) => np,
                    _ => return Err(Error::Cypher("Relationship must be followed by a node".into())),
                };

                let target_id = if let Some(ref var) = target_node.variable {
                    if let Some(&existing_id) = bindings.get(var) {
                        // Variable already bound - use existing node
                        existing_id
                    } else {
                        // Create new node and bind it
                        let id = create_node(target_node, storage, stats)?;
                        bindings.insert(var.clone(), id);
                        id
                    }
                } else {
                    // Anonymous node - always create
                    create_node(target_node, storage, stats)?
                };

                // Create the relationship
                create_relationship(rel_pattern, source_id, target_id, storage, stats)?;

                // Skip the relationship and the target node (we already processed it)
                i += 2;
            }
        }
    }

    Ok(())
}

// =============================================================================
// MATCH Execution
// =============================================================================

/// Execute a MATCH statement.
fn execute_match(
    match_clause: &MatchClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<QueryResult> {
    // For now, we only support simple single-node patterns: MATCH (n) or MATCH (n:Label)
    // Get the first node pattern from the MATCH
    let node_pattern = get_single_node_pattern(&match_clause.pattern)?;

    // Scan nodes based on the pattern
    let nodes = scan_nodes(node_pattern, storage)?;

    // Filter by properties if specified in the pattern
    let nodes = filter_by_properties(nodes, node_pattern)?;

    // Build result based on RETURN clause
    let return_clause = match_clause.return_clause.as_ref()
        .ok_or_else(|| Error::Cypher("MATCH requires RETURN clause".into()))?;

    let variable = node_pattern.variable.as_deref().unwrap_or("_");

    build_match_result(nodes, variable, return_clause, stats)
}

/// Extract a single node pattern from a MATCH pattern.
/// For M3, we only support simple single-node patterns.
fn get_single_node_pattern(pattern: &Pattern) -> Result<&NodePattern> {
    if pattern.elements.len() != 1 {
        return Err(Error::Cypher(
            "Only single-node MATCH patterns are supported (M3)".into()
        ));
    }

    match &pattern.elements[0] {
        PatternElement::Node(np) => Ok(np),
        PatternElement::Relationship(_) => {
            Err(Error::Cypher("MATCH pattern must start with a node".into()))
        }
    }
}

/// Scan nodes from storage based on the node pattern.
/// This implements the Node Scan and Label Filter operators.
fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - scan all nodes
        storage.scan_all_nodes()
    } else {
        // Filter by first label (we can AND multiple labels later)
        let label = &pattern.labels[0];
        let mut nodes = storage.find_nodes_by_label(label)?;

        // If multiple labels, filter to only nodes that have ALL labels
        if pattern.labels.len() > 1 {
            nodes.retain(|node| {
                pattern.labels.iter().all(|l| node.has_label(l))
            });
        }

        Ok(nodes)
    }
}

/// Filter nodes by properties specified in the pattern.
/// This implements the Property Filter operator.
fn filter_by_properties(nodes: Vec<Node>, pattern: &NodePattern) -> Result<Vec<Node>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(nodes);
    };

    // Properties should be a Map expression
    let required_props = match props_expr {
        Expression::Map(entries) => entries,
        _ => return Err(Error::Cypher("Pattern properties must be a map".into())),
    };

    if required_props.is_empty() {
        return Ok(nodes);
    }

    let filtered: Vec<Node> = nodes
        .into_iter()
        .filter(|node| {
            required_props.iter().all(|(key, value)| {
                match node.get(key) {
                    Some(node_value) => property_matches(node_value, value),
                    None => {
                        // Check if required value is null
                        matches!(value, Expression::Literal(Literal::Null))
                    }
                }
            })
        })
        .collect();

    Ok(filtered)
}

/// Check if a node's property value matches an expression.
fn property_matches(node_value: &PropertyValue, expr: &Expression) -> bool {
    match expr {
        Expression::Literal(lit) => literal_matches_property(lit, node_value),
        _ => false, // Complex expressions not supported in property matching
    }
}

/// Check if a literal matches a property value.
fn literal_matches_property(lit: &Literal, prop: &PropertyValue) -> bool {
    match (lit, prop) {
        (Literal::Null, _) => false, // NULL never equals anything
        (Literal::Boolean(a), PropertyValue::Bool(b)) => a == b,
        (Literal::Integer(a), PropertyValue::Integer(b)) => a == b,
        (Literal::Float(a), PropertyValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Literal::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type comparisons
        (Literal::Integer(a), PropertyValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (Literal::Float(a), PropertyValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        _ => false,
    }
}

/// Build the query result based on the RETURN clause.
/// This implements the Projection operator.
fn build_match_result(
    nodes: Vec<Node>,
    variable: &str,
    return_clause: &ReturnClause,
    _stats: &mut QueryStats,
) -> Result<QueryResult> {
    // Build column names from return items
    let columns: Vec<String> = return_clause.items.iter().map(|item| {
        if let Some(ref alias) = item.alias {
            alias.clone()
        } else {
            expr_to_column_name(&item.expression, variable)
        }
    }).collect();

    // Build rows
    let mut rows = Vec::with_capacity(nodes.len());

    for node in nodes {
        let mut values = HashMap::new();

        for (i, item) in return_clause.items.iter().enumerate() {
            let column_name = &columns[i];
            let value = evaluate_return_item(&item.expression, variable, &node)?;
            values.insert(column_name.clone(), value);
        }

        rows.push(Row { values });
    }

    Ok(QueryResult {
        columns,
        rows,
        stats: QueryStats::default(),
    })
}

/// Convert an expression to a column name.
fn expr_to_column_name(expr: &Expression, default_var: &str) -> String {
    match expr {
        Expression::Variable(name) => name.clone(),
        Expression::Property { base, property } => {
            let base_name = expr_to_column_name(base, default_var);
            format!("{}.{}", base_name, property)
        }
        Expression::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                let arg_str = args.iter()
                    .map(|a| expr_to_column_name(a, default_var))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, arg_str)
            }
        }
        _ => default_var.to_string(),
    }
}

/// Evaluate a return item expression for a given node.
fn evaluate_return_item(
    expr: &Expression,
    variable: &str,
    node: &Node,
) -> Result<ResultValue> {
    match expr {
        Expression::Variable(name) => {
            if name == variable || name == "*" {
                // Return the whole node
                Ok(ResultValue::Node {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                })
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }
        Expression::Property { base, property } => {
            // Check if base is our variable
            if let Expression::Variable(base_name) = base.as_ref() {
                if base_name == variable {
                    // Return the property value
                    let value = node.get(property).cloned()
                        .unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
            }
            Err(Error::Cypher(format!("Cannot access property on non-variable")))
        }
        Expression::Literal(lit) => {
            // Return the literal value
            let prop_value = literal_to_property(lit)?;
            Ok(ResultValue::Property(prop_value))
        }
        _ => Err(Error::Cypher("Complex expressions in RETURN not yet supported".into())),
    }
}

/// Convert a literal to a PropertyValue.
fn literal_to_property(lit: &Literal) -> Result<PropertyValue> {
    Ok(match lit {
        Literal::Null => PropertyValue::Null,
        Literal::Boolean(b) => PropertyValue::Bool(*b),
        Literal::Integer(n) => PropertyValue::Integer(*n),
        Literal::Float(f) => PropertyValue::Float(*f),
        Literal::String(s) => PropertyValue::String(s.clone()),
    })
}

// =============================================================================
// CREATE Helpers
// =============================================================================

/// Get the ID of a node from a pattern element.
fn get_node_id(element: &PatternElement, bindings: &HashMap<String, i64>) -> Result<i64> {
    match element {
        PatternElement::Node(np) => {
            if let Some(ref var) = np.variable {
                if let Some(&id) = bindings.get(var) {
                    return Ok(id);
                }
            }
            // Anonymous node or unbound variable
            Err(Error::Cypher("Cannot reference unbound node in relationship".into()))
        }
        PatternElement::Relationship(_) => {
            Err(Error::Cypher("Expected node, found relationship".into()))
        }
    }
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

    // =========================================================================
    // MATCH Tests
    // =========================================================================

    #[test]
    fn test_match_all_nodes() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes first
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(), &storage).unwrap();

        // Match all nodes
        let result = execute(&parse("MATCH (n) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.columns, vec!["n"]);
    }

    #[test]
    fn test_match_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(), &storage).unwrap();

        // Match only Person nodes
        let result = execute(&parse("MATCH (n:Person) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_match_by_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with varying labels
        execute(&parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person:Director {name: 'Oliver'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Match only Person+Actor nodes
        let result = execute(&parse("MATCH (n:Person:Actor) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_by_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        // Match by name property
        let result = execute(&parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_return_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();

        // Match and return specific property
        let result = execute(&parse("MATCH (n:Person) RETURN n.name").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns, vec!["n.name"]);

        // Check the value
        let row = &result.rows[0];
        let name = row.values.get("n.name").unwrap();
        assert!(matches!(name, ResultValue::Property(PropertyValue::String(s)) if s == "Alice"));
    }

    #[test]
    fn test_match_return_multiple_properties() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(&parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &storage).unwrap();

        // Match and return multiple properties
        let result = execute(&parse("MATCH (n:Person) RETURN n.name, n.age").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.contains(&"n.name".to_string()));
        assert!(result.columns.contains(&"n.age".to_string()));
    }

    #[test]
    fn test_match_empty_result() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with one label
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Match with non-existent label
        let result = execute(&parse("MATCH (n:Movie) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_property_not_found() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with different properties
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        // Match by property that doesn't match any node
        let result = execute(&parse("MATCH (n:Person {name: 'Charlie'}) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_return_missing_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node without 'age' property
        execute(&parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &storage).unwrap();

        // Return a property that doesn't exist
        let result = execute(&parse("MATCH (n:Person) RETURN n.age").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);

        // Should return null for missing property
        let row = &result.rows[0];
        let age = row.values.get("n.age").unwrap();
        assert!(matches!(age, ResultValue::Property(PropertyValue::Null)));
    }
}
