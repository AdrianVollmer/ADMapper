//! CREATE statement execution.

use crate::error::{Error, Result};
use crate::query::parser::{
    CreateClause, Direction, Expression, Literal, NodePattern, PatternElement, RelationshipPattern,
};
use crate::query::QueryStats;
use crate::storage::SqliteStorage;
use std::collections::HashMap;

/// Execute a CREATE statement.
pub fn execute_create(
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
                if let Some(ref var) = node_pattern.variable {
                    if !bindings.contains_key(var) {
                        // Create new node and bind it
                        let id = create_node(node_pattern, storage, stats)?;
                        bindings.insert(var.clone(), id);
                    }
                    // If already bound, nothing to do - node exists
                } else {
                    // Anonymous node - always create
                    create_node(node_pattern, storage, stats)?;
                }

                i += 1;
            }
            PatternElement::Relationship(rel_pattern) => {
                // A relationship must be between two nodes
                // The previous element should have been a node (source)
                // The next element should be a node (target)

                if i == 0 {
                    return Err(Error::Cypher(
                        "Relationship pattern must follow a node".into(),
                    ));
                }
                if i + 1 >= elements.len() {
                    return Err(Error::Cypher(
                        "Relationship pattern must be followed by a node".into(),
                    ));
                }

                // Get source node ID from previous node
                let source_id = get_node_id(&elements[i - 1], &bindings)?;

                // Get or create the target node
                let target_node = match &elements[i + 1] {
                    PatternElement::Node(np) => np,
                    _ => {
                        return Err(Error::Cypher(
                            "Relationship must be followed by a node".into(),
                        ))
                    }
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

/// Get the ID of a node from a pattern element.
fn get_node_id(element: &PatternElement, bindings: &HashMap<String, i64>) -> Result<i64> {
    match element {
        PatternElement::Node(np) => {
            if let Some(ref var) = np.variable {
                if let Some(&id) = bindings.get(var) {
                    return Ok(id);
                }
            }
            Err(Error::Cypher(
                "Cannot reference unbound node in relationship".into(),
            ))
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
    // Flatten label groups for CREATE (all labels are added to the node)
    let labels: Vec<String> = pattern.labels.iter().flatten().cloned().collect();

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
    let rel_type = pattern
        .types
        .first()
        .ok_or_else(|| Error::Cypher("Relationship must have a type".into()))?;

    let properties = match &pattern.properties {
        Some(expr) => expression_to_json(expr)?,
        None => serde_json::json!({}),
    };

    let (actual_source, actual_target) = match pattern.direction {
        Direction::Outgoing => (source_id, target_id),
        Direction::Incoming => (target_id, source_id),
        Direction::Both => (source_id, target_id),
    };

    let rel_id =
        storage.insert_relationship(actual_source, actual_target, rel_type, &properties)?;

    stats.relationships_created += 1;

    if let serde_json::Value::Object(map) = &properties {
        stats.properties_set += map.len();
    }

    Ok(rel_id)
}

/// Convert an AST Expression to a JSON value.
pub fn expression_to_json(expr: &Expression) -> Result<serde_json::Value> {
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
        Expression::Variable(name) => Err(Error::Cypher(format!(
            "Cannot use variable '{}' in CREATE properties",
            name
        ))),
        Expression::Parameter(name) => Err(Error::Cypher(format!(
            "Parameters not yet supported: ${}",
            name
        ))),
        _ => Err(Error::Cypher(
            "Complex expressions not supported in CREATE properties".into(),
        )),
    }
}

/// Convert a literal to a JSON value.
pub fn literal_to_json(lit: &Literal) -> Result<serde_json::Value> {
    Ok(match lit {
        Literal::Null => serde_json::Value::Null,
        Literal::Boolean(b) => serde_json::Value::Bool(*b),
        Literal::Integer(n) => serde_json::Value::Number((*n).into()),
        Literal::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| Error::Cypher(format!("Invalid float value: {}", f)))?,
        Literal::String(s) => serde_json::Value::String(s.clone()),
    })
}
