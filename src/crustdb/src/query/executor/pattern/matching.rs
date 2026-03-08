//! Pattern matching helper functions for nodes and relationships.

use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue, Relationship};
use crate::query::parser::{Expression, Literal, NodePattern, RelationshipPattern};
use crate::storage::SqliteStorage;
use std::collections::HashSet;

use super::super::create::literal_to_json;
use super::super::eval::floats_equal;

/// Check if a node matches a node pattern (labels and properties).
pub fn node_matches_pattern(node: &Node, pattern: &NodePattern) -> bool {
    // Check labels: each label group is AND'd, alternatives within a group are OR'd
    for label_group in &pattern.labels {
        // Node must have at least one label from this group
        let has_any = label_group.iter().any(|label| node.has_label(label));
        if !has_any {
            return false;
        }
    }

    // Check properties
    if let Some(Expression::Map(entries)) = &pattern.properties {
        for (key, value) in entries {
            match node.get(key) {
                Some(node_value) => {
                    if !property_matches(node_value, value) {
                        return false;
                    }
                }
                None => {
                    if !matches!(value, Expression::Literal(Literal::Null)) {
                        return false;
                    }
                }
            }
        }
    }

    true
}

/// Filter relationships by properties specified in the relationship pattern.
pub fn filter_relationships_by_properties(
    relationships: Vec<Relationship>,
    pattern: &RelationshipPattern,
) -> Result<Vec<Relationship>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(relationships);
    };

    let required_props = match props_expr {
        Expression::Map(entries) => entries,
        _ => {
            return Err(Error::Cypher(
                "Relationship properties must be a map".into(),
            ))
        }
    };

    if required_props.is_empty() {
        return Ok(relationships);
    }

    let filtered: Vec<Relationship> = relationships
        .into_iter()
        .filter(|relationship| {
            required_props
                .iter()
                .all(|(key, value)| match relationship.properties.get(key) {
                    Some(rel_value) => property_matches(rel_value, value),
                    None => matches!(value, Expression::Literal(Literal::Null)),
                })
        })
        .collect();

    Ok(filtered)
}

/// Scan nodes from storage based on the node pattern.
/// This implements the Node Scan and Label Filter operators.
///
/// When the pattern has no labels but has properties with an available index,
/// uses index lookup instead of full scan for O(1) vs O(N) performance.
pub fn scan_nodes(pattern: &NodePattern, storage: &SqliteStorage) -> Result<Vec<Node>> {
    if pattern.labels.is_empty() {
        // No label filter - try property index lookup first
        if let Some(Expression::Map(props)) = &pattern.properties {
            // Try to find an indexed property we can use
            for (key, value) in props {
                if let Ok(true) = storage.has_property_index(key) {
                    if let Expression::Literal(lit) = value {
                        let json_value = literal_to_json(lit)?;
                        // Use property index lookup
                        // Other properties will be filtered by filter_by_properties() later
                        return storage.find_nodes_by_property(key, &json_value, &[], None);
                    }
                }
            }
        }
        // No indexed property found - fall back to full scan
        storage.scan_all_nodes()
    } else {
        // For efficiency, use first label from first group for initial scan
        // Then filter using full label expression
        let first_group = &pattern.labels[0];
        if first_group.is_empty() {
            return storage.scan_all_nodes();
        }

        // If first group has alternatives (OR), we need to scan for each and merge
        let mut all_nodes = Vec::new();
        let mut seen_ids = HashSet::new();

        for label in first_group {
            let nodes = storage.find_nodes_by_label(label)?;
            for node in nodes {
                if seen_ids.insert(node.id) {
                    all_nodes.push(node);
                }
            }
        }

        // Filter to match full label expression (handles AND of multiple groups)
        if pattern.labels.len() > 1 {
            all_nodes.retain(|node| {
                pattern
                    .labels
                    .iter()
                    .all(|group| group.iter().any(|label| node.has_label(label)))
            });
        }

        Ok(all_nodes)
    }
}

/// Scan nodes with optional SQL-level LIMIT pushdown.
/// Only applies limit when we have a simple label pattern (no OR, no AND of multiple groups),
/// or when using an indexed property lookup.
pub fn scan_nodes_with_limit(
    pattern: &NodePattern,
    storage: &SqliteStorage,
    limit: Option<u64>,
) -> Result<Vec<Node>> {
    // Try property index lookup with limit if no labels but has indexed property
    if pattern.labels.is_empty() {
        if let Some(Expression::Map(props)) = &pattern.properties {
            for (key, value) in props {
                if let Ok(true) = storage.has_property_index(key) {
                    if let Expression::Literal(lit) = value {
                        let json_value = literal_to_json(lit)?;
                        // Use property index lookup with limit
                        return storage.find_nodes_by_property(key, &json_value, &[], limit);
                    }
                }
            }
        }
    }

    // Can only push limit for simple patterns without properties
    let can_push_limit = limit.is_some()
        && pattern.properties.is_none()
        && (pattern.labels.is_empty()
            || (pattern.labels.len() == 1 && pattern.labels[0].len() == 1));

    if can_push_limit {
        let limit = limit.unwrap();
        if pattern.labels.is_empty() {
            storage.get_all_nodes_limit(Some(limit))
        } else {
            let label = &pattern.labels[0][0];
            storage.find_nodes_by_label_limit(label, Some(limit))
        }
    } else {
        // Fall back to regular scan (no limit pushdown)
        scan_nodes(pattern, storage)
    }
}

/// Filter nodes by properties specified in the pattern.
/// This implements the Property Filter operator.
pub fn filter_by_properties(nodes: Vec<Node>, pattern: &NodePattern) -> Result<Vec<Node>> {
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
        (Literal::Float(a), PropertyValue::Float(b)) => floats_equal(*a, *b),
        (Literal::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type comparisons
        (Literal::Integer(a), PropertyValue::Float(b)) => floats_equal(*a as f64, *b),
        (Literal::Float(a), PropertyValue::Integer(b)) => floats_equal(*a, *b as f64),
        _ => false,
    }
}
