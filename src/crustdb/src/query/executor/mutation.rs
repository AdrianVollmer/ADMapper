//! Mutation execution: SET and DELETE operations.

use crate::error::{Error, Result};
use crate::query::parser::{DeleteClause, Expression, SetClause, SetItem};
use crate::query::QueryStats;
use crate::storage::SqliteStorage;

use super::eval::evaluate_expression_with_bindings;
use super::Binding;

/// Execute a SET clause.
pub fn execute_set(
    bindings: &[Binding],
    set_clause: &SetClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for item in &set_clause.items {
            match item {
                SetItem::Property {
                    variable,
                    property,
                    value,
                } => {
                    // Evaluate the value expression
                    let prop_value = evaluate_expression_with_bindings(value, binding)?;

                    // Find the node to update
                    if let Some(node) = binding.get_node(variable) {
                        storage.update_node_property(node.id, property, &prop_value)?;
                        stats.properties_set += 1;
                    } else {
                        return Err(Error::Cypher(format!(
                            "Variable '{}' not found in binding",
                            variable
                        )));
                    }
                }
                SetItem::Labels { variable, labels } => {
                    // Find the node to add labels to
                    if let Some(node) = binding.get_node(variable) {
                        for label in labels {
                            storage.add_node_label(node.id, label)?;
                            stats.labels_added += 1;
                        }
                    } else {
                        return Err(Error::Cypher(format!(
                            "Variable '{}' not found in binding",
                            variable
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Execute a DELETE clause.
pub fn execute_delete(
    bindings: &[Binding],
    delete_clause: &DeleteClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for expr in &delete_clause.expressions {
            match expr {
                Expression::Variable(name) => {
                    // Check if it's a node
                    if let Some(node) = binding.get_node(name) {
                        if delete_clause.detach {
                            // DETACH DELETE - delete node and all its relationships
                            storage.delete_node(node.id)?;
                            stats.nodes_deleted += 1;
                        } else {
                            // Regular DELETE - fail if node has relationships
                            if storage.has_edges(node.id)? {
                                return Err(Error::Cypher(format!(
                                    "Cannot delete node {} because it still has relationships. Use DETACH DELETE to delete it along with its relationships.",
                                    node.id
                                )));
                            }
                            storage.delete_node(node.id)?;
                            stats.nodes_deleted += 1;
                        }
                    } else if let Some(relationship) = binding.get_edge(name) {
                        // Delete relationship
                        storage.delete_edge(relationship.id)?;
                        stats.relationships_deleted += 1;
                    } else {
                        return Err(Error::Cypher(format!(
                            "Variable '{}' not found in binding",
                            name
                        )));
                    }
                }
                _ => {
                    return Err(Error::Cypher(
                        "DELETE expressions must be simple variables".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}
