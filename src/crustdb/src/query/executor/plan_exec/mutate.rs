use super::{
    evaluate_expr, execute_operator, plan_properties_to_json, Binding, ExecutionContext,
    ExecutionResult,
};
use crate::error::{Error, Result};
use crate::query::planner::{CreateNode, CreateRelationship, PlanOperator, SetOperation};
use crate::query::QueryStats;
use crate::storage::{EntityCache, SqliteStorage};
use std::collections::HashMap;

pub(super) fn execute_create(
    source: Option<&PlanOperator>,
    nodes: &[CreateNode],
    relationships: &[CreateRelationship],
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
    ctx: &mut ExecutionContext,
) -> Result<ExecutionResult> {
    // If there's a source operator (MATCH...CREATE), execute it first
    // to get bindings for matched variables.
    let source_bindings = if let Some(source_op) = source {
        let result = execute_operator(source_op, storage, ctx, cache)?;
        match result {
            ExecutionResult::Bindings(b) => b,
            ExecutionResult::Rows { .. } => {
                return Err(Error::Cypher(
                    "MATCH...CREATE source must produce bindings".into(),
                ))
            }
        }
    } else {
        Vec::new()
    };

    if source_bindings.is_empty() && source.is_some() {
        // MATCH found no results, nothing to create
        return Ok(ExecutionResult::Bindings(Vec::new()));
    }

    // For MATCH...CREATE, execute the CREATE for each binding row
    // For standalone CREATE, execute once with no bindings
    let binding_sets: Vec<Option<&Binding>> = if source_bindings.is_empty() {
        vec![None]
    } else {
        source_bindings.iter().map(Some).collect()
    };

    for binding in &binding_sets {
        let mut var_to_id: HashMap<String, i64> = HashMap::new();

        // Populate var_to_id from MATCH bindings
        if let Some(b) = binding {
            for (var, node) in b.nodes() {
                var_to_id.insert(var.clone(), node.id);
            }
        }

        // Create new nodes (only nodes not already bound from MATCH)
        for create_node in nodes {
            if let Some(ref var) = create_node.variable {
                if var_to_id.contains_key(var) {
                    continue; // Already bound from MATCH, skip creation
                }
            }

            let props = plan_properties_to_json(&create_node.properties)?;
            let node_id = storage.insert_node(&create_node.labels, &props)?;

            ctx.stats.nodes_created += 1;
            ctx.stats.labels_added += create_node.labels.len();
            ctx.stats.properties_set += create_node.properties.len();

            if let Some(ref var) = create_node.variable {
                var_to_id.insert(var.clone(), node_id);
            }
        }

        // Create relationships using variable name lookup
        for create_rel in relationships {
            let source_id = var_to_id.get(&create_rel.source).ok_or_else(|| {
                Error::Cypher(format!("Unknown source variable: {}", create_rel.source))
            })?;
            let target_id = var_to_id.get(&create_rel.target).ok_or_else(|| {
                Error::Cypher(format!("Unknown target variable: {}", create_rel.target))
            })?;
            let props = plan_properties_to_json(&create_rel.properties)?;

            storage.insert_relationship(*source_id, *target_id, &create_rel.rel_type, &props)?;
            ctx.stats.relationships_created += 1;
            ctx.stats.properties_set += create_rel.properties.len();
        }
    }

    Ok(ExecutionResult::Bindings(Vec::new()))
}

pub(super) fn execute_set_properties(
    bindings: &[Binding],
    sets: &[SetOperation],
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for set_op in sets {
            match set_op {
                SetOperation::Property {
                    variable,
                    property,
                    value,
                } => {
                    if let Some(node) = binding.get_node(variable) {
                        let prop_val = evaluate_expr(value, binding)?;
                        storage.update_node_property(node.id, property, &prop_val)?;
                        stats.properties_set += 1;
                    } else if let Some(rel) = binding.get_relationship(variable) {
                        let prop_val = evaluate_expr(value, binding)?;
                        storage.update_relationship_property(rel.id, property, &prop_val)?;
                        stats.properties_set += 1;
                    }
                }
                SetOperation::AddLabel { variable, label } => {
                    if let Some(node) = binding.get_node(variable) {
                        storage.add_node_label(node.id, label)?;
                        stats.labels_added += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn execute_delete(
    bindings: &[Binding],
    variables: &[String],
    detach: bool,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<()> {
    for binding in bindings {
        for var in variables {
            if let Some(node) = binding.get_node(var) {
                // Check for relationships if not DETACH DELETE
                if !detach && storage.has_relationships(node.id)? {
                    return Err(Error::Cypher(
                        "Cannot delete node with relationships. Use DETACH DELETE.".into(),
                    ));
                }
                storage.delete_node(node.id)?;
                stats.nodes_deleted += 1;
            } else if let Some(relationship) = binding.get_relationship(var) {
                storage.delete_relationship(relationship.id)?;
                stats.relationships_deleted += 1;
            }
        }
    }
    Ok(())
}
