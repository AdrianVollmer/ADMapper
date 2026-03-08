//! CREATE clause planning.

use super::{
    plan_properties, CreateNode, CreateRelationship, Error, PatternElement, PlanOperator, Result,
};

/// Plan a CREATE statement.
pub(super) fn plan_create(create: &super::super::parser::CreateClause) -> Result<PlanOperator> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut auto_var_counter = 0;
    let mut declared_vars: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Process pattern, tracking the previous node variable for relationship binding
    let mut prev_node_var: Option<String> = None;
    let mut pending_rel: Option<(super::super::parser::RelationshipPattern, String)> = None; // (rel_pattern, source_var)

    for element in &create.pattern.elements {
        match element {
            PatternElement::Node(np) => {
                // Assign a variable if not present
                let var = np.variable.clone().unwrap_or_else(|| {
                    auto_var_counter += 1;
                    format!("_auto_n{}", auto_var_counter)
                });

                // If there's a pending relationship, complete it now
                if let Some((rp, source_var)) = pending_rel.take() {
                    let rel_type = rp.types.first().cloned().unwrap_or_default();
                    let properties = plan_properties(&rp.properties)?;
                    let (source, target) = match rp.direction {
                        super::super::parser::Direction::Outgoing => (source_var, var.clone()),
                        super::super::parser::Direction::Incoming => (var.clone(), source_var),
                        super::super::parser::Direction::Both => (source_var, var.clone()), // Default to outgoing
                    };
                    relationships.push(CreateRelationship {
                        variable: rp.variable.clone(),
                        source,
                        target,
                        rel_type,
                        properties,
                    });
                }

                let labels: Vec<String> = np.labels.iter().flatten().cloned().collect();
                let properties = plan_properties(&np.properties)?;

                // Only create a node if it has labels/properties (declaration) or hasn't been declared yet
                // A node pattern like `(charlie)` appearing after `(charlie:Person {...})` is a reference
                let is_reference =
                    labels.is_empty() && properties.is_empty() && declared_vars.contains(&var);

                if !is_reference {
                    nodes.push(CreateNode {
                        variable: Some(var.clone()),
                        labels,
                        properties,
                    });
                    declared_vars.insert(var.clone());
                }

                prev_node_var = Some(var);
            }
            PatternElement::Relationship(rp) => {
                // Store the relationship to be completed when we see the next node
                let source_var = prev_node_var
                    .clone()
                    .ok_or_else(|| Error::Cypher("Relationship must follow a node".into()))?;
                pending_rel = Some((rp.clone(), source_var));
            }
        }
    }

    Ok(PlanOperator::Create {
        source: None,
        nodes,
        relationships,
    })
}

/// Plan a CREATE clause that follows a MATCH (MATCH...CREATE).
///
/// Unlike standalone CREATE, nodes here are references to already-matched
/// variables, not new nodes to create. Only new nodes (with labels/properties
/// not already matched) are created; the rest come from MATCH bindings.
pub(super) fn plan_create_after_match(
    create: &super::super::parser::CreateClause,
) -> Result<(Vec<CreateNode>, Vec<CreateRelationship>)> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut prev_node_var: Option<String> = None;
    let mut pending_rel: Option<(super::super::parser::RelationshipPattern, String)> = None;

    for element in &create.pattern.elements {
        match element {
            PatternElement::Node(np) => {
                let var = np.variable.clone().ok_or_else(|| {
                    Error::Cypher("MATCH...CREATE nodes must have variables".into())
                })?;

                if let Some((rp, source_var)) = pending_rel.take() {
                    let rel_type = rp.types.first().cloned().unwrap_or_default();
                    let properties = plan_properties(&rp.properties)?;
                    let (source, target) = match rp.direction {
                        super::super::parser::Direction::Outgoing => (source_var, var.clone()),
                        super::super::parser::Direction::Incoming => (var.clone(), source_var),
                        super::super::parser::Direction::Both => (source_var, var.clone()),
                    };
                    relationships.push(CreateRelationship {
                        variable: rp.variable.clone(),
                        source,
                        target,
                        rel_type,
                        properties,
                    });
                }

                // Only create a new node if it has labels or properties
                // (i.e. it's a genuinely new node, not a reference to a MATCH binding)
                let labels: Vec<String> = np.labels.iter().flatten().cloned().collect();
                let properties = plan_properties(&np.properties)?;
                if !labels.is_empty() || !properties.is_empty() {
                    nodes.push(CreateNode {
                        variable: Some(var.clone()),
                        labels,
                        properties,
                    });
                }

                prev_node_var = Some(var);
            }
            PatternElement::Relationship(rp) => {
                let source_var = prev_node_var
                    .clone()
                    .ok_or_else(|| Error::Cypher("Relationship must follow a node".into()))?;
                pending_rel = Some((rp.clone(), source_var));
            }
        }
    }

    Ok((nodes, relationships))
}
