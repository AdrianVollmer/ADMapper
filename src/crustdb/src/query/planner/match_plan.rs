//! MATCH clause planning - pattern matching, scans, and expansions.

use super::{
    extract_simple_property_filter, plan_create_after_match, plan_expression_as_predicate,
    plan_inline_properties, plan_return, plan_set_clause, Error, ExpandParams, Expression,
    FilterPredicate, MatchClause, Pattern, PatternElement, PlanOperator, Result,
    ShortestPathParams, VarLenExpandParams,
};

/// Plan a MATCH statement.
pub(super) fn plan_match(match_clause: &MatchClause) -> Result<PlanOperator> {
    let pattern = &match_clause.pattern;

    // Start with pattern execution
    let mut plan = plan_pattern(pattern)?;

    // Add WHERE clause filter
    if let Some(ref where_clause) = match_clause.where_clause {
        let predicate = plan_expression_as_predicate(&where_clause.predicate)?;
        // Don't wrap with trivial True predicate
        if !matches!(predicate, FilterPredicate::True) {
            plan = PlanOperator::Filter {
                source: Box::new(plan),
                predicate,
            };
        }
    }

    // Add SET clause
    if let Some(ref set_clause) = match_clause.set_clause {
        let sets = plan_set_clause(set_clause)?;
        plan = PlanOperator::SetProperties {
            source: Box::new(plan),
            sets,
        };
    }

    // Add DELETE clause
    if let Some(ref delete_clause) = match_clause.delete_clause {
        // Extract variable names from delete expressions
        let variables: Vec<String> = delete_clause
            .expressions
            .iter()
            .filter_map(|expr| {
                if let Expression::Variable(v) = expr {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect();
        plan = PlanOperator::Delete {
            source: Box::new(plan),
            variables,
            detach: delete_clause.detach,
        };
    }

    // Add CREATE clause (MATCH...CREATE)
    if let Some(ref create_clause) = match_clause.create_clause {
        let (nodes, relationships) = plan_create_after_match(create_clause)?;
        plan = PlanOperator::Create {
            source: Some(Box::new(plan)),
            nodes,
            relationships,
        };
    }

    // Add RETURN clause projection and pagination
    if let Some(ref return_clause) = match_clause.return_clause {
        plan = plan_return(plan, return_clause)?;
    }

    Ok(plan)
}

/// Plan a pattern into scan/expand operators.
///
/// Handles comma-separated patterns (e.g. `MATCH (a), (b)`) by splitting
/// at node-node boundaries and cross-joining the independent parts.
pub(super) fn plan_pattern(pattern: &Pattern) -> Result<PlanOperator> {
    if pattern.elements.is_empty() {
        return Ok(PlanOperator::Empty);
    }

    // Check if this is a shortest path pattern (shortestPath() or allShortestPaths())
    if let Some(mode) = pattern.shortest_path {
        let all_paths = mode == crate::query::parser::ShortestPathMode::All;

        if pattern.elements.len() > 3 {
            // Comma-separated pattern with shortestPath, e.g.:
            //   MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..5]->(da))
            // The last 3 elements are the shortestPath pattern (node-rel-node);
            // preceding elements are regular patterns that bind variables.
            // Propagate labels/properties from the preceding elements to the
            // bare variable references inside shortestPath.
            let split = pattern.elements.len() - 3;
            let prefix_elements = &pattern.elements[..split];
            let mut sp_elements: Vec<PatternElement> = pattern.elements[split..].to_vec();

            // Build a lookup of variable -> node pattern from prefix elements
            let mut var_nodes = std::collections::HashMap::new();
            for elem in prefix_elements {
                if let PatternElement::Node(np) = elem {
                    if let Some(ref var) = np.variable {
                        var_nodes.insert(var.clone(), np);
                    }
                }
            }

            // Enrich bare nodes in the shortestPath pattern with labels/props
            // from the prefix-bound variables.
            for elem in &mut sp_elements {
                if let PatternElement::Node(np) = elem {
                    if let Some(ref var) = np.variable {
                        if np.labels.is_empty() && np.properties.is_none() {
                            if let Some(bound) = var_nodes.get(var) {
                                np.labels = bound.labels.clone();
                                np.properties = bound.properties.clone();
                            }
                        }
                    }
                }
            }

            let sp_pattern = Pattern {
                elements: sp_elements,
                path_variable: pattern.path_variable.clone(),
                shortest_path: pattern.shortest_path,
            };
            return plan_shortest_path_pattern(&sp_pattern, all_paths);
        }

        return plan_shortest_path_pattern(pattern, all_paths);
    }

    // Split elements into independent pattern segments at node-node boundaries.
    // e.g. [(a), -[:R]->,(b), (c)] splits into [(a), -[:R]->, (b)] and [(c)]
    let segments = split_pattern_segments(&pattern.elements);

    let mut plan: Option<PlanOperator> = None;
    for (seg_idx, segment) in segments.iter().enumerate() {
        // Only the first segment gets the path variable (path patterns can't be comma-separated)
        let path_var = if seg_idx == 0 {
            pattern.path_variable.clone()
        } else {
            None
        };
        let segment_plan = plan_pattern_segment(segment, path_var)?;
        plan = Some(match plan {
            None => segment_plan,
            Some(left) => PlanOperator::CrossJoin {
                left: Box::new(left),
                right: Box::new(segment_plan),
            },
        });
    }

    plan.ok_or_else(|| Error::Cypher("Empty pattern".into()))
}

/// Split pattern elements into independent segments at node-node boundaries.
pub(super) fn split_pattern_segments(elements: &[PatternElement]) -> Vec<Vec<&PatternElement>> {
    let mut segments: Vec<Vec<&PatternElement>> = Vec::new();
    let mut current: Vec<&PatternElement> = Vec::new();

    for (i, elem) in elements.iter().enumerate() {
        if i > 0 && matches!(elem, PatternElement::Node(_)) {
            // Check if the previous element was also a node (boundary)
            if matches!(elements[i - 1], PatternElement::Node(_)) {
                segments.push(current);
                current = Vec::new();
            }
        }
        current.push(elem);
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

/// Plan a single pattern segment (no node-node boundaries).
pub(super) fn plan_pattern_segment(
    elements: &[&PatternElement],
    path_variable: Option<String>,
) -> Result<PlanOperator> {
    let first_node = match elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Pattern must start with a node".into())),
    };

    let variable = first_node
        .variable
        .clone()
        .unwrap_or_else(|| "_n0".to_string());
    let label_groups: Vec<Vec<String>> = first_node.labels.clone();

    // Try to extract a simple property equality filter for pushdown
    let property_filter = first_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    let mut plan = PlanOperator::NodeScan {
        variable: variable.clone(),
        label_groups,
        limit: None,
        property_filter: property_filter.clone(),
    };

    // Add inline property filter if present and not already pushed down
    if let Some(ref props) = first_node.properties {
        if property_filter.is_none() {
            let predicate = plan_inline_properties(&variable, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    // Process remaining pattern elements (relationships and nodes)
    let mut current_var = variable;
    let mut elem_idx = 1;

    while elem_idx < elements.len() {
        // Expect relationship
        let rel = match elements[elem_idx] {
            PatternElement::Relationship(rp) => rp,
            _ => return Err(Error::Cypher("Expected relationship in pattern".into())),
        };

        elem_idx += 1;

        // Expect target node
        let target_node = match elements.get(elem_idx) {
            Some(PatternElement::Node(np)) => np,
            _ => {
                return Err(Error::Cypher(
                    "Expected target node after relationship".into(),
                ))
            }
        };

        elem_idx += 1;

        let target_var = target_node
            .variable
            .clone()
            .unwrap_or_else(|| format!("_n{}", elem_idx / 2));
        let target_labels: Vec<String> = target_node.labels.iter().flatten().cloned().collect();

        // Check if this is a variable-length relationship
        if let Some(ref length) = rel.length {
            let min_hops = length.min.unwrap_or(1);
            let max_hops = length.max.unwrap_or(100); // Reasonable default

            plan = PlanOperator::VariableLengthExpand(VarLenExpandParams {
                source: Box::new(plan),
                source_variable: current_var.clone(),
                rel_variable: rel.variable.clone(),
                target_variable: target_var.clone(),
                target_labels: target_labels.clone(),
                path_variable: path_variable.clone(),
                types: rel.types.clone(),
                direction: rel.direction.into(),
                min_hops,
                max_hops,
                target_ids: None,
                limit: None,
                target_property_filter: None, // Will be populated by predicate pushdown
            });
        } else {
            // Single-hop expand
            plan = PlanOperator::Expand(ExpandParams {
                source: Box::new(plan),
                source_variable: current_var.clone(),
                rel_variable: rel.variable.clone(),
                target_variable: target_var.clone(),
                target_labels: target_labels.clone(),
                path_variable: path_variable.clone(),
                types: rel.types.clone(),
                direction: rel.direction.into(),
                limit: None,
            });
        }

        // Add inline property filter for target node
        if let Some(ref props) = target_node.properties {
            let predicate = plan_inline_properties(&target_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }

        current_var = target_var;
    }

    Ok(plan)
}

/// Plan a shortest path pattern (used by shortestPath() expression).
/// This is now called when evaluating shortestPath expressions, not from pattern matching.
pub fn plan_shortest_path_pattern(pattern: &Pattern, all_paths: bool) -> Result<PlanOperator> {
    // shortestPath requires exactly node-rel-node
    if pattern.elements.len() != 3 {
        return Err(Error::Cypher(
            "shortestPath requires (a)-[*]->(b) pattern".into(),
        ));
    }

    let source_node = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected source node".into())),
    };

    let rel = match &pattern.elements[1] {
        PatternElement::Relationship(rp) => rp,
        _ => return Err(Error::Cypher("Expected relationship".into())),
    };

    let target_node = match &pattern.elements[2] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected target node".into())),
    };

    let source_var = source_node
        .variable
        .clone()
        .unwrap_or_else(|| "_src".to_string());
    let target_var = target_node
        .variable
        .clone()
        .unwrap_or_else(|| "_tgt".to_string());
    let source_label_groups: Vec<Vec<String>> = source_node.labels.clone();
    let target_labels: Vec<String> = target_node.labels.iter().flatten().cloned().collect();

    // Determine hop bounds from variable-length spec (e.g., [*1..5])
    let (min_hops, max_hops) = if let Some(ref length) = rel.length {
        (length.min.unwrap_or(1), length.max.unwrap_or(100))
    } else {
        // Default: variable length up to 100 hops
        (1, 100)
    };

    // Try to extract a simple property filter for pushdown
    let source_property_filter = source_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    let mut plan = PlanOperator::NodeScan {
        variable: source_var.clone(),
        label_groups: source_label_groups,
        limit: None,
        property_filter: source_property_filter.clone(),
    };

    // Add inline property filter for source if not pushed down
    if let Some(ref props) = source_node.properties {
        if source_property_filter.is_none() {
            let predicate = plan_inline_properties(&source_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    // Try to extract a simple property filter for target (for early BFS termination)
    let target_property_filter = target_node
        .properties
        .as_ref()
        .and_then(extract_simple_property_filter);

    // k=1 for shortestPath(), k=MAX for allShortestPaths() (all paths of shortest length)
    let k = if all_paths { u32::MAX } else { 1 };

    plan = PlanOperator::ShortestPath(ShortestPathParams {
        source: Box::new(plan),
        source_variable: source_var,
        target_variable: target_var.clone(),
        target_labels,
        path_variable: pattern.path_variable.clone(),
        types: rel.types.clone(),
        direction: rel.direction.into(),
        min_hops,
        max_hops,
        k,
        target_property_filter: target_property_filter.clone(),
    });

    // Add inline property filter for target if not pushed down (complex expressions)
    if let Some(ref props) = target_node.properties {
        if target_property_filter.is_none() {
            let predicate = plan_inline_properties(&target_var, props)?;
            if !matches!(predicate, FilterPredicate::True) {
                plan = PlanOperator::Filter {
                    source: Box::new(plan),
                    predicate,
                };
            }
        }
    }

    Ok(plan)
}
