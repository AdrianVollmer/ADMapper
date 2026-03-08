//! Traversal execution functions for various pattern types.

use crate::error::{Error, Result};
use crate::graph::{Node, Relationship};
use crate::query::parser::{Direction, NodePattern, Pattern, PatternElement, RelationshipPattern};
use crate::storage::SqliteStorage;
use std::collections::HashSet;
use std::collections::VecDeque;

use super::super::{Binding, Path};
use super::matching::{
    filter_by_properties, filter_relationships_by_properties, node_matches_pattern, scan_nodes,
    scan_nodes_with_limit,
};
use super::{TraversalState, DEFAULT_MAX_PATH_DEPTH};

/// Execute a single-node pattern match.
pub fn execute_single_node_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
    limit: Option<u64>,
) -> Result<Vec<Binding>> {
    let node_pattern = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected node pattern".into())),
    };

    let variable = node_pattern.variable.as_deref().unwrap_or("_");

    // Scan and filter nodes (with optional SQL-level limit)
    let nodes = scan_nodes_with_limit(node_pattern, storage, limit)?;
    let nodes = filter_by_properties(nodes, node_pattern)?;

    // Convert to bindings
    let bindings = nodes
        .into_iter()
        .map(|node| Binding::new().with_node(variable, node))
        .collect();

    Ok(bindings)
}

/// Check if a node pattern has filters (labels or properties).
/// Used to determine selectivity for query optimization.
fn pattern_has_filters(pattern: &NodePattern) -> bool {
    !pattern.labels.is_empty() || pattern.properties.is_some()
}

/// Decomposed single-hop pattern components for execution.
struct SingleHopComponents<'a> {
    source_pattern: &'a NodePattern,
    rel_pattern: &'a RelationshipPattern,
    target_pattern: &'a NodePattern,
    source_var: &'a str,
    rel_var: Option<&'a str>,
    target_var: &'a str,
    path_var: Option<&'a str>,
}

impl<'a> SingleHopComponents<'a> {
    /// Extract components from a parsed single-hop pattern.
    fn from_pattern(pattern: &'a Pattern) -> Result<Self> {
        let (source_pattern, rel_pattern, target_pattern) = match (
            &pattern.elements[0],
            &pattern.elements[1],
            &pattern.elements[2],
        ) {
            (PatternElement::Node(s), PatternElement::Relationship(r), PatternElement::Node(t)) => {
                (s, r, t)
            }
            _ => return Err(Error::Cypher("Invalid single-hop pattern".into())),
        };

        Ok(Self {
            source_pattern,
            rel_pattern,
            target_pattern,
            source_var: source_pattern.variable.as_deref().unwrap_or("_src"),
            rel_var: rel_pattern.variable.as_deref(),
            target_var: target_pattern.variable.as_deref().unwrap_or("_tgt"),
            path_var: pattern.path_variable.as_deref(),
        })
    }

    /// Build a binding from a matched source, relationship, and target.
    fn build_binding(
        &self,
        source_node: &Node,
        relationship: &Relationship,
        target_node: &Node,
    ) -> Binding {
        let mut binding = Binding::new()
            .with_node(self.source_var, source_node.clone())
            .with_node(self.target_var, target_node.clone());

        if let Some(rv) = self.rel_var {
            binding = binding.with_relationship(rv, relationship.clone());
        }

        if let Some(pv) = self.path_var {
            let path = Path {
                nodes: vec![source_node.clone(), target_node.clone()],
                relationships: vec![relationship.clone()],
            };
            binding = binding.with_path(pv, path);
        }

        binding
    }
}

/// Execute a single-hop relationship pattern match.
pub fn execute_single_hop_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    let hop = SingleHopComponents::from_pattern(pattern)?;

    // Optimization: flip traversal direction when target is more selective than source.
    // For example: MATCH (a)-[r]->(b {id: '...'}) should start from b and find incoming relationships.
    let source_has_filters = pattern_has_filters(hop.source_pattern);
    let target_has_filters = pattern_has_filters(hop.target_pattern);
    if !source_has_filters && target_has_filters {
        return execute_single_hop_pattern_flipped(&hop, storage);
    }

    // Scan source nodes
    let source_nodes = scan_nodes(hop.source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, hop.source_pattern)?;

    let mut bindings = Vec::new();

    // For each source node, find matching relationships and targets
    for source_node in source_nodes {
        let relationships = find_relationships_from(source_node.id, hop.rel_pattern, storage)?;
        let relationships = filter_relationships_by_properties(relationships, hop.rel_pattern)?;

        for relationship in relationships {
            let target_id = resolve_other_end(&relationship, source_node.id, hop.rel_pattern);

            let target_node = match storage.get_node(target_id)? {
                Some(n) => n,
                None => continue,
            };

            if !node_matches_pattern(&target_node, hop.target_pattern) {
                continue;
            }

            bindings.push(hop.build_binding(&source_node, &relationship, &target_node));
        }
    }

    Ok(bindings)
}

/// Execute single-hop pattern with flipped traversal direction.
/// Called when the target node is more selective than the source node.
fn execute_single_hop_pattern_flipped(
    hop: &SingleHopComponents<'_>,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    let target_nodes = scan_nodes(hop.target_pattern, storage)?;
    let target_nodes = filter_by_properties(target_nodes, hop.target_pattern)?;

    let mut bindings = Vec::new();

    for target_node in target_nodes {
        // Find relationships in the reverse direction (scanning from target side)
        let relationships = find_relationships_flipped(target_node.id, hop.rel_pattern, storage)?;
        let relationships = filter_relationships_by_properties(relationships, hop.rel_pattern)?;

        for relationship in relationships {
            let source_id =
                resolve_other_end_flipped(&relationship, target_node.id, hop.rel_pattern);

            let source_node = match storage.get_node(source_id)? {
                Some(n) => n,
                None => continue,
            };

            if !node_matches_pattern(&source_node, hop.source_pattern) {
                continue;
            }

            bindings.push(hop.build_binding(&source_node, &relationship, &target_node));
        }
    }

    Ok(bindings)
}

/// Find relationships from a node in the direction specified by the pattern.
fn find_relationships_from(
    node_id: i64,
    rel_pattern: &RelationshipPattern,
    storage: &SqliteStorage,
) -> Result<Vec<Relationship>> {
    let mut relationships = match rel_pattern.direction {
        Direction::Outgoing => storage.find_outgoing_relationships(node_id)?,
        Direction::Incoming => storage.find_incoming_relationships(node_id)?,
        Direction::Both => {
            let mut rels = storage.find_outgoing_relationships(node_id)?;
            rels.extend(storage.find_incoming_relationships(node_id)?);
            rels
        }
    };

    // Filter by type if specified
    if !rel_pattern.types.is_empty() {
        relationships.retain(|e| rel_pattern.types.contains(&e.rel_type));
    }

    Ok(relationships)
}

/// Find relationships from a node in the REVERSE direction (for flipped traversal).
fn find_relationships_flipped(
    node_id: i64,
    rel_pattern: &RelationshipPattern,
    storage: &SqliteStorage,
) -> Result<Vec<Relationship>> {
    let mut relationships = match rel_pattern.direction {
        Direction::Outgoing => storage.find_incoming_relationships(node_id)?,
        Direction::Incoming => storage.find_outgoing_relationships(node_id)?,
        Direction::Both => {
            let mut rels = storage.find_outgoing_relationships(node_id)?;
            rels.extend(storage.find_incoming_relationships(node_id)?);
            rels
        }
    };

    if !rel_pattern.types.is_empty() {
        relationships.retain(|e| rel_pattern.types.contains(&e.rel_type));
    }

    Ok(relationships)
}

/// Resolve the "other end" of a relationship from a known starting node.
fn resolve_other_end(
    relationship: &Relationship,
    from_id: i64,
    rel_pattern: &RelationshipPattern,
) -> i64 {
    match rel_pattern.direction {
        Direction::Outgoing => relationship.target,
        Direction::Incoming => relationship.source,
        Direction::Both => {
            if relationship.source == from_id {
                relationship.target
            } else {
                relationship.source
            }
        }
    }
}

/// Resolve the "other end" in flipped traversal (target is the anchor node).
fn resolve_other_end_flipped(
    relationship: &Relationship,
    target_id: i64,
    rel_pattern: &RelationshipPattern,
) -> i64 {
    match rel_pattern.direction {
        Direction::Outgoing => relationship.source,
        Direction::Incoming => relationship.target,
        Direction::Both => {
            if relationship.target == target_id {
                relationship.source
            } else {
                relationship.target
            }
        }
    }
}

/// Execute a multi-hop pattern match (e.g., (a)-[r1]->(b)-[r2]->(c)).
pub fn execute_multi_hop_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    let path_var = pattern.path_variable.as_deref();

    // Extract all nodes and relationships from the pattern
    let mut node_patterns: Vec<&NodePattern> = Vec::new();
    let mut rel_patterns: Vec<&RelationshipPattern> = Vec::new();

    for (i, elem) in pattern.elements.iter().enumerate() {
        if i % 2 == 0 {
            match elem {
                PatternElement::Node(np) => node_patterns.push(np),
                _ => return Err(Error::Cypher("Invalid multi-hop pattern".into())),
            }
        } else {
            match elem {
                PatternElement::Relationship(rp) => rel_patterns.push(rp),
                _ => return Err(Error::Cypher("Invalid multi-hop pattern".into())),
            }
        }
    }

    // Start with the first node pattern
    let first_node_pattern = node_patterns[0];
    let first_var = first_node_pattern.variable.as_deref().unwrap_or("_n0");

    let initial_nodes = scan_nodes(first_node_pattern, storage)?;
    let initial_nodes = filter_by_properties(initial_nodes, first_node_pattern)?;

    // Initialize bindings with first node
    let mut current_bindings: Vec<(Binding, Vec<i64>, Vec<i64>)> = initial_nodes
        .into_iter()
        .map(|node| {
            let node_id = node.id;
            let binding = Binding::new().with_node(first_var, node);
            (binding, vec![node_id], vec![])
        })
        .collect();

    // Process each hop
    for hop_idx in 0..rel_patterns.len() {
        let rel_pattern = rel_patterns[hop_idx];
        let target_pattern = node_patterns[hop_idx + 1];

        let rel_var = rel_pattern.variable.as_deref();
        let default_target_var = format!("_n{}", hop_idx + 1);
        let target_var = target_pattern
            .variable
            .as_deref()
            .unwrap_or(&default_target_var);

        let mut next_bindings: Vec<(Binding, Vec<i64>, Vec<i64>)> = Vec::new();

        for (binding, path_nodes, path_rels) in current_bindings {
            // Get the last node in the path
            let last_node_id = *path_nodes.last().unwrap();

            // Find relationships from the last node
            let relationships = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_relationships(last_node_id)?,
                Direction::Incoming => storage.find_incoming_relationships(last_node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_relationships(last_node_id)?;
                    relationships.extend(storage.find_incoming_relationships(last_node_id)?);
                    relationships
                }
            };

            // Filter relationships by type
            let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
                relationships
            } else {
                relationships
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.rel_type))
                    .collect()
            };

            // Filter relationships by properties
            let relationships = filter_relationships_by_properties(relationships, rel_pattern)?;

            for relationship in relationships {
                // Determine the target node
                let target_id = match rel_pattern.direction {
                    Direction::Outgoing => relationship.target,
                    Direction::Incoming => relationship.source,
                    Direction::Both => {
                        if relationship.source == last_node_id {
                            relationship.target
                        } else {
                            relationship.source
                        }
                    }
                };

                // Get target node
                let target_node = match storage.get_node(target_id)? {
                    Some(n) => n,
                    None => continue,
                };

                // Check if target matches pattern
                if !node_matches_pattern(&target_node, target_pattern) {
                    continue;
                }

                // Create new binding
                let mut new_binding = binding.clone().with_node(target_var, target_node);

                if let Some(rv) = rel_var {
                    new_binding = new_binding.with_relationship(rv, relationship.clone());
                }

                // Update path
                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(target_id);
                let mut new_path_rels = path_rels.clone();
                new_path_rels.push(relationship.id);

                next_bindings.push((new_binding, new_path_nodes, new_path_rels));
            }
        }

        current_bindings = next_bindings;
    }

    // Convert to final bindings with optional path variable
    let mut bindings = Vec::new();
    for (mut binding, path_node_ids, path_rel_ids) in current_bindings {
        if let Some(pv) = path_var {
            // Fetch full node objects
            let mut path_nodes: Vec<Node> = Vec::new();
            for &nid in &path_node_ids {
                if let Some(node) = storage.get_node(nid)? {
                    path_nodes.push(node);
                }
            }
            // Fetch full relationship objects
            let mut path_rels: Vec<Relationship> = Vec::new();
            for &eid in &path_rel_ids {
                if let Some(relationship) = storage.get_relationship(eid)? {
                    path_rels.push(relationship);
                }
            }
            let path = Path {
                nodes: path_nodes,
                relationships: path_rels,
            };
            binding = binding.with_path(pv, path);
        }
        bindings.push(binding);
    }

    Ok(bindings)
}

/// Execute a variable-length relationship pattern match using BFS.
pub fn execute_variable_length_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    // Extract pattern components
    let (source_pattern, rel_pattern, target_pattern) = match (
        &pattern.elements[0],
        &pattern.elements[1],
        &pattern.elements[2],
    ) {
        (PatternElement::Node(s), PatternElement::Relationship(r), PatternElement::Node(t)) => {
            (s, r, t)
        }
        _ => return Err(Error::Cypher("Invalid variable-length pattern".into())),
    };

    let source_var = source_pattern.variable.as_deref().unwrap_or("_src");
    let target_var = target_pattern.variable.as_deref().unwrap_or("_tgt");
    let rel_var = rel_pattern.variable.as_deref();
    let path_var = pattern.path_variable.as_deref();

    // Get length constraints
    let length_spec = rel_pattern
        .length
        .as_ref()
        .ok_or_else(|| Error::Cypher("Variable-length pattern requires length spec".into()))?;

    let min_depth = length_spec.min.unwrap_or(1) as usize;
    let max_depth = length_spec.max.unwrap_or(DEFAULT_MAX_PATH_DEPTH as u32) as usize;

    // Scan source nodes
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;

    let mut bindings = Vec::new();

    // BFS from each source node
    for source_node in source_nodes {
        let mut queue: VecDeque<TraversalState> = VecDeque::new();
        // Track visited nodes per path to avoid cycles within a single path
        let mut found_targets: HashSet<i64> = HashSet::new();

        queue.push_back(TraversalState {
            node_id: source_node.id,
            path_nodes: vec![source_node.id],
            path_rels: vec![],
        });

        while let Some(state) = queue.pop_front() {
            let depth = state.path_rels.len();

            // If we've reached a valid depth, check if current node matches target pattern
            if depth >= min_depth && depth <= max_depth {
                if let Some(target_node) = storage.get_node(state.node_id)? {
                    if node_matches_pattern(&target_node, target_pattern) {
                        // Avoid duplicate results for same source-target pair
                        if !found_targets.contains(&state.node_id)
                            && state.node_id != source_node.id
                        {
                            found_targets.insert(state.node_id);

                            let mut binding = Binding::new()
                                .with_node(source_var, source_node.clone())
                                .with_node(target_var, target_node);

                            // Add path variable if requested
                            if let Some(pv) = path_var {
                                // Fetch full node objects for the path
                                let mut path_nodes_full: Vec<Node> = Vec::new();
                                for &nid in &state.path_nodes {
                                    if let Some(node) = storage.get_node(nid)? {
                                        path_nodes_full.push(node);
                                    }
                                }
                                binding = binding.with_path(
                                    pv,
                                    Path {
                                        nodes: path_nodes_full,
                                        relationships: state.path_rels.clone(),
                                    },
                                );
                            }

                            // Add relationship list if relationship variable is specified
                            if let Some(rv) = rel_var {
                                binding =
                                    binding.with_relationship_list(rv, state.path_rels.clone());
                            }

                            bindings.push(binding);
                        }
                    }
                }
            }

            // Don't expand beyond max depth
            if depth >= max_depth {
                continue;
            }

            // Get relationships from current node
            let relationships = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_relationships(state.node_id)?,
                Direction::Incoming => storage.find_incoming_relationships(state.node_id)?,
                Direction::Both => {
                    let mut relationships = storage.find_outgoing_relationships(state.node_id)?;
                    relationships.extend(storage.find_incoming_relationships(state.node_id)?);
                    relationships
                }
            };

            // Filter by relationship type if specified
            let relationships: Vec<Relationship> = if rel_pattern.types.is_empty() {
                relationships
            } else {
                relationships
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.rel_type))
                    .collect()
            };

            // Add neighbors to queue
            for relationship in relationships {
                let next_id = match rel_pattern.direction {
                    Direction::Outgoing => relationship.target,
                    Direction::Incoming => relationship.source,
                    Direction::Both => {
                        if relationship.source == state.node_id {
                            relationship.target
                        } else {
                            relationship.source
                        }
                    }
                };

                // Avoid cycles within the same path
                if state.path_nodes.contains(&next_id) {
                    continue;
                }

                let mut new_path_nodes = state.path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_rels = state.path_rels.clone();
                new_path_rels.push(relationship);

                queue.push_back(TraversalState {
                    node_id: next_id,
                    path_nodes: new_path_nodes,
                    path_rels: new_path_rels,
                });
            }
        }
    }

    Ok(bindings)
}
