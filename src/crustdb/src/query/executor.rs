//! Query executor - runs parsed statements against the storage backend.

use super::parser::{
    CreateClause, DeleteClause, Direction, Expression, Literal, MatchClause, NodePattern, Pattern,
    PatternElement, RelationshipPattern, ReturnClause, SetClause, SetItem, Statement,
};
use super::{QueryResult, QueryStats, ResultValue, Row};
use crate::error::{Error, Result};
use crate::graph::{Edge, Node, PropertyValue};
use crate::storage::SqliteStorage;
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
        Statement::Match(match_clause) => execute_match(match_clause, storage, &mut stats)?,
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

// =============================================================================
// MATCH Execution
// =============================================================================

/// A path through the graph (sequence of node IDs and edge IDs).
#[derive(Debug, Clone)]
struct Path {
    node_ids: Vec<i64>,
    edge_ids: Vec<i64>,
}

/// A binding represents a matched graph element (node or edge) with its variable name.
#[derive(Debug, Clone)]
struct Binding {
    nodes: HashMap<String, Node>,
    edges: HashMap<String, Edge>,
    /// Paths bound to variables (for `p = (a)-[*]->(b)` syntax).
    paths: HashMap<String, Path>,
    /// Edge lists for variable-length relationship bindings.
    edge_lists: HashMap<String, Vec<Edge>>,
}

impl Binding {
    fn new() -> Self {
        Binding {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            paths: HashMap::new(),
            edge_lists: HashMap::new(),
        }
    }

    fn with_node(mut self, var: &str, node: Node) -> Self {
        self.nodes.insert(var.to_string(), node);
        self
    }

    fn with_edge(mut self, var: &str, edge: Edge) -> Self {
        self.edges.insert(var.to_string(), edge);
        self
    }

    fn with_path(mut self, var: &str, path: Path) -> Self {
        self.paths.insert(var.to_string(), path);
        self
    }

    fn with_edge_list(mut self, var: &str, edges: Vec<Edge>) -> Self {
        self.edge_lists.insert(var.to_string(), edges);
        self
    }
}

/// Execute a MATCH statement.
fn execute_match(
    match_clause: &MatchClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<QueryResult> {
    let pattern = &match_clause.pattern;

    // Determine pattern type and execute accordingly
    let bindings = if is_single_node_pattern(pattern) {
        // Simple single-node pattern: MATCH (n) or MATCH (n:Label)
        execute_single_node_pattern(pattern, storage)?
    } else if is_single_hop_pattern(pattern) {
        // Single-hop relationship pattern: MATCH (a)-[r]->(b)
        execute_single_hop_pattern(pattern, storage)?
    } else if is_variable_length_pattern(pattern) {
        // Variable-length pattern: MATCH (a)-[*1..3]->(b)
        execute_variable_length_pattern(pattern, storage)?
    } else {
        return Err(Error::Cypher("Unsupported pattern type".into()));
    };

    // Filter by WHERE clause if present
    let bindings = if let Some(ref where_clause) = match_clause.where_clause {
        filter_bindings_by_where(bindings, &where_clause.predicate)?
    } else {
        bindings
    };

    // Execute SET clause if present
    if let Some(ref set_clause) = match_clause.set_clause {
        execute_set(&bindings, set_clause, storage, stats)?;
    }

    // Execute DELETE clause if present
    if let Some(ref delete_clause) = match_clause.delete_clause {
        execute_delete(&bindings, delete_clause, storage, stats)?;
    }

    // Build result based on RETURN clause (if present)
    if let Some(ref return_clause) = match_clause.return_clause {
        build_match_result_from_bindings(bindings, return_clause, stats)
    } else {
        // No RETURN clause - return empty result (mutation only)
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            stats: stats.clone(),
        })
    }
}

/// Check if pattern is a single node pattern.
fn is_single_node_pattern(pattern: &Pattern) -> bool {
    pattern.elements.len() == 1 && matches!(pattern.elements[0], PatternElement::Node(_))
}

/// Check if pattern is a single-hop relationship pattern (node-rel-node) without variable length.
fn is_single_hop_pattern(pattern: &Pattern) -> bool {
    if pattern.elements.len() != 3 {
        return false;
    }
    if !matches!(pattern.elements[0], PatternElement::Node(_)) {
        return false;
    }
    if !matches!(pattern.elements[2], PatternElement::Node(_)) {
        return false;
    }
    // Check that the relationship has no variable length
    match &pattern.elements[1] {
        PatternElement::Relationship(rel) => rel.length.is_none(),
        _ => false,
    }
}

/// Check if pattern is a variable-length relationship pattern (node-[*..]-node).
fn is_variable_length_pattern(pattern: &Pattern) -> bool {
    if pattern.elements.len() != 3 {
        return false;
    }
    if !matches!(pattern.elements[0], PatternElement::Node(_)) {
        return false;
    }
    if !matches!(pattern.elements[2], PatternElement::Node(_)) {
        return false;
    }
    // Check that the relationship has variable length
    match &pattern.elements[1] {
        PatternElement::Relationship(rel) => rel.length.is_some(),
        _ => false,
    }
}

/// Execute a single-node pattern match.
fn execute_single_node_pattern(pattern: &Pattern, storage: &SqliteStorage) -> Result<Vec<Binding>> {
    let node_pattern = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Err(Error::Cypher("Expected node pattern".into())),
    };

    let variable = node_pattern.variable.as_deref().unwrap_or("_");

    // Scan and filter nodes
    let nodes = scan_nodes(node_pattern, storage)?;
    let nodes = filter_by_properties(nodes, node_pattern)?;

    // Convert to bindings
    let bindings = nodes
        .into_iter()
        .map(|node| Binding::new().with_node(variable, node))
        .collect();

    Ok(bindings)
}

/// Execute a single-hop relationship pattern match.
fn execute_single_hop_pattern(pattern: &Pattern, storage: &SqliteStorage) -> Result<Vec<Binding>> {
    // Extract pattern components
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

    let source_var = source_pattern.variable.as_deref().unwrap_or("_src");
    let rel_var = rel_pattern.variable.as_deref();
    let target_var = target_pattern.variable.as_deref().unwrap_or("_tgt");

    // Scan source nodes
    let source_nodes = scan_nodes(source_pattern, storage)?;
    let source_nodes = filter_by_properties(source_nodes, source_pattern)?;

    let mut bindings = Vec::new();

    // For each source node, find matching relationships and targets
    for source_node in source_nodes {
        let edges = match rel_pattern.direction {
            Direction::Outgoing => storage.find_outgoing_edges(source_node.id)?,
            Direction::Incoming => storage.find_incoming_edges(source_node.id)?,
            Direction::Both => {
                let mut edges = storage.find_outgoing_edges(source_node.id)?;
                edges.extend(storage.find_incoming_edges(source_node.id)?);
                edges
            }
        };

        // Filter edges by type if specified
        let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
            edges
        } else {
            edges
                .into_iter()
                .filter(|e| rel_pattern.types.contains(&e.edge_type))
                .collect()
        };

        // Filter edges by properties if specified
        let edges = filter_edges_by_properties(edges, rel_pattern)?;

        // For each matching edge, get the target node and check if it matches
        for edge in edges {
            // Determine the actual target node based on direction
            let target_id = match rel_pattern.direction {
                Direction::Outgoing => edge.target,
                Direction::Incoming => edge.source,
                Direction::Both => {
                    if edge.source == source_node.id {
                        edge.target
                    } else {
                        edge.source
                    }
                }
            };

            // Get the target node
            let target_node = match storage.get_node(target_id)? {
                Some(n) => n,
                None => continue,
            };

            // Check if target node matches the target pattern
            if !node_matches_pattern(&target_node, target_pattern) {
                continue;
            }

            // Create binding for this match
            let mut binding = Binding::new()
                .with_node(source_var, source_node.clone())
                .with_node(target_var, target_node);

            if let Some(rv) = rel_var {
                binding = binding.with_edge(rv, edge);
            }

            bindings.push(binding);
        }
    }

    Ok(bindings)
}

/// State for BFS traversal with path tracking.
#[derive(Clone)]
struct TraversalState {
    node_id: i64,
    /// Node IDs in the path (including current).
    path_nodes: Vec<i64>,
    /// Edges traversed to reach this state.
    path_edges: Vec<Edge>,
}

/// Execute a variable-length relationship pattern match using BFS.
fn execute_variable_length_pattern(
    pattern: &Pattern,
    storage: &SqliteStorage,
) -> Result<Vec<Binding>> {
    use std::collections::{HashSet, VecDeque};

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
    let max_depth = length_spec.max.unwrap_or(100) as usize;

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
            path_edges: vec![],
        });

        while let Some(state) = queue.pop_front() {
            let depth = state.path_edges.len();

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
                                binding = binding.with_path(
                                    pv,
                                    Path {
                                        node_ids: state.path_nodes.clone(),
                                        edge_ids: state.path_edges.iter().map(|e| e.id).collect(),
                                    },
                                );
                            }

                            // Add edge list if relationship variable is specified
                            if let Some(rv) = rel_var {
                                binding = binding.with_edge_list(rv, state.path_edges.clone());
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

            // Get edges from current node
            let edges = match rel_pattern.direction {
                Direction::Outgoing => storage.find_outgoing_edges(state.node_id)?,
                Direction::Incoming => storage.find_incoming_edges(state.node_id)?,
                Direction::Both => {
                    let mut edges = storage.find_outgoing_edges(state.node_id)?;
                    edges.extend(storage.find_incoming_edges(state.node_id)?);
                    edges
                }
            };

            // Filter by relationship type if specified
            let edges: Vec<Edge> = if rel_pattern.types.is_empty() {
                edges
            } else {
                edges
                    .into_iter()
                    .filter(|e| rel_pattern.types.contains(&e.edge_type))
                    .collect()
            };

            // Add neighbors to queue
            for edge in edges {
                let next_id = match rel_pattern.direction {
                    Direction::Outgoing => edge.target,
                    Direction::Incoming => edge.source,
                    Direction::Both => {
                        if edge.source == state.node_id {
                            edge.target
                        } else {
                            edge.source
                        }
                    }
                };

                // Avoid cycles within the same path
                if state.path_nodes.contains(&next_id) {
                    continue;
                }

                let mut new_path_nodes = state.path_nodes.clone();
                new_path_nodes.push(next_id);

                let mut new_path_edges = state.path_edges.clone();
                new_path_edges.push(edge);

                queue.push_back(TraversalState {
                    node_id: next_id,
                    path_nodes: new_path_nodes,
                    path_edges: new_path_edges,
                });
            }
        }
    }

    Ok(bindings)
}

/// Check if a node matches a node pattern (labels and properties).
fn node_matches_pattern(node: &Node, pattern: &NodePattern) -> bool {
    // Check labels
    for label in &pattern.labels {
        if !node.has_label(label) {
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

/// Filter edges by properties specified in the relationship pattern.
fn filter_edges_by_properties(
    edges: Vec<Edge>,
    pattern: &RelationshipPattern,
) -> Result<Vec<Edge>> {
    let Some(ref props_expr) = pattern.properties else {
        return Ok(edges);
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
        return Ok(edges);
    }

    let filtered: Vec<Edge> = edges
        .into_iter()
        .filter(|edge| {
            required_props
                .iter()
                .all(|(key, value)| match edge.properties.get(key) {
                    Some(edge_value) => property_matches(edge_value, value),
                    None => matches!(value, Expression::Literal(Literal::Null)),
                })
        })
        .collect();

    Ok(filtered)
}

/// Filter bindings by WHERE clause predicate.
fn filter_bindings_by_where(
    bindings: Vec<Binding>,
    predicate: &Expression,
) -> Result<Vec<Binding>> {
    let mut filtered = Vec::new();

    for binding in bindings {
        let result = evaluate_expression_with_bindings(predicate, &binding)?;
        if is_truthy(&result) {
            filtered.push(binding);
        }
    }

    Ok(filtered)
}

/// Evaluate an expression using bindings (supports multiple variables).
fn evaluate_expression_with_bindings(
    expr: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    match expr {
        Expression::Literal(lit) => Ok(literal_to_property_value(lit)),

        Expression::Variable(name) => {
            if let Some(node) = binding.nodes.get(name) {
                Ok(PropertyValue::Map(node.properties.clone()))
            } else if let Some(edge) = binding.edges.get(name) {
                Ok(PropertyValue::Map(edge.properties.clone()))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }

        Expression::Property { base, property } => {
            if let Expression::Variable(base_name) = base.as_ref() {
                if let Some(node) = binding.nodes.get(base_name) {
                    return Ok(node.get(property).cloned().unwrap_or(PropertyValue::Null));
                }
                if let Some(edge) = binding.edges.get(base_name) {
                    return Ok(edge
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(PropertyValue::Null));
                }
            }
            Err(Error::Cypher("Property access on unknown variable".into()))
        }

        Expression::BinaryOp { left, op, right } => {
            evaluate_binary_op_with_bindings(left, *op, right, binding)
        }

        Expression::UnaryOp { op, operand } => {
            evaluate_unary_op_with_bindings(*op, operand, binding)
        }

        Expression::FunctionCall { name, args } => {
            evaluate_function_call_with_bindings(name, args, binding)
        }

        Expression::List(items) => {
            let values: Result<Vec<_>> = items
                .iter()
                .map(|item| evaluate_expression_with_bindings(item, binding))
                .collect();
            Ok(PropertyValue::List(values?))
        }

        _ => Err(Error::Cypher("Expression type not supported".into())),
    }
}

/// Evaluate binary operation with bindings.
fn evaluate_binary_op_with_bindings(
    left: &Expression,
    op: BinaryOperator,
    right: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    // Short-circuit for logical operators
    match op {
        BinaryOperator::And => {
            let left_val = evaluate_expression_with_bindings(left, binding)?;
            if !is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(false));
            }
            let right_val = evaluate_expression_with_bindings(right, binding)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        BinaryOperator::Or => {
            let left_val = evaluate_expression_with_bindings(left, binding)?;
            if is_truthy(&left_val) {
                return Ok(PropertyValue::Bool(true));
            }
            let right_val = evaluate_expression_with_bindings(right, binding)?;
            return Ok(PropertyValue::Bool(is_truthy(&right_val)));
        }
        _ => {}
    }

    let left_val = evaluate_expression_with_bindings(left, binding)?;
    let right_val = evaluate_expression_with_bindings(right, binding)?;

    // Reuse existing comparison logic
    match op {
        BinaryOperator::Eq => Ok(PropertyValue::Bool(values_equal(&left_val, &right_val))),
        BinaryOperator::Ne => Ok(PropertyValue::Bool(!values_equal(&left_val, &right_val))),
        BinaryOperator::Lt => {
            compare_values(&left_val, &right_val, |ord| ord == std::cmp::Ordering::Less)
        }
        BinaryOperator::Le => compare_values(&left_val, &right_val, |ord| {
            ord != std::cmp::Ordering::Greater
        }),
        BinaryOperator::Gt => compare_values(&left_val, &right_val, |ord| {
            ord == std::cmp::Ordering::Greater
        }),
        BinaryOperator::Ge => {
            compare_values(&left_val, &right_val, |ord| ord != std::cmp::Ordering::Less)
        }
        BinaryOperator::StartsWith => {
            string_predicate(&left_val, &right_val, |s, p| s.starts_with(p))
        }
        BinaryOperator::EndsWith => string_predicate(&left_val, &right_val, |s, p| s.ends_with(p)),
        BinaryOperator::Contains => string_predicate(&left_val, &right_val, |s, p| s.contains(p)),
        BinaryOperator::RegexMatch => match (&left_val, &right_val) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(PropertyValue::Bool(re.is_match(text))),
                    Err(e) => Err(Error::Cypher(format!("Invalid regex: {}", e))),
                }
            }
            _ => Ok(PropertyValue::Null),
        },
        BinaryOperator::In => {
            if let PropertyValue::List(list) = &right_val {
                Ok(PropertyValue::Bool(
                    list.iter().any(|v| values_equal(&left_val, v)),
                ))
            } else {
                Ok(PropertyValue::Null)
            }
        }
        BinaryOperator::Add => arithmetic_op(&left_val, &right_val, |a, b| a + b, |a, b| a + b),
        BinaryOperator::Sub => arithmetic_op(&left_val, &right_val, |a, b| a - b, |a, b| a - b),
        BinaryOperator::Mul => arithmetic_op(&left_val, &right_val, |a, b| a * b, |a, b| a * b),
        BinaryOperator::Div => arithmetic_op(&left_val, &right_val, |a, b| a / b, |a, b| a / b),
        BinaryOperator::Mod => arithmetic_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b),
        BinaryOperator::Pow => match (&left_val, &right_val) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Float((*a as f64).powf(*b as f64)))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                Ok(PropertyValue::Float(a.powf(*b)))
            }
            _ => Ok(PropertyValue::Null),
        },
        BinaryOperator::Xor => {
            let l = is_truthy(&left_val);
            let r = is_truthy(&right_val);
            Ok(PropertyValue::Bool(l ^ r))
        }
        BinaryOperator::And | BinaryOperator::Or => unreachable!(),
    }
}

/// Evaluate unary operation with bindings.
fn evaluate_unary_op_with_bindings(
    op: super::parser::UnaryOperator,
    operand: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    let val = evaluate_expression_with_bindings(operand, binding)?;

    match op {
        super::parser::UnaryOperator::Not => Ok(PropertyValue::Bool(!is_truthy(&val))),
        super::parser::UnaryOperator::Neg => match val {
            PropertyValue::Integer(n) => Ok(PropertyValue::Integer(-n)),
            PropertyValue::Float(f) => Ok(PropertyValue::Float(-f)),
            _ => Ok(PropertyValue::Null),
        },
        super::parser::UnaryOperator::IsNull => {
            Ok(PropertyValue::Bool(matches!(val, PropertyValue::Null)))
        }
        super::parser::UnaryOperator::IsNotNull => {
            Ok(PropertyValue::Bool(!matches!(val, PropertyValue::Null)))
        }
    }
}

/// Evaluate function call with bindings.
fn evaluate_function_call_with_bindings(
    name: &str,
    args: &[Expression],
    binding: &Binding,
) -> Result<PropertyValue> {
    let name_upper = name.to_uppercase();

    // For now, support basic functions
    match name_upper.as_str() {
        "COALESCE" => {
            for arg in args {
                let val = evaluate_expression_with_bindings(arg, binding)?;
                if !matches!(val, PropertyValue::Null) {
                    return Ok(val);
                }
            }
            Ok(PropertyValue::Null)
        }
        "SIZE" | "LENGTH" => {
            if args.len() != 1 {
                return Err(Error::Cypher("size() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::Integer(s.len() as i64)),
                PropertyValue::List(l) => Ok(PropertyValue::Integer(l.len() as i64)),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOLOWER" | "LOWER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toLower() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_lowercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        "TOUPPER" | "UPPER" => {
            if args.len() != 1 {
                return Err(Error::Cypher("toUpper() requires 1 argument".into()));
            }
            let val = evaluate_expression_with_bindings(&args[0], binding)?;
            match val {
                PropertyValue::String(s) => Ok(PropertyValue::String(s.to_uppercase())),
                PropertyValue::Null => Ok(PropertyValue::Null),
                _ => Ok(PropertyValue::Null),
            }
        }
        _ => Err(Error::Cypher(format!("Unknown function: {}", name))),
    }
}

/// Build query result from bindings.
fn build_match_result_from_bindings(
    bindings: Vec<Binding>,
    return_clause: &ReturnClause,
    _stats: &mut QueryStats,
) -> Result<QueryResult> {
    // Build column names
    let columns: Vec<String> = return_clause
        .items
        .iter()
        .map(|item| {
            if let Some(ref alias) = item.alias {
                alias.clone()
            } else {
                expr_to_column_name_generic(&item.expression)
            }
        })
        .collect();

    // Build rows
    let mut rows = Vec::with_capacity(bindings.len());

    for binding in bindings {
        let mut values = HashMap::new();

        for (i, item) in return_clause.items.iter().enumerate() {
            let column_name = &columns[i];
            let value = evaluate_return_item_with_bindings(&item.expression, &binding)?;
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

/// Convert expression to column name (generic version).
fn expr_to_column_name_generic(expr: &Expression) -> String {
    match expr {
        Expression::Variable(name) => name.clone(),
        Expression::Property { base, property } => {
            let base_name = expr_to_column_name_generic(base);
            format!("{}.{}", base_name, property)
        }
        Expression::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                let arg_str = args
                    .iter()
                    .map(expr_to_column_name_generic)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, arg_str)
            }
        }
        _ => "expr".to_string(),
    }
}

/// Evaluate return item expression with bindings.
fn evaluate_return_item_with_bindings(expr: &Expression, binding: &Binding) -> Result<ResultValue> {
    match expr {
        Expression::Variable(name) => {
            if let Some(node) = binding.nodes.get(name) {
                Ok(ResultValue::Node {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                })
            } else if let Some(edge) = binding.edges.get(name) {
                Ok(ResultValue::Edge {
                    id: edge.id,
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type.clone(),
                    properties: edge.properties.clone(),
                })
            } else if let Some(path) = binding.paths.get(name) {
                Ok(ResultValue::Path {
                    nodes: path.node_ids.clone(),
                    edges: path.edge_ids.clone(),
                })
            } else if let Some(edge_list) = binding.edge_lists.get(name) {
                // Return edge list as a list of edge property maps
                let list: Vec<PropertyValue> = edge_list
                    .iter()
                    .map(|e| PropertyValue::Map(e.properties.clone()))
                    .collect();
                Ok(ResultValue::Property(PropertyValue::List(list)))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }
        Expression::Property { base, property } => {
            if let Expression::Variable(base_name) = base.as_ref() {
                if let Some(node) = binding.nodes.get(base_name) {
                    let value = node.get(property).cloned().unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
                if let Some(edge) = binding.edges.get(base_name) {
                    let value = edge
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(PropertyValue::Null);
                    return Ok(ResultValue::Property(value));
                }
            }
            Err(Error::Cypher("Property access on unknown variable".into()))
        }
        Expression::Literal(lit) => {
            let prop_value = literal_to_property_value(lit);
            Ok(ResultValue::Property(prop_value))
        }
        _ => Err(Error::Cypher(
            "Complex expressions in RETURN not yet supported".into(),
        )),
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
            nodes.retain(|node| pattern.labels.iter().all(|l| node.has_label(l)));
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

// =============================================================================
// WHERE Clause Evaluation (M4)
// =============================================================================

use super::parser::BinaryOperator;

/// Convert a literal to a PropertyValue.
fn literal_to_property_value(lit: &Literal) -> PropertyValue {
    match lit {
        Literal::Null => PropertyValue::Null,
        Literal::Boolean(b) => PropertyValue::Bool(*b),
        Literal::Integer(n) => PropertyValue::Integer(*n),
        Literal::Float(f) => PropertyValue::Float(*f),
        Literal::String(s) => PropertyValue::String(s.clone()),
    }
}

/// Check if two PropertyValues are equal.
fn values_equal(a: &PropertyValue, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => false, // NULL never equals anything
        (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a == b,
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a == b,
        (PropertyValue::Float(a), PropertyValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
        // Cross-type numeric comparisons
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
            (*a as f64 - b).abs() < f64::EPSILON
        }
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
            (a - *b as f64).abs() < f64::EPSILON
        }
        // List equality
        (PropertyValue::List(a), PropertyValue::List(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal(x, y))
        }
        _ => false,
    }
}

/// Compare two values and apply a comparison function.
fn compare_values<F>(a: &PropertyValue, b: &PropertyValue, cmp: F) -> Result<PropertyValue>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    let ordering = match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => {
            return Ok(PropertyValue::Null); // Comparisons with NULL return NULL
        }
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        _ => return Ok(PropertyValue::Null), // Incompatible types
    };

    Ok(PropertyValue::Bool(cmp(ordering)))
}

/// Apply a string predicate.
fn string_predicate<F>(a: &PropertyValue, b: &PropertyValue, pred: F) -> Result<PropertyValue>
where
    F: Fn(&str, &str) -> bool,
{
    match (a, b) {
        (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
        (PropertyValue::String(s), PropertyValue::String(p)) => Ok(PropertyValue::Bool(pred(s, p))),
        _ => Ok(PropertyValue::Null), // Non-string types return NULL
    }
}

/// Apply an arithmetic operation.
fn arithmetic_op<F, G>(
    a: &PropertyValue,
    b: &PropertyValue,
    int_op: F,
    float_op: G,
) -> Result<PropertyValue>
where
    F: Fn(i64, i64) -> i64,
    G: Fn(f64, f64) -> f64,
{
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
            Ok(PropertyValue::Integer(int_op(*a, *b)))
        }
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            Ok(PropertyValue::Float(float_op(*a, *b)))
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
            Ok(PropertyValue::Float(float_op(*a as f64, *b)))
        }
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
            Ok(PropertyValue::Float(float_op(*a, *b as f64)))
        }
        // String concatenation with +
        (PropertyValue::String(a), PropertyValue::String(b)) => {
            Ok(PropertyValue::String(format!("{}{}", a, b)))
        }
        _ => Ok(PropertyValue::Null),
    }
}

/// Check if a PropertyValue is truthy.
fn is_truthy(val: &PropertyValue) -> bool {
    match val {
        PropertyValue::Null => false,
        PropertyValue::Bool(b) => *b,
        PropertyValue::Integer(n) => *n != 0,
        PropertyValue::Float(f) => *f != 0.0,
        PropertyValue::String(s) => !s.is_empty(),
        PropertyValue::List(l) => !l.is_empty(),
        PropertyValue::Map(m) => !m.is_empty(),
    }
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
    let edge_type = pattern
        .types
        .first()
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
            Err(Error::Cypher(format!(
                "Cannot use variable '{}' in CREATE properties",
                name
            )))
        }
        Expression::Parameter(name) => {
            // Parameters not yet supported
            Err(Error::Cypher(format!(
                "Parameters not yet supported: ${}",
                name
            )))
        }
        _ => Err(Error::Cypher(
            "Complex expressions not supported in CREATE properties".into(),
        )),
    }
}

/// Convert a literal to a JSON value.
fn literal_to_json(lit: &Literal) -> Result<serde_json::Value> {
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

// =============================================================================
// Mutation Execution (M7)
// =============================================================================

/// Execute a SET clause.
fn execute_set(
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
                    if let Some(node) = binding.nodes.get(variable) {
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
                    if let Some(node) = binding.nodes.get(variable) {
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
fn execute_delete(
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
                    if let Some(node) = binding.nodes.get(name) {
                        if delete_clause.detach {
                            // DETACH DELETE - delete node and all its edges
                            storage.delete_node(node.id)?;
                            stats.nodes_deleted += 1;
                        } else {
                            // Regular DELETE - fail if node has edges
                            if storage.has_edges(node.id)? {
                                return Err(Error::Cypher(format!(
                                    "Cannot delete node {} because it still has relationships. Use DETACH DELETE to delete it along with its relationships.",
                                    node.id
                                )));
                            }
                            storage.delete_node(node.id)?;
                            stats.nodes_deleted += 1;
                        }
                    } else if let Some(edge) = binding.edges.get(name) {
                        // Delete edge
                        storage.delete_edge(edge.id)?;
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
        let stmt =
            parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

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
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(
            &parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match all nodes
        let result = execute(&parse("MATCH (n) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.columns, vec!["n"]);
    }

    #[test]
    fn test_match_by_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(
            &parse("CREATE (n:Movie {title: 'Matrix'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match only Person nodes
        let result = execute(&parse("MATCH (n:Person) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_match_by_multiple_labels() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with varying labels
        execute(
            &parse("CREATE (n:Person:Actor {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person:Director {name: 'Oliver'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match only Person+Actor nodes
        let result = execute(&parse("MATCH (n:Person:Actor) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_by_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create some nodes
        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match by name property
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_return_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();

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
        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match and return multiple properties
        let result = execute(
            &parse("MATCH (n:Person) RETURN n.name, n.age").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.contains(&"n.name".to_string()));
        assert!(result.columns.contains(&"n.age".to_string()));
    }

    #[test]
    fn test_match_empty_result() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with one label
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match with non-existent label
        let result = execute(&parse("MATCH (n:Movie) RETURN n").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_property_not_found() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes with different properties
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        // Match by property that doesn't match any node
        let result = execute(
            &parse("MATCH (n:Person {name: 'Charlie'}) RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_match_return_missing_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node without 'age' property
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Return a property that doesn't exist
        let result = execute(&parse("MATCH (n:Person) RETURN n.age").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1);

        // Should return null for missing property
        let row = &result.rows[0];
        let age = row.values.get("n.age").unwrap();
        assert!(matches!(age, ResultValue::Property(PropertyValue::Null)));
    }

    // =========================================================================
    // WHERE Tests (M4)
    // =========================================================================

    #[test]
    fn test_where_comparison_greater_than() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.age > 28 RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_where_comparison_less_than_or_equal() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.age <= 25 RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_equality() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_where_inequality() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name <> 'Alice' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_and() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.age >= 25 AND n.age <= 30 RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_where_or() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Charlie' RETURN n")
                .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_where_not() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE NOT n.name = 'Alice' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_starts_with() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Adam'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Adam
    }

    #[test]
    fn test_where_ends_with() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Grace'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name ENDS WITH 'ce' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Grace
    }

    #[test]
    fn test_where_contains() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Alicia'})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Alicia
    }

    #[test]
    fn test_where_is_null() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap(); // No age

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.age IS NULL RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Bob only
    }

    #[test]
    fn test_where_is_not_null() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap(); // No age

        let result = execute(
            &parse("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Alice only
    }

    #[test]
    fn test_where_complex_expression() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap(),
            &storage,
        )
        .unwrap();

        // (age > 25 AND age < 35) OR name = 'Charlie'
        let result = execute(
            &parse(
                "MATCH (n:Person) WHERE (n.age > 25 AND n.age < 35) OR n.name = 'Charlie' RETURN n",
            )
            .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice (30) and Charlie
    }

    #[test]
    fn test_where_regex_match() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Person {name: 'Adam'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        // Match names starting with 'A'
        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name =~ '^A.*' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Adam
    }

    #[test]
    fn test_where_regex_match_case_insensitive() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (n:Person {name: 'bob'})").unwrap(), &storage).unwrap();

        // Case-insensitive match for 'alice'
        let result = execute(
            &parse("MATCH (n:Person) WHERE n.name =~ '(?i)alice' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_where_regex_match_digit_pattern() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (n:Product {code: 'ABC123'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Product {code: 'XYZ789'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (n:Product {code: 'NoDigits'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match codes containing digits
        let result = execute(
            &parse("MATCH (n:Product) WHERE n.code =~ '.*[0-9]+.*' RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // ABC123 and XYZ789
    }

    // =========================================================================
    // Single-Hop Traversal Tests (M5)
    // =========================================================================

    #[test]
    fn test_single_hop_outgoing() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice -[:KNOWS]-> Bob
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert!(result.columns.contains(&"a.name".to_string()));
        assert!(result.columns.contains(&"b.name".to_string()));
    }

    #[test]
    fn test_single_hop_incoming() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice -[:KNOWS]-> Bob
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match incoming to Bob
        let result = execute(
            &parse("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(a:Person) RETURN a.name, b.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_single_hop_any_direction() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice -[:KNOWS]-> Bob -[:KNOWS]-> Charlie
        execute(&parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap(), &storage).unwrap();

        // Match any direction from Bob
        let result = execute(
            &parse("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(other:Person) RETURN other.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Alice and Charlie
    }

    #[test]
    fn test_single_hop_with_relationship_variable() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create relationship with properties
        execute(&parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(
            &parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r, b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert!(result.columns.contains(&"r".to_string()));
    }

    #[test]
    fn test_single_hop_filter_by_relationship_type() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice -[:KNOWS]-> Bob, Alice -[:WORKS_WITH]-> Charlie
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:WORKS_WITH]->(c:Person {name: 'Charlie'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Only match KNOWS relationships
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_single_hop_any_relationship_type() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice -[:KNOWS]-> Bob, Alice -[:WORKS_WITH]-> Charlie
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:WORKS_WITH]->(c:Person {name: 'Charlie'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Match any relationship type
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[]->(b:Person) RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_single_hop_with_where_clause() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create relationships
        execute(&parse("CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS]->(b:Person {name: 'Bob', age: 25})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS]->(c:Person {name: 'Charlie', age: 35})").unwrap(), &storage).unwrap();

        // Filter with WHERE
        let result = execute(&parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) WHERE b.age > 30 RETURN b.name").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 1); // Only Charlie
    }

    #[test]
    fn test_single_hop_return_nodes() {
        let storage = SqliteStorage::in_memory().unwrap();

        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);

        // Check that we got nodes back
        let row = &result.rows[0];
        assert!(matches!(
            row.values.get("a"),
            Some(ResultValue::Node { .. })
        ));
        assert!(matches!(
            row.values.get("b"),
            Some(ResultValue::Node { .. })
        ));
    }

    #[test]
    fn test_single_hop_multiple_matches() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create: Alice knows Bob and Charlie
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(c:Person {name: 'Charlie'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_single_hop_no_matches() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create nodes without the relationship
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(&parse("CREATE (b:Person {name: 'Bob'})").unwrap(), &storage).unwrap();

        let result = execute(
            &parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_single_hop_multiple_relationship_types() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create movie with actor and director
        execute(&parse("CREATE (m:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(c:Person {name: 'Charlie Sheen'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (m:Movie {title: 'Wall Street'})<-[:DIRECTED]-(o:Person {name: 'Oliver Stone'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (m:Movie {title: 'Wall Street'})<-[:PRODUCED]-(p:Person {name: 'Ed Pressman'})").unwrap(), &storage).unwrap();

        // Match either ACTED_IN or DIRECTED
        let result = execute(&parse("MATCH (:Movie {title: 'Wall Street'})<-[:ACTED_IN|DIRECTED]-(person:Person) RETURN person.name").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Charlie and Oliver, not Ed
    }

    #[test]
    fn test_single_hop_multiple_types_outgoing() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create person with different relationships
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:WORKS_WITH]->(c:Person {name: 'Charlie'})")
                .unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:LIVES_WITH]->(d:Person {name: 'Dave'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Match KNOWS or WORKS_WITH
        let result = execute(
            &parse(
                "MATCH (a:Person {name: 'Alice'})-[:KNOWS|WORKS_WITH]->(b:Person) RETURN b.name",
            )
            .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Bob and Charlie, not Dave
    }

    #[test]
    fn test_single_hop_multiple_types_with_variable() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create relationships
        execute(&parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap(), &storage).unwrap();
        execute(&parse("CREATE (a:Person {name: 'Alice'})-[:WORKS_WITH {since: 2015}]->(c:Person {name: 'Charlie'})").unwrap(), &storage).unwrap();

        // Match with relationship variable
        let result = execute(&parse("MATCH (a:Person {name: 'Alice'})-[r:KNOWS|WORKS_WITH]->(b:Person) RETURN b.name, r").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
        assert!(result.columns.contains(&"r".to_string()));
    }

    #[test]
    fn test_single_hop_multiple_types_undirected() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create bidirectional relationships
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (c:Person {name: 'Charlie'})-[:WORKS_WITH]->(a:Person {name: 'Alice'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Match undirected with multiple types
        let result = execute(&parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS|WORKS_WITH]-(other:Person) RETURN other.name").unwrap(), &storage).unwrap();

        assert_eq!(result.rows.len(), 2); // Bob and Charlie
    }

    #[test]
    fn test_single_hop_where_on_relationship() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create relationships with different 'since' properties
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2000}]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Filter by relationship property
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) WHERE r.since > 2005 RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        // Only Bob (since: 2010 > 2005), not Charlie (since: 2000 < 2005)
        assert_eq!(result.rows.len(), 1);
    }

    // M6: Variable-length pattern tests
    #[test]
    fn test_variable_length_basic() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain: Alice -> Bob -> Charlie -> Dave (single CREATE)
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})-[:KNOWS]->(d:Person {name: 'Dave'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Find all people reachable from Alice within 1-2 hops
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Bob (1 hop), Charlie (2 hops), NOT Dave (3 hops)
    }

    #[test]
    fn test_variable_length_unbounded() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain: Alice -> Bob -> Charlie (single CREATE)
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Find all people reachable from Alice (unbounded)
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person) RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Bob and Charlie
    }

    #[test]
    fn test_variable_length_exact() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain: Alice -> Bob -> Charlie -> Dave (single CREATE)
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})-[:KNOWS]->(d:Person {name: 'Dave'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Find people exactly 2 hops from Alice
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(b:Person) RETURN b.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // Only Charlie (exactly 2 hops)
    }

    #[test]
    fn test_variable_length_cycle_handling() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain first: Alice -> Bob -> Charlie
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Add edge back to Alice using variable reference (creates cycle)
        // Note: We need to find the actual Alice node and create edge to it
        // For now, test with a simple non-cyclic case
        // TODO: Test actual cycles when MATCH...CREATE is supported

        // Should find reachable nodes
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person) RETURN b.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Bob (1 hop), Charlie (2 hops)
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_variable_length_any_type() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create mixed relationship chain
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:WORKS_WITH]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match any relationship type
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[*1..2]->(b:Person) RETURN b.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Bob and Charlie
    }

    #[test]
    fn test_variable_length_with_type_filter() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain: Alice -KNOWS-> Bob -KNOWS-> Dave
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(d:Person {name: 'Dave'})").unwrap(),
            &storage,
        )
        .unwrap();
        // Add branch: Bob -WORKS_WITH-> Charlie
        execute(
            &parse("CREATE (b:Person {name: 'Bob'})-[:WORKS_WITH]->(c:Person {name: 'Charlie'})")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Match only KNOWS relationships from Alice
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name")
                .unwrap(),
            &storage,
        )
        .unwrap();

        // Bob (1 hop via KNOWS) and Dave (2 hops via KNOWS->KNOWS)
        // NOT Charlie (Bob-WORKS_WITH->Charlie doesn't match KNOWS)
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_variable_length_path_variable() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain: Alice -> Bob -> Charlie
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match with path variable
        let result = execute(
            &parse("MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*]->(c:Person) RETURN p").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // Two paths: Alice->Bob and Alice->Bob->Charlie
        assert!(result.columns.contains(&"p".to_string()));

        // Verify path result type
        for row in &result.rows {
            match row.values.get("p") {
                Some(ResultValue::Path { nodes, edges }) => {
                    assert!(nodes.len() >= 2); // At least source and target
                    assert_eq!(edges.len(), nodes.len() - 1);
                }
                _ => panic!("Expected Path result"),
            }
        }
    }

    #[test]
    fn test_variable_length_edge_list() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a chain with relationship properties
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})-[:KNOWS {since: 2015}]->(c:Person {name: 'Charlie'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Match with relationship list variable
        let result = execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[r:KNOWS*2]->(c:Person) RETURN r").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1); // One path of exactly length 2
        assert!(result.columns.contains(&"r".to_string()));

        // Verify edge list result
        match result.rows[0].values.get("r") {
            Some(ResultValue::Property(PropertyValue::List(edges))) => {
                assert_eq!(edges.len(), 2); // Two edges in the path
            }
            _ => panic!("Expected list of edges"),
        }
    }

    // ==========================================================================
    // M7: SET and DELETE tests
    // ==========================================================================

    #[test]
    fn test_set_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(
            &parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            &storage,
        )
        .unwrap();

        // Set property
        execute(
            &parse("MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify the property was updated
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) RETURN n.age").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        match result.rows[0].values.get("n.age") {
            Some(ResultValue::Property(PropertyValue::Integer(31))) => {}
            other => panic!("Expected age 31, got {:?}", other),
        }
    }

    #[test]
    fn test_set_new_property() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Add a new property
        execute(
            &parse("MATCH (n:Person {name: 'Alice'}) SET n.email = 'alice@example.com'").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify the property was added
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) RETURN n.email").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        match result.rows[0].values.get("n.email") {
            Some(ResultValue::Property(PropertyValue::String(s))) => {
                assert_eq!(s, "alice@example.com");
            }
            other => panic!("Expected email string, got {:?}", other),
        }
    }

    #[test]
    fn test_set_label() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Add a label
        execute(
            &parse("MATCH (n:Person {name: 'Alice'}) SET n:Employee").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify the label was added by matching on both labels
        let result = execute(
            &parse("MATCH (n:Person:Employee) RETURN n.name").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_delete_node_no_edges() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node
        execute(
            &parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Delete the node
        execute(
            &parse("MATCH (n:Person {name: 'Alice'}) DELETE n").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify the node was deleted
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(),
            &storage,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_delete_node_with_edges_fails() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with relationships
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Try to delete node with relationships (should fail)
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) DELETE n").unwrap(),
            &storage,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("still has relationships"));
    }

    #[test]
    fn test_detach_delete_node() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a node with relationships
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Detach delete should succeed
        execute(
            &parse("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify Alice is gone
        let result = execute(
            &parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(),
            &storage,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 0);

        // Verify Bob still exists
        let result = execute(
            &parse("MATCH (n:Person {name: 'Bob'}) RETURN n").unwrap(),
            &storage,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Verify relationship is gone
        let result = execute(
            &parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap(),
            &storage,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_delete_relationship() {
        let storage = SqliteStorage::in_memory().unwrap();

        // Create a relationship
        execute(
            &parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap(),
            &storage,
        )
        .unwrap();

        // Delete the relationship
        execute(
            &parse("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) DELETE r").unwrap(),
            &storage,
        )
        .unwrap();

        // Verify relationship is gone
        let result = execute(
            &parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap(),
            &storage,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 0);

        // Verify nodes still exist
        let result = execute(&parse("MATCH (n:Person) RETURN n.name").unwrap(), &storage).unwrap();
        assert_eq!(result.rows.len(), 2);
    }
}
