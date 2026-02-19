//! Query executor - runs parsed statements against the storage backend.

mod aggregate;
mod create;
mod eval;
mod mutation;
mod pattern;
mod result;

use super::parser::{BinaryOperator, Expression, Literal, MatchClause, PatternElement, Statement};
use super::{QueryResult, QueryStats, ResultValue, Row};
use crate::error::{Error, Result};
use crate::graph::{Edge, Node, PropertyValue};
use crate::storage::SqliteStorage;
use std::collections::HashMap;
use std::time::Instant;

// Re-exports for submodules
pub use aggregate::{evaluate_aggregate, has_aggregate_functions, is_aggregate_function};
pub use create::execute_create;
pub use eval::{
    evaluate_expression_with_bindings, evaluate_function_call_with_bindings,
    filter_bindings_by_where, literal_to_property_value,
};
pub use mutation::{execute_delete, execute_set};
pub use pattern::{
    execute_multi_hop_pattern, execute_shortest_path_pattern, execute_single_hop_pattern,
    execute_single_node_pattern, execute_variable_length_pattern, get_path_endpoint_vars,
    is_multi_hop_pattern, is_shortest_path_pattern, is_single_hop_pattern, is_single_node_pattern,
    is_variable_length_pattern,
};
pub use result::{build_match_result_from_bindings, evaluate_return_item_with_bindings};

// =============================================================================
// Core Data Structures
// =============================================================================

/// A path through the graph (sequence of nodes and edges with full data).
#[derive(Debug, Clone)]
pub struct Path {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// A binding represents a matched graph element (node or edge) with its variable name.
#[derive(Debug, Clone)]
pub struct Binding {
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Edge>,
    /// Paths bound to variables (for `p = (a)-[*]->(b)` syntax).
    pub paths: HashMap<String, Path>,
    /// Edge lists for variable-length relationship bindings.
    pub edge_lists: HashMap<String, Vec<Edge>>,
}

impl Binding {
    pub fn new() -> Self {
        Binding {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            paths: HashMap::new(),
            edge_lists: HashMap::new(),
        }
    }

    pub fn with_node(mut self, var: &str, node: Node) -> Self {
        self.nodes.insert(var.to_string(), node);
        self
    }

    pub fn with_edge(mut self, var: &str, edge: Edge) -> Self {
        self.edges.insert(var.to_string(), edge);
        self
    }

    pub fn with_path(mut self, var: &str, path: Path) -> Self {
        self.paths.insert(var.to_string(), path);
        self
    }

    pub fn with_edge_list(mut self, var: &str, edges: Vec<Edge>) -> Self {
        self.edge_lists.insert(var.to_string(), edges);
        self
    }
}

impl Default for Binding {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Predicate Pushdown
// =============================================================================

/// Property constraints for a single variable (property name -> allowed values).
pub type PropertyConstraints = HashMap<String, Vec<PropertyValue>>;

/// Constraints extracted from WHERE clause, keyed by variable name.
///
/// This generic structure can be used for any pattern type, not just shortest paths.
/// Each variable maps to its property constraints extracted from equality predicates.
pub type VariableConstraints = HashMap<String, PropertyConstraints>;

/// Constraints extracted from WHERE clause for shortest path optimization.
///
/// This is a convenience wrapper around `VariableConstraints` for the common
/// case of source/target path endpoints.
#[derive(Debug, Default)]
pub struct PathConstraints {
    /// Property constraints for source nodes (e.g., `src.name = 'A'` -> ("name", ["A"])).
    pub source_props: PropertyConstraints,
    /// Property constraints for target nodes (e.g., `dst.name = 'B'` -> ("name", ["B"])).
    pub target_props: PropertyConstraints,
}

impl PathConstraints {
    /// Create PathConstraints from generic VariableConstraints.
    pub fn from_variable_constraints(
        constraints: VariableConstraints,
        source_var: &str,
        target_var: &str,
    ) -> Self {
        Self {
            source_props: constraints.get(source_var).cloned().unwrap_or_default(),
            target_props: constraints.get(target_var).cloned().unwrap_or_default(),
        }
    }
}

/// Extract property constraints from a WHERE clause predicate.
///
/// Returns constraints for all variables found in equality predicates.
/// This is a generic extraction that doesn't assume any specific variable roles.
///
/// Looks for patterns like:
/// - `n.id = 5` (integer)
/// - `n.name = 'Alice'` (string)
/// - `a.id = 5 AND b.name = 'Bob'` (combined)
pub fn extract_variable_constraints(predicate: &Expression) -> VariableConstraints {
    let mut constraints = VariableConstraints::new();
    extract_constraints_recursive(predicate, &mut constraints);
    constraints
}

/// Recursively extract constraints from AND-combined predicates.
fn extract_constraints_recursive(expr: &Expression, constraints: &mut VariableConstraints) {
    match expr {
        // Handle AND: recurse into both sides
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract_constraints_recursive(left, constraints);
            extract_constraints_recursive(right, constraints);
        }

        // Handle equality: var.prop = value
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            // Try both orderings: `var.id = 5` and `5 = var.id`
            if let Some((var, prop, value)) = extract_property_equals(left, right) {
                add_property_constraint(var, prop, value, constraints);
            } else if let Some((var, prop, value)) = extract_property_equals(right, left) {
                add_property_constraint(var, prop, value, constraints);
            }
        }

        _ => {}
    }
}

/// Extract (variable, property, value) from `var.prop = literal`.
fn extract_property_equals<'a>(
    prop_expr: &'a Expression,
    value_expr: &'a Expression,
) -> Option<(&'a str, &'a str, PropertyValue)> {
    // Check if left side is a property access
    if let Expression::Property { base, property } = prop_expr {
        if let Expression::Variable(var) = base.as_ref() {
            // Check if right side is a literal
            match value_expr {
                Expression::Literal(Literal::Integer(val)) => {
                    return Some((
                        var.as_str(),
                        property.as_str(),
                        PropertyValue::Integer(*val),
                    ));
                }
                Expression::Literal(Literal::String(val)) => {
                    return Some((
                        var.as_str(),
                        property.as_str(),
                        PropertyValue::String(val.clone()),
                    ));
                }
                Expression::Literal(Literal::Boolean(val)) => {
                    return Some((var.as_str(), property.as_str(), PropertyValue::Bool(*val)));
                }
                _ => {}
            }
        }
    }
    None
}

/// Add a property constraint for a variable.
fn add_property_constraint(
    var: &str,
    prop: &str,
    value: PropertyValue,
    constraints: &mut VariableConstraints,
) {
    constraints
        .entry(var.to_string())
        .or_default()
        .entry(prop.to_string())
        .or_default()
        .push(value);
}

// =============================================================================
// SQL Pushdown Optimizations
// =============================================================================

/// Try to execute a COUNT query using optimized SQL pushdown.
/// Returns Some(result) if optimization was applied, None otherwise.
fn try_optimized_count(
    match_clause: &MatchClause,
    storage: &SqliteStorage,
) -> Result<Option<QueryResult>> {
    // Only optimize single-node patterns without WHERE clause (for now)
    let pattern = &match_clause.pattern;

    // Must be a single node pattern
    if pattern.elements.len() != 1 {
        return Ok(None);
    }

    let node_pattern = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return Ok(None),
    };

    // Must have a RETURN clause with exactly COUNT(variable)
    let return_clause = match &match_clause.return_clause {
        Some(rc) => rc,
        None => return Ok(None),
    };

    // Only single return item
    if return_clause.items.len() != 1 {
        return Ok(None);
    }

    let item = &return_clause.items[0];

    // Must be COUNT function
    let (fn_name, fn_name_original, args) = match &item.expression {
        Expression::FunctionCall { name, args } => (name.to_uppercase(), name.clone(), args),
        _ => return Ok(None),
    };

    if fn_name != "COUNT" {
        return Ok(None);
    }

    // COUNT argument must be the bound variable or empty (COUNT(*))
    let var_name = node_pattern.variable.as_deref().unwrap_or("_");
    let is_count_var =
        args.is_empty() || matches!(&args[0], Expression::Variable(v) if v == var_name);

    if !is_count_var {
        return Ok(None);
    }

    // No WHERE clause supported yet (could extend later)
    if match_clause.where_clause.is_some() {
        return Ok(None);
    }

    // No property filters in pattern
    if node_pattern.properties.is_some() {
        return Ok(None);
    }

    // Execute optimized count
    let count = if node_pattern.labels.is_empty() {
        // COUNT all nodes
        storage.count_nodes()?
    } else if node_pattern.labels.len() == 1 && node_pattern.labels[0].len() == 1 {
        // COUNT nodes with single label (most common case)
        let label = &node_pattern.labels[0][0];
        storage.count_nodes_by_label(label)?
    } else {
        // Complex label expression - fall back to general path
        return Ok(None);
    };

    // Build result
    let column_name = if let Some(ref alias) = item.alias {
        alias.clone()
    } else {
        format!("{}({})", fn_name_original, var_name)
    };

    let mut values = HashMap::new();
    values.insert(
        column_name.clone(),
        ResultValue::Property(PropertyValue::Integer(count as i64)),
    );

    Ok(Some(QueryResult {
        columns: vec![column_name],
        rows: vec![Row { values }],
        stats: QueryStats::default(),
    }))
}

/// Check if LIMIT can be pushed down to SQL.
/// Returns the limit value if pushable, None otherwise.
fn get_pushable_limit(match_clause: &MatchClause) -> Option<u64> {
    // Must be a single node pattern
    let pattern = &match_clause.pattern;
    if pattern.elements.len() != 1 {
        return None;
    }

    let node_pattern = match &pattern.elements[0] {
        PatternElement::Node(np) => np,
        _ => return None,
    };

    // No property filters (would need to be evaluated after fetch)
    if node_pattern.properties.is_some() {
        return None;
    }

    // No WHERE clause (would need to be evaluated after fetch)
    if match_clause.where_clause.is_some() {
        return None;
    }

    // Must have a RETURN clause with LIMIT
    let return_clause = match &match_clause.return_clause {
        Some(rc) => rc,
        None => return None,
    };

    // No aggregates (COUNT is handled separately)
    if has_aggregate_functions(return_clause) {
        return None;
    }

    // Must have LIMIT and no SKIP (SKIP complicates pushdown)
    if return_clause.skip.is_some() {
        return None;
    }

    return_clause.limit
}

// =============================================================================
// Main Execution Entry Point
// =============================================================================

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

/// Execute a MATCH statement.
fn execute_match(
    match_clause: &MatchClause,
    storage: &SqliteStorage,
    stats: &mut QueryStats,
) -> Result<QueryResult> {
    let pattern = &match_clause.pattern;

    // Try optimized paths for simple queries that can be pushed to SQL
    if let Some(result) = try_optimized_count(match_clause, storage)? {
        return Ok(result);
    }

    // Check if we can push LIMIT to SQL for simple single-node queries
    let pushdown_limit = get_pushable_limit(match_clause);

    // Determine pattern type and execute accordingly
    let bindings = if is_shortest_path_pattern(pattern) {
        // Shortest path pattern: MATCH p = SHORTEST k (a)-[:TYPE]-+(b)
        // Extract constraints from WHERE clause for predicate pushdown
        let constraints = if let Some(ref where_clause) = match_clause.where_clause {
            // Get source/target variable names from pattern
            let (source_var, target_var) = get_path_endpoint_vars(pattern);
            // Extract generic variable constraints, then specialize for path endpoints
            let var_constraints = extract_variable_constraints(&where_clause.predicate);
            PathConstraints::from_variable_constraints(var_constraints, &source_var, &target_var)
        } else {
            PathConstraints::default()
        };
        execute_shortest_path_pattern(pattern, storage, &constraints)?
    } else if is_single_node_pattern(pattern) {
        // Simple single-node pattern: MATCH (n) or MATCH (n:Label)
        execute_single_node_pattern(pattern, storage, pushdown_limit)?
    } else if is_single_hop_pattern(pattern) {
        // Single-hop relationship pattern: MATCH (a)-[r]->(b)
        execute_single_hop_pattern(pattern, storage)?
    } else if is_variable_length_pattern(pattern) {
        // Variable-length pattern: MATCH (a)-[*1..3]->(b)
        execute_variable_length_pattern(pattern, storage)?
    } else if is_multi_hop_pattern(pattern) {
        // Multi-hop pattern: MATCH (a)-[r1]->(b)-[r2]->(c)
        execute_multi_hop_pattern(pattern, storage)?
    } else {
        return Err(Error::Cypher("Unsupported pattern type".into()));
    };

    // Filter by WHERE clause if present
    let mut bindings = if let Some(ref where_clause) = match_clause.where_clause {
        filter_bindings_by_where(bindings, &where_clause.predicate)?
    } else {
        bindings
    };

    // Apply SHORTEST k limit after WHERE clause filtering
    if let Some(k) = pattern.shortest_k {
        bindings.truncate(k as usize);
    }

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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse;
    use tempfile::tempdir;

    fn create_test_storage() -> (SqliteStorage, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::open(&db_path).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_create_single_node() {
        let (storage, _dir) = create_test_storage();

        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.labels_added, 1);
        assert_eq!(result.stats.properties_set, 2);

        // Verify the node was created
        let nodes = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
        assert_eq!(nodes[0].get("age"), Some(&PropertyValue::Integer(30)));
    }

    #[test]
    fn test_create_node_with_relationship() {
        let (storage, _dir) = create_test_storage();

        let stmt = parse(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        )
        .unwrap();
        let result = execute(&stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        // Verify nodes
        let nodes = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(nodes.len(), 2);

        // Verify relationship
        let edges = storage.find_edges_by_type("KNOWS").unwrap();
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_match_single_node() {
        let (storage, _dir) = create_test_storage();

        // Create some nodes
        let create_stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        let create_stmt = parse("CREATE (n:Person {name: 'Bob'})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Match all Person nodes
        let match_stmt = parse("MATCH (n:Person) RETURN n.name").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_match_with_where() {
        let (storage, _dir) = create_test_storage();

        // Create some nodes
        let create_stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        let create_stmt = parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Match with WHERE filter
        let match_stmt = parse("MATCH (n:Person) WHERE n.age > 28 RETURN n.name").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_relationship() {
        let (storage, _dir) = create_test_storage();

        // Create nodes and relationship
        let create_stmt =
            parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Match the relationship
        let match_stmt =
            parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_count_aggregate() {
        let (storage, _dir) = create_test_storage();

        // Create some nodes
        for i in 0..5 {
            let stmt = parse(&format!("CREATE (n:Person {{name: 'Person{}'}})", i)).unwrap();
            execute(&stmt, &storage).unwrap();
        }

        // Count nodes
        let match_stmt = parse("MATCH (n:Person) RETURN count(n)").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0].values.get("count(n)").unwrap();
        assert!(matches!(
            count,
            ResultValue::Property(PropertyValue::Integer(5))
        ));
    }

    #[test]
    fn test_match_with_limit() {
        let (storage, _dir) = create_test_storage();

        // Create nodes
        for i in 0..10 {
            let stmt = parse(&format!("CREATE (n:Person {{name: 'Person{}'}})", i)).unwrap();
            execute(&stmt, &storage).unwrap();
        }

        // Match with limit
        let match_stmt = parse("MATCH (n:Person) RETURN n.name LIMIT 3").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_set_property() {
        let (storage, _dir) = create_test_storage();

        // Create a node
        let create_stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Update the property
        let update_stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31").unwrap();
        let result = execute(&update_stmt, &storage).unwrap();

        assert_eq!(result.stats.properties_set, 1);

        // Verify the update
        let nodes = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(nodes[0].get("age"), Some(&PropertyValue::Integer(31)));
    }

    #[test]
    fn test_delete_node() {
        let (storage, _dir) = create_test_storage();

        // Create a node
        let create_stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Delete the node
        let delete_stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' DETACH DELETE n").unwrap();
        let result = execute(&delete_stmt, &storage).unwrap();

        assert_eq!(result.stats.nodes_deleted, 1);

        // Verify deletion
        let nodes = storage.find_nodes_by_label("Person").unwrap();
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_variable_length_path() {
        let (storage, _dir) = create_test_storage();

        // Create a chain: A -> B -> C -> D
        let create_stmt = parse(
            "CREATE (a:Node {name: 'A'})-[:NEXT]->(b:Node {name: 'B'})-[:NEXT]->(c:Node {name: 'C'})-[:NEXT]->(d:Node {name: 'D'})",
        )
        .unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Find paths of length 2-3
        let match_stmt =
            parse("MATCH (a:Node {name: 'A'})-[*2..3]->(b:Node) RETURN b.name").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        // Should find C (2 hops) and D (3 hops)
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_multi_hop_pattern() {
        let (storage, _dir) = create_test_storage();

        // Create: Alice -KNOWS-> Bob -WORKS_AT-> Acme
        let create_stmt = parse(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:WORKS_AT]->(c:Company {name: 'Acme'})",
        )
        .unwrap();
        execute(&create_stmt, &storage).unwrap();

        // Match the two-hop pattern
        let match_stmt = parse(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name",
        )
        .unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_collect_aggregate() {
        let (storage, _dir) = create_test_storage();

        // Create nodes
        for name in ["Alice", "Bob", "Charlie"] {
            let stmt = parse(&format!("CREATE (n:Person {{name: '{}'}})", name)).unwrap();
            execute(&stmt, &storage).unwrap();
        }

        // Collect names
        let match_stmt = parse("MATCH (n:Person) RETURN collect(n.name)").unwrap();
        let result = execute(&match_stmt, &storage).unwrap();

        assert_eq!(result.rows.len(), 1);
        let collected = result.rows[0].values.get("collect(n.name)").unwrap();
        if let ResultValue::Property(PropertyValue::List(names)) = collected {
            assert_eq!(names.len(), 3);
        } else {
            panic!("Expected list");
        }
    }
}
