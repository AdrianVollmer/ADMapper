//! Query executor - runs parsed statements against the storage backend.
//!
//! This module now uses the query planner to generate execution plans,
//! which are then interpreted by the plan executor.

mod aggregate;
pub mod algorithms;
mod create;
mod eval;
mod mutation;
mod pattern;
mod plan_exec;
mod result;

use super::parser::Statement;
use super::planner;
use super::QueryResult;
use crate::error::Result;
use crate::graph::{Edge, Node, PropertyValue};
use crate::storage::SqliteStorage;
use smallvec::SmallVec;
use std::collections::HashMap;

// Re-exports for submodules (some kept for backwards compatibility)
#[allow(unused_imports)]
pub use aggregate::{evaluate_aggregate, has_aggregate_functions, is_aggregate_function};
#[allow(unused_imports)]
pub use create::{execute_create, literal_to_json};
#[allow(unused_imports)]
pub use eval::{
    evaluate_expression_with_bindings, evaluate_function_call_with_bindings,
    filter_bindings_by_where, literal_to_property_value,
};
#[allow(unused_imports)]
pub use mutation::{execute_delete, execute_set};
#[allow(unused_imports)]
pub use pattern::{
    execute_multi_hop_pattern, execute_shortest_path_pattern, execute_single_hop_pattern,
    execute_single_node_pattern, execute_variable_length_pattern, get_path_endpoint_vars,
    is_multi_hop_pattern, is_shortest_path_pattern, is_single_hop_pattern, is_single_node_pattern,
    is_variable_length_pattern,
};
#[allow(unused_imports)]
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
///
/// Uses SmallVec for cache-friendly storage. Most queries have 1-5 variables,
/// so linear search beats HashMap hashing overhead.
#[derive(Debug, Clone)]
pub struct Binding {
    nodes: SmallVec<[(String, Node); 4]>,
    edges: SmallVec<[(String, Edge); 2]>,
    /// Paths bound to variables (for `p = (a)-[*]->(b)` syntax).
    paths: SmallVec<[(String, Path); 1]>,
    /// Edge lists for variable-length relationship bindings.
    edge_lists: SmallVec<[(String, Vec<Edge>); 1]>,
}

impl Binding {
    pub fn new() -> Self {
        Binding {
            nodes: SmallVec::new(),
            edges: SmallVec::new(),
            paths: SmallVec::new(),
            edge_lists: SmallVec::new(),
        }
    }

    /// Look up a node by variable name.
    #[inline]
    pub fn get_node(&self, name: &str) -> Option<&Node> {
        self.nodes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, node)| node)
    }

    /// Look up an edge by variable name.
    #[inline]
    pub fn get_edge(&self, name: &str) -> Option<&Edge> {
        self.edges
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, edge)| edge)
    }

    /// Look up a path by variable name.
    #[inline]
    pub fn get_path(&self, name: &str) -> Option<&Path> {
        self.paths
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, path)| path)
    }

    /// Look up an edge list by variable name.
    #[inline]
    pub fn get_edge_list(&self, name: &str) -> Option<&Vec<Edge>> {
        self.edge_lists
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, edges)| edges)
    }

    /// Check if a node variable exists.
    #[inline]
    pub fn has_node(&self, name: &str) -> bool {
        self.nodes.iter().any(|(n, _)| n == name)
    }

    /// Check if an edge variable exists.
    #[inline]
    pub fn has_edge(&self, name: &str) -> bool {
        self.edges.iter().any(|(n, _)| n == name)
    }

    pub fn with_node(mut self, var: &str, node: Node) -> Self {
        self.nodes.push((var.to_string(), node));
        self
    }

    pub fn with_edge(mut self, var: &str, edge: Edge) -> Self {
        self.edges.push((var.to_string(), edge));
        self
    }

    pub fn with_path(mut self, var: &str, path: Path) -> Self {
        self.paths.push((var.to_string(), path));
        self
    }

    pub fn with_edge_list(mut self, var: &str, edges: Vec<Edge>) -> Self {
        self.edge_lists.push((var.to_string(), edges));
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

// =============================================================================
// Main Execution Entry Point
// =============================================================================

/// Execute a parsed statement against the storage.
///
/// This uses the query planner to generate an execution plan,
/// which is then interpreted by the plan executor.
pub fn execute(statement: &Statement, storage: &SqliteStorage) -> Result<QueryResult> {
    // Generate query plan from AST
    let plan = planner::plan(statement)?;

    // Apply optimization passes
    let optimized_plan = planner::optimize(plan);

    // Execute the plan
    plan_exec::execute_plan(&optimized_plan, storage)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse;
    use crate::query::ResultValue;
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
