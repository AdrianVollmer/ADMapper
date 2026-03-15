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
use super::{QueryResult, QueryStats, ResultValue, Row};
use crate::error::{Error, Result};
use crate::graph::{Node, PropertyValue, Relationship};
use crate::storage::{EntityCache, SqliteStorage};
use smallvec::SmallVec;
use std::collections::HashMap;
use tracing::{debug, trace};

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

/// A path through the graph (sequence of nodes and relationships with full data).
#[derive(Debug, Clone)]
pub struct Path {
    pub nodes: Vec<Node>,
    pub relationships: Vec<Relationship>,
}

/// A binding represents a matched graph element (node or relationship) with its variable name.
///
/// Uses SmallVec for cache-friendly storage. Most queries have 1-5 variables,
/// so linear search beats HashMap hashing overhead.
#[derive(Debug, Clone)]
pub struct Binding {
    nodes: SmallVec<[(String, Node); 4]>,
    relationships: SmallVec<[(String, Relationship); 2]>,
    /// Paths bound to variables (for `p = (a)-[*]->(b)` syntax).
    paths: SmallVec<[(String, Path); 1]>,
    /// Relationship lists for variable-length relationship bindings.
    relationship_lists: SmallVec<[(String, Vec<Relationship>); 1]>,
    /// Scalar values from WITH projections (e.g., `WITH n.name AS name`).
    scalars: SmallVec<[(String, PropertyValue); 4]>,
}

impl Binding {
    pub fn new() -> Self {
        Binding {
            nodes: SmallVec::new(),
            relationships: SmallVec::new(),
            paths: SmallVec::new(),
            relationship_lists: SmallVec::new(),
            scalars: SmallVec::new(),
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

    /// Look up a relationship by variable name.
    #[inline]
    pub fn get_relationship(&self, name: &str) -> Option<&Relationship> {
        self.relationships
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, relationship)| relationship)
    }

    /// Look up a path by variable name.
    #[inline]
    pub fn get_path(&self, name: &str) -> Option<&Path> {
        self.paths
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, path)| path)
    }

    /// Look up a relationship list by variable name.
    #[inline]
    pub fn get_relationship_list(&self, name: &str) -> Option<&Vec<Relationship>> {
        self.relationship_lists
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, relationships)| relationships)
    }

    /// Look up a scalar value by variable name.
    #[inline]
    pub fn get_scalar(&self, name: &str) -> Option<&PropertyValue> {
        self.scalars
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, val)| val)
    }

    /// Iterate over all bound node variables and their nodes.
    #[inline]
    pub fn nodes(&self) -> impl Iterator<Item = &(String, Node)> {
        self.nodes.iter()
    }

    /// Check if a node variable exists.
    #[inline]
    pub fn has_node(&self, name: &str) -> bool {
        self.nodes.iter().any(|(n, _)| n == name)
    }

    /// Check if a relationship variable exists.
    #[inline]
    pub fn has_relationship(&self, name: &str) -> bool {
        self.relationships.iter().any(|(n, _)| n == name)
    }

    pub fn with_node(mut self, var: &str, node: Node) -> Self {
        self.nodes.push((var.to_string(), node));
        self
    }

    pub fn with_relationship(mut self, var: &str, relationship: Relationship) -> Self {
        self.relationships.push((var.to_string(), relationship));
        self
    }

    pub fn with_path(mut self, var: &str, path: Path) -> Self {
        self.paths.push((var.to_string(), path));
        self
    }

    pub fn with_relationship_list(mut self, var: &str, relationships: Vec<Relationship>) -> Self {
        self.relationship_lists
            .push((var.to_string(), relationships));
        self
    }

    pub fn with_scalar(mut self, var: &str, value: PropertyValue) -> Self {
        self.scalars.push((var.to_string(), value));
        self
    }

    /// Merge two bindings together (cross join).
    pub fn merge(&self, other: &Binding) -> Binding {
        let mut result = self.clone();
        result.nodes.extend(other.nodes.iter().cloned());
        result
            .relationships
            .extend(other.relationships.iter().cloned());
        result.paths.extend(other.paths.iter().cloned());
        result
            .relationship_lists
            .extend(other.relationship_lists.iter().cloned());
        result.scalars.extend(other.scalars.iter().cloned());
        result
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
    execute_with_cache(statement, storage, None, None)
}

/// Execute a parsed statement with an optional entity cache and binding limit.
///
/// The cache improves performance for BFS/DFS traversals by avoiding repeated
/// SQLite lookups for the same nodes and relationships.
///
/// `max_bindings` limits the number of intermediate bindings to prevent OOM
/// on queries that produce explosive results (cross joins, deep BFS, etc.).
/// `None` means unlimited.
pub fn execute_with_cache(
    statement: &Statement,
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
    max_bindings: Option<usize>,
) -> Result<QueryResult> {
    // Handle UNION/UNION ALL at the statement level: execute each branch and concatenate
    if let Statement::UnionAll(queries) = statement {
        return execute_union_all(queries, storage, cache, max_bindings);
    }
    if let Statement::Union(queries) = statement {
        return execute_union(queries, storage, cache, max_bindings);
    }
    if let Statement::Pipeline {
        stages,
        final_query,
    } = statement
    {
        return execute_pipeline(stages, final_query, storage, cache, max_bindings);
    }

    let t0 = std::time::Instant::now();

    // Generate query plan from AST
    let plan = planner::plan(statement)?;
    let plan_ms = t0.elapsed().as_micros();

    // Apply optimization passes
    let optimized_plan = planner::optimize(plan);
    let opt_ms = t0.elapsed().as_micros();

    debug!(
        "query plan: {:?} (plan: {}us, optimize: {}us)",
        optimized_plan.root.variant_name(),
        plan_ms,
        opt_ms - plan_ms
    );
    trace!("full plan: {:?}", optimized_plan.root);

    // Execute the plan
    let result = plan_exec::execute_plan(&optimized_plan, storage, cache, max_bindings)?;
    let exec_ms = t0.elapsed().as_micros();

    debug!(
        "query executed: {} rows in {}us (plan: {}us, opt: {}us, exec: {}us)",
        result.rows.len(),
        exec_ms,
        plan_ms,
        opt_ms - plan_ms,
        exec_ms - opt_ms
    );

    Ok(result)
}

/// Execute a multi-part query with WITH clauses.
///
/// Each WITH stage: execute MATCH -> project WITH items into new bindings -> filter post-WHERE.
/// The final query's RETURN projects from the accumulated bindings.
fn execute_pipeline(
    stages: &[super::ast::WithStage],
    final_query: &Statement,
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
    max_bindings: Option<usize>,
) -> Result<QueryResult> {
    let t0 = std::time::Instant::now();
    let mut current_bindings: Option<Vec<Binding>> = None;

    for stage in stages {
        // Step 1: Execute the MATCH clause of this stage (if present)
        let stage_bindings = if let Some(ref match_clause) = stage.match_clause {
            // Plan just the MATCH (no RETURN)
            let match_stmt = Statement::Match(super::ast::MatchClause {
                pattern: match_clause.pattern.clone(),
                where_clause: match_clause.where_clause.clone(),
                return_clause: None,
                delete_clause: None,
                set_clause: None,
                create_clause: None,
            });
            let plan = planner::plan(&match_stmt)?;
            let optimized = planner::optimize(plan);
            let mut ctx = plan_exec::ExecutionContext::new(max_bindings);
            let bindings = plan_exec::execute_plan_bindings(
                &optimized,
                storage,
                &mut ctx,
                cache.as_deref_mut(),
            )?;

            // Cross-join with previous bindings if any
            if let Some(prev) = &current_bindings {
                let mut joined = Vec::new();
                for pb in prev {
                    for nb in &bindings {
                        joined.push(pb.merge(nb));
                    }
                }
                joined
            } else {
                bindings
            }
        } else if let Some(prev) = current_bindings.take() {
            prev
        } else {
            vec![Binding::new()]
        };

        // Step 2: Project WITH items into new bindings
        let with_return = &stage.with_clause;
        let has_aggregates = with_return
            .items
            .iter()
            .any(|item| planner::expression::is_aggregate_expression(&item.expression));

        let projected_bindings = if has_aggregates {
            // For aggregation, execute the full aggregate plan and convert rows back to bindings
            let plan = planner::plan_return(planner::PlanOperator::ProduceRow, with_return)?;
            let optimized = planner::optimize(planner::QueryPlan { root: plan });
            let mut ctx = plan_exec::ExecutionContext::new(max_bindings);
            // Feed the stage bindings through the aggregate plan
            let result = plan_exec::execute_plan_on_bindings(
                &optimized,
                stage_bindings,
                storage,
                &mut ctx,
                cache.as_deref_mut(),
            )?;
            rows_to_bindings(&result)
        } else {
            project_with_bindings(&stage_bindings, with_return)?
        };

        // Step 3: Apply post-WHERE filter
        let filtered_bindings = if let Some(ref where_expr) = stage.post_where {
            let pred = planner::expression::plan_expression_as_predicate(where_expr)?;
            let mut result = Vec::new();
            for binding in projected_bindings {
                if plan_exec::filter::evaluate_predicate_on(&pred, &binding)? {
                    result.push(binding);
                }
            }
            result
        } else {
            projected_bindings
        };

        current_bindings = Some(filtered_bindings);
    }

    // Execute final query: project bindings into result rows
    let bindings = current_bindings.unwrap_or_default();

    // Extract the RETURN clause from the final query
    let return_clause = match final_query {
        Statement::Return(rc) => rc,
        Statement::Match(mc) => mc
            .return_clause
            .as_ref()
            .ok_or_else(|| Error::Internal("Pipeline final MATCH requires RETURN".into()))?,
        _ => {
            return Err(Error::Internal(
                "Pipeline final query must be RETURN or MATCH...RETURN".into(),
            ))
        }
    };

    // For the final stage, if there's a MATCH, we need to execute it with the bindings
    // For now, handle the simple case: just RETURN (no additional MATCH)
    let final_bindings = if let Statement::Match(mc) = final_query {
        // Execute the final MATCH for each binding
        let match_stmt = Statement::Match(super::ast::MatchClause {
            pattern: mc.pattern.clone(),
            where_clause: mc.where_clause.clone(),
            return_clause: None,
            delete_clause: None,
            set_clause: None,
            create_clause: None,
        });
        let plan = planner::plan(&match_stmt)?;
        let optimized = planner::optimize(plan);
        let mut ctx = plan_exec::ExecutionContext::new(max_bindings);
        let new_bindings =
            plan_exec::execute_plan_bindings(&optimized, storage, &mut ctx, cache.as_deref_mut())?;
        // Cross-join with accumulated bindings
        let mut joined = Vec::new();
        for pb in &bindings {
            for nb in &new_bindings {
                joined.push(pb.merge(nb));
            }
        }
        joined
    } else {
        bindings
    };

    // Project the final RETURN
    let plan = planner::plan_return(planner::PlanOperator::ProduceRow, return_clause)?;
    let optimized = planner::optimize(planner::QueryPlan { root: plan });
    let mut ctx = plan_exec::ExecutionContext::new(max_bindings);
    let mut result = plan_exec::execute_plan_on_bindings(
        &optimized,
        final_bindings,
        storage,
        &mut ctx,
        cache.as_deref_mut(),
    )?;
    result.stats.execution_time_ms = t0.elapsed().as_millis() as u64;

    debug!(
        "Pipeline executed: {} stages, {} rows in {}us",
        stages.len(),
        result.rows.len(),
        t0.elapsed().as_micros()
    );

    Ok(result)
}

/// Convert result rows back to bindings with scalar values.
fn rows_to_bindings(result: &QueryResult) -> Vec<Binding> {
    result
        .rows
        .iter()
        .map(|row| {
            let mut binding = Binding::new();
            for (col, val) in &row.values {
                match val {
                    ResultValue::Node {
                        id,
                        labels,
                        properties,
                    } => {
                        binding = binding.with_node(
                            col,
                            Node {
                                id: *id,
                                labels: labels.clone(),
                                properties: properties.clone(),
                            },
                        );
                    }
                    ResultValue::Property(pv) => {
                        binding = binding.with_scalar(col, pv.clone());
                    }
                    _ => {}
                }
            }
            binding
        })
        .collect()
}

/// Project WITH items into new bindings (non-aggregate case).
fn project_with_bindings(
    bindings: &[Binding],
    with_clause: &super::ast::ReturnClause,
) -> Result<Vec<Binding>> {
    use super::ast::Expression;

    let mut result = Vec::new();
    let mut seen = if with_clause.distinct {
        Some(std::collections::HashSet::new())
    } else {
        None
    };

    for binding in bindings {
        let mut new_binding = Binding::new();

        for item in &with_clause.items {
            let alias = item
                .alias
                .clone()
                .unwrap_or_else(|| planner::expression::format_expression(&item.expression));

            match &item.expression {
                Expression::Variable(var) => {
                    if let Some(node) = binding.get_node(var) {
                        new_binding = new_binding.with_node(&alias, node.clone());
                    } else if let Some(rel) = binding.get_relationship(var) {
                        new_binding = new_binding.with_relationship(&alias, rel.clone());
                    } else if let Some(path) = binding.get_path(var) {
                        new_binding = new_binding.with_path(&alias, path.clone());
                    } else if let Some(scalar) = binding.get_scalar(var) {
                        new_binding = new_binding.with_scalar(&alias, scalar.clone());
                    }
                }
                _ => {
                    let plan_expr = planner::expression::plan_expression(&item.expression)?;
                    let val = plan_exec::eval::evaluate_expr_pub(&plan_expr, binding)?;
                    new_binding = new_binding.with_scalar(&alias, val);
                }
            }
        }

        if let Some(ref mut seen_set) = seen {
            let key = format!("{:?}", new_binding.scalars);
            if !seen_set.insert(key) {
                continue;
            }
        }

        result.push(new_binding);
    }

    Ok(result)
}

/// Execute a UNION query: run each branch, concatenate, then deduplicate.
fn execute_union(
    queries: &[Statement],
    storage: &SqliteStorage,
    cache: Option<&mut EntityCache>,
    max_bindings: Option<usize>,
) -> Result<QueryResult> {
    let mut result = execute_union_all(queries, storage, cache, max_bindings)?;

    // Deduplicate rows by serializing each row to a comparable key.
    // We use the Debug representation of the sorted key-value pairs.
    let mut seen = std::collections::HashSet::new();
    result.rows.retain(|row| {
        let mut pairs: Vec<_> = row.values.iter().collect();
        pairs.sort_by_key(|(k, _)| (*k).clone());
        let key = format!("{:?}", pairs);
        seen.insert(key)
    });

    Ok(result)
}

/// Execute a UNION ALL query: run each branch and concatenate the results.
fn execute_union_all(
    queries: &[Statement],
    storage: &SqliteStorage,
    mut cache: Option<&mut EntityCache>,
    max_bindings: Option<usize>,
) -> Result<QueryResult> {
    let t0 = std::time::Instant::now();
    let mut combined_columns: Option<Vec<String>> = None;
    let mut combined_rows: Vec<Row> = Vec::new();
    let mut combined_stats = QueryStats::default();

    for query in queries {
        let result = execute_with_cache(query, storage, cache.as_deref_mut(), max_bindings)?;

        match &combined_columns {
            None => {
                combined_columns = Some(result.columns.clone());
            }
            Some(cols) => {
                if cols.len() != result.columns.len() {
                    return Err(Error::Cypher(format!(
                        "All sub queries in an UNION must have the same return column names \
                         (expected {}, got {})",
                        cols.len(),
                        result.columns.len()
                    )));
                }
                if cols != &result.columns {
                    return Err(Error::Cypher(format!(
                        "All sub queries in an UNION must have the same return column names \
                         (expected [{}], got [{}])",
                        cols.join(", "),
                        result.columns.join(", ")
                    )));
                }
            }
        }

        combined_rows.extend(result.rows);
        combined_stats.nodes_created += result.stats.nodes_created;
        combined_stats.nodes_deleted += result.stats.nodes_deleted;
        combined_stats.relationships_created += result.stats.relationships_created;
        combined_stats.relationships_deleted += result.stats.relationships_deleted;
        combined_stats.properties_set += result.stats.properties_set;
        combined_stats.labels_added += result.stats.labels_added;
    }

    debug!(
        "UNION ALL executed: {} branches, {} total rows in {}us",
        queries.len(),
        combined_rows.len(),
        t0.elapsed().as_micros()
    );

    Ok(QueryResult {
        columns: combined_columns.unwrap_or_default(),
        rows: combined_rows,
        stats: combined_stats,
    })
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
        let relationships = storage.find_relationships_by_type("KNOWS").unwrap();
        assert_eq!(relationships.len(), 1);
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

    #[test]
    fn test_standalone_return() {
        let (storage, _dir) = create_test_storage();

        // Test RETURN with literal integer
        let stmt = parse("RETURN 1").unwrap();
        let result = execute(&stmt, &storage).unwrap();
        assert_eq!(result.rows.len(), 1);
        match result.rows[0].values.get("1") {
            Some(ResultValue::Property(PropertyValue::Integer(1))) => {}
            other => panic!("Expected integer 1, got {:?}", other),
        }

        // Test RETURN with literal string
        let stmt = parse("RETURN 'hello'").unwrap();
        let result = execute(&stmt, &storage).unwrap();
        assert_eq!(result.rows.len(), 1);
        match result.rows[0].values.get("'hello'") {
            Some(ResultValue::Property(PropertyValue::String(s))) if s == "hello" => {}
            other => panic!("Expected string 'hello', got {:?}", other),
        }

        // Test RETURN with alias
        let stmt = parse("RETURN 42 AS answer").unwrap();
        let result = execute(&stmt, &storage).unwrap();
        assert_eq!(result.rows.len(), 1);
        match result.rows[0].values.get("answer") {
            Some(ResultValue::Property(PropertyValue::Integer(42))) => {}
            other => panic!("Expected integer 42, got {:?}", other),
        }

        // Test RETURN with multiple items
        let stmt = parse("RETURN 1, 'test'").unwrap();
        let result = execute(&stmt, &storage).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].values.contains_key("1"));
        assert!(result.rows[0].values.contains_key("'test'"));
    }
}
