//! Query planner - converts AST to execution plan.
//!
//! The planner takes a parsed Cypher AST and produces an optimized
//! execution plan that can be run by the executor.

use crate::error::Result;
use super::parser::Statement;

/// A query execution plan.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Root operator of the plan.
    pub root: PlanOperator,
}

/// Plan operators.
#[derive(Debug, Clone)]
pub enum PlanOperator {
    /// Scan all nodes with optional label filter.
    NodeScan {
        variable: String,
        labels: Vec<String>,
    },
    /// Scan all edges with optional type filter.
    EdgeScan {
        variable: String,
        types: Vec<String>,
    },
    /// Expand from a node along relationships.
    Expand {
        source: Box<PlanOperator>,
        rel_variable: Option<String>,
        target_variable: String,
        types: Vec<String>,
        direction: ExpandDirection,
        min_hops: u32,
        max_hops: u32,
    },
    /// Filter rows.
    Filter {
        source: Box<PlanOperator>,
        predicate: FilterPredicate,
    },
    /// Project columns.
    Project {
        source: Box<PlanOperator>,
        columns: Vec<ProjectColumn>,
    },
    /// Sort rows.
    Sort {
        source: Box<PlanOperator>,
        keys: Vec<SortKey>,
    },
    /// Limit number of rows.
    Limit {
        source: Box<PlanOperator>,
        count: u64,
    },
    /// Skip rows.
    Skip {
        source: Box<PlanOperator>,
        count: u64,
    },
    /// Create nodes/edges.
    Create {
        source: Option<Box<PlanOperator>>,
        nodes: Vec<CreateNode>,
        edges: Vec<CreateEdge>,
    },
    /// Delete nodes/edges.
    Delete {
        source: Box<PlanOperator>,
        variables: Vec<String>,
        detach: bool,
    },
    /// Set properties/labels.
    SetProperties {
        source: Box<PlanOperator>,
        sets: Vec<SetOperation>,
    },
    /// Empty result.
    Empty,
}

/// Expand direction for traversal.
#[derive(Debug, Clone, Copy)]
pub enum ExpandDirection {
    Outgoing,
    Incoming,
    Both,
}

/// Filter predicate.
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    Eq { left: PlanExpr, right: PlanExpr },
    Ne { left: PlanExpr, right: PlanExpr },
    Lt { left: PlanExpr, right: PlanExpr },
    Le { left: PlanExpr, right: PlanExpr },
    Gt { left: PlanExpr, right: PlanExpr },
    Ge { left: PlanExpr, right: PlanExpr },
    And { left: Box<FilterPredicate>, right: Box<FilterPredicate> },
    Or { left: Box<FilterPredicate>, right: Box<FilterPredicate> },
    Not { inner: Box<FilterPredicate> },
    IsNull { expr: PlanExpr },
    IsNotNull { expr: PlanExpr },
    HasLabel { variable: String, label: String },
}

/// Plan expression.
#[derive(Debug, Clone)]
pub enum PlanExpr {
    Literal(PlanLiteral),
    Variable(String),
    Property { variable: String, property: String },
    Function { name: String, args: Vec<PlanExpr> },
}

/// Plan literal value.
#[derive(Debug, Clone)]
pub enum PlanLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

/// Column projection.
#[derive(Debug, Clone)]
pub struct ProjectColumn {
    pub expr: PlanExpr,
    pub alias: String,
}

/// Sort key.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: PlanExpr,
    pub descending: bool,
}

/// Node creation specification.
#[derive(Debug, Clone)]
pub struct CreateNode {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Edge creation specification.
#[derive(Debug, Clone)]
pub struct CreateEdge {
    pub variable: Option<String>,
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Set operation.
#[derive(Debug, Clone)]
pub enum SetOperation {
    Property { variable: String, property: String, value: PlanExpr },
    AddLabel { variable: String, label: String },
    RemoveLabel { variable: String, label: String },
}

/// Plan a parsed statement.
pub fn plan(statement: &Statement) -> Result<QueryPlan> {
    // TODO: Implement query planner
    let _ = statement;
    Ok(QueryPlan {
        root: PlanOperator::Empty,
    })
}
