//! Plan operators and related data structures.
//!
//! This module contains all the data types used in query execution plans,
//! including operators, predicates, expressions, and helper structures.

use super::ast::{Direction, ListPredicateKind};

// =============================================================================
// Plan Data Structures
// =============================================================================

/// A query execution plan.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Root operator of the plan.
    pub root: PlanOperator,
}

/// Parameters for expanding from a node along relationships.
#[derive(Debug, Clone)]
pub struct ExpandParams {
    pub source: Box<PlanOperator>,
    pub source_variable: String,
    pub rel_variable: Option<String>,
    pub target_variable: String,
    pub target_labels: Vec<String>,
    pub path_variable: Option<String>,
    pub types: Vec<String>,
    pub direction: ExpandDirection,
    /// Limit for early termination (pushed down from RETURN ... LIMIT).
    pub limit: Option<u64>,
}

/// Parameters for variable-length expand (BFS).
#[derive(Debug, Clone)]
pub struct VarLenExpandParams {
    pub source: Box<PlanOperator>,
    pub source_variable: String,
    pub rel_variable: Option<String>,
    pub target_variable: String,
    pub target_labels: Vec<String>,
    pub path_variable: Option<String>,
    pub types: Vec<String>,
    pub direction: ExpandDirection,
    pub min_hops: u32,
    pub max_hops: u32,
    /// Pre-resolved target node IDs for early termination.
    /// When set, BFS only reports paths to these specific nodes.
    pub target_ids: Option<Vec<i64>>,
    /// Limit for early termination (pushed down from RETURN ... LIMIT).
    pub limit: Option<u64>,
    /// Target property filter for early termination (e.g., objectid ENDS WITH '-519').
    /// Format: (property_name, operator, value) where operator is "=", "ENDS WITH", etc.
    pub target_property_filter: Option<TargetPropertyFilter>,
}

/// Parameters for shortest path search (BFS with k paths).
#[derive(Debug, Clone)]
pub struct ShortestPathParams {
    pub source: Box<PlanOperator>,
    pub source_variable: String,
    pub target_variable: String,
    pub target_labels: Vec<String>,
    pub path_variable: Option<String>,
    pub types: Vec<String>,
    pub direction: ExpandDirection,
    pub min_hops: u32,
    pub max_hops: u32,
    pub k: u32,
    /// Target property filter for early termination (e.g., {id: 99}).
    /// When set, BFS can terminate as soon as the specific target is found.
    pub target_property_filter: Option<(String, serde_json::Value)>,
}

/// Plan operators form a tree representing the execution strategy.
#[derive(Debug, Clone)]
pub enum PlanOperator {
    /// Scan all nodes with optional label filter.
    /// `label_groups` preserves OR semantics: each inner Vec is OR'd, outer Vec is AND'd.
    /// Example: `:Person|Company` → `[["Person", "Company"]]`
    /// Example: `:Person:Actor|Director` → `[["Person"], ["Actor", "Director"]]`
    NodeScan {
        variable: String,
        label_groups: Vec<Vec<String>>,
        /// SQL-level limit pushdown for simple scans.
        limit: Option<u64>,
        /// Property equality filters pushed down to SQL for indexed lookup.
        /// Format: (property_name, value) - uses property index if available.
        property_filter: Option<(String, serde_json::Value)>,
    },
    /// Scan all relationships with optional type filter.
    #[allow(dead_code)]
    RelationshipScan {
        variable: String,
        types: Vec<String>,
    },
    /// Expand from a node along relationships.
    Expand(ExpandParams),
    /// Variable-length expand (BFS).
    VariableLengthExpand(VarLenExpandParams),
    /// Shortest path search (BFS with k paths).
    ShortestPath(ShortestPathParams),
    /// Filter rows by predicate.
    Filter {
        source: Box<PlanOperator>,
        predicate: FilterPredicate,
    },
    /// Project columns for RETURN clause.
    Project {
        source: Box<PlanOperator>,
        columns: Vec<ProjectColumn>,
        /// Whether this is a RETURN DISTINCT.
        distinct: bool,
    },
    /// Aggregate rows (GROUP BY with aggregation functions).
    Aggregate {
        source: Box<PlanOperator>,
        /// Columns that aren't aggregates (implicit GROUP BY).
        group_by: Vec<ProjectColumn>,
        /// Aggregate functions to compute.
        aggregates: Vec<AggregateColumn>,
    },
    /// SQL pushdown for COUNT(*) or COUNT(n) without grouping.
    CountPushdown {
        label: Option<String>,
        alias: String,
    },
    /// SQL pushdown for COUNT of relationships.
    /// Much faster than expanding all relationships then counting.
    RelationshipCountPushdown {
        /// Optional type filter (e.g., "MemberOf").
        rel_type: Option<String>,
        alias: String,
    },
    /// SQL pushdown for DISTINCT type(r) - returns all relationship types directly.
    /// Much faster than scanning all relationships: O(distinct_types) vs O(relationships).
    RelationshipTypesScan { alias: String },
    /// Sort rows by column values.
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
    /// Create nodes/relationships.
    Create {
        source: Option<Box<PlanOperator>>,
        nodes: Vec<CreateNode>,
        relationships: Vec<CreateRelationship>,
    },
    /// Delete nodes/relationships.
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
    /// Cross join two independent patterns (cartesian product).
    /// Used for comma-separated patterns like `MATCH (a), (b)`.
    CrossJoin {
        left: Box<PlanOperator>,
        right: Box<PlanOperator>,
    },
    /// Empty result (no rows).
    Empty,
    /// Produce a single empty row (for standalone CREATE).
    ProduceRow,
}

impl PlanOperator {
    /// Return a short name for the operator variant (for logging/diagnostics).
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::NodeScan { .. } => "NodeScan",
            Self::RelationshipScan { .. } => "RelationshipScan",
            Self::Expand(_) => "Expand",
            Self::VariableLengthExpand(_) => "VariableLengthExpand",
            Self::ShortestPath(_) => "ShortestPath",
            Self::Filter { .. } => "Filter",
            Self::Project { .. } => "Project",
            Self::Aggregate { .. } => "Aggregate",
            Self::CountPushdown { .. } => "CountPushdown",
            Self::RelationshipCountPushdown { .. } => "RelationshipCountPushdown",
            Self::RelationshipTypesScan { .. } => "RelationshipTypesScan",
            Self::Sort { .. } => "Sort",
            Self::Limit { .. } => "Limit",
            Self::Skip { .. } => "Skip",
            Self::Create { .. } => "Create",
            Self::Delete { .. } => "Delete",
            Self::SetProperties { .. } => "SetProperties",
            Self::CrossJoin { .. } => "CrossJoin",
            Self::Empty => "Empty",
            Self::ProduceRow => "ProduceRow",
        }
    }
}

/// Expand direction for traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpandDirection {
    Outgoing,
    Incoming,
    Both,
}

impl From<Direction> for ExpandDirection {
    fn from(d: Direction) -> Self {
        match d {
            Direction::Outgoing => ExpandDirection::Outgoing,
            Direction::Incoming => ExpandDirection::Incoming,
            Direction::Both => ExpandDirection::Both,
        }
    }
}

/// Target property filter for variable-length path early termination.
/// Used to pre-resolve matching target nodes during BFS.
#[derive(Debug, Clone)]
pub enum TargetPropertyFilter {
    /// Exact property match: property = value
    Eq {
        property: String,
        value: serde_json::Value,
    },
    /// Property ends with suffix: property ENDS WITH 'suffix'
    EndsWith { property: String, suffix: String },
    /// Property starts with prefix: property STARTS WITH 'prefix'
    StartsWith { property: String, prefix: String },
    /// Property contains substring: property CONTAINS 'substring'
    Contains { property: String, substring: String },
}

/// Filter predicate in execution plan.
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    Eq {
        left: PlanExpr,
        right: PlanExpr,
    },
    Ne {
        left: PlanExpr,
        right: PlanExpr,
    },
    Lt {
        left: PlanExpr,
        right: PlanExpr,
    },
    Le {
        left: PlanExpr,
        right: PlanExpr,
    },
    Gt {
        left: PlanExpr,
        right: PlanExpr,
    },
    Ge {
        left: PlanExpr,
        right: PlanExpr,
    },
    And {
        left: Box<FilterPredicate>,
        right: Box<FilterPredicate>,
    },
    Or {
        left: Box<FilterPredicate>,
        right: Box<FilterPredicate>,
    },
    Not {
        inner: Box<FilterPredicate>,
    },
    IsNull {
        expr: PlanExpr,
    },
    IsNotNull {
        expr: PlanExpr,
    },
    StartsWith {
        expr: PlanExpr,
        prefix: String,
    },
    EndsWith {
        expr: PlanExpr,
        suffix: String,
    },
    Contains {
        expr: PlanExpr,
        substring: String,
    },
    Regex {
        expr: PlanExpr,
        pattern: String,
    },
    HasLabel {
        variable: String,
        label: String,
    },
    /// IN list check.
    In {
        expr: PlanExpr,
        list: Vec<PlanExpr>,
    },
    /// List predicate: ALL/ANY/NONE/SINGLE(var IN list WHERE pred).
    ListPredicate {
        kind: ListPredicateKind,
        variable: String,
        list: PlanExpr,
        filter: Option<Box<FilterPredicate>>,
    },
    /// Always true (for optimized-away predicates).
    True,
}

/// Expression in execution plan.
#[derive(Debug, Clone)]
pub enum PlanExpr {
    Literal(PlanLiteral),
    Variable(String),
    Property { variable: String, property: String },
    Function { name: String, args: Vec<PlanExpr> },
    PathLength { path_variable: String },
}

/// Literal value in plan.
#[derive(Debug, Clone)]
pub enum PlanLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

/// Column projection for RETURN.
#[derive(Debug, Clone)]
pub struct ProjectColumn {
    pub expr: PlanExpr,
    pub alias: String,
}

/// Aggregate column.
#[derive(Debug, Clone)]
pub struct AggregateColumn {
    pub function: AggregateFunction,
    pub alias: String,
}

/// Supported aggregate functions.
#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count(Option<PlanExpr>),
    Sum(PlanExpr),
    Avg(PlanExpr),
    Min(PlanExpr),
    Max(PlanExpr),
    Collect(PlanExpr),
}

/// Sort key referencing a projected column by name.
#[derive(Debug, Clone)]
pub struct SortKey {
    /// Column name to sort by (must match a projected alias).
    pub column: String,
    pub descending: bool,
}

/// Node creation specification.
#[derive(Debug, Clone)]
pub struct CreateNode {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Relationship creation specification.
#[derive(Debug, Clone)]
pub struct CreateRelationship {
    pub variable: Option<String>,
    pub source: String,
    pub target: String,
    pub rel_type: String,
    pub properties: Vec<(String, PlanExpr)>,
}

/// Set operation.
#[derive(Debug, Clone)]
pub enum SetOperation {
    Property {
        variable: String,
        property: String,
        value: PlanExpr,
    },
    AddLabel {
        variable: String,
        label: String,
    },
    #[allow(dead_code)]
    RemoveLabel {
        variable: String,
        label: String,
    },
}

// =============================================================================
// Executor Request Structs (parameter bundles for functions with many args)
// =============================================================================

/// Parameters for a single-hop expand operation.
///
/// Bundles the parameters needed for `execute_expand` to improve readability
/// and make the API easier to evolve.
#[derive(Debug, Clone)]
pub struct ExpandRequest<'a> {
    pub source_variable: &'a str,
    pub rel_variable: Option<&'a str>,
    pub target_variable: &'a str,
    pub target_labels: &'a [String],
    pub path_variable: Option<&'a str>,
    pub types: &'a [String],
    pub direction: ExpandDirection,
    /// Limit for early termination.
    pub limit: Option<u64>,
}

/// Parameters for a variable-length expand operation (BFS).
///
/// Bundles the parameters needed for `execute_variable_length_expand` to improve
/// readability and make the API easier to evolve.
#[derive(Debug, Clone)]
pub struct VariableLengthExpandRequest<'a> {
    /// Base expand parameters.
    pub source_variable: &'a str,
    pub rel_variable: Option<&'a str>,
    pub target_variable: &'a str,
    pub target_labels: &'a [String],
    pub path_variable: Option<&'a str>,
    pub types: &'a [String],
    pub direction: ExpandDirection,
    /// Minimum number of hops.
    pub min_hops: u32,
    /// Maximum number of hops.
    pub max_hops: u32,
    /// Pre-resolved target node IDs for early termination.
    pub target_ids: Option<&'a [i64]>,
    /// Limit for early termination.
    pub limit: Option<u64>,
    /// Target property filter for early termination.
    pub target_property_filter: Option<&'a TargetPropertyFilter>,
}
