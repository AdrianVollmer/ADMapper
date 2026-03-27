//! Filter pushdown into expand-like operators.

use super::super::{
    ExpandParams, FilterPredicate, PlanOperator, ShortestPathParams, VarLenExpandParams,
};
use super::filter::extract_target_property_filter;
use super::helpers::{
    try_push_source_filter_into_node_scan, wrap_with_filter, wrap_with_remaining,
};
use super::optimize_operator;

/// Push filter predicates from a WHERE clause into a VariableLengthExpand.
///
/// Target predicates (on the target variable) become `target_property_filter`
/// for early BFS termination. Source predicates go into the NodeScan for
/// reduced starting set.
pub(super) fn push_filter_into_var_len_expand(
    mut p: VarLenExpandParams,
    predicate: FilterPredicate,
) -> PlanOperator {
    if p.target_property_filter.is_none() {
        if let Some((pushed_filter, remaining_predicate)) =
            extract_target_property_filter(&predicate, &p.target_variable)
        {
            p.source = Box::new(optimize_operator(*p.source));
            p.target_property_filter = Some(pushed_filter);
            let expand = PlanOperator::VariableLengthExpand(p);
            return wrap_with_remaining(expand, remaining_predicate);
        }
    }
    // Couldn't push target predicate (or already has one), try source pushdown
    p.source = Box::new(optimize_operator(*p.source));
    let expand = PlanOperator::VariableLengthExpand(p);
    let (pushed, leftover) = try_push_source_filter_into_node_scan(predicate, expand);
    wrap_with_filter(pushed, leftover)
}

/// Push filter predicates from a WHERE clause into a ShortestPath.
///
/// Target predicates (on the target variable) become `target_property_filter`
/// for early BFS termination. Source predicates go into the NodeScan for
/// reduced starting set.
pub(super) fn push_filter_into_shortest_path(
    mut p: ShortestPathParams,
    predicate: FilterPredicate,
) -> PlanOperator {
    if p.target_property_filter.is_none() {
        if let Some((pushed_filter, remaining_predicate)) =
            extract_target_property_filter(&predicate, &p.target_variable)
        {
            p.source = Box::new(optimize_operator(*p.source));
            p.target_property_filter = Some(pushed_filter);
            let sp = PlanOperator::ShortestPath(p);
            return wrap_with_remaining(sp, remaining_predicate);
        }
    }
    // Couldn't push target predicate (or already has one), try source pushdown
    p.source = Box::new(optimize_operator(*p.source));
    let sp = PlanOperator::ShortestPath(p);
    let (pushed, leftover) = try_push_source_filter_into_node_scan(predicate, sp);
    wrap_with_filter(pushed, leftover)
}

/// Push filter predicates from a WHERE clause into a single-hop Expand.
///
/// Target predicates (on the target variable) become `target_property_filter`
/// for early rejection of non-matching neighbors. Source predicates go into
/// the NodeScan for reduced starting set.
pub(super) fn push_filter_into_expand(
    mut p: ExpandParams,
    predicate: FilterPredicate,
) -> PlanOperator {
    if p.target_property_filter.is_none() {
        if let Some((pushed_filter, remaining_predicate)) =
            extract_target_property_filter(&predicate, &p.target_variable)
        {
            p.source = Box::new(optimize_operator(*p.source));
            p.target_property_filter = Some(pushed_filter);
            let expand = PlanOperator::Expand(p);
            return wrap_with_remaining(expand, remaining_predicate);
        }
    }
    // Couldn't push target predicate (or already has one), try source pushdown
    p.source = Box::new(optimize_operator(*p.source));
    let expand = PlanOperator::Expand(p);
    let (pushed, leftover) = try_push_source_filter_into_node_scan(predicate, expand);
    wrap_with_filter(pushed, leftover)
}
