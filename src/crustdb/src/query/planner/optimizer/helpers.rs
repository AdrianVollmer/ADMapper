//! Helper functions for filter pushdown and pattern detection.

use super::super::{FilterPredicate, PlanOperator};
use super::filter::extract_source_property_filter;

/// After pushing a target filter, handle remaining predicates:
/// try source pushdown, then wrap leftovers in a Filter.
pub(super) fn wrap_with_remaining(
    operator: PlanOperator,
    remaining: Option<FilterPredicate>,
) -> PlanOperator {
    if let Some(remaining_pred) = remaining {
        let (pushed, leftover) = try_push_source_filter_into_node_scan(remaining_pred, operator);
        wrap_with_filter(pushed, leftover)
    } else {
        operator
    }
}

/// Wrap an operator in a Filter if there's a leftover predicate.
pub(super) fn wrap_with_filter(
    operator: PlanOperator,
    leftover: Option<FilterPredicate>,
) -> PlanOperator {
    if let Some(pred) = leftover {
        PlanOperator::Filter {
            source: Box::new(operator),
            predicate: pred,
        }
    } else {
        operator
    }
}

/// Try to push a source property equality filter into the NodeScan that feeds
/// a VariableLengthExpand or ShortestPath.
///
/// Transforms:
///   Filter(a.prop = 'X') -> Expand(source: NodeScan(no filter))
/// Into:
///   Expand(source: NodeScan(property_filter: (prop, 'X')))
///
/// Returns the (possibly modified) operator and any remaining predicate.
pub(super) fn try_push_source_filter_into_node_scan(
    predicate: FilterPredicate,
    operator: PlanOperator,
) -> (PlanOperator, Option<FilterPredicate>) {
    match operator {
        PlanOperator::VariableLengthExpand(mut p) => {
            if let Some(result) =
                try_push_into_inner_scan(&predicate, &p.source_variable, &mut p.source)
            {
                return (PlanOperator::VariableLengthExpand(p), result);
            }
            (PlanOperator::VariableLengthExpand(p), Some(predicate))
        }
        PlanOperator::ShortestPath(mut p) => {
            if let Some(result) =
                try_push_into_inner_scan(&predicate, &p.source_variable, &mut p.source)
            {
                return (PlanOperator::ShortestPath(p), result);
            }
            (PlanOperator::ShortestPath(p), Some(predicate))
        }
        PlanOperator::Expand(mut p) => {
            if let Some(result) =
                try_push_into_inner_scan(&predicate, &p.source_variable, &mut p.source)
            {
                return (PlanOperator::Expand(p), result);
            }
            (PlanOperator::Expand(p), Some(predicate))
        }
        other => (other, Some(predicate)),
    }
}

/// Try to push a source property filter into a NodeScan that is the inner
/// source of an expand operator. Returns `Some(remaining)` on success.
pub(super) fn try_push_into_inner_scan(
    predicate: &FilterPredicate,
    source_variable: &str,
    source: &mut Box<PlanOperator>,
) -> Option<Option<FilterPredicate>> {
    if let PlanOperator::NodeScan {
        property_filter: ref mut pf @ None,
        ..
    } = **source
    {
        if let Some((prop_filter, remaining)) =
            extract_source_property_filter(predicate, source_variable)
        {
            *pf = Some(prop_filter);
            return Some(remaining);
        }
    }
    None
}

/// Check if a plan involves a relationship pattern that binds the given variable.
///
/// This is used to detect patterns like `MATCH ()-[r]->() RETURN DISTINCT type(r)`
/// where we can optimize to use RelationshipTypesScan instead of scanning all relationships.
pub(super) fn is_relationship_pattern_with_var(op: &PlanOperator, rel_var: &str) -> bool {
    match op {
        PlanOperator::Expand(p) => p.rel_variable.as_deref() == Some(rel_var),
        PlanOperator::VariableLengthExpand(p) => p.rel_variable.as_deref() == Some(rel_var),
        PlanOperator::Filter { source, .. } => is_relationship_pattern_with_var(source, rel_var),
        _ => false,
    }
}
