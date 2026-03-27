//! Cardinality estimation for CrossJoin reordering.

use super::super::PlanOperator;

/// Estimate the cardinality of a plan operator for CrossJoin reordering.
///
/// These are rough heuristics — we don't have runtime stats, so we use
/// structural properties of the plan to estimate relative sizes.
pub(super) fn estimate_cardinality(op: &PlanOperator) -> u64 {
    match op {
        PlanOperator::NodeScan {
            property_filter,
            limit,
            ..
        } => {
            let base = if property_filter.is_some() {
                // Indexed property lookup — likely very selective
                10
            } else {
                // Full label scan — assume moderate size
                1000
            };
            match limit {
                Some(l) => base.min(*l),
                None => base,
            }
        }
        PlanOperator::Expand(p) => {
            // Source cardinality * estimated fan-out
            let source_card = estimate_cardinality(&p.source);
            let fan_out = if p.target_property_filter.is_some() {
                2 // Filtered expand — low fan-out
            } else {
                10 // Unfiltered — moderate fan-out
            };
            source_card.saturating_mul(fan_out)
        }
        PlanOperator::VariableLengthExpand(p) => {
            let source_card = estimate_cardinality(&p.source);
            source_card.saturating_mul(100) // BFS can reach many nodes
        }
        PlanOperator::ShortestPath(p) => {
            let source_card = estimate_cardinality(&p.source);
            source_card.saturating_mul(5) // Typically few shortest paths per source
        }
        PlanOperator::Filter { source, .. } => {
            // Assume filter removes ~50% of rows
            estimate_cardinality(source) / 2
        }
        PlanOperator::Limit { count, .. } => *count,
        PlanOperator::CrossJoin { left, right } => {
            estimate_cardinality(left).saturating_mul(estimate_cardinality(right))
        }
        // Default: treat as large to avoid moving unknowns
        _ => 10_000,
    }
}
