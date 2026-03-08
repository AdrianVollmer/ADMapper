//! Pattern matching execution for MATCH queries.

mod matching;
mod shortest_path;
mod traversal;

pub use shortest_path::*;
pub use traversal::*;

use crate::graph::Relationship;
use crate::query::parser::{Pattern, PatternElement};

// =============================================================================
// Configuration Constants
// =============================================================================

/// Default maximum path length for unbounded traversals.
///
/// Used when queries specify open-ended patterns like `(a)-[:REL*]->(b)` or
/// `(a)-[:REL*1..]->(b)` without an explicit upper bound. This prevents infinite
/// loops and memory exhaustion on cyclic or very deep graphs.
///
/// The value 10000 allows traversing reasonably large graphs while providing
/// a safety limit. For queries that need deeper traversal, use explicit bounds
/// like `(a)-[:REL*1..50000]->(b)`.
pub const DEFAULT_MAX_PATH_DEPTH: usize = 10000;

/// State for BFS traversal with path tracking.
#[derive(Clone)]
pub(crate) struct TraversalState {
    pub(crate) node_id: i64,
    /// Node IDs in the path (including current).
    pub(crate) path_nodes: Vec<i64>,
    /// Edges traversed to reach this state.
    pub(crate) path_rels: Vec<Relationship>,
}

/// Check if pattern is a single node pattern.
pub fn is_single_node_pattern(pattern: &Pattern) -> bool {
    pattern.elements.len() == 1 && matches!(pattern.elements[0], PatternElement::Node(_))
}

/// Check if pattern is a single-hop relationship pattern (node-rel-node) without variable length.
pub fn is_single_hop_pattern(pattern: &Pattern) -> bool {
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
pub fn is_variable_length_pattern(pattern: &Pattern) -> bool {
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

/// Check if pattern is a multi-hop pattern (node-rel-node-rel-node...).
pub fn is_multi_hop_pattern(pattern: &Pattern) -> bool {
    // Must have at least 5 elements (node-rel-node-rel-node)
    if pattern.elements.len() < 5 {
        return false;
    }
    // Must have odd number of elements (alternating node-rel-node...)
    if pattern.elements.len().is_multiple_of(2) {
        return false;
    }
    // Check alternating pattern: node, rel, node, rel, node, ...
    for (i, elem) in pattern.elements.iter().enumerate() {
        if i % 2 == 0 {
            // Even indices should be nodes
            if !matches!(elem, PatternElement::Node(_)) {
                return false;
            }
        } else {
            // Odd indices should be relationships without variable length
            match elem {
                PatternElement::Relationship(rel) => {
                    if rel.length.is_some() {
                        return false; // Variable length not supported in multi-hop yet
                    }
                }
                _ => return false,
            }
        }
    }
    true
}

/// Check if pattern is a shortest path pattern.
///
/// Note: In openCypher 9, shortest paths are expressed using shortestPath() and
/// allShortestPaths() functions, which are parsed as Expression variants. This
/// function checks if a pattern has variable-length relationships that would be
/// suitable for shortest path queries.
pub fn is_shortest_path_pattern(pattern: &Pattern) -> bool {
    // A pattern is suitable for shortest path if it has a variable-length relationship
    for elem in &pattern.elements {
        if let PatternElement::Relationship(rel) = elem {
            if rel.length.is_some() {
                return true;
            }
        }
    }
    false
}

/// Extract source and target variable names from a shortest path pattern.
pub fn get_path_endpoint_vars(pattern: &Pattern) -> (String, String) {
    let source_var = pattern
        .elements
        .first()
        .and_then(|e| match e {
            PatternElement::Node(n) => n.variable.clone(),
            _ => None,
        })
        .unwrap_or_else(|| "_src".to_string());

    let target_var = pattern
        .elements
        .last()
        .and_then(|e| match e {
            PatternElement::Node(n) => n.variable.clone(),
            _ => None,
        })
        .unwrap_or_else(|| "_tgt".to_string());

    (source_var, target_var)
}
