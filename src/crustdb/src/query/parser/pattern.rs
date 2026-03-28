//! Pattern-related builders for the Cypher parser.

use super::super::ast::{
    Direction, Expression, LengthSpec, NodePattern, Pattern, PatternElement, RelationshipPattern,
    ShortestPathMode,
};
use super::RelationshipDetail;
use super::Rule;
use crate::error::{Error, Result};
use pest::iterators::Pair;

/// Build a pattern from a Pattern rule.
pub(super) fn build_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut elements = Vec::new();
    let mut path_variable = None;
    let mut shortest_path = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::PatternPart {
            let (path_var, part_elements, sp_mode) = build_pattern_part(inner)?;
            if path_var.is_some() {
                path_variable = path_var;
            }
            if sp_mode.is_some() {
                shortest_path = sp_mode;
            }
            elements.extend(part_elements);
        }
    }

    Ok(Pattern {
        elements,
        path_variable,
        shortest_path,
    })
}

/// Build pattern elements from a PatternPart.
/// Returns (path_variable, elements, shortest_path_mode).
pub(super) fn build_pattern_part(
    pair: Pair<Rule>,
) -> Result<(
    Option<String>,
    Vec<PatternElement>,
    Option<ShortestPathMode>,
)> {
    let mut path_variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::AnonymousPatternPart => {
                let (elements, shortest_path) = build_anonymous_pattern_part(inner)?;
                return Ok((path_variable, elements, shortest_path));
            }
            Rule::Variable => {
                // Named pattern (p = ...)
                path_variable = Some(super::extract_variable(inner)?);
            }
            _ => {}
        }
    }
    Err(Error::Parse(
        "expected anonymous pattern part in pattern part".into(),
    ))
}

/// Build pattern elements from an AnonymousPatternPart.
/// Returns (elements, shortest_path_mode).
fn build_anonymous_pattern_part(
    pair: Pair<Rule>,
) -> Result<(Vec<PatternElement>, Option<ShortestPathMode>)> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PatternElement => {
                return Ok((build_pattern_element(inner)?, None));
            }
            Rule::ShortestPathPattern => {
                // Extract the inner pattern from shortestPath() or allShortestPaths()
                let (elements, is_all) = build_shortest_path_pattern_elements(inner)?;
                let mode = if is_all {
                    ShortestPathMode::All
                } else {
                    ShortestPathMode::Single
                };
                return Ok((elements, Some(mode)));
            }
            _ => {}
        }
    }
    Err(Error::Parse(
        "expected pattern element or shortest path pattern in anonymous pattern part".into(),
    ))
}

/// Extract pattern elements from a ShortestPathPattern.
/// Returns (elements, is_all_shortest_paths).
pub(super) fn build_shortest_path_pattern_elements(
    pair: Pair<Rule>,
) -> Result<(Vec<PatternElement>, bool)> {
    let mut is_all = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::SHORTESTPATH => is_all = false,
            Rule::ALLSHORTESTPATHS => is_all = true,
            Rule::PatternElement => {
                let elements = build_pattern_element(inner)?;
                return Ok((elements, is_all));
            }
            _ => {}
        }
    }
    Err(Error::Parse(
        "expected pattern element in shortest path pattern".into(),
    ))
}

/// Build pattern elements from a PatternElement.
pub(super) fn build_pattern_element(pair: Pair<Rule>) -> Result<Vec<PatternElement>> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NodePattern => {
                elements.push(PatternElement::Node(build_node_pattern(inner)?));
            }
            Rule::PatternElementChain => {
                let (rel, node) = build_pattern_element_chain(inner)?;
                elements.push(PatternElement::Relationship(rel));
                elements.push(PatternElement::Node(node));
            }
            Rule::PatternElement => {
                // Parenthesized pattern element
                let nested = build_pattern_element(inner)?;
                elements.extend(nested);
            }
            _ => {}
        }
    }

    Ok(elements)
}

/// Build a relationship and node from a PatternElementChain.
fn build_pattern_element_chain(pair: Pair<Rule>) -> Result<(RelationshipPattern, NodePattern)> {
    let mut rel = None;
    let mut node = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::RelationshipPattern => {
                rel = Some(build_relationship_pattern(inner)?);
            }
            Rule::NodePattern => {
                node = Some(build_node_pattern(inner)?);
            }
            _ => {}
        }
    }

    let rel =
        rel.ok_or_else(|| Error::Parse("expected relationship in pattern element chain".into()))?;
    let node = node.ok_or_else(|| Error::Parse("expected node in pattern element chain".into()))?;
    Ok((rel, node))
}

/// Build a NodePattern from a NodePattern rule.
pub(super) fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Variable => {
                variable = Some(super::extract_variable(inner)?);
            }
            Rule::NodeLabels => {
                labels = build_node_labels(inner)?;
            }
            Rule::Properties => {
                properties = Some(build_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
    })
}

/// Build node labels from a NodeLabels rule.
/// Returns Vec<Vec<String>> where each inner Vec is OR'd (alternatives), outer Vec is AND'd.
pub(super) fn build_node_labels(pair: Pair<Rule>) -> Result<Vec<Vec<String>>> {
    let mut label_groups = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::NodeLabel {
            let mut alternatives = Vec::new();
            for label_inner in inner.into_inner() {
                if label_inner.as_rule() == Rule::LabelName {
                    alternatives.push(super::extract_schema_name(label_inner)?);
                }
            }
            if !alternatives.is_empty() {
                label_groups.push(alternatives);
            }
        }
    }

    Ok(label_groups)
}

/// Build a RelationshipPattern from a RelationshipPattern rule.
pub(super) fn build_relationship_pattern(pair: Pair<Rule>) -> Result<RelationshipPattern> {
    let mut variable = None;
    let mut types = Vec::new();
    let mut properties = None;
    let mut length = None;
    let mut has_left_arrow = false;
    let mut has_right_arrow = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::LeftArrowHead => has_left_arrow = true,
            Rule::RightArrowHead => has_right_arrow = true,
            Rule::RelationshipDetail => {
                let detail = build_relationship_detail(inner)?;
                variable = detail.0;
                types = detail.1;
                length = detail.2;
                properties = detail.3;
            }
            Rule::Dash => {}
            _ => {}
        }
    }

    let direction = match (has_left_arrow, has_right_arrow) {
        (false, true) => Direction::Outgoing, // -->
        (true, false) => Direction::Incoming, // <--
        (true, true) => Direction::Both,      // <-->
        (false, false) => Direction::Both,    // --
    };

    Ok(RelationshipPattern {
        variable,
        types,
        properties,
        direction,
        length,
    })
}

/// Build relationship detail (variable, types, range, properties).
pub(super) fn build_relationship_detail(pair: Pair<Rule>) -> Result<RelationshipDetail> {
    let mut variable = None;
    let mut types = Vec::new();
    let mut length = None;
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Variable => {
                variable = Some(super::extract_variable(inner)?);
            }
            Rule::RelationshipTypes => {
                types = build_relationship_types(inner)?;
            }
            Rule::RangeLiteral => {
                length = Some(build_range_literal(inner)?);
            }
            Rule::Properties => {
                properties = Some(build_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok((variable, types, length, properties))
}

/// Build relationship types from a RelationshipTypes rule.
pub(super) fn build_relationship_types(pair: Pair<Rule>) -> Result<Vec<String>> {
    let mut types = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::RelTypeName {
            types.push(super::extract_schema_name(inner)?);
        }
    }

    Ok(types)
}

/// Build a range literal (*1..3).
pub(super) fn build_range_literal(pair: Pair<Rule>) -> Result<LengthSpec> {
    let mut min = None;
    let mut max = None;
    let mut saw_dots = false;
    let mut first_int = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::IntegerLiteral => {
                let n = super::parse_integer_literal(inner)? as u32;
                if !saw_dots && first_int {
                    min = Some(n);
                    first_int = false;
                } else {
                    max = Some(n);
                }
            }
            Rule::DOT_DOT => {
                saw_dots = true;
            }
            _ => {}
        }
    }

    // If we only have one number and no dots, it's an exact length
    if !saw_dots && min.is_some() {
        max = min;
    }

    Ok(LengthSpec { min, max })
}

/// Build properties from a Properties rule.
pub(super) fn build_properties(pair: Pair<Rule>) -> Result<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::MapLiteral => {
                return super::build_map_literal(inner);
            }
            Rule::Parameter => {
                return super::build_parameter(inner);
            }
            _ => {}
        }
    }
    Err(Error::Parse(
        "expected map or parameter in properties".into(),
    ))
}
