//! Statement and clause-level builders for the Cypher parser.

use super::super::ast::{
    CreateClause, DeleteClause, Expression, MatchClause, MergeClause, OrderByItem, Pattern,
    ReturnClause, ReturnItem, SetClause, SetItem, Statement, WhereClause,
};
use super::Rule;
use crate::error::{Error, Result};
use pest::iterators::Pair;

/// Build a CREATE statement (standalone, not attached to MATCH).
pub(super) fn build_create_statement(pair: Pair<Rule>) -> Result<Statement> {
    let clause = build_create_clause(pair)?;
    Ok(Statement::Create(clause))
}

/// Build a CREATE clause (used by both standalone CREATE and MATCH...CREATE).
pub(super) fn build_create_clause(pair: Pair<Rule>) -> Result<CreateClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Pattern {
            let pattern = super::build_pattern(inner)?;
            return Ok(CreateClause { pattern });
        }
    }
    Err(Error::Parse("CREATE requires a pattern".into()))
}

/// Build a MATCH statement.
pub(super) fn build_match_statement(
    pair: Pair<Rule>,
    return_clause: Option<ReturnClause>,
    delete_clause: Option<DeleteClause>,
    set_clause: Option<SetClause>,
    create_clause: Option<CreateClause>,
) -> Result<Statement> {
    let mut pattern = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Pattern => {
                pattern = Some(super::build_pattern(inner)?);
            }
            Rule::Where => {
                where_clause = Some(build_where_clause(inner)?);
            }
            _ => {}
        }
    }

    let pattern = pattern.ok_or_else(|| Error::Parse("MATCH requires a pattern".into()))?;
    Ok(Statement::Match(MatchClause {
        pattern,
        where_clause,
        return_clause,
        delete_clause,
        set_clause,
        create_clause,
    }))
}

/// Build a MERGE statement.
pub(super) fn build_merge_statement(pair: Pair<Rule>) -> Result<Statement> {
    let mut pattern = None;
    let mut on_create = None;
    let mut on_match = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::PatternPart => {
                let (path_variable, elements, shortest_path) = super::build_pattern_part(inner)?;
                pattern = Some(Pattern {
                    elements,
                    path_variable,
                    shortest_path,
                });
            }
            Rule::MergeAction => {
                let (is_create, set_clause) = build_merge_action(inner)?;
                if is_create {
                    on_create = Some(set_clause);
                } else {
                    on_match = Some(set_clause);
                }
            }
            _ => {}
        }
    }

    let pattern = pattern.ok_or_else(|| Error::Parse("MERGE requires a pattern".into()))?;
    Ok(Statement::Merge(MergeClause {
        pattern,
        on_create,
        on_match,
    }))
}

/// Build a merge action (ON CREATE SET / ON MATCH SET).
pub(super) fn build_merge_action(pair: Pair<Rule>) -> Result<(bool, SetClause)> {
    let mut is_create = false;
    let mut set_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::CREATE => is_create = true,
            Rule::MATCH => is_create = false,
            Rule::Set => set_clause = Some(build_set_clause(inner)?),
            _ => {}
        }
    }

    let set_clause = set_clause.ok_or_else(|| Error::Parse("Merge action requires SET".into()))?;
    Ok((is_create, set_clause))
}

/// Build a DELETE clause.
pub(super) fn build_delete_clause(pair: Pair<Rule>) -> Result<DeleteClause> {
    let mut detach = false;
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DETACH => detach = true,
            Rule::Expression => {
                expressions.push(super::build_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(DeleteClause {
        detach,
        expressions,
    })
}

/// Build a SET clause.
pub(super) fn build_set_clause(pair: Pair<Rule>) -> Result<SetClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SetItem {
            items.push(build_set_item(inner)?);
        }
    }

    Ok(SetClause { items })
}

/// Build a single SET item.
pub(super) fn build_set_item(pair: Pair<Rule>) -> Result<SetItem> {
    let mut parts: Vec<Pair<Rule>> = pair.into_inner().collect();

    // Check if this is a property expression (n.prop = value)
    if let Some(first) = parts.first() {
        if first.as_rule() == Rule::PropertyExpression {
            let prop_expr = parts.remove(0);
            let (variable, property) = build_property_expression_parts(prop_expr)?;

            // Find the value expression
            for part in parts {
                if part.as_rule() == Rule::Expression {
                    let value = super::build_expression(part)?;
                    return Ok(SetItem::Property {
                        variable,
                        property,
                        value,
                    });
                }
            }
        } else if first.as_rule() == Rule::Variable {
            let var = super::extract_variable(parts.remove(0))?;

            // Check if this is label assignment or value assignment
            for part in parts {
                match part.as_rule() {
                    Rule::NodeLabels => {
                        // Flatten label groups for SET (all labels are added)
                        let label_groups = super::build_node_labels(part)?;
                        let labels: Vec<String> = label_groups.into_iter().flatten().collect();
                        return Ok(SetItem::Labels {
                            variable: var,
                            labels,
                        });
                    }
                    Rule::Expression => {
                        // This is actually setting a variable to a value, treat as error for now
                        return Err(Error::Parse(
                            "Setting variable to expression not supported, use property access"
                                .into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    Err(Error::Parse("Invalid SET item".into()))
}

/// Extract variable and property from a PropertyExpression.
pub(super) fn build_property_expression_parts(pair: Pair<Rule>) -> Result<(String, String)> {
    let mut variable = None;
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Atom => {
                // The atom should contain a Variable
                for atom_inner in inner.into_inner() {
                    if atom_inner.as_rule() == Rule::Variable {
                        variable = Some(super::extract_variable(atom_inner)?);
                    }
                }
            }
            Rule::PropertyLookup => {
                for lookup_inner in inner.into_inner() {
                    if lookup_inner.as_rule() == Rule::PropertyKeyName {
                        property = Some(super::extract_schema_name(lookup_inner)?);
                    }
                }
            }
            _ => {}
        }
    }

    let variable =
        variable.ok_or_else(|| Error::Parse("Property expression missing variable".into()))?;
    let property =
        property.ok_or_else(|| Error::Parse("Property expression missing property".into()))?;
    Ok((variable, property))
}

/// Build a WHERE clause.
pub(super) fn build_where_clause(pair: Pair<Rule>) -> Result<WhereClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            let predicate = super::build_expression(inner)?;
            return Ok(WhereClause { predicate });
        }
    }
    Err(Error::Parse("WHERE requires an expression".into()))
}

/// Build a RETURN clause.
pub(super) fn build_return_clause(pair: Pair<Rule>) -> Result<ReturnClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::ProjectionBody {
            return build_projection_body(inner);
        }
    }
    Err(Error::Parse("RETURN requires projection body".into()))
}

/// Build projection body (the part after RETURN/WITH).
pub(super) fn build_projection_body(pair: Pair<Rule>) -> Result<ReturnClause> {
    let mut distinct = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DISTINCT => distinct = true,
            Rule::ProjectionItems => {
                items = build_projection_items(inner)?;
            }
            Rule::Order => {
                order_by = Some(build_order_by(inner)?);
            }
            Rule::Skip => {
                skip = Some(build_skip_or_limit(inner)?);
            }
            Rule::Limit => {
                limit = Some(build_skip_or_limit(inner)?);
            }
            _ => {}
        }
    }

    Ok(ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    })
}

/// Build projection items.
pub(super) fn build_projection_items(pair: Pair<Rule>) -> Result<Vec<ReturnItem>> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::STAR => {
                // Return all - represented as a special expression
                items.push(ReturnItem {
                    expression: Expression::Variable("*".into()),
                    alias: None,
                });
            }
            Rule::ProjectionItem => {
                items.push(build_projection_item(inner)?);
            }
            _ => {}
        }
    }

    Ok(items)
}

/// Build a single projection item.
pub(super) fn build_projection_item(pair: Pair<Rule>) -> Result<ReturnItem> {
    let mut expression = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Expression => {
                expression = Some(super::build_expression(inner)?);
            }
            Rule::Variable => {
                alias = Some(super::extract_variable(inner)?);
            }
            _ => {}
        }
    }

    let expression =
        expression.ok_or_else(|| Error::Parse("Projection item requires expression".into()))?;
    Ok(ReturnItem { expression, alias })
}

/// Build ORDER BY clause.
pub(super) fn build_order_by(pair: Pair<Rule>) -> Result<Vec<OrderByItem>> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::SortItem {
            items.push(build_sort_item(inner)?);
        }
    }

    Ok(items)
}

/// Build a single sort item.
pub(super) fn build_sort_item(pair: Pair<Rule>) -> Result<OrderByItem> {
    let mut expression = None;
    let mut descending = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::Expression => {
                expression = Some(super::build_expression(inner)?);
            }
            Rule::DESCENDING | Rule::DESC => descending = true,
            Rule::ASCENDING | Rule::ASC => descending = false,
            _ => {}
        }
    }

    let expression =
        expression.ok_or_else(|| Error::Parse("Sort item requires expression".into()))?;
    Ok(OrderByItem {
        expression,
        descending,
    })
}

/// Build SKIP or LIMIT value.
pub(super) fn build_skip_or_limit(pair: Pair<Rule>) -> Result<u64> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::Expression {
            let expr = super::build_expression(inner)?;
            if let Expression::Literal(super::super::ast::Literal::Integer(n)) = expr {
                return Ok(n as u64);
            }
            return Err(Error::Parse("SKIP/LIMIT requires integer literal".into()));
        }
    }
    Err(Error::Parse("SKIP/LIMIT requires expression".into()))
}
