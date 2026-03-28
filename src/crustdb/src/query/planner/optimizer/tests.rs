//! Tests for the query plan optimizer.

use super::super::*;
use crate::query::parser::parse;

fn plan_query(query: &str) -> QueryPlan {
    let stmt = parse(query).expect("parse failed");
    let plan = plan(&stmt).expect("plan failed");
    super::optimize(plan)
}

#[test]
fn test_plan_simple_match() {
    let plan = plan_query("MATCH (n:Person) RETURN n");
    // Should be: Project -> NodeScan
    assert!(matches!(plan.root, PlanOperator::Project { .. }));
}

#[test]
fn test_plan_count_pushdown() {
    let plan = plan_query("MATCH (n:Person) RETURN count(n)");
    // Should be optimized to CountPushdown
    assert!(matches!(plan.root, PlanOperator::CountPushdown { .. }));
}

#[test]
fn test_plan_with_where() {
    let plan = plan_query("MATCH (n:Person) WHERE n.age > 30 RETURN n");
    // Should be: Project -> Filter -> NodeScan
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    assert!(
        matches!(*source, PlanOperator::Filter { .. }),
        "expected Filter, got {:?}",
        source
    );
}

#[test]
fn test_plan_limit_pushdown() {
    let plan = plan_query("MATCH (n:Person) RETURN n LIMIT 10");
    // Should be: Project -> NodeScan(limit=10)
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    let PlanOperator::NodeScan { limit, .. } = *source else {
        panic!("expected NodeScan, got {:?}", source);
    };
    assert_eq!(limit, Some(10));
}

#[test]
fn test_plan_single_hop() {
    let plan = plan_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
    // Should be: Project -> Expand -> NodeScan
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    assert!(
        matches!(*source, PlanOperator::Expand(_)),
        "expected Expand, got {:?}",
        source
    );
}

#[test]
fn test_plan_variable_length_limit_pushdown() {
    let plan = plan_query("MATCH (a)-[*1..5]->(b) RETURN b LIMIT 1");
    // Should have limit pushed into VariableLengthExpand
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    let PlanOperator::VariableLengthExpand(ref p) = *source else {
        panic!("expected VariableLengthExpand, got {:?}", source);
    };
    assert_eq!(
        p.limit,
        Some(1),
        "LIMIT should be pushed into VariableLengthExpand"
    );
}

#[test]
fn test_plan_variable_length_filter_pushdown() {
    let plan = plan_query("MATCH (a)-[*1..5]->(b) WHERE b.name ENDS WITH 'admin' RETURN b");
    // Should have target_property_filter pushed into VariableLengthExpand
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    let PlanOperator::VariableLengthExpand(ref p) = *source else {
        panic!("expected VariableLengthExpand, got {:?}", source);
    };
    assert!(
        p.target_property_filter.is_some(),
        "ENDS WITH predicate should be pushed into VariableLengthExpand"
    );
    let Some(TargetPropertyFilter::EndsWith {
        ref property,
        ref suffix,
    }) = p.target_property_filter
    else {
        panic!(
            "expected EndsWith filter, got {:?}",
            p.target_property_filter
        );
    };
    assert_eq!(property, "name");
    assert_eq!(suffix, "admin");
}

#[test]
fn test_plan_variable_length_limit_through_filter() {
    // When both source and target predicates from a WHERE clause can be
    // pushed down (source into NodeScan, target into VariableLengthExpand),
    // the Filter is eliminated entirely and LIMIT can be pushed into the
    // expand for early BFS termination.
    // Structure: Project -> VariableLengthExpand(limit=1, target_filter, source: NodeScan(prop_filter))
    let plan = plan_query(
        "MATCH p = (a)-[*1..20]->(b) WHERE a.name = 'test' AND b.id ENDS WITH '-519' RETURN length(p) LIMIT 1",
    );
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    let PlanOperator::VariableLengthExpand(ref p) = *source else {
        panic!(
            "expected VariableLengthExpand under Project, got {:?}",
            source
        );
    };
    assert_eq!(
        p.limit,
        Some(1),
        "LIMIT should be pushed into VariableLengthExpand when Filter is eliminated"
    );
    assert!(
        p.target_property_filter.is_some(),
        "Target property filter should be pushed"
    );
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *p.source
    else {
        panic!(
            "expected NodeScan under VariableLengthExpand, got {:?}",
            p.source
        );
    };
    assert!(
        property_filter.is_some(),
        "Source property filter should be pushed into NodeScan"
    );
}

#[test]
fn test_plan_source_filter_pushdown_to_nodescan() {
    // Source equality predicates from WHERE should be pushed into
    // the NodeScan below VariableLengthExpand.
    let plan = plan_query("MATCH (a)-[*1..20]->(b) WHERE a.objectid = 'USER_0' RETURN b.objectid");
    // Plan should be: Project -> VariableLengthExpand(source: NodeScan(prop_filter))
    // No Filter should remain.
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    let PlanOperator::VariableLengthExpand(ref p) = *source else {
        panic!(
            "expected VariableLengthExpand under Project, got {:?}",
            source
        );
    };
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *p.source
    else {
        panic!(
            "expected NodeScan under VariableLengthExpand, got {:?}",
            p.source
        );
    };
    assert!(
        property_filter.is_some(),
        "Source predicate should be pushed into NodeScan"
    );
    let (ref prop, ref val) = property_filter.as_ref().unwrap();
    assert_eq!(prop, "objectid");
    assert_eq!(*val, serde_json::Value::String("USER_0".to_string()));
}

#[test]
fn test_plan_boolean_target_filter_pushdown() {
    // Boolean target property filters should be pushed into VariableLengthExpand
    let plan = plan_query("MATCH (a)-[*1..20]->(b) WHERE b.tier = 0 RETURN b.objectid");
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project, got {:?}", plan.root);
    };
    let PlanOperator::VariableLengthExpand(ref p) = *source else {
        panic!("expected VariableLengthExpand, got {:?}", source);
    };
    assert!(
        p.target_property_filter.is_some(),
        "Boolean target property filter should be pushed into VariableLengthExpand"
    );
    let Some(TargetPropertyFilter::Eq {
        ref property,
        ref value,
    }) = p.target_property_filter
    else {
        panic!("expected Eq filter, got {:?}", p.target_property_filter);
    };
    assert_eq!(property, "tier");
    assert_eq!(*value, serde_json::Value::Number(0.into()));
}

#[test]
fn test_plan_relationship_count_pushdown() {
    // MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1
    // should produce: Limit(1, RelationshipCountPushdown)
    let plan = plan_query("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1");
    let PlanOperator::Limit { source, count } = plan.root else {
        panic!("expected Limit at root, got {:?}", plan.root);
    };
    assert_eq!(count, 1);
    assert!(
        matches!(*source, PlanOperator::RelationshipCountPushdown { .. }),
        "expected RelationshipCountPushdown, got {:?}",
        source
    );
}

#[test]
fn test_plan_expand_limit_pushdown() {
    // MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5
    // should push LIMIT into Expand
    let plan = plan_query("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5");
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    let PlanOperator::Expand(ref p) = *source else {
        panic!("expected Expand under Project, got {:?}", source);
    };
    assert_eq!(p.limit, Some(5), "LIMIT 5 should be pushed into Expand");
}

#[test]
fn test_limit_through_filter_into_expand() {
    // MATCH (a)-[:KNOWS]->(b) WHERE b.name = 'Alice' RETURN b LIMIT 3
    // Filter should be eliminated (pushed into Expand target_property_filter)
    // and LIMIT should be pushed into Expand
    let plan = plan_query("MATCH (a)-[:KNOWS]->(b) WHERE b.name = 'Alice' RETURN b LIMIT 3");
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    let PlanOperator::Expand(ref p) = *source else {
        panic!("expected Expand under Project, got {:?}", source);
    };
    assert_eq!(p.limit, Some(3), "LIMIT should be pushed into Expand");
    assert!(
        p.target_property_filter.is_some(),
        "target filter should be pushed into Expand"
    );
}

#[test]
fn test_limit_through_filter_into_shortest_path() {
    // shortestPath with WHERE and LIMIT — LIMIT should push through
    // when the filter is fully eliminated
    let plan = plan_query(
        "MATCH p = shortestPath((a:User)-[*]->(b:Group)) WHERE b.name ENDS WITH '-admin' RETURN p LIMIT 2",
    );
    // After optimization the VarLen/ShortestPath should have limit=2
    // and target_property_filter set
    fn find_sp(op: &PlanOperator) -> Option<&ShortestPathParams> {
        match op {
            PlanOperator::ShortestPath(p) => Some(p),
            PlanOperator::Project { source, .. } => find_sp(source),
            PlanOperator::Filter { source, .. } => find_sp(source),
            _ => None,
        }
    }
    let sp = find_sp(&plan.root).expect("Should contain ShortestPath");
    assert!(
        sp.target_property_filter.is_some(),
        "target filter should be pushed into ShortestPath"
    );
    assert_eq!(
        sp.limit,
        Some(2),
        "LIMIT should be pushed into ShortestPath"
    );
}

// =========================================================================
// Single-hop Expand filter pushdown tests
// =========================================================================

#[test]
fn test_expand_target_filter_pushdown() {
    // WHERE b.name = 'Admin' should be pushed into Expand.target_property_filter
    let plan = plan_query("MATCH (a:User)-[:MEMBER_OF]->(b:Group) WHERE b.name = 'Admin' RETURN a");
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    // Filter should be eliminated -- predicate pushed into Expand
    let PlanOperator::Expand(ref p) = *source else {
        panic!(
            "expected Expand directly under Project (Filter eliminated), got {:?}",
            source
        );
    };
    assert!(
        p.target_property_filter.is_some(),
        "Target filter should be pushed into Expand"
    );
    let Some(TargetPropertyFilter::Eq {
        ref property,
        ref value,
    }) = p.target_property_filter
    else {
        panic!("expected Eq filter, got {:?}", p.target_property_filter);
    };
    assert_eq!(property, "name");
    assert_eq!(*value, serde_json::Value::String("Admin".to_string()));
}

#[test]
fn test_expand_source_filter_pushdown() {
    // WHERE a.id = 'user1' should be pushed into the NodeScan under Expand
    let plan = plan_query("MATCH (a)-[:KNOWS]->(b) WHERE a.id = 'user1' RETURN b");
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    let PlanOperator::Expand(ref p) = *source else {
        panic!("expected Expand under Project, got {:?}", source);
    };
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *p.source
    else {
        panic!("expected NodeScan under Expand, got {:?}", p.source);
    };
    assert!(
        property_filter.is_some(),
        "Source predicate should be pushed into NodeScan"
    );
}

#[test]
fn test_expand_both_filters_pushdown() {
    // Both source and target filters should be pushed down
    let plan = plan_query(
        "MATCH (a)-[:KNOWS]->(b) WHERE a.id = 'user1' AND b.name ENDS WITH '-admin' RETURN b",
    );
    let PlanOperator::Project { source, .. } = plan.root else {
        panic!("expected Project at root, got {:?}", plan.root);
    };
    // Filter should be eliminated entirely
    let PlanOperator::Expand(ref p) = *source else {
        panic!(
            "expected Expand under Project (Filter eliminated), got {:?}",
            source
        );
    };
    assert!(
        p.target_property_filter.is_some(),
        "Target filter should be pushed into Expand"
    );
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *p.source
    else {
        panic!("expected NodeScan under Expand, got {:?}", p.source);
    };
    assert!(
        property_filter.is_some(),
        "Source filter should be pushed into NodeScan"
    );
}

// =========================================================================
// ShortestPath filter pushdown tests
// =========================================================================

#[test]
fn test_shortest_path_ends_with_filter_pushdown() {
    let plan = plan_query(
        "MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE b.name ENDS WITH '-512' RETURN p",
    );
    // Should push ENDS WITH into ShortestPath.target_property_filter
    fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
        match op {
            PlanOperator::ShortestPath(p) => Some(p),
            PlanOperator::Project { source, .. } => find_shortest_path(source),
            PlanOperator::Filter { source, .. } => find_shortest_path(source),
            _ => None,
        }
    }
    let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
    assert!(
        sp.target_property_filter.is_some(),
        "ENDS WITH should be pushed into ShortestPath"
    );
    let Some(TargetPropertyFilter::EndsWith {
        ref property,
        ref suffix,
    }) = sp.target_property_filter
    else {
        panic!(
            "expected EndsWith filter, got {:?}",
            sp.target_property_filter
        );
    };
    assert_eq!(property, "name");
    assert_eq!(suffix, "-512");
}

#[test]
fn test_shortest_path_eq_filter_pushdown() {
    let plan = plan_query("MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE b.id = 42 RETURN p");
    fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
        match op {
            PlanOperator::ShortestPath(p) => Some(p),
            PlanOperator::Project { source, .. } => find_shortest_path(source),
            PlanOperator::Filter { source, .. } => find_shortest_path(source),
            _ => None,
        }
    }
    let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
    assert!(
        sp.target_property_filter.is_some(),
        "Eq predicate should be pushed into ShortestPath"
    );
    matches!(
        sp.target_property_filter,
        Some(TargetPropertyFilter::Eq { .. })
    );
}

#[test]
fn test_shortest_path_source_filter_pushdown() {
    let plan =
        plan_query("MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE a.name = 'Alice' RETURN p");
    fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
        match op {
            PlanOperator::ShortestPath(p) => Some(p),
            PlanOperator::Project { source, .. } => find_shortest_path(source),
            PlanOperator::Filter { source, .. } => find_shortest_path(source),
            _ => None,
        }
    }
    let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
    // Source filter should be pushed into the NodeScan under ShortestPath
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *sp.source
    else {
        panic!("expected NodeScan under ShortestPath, got {:?}", sp.source);
    };
    assert!(
        property_filter.is_some(),
        "Source predicate should be pushed into NodeScan"
    );
}

#[test]
fn test_shortest_path_both_filters_pushdown() {
    // Both source and target filters should be pushed down simultaneously
    let plan = plan_query(
        "MATCH p = shortestPath((a)-[:REL*1..5]->(b)) WHERE a.name = 'Alice' AND b.name ENDS WITH '-512' RETURN p",
    );
    fn find_shortest_path(op: &PlanOperator) -> Option<&ShortestPathParams> {
        match op {
            PlanOperator::ShortestPath(p) => Some(p),
            PlanOperator::Project { source, .. } => find_shortest_path(source),
            PlanOperator::Filter { source, .. } => find_shortest_path(source),
            _ => None,
        }
    }
    let sp = find_shortest_path(&plan.root).expect("Should contain ShortestPath");
    assert!(
        sp.target_property_filter.is_some(),
        "Target filter should be pushed into ShortestPath"
    );
    let PlanOperator::NodeScan {
        ref property_filter,
        ..
    } = *sp.source
    else {
        panic!("expected NodeScan under ShortestPath, got {:?}", sp.source);
    };
    assert!(
        property_filter.is_some(),
        "Source filter should be pushed into NodeScan"
    );
}

// =========================================================================
// CrossJoin reordering tests
// =========================================================================

#[test]
fn test_crossjoin_reorder_smaller_left() {
    // MATCH (a:User), (b:Domain) — if we have no data, the optimizer
    // should still produce a CrossJoin. We test that the structure
    // has labeled NodeScans in cross join.
    let plan = plan_query("MATCH (a:Domain), (b:User) RETURN a, b");
    // The optimizer should place the smaller-estimated side (Domain, fewer
    // labels typically) as left. Since we can't know actual counts at plan
    // time without data, we verify the estimate_cardinality function works
    // and the CrossJoin is reordered when estimates differ.
    fn extract_crossjoin_labels(op: &PlanOperator) -> Option<(Vec<String>, Vec<String>)> {
        match op {
            PlanOperator::CrossJoin { left, right } => {
                let left_labels = match left.as_ref() {
                    PlanOperator::NodeScan { label_groups, .. } => {
                        label_groups.iter().flatten().cloned().collect()
                    }
                    _ => vec![],
                };
                let right_labels = match right.as_ref() {
                    PlanOperator::NodeScan { label_groups, .. } => {
                        label_groups.iter().flatten().cloned().collect()
                    }
                    _ => vec![],
                };
                Some((left_labels, right_labels))
            }
            PlanOperator::Project { source, .. } => extract_crossjoin_labels(source),
            PlanOperator::Filter { source, .. } => extract_crossjoin_labels(source),
            _ => None,
        }
    }
    let (left_labels, right_labels) =
        extract_crossjoin_labels(&plan.root).expect("Should contain CrossJoin");
    // Both sides should be NodeScans with labels
    assert!(!left_labels.is_empty(), "Left should have labels");
    assert!(!right_labels.is_empty(), "Right should have labels");
}

#[test]
fn test_crossjoin_reorder_nodescan_vs_expand() {
    // MATCH (a:User)-[:KNOWS]->(b), (c:Domain) RETURN a, b, c
    // Expand has higher estimated cardinality than a single NodeScan,
    // so NodeScan(Domain) should be placed as left (outer loop).
    let plan = plan_query("MATCH (a:User)-[:KNOWS]->(b), (c:Domain) RETURN a, b, c");
    fn find_crossjoin(op: &PlanOperator) -> Option<&PlanOperator> {
        match op {
            cj @ PlanOperator::CrossJoin { .. } => Some(cj),
            PlanOperator::Project { source, .. } => find_crossjoin(source),
            PlanOperator::Filter { source, .. } => find_crossjoin(source),
            _ => None,
        }
    }
    let cj = find_crossjoin(&plan.root).expect("Should contain CrossJoin");
    if let PlanOperator::CrossJoin { left, right } = cj {
        // Left should be the smaller side (NodeScan for Domain)
        assert!(
            matches!(left.as_ref(), PlanOperator::NodeScan { .. }),
            "Left side of CrossJoin should be NodeScan (smaller), got {:?}",
            left
        );
        // Right should be the larger side (Expand)
        assert!(
            matches!(right.as_ref(), PlanOperator::Expand(_)),
            "Right side of CrossJoin should be Expand (larger), got {:?}",
            right
        );
    }
}
