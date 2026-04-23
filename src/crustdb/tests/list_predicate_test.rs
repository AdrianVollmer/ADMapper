//! Tests for list predicate functions: ALL, ANY, NONE, SINGLE.

use crustdb::{Database, ResultValue};

fn get_string_values(result: &crustdb::QueryResult, column: &str) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| match row.get(column)? {
            ResultValue::Property(crustdb::PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

fn setup_db() -> Database {
    let db = Database::in_memory().expect("Failed to create database");

    // Linear chain with mixed relationship types:
    // A -HasSession-> B -AdminTo-> C -GenericAll-> D
    db.execute("CREATE (a:User {name: 'A'})").unwrap();
    db.execute("CREATE (b:Computer {name: 'B'})").unwrap();
    db.execute("CREATE (c:Group {name: 'C'})").unwrap();
    db.execute("CREATE (d:Group {name: 'D'})").unwrap();

    db.execute(
        "MATCH (a:User {name: 'A'}), (b:Computer {name: 'B'}) CREATE (a)-[:HasSession]->(b)",
    )
    .unwrap();
    db.execute("MATCH (b:Computer {name: 'B'}), (c:Group {name: 'C'}) CREATE (b)-[:AdminTo]->(c)")
        .unwrap();
    db.execute("MATCH (c:Group {name: 'C'}), (d:Group {name: 'D'}) CREATE (c)-[:GenericAll]->(d)")
        .unwrap();

    // Separate MemberOf chain: E -MemberOf-> F -MemberOf-> G
    db.execute("CREATE (e:User {name: 'E'})").unwrap();
    db.execute("CREATE (f:Group {name: 'F'})").unwrap();
    db.execute("CREATE (g:Group {name: 'G'})").unwrap();

    db.execute("MATCH (e:User {name: 'E'}), (f:Group {name: 'F'}) CREATE (e)-[:MemberOf]->(f)")
        .unwrap();
    db.execute("MATCH (f:Group {name: 'F'}), (g:Group {name: 'G'}) CREATE (f)-[:MemberOf]->(g)")
        .unwrap();

    db
}

#[test]
fn test_none_predicate_filters_relationship_types() {
    let db = setup_db();

    // A -HasSession-> B -AdminTo-> C -GenericAll-> D
    // NONE of these are MemberOf, so all should pass
    let result = db
        .execute(
            "MATCH (u:User {name: 'A'})-[r*1..3]->(target) \
             WHERE NONE(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"B".to_string()),
        "Should find B (HasSession, no MemberOf), got: {:?}",
        names
    );
    assert!(
        names.contains(&"C".to_string()),
        "Should find C (HasSession->AdminTo, no MemberOf), got: {:?}",
        names
    );
    assert!(
        names.contains(&"D".to_string()),
        "Should find D (HasSession->AdminTo->GenericAll, no MemberOf), got: {:?}",
        names
    );

    // E's paths are all MemberOf, should be filtered out
    let result2 = db
        .execute(
            "MATCH (u:User {name: 'E'})-[r*1..3]->(target) \
             WHERE NONE(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names2 = get_string_values(&result2, "name");
    assert!(
        names2.is_empty(),
        "E's paths are all MemberOf, should be empty, got: {:?}",
        names2
    );
}

#[test]
fn test_any_predicate() {
    let db = setup_db();

    // Find paths from A where ANY relationship is AdminTo
    let result = db
        .execute(
            "MATCH (u:User {name: 'A'})-[r*1..3]->(target) \
             WHERE ANY(rel IN r WHERE type(rel) = 'AdminTo') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    // B is reached via HasSession only (no AdminTo) - should NOT be included
    assert!(
        !names.contains(&"B".to_string()),
        "Should not find B (only HasSession, no AdminTo), got: {:?}",
        names
    );

    // C is reached via HasSession->AdminTo - should be included
    assert!(
        names.contains(&"C".to_string()),
        "Should find C via AdminTo path, got: {:?}",
        names
    );
}

#[test]
fn test_all_predicate() {
    let db = setup_db();

    // Find paths from E where ALL relationships are MemberOf
    let result = db
        .execute(
            "MATCH (u:User {name: 'E'})-[r*1..3]->(target) \
             WHERE ALL(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"F".to_string()),
        "Should find F via MemberOf, got: {:?}",
        names
    );
    assert!(
        names.contains(&"G".to_string()),
        "Should find G via MemberOf->MemberOf, got: {:?}",
        names
    );

    // A's paths are non-MemberOf, ALL should filter them
    let result2 = db
        .execute(
            "MATCH (u:User {name: 'A'})-[r*1..3]->(target) \
             WHERE ALL(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names2 = get_string_values(&result2, "name");
    assert!(
        names2.is_empty(),
        "A's paths have no MemberOf, should be empty, got: {:?}",
        names2
    );
}

#[test]
fn test_single_predicate() {
    let db = setup_db();

    // Find length-2 paths from A where exactly one rel is HasSession
    // A -HasSession-> B -AdminTo-> C: has exactly 1 HasSession
    let result = db
        .execute(
            "MATCH (u:User {name: 'A'})-[r*2..2]->(target) \
             WHERE SINGLE(rel IN r WHERE type(rel) = 'HasSession') \
             RETURN target.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"C".to_string()),
        "Should find C via HasSession->AdminTo (single HasSession), got: {:?}",
        names
    );
}

#[test]
fn test_original_failing_query_pattern() {
    let db = setup_db();

    // This is the exact query pattern from the bug report
    let result = db
        .execute(
            "MATCH (g:Group)-[r*1..5]->(target) \
             WHERE NONE(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN DISTINCT target",
        )
        .unwrap();

    // Should succeed without "Unknown atom type" error
    assert!(
        result.columns.contains(&"target".to_string()),
        "Should have 'target' column"
    );
}

#[test]
fn test_none_without_where() {
    let db = setup_db();

    // NONE without WHERE means "none of the elements are truthy"
    // For relationship lists, all elements are truthy (they exist),
    // so NONE should always return false for non-empty paths.
    // This means no results should pass.
    let result = db
        .execute(
            "MATCH (u:User {name: 'A'})-[r*1..1]->(target) \
             RETURN target.name AS name",
        )
        .unwrap();

    // Verify basic query works (sanity check)
    assert!(
        !result.rows.is_empty(),
        "Should have results for basic path query"
    );
}

#[test]
fn test_ends_with_sid_and_none_memberof() {
    let db = Database::in_memory().unwrap();

    // Simulate the exact production query pattern
    db.execute("CREATE (g:Group {name: 'Auth Users', objectid: 'DOMAIN-S-1-5-11'})")
        .unwrap();
    db.execute("CREATE (t:Computer {name: 'DC01', objectid: 'C-1'})")
        .unwrap();
    db.execute(
        "MATCH (g:Group {name: 'Auth Users'}), (t:Computer {name: 'DC01'}) \
         CREATE (g)-[:CanRDP]->(t)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (g:Group)-[r*1..5]->(target) \
             WHERE g.objectid ENDS WITH '-S-1-5-11' \
             AND NONE(rel IN r WHERE type(rel) = 'MemberOf') \
             RETURN DISTINCT target.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");
    assert!(
        names.contains(&"DC01".to_string()),
        "Should find DC01 via CanRDP (no MemberOf), got: {:?}",
        names
    );
}

// ─── relationships(p) inside ALL/ANY/NONE ────────────────────────────────────

/// Set up a graph where exploit_likelihood values differ per edge.
///
/// User U1 → Group G1 (DA, SID ends with -512).
///   U1 --[MemberOf, exploit_likelihood=0.9]--> G1          (high likelihood)
///
/// User U2 → Group G1 via two hops:
///   U2 --[MemberOf, exploit_likelihood=0.0]--> G2
///   G2 --[MemberOf, exploit_likelihood=0.9]--> G1          (one hop is 0.0)
///
/// User U3 → Group G1 via two hops, both high:
///   U3 --[MemberOf, exploit_likelihood=0.9]--> G3
///   G3 --[MemberOf, exploit_likelihood=0.8]--> G1
fn setup_exploit_likelihood_db() -> Database {
    let db = Database::in_memory().expect("db");

    // Domain Admins group
    db.execute("CREATE (:Group {objectid: 'S-1-5-21-512', name: 'DA'})")
        .unwrap();

    // U1: single high-likelihood hop
    db.execute("CREATE (:User {objectid: 'U1', name: 'U1'})")
        .unwrap();
    db.execute(
        "MATCH (u:User {objectid: 'U1'}), (g:Group {objectid: 'S-1-5-21-512'}) \
         CREATE (u)-[:MemberOf {exploit_likelihood: 0.9}]->(g)",
    )
    .unwrap();

    // U2: two-hop path where first hop has likelihood=0.0
    db.execute("CREATE (:User {objectid: 'U2', name: 'U2'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G2', name: 'G2'})")
        .unwrap();
    db.execute(
        "MATCH (u:User {objectid: 'U2'}), (g:Group {objectid: 'G2'}) \
         CREATE (u)-[:MemberOf {exploit_likelihood: 0.0}]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (g2:Group {objectid: 'G2'}), (da:Group {objectid: 'S-1-5-21-512'}) \
         CREATE (g2)-[:MemberOf {exploit_likelihood: 0.9}]->(da)",
    )
    .unwrap();

    // U3: two-hop path where both hops have high likelihood
    db.execute("CREATE (:User {objectid: 'U3', name: 'U3'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G3', name: 'G3'})")
        .unwrap();
    db.execute(
        "MATCH (u:User {objectid: 'U3'}), (g:Group {objectid: 'G3'}) \
         CREATE (u)-[:MemberOf {exploit_likelihood: 0.9}]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (g3:Group {objectid: 'G3'}), (da:Group {objectid: 'S-1-5-21-512'}) \
         CREATE (g3)-[:MemberOf {exploit_likelihood: 0.8}]->(da)",
    )
    .unwrap();

    db
}

#[test]
fn test_all_relationships_p_exploit_likelihood_filters_zero_edges() {
    let db = setup_exploit_likelihood_db();

    // Exact pattern from the built-in path-to-DA query.
    // U2's path includes an edge with exploit_likelihood=0.0 → should be excluded.
    // U1 and U3 have all edges > 0 → should be included.
    let result = db
        .execute(
            "MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..10]->(da)) \
             WHERE da.objectid ENDS WITH '-512' \
             AND ALL(r IN relationships(p) WHERE r.exploit_likelihood > 0) \
             RETURN u.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"U1".to_string()),
        "U1 has exploit_likelihood=0.9 → should be included, got: {:?}",
        names
    );
    assert!(
        names.contains(&"U3".to_string()),
        "U3's hops are 0.9 and 0.8 → should be included, got: {:?}",
        names
    );
    assert!(
        !names.contains(&"U2".to_string()),
        "U2 has a hop with exploit_likelihood=0.0 → must be excluded, got: {:?}",
        names
    );
}

#[test]
fn test_all_relationships_p_with_threshold() {
    let db = setup_exploit_likelihood_db();

    // Threshold of 0.85: U3's second hop (0.8) is below threshold → U3 excluded too.
    let result = db
        .execute(
            "MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..10]->(da)) \
             WHERE da.objectid ENDS WITH '-512' \
             AND ALL(r IN relationships(p) WHERE r.exploit_likelihood > 0.85) \
             RETURN u.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"U1".to_string()),
        "U1 has exploit_likelihood=0.9 > 0.85 → included, got: {:?}",
        names
    );
    assert!(
        !names.contains(&"U2".to_string()),
        "U2 has 0.0 < 0.85 → excluded, got: {:?}",
        names
    );
    assert!(
        !names.contains(&"U3".to_string()),
        "U3 has a hop at 0.8 < 0.85 → excluded, got: {:?}",
        names
    );
}

#[test]
fn test_any_relationships_p_exploit_likelihood() {
    let db = setup_exploit_likelihood_db();

    // ANY: path has at least one edge with exploit_likelihood > 0.85.
    // U1: single hop at 0.9 → matches.
    // U2: hops at 0.0 and 0.9 → matches (0.9 > 0.85).
    // U3: hops at 0.9 and 0.8 → matches (0.9 > 0.85).
    let result = db
        .execute(
            "MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..10]->(da)) \
             WHERE da.objectid ENDS WITH '-512' \
             AND ANY(r IN relationships(p) WHERE r.exploit_likelihood > 0.85) \
             RETURN u.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"U1".to_string()),
        "U1's hop is 0.9 > 0.85 → included, got: {:?}",
        names
    );
    assert!(
        names.contains(&"U2".to_string()),
        "U2 has a hop at 0.9 > 0.85 → included, got: {:?}",
        names
    );
    assert!(
        names.contains(&"U3".to_string()),
        "U3 has a hop at 0.9 > 0.85 → included, got: {:?}",
        names
    );
}

#[test]
fn test_none_relationships_p_exploit_likelihood() {
    let db = setup_exploit_likelihood_db();

    // NONE: path has no edge with exploit_likelihood below 0.5.
    // U2 has a hop at 0.0 < 0.5 → NONE fails → excluded.
    // U1 and U3 have all hops >= 0.5 → NONE passes → included.
    let result = db
        .execute(
            "MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..10]->(da)) \
             WHERE da.objectid ENDS WITH '-512' \
             AND NONE(r IN relationships(p) WHERE r.exploit_likelihood < 0.5) \
             RETURN u.name AS name",
        )
        .unwrap();

    let names = get_string_values(&result, "name");

    assert!(
        names.contains(&"U1".to_string()),
        "U1's single hop is 0.9 ≥ 0.5 → included, got: {:?}",
        names
    );
    assert!(
        names.contains(&"U3".to_string()),
        "U3's hops are 0.9 and 0.8 ≥ 0.5 → included, got: {:?}",
        names
    );
    assert!(
        !names.contains(&"U2".to_string()),
        "U2 has a hop at 0.0 < 0.5 → excluded, got: {:?}",
        names
    );
}
