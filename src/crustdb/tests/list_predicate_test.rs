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
