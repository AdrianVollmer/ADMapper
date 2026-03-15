//! Integration tests for UNION ALL support.
//!
//! Verifies that UNION ALL correctly concatenates results from multiple
//! query branches, including the "Domain Trusts" use case where we need
//! all domains (even those without trust relationships).

use crustdb::{Database, PropertyValue, ResultValue};

fn setup_domain_graph(db: &Database) {
    // Create three domains
    db.execute("CREATE (:Domain {objectid: 'D_ALPHA', name: 'alpha.local'})")
        .unwrap();
    db.execute("CREATE (:Domain {objectid: 'D_BETA', name: 'beta.local'})")
        .unwrap();
    db.execute("CREATE (:Domain {objectid: 'D_GAMMA', name: 'gamma.local'})")
        .unwrap();

    // Create a trust: alpha trusts beta (only one trust, gamma is isolated)
    let alpha_id = db
        .find_node_by_property("objectid", "D_ALPHA")
        .unwrap()
        .expect("alpha node");
    let beta_id = db
        .find_node_by_property("objectid", "D_BETA")
        .unwrap()
        .expect("beta node");

    db.insert_relationships_batch(&[(
        alpha_id,
        beta_id,
        "TrustedBy".to_string(),
        serde_json::json!({}),
    )])
    .unwrap();
}

#[test]
fn test_union_all_basic() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G1', name: 'Admins'})")
        .unwrap();

    // Use matching column aliases across branches
    let result = db
        .execute(
            "MATCH (u:User) RETURN u.name AS name \
             UNION ALL \
             MATCH (g:Group) RETURN g.name AS name",
        )
        .unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 2);

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| {
            r.values.values().next().and_then(|v| match v {
                ResultValue::Property(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            })
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Admins".to_string()));
}

#[test]
fn test_union_all_preserves_duplicates() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();

    // Both branches return Alice - UNION ALL should keep both
    let result = db
        .execute("MATCH (u:User) RETURN u.name UNION ALL MATCH (u:User) RETURN u.name")
        .unwrap();

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_union_all_column_count_mismatch() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();

    let result = db
        .execute("MATCH (u:User) RETURN u.name, u.objectid UNION ALL MATCH (u:User) RETURN u.name");
    assert!(result.is_err(), "Should reject mismatched column counts");
}

#[test]
fn test_union_all_column_name_mismatch() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G1', name: 'Admins'})")
        .unwrap();

    // Different column names without explicit alias should be rejected
    let result = db.execute("MATCH (u:User) RETURN u.name UNION ALL MATCH (g:Group) RETURN g.name");
    assert!(result.is_err(), "Should reject mismatched column names");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("same return column names"),
        "Error should mention column names, got: {err}"
    );
}

#[test]
fn test_union_all_three_branches() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G1', name: 'Admins'})")
        .unwrap();
    db.execute("CREATE (:Computer {objectid: 'C1', name: 'DC01'})")
        .unwrap();

    // Use matching column aliases
    let result = db
        .execute(
            "MATCH (u:User) RETURN u.name AS name \
             UNION ALL MATCH (g:Group) RETURN g.name AS name \
             UNION ALL MATCH (c:Computer) RETURN c.name AS name",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_union_all_three_branches_rejects_name_mismatch() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'G1', name: 'Admins'})")
        .unwrap();
    db.execute("CREATE (:Computer {objectid: 'C1', name: 'DC01'})")
        .unwrap();

    // Different column names across branches should be rejected
    let result = db.execute(
        "MATCH (u:User) RETURN u.name \
         UNION ALL MATCH (g:Group) RETURN g.name \
         UNION ALL MATCH (c:Computer) RETURN c.name",
    );
    assert!(result.is_err(), "Should reject mismatched column names");
}

#[test]
fn test_union_all_empty_branch() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:User {objectid: 'U1', name: 'Alice'})")
        .unwrap();

    // No groups exist, so second branch returns nothing.
    // Use matching aliases.
    let result = db
        .execute(
            "MATCH (u:User) RETURN u.name AS name \
             UNION ALL \
             MATCH (g:Group) RETURN g.name AS name",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_domain_trusts_query_returns_all_domains() {
    let db = Database::in_memory().unwrap();
    setup_domain_graph(&db);

    // The actual query used by ADMapper's "Domain Trusts" built-in
    // (with matching column aliases per openCypher UNION rules)
    let result = db
        .execute(
            "MATCH (d:Domain) RETURN d AS result \
             UNION ALL \
             MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p AS result",
        )
        .unwrap();

    // First branch: 3 domain nodes
    // Second branch: 1 path (alpha -> beta)
    assert_eq!(result.rows.len(), 4);

    // Verify we get all three domains as nodes
    let node_count = result
        .rows
        .iter()
        .filter(|r| {
            r.values
                .values()
                .any(|v| matches!(v, ResultValue::Node { .. }))
        })
        .count();
    assert_eq!(node_count, 3, "Should return all 3 domains");

    // Verify we get the trust path
    let path_count = result
        .rows
        .iter()
        .filter(|r| {
            r.values
                .values()
                .any(|v| matches!(v, ResultValue::Path { .. }))
        })
        .count();
    assert_eq!(path_count, 1, "Should return 1 trust path");
}

#[test]
fn test_domain_trusts_mismatched_columns_rejected() {
    // The old Domain Trusts query used different return variable names (d vs p).
    // Per openCypher spec, this must be rejected.
    let db = Database::in_memory().unwrap();
    setup_domain_graph(&db);

    let result = db.execute(
        "MATCH (d:Domain) RETURN d \
         UNION ALL \
         MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p",
    );

    assert!(
        result.is_err(),
        "Should reject UNION ALL with mismatched column names (d vs p)"
    );
}

#[test]
fn test_domain_trusts_column_name_consistency() {
    // With matching column aliases, all rows should be accessible by "result"
    let db = Database::in_memory().unwrap();
    setup_domain_graph(&db);

    let result = db
        .execute(
            "MATCH (d:Domain) RETURN d AS result \
             UNION ALL \
             MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p AS result",
        )
        .unwrap();

    assert_eq!(result.columns, vec!["result"]);

    for (i, row) in result.rows.iter().enumerate() {
        assert!(
            row.values.get("result").is_some(),
            "Row {} should have value under column name 'result', but keys are: {:?}",
            i,
            row.values.keys().collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_domain_trusts_no_trusts_still_returns_domains() {
    let db = Database::in_memory().unwrap();

    // Create domains with no trust relationships
    db.execute("CREATE (:Domain {objectid: 'D_ONE', name: 'one.local'})")
        .unwrap();
    db.execute("CREATE (:Domain {objectid: 'D_TWO', name: 'two.local'})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (d:Domain) RETURN d AS result \
             UNION ALL \
             MATCH p = (d:Domain)-[:TrustedBy]->(t:Domain) RETURN p AS result",
        )
        .unwrap();

    // Should still return both domains even without trusts
    assert_eq!(result.rows.len(), 2);

    let node_count = result
        .rows
        .iter()
        .filter(|r| {
            r.values
                .values()
                .any(|v| matches!(v, ResultValue::Node { .. }))
        })
        .count();
    assert_eq!(node_count, 2, "Should return both domains");
}

// =============================================================================
// UNION (with deduplication) Tests
// =============================================================================

#[test]
fn test_union_dedup_basic() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Employee {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Employee {name: 'Charlie'})").unwrap();

    // UNION should deduplicate Alice
    let result = db
        .execute(
            "MATCH (p:Person) RETURN p.name AS name \
             UNION \
             MATCH (e:Employee) RETURN e.name AS name",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3, "Alice should appear only once");

    let names: std::collections::HashSet<String> = result
        .rows
        .iter()
        .map(|r| match r.get("name").unwrap() {
            ResultValue::Property(PropertyValue::String(s)) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));
    assert!(names.contains("Charlie"));
}

#[test]
fn test_union_all_preserves_dupes_union_removes_them() {
    let db = Database::in_memory().unwrap();

    db.execute("CREATE (:A {val: 1})").unwrap();
    db.execute("CREATE (:B {val: 1})").unwrap();

    // UNION ALL: should return 2 rows (both val=1)
    let result_all = db
        .execute(
            "MATCH (a:A) RETURN a.val AS v \
             UNION ALL \
             MATCH (b:B) RETURN b.val AS v",
        )
        .unwrap();
    assert_eq!(result_all.rows.len(), 2);

    // UNION: should return 1 row (deduped)
    let result_union = db
        .execute(
            "MATCH (a:A) RETURN a.val AS v \
             UNION \
             MATCH (b:B) RETURN b.val AS v",
        )
        .unwrap();
    assert_eq!(result_union.rows.len(), 1);
}
