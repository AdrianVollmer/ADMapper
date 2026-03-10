use crustdb::Database;

fn setup_db() -> Database {
    Database::in_memory().unwrap()
}

#[test]
fn test_cross_join_exceeds_limit() {
    let mut db = setup_db();
    // Create 20 nodes of two labels
    for i in 0..20 {
        db.execute(&format!("CREATE (:A {{n: {}}})", i)).unwrap();
        db.execute(&format!("CREATE (:B {{n: {}}})", i)).unwrap();
    }
    // Cross join of 20x20 = 400 bindings
    db.set_max_intermediate_bindings(Some(100));
    let result = db.execute("MATCH (a:A), (b:B) RETURN a.n, b.n");
    assert!(result.is_err(), "Expected ResourceLimit error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("intermediate results"),
        "Error should mention intermediate results, got: {}",
        err
    );
}

#[test]
fn test_cross_join_within_limit() {
    let mut db = setup_db();
    for i in 0..5 {
        db.execute(&format!("CREATE (:A {{n: {}}})", i)).unwrap();
        db.execute(&format!("CREATE (:B {{n: {}}})", i)).unwrap();
    }
    // Cross join of 5x5 = 25 bindings, well within limit
    db.set_max_intermediate_bindings(Some(100));
    let result = db.execute("MATCH (a:A), (b:B) RETURN a.n, b.n").unwrap();
    assert_eq!(result.rows.len(), 25);
}

#[test]
fn test_unlimited_bindings_by_default() {
    let db = setup_db();
    // Default is unlimited - should not error even with larger result sets
    for i in 0..30 {
        db.execute(&format!("CREATE (:X {{n: {}}})", i)).unwrap();
    }
    let result = db.execute("MATCH (x:X) RETURN x.n").unwrap();
    assert_eq!(result.rows.len(), 30);
}

#[test]
fn test_variable_length_expand_respects_limit() {
    let mut db = setup_db();
    // Create a chain: n0 -> n1 -> n2 -> ... -> n19
    for i in 0..20 {
        db.execute(&format!("CREATE (:Chain {{idx: {}}})", i))
            .unwrap();
    }
    for i in 0..19 {
        db.execute(&format!(
            "MATCH (a:Chain {{idx: {}}}), (b:Chain {{idx: {}}}) CREATE (a)-[:NEXT]->(b)",
            i,
            i + 1
        ))
        .unwrap();
    }

    // Variable length expand from n0 with a tight limit
    db.set_max_intermediate_bindings(Some(5));
    let result = db.execute("MATCH (a:Chain {idx: 0})-[:NEXT*1..20]->(b:Chain) RETURN b.idx");
    assert!(result.is_err(), "Expected ResourceLimit error for deep BFS");
}

#[test]
fn test_normal_query_unaffected_by_high_limit() {
    let mut db = setup_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.set_max_intermediate_bindings(Some(1_000_000));
    let result = db.execute("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_set_limit_to_none_removes_safeguard() {
    let mut db = setup_db();
    for i in 0..10 {
        db.execute(&format!("CREATE (:Y {{n: {}}})", i)).unwrap();
        db.execute(&format!("CREATE (:Z {{n: {}}})", i)).unwrap();
    }

    // Set a tight limit, then remove it
    db.set_max_intermediate_bindings(Some(10));
    let result = db.execute("MATCH (y:Y), (z:Z) RETURN y.n, z.n");
    assert!(result.is_err());

    db.set_max_intermediate_bindings(None);
    let result = db.execute("MATCH (y:Y), (z:Z) RETURN y.n, z.n").unwrap();
    assert_eq!(result.rows.len(), 100);
}
