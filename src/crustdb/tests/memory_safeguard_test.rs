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

// =============================================================================
// BFS frontier limit tests
// =============================================================================

/// Build a dense "star" graph where a hub connects to many spokes,
/// and each spoke connects to the next. This creates a graph where
/// BFS frontier grows quickly with depth.
fn setup_dense_graph(db: &Database, fan_out: usize) {
    db.execute("CREATE (:Hub {name: 'hub'})").unwrap();
    for i in 0..fan_out {
        db.execute(&format!("CREATE (:Spoke {{idx: {}}})", i))
            .unwrap();
    }
    for i in 0..fan_out {
        db.execute(&format!(
            "MATCH (h:Hub), (s:Spoke {{idx: {}}}) CREATE (h)-[:LINK]->(s)",
            i
        ))
        .unwrap();
    }
    // Connect spokes in a ring so BFS has depth > 1
    for i in 0..fan_out {
        let next = (i + 1) % fan_out;
        db.execute(&format!(
            "MATCH (a:Spoke {{idx: {}}}), (b:Spoke {{idx: {}}}) CREATE (a)-[:LINK]->(b)",
            i, next
        ))
        .unwrap();
    }
}

#[test]
fn test_frontier_limit_triggers_on_variable_length_expand() {
    let mut db = setup_db();
    setup_dense_graph(&db, 50);

    // With a very tight frontier limit, the BFS should fail
    db.set_max_frontier_entries(Some(10));
    let result = db.execute("MATCH (h:Hub {name: 'hub'})-[:LINK*1..3]->(s) RETURN s.idx");
    assert!(result.is_err(), "Expected frontier limit error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BFS frontier"),
        "Error should mention BFS frontier, got: {}",
        err
    );
}

#[test]
fn test_frontier_limit_triggers_on_shortest_path() {
    let mut db = setup_db();
    setup_dense_graph(&db, 50);

    db.set_max_frontier_entries(Some(10));
    let result = db.execute(
        "MATCH p = shortestPath((h:Hub {name: 'hub'})-[:LINK*1..3]->(s:Spoke {idx: 25})) RETURN p",
    );
    assert!(result.is_err(), "Expected frontier limit error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BFS frontier"),
        "Error should mention BFS frontier, got: {}",
        err
    );
}

#[test]
fn test_frontier_limit_allows_small_queries() {
    let mut db = setup_db();
    // Small chain: A -> B -> C
    db.execute(
        "CREATE (:Start {name: 'A'})-[:NEXT]->(:Mid {name: 'B'})-[:NEXT]->(:End {name: 'C'})",
    )
    .unwrap();

    // Frontier limit of 100 is plenty for a 3-node chain
    db.set_max_frontier_entries(Some(100));
    let result = db
        .execute("MATCH (a:Start {name: 'A'})-[:NEXT*1..2]->(b) RETURN b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2); // B and C
}

#[test]
fn test_frontier_limit_default_allows_normal_queries() {
    let db = setup_db();
    setup_dense_graph(&db, 50);

    // Default frontier limit (2M) is high enough for normal queries
    let result = db
        .execute("MATCH (h:Hub {name: 'hub'})-[:LINK*1..2]->(s) RETURN s.idx")
        .unwrap();
    assert!(!result.rows.is_empty());
}
