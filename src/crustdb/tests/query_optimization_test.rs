//! Integration tests for query optimizations.
//!
//! These tests verify that SQL pushdowns and limit pushdowns work correctly
//! at e2e-realistic graph sizes (~1000 nodes, ~6000 relationships).
//!
//! Mirrors the exact queries from the ADMapper e2e test suite:
//! - "Query with relationship": MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1
//! - "Query with type() function": MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5

use crustdb::{Database, PropertyValue, ResultValue};
use std::time::Instant;

/// Build a graph approximating the e2e BloodHound dataset.
///
/// Target: ~1000 nodes, ~6000 relationships (matching the real e2e test data).
fn build_e2e_scale_graph(db: &Database) -> (usize, usize) {
    // Create diverse node types (like real AD data)
    let user_count = 500;
    let group_count = 300;
    let computer_count = 200;
    let ou_count = 50;
    let domain_count = 3;

    for i in 0..user_count {
        db.execute(&format!(
            "CREATE (:User {{objectid: 'U_{i}', name: 'User{i}', enabled: true}})"
        ))
        .unwrap();
    }
    for i in 0..group_count {
        let hv = if i < 5 { ", tier: 0" } else { "" };
        db.execute(&format!(
            "CREATE (:Group {{objectid: 'G_{i}', name: 'Group{i}'{hv}}})"
        ))
        .unwrap();
    }
    for i in 0..computer_count {
        db.execute(&format!(
            "CREATE (:Computer {{objectid: 'C_{i}', name: 'Computer{i}'}})"
        ))
        .unwrap();
    }
    for i in 0..ou_count {
        db.execute(&format!(
            "CREATE (:OU {{objectid: 'OU_{i}', name: 'OrgUnit{i}'}})"
        ))
        .unwrap();
    }
    for i in 0..domain_count {
        db.execute(&format!(
            "CREATE (:Domain {{objectid: 'D_{i}', name: 'Domain{i}'}})"
        ))
        .unwrap();
    }

    // Build dense relationship structure to hit ~13000 edges.
    // Each user -> 2-3 groups (MemberOf): ~1200 rels
    for i in 0..user_count {
        for g in [i % group_count, (i * 7 + 13) % group_count] {
            insert_rel(db, &format!("U_{i}"), &format!("G_{g}"), "MemberOf");
        }
        if i % 3 == 0 {
            let g = (i * 11 + 37) % group_count;
            insert_rel(db, &format!("U_{i}"), &format!("G_{g}"), "MemberOf");
        }
    }
    // Group -> Group (MemberOf): ~2000 rels
    for i in 0..group_count {
        for offset in &[1, 3, 7, 15, 31, 67] {
            let target = (i + offset) % group_count;
            if target != i {
                insert_rel(db, &format!("G_{i}"), &format!("G_{target}"), "MemberOf");
            }
        }
    }
    // User -> Computer (AdminTo, HasSession): ~2000 rels
    for i in 0..user_count {
        let c = i % computer_count;
        insert_rel(db, &format!("U_{i}"), &format!("C_{c}"), "HasSession");
        if i % 3 == 0 {
            let c2 = (i * 3 + 7) % computer_count;
            insert_rel(db, &format!("U_{i}"), &format!("C_{c2}"), "AdminTo");
        }
    }
    // Computer -> Group (MemberOf): ~400 rels
    for i in 0..computer_count {
        let g = (i * 3) % group_count;
        insert_rel(db, &format!("C_{i}"), &format!("G_{g}"), "MemberOf");
        if i % 2 == 0 {
            let g2 = (i * 7 + 11) % group_count;
            insert_rel(db, &format!("C_{i}"), &format!("G_{g2}"), "MemberOf");
        }
    }
    // Group -> OU (Contains): ~300 rels
    for i in 0..group_count {
        let ou = i % ou_count;
        insert_rel(db, &format!("OU_{ou}"), &format!("G_{i}"), "Contains");
    }
    // OU -> Domain (Contains): ~50 rels
    for i in 0..ou_count {
        let d = i % domain_count;
        insert_rel(db, &format!("D_{d}"), &format!("OU_{i}"), "Contains");
    }
    // Extra cross-edges to reach ~13000: Group -> Computer (CanRDP, etc.)
    for i in 0..group_count {
        for offset in &[1, 5, 11, 23, 47, 97] {
            let c = (i + offset) % computer_count;
            let rel_type = match offset % 4 {
                0 => "CanRDP",
                1 => "CanPSRemote",
                2 => "ExecuteDCOM",
                _ => "GenericAll",
            };
            insert_rel(db, &format!("G_{i}"), &format!("C_{c}"), rel_type);
        }
    }
    // User -> User (some lateral movement edges)
    for i in (0..user_count).step_by(5) {
        let target = (i + 17) % user_count;
        if target != i {
            insert_rel(db, &format!("U_{i}"), &format!("U_{target}"), "CanRDP");
        }
    }

    let stats = db.stats().unwrap();
    (stats.node_count, stats.relationship_count)
}

fn insert_rel(db: &Database, source_oid: &str, target_oid: &str, rel_type: &str) {
    let source_id = db
        .find_node_by_property("objectid", source_oid)
        .unwrap()
        .unwrap_or_else(|| panic!("Node {} not found", source_oid));
    let target_id = db
        .find_node_by_property("objectid", target_oid)
        .unwrap()
        .unwrap_or_else(|| panic!("Node {} not found", target_oid));
    db.insert_relationships_batch(&[(
        source_id,
        target_id,
        rel_type.to_string(),
        serde_json::json!({}),
    )])
    .unwrap();
}

/// Shared setup: build an e2e-scale graph and return the database with stats.
fn setup() -> (Database, usize, usize) {
    let db = Database::in_memory().unwrap();
    let (nodes, rels) = build_e2e_scale_graph(&db);
    eprintln!("Test graph: {nodes} nodes, {rels} relationships");
    assert!(nodes > 1000, "Expected 1000+ nodes, got {nodes}");
    assert!(rels > 5000, "Expected 5000+ relationships, got {rels}");
    (db, nodes, rels)
}

// =============================================================================
// count(r) - RelationshipCountPushdown
// =============================================================================

#[test]
fn test_count_r_returns_correct_total() {
    let (db, _, expected_rels) = setup();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN count(r) AS edges")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let edges = match result.rows[0].get("edges") {
        Some(ResultValue::Property(PropertyValue::Integer(n))) => *n as usize,
        other => panic!("Expected integer, got {:?}", other),
    };
    assert_eq!(
        edges, expected_rels,
        "count(r) should equal total relationships"
    );
}

#[test]
fn test_count_r_with_limit_returns_correct_total() {
    let (db, _, expected_rels) = setup();
    // The LIMIT 1 wraps the aggregate, not the expand. Result is still the full count.
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let edges = match result.rows[0].get("edges") {
        Some(ResultValue::Property(PropertyValue::Integer(n))) => *n as usize,
        other => panic!("Expected integer, got {:?}", other),
    };
    assert_eq!(edges, expected_rels);
}

#[test]
fn test_count_r_is_fast_at_scale() {
    let (db, _, _) = setup();

    // Warm up
    let _ = db.execute("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1");

    // With SQL pushdown, this should be a single SELECT COUNT(*) -- sub-millisecond.
    let start = Instant::now();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    assert_eq!(result.rows.len(), 1);
    eprintln!("count(r) elapsed: {elapsed}ms");
    assert!(
        elapsed < 50,
        "count(r) took {elapsed}ms; expected <50ms with RelationshipCountPushdown"
    );
}

// =============================================================================
// type(r) LIMIT 5 - Expand limit pushdown
// =============================================================================

#[test]
fn test_type_r_limit_returns_correct_count() {
    let (db, _, _) = setup();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5")
        .unwrap();
    assert_eq!(result.rows.len(), 5, "Should return exactly 5 rows");
}

#[test]
fn test_type_r_limit_returns_valid_types() {
    let (db, _, _) = setup();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5")
        .unwrap();
    let known_types = [
        "MemberOf",
        "HasSession",
        "AdminTo",
        "Contains",
        "CanRDP",
        "CanPSRemote",
        "GenericAll",
    ];
    for row in &result.rows {
        let rel_type = match row.get("rel_type") {
            Some(ResultValue::Property(PropertyValue::String(s))) => s.as_str(),
            other => panic!("Expected string rel_type, got {:?}", other),
        };
        assert!(
            known_types.contains(&rel_type),
            "Unexpected relationship type: {rel_type}"
        );
    }
}

#[test]
fn test_type_r_limit_is_fast_at_scale() {
    let (db, _, _) = setup();

    // Warm up
    let _ = db.execute("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5");

    // With limit pushed into Expand, this should scan very few nodes before
    // finding 5 relationships -- should not scan all 6000+ relationships.
    let start = Instant::now();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    assert_eq!(result.rows.len(), 5);
    eprintln!("type(r) LIMIT 5 elapsed: {elapsed}ms");
    assert!(
        elapsed < 200,
        "type(r) LIMIT 5 took {elapsed}ms; expected <200ms with Expand limit pushdown"
    );
}

// =============================================================================
// RETURN DISTINCT type(r) - RelationshipTypesScan pushdown
// =============================================================================

#[test]
fn test_distinct_type_r_returns_all_types() {
    let (db, _, _) = setup();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN DISTINCT type(r) AS rel_type")
        .unwrap();
    // We created at least 7 distinct relationship types
    assert!(
        result.rows.len() >= 7,
        "Expected at least 7 distinct types, got {}",
        result.rows.len()
    );
}

#[test]
fn test_distinct_type_r_is_fast_at_scale() {
    let (db, _, _) = setup();

    // Warm up
    let _ = db.execute("MATCH (n)-[r]->(m) RETURN DISTINCT type(r) AS rel_type");

    // With RelationshipTypesScan, this should be a single SQL DISTINCT query
    // on the rel_type column -- not expanding all 6000+ relationships.
    let start = Instant::now();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN DISTINCT type(r) AS rel_type")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    assert!(result.rows.len() >= 7);
    eprintln!("DISTINCT type(r) elapsed: {elapsed}ms");
    assert!(
        elapsed < 50,
        "DISTINCT type(r) took {elapsed}ms; expected <50ms with RelationshipTypesScan"
    );
}

// =============================================================================
// VarLenExpand target_ids pre-resolution
// =============================================================================

#[test]
fn test_varlen_expand_target_labels_correct_results() {
    let (db, _, _) = setup();
    // Variable-length path from User to Computer — User->Computer via HasSession/AdminTo
    // or User->Group->Computer via MemberOf+CanRDP. Should only return Computer nodes.
    let result = db
        .execute("MATCH (a:User)-[*1..2]->(b:Computer) RETURN DISTINCT b.name AS comp LIMIT 10")
        .unwrap();
    assert!(
        !result.rows.is_empty(),
        "Should find paths from User to Computer"
    );
    for row in &result.rows {
        let name = match row.get("comp") {
            Some(ResultValue::Property(PropertyValue::String(s))) => s.as_str(),
            other => panic!("Expected string computer name, got {:?}", other),
        };
        assert!(
            name.starts_with("Computer"),
            "Target should be a Computer node, got: {name}"
        );
    }
}

#[test]
fn test_varlen_expand_target_labels_is_fast() {
    let (db, _, _) = setup();

    // Warm up
    let _ = db.execute("MATCH (a:User)-[*1..2]->(b:Computer) RETURN b.name LIMIT 5");

    // With target_ids pre-resolution, BFS uses HashSet::contains() instead
    // of scanning labels on every explored node.
    let start = Instant::now();
    let result = db
        .execute("MATCH (a:User)-[*1..2]->(b:Computer) RETURN b.name LIMIT 5")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    assert!(!result.rows.is_empty());
    eprintln!("VarLen User->Computer LIMIT 5 elapsed: {elapsed}ms");
    assert!(
        elapsed < 500,
        "VarLen expand took {elapsed}ms; expected <500ms with target_ids pre-resolution"
    );
}

// =============================================================================
// Regression: without optimizations, these queries would be catastrophically slow
// =============================================================================

#[test]
fn test_count_r_scales_linearly_not_quadratically() {
    // Verify that doubling the graph size doesn't quadruple query time.
    // Without RelationshipCountPushdown, time would be O(nodes * avg_degree).
    // With pushdown, it's O(1) -- a single SQL COUNT.
    let db_small = Database::in_memory().unwrap();
    for i in 0..100 {
        db_small
            .execute(&format!(
                "CREATE (:N {{id: 'A_{i}'}})-[:E]->(:N {{id: 'B_{i}'}})"
            ))
            .unwrap();
    }

    let db_large = Database::in_memory().unwrap();
    for i in 0..1000 {
        db_large
            .execute(&format!(
                "CREATE (:N {{id: 'A_{i}'}})-[:E]->(:N {{id: 'B_{i}'}})"
            ))
            .unwrap();
    }

    let query = "MATCH (n)-[r]->(m) RETURN count(r) AS edges";

    let start = Instant::now();
    db_small.execute(query).unwrap();
    let small_ms = start.elapsed().as_micros();

    let start = Instant::now();
    db_large.execute(query).unwrap();
    let large_ms = start.elapsed().as_micros();

    eprintln!("count(r): small={small_ms}us, large={large_ms}us (10x data)");
    // With SQL pushdown, 10x more data should take at most ~3x more time (not 100x).
    // In practice both should be sub-millisecond.
    assert!(
        large_ms < small_ms * 10 + 5000,
        "count(r) scales poorly: {small_ms}us vs {large_ms}us for 10x data"
    );
}
