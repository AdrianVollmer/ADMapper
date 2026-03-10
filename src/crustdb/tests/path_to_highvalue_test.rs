//! Integration test for "path to high-value" queries.
//!
//! This test isolates the performance bottleneck seen in the ADMapper
//! "Node status API works" e2e test, which runs a variable-length path
//! query to find paths from a source node to any high-value target.
//!
//! The query pattern is:
//! ```cypher
//! MATCH p = (a)-[*1..20]->(b)
//! WHERE a.objectid = '...' AND b.is_highvalue = true
//! RETURN length(p) AS hops
//! LIMIT 1
//! ```

use crustdb::{Database, EntityCacheConfig};
use std::time::Instant;

/// Helper to create a relationship between two nodes by their objectids.
fn create_relationship(db: &Database, source_oid: &str, target_oid: &str, rel_type: &str) {
    let source_id = db
        .find_node_by_property("objectid", source_oid)
        .unwrap()
        .unwrap_or_else(|| panic!("Source node {} not found", source_oid));
    let target_id = db
        .find_node_by_property("objectid", target_oid)
        .unwrap()
        .unwrap_or_else(|| panic!("Target node {} not found", target_oid));

    db.insert_relationships_batch(&[(
        source_id,
        target_id,
        rel_type.to_string(),
        serde_json::json!({}),
    )])
    .unwrap();
}

/// Create a test graph that mimics a BloodHound AD structure:
/// - Multiple "User" nodes
/// - Some "Group" nodes (some marked as high-value)
/// - MemberOf relationships forming a hierarchy
fn setup_ad_like_graph(db: &Database, num_users: usize, num_groups: usize, chain_length: usize) {
    // Create high-value groups (Domain Admins, Enterprise Admins, etc.)
    for i in 0..3 {
        db.execute(&format!(
            "CREATE (:Group {{objectid: 'HV_GROUP_{}', name: 'HighValueGroup{}', is_highvalue: true}})",
            i, i
        ))
        .unwrap();
    }

    // Create regular groups
    for i in 0..num_groups {
        db.execute(&format!(
            "CREATE (:Group {{objectid: 'GROUP_{}', name: 'Group{}'}})",
            i, i
        ))
        .unwrap();
    }

    // Create users
    for i in 0..num_users {
        db.execute(&format!(
            "CREATE (:User {{objectid: 'USER_{}', name: 'User{}'}})",
            i, i
        ))
        .unwrap();
    }

    // Create the chain: GROUP_0 -> GROUP_1 -> ... -> GROUP_{chain_length-1} -> HV_GROUP_0
    for i in 0..chain_length.saturating_sub(1) {
        create_relationship(
            db,
            &format!("GROUP_{}", i),
            &format!("GROUP_{}", i + 1),
            "MemberOf",
        );
    }

    // Connect last group in chain to high-value group
    if chain_length > 0 {
        let last_group = chain_length.saturating_sub(1);
        create_relationship(
            db,
            &format!("GROUP_{}", last_group),
            "HV_GROUP_0",
            "MemberOf",
        );
    }

    // Connect users to groups - every 10th user connects to GROUP_0
    for i in (0..num_users).step_by(10) {
        create_relationship(db, &format!("USER_{}", i), "GROUP_0", "MemberOf");
    }

    // Add cross-connections for more realistic graph
    for i in (0..num_groups.saturating_sub(2)).step_by(3) {
        create_relationship(
            db,
            &format!("GROUP_{}", i),
            &format!("GROUP_{}", i + 2),
            "MemberOf",
        );
    }
}

/// The slow query from node_status: find path to high-value node
fn run_path_to_highvalue_query(db: &Database, source_objectid: &str) -> (bool, u128) {
    let query = format!(
        "MATCH (a)-[*1..20]->(b) \
         WHERE a.objectid = '{}' AND b.is_highvalue = true \
         RETURN b.objectid",
        source_objectid
    );

    let start = Instant::now();
    let result = db.execute(&query).unwrap();
    let elapsed = start.elapsed().as_millis();

    let found = !result.rows.is_empty();
    (found, elapsed)
}

/// The exact query from `check_path_to_condition` in the backend handler.
/// This is what the e2e "Node status API works" test exercises.
fn run_exact_e2e_query(db: &Database, source_objectid: &str) -> (Option<i64>, u128) {
    let query = format!(
        "MATCH p = (a)-[*1..20]->(b) \
         WHERE a.objectid = '{}' AND (b.is_highvalue = true) \
         RETURN length(p) AS hops LIMIT 1",
        source_objectid
    );

    let start = Instant::now();
    let result = db.execute(&query).unwrap();
    let elapsed = start.elapsed().as_millis();

    let hops = result.rows.first().and_then(|row| match row.get("hops") {
        Some(crustdb::ResultValue::Property(crustdb::PropertyValue::Integer(n))) => Some(*n),
        _ => None,
    });
    (hops, elapsed)
}

#[test]
fn test_path_to_highvalue_small_graph() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(10_000));

    // Small graph: 100 users, 20 groups, chain of 5
    setup_ad_like_graph(&db, 100, 20, 5);

    let stats = db.stats().unwrap();
    println!(
        "Small graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // Debug: verify the chain exists
    let chain_check = db
        .execute("MATCH (a:Group {objectid: 'GROUP_0'})-[:MemberOf]->(b) RETURN b.objectid")
        .unwrap();
    println!("GROUP_0 connects to: {:?}", chain_check.rows);

    let hv_check = db
        .execute("MATCH (a:Group {is_highvalue: true}) RETURN a.objectid")
        .unwrap();
    println!("High-value groups: {:?}", hv_check.rows);

    let user0_check = db
        .execute("MATCH (u:User {objectid: 'USER_0'})-[:MemberOf]->(g) RETURN g.objectid")
        .unwrap();
    println!("USER_0 is member of: {:?}", user0_check.rows);

    // Test from a user that HAS a path (USER_0 -> GROUP_0 -> ... -> HV_GROUP_0)
    let (found, elapsed) = run_path_to_highvalue_query(&db, "USER_0");
    println!("USER_0 (has path): found={}, elapsed={}ms", found, elapsed);
    assert!(found, "USER_0 should have path to high-value");
    assert!(elapsed < 500, "Query took too long: {}ms", elapsed);

    // Test from a user that has NO path (USER_1 is not connected)
    let (found, elapsed) = run_path_to_highvalue_query(&db, "USER_1");
    println!("USER_1 (no path): found={}, elapsed={}ms", found, elapsed);
    assert!(!found, "USER_1 should NOT have path to high-value");
    assert!(elapsed < 500, "Query took too long: {}ms", elapsed);
}

#[test]
fn test_path_to_highvalue_medium_graph() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(100_000));

    // Medium graph: 1000 users, 100 groups, chain of 10
    setup_ad_like_graph(&db, 1000, 100, 10);

    let stats = db.stats().unwrap();
    println!(
        "Medium graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // Warm up cache
    let _ = run_path_to_highvalue_query(&db, "USER_0");

    // Test query performance
    let (found, elapsed) = run_path_to_highvalue_query(&db, "USER_0");
    println!("USER_0 (has path): found={}, elapsed={}ms", found, elapsed);
    assert!(found);
    assert!(elapsed < 1000, "Query took too long: {}ms", elapsed);

    let (found, elapsed) = run_path_to_highvalue_query(&db, "USER_1");
    println!("USER_1 (no path): found={}, elapsed={}ms", found, elapsed);
    assert!(!found);
    assert!(elapsed < 1000, "Query took too long: {}ms", elapsed);

    // Check cache stats
    let cache_stats = db.entity_cache_stats();
    println!(
        "Cache stats: node_hits={}, node_misses={}, hit_rate={:.1}%",
        cache_stats.node_hits,
        cache_stats.node_misses,
        cache_stats.node_hit_rate() * 100.0
    );
}

#[test]
fn test_path_to_highvalue_large_graph() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));

    // Large graph: 5000 users, 500 groups, chain of 15
    setup_ad_like_graph(&db, 5000, 500, 15);

    let stats = db.stats().unwrap();
    println!(
        "Large graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // Warm up
    let _ = run_path_to_highvalue_query(&db, "USER_0");

    // Run multiple queries to test cache effectiveness
    let mut total_elapsed = 0;
    for i in (0..100).step_by(10) {
        let source = format!("USER_{}", i);
        let (found, elapsed) = run_path_to_highvalue_query(&db, &source);
        total_elapsed += elapsed;
        if i == 0 {
            assert!(found, "USER_0 should have path");
        }
        println!("{}: found={}, elapsed={}ms", source, found, elapsed);
    }

    let avg_elapsed = total_elapsed / 10;
    println!("Average query time: {}ms", avg_elapsed);

    // Check cache effectiveness
    let cache_stats = db.entity_cache_stats();
    println!(
        "Cache stats: node_hits={}, node_misses={}, hit_rate={:.1}%",
        cache_stats.node_hits,
        cache_stats.node_misses,
        cache_stats.node_hit_rate() * 100.0
    );
    println!(
        "Relationship cache: hits={}, misses={}, hit_rate={:.1}%",
        cache_stats.relationship_hits,
        cache_stats.relationship_misses,
        cache_stats.relationship_hit_rate() * 100.0
    );

    // Performance assertion
    assert!(
        avg_elapsed < 2000,
        "Average query time too slow: {}ms",
        avg_elapsed
    );
}

/// Test mirroring the exact e2e "Node status API works" query.
///
/// The e2e test runs check_path_to_condition which executes:
///   MATCH p = (a)-[*1..20]->(b)
///   WHERE a.objectid = '...' AND (b.is_highvalue = true)
///   RETURN length(p) AS hops LIMIT 1
///
/// The e2e test failed at 16.9s against a 3s timeout with 1,480 nodes and
/// 13,674 relationships. This integration test catches the same performance
/// issue with a comparable graph size.
#[test]
fn test_exact_e2e_node_status_query() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));

    // Build a graph approximating the e2e dataset size (~1500 nodes, ~5000+ rels).
    // setup_ad_like_graph creates the skeleton; we add extra cross-connections below.
    setup_ad_like_graph(&db, 1000, 500, 10);

    // Add denser group-to-group connections to approximate real AD structures.
    // Real AD graphs have many MemberOf and other relationship types.
    for i in 0..500 {
        for offset in &[5, 10, 20, 50] {
            let target = (i + offset) % 500;
            if target != i {
                create_relationship(
                    &db,
                    &format!("GROUP_{}", i),
                    &format!("GROUP_{}", target),
                    "MemberOf",
                );
            }
        }
    }
    // Connect users to groups (skip USER_1 to keep it isolated for testing)
    for i in 0..1000 {
        if i == 1 {
            continue;
        }
        let group = i % 500;
        create_relationship(
            &db,
            &format!("USER_{}", i),
            &format!("GROUP_{}", group),
            "MemberOf",
        );
    }

    let stats = db.stats().unwrap();
    println!(
        "E2e-like graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // Warm up cache (mirrors real-world: the server has been running)
    let _ = run_exact_e2e_query(&db, "USER_0");

    // Test 1: Source with a path to high-value (USER_0 -> GROUP_0 -> ... -> HV_GROUP_0)
    let (hops, elapsed) = run_exact_e2e_query(&db, "USER_0");
    println!("USER_0 (has path): hops={:?}, elapsed={}ms", hops, elapsed);
    assert!(hops.is_some(), "USER_0 should have path to high-value");
    assert!(
        hops.unwrap() > 0,
        "Path length should be positive, got {}",
        hops.unwrap()
    );
    // The e2e test has a 3s timeout. We assert under 2s here to have margin.
    assert!(
        elapsed < 2000,
        "Query with path took {}ms, exceeds 2s budget (e2e timeout is 3s)",
        elapsed
    );

    // Test 2: Source with NO path (USER_1 is not connected to any group)
    // This is the worst case -- must explore nothing or everything reachable.
    let (hops, elapsed) = run_exact_e2e_query(&db, "USER_1");
    println!("USER_1 (no path): hops={:?}, elapsed={}ms", hops, elapsed);
    assert!(hops.is_none(), "USER_1 should have no path to high-value");
    assert!(
        elapsed < 2000,
        "Query without path took {}ms, exceeds 2s budget (e2e timeout is 3s)",
        elapsed
    );

    // Test 3: Run multiple queries in sequence (mimics multiple API calls)
    let mut max_elapsed = 0;
    for i in (0..100).step_by(10) {
        let source = format!("USER_{}", i);
        let (_hops, elapsed) = run_exact_e2e_query(&db, &source);
        if elapsed > max_elapsed {
            max_elapsed = elapsed;
        }
    }
    println!("Max query time across 10 users: {}ms", max_elapsed);
    assert!(
        max_elapsed < 2000,
        "Slowest query took {}ms, exceeds 2s budget",
        max_elapsed
    );
}

/// Test mirroring e2e "Query with relationship": MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1
///
/// This was taking ~1.5s in e2e (vs ~15ms in FalkorDB) because it expanded all
/// 13,674 relationships before counting. With RelationshipCountPushdown, this
/// should use SQL COUNT directly.
#[test]
fn test_e2e_query_with_relationship_count() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));

    // Build a graph with many relationships
    setup_ad_like_graph(&db, 1000, 500, 10);
    for i in 0..500 {
        for offset in &[5, 10, 20, 50] {
            let target = (i + offset) % 500;
            if target != i {
                create_relationship(
                    &db,
                    &format!("GROUP_{}", i),
                    &format!("GROUP_{}", target),
                    "MemberOf",
                );
            }
        }
    }
    for i in 0..1000 {
        let group = i % 500;
        create_relationship(
            &db,
            &format!("USER_{}", i),
            &format!("GROUP_{}", group),
            "MemberOf",
        );
    }

    let stats = db.stats().unwrap();
    println!(
        "Graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // The exact e2e query
    let start = Instant::now();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    println!("count(r) = {:?}, elapsed={}ms", result.rows, elapsed);
    assert_eq!(result.rows.len(), 1, "Should return exactly 1 row");
    assert!(
        elapsed < 100,
        "count(r) took {}ms, should be <100ms with SQL pushdown",
        elapsed
    );
}

/// Test mirroring e2e "Query with type() function": MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5
///
/// This was taking ~1.6s in e2e because it expanded all relationships before
/// applying LIMIT. With Expand limit pushdown, it should stop after 5 results.
#[test]
fn test_e2e_query_with_type_function_limit() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(500_000));

    // Build a graph with many relationships
    setup_ad_like_graph(&db, 1000, 500, 10);
    for i in 0..500 {
        for offset in &[5, 10, 20, 50] {
            let target = (i + offset) % 500;
            if target != i {
                create_relationship(
                    &db,
                    &format!("GROUP_{}", i),
                    &format!("GROUP_{}", target),
                    "MemberOf",
                );
            }
        }
    }
    for i in 0..1000 {
        let group = i % 500;
        create_relationship(
            &db,
            &format!("USER_{}", i),
            &format!("GROUP_{}", group),
            "MemberOf",
        );
    }

    let stats = db.stats().unwrap();
    println!(
        "Graph: {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // The exact e2e query
    let start = Instant::now();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5")
        .unwrap();
    let elapsed = start.elapsed().as_millis();

    println!(
        "type(r) rows = {}, elapsed={}ms",
        result.rows.len(),
        elapsed
    );
    assert_eq!(result.rows.len(), 5, "Should return exactly 5 rows");
    assert!(
        elapsed < 500,
        "type(r) LIMIT 5 took {}ms, should be <500ms with Expand limit pushdown",
        elapsed
    );
}

/// Test that LIMIT works correctly with variable-length paths and WHERE clause filters.
///
/// This is a regression test for the bug where adding LIMIT to a query with WHERE
/// filters on variable-length paths returns empty results.
#[test]
fn test_limit_with_varlen_path_and_where_clause() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(10_000));

    // Small graph: USER_0 -> GROUP_0 -> GROUP_1 -> ... -> GROUP_4 -> HV_GROUP_0
    setup_ad_like_graph(&db, 100, 20, 5);

    // Sanity: the query without LIMIT works
    let without_limit = db
        .execute(
            "MATCH (a)-[*1..20]->(b) \
             WHERE a.objectid = 'USER_0' AND b.is_highvalue = true \
             RETURN b.objectid",
        )
        .unwrap();
    assert!(
        !without_limit.rows.is_empty(),
        "Query without LIMIT should return results"
    );

    // The same query with LIMIT 1 must also return results
    let with_limit = db
        .execute(
            "MATCH (a)-[*1..20]->(b) \
             WHERE a.objectid = 'USER_0' AND b.is_highvalue = true \
             RETURN b.objectid LIMIT 1",
        )
        .unwrap();
    assert_eq!(
        with_limit.rows.len(),
        1,
        "LIMIT 1 should return exactly 1 result, got {}",
        with_limit.rows.len()
    );

    // Also test LIMIT with a higher count
    let with_limit_10 = db
        .execute(
            "MATCH (a)-[*1..20]->(b) \
             WHERE a.objectid = 'USER_0' AND b.is_highvalue = true \
             RETURN b.objectid LIMIT 10",
        )
        .unwrap();
    assert!(
        !with_limit_10.rows.is_empty(),
        "LIMIT 10 should also return results"
    );
    assert!(
        with_limit_10.rows.len() <= 10,
        "LIMIT 10 should return at most 10 results"
    );

    // Test LIMIT with a user that has no path - should still return empty
    let no_path = db
        .execute(
            "MATCH (a)-[*1..20]->(b) \
             WHERE a.objectid = 'USER_1' AND b.is_highvalue = true \
             RETURN b.objectid LIMIT 1",
        )
        .unwrap();
    assert!(
        no_path.rows.is_empty(),
        "USER_1 has no path, LIMIT 1 should return empty"
    );
}

/// Test that inline source property filter + WHERE target filter works correctly.
///
/// Regression test for the bug where combining inline source properties with
/// WHERE clause boolean target filters returns empty results.
#[test]
fn test_inline_source_filter_with_where_target_filter() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(10_000));

    // Minimal graph matching the exact issue reproduction
    db.execute("CREATE (:Group {objectid: 'HV_GROUP', is_highvalue: true})")
        .unwrap();
    db.execute("CREATE (:Group {objectid: 'GROUP_0'})")
        .unwrap();
    db.execute("CREATE (:User {objectid: 'USER_0'})").unwrap();

    let user_id = db
        .find_node_by_property("objectid", "USER_0")
        .unwrap()
        .unwrap();
    let group_id = db
        .find_node_by_property("objectid", "GROUP_0")
        .unwrap()
        .unwrap();
    let hv_id = db
        .find_node_by_property("objectid", "HV_GROUP")
        .unwrap()
        .unwrap();

    db.insert_relationships_batch(&[
        (
            user_id,
            group_id,
            "MemberOf".to_string(),
            serde_json::json!({}),
        ),
        (
            group_id,
            hv_id,
            "MemberOf".to_string(),
            serde_json::json!({}),
        ),
    ])
    .unwrap();

    // Baseline: both filters in WHERE clause (known to work)
    let baseline = db
        .execute(
            "MATCH (a)-[*1..20]->(b) \
             WHERE a.objectid = 'USER_0' AND b.is_highvalue = true \
             RETURN b.objectid",
        )
        .unwrap();
    assert!(
        !baseline.rows.is_empty(),
        "Baseline query should return results"
    );

    // Bug case: inline source filter + WHERE target filter
    let inline_source = db
        .execute(
            "MATCH (a {objectid: 'USER_0'})-[*1..20]->(b) \
             WHERE b.is_highvalue = true \
             RETURN b.objectid",
        )
        .unwrap();
    assert!(
        !inline_source.rows.is_empty(),
        "Inline source filter + WHERE target filter should return results"
    );
    assert_eq!(
        baseline.rows.len(),
        inline_source.rows.len(),
        "Both query forms should return the same number of results"
    );

    // Also test: inline source filter WITH label + WHERE target filter (reported as working)
    let with_label = db
        .execute(
            "MATCH (a:User {objectid: 'USER_0'})-[*1..20]->(b) \
             WHERE b.is_highvalue = true \
             RETURN b.objectid",
        )
        .unwrap();
    assert_eq!(
        baseline.rows.len(),
        with_label.rows.len(),
        "With-label variant should return same results"
    );

    // And: inline filters on both source and target
    let inline_both = db
        .execute(
            "MATCH (a {objectid: 'USER_0'})-[*1..20]->(b {is_highvalue: true}) \
             RETURN b.objectid",
        )
        .unwrap();
    assert!(
        !inline_both.rows.is_empty(),
        "Inline filters on both should return results"
    );

    // Inline source + WHERE target + LIMIT (combines both bugs)
    let inline_with_limit = db
        .execute(
            "MATCH (a {objectid: 'USER_0'})-[*1..20]->(b) \
             WHERE b.is_highvalue = true \
             RETURN b.objectid LIMIT 1",
        )
        .unwrap();
    assert_eq!(
        inline_with_limit.rows.len(),
        1,
        "Inline source + WHERE target + LIMIT should return 1 result"
    );
}

/// Test the worst case: no path exists, must explore entire reachable graph
#[test]
fn test_path_to_highvalue_no_path_worst_case() {
    let db = Database::in_memory().unwrap();
    db.set_entity_cache(EntityCacheConfig::with_capacity(100_000));

    // Create a graph where no path to high-value exists
    // This is the worst case as BFS must explore everything

    // Create isolated high-value groups (not connected to anything)
    for i in 0..3 {
        db.execute(&format!(
            "CREATE (g:Group {{objectid: 'ISOLATED_HV_{}', is_highvalue: true}})",
            i
        ))
        .unwrap();
    }

    // Create a dense connected component with no path to high-value
    for i in 0..500 {
        db.execute(&format!(
            "CREATE (u:User {{objectid: 'DENSE_USER_{}'}})",
            i
        ))
        .unwrap();
    }

    for i in 0..100 {
        db.execute(&format!(
            "CREATE (g:Group {{objectid: 'DENSE_GROUP_{}'}})",
            i
        ))
        .unwrap();
    }

    // Connect users to groups using direct relationship creation
    for i in 0..500 {
        let group = i % 100;
        create_relationship(
            &db,
            &format!("DENSE_USER_{}", i),
            &format!("DENSE_GROUP_{}", group),
            "MemberOf",
        );
    }

    // Connect groups to each other (creates dense subgraph)
    for i in 0..100 {
        for j in (i + 1)..std::cmp::min(i + 5, 100) {
            create_relationship(
                &db,
                &format!("DENSE_GROUP_{}", i),
                &format!("DENSE_GROUP_{}", j),
                "MemberOf",
            );
        }
    }

    let stats = db.stats().unwrap();
    println!(
        "Dense graph (no path): {} nodes, {} relationships",
        stats.node_count, stats.relationship_count
    );

    // This query must explore the entire reachable subgraph before concluding no path
    let (found, elapsed) = run_path_to_highvalue_query(&db, "DENSE_USER_0");
    println!(
        "DENSE_USER_0 (no path, worst case): found={}, elapsed={}ms",
        found, elapsed
    );
    assert!(!found);

    // This is the critical performance test - should still be reasonably fast
    assert!(
        elapsed < 5000,
        "Worst case query took too long: {}ms (should be < 5000ms)",
        elapsed
    );
}
