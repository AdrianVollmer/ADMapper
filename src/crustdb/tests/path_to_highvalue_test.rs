//! Integration test for "path to high-value" queries.
//!
//! This test isolates the performance bottleneck seen in the ADMapper
//! "Node status API works" e2e test, which runs a variable-length path
//! query to find paths from a source node to any high-value target.
//!
//! The query pattern is:
//! ```cypher
//! MATCH p = (a)-[*1..20]->(b)
//! WHERE a.object_id = '...' AND b.is_highvalue = true
//! RETURN length(p) AS hops
//! LIMIT 1
//! ```

use crustdb::{Database, EntityCacheConfig};
use std::time::Instant;

/// Helper to create a relationship between two nodes by their object_ids.
fn create_relationship(db: &Database, source_oid: &str, target_oid: &str, rel_type: &str) {
    let source_id = db
        .find_node_by_property("object_id", source_oid)
        .unwrap()
        .unwrap_or_else(|| panic!("Source node {} not found", source_oid));
    let target_id = db
        .find_node_by_property("object_id", target_oid)
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
            "CREATE (:Group {{object_id: 'HV_GROUP_{}', name: 'HighValueGroup{}', is_highvalue: true}})",
            i, i
        ))
        .unwrap();
    }

    // Create regular groups
    for i in 0..num_groups {
        db.execute(&format!(
            "CREATE (:Group {{object_id: 'GROUP_{}', name: 'Group{}'}})",
            i, i
        ))
        .unwrap();
    }

    // Create users
    for i in 0..num_users {
        db.execute(&format!(
            "CREATE (:User {{object_id: 'USER_{}', name: 'User{}'}})",
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
fn run_path_to_highvalue_query(db: &Database, source_object_id: &str) -> (bool, u128) {
    let query = format!(
        "MATCH (a)-[*1..20]->(b) \
         WHERE a.object_id = '{}' AND b.is_highvalue = true \
         RETURN b.object_id",
        source_object_id
    );

    let start = Instant::now();
    let result = db.execute(&query).unwrap();
    let elapsed = start.elapsed().as_millis();

    let found = !result.rows.is_empty();
    (found, elapsed)
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
        .execute("MATCH (a:Group {object_id: 'GROUP_0'})-[:MemberOf]->(b) RETURN b.object_id")
        .unwrap();
    println!("GROUP_0 connects to: {:?}", chain_check.rows);

    let hv_check = db
        .execute("MATCH (a:Group {is_highvalue: true}) RETURN a.object_id")
        .unwrap();
    println!("High-value groups: {:?}", hv_check.rows);

    let user0_check = db
        .execute("MATCH (u:User {object_id: 'USER_0'})-[:MemberOf]->(g) RETURN g.object_id")
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
             WHERE a.object_id = 'USER_0' AND b.is_highvalue = true \
             RETURN b.object_id",
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
             WHERE a.object_id = 'USER_0' AND b.is_highvalue = true \
             RETURN b.object_id LIMIT 1",
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
             WHERE a.object_id = 'USER_0' AND b.is_highvalue = true \
             RETURN b.object_id LIMIT 10",
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
             WHERE a.object_id = 'USER_1' AND b.is_highvalue = true \
             RETURN b.object_id LIMIT 1",
        )
        .unwrap();
    assert!(
        no_path.rows.is_empty(),
        "USER_1 has no path, LIMIT 1 should return empty"
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
            "CREATE (g:Group {{object_id: 'ISOLATED_HV_{}', is_highvalue: true}})",
            i
        ))
        .unwrap();
    }

    // Create a dense connected component with no path to high-value
    for i in 0..500 {
        db.execute(&format!(
            "CREATE (u:User {{object_id: 'DENSE_USER_{}'}})",
            i
        ))
        .unwrap();
    }

    for i in 0..100 {
        db.execute(&format!(
            "CREATE (g:Group {{object_id: 'DENSE_GROUP_{}'}})",
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
