#[cfg(test)]
mod tests {
    use crate::database::{Database, NewQueryHistoryEntry};
    use crate::error::Error;
    use crate::graph::PropertyValue;
    use crate::query::ResultValue;

    #[test]
    fn test_database_create_single_node() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.label_count, 1);
    }

    #[test]
    fn test_database_create_relationship() {
        let db = Database::in_memory().unwrap();

        let result = db.execute(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        ).unwrap();

        assert_eq!(result.stats.nodes_created, 2);
        assert_eq!(result.stats.relationships_created, 1);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.relationship_count, 1);
        assert_eq!(stats.relationship_type_count, 1);
    }

    #[test]
    fn test_match_create_relationship_between_existing_nodes() {
        let db = Database::in_memory().unwrap();

        // Create two nodes separately
        db.execute("CREATE (:Group {objectid: 'G1', name: 'Group1'})")
            .unwrap();
        db.execute("CREATE (:Group {objectid: 'G2', name: 'Group2'})")
            .unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.relationship_count, 0);

        // MATCH...CREATE should create a relationship between existing nodes, not new ones
        let result = db
            .execute(
                "MATCH (a:Group {objectid: 'G1'}), (b:Group {objectid: 'G2'}) \
                 CREATE (a)-[:MemberOf]->(b)",
            )
            .unwrap();

        assert_eq!(
            result.stats.nodes_created, 0,
            "MATCH...CREATE should not create new nodes"
        );
        assert_eq!(
            result.stats.relationships_created, 1,
            "MATCH...CREATE should create 1 relationship"
        );

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2, "Node count should remain 2");
        assert_eq!(stats.relationship_count, 1, "Should have 1 relationship");

        // Verify the relationship connects the right nodes
        let verify = db
            .execute("MATCH (a:Group {objectid: 'G1'})-[:MemberOf]->(b:Group) RETURN b.objectid")
            .unwrap();
        assert_eq!(verify.rows.len(), 1);
    }

    #[test]
    fn test_database_multiple_creates() {
        let db = Database::in_memory().unwrap();

        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_database_complex_pattern() {
        let db = Database::in_memory().unwrap();

        let result = db
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)")
            .unwrap();

        assert_eq!(result.stats.nodes_created, 3);
        assert_eq!(result.stats.relationships_created, 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.relationship_count, 2);
        assert_eq!(stats.relationship_type_count, 2); // KNOWS, WORKS_AT
    }

    #[test]
    fn test_database_syntax_error() {
        let db = Database::in_memory().unwrap();

        let result = db.execute("CREATE n:Person");
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_insert_nodes() {
        let db = Database::in_memory().unwrap();

        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "objectid": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "objectid": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "objectid": "acme-3"}),
            ),
        ];

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 3);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.label_count, 2); // Person, Company
    }

    #[test]
    fn test_batch_insert_relationships() {
        let db = Database::in_memory().unwrap();

        // Create nodes first
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "objectid": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "objectid": "bob-2"}),
            ),
            (
                vec!["Company".to_string()],
                serde_json::json!({"name": "Acme", "objectid": "acme-3"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(node_ids.len(), 3);

        // Create relationships using node IDs
        let relationships = vec![
            (
                node_ids[0],
                node_ids[1],
                "KNOWS".to_string(),
                serde_json::json!({"since": 2020}),
            ),
            (
                node_ids[0],
                node_ids[2],
                "WORKS_AT".to_string(),
                serde_json::json!({}),
            ),
        ];

        let rel_ids = db.insert_relationships_batch(&relationships).unwrap();
        assert_eq!(rel_ids.len(), 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.relationship_count, 2);
        assert_eq!(stats.relationship_type_count, 2);
    }

    #[test]
    fn test_property_index() {
        let db = Database::in_memory().unwrap();

        // Create nodes with objectid property
        let nodes = vec![
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Alice", "objectid": "alice-1"}),
            ),
            (
                vec!["Person".to_string()],
                serde_json::json!({"name": "Bob", "objectid": "bob-2"}),
            ),
        ];

        let node_ids = db.insert_nodes_batch(&nodes).unwrap();

        // Build property index
        let index = db.build_property_index("objectid").unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(index.get("alice-1"), Some(&node_ids[0]));
        assert_eq!(index.get("bob-2"), Some(&node_ids[1]));

        // Find node by property
        let found = db.find_node_by_property("objectid", "alice-1").unwrap();
        assert_eq!(found, Some(node_ids[0]));

        let not_found = db.find_node_by_property("objectid", "nobody").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_create_property_index() {
        let db = Database::in_memory().unwrap();

        // Create nodes first
        db.execute("CREATE (n:Person {objectid: 'p1', name: 'Alice'})")
            .unwrap();
        db.execute("CREATE (n:Person {objectid: 'p2', name: 'Bob'})")
            .unwrap();

        // Initially no property indexes
        assert!(db.list_property_indexes().unwrap().is_empty());

        // Create index on objectid
        db.create_property_index("objectid").unwrap();
        assert!(db.has_property_index("objectid").unwrap());

        // Index should be listed
        let indexes = db.list_property_indexes().unwrap();
        assert_eq!(indexes, vec!["objectid"]);

        // Queries using the indexed property should still work correctly
        let result = db
            .execute("MATCH (n {objectid: 'p1'}) RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Drop the index
        assert!(db.drop_property_index("objectid").unwrap());
        assert!(!db.has_property_index("objectid").unwrap());

        // Query still works after dropping index
        let result = db
            .execute("MATCH (n {objectid: 'p2'}) RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_batch_insert_large() {
        let db = Database::in_memory().unwrap();

        // Create 1000 nodes in a batch
        let nodes: Vec<_> = (0..1000)
            .map(|i| {
                (
                    vec!["TestNode".to_string()],
                    serde_json::json!({"id": i, "objectid": format!("node-{}", i)}),
                )
            })
            .collect();

        let ids = db.insert_nodes_batch(&nodes).unwrap();
        assert_eq!(ids.len(), 1000);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1000);
    }

    #[test]
    fn test_upsert_nodes_batch() {
        let db = Database::in_memory().unwrap();

        // Create a placeholder node (like an orphan from relationship import)
        let placeholder = vec![(
            vec!["Base".to_string()],
            serde_json::json!({
                "objectid": "test-user-1",
                "name": "test-user-1",
                "placeholder": true
            }),
        )];
        let ids1 = db.upsert_nodes_batch(&placeholder).unwrap();
        assert_eq!(ids1.len(), 1);

        // Upsert with full data - should merge properties
        let full_data = vec![(
            vec!["User".to_string()],
            serde_json::json!({
                "objectid": "test-user-1",
                "name": "alice@corp.local",
                "enabled": true,
                "department": "Engineering"
            }),
        )];
        let ids2 = db.upsert_nodes_batch(&full_data).unwrap();
        assert_eq!(ids2.len(), 1);

        // Should be the same node
        assert_eq!(ids1[0], ids2[0]);

        // Only one node should exist
        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 1);

        // Verify via Cypher query that properties were merged
        let result = db
            .execute("MATCH (n {objectid: 'test-user-1'}) RETURN n.name, n.enabled, n.department")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_get_or_create_node_by_objectid() {
        let db = Database::in_memory().unwrap();

        // Create an orphan node
        let id1 = db
            .get_or_create_node_by_objectid("orphan-1", "User")
            .unwrap();
        assert!(id1 > 0);

        // Same objectid should return same ID
        let id2 = db
            .get_or_create_node_by_objectid("orphan-1", "Computer")
            .unwrap();
        assert_eq!(id1, id2);

        // Different objectid should create new node
        let id3 = db
            .get_or_create_node_by_objectid("orphan-2", "User")
            .unwrap();
        assert_ne!(id1, id3);

        // Should have 2 nodes
        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
    }

    #[test]
    fn test_count_aggregate() {
        let db = Database::in_memory().unwrap();

        // Create some nodes
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

        // Count all nodes
        let result = db.execute("MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(result.rows.len(), 1, "Should return single row");

        // Extract count
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 3, "Should count 3 nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }

        // Count by label
        let result = db.execute("MATCH (n:Person) RETURN count(n)").unwrap();
        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 Person nodes");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }

    #[test]
    fn test_count_relationships() {
        let db = Database::in_memory().unwrap();

        // Create nodes with relationships
        db.execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        db.execute("CREATE (c:Person)-[:WORKS_AT]->(d:Company)")
            .unwrap();

        // Count all relationships
        let result = db.execute("MATCH ()-[r]->() RETURN count(r)").unwrap();
        assert_eq!(result.rows.len(), 1);

        let count_val = result.rows[0].values.values().next().unwrap();
        match count_val {
            ResultValue::Property(PropertyValue::Integer(n)) => {
                assert_eq!(*n, 2, "Should count 2 relationships");
            }
            other => panic!("Expected integer, got {:?}", other),
        }
    }

    #[test]
    fn test_query_history_api() {
        let db = Database::in_memory().unwrap();

        // Add a query to history
        db.add_query_history(NewQueryHistoryEntry {
            id: "test-id-1",
            name: "Test Query",
            query: "MATCH (n) RETURN n",
            timestamp: 1700000000,
            result_count: Some(42),
            status: "completed",
            started_at: 1700000000,
            duration_ms: Some(150),
            error: None,
            background: false,
        })
        .unwrap();

        // Get query history
        let (rows, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "test-id-1");
        assert_eq!(rows[0].name, "Test Query");
        assert_eq!(rows[0].result_count, Some(42));
        assert_eq!(rows[0].duration_ms, Some(150));

        // Update status
        db.update_query_status("test-id-1", "archived", Some(200), Some(50), None)
            .unwrap();

        let (rows, _) = db.get_query_history(10, 0).unwrap();
        assert_eq!(rows[0].status, "archived");
        assert_eq!(rows[0].duration_ms, Some(200));
        assert_eq!(rows[0].result_count, Some(50));

        // Delete query
        db.delete_query_history("test-id-1").unwrap();
        let (rows, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
        assert!(rows.is_empty());

        // Add another and clear all
        db.add_query_history(NewQueryHistoryEntry {
            id: "test-id-2",
            name: "Another",
            query: "MATCH (n) RETURN n",
            timestamp: 1700000001,
            result_count: None,
            status: "pending",
            started_at: 1700000001,
            duration_ms: None,
            error: None,
            background: true, // background query
        })
        .unwrap();
        db.clear_query_history().unwrap();
        let (_, total) = db.get_query_history(10, 0).unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_caching_disabled_by_default() {
        let db = Database::in_memory().unwrap();
        assert!(!db.caching_enabled());

        // Execute a query - should not be cached
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();

        // Cache should be empty
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_query_caching_basic() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);
        assert!(db.caching_enabled());

        // Create some data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Execute a read-only query
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 2);

        // Check cache has one entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Execute the same query again - should hit cache
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 2);

        // Cache should still have one entry (not doubled)
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Results should be equivalent
        assert_eq!(result1.columns, result2.columns);
        assert_eq!(result1.rows.len(), result2.rows.len());
    }

    #[test]
    fn test_cache_invalidation_on_insert() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 1);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Insert new data - should invalidate cache
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // Execute query again - should get fresh result
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 2);
    }

    #[test]
    fn test_cache_invalidation_on_update() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.age").unwrap();
        assert_eq!(result1.rows.len(), 1);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Update data - should invalidate cache
        db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
            .unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_cache_invalidation_on_delete() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create initial data
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // Execute a query - it will be cached
        let result1 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result1.rows.len(), 2);

        // Cache should have an entry
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Delete data - should invalidate cache
        db.execute("MATCH (n:Person {name: 'Bob'}) DELETE n")
            .unwrap();

        // Cache should be cleared by trigger
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // Execute query again - should get fresh result
        let result2 = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result2.rows.len(), 1);
    }

    #[test]
    fn test_write_queries_not_cached() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // CREATE is not cached
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // MATCH with SET is not cached (not read-only)
        db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 30")
            .unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // MATCH with DELETE is not cached
        db.execute("CREATE (n:Temp {x: 1})").unwrap();
        db.execute("MATCH (n:Temp) DELETE n").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);

        // But pure MATCH RETURN is cached
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_clear_cache() {
        let mut db = Database::in_memory().unwrap();
        db.set_caching(true);

        // Create data and cache a query
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("MATCH (n:Person) RETURN n.name").unwrap();

        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 1);

        // Manually clear cache
        db.clear_cache().unwrap();

        let stats = db.cache_stats().unwrap();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        // Create a file-backed database to test connection pooling
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_concurrent.db");
        let db = Arc::new(Database::open(&db_path).unwrap());

        // Create some test data
        db.execute("CREATE (n:Person {name: 'Alice', id: 1})")
            .unwrap();
        db.execute("CREATE (n:Person {name: 'Bob', id: 2})")
            .unwrap();
        db.execute("CREATE (n:Person {name: 'Charlie', id: 3})")
            .unwrap();

        // Spawn multiple threads to read concurrently
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..10 {
                        let result = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
                        assert_eq!(result.rows.len(), 3);
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data integrity
        let result = db.execute("MATCH (n:Person) RETURN count(n)").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_read_pool_with_custom_size() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_pool.db");

        // Create with pool size 2
        let db = Database::open_with_pool_size(&db_path, 2).unwrap();

        db.execute("CREATE (n:Test {x: 1})").unwrap();
        let result = db.execute("MATCH (n:Test) RETURN n.x").unwrap();
        assert_eq!(result.rows.len(), 1);

        // Pool size 0 should also work (no read pool)
        let db2 = Database::open_with_pool_size(&db_path, 0).unwrap();
        let result = db2.execute("MATCH (n:Test) RETURN n.x").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_read_only_allows_reads() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_readonly.db");

        // Create and populate database
        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
            db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        }

        // Open read-only and verify reads work
        let db = Database::open_read_only(&db_path).unwrap();
        assert!(db.is_read_only());

        let result = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result.rows.len(), 2);

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
    }

    #[test]
    fn test_read_only_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_readonly_write.db");

        // Create database
        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        }

        // Open read-only
        let db = Database::open_read_only(&db_path).unwrap();

        // CREATE should fail
        let err = db.execute("CREATE (n:Person {name: 'Eve'})").unwrap_err();
        assert!(
            matches!(err, Error::ReadOnly),
            "Expected ReadOnly error, got: {}",
            err
        );

        // SET should fail
        let err = db.execute("MATCH (n:Person) SET n.age = 30").unwrap_err();
        assert!(matches!(err, Error::ReadOnly));

        // DELETE should fail
        let err = db.execute("MATCH (n) DELETE n").unwrap_err();
        assert!(matches!(err, Error::ReadOnly));

        // clear() should fail
        let err = db.clear().unwrap_err();
        assert!(matches!(err, Error::ReadOnly));
    }

    #[test]
    fn test_read_only_fails_on_nonexistent_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("nonexistent.db");

        // Should fail because the file doesn't exist
        let result = Database::open_read_only(&db_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_cleanup_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_wal_cleanup.db");
        let wal_path = dir.path().join("test_wal_cleanup.db-wal");

        // Create database, write data, then drop (should checkpoint)
        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE (n:Test {x: 1})").unwrap();
        }

        // After clean drop, WAL should not exist (checkpoint truncates it)
        assert!(
            !wal_path.exists(),
            "WAL file should be cleaned up after Database::drop()"
        );

        // Data should survive the checkpoint
        let db = Database::open(&db_path).unwrap();
        let result = db.execute("MATCH (n:Test) RETURN n.x").unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}
