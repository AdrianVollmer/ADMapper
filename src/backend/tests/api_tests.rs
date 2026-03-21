//! Integration tests for the ADMapper API.
//!
//! These tests use the actual application router and database,
//! not mocks. Each test creates a fresh in-memory database.
//!
//! These tests require the `crustdb` feature to be enabled.

#![cfg(feature = "crustdb")]

use admapper::{
    create_api_router, AppState, CrustDatabase, DatabaseBackend, DatabaseType, DbEdge, DbNode,
};
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tower::ServiceExt;

/// Test application with access to both router and database.
struct TestApp {
    router: Router,
    db: Arc<dyn DatabaseBackend>,
}

impl TestApp {
    fn new() -> Self {
        let db = CrustDatabase::in_memory().unwrap();
        let db_arc: Arc<dyn DatabaseBackend> = Arc::new(db);
        // Clone Arc before passing to state so we keep a reference for seeding
        let db_clone = Arc::clone(&db_arc);
        let state = AppState::new_connected(db_arc, DatabaseType::CrustDB, None);
        let router = create_api_router(state);
        Self {
            router,
            db: db_clone,
        }
    }

    fn router(&self) -> &Router {
        &self.router
    }

    fn db(&self) -> &Arc<dyn DatabaseBackend> {
        &self.db
    }
}

/// Create a test application with an in-memory database.
fn create_test_app() -> Router {
    let db = CrustDatabase::in_memory().unwrap();
    let db_arc: Arc<dyn DatabaseBackend> = Arc::new(db);
    let state = AppState::new_connected(db_arc, DatabaseType::CrustDB, None);
    create_api_router(state)
}

/// Helper to make a GET request and return the response body as JSON.
async fn get_json(app: &Router, uri: &str) -> (StatusCode, JsonValue) {
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&body).unwrap_or(JsonValue::Null);

    (status, json)
}

/// Helper to make a POST request with JSON body.
async fn post_json(app: &Router, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&body).unwrap_or(JsonValue::Null);

    (status, json)
}

/// Helper to make a DELETE request.
async fn delete(app: &Router, uri: &str) -> StatusCode {
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

// ============================================================================
// Health Check Tests
// ============================================================================

#[tokio::test]
async fn test_health_check() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
}

// ============================================================================
// Graph Stats Tests
// ============================================================================

#[tokio::test]
async fn test_graph_stats_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/stats").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["nodes"], 0);
    assert_eq!(json["relationships"], 0);
}

// ============================================================================
// Graph Search Tests
// ============================================================================

#[tokio::test]
async fn test_graph_search_min_length() {
    let app = create_test_app();

    // Query less than 2 characters should return empty
    let (status, json) = get_json(&app, "/api/graph/search?q=a").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "Search uses MATCH queries"]
async fn test_graph_search_no_results() {
    let app = create_test_app();

    // Search on empty database
    let (status, json) = get_json(&app, "/api/graph/search?q=nonexistent").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

// ============================================================================
// Graph Path Tests
// ============================================================================

#[tokio::test]
#[ignore = "Path resolution can trigger MATCH queries"]
async fn test_graph_path_node_not_found() {
    let app = create_test_app();

    // Nonexistent nodes should return 404
    let (status, _json) = get_json(&app, "/api/graph/path?from=node1&to=node2").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ============================================================================
// Query History Tests
// ============================================================================

#[tokio::test]
async fn test_query_history_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/query-history").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["total"], 0);
    assert!(json["entries"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_query_history_crud() {
    let app = create_test_app();

    // Add a query
    let (status, json) = post_json(
        &app,
        "/api/query-history",
        json!({
            "name": "Test Query",
            "query": "?[x] := x = 1",
            "result_count": 1
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let id = json["id"].as_str().unwrap().to_string();
    assert_eq!(json["name"], "Test Query");

    // Verify it's in the list
    let (status, json) = get_json(&app, "/api/query-history").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["total"], 1);

    // Delete it
    let status = delete(&app, &format!("/api/query-history/{}", id)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify it's gone
    let (status, json) = get_json(&app, "/api/query-history").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["total"], 0);
}

#[tokio::test]
async fn test_query_history_pagination() {
    let app = create_test_app();

    // Add 5 queries
    for i in 0..5 {
        post_json(
            &app,
            "/api/query-history",
            json!({
                "name": format!("Query {}", i),
                "query": format!("?[x] := x = {}", i),
                "result_count": i
            }),
        )
        .await;
    }

    // Get page 1 with 2 per page
    let (status, json) = get_json(&app, "/api/query-history?page=1&per_page=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["total"], 5);
    assert_eq!(json["entries"].as_array().unwrap().len(), 2);
    assert_eq!(json["page"], 1);
    assert_eq!(json["per_page"], 2);

    // Get page 2
    let (_, json) = get_json(&app, "/api/query-history?page=2&per_page=2").await;
    assert_eq!(json["entries"].as_array().unwrap().len(), 2);

    // Get page 3 (should have 1 entry)
    let (_, json) = get_json(&app, "/api/query-history?page=3&per_page=2").await;
    assert_eq!(json["entries"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_query_history_clear() {
    let app = create_test_app();

    // Add some queries
    for i in 0..3 {
        post_json(
            &app,
            "/api/query-history",
            json!({
                "name": format!("Query {}", i),
                "query": "test",
                "result_count": null
            }),
        )
        .await;
    }

    // Verify they exist
    let (_, json) = get_json(&app, "/api/query-history").await;
    assert_eq!(json["total"], 3);

    // Clear all
    let (status, _) = post_json(&app, "/api/query-history/clear", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify all cleared
    let (_, json) = get_json(&app, "/api/query-history").await;
    assert_eq!(json["total"], 0);
}

// ============================================================================
// Custom Query Tests
// ============================================================================
//
// Note: The /api/graph/query endpoint is async (returns query_id, results via SSE).
// Direct database query tests (run_custom_query) are tested in the crustdb crate.
// The API query execution involves complex async machinery that's difficult to test
// in integration tests without full SSE support.

#[tokio::test]
#[ignore = "Query API spawns background MATCH query that can hang"]
async fn test_custom_query_api_returns_query_id() {
    let app = create_test_app();

    // The query API should return a query_id for async tracking
    let (status, json) = post_json(
        &app,
        "/api/graph/query",
        json!({
            "query": "MATCH (n) RETURN n",
            "extract_graph": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        json["query_id"].is_string(),
        "Should return query_id: {:?}",
        json
    );
}

#[tokio::test]
async fn test_custom_query_invalid_syntax() {
    let app = TestApp::new();

    // Invalid Cypher syntax should fail at parse time
    let result = app.db().run_custom_query("this is not valid cypher syntax");

    assert!(result.is_err(), "Invalid query should fail");
}

// ============================================================================
// Graph Data Tests (require data setup)
// ============================================================================

// Note: Tests that use MATCH queries (like /api/graph/all, /api/graph/nodes)
// can hang in the tokio test context due to CrustDB executor issues.
// These tests are skipped until the underlying issue is resolved.

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_all_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/all").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["nodes"].as_array().unwrap().is_empty());
    assert!(json["relationships"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_nodes_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/nodes").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_edges_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/relationships").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

// ============================================================================
// Tests with Seeded Data
// ============================================================================

/// Helper to create test nodes
fn test_nodes() -> Vec<DbNode> {
    vec![
        DbNode {
            id: "user-jsmith".to_string(),
            name: "jsmith@corp.local".to_string(),
            label: "User".to_string(),
            properties: json!({"enabled": true}),
        },
        DbNode {
            id: "user-admin".to_string(),
            name: "admin@corp.local".to_string(),
            label: "User".to_string(),
            properties: json!({"enabled": true, "admincount": true}),
        },
        DbNode {
            id: "group-admins".to_string(),
            name: "Domain Admins".to_string(),
            label: "Group".to_string(),
            properties: json!({}),
        },
        DbNode {
            id: "computer-dc01".to_string(),
            name: "DC01.corp.local".to_string(),
            label: "Computer".to_string(),
            properties: json!({"operatingsystem": "Windows Server 2019"}),
        },
    ]
}

/// Helper to create test relationships
fn test_edges() -> Vec<DbEdge> {
    vec![
        DbEdge {
            source: "user-admin".to_string(),
            target: "group-admins".to_string(),
            rel_type: "MemberOf".to_string(),
            properties: json!({}),
            source_type: None,
            target_type: None,
        },
        DbEdge {
            source: "group-admins".to_string(),
            target: "computer-dc01".to_string(),
            rel_type: "AdminTo".to_string(),
            properties: json!({}),
            source_type: None,
            target_type: None,
        },
    ]
}

// Note: Tests that use MATCH-based endpoints can hang in tokio test context.
// The `graph_stats` endpoint uses SQL directly, so it works.
// The `graph_nodes`, `graph_edges`, `graph_all` endpoints use MATCH queries.
// The `graph_search` endpoint uses SQL LIKE queries directly.

#[tokio::test]
async fn test_graph_stats_with_data() {
    let app = TestApp::new();

    // Seed data
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/stats").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["nodes"], 4);
    assert_eq!(json["relationships"], 2);
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_nodes_with_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/nodes").await;

    assert_eq!(status, StatusCode::OK);
    let nodes = json.as_array().unwrap();
    assert_eq!(nodes.len(), 4);

    // Verify node structure
    let user_node = nodes.iter().find(|n| n["id"] == "user-jsmith").unwrap();
    assert_eq!(user_node["label"], "jsmith@corp.local");
    assert_eq!(user_node["type"], "User");
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_edges_with_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/relationships").await;

    assert_eq!(status, StatusCode::OK);
    let relationships = json.as_array().unwrap();
    assert_eq!(relationships.len(), 2);

    // Verify relationship structure
    let member_edge = relationships
        .iter()
        .find(|e| e["type"] == "MemberOf")
        .unwrap();
    assert_eq!(member_edge["source"], "user-admin");
    assert_eq!(member_edge["target"], "group-admins");
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_all_with_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/all").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["nodes"].as_array().unwrap().len(), 4);
    assert_eq!(json["relationships"].as_array().unwrap().len(), 2);
}

// Search tests use MATCH queries internally
#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_search_finds_user() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/search?q=jsmith").await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["id"], "user-jsmith");
    assert_eq!(results[0]["label"], "jsmith@corp.local");
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_search_case_insensitive() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/search?q=JSMITH").await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["id"], "user-jsmith");
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_search_partial_match() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    // "admin" should match both "admin@corp.local" and "Domain Admins"
    let (status, json) = get_json(app.router(), "/api/graph/search?q=admin").await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_search_with_limit() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    // Search for "corp" which matches all 4 nodes, but limit to 2
    let (status, json) = get_json(app.router(), "/api/graph/search?q=corp&limit=2").await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 2);
}

// Path tests use shortest_path which uses MATCH queries internally
#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_finds_direct_path() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=user-admin&to=group-admins",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["found"], true);

    // Path contains nodes with their outgoing relationship types
    // A direct path has 2 nodes: source -> target
    let path = json["path"].as_array().unwrap();
    assert_eq!(path.len(), 2);

    // First step: source node with relationship type to next
    assert_eq!(path[0]["node"]["id"], "user-admin");
    assert_eq!(path[0]["rel_type"], "MemberOf");

    // Last step: target node with no outgoing relationship
    assert_eq!(path[1]["node"]["id"], "group-admins");
    assert!(path[1]["rel_type"].is_null());
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_finds_multi_hop_path() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Path: user-admin -> group-admins -> computer-dc01
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=user-admin&to=computer-dc01",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["found"], true);

    // A 2-hop path has 3 nodes
    let path = json["path"].as_array().unwrap();
    assert_eq!(path.len(), 3);

    // First step: source node
    assert_eq!(path[0]["node"]["id"], "user-admin");
    assert_eq!(path[0]["rel_type"], "MemberOf");

    // Second step: intermediate node
    assert_eq!(path[1]["node"]["id"], "group-admins");
    assert_eq!(path[1]["rel_type"], "AdminTo");

    // Third step: target node
    assert_eq!(path[2]["node"]["id"], "computer-dc01");
    assert!(path[2]["rel_type"].is_null());
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_no_path_exists() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // jsmith is not connected to anything
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=user-jsmith&to=computer-dc01",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["found"], false);
    assert!(json["path"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_by_label() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Find path using labels instead of object IDs
    // admin@corp.local -> Domain Admins
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=admin%40corp.local&to=Domain%20Admins",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["found"], true);

    // Path should be resolved correctly
    let path = json["path"].as_array().unwrap();
    assert_eq!(path.len(), 2);
    assert_eq!(path[0]["node"]["id"], "user-admin");
    assert_eq!(path[1]["node"]["id"], "group-admins");
}

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_nonexistent_node() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Try to find path with nonexistent node
    let (status, _json) = get_json(
        app.router(),
        "/api/graph/path?from=nonexistent&to=group-admins",
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ============================================================================
// Node Status Tests
// ============================================================================

#[tokio::test]
async fn test_node_status_domain_admin_member() {
    let app = TestApp::new();
    let db = app.db();

    // Create DA group and a user that is a direct member via Cypher
    db.run_custom_query(
        "CREATE (:Group {name: 'Domain Admins', objectid: 'S-1-5-21-123-512', tier: 0})",
    )
    .unwrap();
    db.run_custom_query(
        "CREATE (:User {name: 'AdminUser', objectid: 'S-1-5-21-123-1001', enabled: true})",
    )
    .unwrap();
    db.run_custom_query(
        "MATCH (u {objectid: 'S-1-5-21-123-1001'}), (g {objectid: 'S-1-5-21-123-512'}) CREATE (u)-[:MemberOf]->(g)",
    ).unwrap();

    let (status, body) = get_json(app.router(), "/api/graph/node/S-1-5-21-123-1001/status").await;

    println!("DA member status response: {}", body);
    assert_eq!(status, StatusCode::OK, "Response: {:?}", body);
    assert_eq!(
        body["isDomainAdmin"], true,
        "Should be detected as DA member. Full response: {}",
        body
    );
}

#[tokio::test]
async fn test_node_status_transitive_da_member() {
    let app = TestApp::new();
    let db = app.db();

    // Create a tier-0 node and a user 2 hops away via Cypher
    db.run_custom_query(
        "CREATE (:Group {name: 'Domain Admins', objectid: 'S-1-5-21-123-512', tier: 0})",
    )
    .unwrap();
    db.run_custom_query("CREATE (:Group {name: 'IT Group', objectid: 'G-IT'})")
        .unwrap();
    db.run_custom_query("CREATE (:User {name: 'Bob', objectid: 'U-BOB', enabled: true})")
        .unwrap();
    // Bob -> IT Group -> DA
    db.run_custom_query(
        "MATCH (u {objectid: 'U-BOB'}), (g {objectid: 'G-IT'}) CREATE (u)-[:MemberOf]->(g)",
    )
    .unwrap();
    db.run_custom_query(
        "MATCH (u {objectid: 'G-IT'}), (g {objectid: 'S-1-5-21-123-512'}) CREATE (u)-[:MemberOf]->(g)",
    ).unwrap();

    // Bob has a transitive path to DA (2 hops), so should be detected as DA member
    let (status, body) = get_json(app.router(), "/api/graph/node/U-BOB/status").await;

    println!("Transitive DA member status response: {}", body);
    assert_eq!(status, StatusCode::OK, "Response: {:?}", body);
    // Bob is transitively a DA member through IT Group
    assert_eq!(
        body["isDomainAdmin"], true,
        "Should detect transitive DA membership. Full response: {}",
        body
    );
}

#[tokio::test]
async fn test_node_status_with_base_label() {
    let app = TestApp::new();
    let db = app.db();

    // Create nodes with Base as a secondary label (matching import convention)
    db.run_custom_query(
        "CREATE (:Group:Base {name: 'Domain Admins', objectid: 'S-1-5-21-123-512', tier: 0})",
    )
    .unwrap();
    db.run_custom_query(
        "CREATE (:User:Base {name: 'AdminUser', objectid: 'S-1-5-21-123-1001', enabled: true})",
    )
    .unwrap();
    db.run_custom_query(
        "MATCH (u {objectid: 'S-1-5-21-123-1001'}), (g {objectid: 'S-1-5-21-123-512'}) CREATE (u)-[:MemberOf]->(g)",
    ).unwrap();

    let (status, body) = get_json(app.router(), "/api/graph/node/S-1-5-21-123-1001/status").await;

    println!("Base-label DA member status response: {}", body);
    assert_eq!(status, StatusCode::OK, "Response: {:?}", body);
    // Should still detect DA membership even with Base label
    assert_eq!(
        body["isDomainAdmin"], true,
        "Should detect DA member with :Base label. Full response: {}",
        body
    );
}

#[tokio::test]
async fn test_node_status_non_memberof_path_to_tier_zero() {
    let app = TestApp::new();
    let db = app.db();

    // User has AdminTo path to a tier-0 Computer (not MemberOf, so DA check won't catch it)
    db.run_custom_query(
        "CREATE (:Computer {name: 'DC01', objectid: 'C-DC01', tier: 0, enabled: true})",
    )
    .unwrap();
    db.run_custom_query("CREATE (:User {name: 'Eve', objectid: 'U-EVE', enabled: true})")
        .unwrap();
    db.run_custom_query(
        "MATCH (u {objectid: 'U-EVE'}), (c {objectid: 'C-DC01'}) CREATE (u)-[:AdminTo]->(c)",
    )
    .unwrap();

    let (status, body) = get_json(app.router(), "/api/graph/node/U-EVE/status").await;

    println!("Non-MemberOf path to tier-0 status response: {}", body);
    assert_eq!(status, StatusCode::OK, "Response: {:?}", body);
    // Eve has a path to tier-0 via AdminTo (not MemberOf)
    assert_eq!(
        body["hasPathToHighTier"], true,
        "Should detect path to tier-0. Full response: {}",
        body
    );
    assert_eq!(
        body["pathLength"], 1,
        "Path should be 1 hop. Full response: {}",
        body
    );
}
/// Run with: cargo test --no-default-features test_debug_actual_db -- --nocapture --ignored
#[tokio::test]
#[ignore] // Only run manually for debugging
async fn test_debug_actual_db() {
    use std::path::Path;

    let db_path = Path::new("/workspace/admapper.db");
    if !db_path.exists() {
        println!("Database not found at {:?}", db_path);
        return;
    }

    let db = admapper::CrustDatabase::new(db_path, true).expect("Failed to open database");

    // Get all nodes
    let nodes = db.get_all_nodes().unwrap();
    println!("\n=== NODES ({} total) ===", nodes.len());
    for node in nodes.iter().take(10) {
        println!("  ID: {}", node.id);
        println!("  Name: {}", node.name);
        println!("  Label: {}", node.label);
        println!();
    }

    // Get all relationships
    let relationships = db.get_all_edges().unwrap();
    println!(
        "\n=== EDGES ({} total, showing first 20) ===",
        relationships.len()
    );
    for relationship in relationships.iter().take(20) {
        println!(
            "  {} -> {} ({})",
            relationship.source, relationship.target, relationship.rel_type
        );
    }

    // Search for ADMINISTRATOR
    println!("\n=== SEARCHING FOR ADMINISTRATOR ===");
    let results = db.search_nodes("ADMINISTRATOR", 10).unwrap();
    for node in &results {
        println!("  Found: ID={}, Label={}", node.id, node.label);

        // Try to resolve this identifier
        let resolved = db.resolve_node_identifier(&node.label).unwrap();
        println!("    Resolved label '{}' to: {:?}", node.label, resolved);
    }

    // Search for Domain Admins
    println!("\n=== SEARCHING FOR DOMAIN ADMINS ===");
    let results = db.search_nodes("DOMAIN ADMINS", 10).unwrap();
    for node in &results {
        println!("  Found: ID={}, Label={}", node.id, node.label);

        let resolved = db.resolve_node_identifier(&node.label).unwrap();
        println!("    Resolved label '{}' to: {:?}", node.label, resolved);
    }

    // Try to find path between them if we found both
    let admin_results = db.search_nodes("ADMINISTRATOR", 1).unwrap();
    let da_results = db.search_nodes("DOMAIN ADMINS", 1).unwrap();

    if !admin_results.is_empty() && !da_results.is_empty() {
        let from_id = &admin_results[0].id;
        let to_id = &da_results[0].id;

        println!("\n=== TESTING PATH FROM {} TO {} ===", from_id, to_id);

        // Check if there's an relationship
        let edges_from_admin: Vec<_> = relationships
            .iter()
            .filter(|e| e.source == *from_id)
            .collect();
        println!("  Edges FROM {}: {:?}", from_id, edges_from_admin.len());
        for e in &edges_from_admin {
            println!("    -> {} ({})", e.target, e.rel_type);
        }

        // Check relationships TO domain admins
        let edges_to_da: Vec<_> = relationships
            .iter()
            .filter(|e| e.target == *to_id)
            .collect();
        println!("  Edges TO {}: {:?}", to_id, edges_to_da.len());
        for e in edges_to_da.iter().take(10) {
            // Also resolve the source to see who it is
            let source_node = nodes.iter().find(|n| n.id == e.source);
            let source_label = source_node.map(|n| n.label.as_str()).unwrap_or("UNKNOWN");
            println!("    {} ({}) -> ({})", e.source, source_label, e.rel_type);
        }

        // Check if there's a user with the expected SID pattern (-500 for Administrator)
        println!("\n  Looking for users with -500 SID (built-in Administrator):");
        for node in &nodes {
            if node.id.ends_with("-500") {
                println!(
                    "    Found: ID={}, Name={}, Label={}",
                    node.id, node.name, node.label
                );
                // Check relationships from this node
                let edges_from: Vec<_> = relationships
                    .iter()
                    .filter(|e| e.source == node.id)
                    .collect();
                println!("    Edges from this node: {}", edges_from.len());
                for e in edges_from.iter().take(5) {
                    let target_node = nodes.iter().find(|n| n.id == e.target);
                    let target_name = target_node.map(|n| n.name.as_str()).unwrap_or("UNKNOWN");
                    println!("      -> {} ({}) [{}]", e.target, target_name, e.rel_type);
                }
            }
        }

        // Try shortest path
        let path: Option<Vec<(String, Option<String>)>> = db.shortest_path(from_id, to_id).unwrap();
        match path {
            Some(p) => {
                println!("  PATH FOUND! {} hops", p.len());
                for (node_id, rel_type) in &p {
                    println!("    {} (relationship: {:?})", node_id, rel_type);
                }
            }
            None => {
                println!("  NO PATH FOUND!");
            }
        }
    }
}

/// Test path finding with realistic BloodHound-style data
#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_path_bloodhound_style() {
    let app = TestApp::new();

    // Create nodes that mirror real BloodHound data format
    let nodes = vec![
        DbNode {
            id: "S-1-5-21-2697957641-2271029196-387917394-500".to_string(),
            name: "ADMINISTRATOR@PHANTOM.CORP".to_string(),
            label: "User".to_string(),
            properties: json!({"enabled": true, "admincount": true}),
        },
        DbNode {
            id: "S-1-5-21-2697957641-2271029196-387917394-512".to_string(),
            name: "DOMAIN ADMINS@PHANTOM.CORP".to_string(),
            label: "Group".to_string(),
            properties: json!({"admincount": true}),
        },
    ];

    // ADMINISTRATOR is MemberOf DOMAIN ADMINS
    let relationships = vec![DbEdge {
        source: "S-1-5-21-2697957641-2271029196-387917394-500".to_string(),
        target: "S-1-5-21-2697957641-2271029196-387917394-512".to_string(),
        rel_type: "MemberOf".to_string(),
        properties: json!({}),
        source_type: None,
        target_type: None,
    }];

    app.db().insert_nodes(&nodes).unwrap();
    app.db().insert_edges(&relationships).unwrap();

    // Verify data was inserted correctly
    let all_nodes = app.db().get_all_nodes().unwrap();
    let all_edges = app.db().get_all_edges().unwrap();
    assert_eq!(all_nodes.len(), 2, "Should have 2 nodes");
    assert_eq!(all_edges.len(), 1, "Should have 1 relationship");

    // Verify relationship direction
    let relationship = &all_edges[0];
    assert_eq!(
        relationship.source,
        "S-1-5-21-2697957641-2271029196-387917394-500"
    );
    assert_eq!(
        relationship.target,
        "S-1-5-21-2697957641-2271029196-387917394-512"
    );

    // Verify identifier resolution works
    let resolved_from = app
        .db()
        .resolve_node_identifier("ADMINISTRATOR@PHANTOM.CORP")
        .unwrap();
    assert_eq!(
        resolved_from,
        Some("S-1-5-21-2697957641-2271029196-387917394-500".to_string()),
        "Should resolve ADMINISTRATOR label to SID"
    );

    let resolved_to = app
        .db()
        .resolve_node_identifier("DOMAIN ADMINS@PHANTOM.CORP")
        .unwrap();
    assert_eq!(
        resolved_to,
        Some("S-1-5-21-2697957641-2271029196-387917394-512".to_string()),
        "Should resolve DOMAIN ADMINS label to SID"
    );

    // Verify shortest_path works directly
    let path_direct = app
        .db()
        .shortest_path(
            "S-1-5-21-2697957641-2271029196-387917394-500",
            "S-1-5-21-2697957641-2271029196-387917394-512",
        )
        .unwrap();
    assert!(
        path_direct.is_some(),
        "Direct shortest_path call should find path"
    );

    // Test 1: Find path using full labels (as frontend would send)
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=ADMINISTRATOR%40PHANTOM.CORP&to=DOMAIN%20ADMINS%40PHANTOM.CORP",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "Expected 200 OK, got {}", status);
    assert_eq!(
        json["found"], true,
        "Path should be found. Response: {:?}",
        json
    );

    let path = json["path"].as_array().unwrap();
    assert_eq!(path.len(), 2, "Path should have 2 nodes");
    assert_eq!(path[0]["node"]["label"], "ADMINISTRATOR@PHANTOM.CORP");
    assert_eq!(path[1]["node"]["label"], "DOMAIN ADMINS@PHANTOM.CORP");

    // Test 2: Find path using object IDs
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=S-1-5-21-2697957641-2271029196-387917394-500&to=S-1-5-21-2697957641-2271029196-387917394-512",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["found"], true);
}

// Note: Direct run_custom_query tests with MATCH queries are skipped here
// because CrustDB's query executor can hang in certain edge cases.
// These are tested via the API endpoints which use spawn_blocking properly.

#[tokio::test]
#[ignore = "CrustDB MATCH queries can hang in tokio test context"]
async fn test_graph_data_via_api() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Verify data is accessible via the nodes API
    let (status, json) = get_json(app.router(), "/api/graph/nodes").await;

    assert_eq!(status, StatusCode::OK);
    let nodes = json.as_array().unwrap();
    assert_eq!(nodes.len(), 4);

    // Verify we can find users
    let users: Vec<_> = nodes.iter().filter(|n| n["type"] == "User").collect();
    assert_eq!(users.len(), 2);
}
