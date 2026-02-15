//! Integration tests for the ADMapper API.
//!
//! These tests use the actual application router and database,
//! not mocks. Each test creates a fresh in-memory database.

use admapper::{create_api_router, AppState, DbEdge, DbNode, GraphDatabase};
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use tower::ServiceExt;

/// Test application with access to both router and database.
struct TestApp {
    router: Router,
    db: GraphDatabase,
}

impl TestApp {
    fn new() -> Self {
        let db = GraphDatabase::in_memory().unwrap();
        // Clone db before passing to state so we keep a reference for seeding
        let db_clone = db.clone();
        let state = AppState::new(db);
        let router = create_api_router(state);
        Self { router, db: db_clone }
    }

    fn router(&self) -> &Router {
        &self.router
    }

    fn db(&self) -> &GraphDatabase {
        &self.db
    }
}

/// Create a test application with an in-memory database.
fn create_test_app() -> Router {
    let db = GraphDatabase::in_memory().unwrap();
    let state = AppState::new(db);
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

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ok");
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
    assert_eq!(json["edges"], 0);
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

#[tokio::test]
async fn test_custom_query_valid() {
    let app = create_test_app();

    let (status, json) = post_json(
        &app,
        "/api/graph/query",
        json!({
            "query": "?[x] := x = 1 + 1",
            "extract_graph": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["results"].is_object());
    // The query "?[x] := x = 1 + 1" should return [[2]]
    assert_eq!(json["results"]["rows"][0][0], 2);
}

#[tokio::test]
async fn test_custom_query_invalid() {
    let app = create_test_app();

    let (status, _) = post_json(
        &app,
        "/api/graph/query",
        json!({
            "query": "this is not valid cozo syntax",
            "extract_graph": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ============================================================================
// Graph Data Tests (require data setup)
// ============================================================================

// Note: These tests would require setting up data through the import API
// or directly through the database. For now, we test the empty case.

#[tokio::test]
async fn test_graph_all_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/all").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["nodes"].as_array().unwrap().is_empty());
    assert!(json["edges"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_graph_nodes_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/nodes").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_graph_edges_empty() {
    let app = create_test_app();

    let (status, json) = get_json(&app, "/api/graph/edges").await;

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
            label: "jsmith@corp.local".to_string(),
            node_type: "User".to_string(),
            properties: json!({"enabled": true}),
        },
        DbNode {
            id: "user-admin".to_string(),
            label: "admin@corp.local".to_string(),
            node_type: "User".to_string(),
            properties: json!({"enabled": true, "admincount": true}),
        },
        DbNode {
            id: "group-admins".to_string(),
            label: "Domain Admins".to_string(),
            node_type: "Group".to_string(),
            properties: json!({}),
        },
        DbNode {
            id: "computer-dc01".to_string(),
            label: "DC01.corp.local".to_string(),
            node_type: "Computer".to_string(),
            properties: json!({"operatingsystem": "Windows Server 2019"}),
        },
    ]
}

/// Helper to create test edges
fn test_edges() -> Vec<DbEdge> {
    vec![
        DbEdge {
            source: "user-admin".to_string(),
            target: "group-admins".to_string(),
            edge_type: "MemberOf".to_string(),
            properties: json!({}),
        },
        DbEdge {
            source: "group-admins".to_string(),
            target: "computer-dc01".to_string(),
            edge_type: "AdminTo".to_string(),
            properties: json!({}),
        },
    ]
}

#[tokio::test]
async fn test_graph_stats_with_data() {
    let app = TestApp::new();

    // Seed data
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/stats").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["nodes"], 4);
    assert_eq!(json["edges"], 2);
}

#[tokio::test]
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
async fn test_graph_edges_with_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/edges").await;

    assert_eq!(status, StatusCode::OK);
    let edges = json.as_array().unwrap();
    assert_eq!(edges.len(), 2);

    // Verify edge structure
    let member_edge = edges
        .iter()
        .find(|e| e["type"] == "MemberOf")
        .unwrap();
    assert_eq!(member_edge["source"], "user-admin");
    assert_eq!(member_edge["target"], "group-admins");
}

#[tokio::test]
async fn test_graph_all_with_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    let (status, json) = get_json(app.router(), "/api/graph/all").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["nodes"].as_array().unwrap().len(), 4);
    assert_eq!(json["edges"].as_array().unwrap().len(), 2);
}

#[tokio::test]
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
async fn test_graph_search_with_limit() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();

    // Search for "corp" which matches all 4 nodes, but limit to 2
    let (status, json) = get_json(app.router(), "/api/graph/search?q=corp&limit=2").await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
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

    // Path contains nodes with their outgoing edge types
    // A direct path has 2 nodes: source -> target
    let path = json["path"].as_array().unwrap();
    assert_eq!(path.len(), 2);

    // First step: source node with edge type to next
    assert_eq!(path[0]["node"]["id"], "user-admin");
    assert_eq!(path[0]["edge_type"], "MemberOf");

    // Last step: target node with no outgoing edge
    assert_eq!(path[1]["node"]["id"], "group-admins");
    assert!(path[1]["edge_type"].is_null());
}

#[tokio::test]
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
    assert_eq!(path[0]["edge_type"], "MemberOf");

    // Second step: intermediate node
    assert_eq!(path[1]["node"]["id"], "group-admins");
    assert_eq!(path[1]["edge_type"], "AdminTo");

    // Third step: target node
    assert_eq!(path[2]["node"]["id"], "computer-dc01");
    assert!(path[2]["edge_type"].is_null());
}

#[tokio::test]
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

/// Debug test to examine actual database contents
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

    let db = admapper::GraphDatabase::new(db_path).expect("Failed to open database");

    // Get all nodes
    let nodes = db.get_all_nodes().unwrap();
    println!("\n=== NODES ({} total) ===", nodes.len());
    for node in nodes.iter().take(10) {
        println!("  ID: {}", node.id);
        println!("  Label: {}", node.label);
        println!("  Type: {}", node.node_type);
        println!();
    }

    // Get all edges
    let edges = db.get_all_edges().unwrap();
    println!("\n=== EDGES ({} total, showing first 20) ===", edges.len());
    for edge in edges.iter().take(20) {
        println!("  {} -> {} ({})", edge.source, edge.target, edge.edge_type);
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

        // Check if there's an edge
        let edges_from_admin: Vec<_> = edges
            .iter()
            .filter(|e| e.source == *from_id)
            .collect();
        println!("  Edges FROM {}: {:?}", from_id, edges_from_admin.len());
        for e in &edges_from_admin {
            println!("    -> {} ({})", e.target, e.edge_type);
        }

        // Check edges TO domain admins
        let edges_to_da: Vec<_> = edges
            .iter()
            .filter(|e| e.target == *to_id)
            .collect();
        println!("  Edges TO {}: {:?}", to_id, edges_to_da.len());
        for e in edges_to_da.iter().take(10) {
            // Also resolve the source to see who it is
            let source_node = nodes.iter().find(|n| n.id == e.source);
            let source_label = source_node.map(|n| n.label.as_str()).unwrap_or("UNKNOWN");
            println!("    {} ({}) -> ({})", e.source, source_label, e.edge_type);
        }

        // Check if there's a user with the expected SID pattern (-500 for Administrator)
        println!("\n  Looking for users with -500 SID (built-in Administrator):");
        for node in &nodes {
            if node.id.ends_with("-500") {
                println!("    Found: ID={}, Label={}, Type={}", node.id, node.label, node.node_type);
                // Check edges from this node
                let edges_from: Vec<_> = edges.iter().filter(|e| e.source == node.id).collect();
                println!("    Edges from this node: {}", edges_from.len());
                for e in edges_from.iter().take(5) {
                    let target_node = nodes.iter().find(|n| n.id == e.target);
                    let target_label = target_node.map(|n| n.label.as_str()).unwrap_or("UNKNOWN");
                    println!("      -> {} ({}) [{}]", e.target, target_label, e.edge_type);
                }
            }
        }

        // Try shortest path
        let path = db.shortest_path(from_id, to_id).unwrap();
        match path {
            Some(p) => {
                println!("  PATH FOUND! {} hops", p.len());
                for (node_id, edge_type) in &p {
                    println!("    {} (edge: {:?})", node_id, edge_type);
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
async fn test_graph_path_bloodhound_style() {
    let app = TestApp::new();

    // Create nodes that mirror real BloodHound data format
    let nodes = vec![
        DbNode {
            id: "S-1-5-21-2697957641-2271029196-387917394-500".to_string(),
            label: "ADMINISTRATOR@PHANTOM.CORP".to_string(),
            node_type: "User".to_string(),
            properties: json!({"enabled": true, "admincount": true}),
        },
        DbNode {
            id: "S-1-5-21-2697957641-2271029196-387917394-512".to_string(),
            label: "DOMAIN ADMINS@PHANTOM.CORP".to_string(),
            node_type: "Group".to_string(),
            properties: json!({"admincount": true}),
        },
    ];

    // ADMINISTRATOR is MemberOf DOMAIN ADMINS
    let edges = vec![DbEdge {
        source: "S-1-5-21-2697957641-2271029196-387917394-500".to_string(),
        target: "S-1-5-21-2697957641-2271029196-387917394-512".to_string(),
        edge_type: "MemberOf".to_string(),
        properties: json!({}),
    }];

    app.db().insert_nodes(&nodes).unwrap();
    app.db().insert_edges(&edges).unwrap();

    // Verify data was inserted correctly
    let all_nodes = app.db().get_all_nodes().unwrap();
    let all_edges = app.db().get_all_edges().unwrap();
    assert_eq!(all_nodes.len(), 2, "Should have 2 nodes");
    assert_eq!(all_edges.len(), 1, "Should have 1 edge");

    // Verify edge direction
    let edge = &all_edges[0];
    assert_eq!(edge.source, "S-1-5-21-2697957641-2271029196-387917394-500");
    assert_eq!(edge.target, "S-1-5-21-2697957641-2271029196-387917394-512");

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
    assert!(path_direct.is_some(), "Direct shortest_path call should find path");

    // Test 1: Find path using full labels (as frontend would send)
    let (status, json) = get_json(
        app.router(),
        "/api/graph/path?from=ADMINISTRATOR%40PHANTOM.CORP&to=DOMAIN%20ADMINS%40PHANTOM.CORP",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "Expected 200 OK, got {}", status);
    assert_eq!(json["found"], true, "Path should be found. Response: {:?}", json);

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

#[tokio::test]
async fn test_custom_query_on_graph_data() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Query all users
    let (status, json) = post_json(
        app.router(),
        "/api/graph/query",
        json!({
            "query": "?[id, label] := *nodes[id, label, type, _], type = 'User'",
            "extract_graph": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let rows = json["results"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2); // jsmith and admin
}

#[tokio::test]
async fn test_custom_query_extract_graph() {
    let app = TestApp::new();
    app.db().insert_nodes(&test_nodes()).unwrap();
    app.db().insert_edges(&test_edges()).unwrap();

    // Query all User nodes - extract_graph should populate the graph field
    let (status, json) = post_json(
        app.router(),
        "/api/graph/query",
        json!({
            "query": "?[id] := *nodes[id, _, type, _], type = 'User'",
            "extract_graph": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["graph"].is_object());
    let nodes = json["graph"]["nodes"].as_array().unwrap();
    // Should include the 2 users we queried
    assert_eq!(nodes.len(), 2);
}
