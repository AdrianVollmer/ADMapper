//! Integration tests using TOML fixtures.
//!
//! Loads test cases from tests/fixtures/ and runs them against the database.

use crustdb::Database;
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// A test fixture file containing multiple test cases.
#[derive(Debug, Deserialize)]
struct FixtureFile {
    test: Vec<TestCase>,
}

/// A single test case from a fixture file.
#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    #[serde(default)]
    setup: Option<Setup>,
    query: Query,
    expected: Expected,
}

/// Setup data (nodes and edges to create before the test).
#[derive(Debug, Deserialize, Default)]
struct Setup {
    /// Raw Cypher queries to run for setup (preferred for complex setups)
    #[serde(default)]
    cypher: Vec<String>,
    #[serde(default)]
    nodes: Vec<SetupNode>,
    #[serde(default)]
    edges: Vec<SetupEdge>,
}

#[derive(Debug, Deserialize)]
struct SetupNode {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    labels: Vec<String>,
    #[allow(dead_code)]
    properties: Option<toml::Value>,
}

#[derive(Debug, Deserialize)]
struct SetupEdge {
    #[allow(dead_code)]
    from: String,
    #[allow(dead_code)]
    to: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    edge_type: String,
    #[allow(dead_code)]
    properties: Option<toml::Value>,
}

/// The Cypher query to execute.
#[derive(Debug, Deserialize)]
struct Query {
    cypher: String,
}

/// Expected results.
#[derive(Debug, Deserialize, Default)]
struct Expected {
    // For CREATE queries
    nodes_created: Option<usize>,
    edges_created: Option<usize>,

    // For queries with results
    #[allow(dead_code)]
    columns: Option<Vec<String>>,
    #[allow(dead_code)]
    rows: Option<Vec<toml::Value>>,
    #[allow(dead_code)]
    row_count: Option<usize>,

    // For error cases
    #[allow(dead_code)]
    error: Option<String>,
}

/// Load and parse a fixture file.
fn load_fixture(path: &Path) -> FixtureFile {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e));
    toml::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path.display(), e))
}

/// Run a single test case.
fn run_test_case(test: &TestCase) {
    let db = Database::in_memory().expect("Failed to create database");

    // Execute setup if present (for M3+ tests)
    if let Some(setup) = &test.setup {
        // Run raw Cypher setup queries first (preferred for complex setups)
        for cypher_query in &setup.cypher {
            db.execute(cypher_query).unwrap_or_else(|e| {
                panic!("Cypher setup failed for test '{}': {} (query: {})", test.name, e, cypher_query)
            });
        }

        for node in &setup.nodes {
            let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
            let props = match &node.properties {
                Some(v) => toml_to_cypher_props(v),
                None => String::new(),
            };
            let label_str = labels.join(":");
            let query = if props.is_empty() {
                format!("CREATE (n:{})", label_str)
            } else {
                format!("CREATE (n:{} {{{}}})", label_str, props)
            };
            db.execute(&query).unwrap_or_else(|e| {
                panic!("Setup failed for test '{}': {}", test.name, e)
            });
        }

        // Note: Edge setup via `edges` array is not supported yet.
        // Use the `cypher` array for tests that need relationships, e.g.:
        //   [test.setup]
        //   cypher = ["CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"]
        if !setup.edges.is_empty() {
            eprintln!(
                "Warning: test '{}' uses edges array which is not fully supported. Use setup.cypher instead.",
                test.name
            );
        }
    }

    // Execute the main query
    let result = db.execute(&test.query.cypher);

    // Check for expected error
    if let Some(ref _expected_error) = test.expected.error {
        assert!(
            result.is_err(),
            "Test '{}': Expected error but query succeeded",
            test.name
        );
        return;
    }

    // Query should succeed
    let result = result.unwrap_or_else(|e| {
        panic!("Test '{}': Query failed: {}", test.name, e)
    });

    // Check nodes_created
    if let Some(expected) = test.expected.nodes_created {
        assert_eq!(
            result.stats.nodes_created, expected,
            "Test '{}': nodes_created mismatch",
            test.name
        );
    }

    // Check edges_created (called relationships_created in our stats)
    if let Some(expected) = test.expected.edges_created {
        assert_eq!(
            result.stats.relationships_created, expected,
            "Test '{}': edges_created mismatch",
            test.name
        );
    }

    // Check row_count (for MATCH queries)
    if let Some(expected) = test.expected.row_count {
        assert_eq!(
            result.rows.len(), expected,
            "Test '{}': row_count mismatch (expected {}, got {})",
            test.name, expected, result.rows.len()
        );
    }

    // Check columns (for MATCH queries)
    if let Some(ref expected_cols) = test.expected.columns {
        assert_eq!(
            result.columns.len(), expected_cols.len(),
            "Test '{}': column count mismatch",
            test.name
        );
        for col in expected_cols {
            assert!(
                result.columns.contains(col),
                "Test '{}': missing expected column '{}'",
                test.name, col
            );
        }
    }
}

/// Convert TOML value to Cypher property string.
fn toml_to_cypher_props(value: &toml::Value) -> String {
    match value {
        toml::Value::Table(map) => {
            let props: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, toml_to_cypher_value(v)))
                .collect();
            props.join(", ")
        }
        _ => String::new(),
    }
}

/// Convert a TOML value to Cypher literal syntax.
fn toml_to_cypher_value(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        toml::Value::Integer(n) => n.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(toml_to_cypher_value).collect();
            format!("[{}]", items.join(", "))
        }
        toml::Value::Table(_) => {
            // Nested maps - convert to Cypher map syntax
            format!("{{{}}}", toml_to_cypher_props(value))
        }
        toml::Value::Datetime(dt) => format!("'{}'", dt),
    }
}

/// Find all fixture files for a milestone.
fn find_fixtures(milestone_dir: &str) -> Vec<std::path::PathBuf> {
    let pattern = format!("tests/fixtures/{}/*.toml", milestone_dir);
    glob::glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

// =============================================================================
// M2: CREATE Tests
// =============================================================================

#[test]
fn test_m2_create_fixtures() {
    let fixtures = find_fixtures("m2_create");
    assert!(!fixtures.is_empty(), "No M2 fixtures found");

    let mut passed = 0;
    let mut failed = 0;

    for fixture_path in &fixtures {
        let fixture = load_fixture(fixture_path);

        for test in &fixture.test {
            let result = std::panic::catch_unwind(|| {
                run_test_case(test);
            });

            match result {
                Ok(()) => {
                    passed += 1;
                    println!("  ✓ {}", test.name);
                }
                Err(e) => {
                    failed += 1;
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown error".to_string()
                    };
                    println!("  ✗ {}: {}", test.name, msg);
                }
            }
        }
    }

    println!("\nM2 CREATE: {} passed, {} failed", passed, failed);

    if failed > 0 {
        panic!("{} test(s) failed", failed);
    }
}

// Individual test for debugging specific cases
#[test]
fn test_m2_create_single_node() {
    let db = Database::in_memory().unwrap();
    let result = db.execute("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    assert_eq!(result.stats.nodes_created, 1);
}

#[test]
fn test_m2_create_relationship() {
    let db = Database::in_memory().unwrap();
    let result = db.execute(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
    ).unwrap();
    assert_eq!(result.stats.nodes_created, 2);
    assert_eq!(result.stats.relationships_created, 1);
}

// =============================================================================
// M3: MATCH Tests
// =============================================================================

#[test]
fn test_m3_match_fixtures() {
    let fixtures = find_fixtures("m3_match");
    assert!(!fixtures.is_empty(), "No M3 fixtures found");

    let mut passed = 0;
    let mut failed = 0;

    for fixture_path in &fixtures {
        let fixture = load_fixture(fixture_path);

        for test in &fixture.test {
            let result = std::panic::catch_unwind(|| {
                run_test_case(test);
            });

            match result {
                Ok(()) => {
                    passed += 1;
                    println!("  ✓ {}", test.name);
                }
                Err(e) => {
                    failed += 1;
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown error".to_string()
                    };
                    println!("  ✗ {}: {}", test.name, msg);
                }
            }
        }
    }

    println!("\nM3 MATCH: {} passed, {} failed", passed, failed);

    if failed > 0 {
        panic!("{} test(s) failed", failed);
    }
}

// Individual M3 tests for debugging
#[test]
fn test_m3_match_all_nodes() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db.execute("MATCH (n) RETURN n").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_m3_match_by_label() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Movie {title: 'Matrix'})").unwrap();

    let result = db.execute("MATCH (n:Person) RETURN n").unwrap();
    assert_eq!(result.rows.len(), 1);
}

// =============================================================================
// M4: WHERE Tests
// =============================================================================

#[test]
fn test_m4_where_fixtures() {
    let fixtures = find_fixtures("m4_where");
    assert!(!fixtures.is_empty(), "No M4 fixtures found");

    let mut passed = 0;
    let mut failed = 0;

    for fixture_path in &fixtures {
        let fixture = load_fixture(fixture_path);

        for test in &fixture.test {
            let result = std::panic::catch_unwind(|| {
                run_test_case(test);
            });

            match result {
                Ok(()) => {
                    passed += 1;
                    println!("  ✓ {}", test.name);
                }
                Err(e) => {
                    failed += 1;
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown error".to_string()
                    };
                    println!("  ✗ {}: {}", test.name, msg);
                }
            }
        }
    }

    println!("\nM4 WHERE: {} passed, {} failed", passed, failed);

    if failed > 0 {
        panic!("{} test(s) failed", failed);
    }
}

// Individual M4 tests for debugging
#[test]
fn test_m4_where_greater_than() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();

    let result = db.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_m4_where_starts_with() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Adam'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db.execute("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n").unwrap();
    assert_eq!(result.rows.len(), 2);
}

// =============================================================================
// M5: Single-Hop Traversal Tests
// =============================================================================

#[test]
fn test_m5_single_hop_fixtures() {
    let fixtures = find_fixtures("m5_single_hop");
    assert!(!fixtures.is_empty(), "No M5 fixtures found");

    let mut passed = 0;
    let mut failed = 0;

    for fixture_path in &fixtures {
        let fixture = load_fixture(fixture_path);

        for test in &fixture.test {
            let result = std::panic::catch_unwind(|| {
                run_test_case(test);
            });

            match result {
                Ok(()) => {
                    passed += 1;
                    println!("  ✓ {}", test.name);
                }
                Err(e) => {
                    failed += 1;
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown error".to_string()
                    };
                    println!("  ✗ {}: {}", test.name, msg);
                }
            }
        }
    }

    println!("\nM5 Single-Hop: {} passed, {} failed", passed, failed);

    if failed > 0 {
        panic!("{} test(s) failed", failed);
    }
}

#[test]
fn test_m5_single_hop_outgoing() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

    let result = db.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_m5_single_hop_incoming() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

    let result = db.execute("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(a:Person) RETURN a.name").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_m5_single_hop_undirected() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap();

    let result = db.execute("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(other:Person) RETURN other.name").unwrap();
    assert_eq!(result.rows.len(), 2); // Alice and Charlie
}

#[test]
fn test_m5_single_hop_with_where() {
    let db = Database::in_memory().unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob', age: 25})").unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(c:Person {name: 'Charlie', age: 35})").unwrap();

    let result = db.execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) WHERE b.age > 30 RETURN b.name").unwrap();
    assert_eq!(result.rows.len(), 1);
}
