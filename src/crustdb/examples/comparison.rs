//! FalkorDB and Neo4j comparison runner.
//!
//! Runs equivalent queries against external graph databases for benchmarking.
//! Uses subprocess calls to Docker/CLI tools.

use std::process::{Command, Stdio};
use std::time::Instant;

use crate::generators::GeneratedGraph;

/// Result of a single query execution.
#[derive(Debug, Clone)]
pub struct ComparisonResult {
    pub query_name: String,
    pub latency_ms: f64,
    pub rows_returned: Option<usize>,
    pub success: bool,
    pub error: Option<String>,
}

/// Check if Docker is available.
pub fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Check if FalkorDB container is running or can be started.
pub fn falkordb_available() -> bool {
    // Check if redis-cli (or falkordb) is accessible
    Command::new("docker")
        .args(["ps", "-q", "-f", "name=falkordb-bench"])
        .output()
        .map(|o| !o.stdout.is_empty() || start_falkordb().is_ok())
        .unwrap_or(false)
}

/// Start FalkorDB container if not running.
fn start_falkordb() -> Result<(), String> {
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            "falkordb-bench",
            "-p",
            "6379:6379",
            "falkordb/falkordb:latest",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| e.to_string())?;

    if status.success() {
        // Wait for startup
        std::thread::sleep(std::time::Duration::from_secs(2));
        Ok(())
    } else {
        Err("Failed to start FalkorDB container".to_string())
    }
}

/// Stop FalkorDB container.
#[allow(dead_code)]
pub fn stop_falkordb() {
    let _ = Command::new("docker")
        .args(["rm", "-f", "falkordb-bench"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Check if Neo4j container is running or can be started.
pub fn neo4j_available() -> bool {
    Command::new("docker")
        .args(["ps", "-q", "-f", "name=neo4j-bench"])
        .output()
        .map(|o| !o.stdout.is_empty() || start_neo4j().is_ok())
        .unwrap_or(false)
}

/// Start Neo4j container if not running.
fn start_neo4j() -> Result<(), String> {
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            "neo4j-bench",
            "-p",
            "7474:7474",
            "-p",
            "7687:7687",
            "-e",
            "NEO4J_AUTH=none",
            "-e",
            "NEO4J_PLUGINS=[\"apoc\"]",
            "neo4j:5",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| e.to_string())?;

    if status.success() {
        // Wait for Neo4j to fully start (can take a while)
        std::thread::sleep(std::time::Duration::from_secs(10));
        Ok(())
    } else {
        Err("Failed to start Neo4j container".to_string())
    }
}

/// Stop Neo4j container.
#[allow(dead_code)]
pub fn stop_neo4j() {
    let _ = Command::new("docker")
        .args(["rm", "-f", "neo4j-bench"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Load a generated graph into FalkorDB.
pub fn load_falkordb(graph: &GeneratedGraph, graph_name: &str) -> Result<(), String> {
    // Clear existing graph
    let _ = run_falkordb_query(graph_name, "MATCH (n) DETACH DELETE n");

    // Batch insert nodes using UNWIND
    let batch_size = 1000;
    for chunk in graph.nodes.chunks(batch_size) {
        let nodes_json: Vec<_> = chunk
            .iter()
            .enumerate()
            .map(|(i, (_, props))| {
                serde_json::json!({
                    "id": props.get("id").and_then(|v| v.as_i64()).unwrap_or(i as i64),
                    "value": props.get("value").and_then(|v| v.as_i64()).unwrap_or(0)
                })
            })
            .collect();

        let query = format!(
            "UNWIND {} AS n CREATE (:Node {{id: n.id, value: n.value}})",
            serde_json::to_string(&nodes_json).unwrap()
        );
        run_falkordb_query(graph_name, &query)?;
    }

    // Batch insert edges
    for chunk in graph.edges.chunks(batch_size) {
        let edges_json: Vec<_> = chunk
            .iter()
            .map(|(src, tgt, _, _)| {
                let src_id = graph.nodes[*src]
                    .1
                    .get("id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(*src as i64);
                let tgt_id = graph.nodes[*tgt]
                    .1
                    .get("id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(*tgt as i64);
                serde_json::json!({"src": src_id, "tgt": tgt_id})
            })
            .collect();

        let query = format!(
            "UNWIND {} AS e MATCH (a:Node {{id: e.src}}), (b:Node {{id: e.tgt}}) CREATE (a)-[:EDGE]->(b)",
            serde_json::to_string(&edges_json).unwrap()
        );
        run_falkordb_query(graph_name, &query)?;
    }

    Ok(())
}

/// Run a query against FalkorDB and return the result.
fn run_falkordb_query(graph_name: &str, query: &str) -> Result<String, String> {
    let output = Command::new("docker")
        .args([
            "exec",
            "falkordb-bench",
            "redis-cli",
            "GRAPH.QUERY",
            graph_name,
            query,
        ])
        .output()
        .map_err(|e| e.to_string())?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Run benchmark queries against FalkorDB.
pub fn run_falkordb_queries(
    graph_name: &str,
    queries: &[(String, String)],
) -> Vec<ComparisonResult> {
    let mut results = Vec::new();

    for (name, query) in queries {
        let start = Instant::now();
        let result = run_falkordb_query(graph_name, query);
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(output) => {
                // Parse row count from FalkorDB output
                let rows = parse_falkordb_row_count(&output);
                results.push(ComparisonResult {
                    query_name: name.clone(),
                    latency_ms,
                    rows_returned: rows,
                    success: true,
                    error: None,
                });
            }
            Err(e) => {
                results.push(ComparisonResult {
                    query_name: name.clone(),
                    latency_ms,
                    rows_returned: None,
                    success: false,
                    error: Some(e),
                });
            }
        }
    }

    results
}

fn parse_falkordb_row_count(output: &str) -> Option<usize> {
    // FalkorDB output includes row count in various formats
    // Try to extract it from the output
    for line in output.lines() {
        if line.contains("Nodes") || line.contains("count") {
            // Try to extract number
            for word in line.split_whitespace() {
                if let Ok(n) = word.trim_matches(|c: char| !c.is_numeric()).parse::<usize>() {
                    return Some(n);
                }
            }
        }
    }
    None
}

/// Load a generated graph into Neo4j.
pub fn load_neo4j(graph: &GeneratedGraph) -> Result<(), String> {
    // Clear existing data
    let _ = run_neo4j_query("MATCH (n) DETACH DELETE n");

    // Batch insert nodes
    let batch_size = 1000;
    for chunk in graph.nodes.chunks(batch_size) {
        let nodes_json: Vec<_> = chunk
            .iter()
            .enumerate()
            .map(|(i, (_, props))| {
                serde_json::json!({
                    "id": props.get("id").and_then(|v| v.as_i64()).unwrap_or(i as i64),
                    "value": props.get("value").and_then(|v| v.as_i64()).unwrap_or(0)
                })
            })
            .collect();

        let query = format!(
            "UNWIND {} AS n CREATE (:Node {{id: n.id, value: n.value}})",
            serde_json::to_string(&nodes_json).unwrap()
        );
        run_neo4j_query(&query)?;
    }

    // Create index for faster lookups
    let _ = run_neo4j_query("CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id)");

    // Batch insert edges
    for chunk in graph.edges.chunks(batch_size) {
        let edges_json: Vec<_> = chunk
            .iter()
            .map(|(src, tgt, _, _)| {
                let src_id = graph.nodes[*src]
                    .1
                    .get("id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(*src as i64);
                let tgt_id = graph.nodes[*tgt]
                    .1
                    .get("id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(*tgt as i64);
                serde_json::json!({"src": src_id, "tgt": tgt_id})
            })
            .collect();

        let query = format!(
            "UNWIND {} AS e MATCH (a:Node {{id: e.src}}), (b:Node {{id: e.tgt}}) CREATE (a)-[:EDGE]->(b)",
            serde_json::to_string(&edges_json).unwrap()
        );
        run_neo4j_query(&query)?;
    }

    Ok(())
}

/// Run a query against Neo4j via cypher-shell.
fn run_neo4j_query(query: &str) -> Result<String, String> {
    let output = Command::new("docker")
        .args([
            "exec",
            "neo4j-bench",
            "cypher-shell",
            "-u",
            "neo4j",
            "--non-interactive",
            query,
        ])
        .output()
        .map_err(|e| e.to_string())?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Run benchmark queries against Neo4j.
pub fn run_neo4j_queries(queries: &[(String, String)]) -> Vec<ComparisonResult> {
    let mut results = Vec::new();

    for (name, query) in queries {
        let start = Instant::now();
        let result = run_neo4j_query(query);
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(output) => {
                let rows = parse_neo4j_row_count(&output);
                results.push(ComparisonResult {
                    query_name: name.clone(),
                    latency_ms,
                    rows_returned: rows,
                    success: true,
                    error: None,
                });
            }
            Err(e) => {
                results.push(ComparisonResult {
                    query_name: name.clone(),
                    latency_ms,
                    rows_returned: None,
                    success: false,
                    error: Some(e),
                });
            }
        }
    }

    results
}

fn parse_neo4j_row_count(output: &str) -> Option<usize> {
    // Count non-header lines
    let lines: Vec<_> = output.lines().collect();
    if lines.len() > 1 {
        Some(lines.len() - 1) // Subtract header
    } else {
        None
    }
}

/// Translate CrustDB-style queries to FalkorDB-compatible Cypher.
/// FalkorDB uses different syntax for some features.
pub fn translate_for_falkordb(query: &str) -> String {
    // FalkorDB doesn't support SHORTEST syntax, use shortestPath function
    let mut q = query.to_string();

    // Replace SHORTEST 1 (a)-[*]->(b) with shortestPath((a)-[*]->(b))
    // This is a simple heuristic translation
    if q.contains("SHORTEST") {
        // Not directly translatable, return a simpler query
        q = q
            .replace("SHORTEST 1 ", "")
            .replace("SHORTEST 10 ", "")
            .replace("-[*]->", "-[*1..10]->");
    }

    q
}

/// Translate CrustDB-style queries to Neo4j-compatible Cypher.
pub fn translate_for_neo4j(query: &str) -> String {
    let mut q = query.to_string();

    // Neo4j uses shortestPath() function, not SHORTEST keyword
    if q.contains("SHORTEST") {
        // This is a simple heuristic; real translation would need proper parsing
        q = q
            .replace("SHORTEST 1 ", "")
            .replace("SHORTEST 10 ", "")
            .replace("-[*]->", "-[*1..10]->");
    }

    q
}
