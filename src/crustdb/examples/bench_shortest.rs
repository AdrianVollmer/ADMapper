//! Benchmark for SHORTEST path queries.
//!
//! Run with: cargo run --release --example bench_shortest

use crustdb::Database;
use std::time::Instant;

fn main() {
    println!("SHORTEST Path Benchmark");
    println!("========================\n");

    // Test different graph sizes
    let sizes = [10, 25, 50, 100, 250, 500, 1000];

    println!("Linear chain graph (A->B->C->...->N):");
    println!("{:>8} {:>12} {:>12}", "n", "setup (ms)", "query (ms)");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    for &n in &sizes {
        let (setup_ms, query_ms) = bench_linear_chain(n);
        println!("{:>8} {:>12.2} {:>12.2}", n, setup_ms, query_ms);
    }

    println!("\nGrid graph (n x n with diagonal shortcuts):");
    println!(
        "{:>8} {:>8} {:>12} {:>12}",
        "n", "nodes", "setup (ms)", "query (ms)"
    );
    println!("{:-<8} {:-<8} {:-<12} {:-<12}", "", "", "", "");

    let grid_sizes = [5, 10, 15, 20, 25, 30];
    for &n in &grid_sizes {
        let (setup_ms, query_ms) = bench_grid(n);
        println!(
            "{:>8} {:>8} {:>12.2} {:>12.2}",
            n,
            n * n,
            setup_ms,
            query_ms
        );
    }

    println!("\nBinary tree graph (depth d, 2^d - 1 nodes):");
    println!(
        "{:>8} {:>8} {:>12} {:>12}",
        "depth", "nodes", "setup (ms)", "query (ms)"
    );
    println!("{:-<8} {:-<8} {:-<12} {:-<12}", "", "", "", "");

    let depths = [4, 6, 8, 10, 12];
    for &d in &depths {
        let nodes = (1 << d) - 1;
        let (setup_ms, query_ms) = bench_binary_tree(d);
        println!(
            "{:>8} {:>8} {:>12.2} {:>12.2}",
            d, nodes, setup_ms, query_ms
        );
    }
}

/// Benchmark a linear chain: node_0 -> node_1 -> ... -> node_{n-1}
fn bench_linear_chain(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup: create chain using a single CREATE statement
    let setup_start = Instant::now();

    // Build CREATE statement with all nodes and relationships
    // Format: CREATE (n0:Node {id: 0}), (n1:Node {id: 1}), ..., (n0)-[:NEXT]->(n1), ...
    let mut parts = Vec::new();

    // Add node definitions
    for i in 0..n {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }

    // Add relationship definitions
    for i in 0..n - 1 {
        parts.push(format!("(n{})-[:NEXT]->(n{})", i, i + 1));
    }

    let query = format!("CREATE {}", parts.join(", "));
    db.execute(&query).unwrap();

    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Query: find shortest path from first to last
    let query_start = Instant::now();
    let result = db
        .execute(&format!(
            "MATCH p = SHORTEST 1 (src:Node)-[:NEXT]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
            n - 1
        ))
        .unwrap();
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), 1, "Should find exactly one path");

    (setup_ms, query_ms)
}

/// Benchmark a grid graph with some shortcuts.
/// Creates n*n nodes in a grid with relationships to adjacent cells and some diagonals.
fn bench_grid(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    let setup_start = Instant::now();

    // Build CREATE statement with all nodes and relationships
    let mut parts = Vec::new();

    // Add all node definitions
    for i in 0..n * n {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }

    // Add relationship definitions (right, down, and diagonal shortcuts)
    for row in 0..n {
        for col in 0..n {
            let id = row * n + col;

            // Right relationship
            if col + 1 < n {
                let right_id = row * n + (col + 1);
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, right_id));
            }

            // Down relationship
            if row + 1 < n {
                let down_id = (row + 1) * n + col;
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, down_id));
            }

            // Diagonal shortcut (every 3rd cell)
            if row + 1 < n && col + 1 < n && (row + col) % 3 == 0 {
                let diag_id = (row + 1) * n + (col + 1);
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, diag_id));
            }
        }
    }

    let query = format!("CREATE {}", parts.join(", "));
    db.execute(&query).unwrap();

    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Query: find shortest path from top-left to bottom-right
    let query_start = Instant::now();
    let last_id = n * n - 1;
    let result = db
        .execute(&format!(
            "MATCH p = SHORTEST 1 (src:Node)-[:EDGE]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
            last_id
        ))
        .unwrap();
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), 1, "Should find exactly one path");

    (setup_ms, query_ms)
}

/// Benchmark a binary tree of given depth.
fn bench_binary_tree(depth: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");
    let num_nodes = (1 << depth) - 1;

    let setup_start = Instant::now();

    // Build CREATE statement with all nodes and relationships
    let mut parts = Vec::new();

    // Add all node definitions
    for i in 0..num_nodes {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }

    // Add relationship definitions (parent to children)
    for i in 0..num_nodes {
        let left_child = 2 * i + 1;
        let right_child = 2 * i + 2;

        if left_child < num_nodes {
            parts.push(format!("(n{})-[:CHILD]->(n{})", i, left_child));
        }

        if right_child < num_nodes {
            parts.push(format!("(n{})-[:CHILD]->(n{})", i, right_child));
        }
    }

    let query = format!("CREATE {}", parts.join(", "));
    db.execute(&query).unwrap();

    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Query: find shortest path from root to a leaf in the last level
    let query_start = Instant::now();
    let leaf_id = num_nodes - 1; // Last node (a leaf)
    let result = db
        .execute(&format!(
            "MATCH p = SHORTEST 1 (src:Node)-[:CHILD]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
            leaf_id
        ))
        .unwrap();
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), 1, "Should find exactly one path");

    (setup_ms, query_ms)
}
