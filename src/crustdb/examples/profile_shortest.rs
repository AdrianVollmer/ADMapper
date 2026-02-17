//! Profiling harness for SHORTEST path queries.
//!
//! This example is designed to be run with cargo-flamegraph or perf to
//! generate flamecharts/flamegraphs of query execution.
//!
//! # Installation
//!
//! ```bash
//! cargo install flamegraph
//! ```
//!
//! # Usage
//!
//! Generate a flamegraph (SVG):
//! ```bash
//! cargo flamegraph --example profile_shortest -- --grid 20 --iterations 100
//! ```
//!
//! Or with perf directly:
//! ```bash
//! perf record --call-graph dwarf cargo run --release --example profile_shortest -- --grid 30 --iterations 50
//! perf script | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg
//! ```
//!
//! # Options
//!
//! - `--grid N`: Use NxN grid graph (default: 20)
//! - `--chain N`: Use linear chain of N nodes (default: off)
//! - `--tree D`: Use binary tree of depth D (default: off)
//! - `--iterations N`: Number of query iterations (default: 100)
//! - `--warmup N`: Warmup iterations before profiling (default: 10)

use crustdb::Database;
use std::env;
use std::hint::black_box;

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse arguments
    let mut grid_size: Option<usize> = None;
    let mut chain_size: Option<usize> = None;
    let mut tree_depth: Option<usize> = None;
    let mut iterations = 100;
    let mut warmup = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--grid" => {
                grid_size = Some(args[i + 1].parse().expect("Invalid grid size"));
                i += 2;
            }
            "--chain" => {
                chain_size = Some(args[i + 1].parse().expect("Invalid chain size"));
                i += 2;
            }
            "--tree" => {
                tree_depth = Some(args[i + 1].parse().expect("Invalid tree depth"));
                i += 2;
            }
            "--iterations" => {
                iterations = args[i + 1].parse().expect("Invalid iterations");
                i += 2;
            }
            "--warmup" => {
                warmup = args[i + 1].parse().expect("Invalid warmup");
                i += 2;
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_help();
                std::process::exit(1);
            }
        }
    }

    // Default to grid if nothing specified
    if grid_size.is_none() && chain_size.is_none() && tree_depth.is_none() {
        grid_size = Some(20);
    }

    if let Some(n) = grid_size {
        profile_grid(n, iterations, warmup);
    } else if let Some(n) = chain_size {
        profile_chain(n, iterations, warmup);
    } else if let Some(d) = tree_depth {
        profile_tree(d, iterations, warmup);
    }
}

fn print_help() {
    eprintln!(
        r#"
Profile SHORTEST path queries for flamegraph generation.

USAGE:
    cargo flamegraph --example profile_shortest -- [OPTIONS]

OPTIONS:
    --grid N        Use NxN grid graph (default if no graph specified)
    --chain N       Use linear chain of N nodes
    --tree D        Use binary tree of depth D
    --iterations N  Number of query iterations (default: 100)
    --warmup N      Warmup iterations (default: 10)
    --help, -h      Show this help

EXAMPLES:
    # Profile 20x20 grid with 100 iterations
    cargo flamegraph --example profile_shortest -- --grid 20 --iterations 100

    # Profile large chain
    cargo flamegraph --example profile_shortest -- --chain 1000 --iterations 50

    # Profile binary tree
    cargo flamegraph --example profile_shortest -- --tree 12 --iterations 100
"#
    );
}

fn profile_grid(n: usize, iterations: usize, warmup: usize) {
    eprintln!("Setting up {}x{} grid ({} nodes)...", n, n, n * n);

    let db = Database::in_memory().expect("Failed to create database");

    // Build grid
    let mut parts = Vec::new();
    for i in 0..n * n {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }
    for row in 0..n {
        for col in 0..n {
            let id = row * n + col;
            if col + 1 < n {
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, row * n + col + 1));
            }
            if row + 1 < n {
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, (row + 1) * n + col));
            }
            // Diagonal shortcuts
            if row + 1 < n && col + 1 < n && (row + col) % 3 == 0 {
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, (row + 1) * n + col + 1));
            }
        }
    }

    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Failed to create graph");

    let last_id = n * n - 1;
    let query = format!(
        "MATCH p = SHORTEST 1 (src:Node)-[:EDGE]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
        last_id
    );

    run_profile(&db, &query, iterations, warmup, "grid");
}

fn profile_chain(n: usize, iterations: usize, warmup: usize) {
    eprintln!("Setting up chain of {} nodes...", n);

    let db = Database::in_memory().expect("Failed to create database");

    // Build chain
    let mut parts = Vec::new();
    for i in 0..n {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }
    for i in 0..n - 1 {
        parts.push(format!("(n{})-[:NEXT]->(n{})", i, i + 1));
    }

    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Failed to create graph");

    let query = format!(
        "MATCH p = SHORTEST 1 (src:Node)-[:NEXT]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
        n - 1
    );

    run_profile(&db, &query, iterations, warmup, "chain");
}

fn profile_tree(depth: usize, iterations: usize, warmup: usize) {
    let num_nodes = (1 << depth) - 1;
    eprintln!("Setting up binary tree depth {} ({} nodes)...", depth, num_nodes);

    let db = Database::in_memory().expect("Failed to create database");

    // Build tree
    let mut parts = Vec::new();
    for i in 0..num_nodes {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }
    for i in 0..num_nodes {
        let left = 2 * i + 1;
        let right = 2 * i + 2;
        if left < num_nodes {
            parts.push(format!("(n{})-[:CHILD]->(n{})", i, left));
        }
        if right < num_nodes {
            parts.push(format!("(n{})-[:CHILD]->(n{})", i, right));
        }
    }

    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Failed to create graph");

    let query = format!(
        "MATCH p = SHORTEST 1 (src:Node)-[:CHILD]-+(dst:Node) \
         WHERE src.id = 0 AND dst.id = {} \
         RETURN length(p)",
        num_nodes - 1
    );

    run_profile(&db, &query, iterations, warmup, "tree");
}

fn run_profile(db: &Database, query: &str, iterations: usize, warmup: usize, graph_type: &str) {
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query).expect("Query failed");
        black_box(result);
    }

    eprintln!(
        "Profiling {} iterations on {} graph...",
        iterations, graph_type
    );
    eprintln!("Query: {}", query);

    // Main profiling loop - this is what the flamegraph will capture
    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        // black_box prevents the compiler from optimizing away the result
        black_box(result);
    }

    eprintln!("Done. Check flamegraph.svg for results.");
}
