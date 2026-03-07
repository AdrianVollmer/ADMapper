//! Profiling harness for CrustDB bottleneck operations.
//!
//! Generates flamegraph SVGs using the pprof crate to identify performance
//! bottlenecks in operations that showed poor performance in e2e testing:
//!
//! - Edge betweenness centrality (choke points): 3124ms (>3000x slower than others)
//! - Relationship type enumeration: 1953ms (50x slower)
//! - allShortestPaths(): Multi-path BFS
//! - shortestPath(): Deep traversals
//!
//! # Usage
//!
//! ```bash
//! # Profile choke points (edge betweenness) - the biggest bottleneck
//! cargo run --release --example profile_bottlenecks -- --op choke-points --nodes 200
//!
//! # Profile relationship type enumeration
//! cargo run --release --example profile_bottlenecks -- --op edge-types --nodes 500
//!
//! # Profile allShortestPaths (multi-path BFS)
//! cargo run --release --example profile_bottlenecks -- --op all-paths --nodes 100
//!
//! # Profile shortestPath traversal
//! cargo run --release --example profile_bottlenecks -- --op shortest --nodes 300
//!
//! # Run all operations
//! cargo run --release --example profile_bottlenecks -- --op all --nodes 150
//! ```
//!
//! Output: `flamegraph_<operation>.svg` in the current directory.

use crustdb::Database;
use pprof::ProfilerGuardBuilder;
use std::env;
use std::fs::File;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut operation = "choke-points".to_string();
    let mut nodes = 150;
    let mut edges_per_node = 5;
    let mut iterations = 20;
    let mut warmup = 3;
    let mut output_prefix = "flamegraph".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--op" | "--operation" => {
                operation = args[i + 1].clone();
                i += 2;
            }
            "--nodes" | "-n" => {
                nodes = args[i + 1].parse().expect("Invalid node count");
                i += 2;
            }
            "--edges" | "-e" => {
                edges_per_node = args[i + 1].parse().expect("Invalid edge count");
                i += 2;
            }
            "--iterations" | "-i" => {
                iterations = args[i + 1].parse().expect("Invalid iterations");
                i += 2;
            }
            "--warmup" | "-w" => {
                warmup = args[i + 1].parse().expect("Invalid warmup");
                i += 2;
            }
            "--output" | "-o" => {
                output_prefix = args[i + 1].clone();
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

    let ops: Vec<&str> = if operation == "all" {
        vec![
            "choke-points",
            "edge-types",
            "node-types",
            "all-paths",
            "shortest",
        ]
    } else {
        vec![operation.as_str()]
    };

    for op in ops {
        run_profile(
            op,
            nodes,
            edges_per_node,
            iterations,
            warmup,
            &output_prefix,
        );
    }
}

fn print_help() {
    eprintln!(
        r#"
Profile CrustDB bottleneck operations and generate flamegraph SVGs.

USAGE:
    cargo run --release --example profile_bottlenecks -- [OPTIONS]

OPTIONS:
    --op, --operation <OP>  Operation to profile:
                            - choke-points: Edge betweenness centrality (O(V*E))
                            - edge-types: Relationship type enumeration (O(E))
                            - node-types: Node label enumeration (O(N))
                            - all-paths: allShortestPaths() BFS
                            - shortest: shortestPath() traversal
                            - all: Run all operations
    --nodes, -n <N>         Number of nodes (default: 150)
    --edges, -e <N>         Edges per node for graph generation (default: 5)
    --iterations, -i <N>    Profiling iterations (default: 20)
    --warmup, -w <N>        Warmup iterations (default: 3)
    --output, -o <PREFIX>   Output file prefix (default: flamegraph)
    --help, -h              Show this help

EXAMPLES:
    # Profile the biggest bottleneck (choke points / edge betweenness)
    cargo run --release --example profile_bottlenecks -- --op choke-points --nodes 100

    # Profile with larger graph
    cargo run --release --example profile_bottlenecks -- --op edge-types --nodes 500

    # Profile all operations on small graph
    cargo run --release --example profile_bottlenecks -- --op all --nodes 100

NOTES:
    - Edge betweenness is O(V*E), so keep nodes small (<300) for reasonable times
    - Edge/node type enumeration scan all edges/nodes, so larger graphs show the issue
    - Generated SVGs can be viewed in any browser
"#
    );
}

fn run_profile(
    operation: &str,
    nodes: usize,
    edges_per_node: usize,
    iterations: usize,
    warmup: usize,
    output_prefix: &str,
) {
    let output_file = format!("{}_{}.svg", output_prefix, operation.replace('-', "_"));
    eprintln!("\n{:=<60}", "");
    eprintln!("Profiling: {}", operation);
    eprintln!("{:=<60}", "");

    // Create database and build graph
    let db = Database::in_memory().expect("Failed to create database");
    build_ad_like_graph(&db, nodes, edges_per_node);

    match operation {
        "choke-points" => profile_choke_points(&db, iterations, warmup, &output_file),
        "edge-types" => profile_edge_types(&db, iterations, warmup, &output_file),
        "node-types" => profile_node_types(&db, iterations, warmup, &output_file),
        "all-paths" => profile_all_paths(&db, nodes, iterations, warmup, &output_file),
        "shortest" => profile_shortest_path(&db, nodes, iterations, warmup, &output_file),
        _ => {
            eprintln!("Unknown operation: {}", operation);
            std::process::exit(1);
        }
    }
}

/// Build a graph that resembles an AD permission structure.
///
/// Creates:
/// - Users, Computers, Groups with realistic distribution
/// - MemberOf, AdminTo, GenericAll, and other AD relationship types
fn build_ad_like_graph(db: &Database, node_count: usize, edges_per_node: usize) {
    eprintln!(
        "Building AD-like graph: {} nodes, ~{} edges/node...",
        node_count, edges_per_node
    );
    let start = Instant::now();

    // Distribution similar to real AD data
    let user_count = (node_count as f64 * 0.5) as usize;
    let computer_count = (node_count as f64 * 0.2) as usize;
    let group_count = (node_count as f64 * 0.25) as usize;
    let domain_count = (node_count as f64 * 0.05).max(1.0) as usize;

    let mut nodes: Vec<(Vec<String>, serde_json::Value)> = Vec::with_capacity(node_count);

    // Create nodes
    for i in 0..user_count {
        nodes.push((
            vec!["User".to_string()],
            serde_json::json!({
                "object_id": format!("S-1-5-21-{}-{}", i / 1000, i),
                "name": format!("user{}@corp.local", i),
                "enabled": i % 10 != 0,
                "value": i % 1000,
            }),
        ));
    }

    for i in 0..computer_count {
        nodes.push((
            vec!["Computer".to_string()],
            serde_json::json!({
                "object_id": format!("S-1-5-21-{}-C{}", i / 1000, i),
                "name": format!("COMP{}$", i),
                "operatingsystem": "Windows Server 2019",
                "value": i % 1000,
            }),
        ));
    }

    for i in 0..group_count {
        // Mark domain admins group
        let is_da = i == 0;
        nodes.push((
            vec!["Group".to_string()],
            serde_json::json!({
                "object_id": if is_da { "S-1-5-21-1234-512".to_string() } else { format!("S-1-5-21-{}-G{}", i / 1000, i) },
                "name": if is_da { "DOMAIN ADMINS@CORP.LOCAL".to_string() } else { format!("Group{}@corp.local", i) },
                "value": i % 1000,
            }),
        ));
    }

    for i in 0..domain_count {
        nodes.push((
            vec!["Domain".to_string()],
            serde_json::json!({
                "object_id": format!("S-1-5-21-{}-D", i),
                "name": format!("CORP{}.LOCAL", i),
                "value": i % 1000,
            }),
        ));
    }

    let node_ids = db
        .insert_nodes_batch(&nodes)
        .expect("Failed to insert nodes");
    eprintln!(
        "  Inserted {} nodes in {:?}",
        node_ids.len(),
        start.elapsed()
    );

    // Create edges with AD-like relationship types
    let edge_start = Instant::now();
    let rel_types = [
        "MemberOf",
        "AdminTo",
        "GenericAll",
        "GenericWrite",
        "Owns",
        "CanRDP",
        "HasSession",
    ];

    let mut edges: Vec<(i64, i64, String, serde_json::Value)> = Vec::new();

    // Create a mix of edge patterns
    for i in 0..node_ids.len() {
        for j in 0..edges_per_node {
            let target_idx = (i + j + 1) % node_ids.len();
            if target_idx != i {
                let rel_type = rel_types[(i + j) % rel_types.len()];
                edges.push((
                    node_ids[i],
                    node_ids[target_idx],
                    rel_type.to_string(),
                    serde_json::json!({}),
                ));
            }
        }

        // Add some additional cross-links to create interesting path structure
        if i % 10 == 0 && i + 50 < node_ids.len() {
            edges.push((
                node_ids[i],
                node_ids[i + 50],
                "MemberOf".to_string(),
                serde_json::json!({}),
            ));
        }
    }

    let rel_ids = db
        .insert_relationships_batch(&edges)
        .expect("Failed to insert relationships");
    eprintln!(
        "  Inserted {} relationships in {:?}",
        rel_ids.len(),
        edge_start.elapsed()
    );
    eprintln!("  Total build time: {:?}", start.elapsed());
}

/// Profile relationship betweenness centrality (choke points).
///
/// This is the biggest bottleneck: O(V*E) complexity.
fn profile_choke_points(db: &Database, iterations: usize, warmup: usize, output: &str) {
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.relationship_betweenness_centrality(None, true);
        let _ = black_box(result);
    }

    // Clear cache to get fresh profiling
    let _ = db.clear_cache();

    eprintln!(
        "Profiling relationship_betweenness_centrality ({} iterations)...",
        iterations
    );

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        // Clear cache each iteration to profile the actual computation
        let _ = db.clear_cache();
        let result = db
            .relationship_betweenness_centrality(None, true)
            .expect("Query failed");
        black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  Total time: {:?} ({:.2}ms/iter)",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, output);
}

/// Profile relationship type enumeration.
///
/// Query: MATCH ()-[r]->() RETURN DISTINCT type(r)
fn profile_edge_types(db: &Database, iterations: usize, warmup: usize, output: &str) {
    let query = "MATCH ()-[r]->() RETURN DISTINCT type(r)";

    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query);
        let _ = black_box(result);
    }

    // Clear cache to get fresh profiling
    let _ = db.clear_cache();

    eprintln!(
        "Profiling edge type enumeration ({} iterations)...",
        iterations
    );
    eprintln!("Query: {}", query);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.clear_cache();
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  Total time: {:?} ({:.2}ms/iter)",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, output);
}

/// Profile node type enumeration.
///
/// Query: MATCH (n) RETURN DISTINCT n.label
fn profile_node_types(db: &Database, iterations: usize, warmup: usize, output: &str) {
    let query = "MATCH (n) RETURN DISTINCT n.label";

    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query);
        let _ = black_box(result);
    }

    let _ = db.clear_cache();

    eprintln!(
        "Profiling node type enumeration ({} iterations)...",
        iterations
    );
    eprintln!("Query: {}", query);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.clear_cache();
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  Total time: {:?} ({:.2}ms/iter)",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, output);
}

/// Profile allShortestPaths() (multi-path BFS).
fn profile_all_paths(
    db: &Database,
    node_count: usize,
    iterations: usize,
    warmup: usize,
    output: &str,
) {
    // Target a node roughly in the middle of the graph
    let target_id = node_count / 2;
    let query = format!(
        "MATCH p = allShortestPaths((s:User {{name: 'user0@corp.local'}})-[*1..10]->(t:User {{name: 'user{}@corp.local'}})) RETURN p",
        target_id
    );

    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(&query);
        let _ = black_box(result);
    }

    let _ = db.clear_cache();

    eprintln!("Profiling allShortestPaths ({} iterations)...", iterations);
    eprintln!("Query: {}", query);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.clear_cache();
        let result = db.execute(&query);
        // May fail if no path exists - that's ok for profiling
        let _ = black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  Total time: {:?} ({:.2}ms/iter)",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, output);
}

/// Profile shortestPath() traversal.
fn profile_shortest_path(
    db: &Database,
    node_count: usize,
    iterations: usize,
    warmup: usize,
    output: &str,
) {
    // Find path to a distant node
    let target_id = (node_count as f64 * 0.8) as usize;
    let query = format!(
        "MATCH p = shortestPath((s:User {{name: 'user0@corp.local'}})-[*]->(t:User {{name: 'user{}@corp.local'}})) RETURN length(p)",
        target_id
    );

    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(&query);
        let _ = black_box(result);
    }

    let _ = db.clear_cache();

    eprintln!("Profiling shortestPath ({} iterations)...", iterations);
    eprintln!("Query: {}", query);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.clear_cache();
        let result = db.execute(&query);
        let _ = black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  Total time: {:?} ({:.2}ms/iter)",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, output);
}

fn write_flamegraph(guard: pprof::ProfilerGuard, output: &str) {
    if let Ok(report) = guard.report().build() {
        let file = File::create(output).expect("Failed to create output file");
        report.flamegraph(file).expect("Failed to write flamegraph");
        eprintln!("Flamegraph written to: {}", output);
    } else {
        eprintln!("Failed to generate profiler report");
    }
}
