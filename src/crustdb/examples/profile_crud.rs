//! Profiling harness for CRUD operations.
//!
//! Generates flamegraph SVGs using the pprof crate (no perf required).
//!
//! # Usage
//!
//! ```bash
//! # Profile COUNT query on 10000 nodes
//! cargo run --release --example profile_crud -- --op count --nodes 10000
//!
//! # Profile MATCH query
//! cargo run --release --example profile_crud -- --op match --nodes 5000
//!
//! # Profile INSERT operations
//! cargo run --release --example profile_crud -- --op insert --nodes 1000 --iterations 50
//! ```
//!
//! This generates `flamegraph.svg` in the current directory.

use crustdb::Database;
use pprof::ProfilerGuardBuilder;
use std::env;
use std::fs::File;
use std::hint::black_box;

#[derive(Clone, Copy)]
enum Operation {
    Insert,
    Count,
    Match,
    MatchFiltered,
    MatchLimit,
    Delete,
    Mixed,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse arguments
    let mut operation = Operation::Count;
    let mut nodes = 10_000;
    let mut iterations = 100;
    let mut warmup = 10;
    let mut output = "flamegraph.svg".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--op" | "--operation" => {
                operation = match args[i + 1].as_str() {
                    "insert" => Operation::Insert,
                    "count" => Operation::Count,
                    "match" => Operation::Match,
                    "match-filtered" | "filter" => Operation::MatchFiltered,
                    "match-limit" | "limit" => Operation::MatchLimit,
                    "delete" => Operation::Delete,
                    "mixed" => Operation::Mixed,
                    _ => {
                        eprintln!("Unknown operation: {}", args[i + 1]);
                        print_help();
                        std::process::exit(1);
                    }
                };
                i += 2;
            }
            "--nodes" | "-n" => {
                nodes = args[i + 1].parse().expect("Invalid node count");
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
                output = args[i + 1].clone();
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

    match operation {
        Operation::Insert => profile_insert(nodes, iterations, warmup, &output),
        Operation::Count => profile_count(nodes, iterations, warmup, &output),
        Operation::Match => profile_match(nodes, iterations, warmup, &output),
        Operation::MatchFiltered => profile_match_filtered(nodes, iterations, warmup, &output),
        Operation::MatchLimit => profile_match_limit(nodes, iterations, warmup, &output),
        Operation::Delete => profile_delete(nodes, iterations, warmup, &output),
        Operation::Mixed => profile_mixed(nodes, iterations, warmup, &output),
    }
}

fn print_help() {
    eprintln!(
        r#"
Profile CRUD operations and generate flamegraph SVG.

USAGE:
    cargo run --release --example profile_crud -- [OPTIONS]

OPTIONS:
    --op, --operation OP   Operation to profile:
                           insert, count, match, match-filtered,
                           match-limit, delete, mixed
                           (default: count)
    --nodes, -n N          Number of nodes (default: 10000)
    --iterations, -i N     Number of profiled iterations (default: 100)
    --warmup, -w N         Warmup iterations (default: 10)
    --output, -o FILE      Output flamegraph file (default: flamegraph.svg)
    --help, -h             Show this help

EXAMPLES:
    # Profile COUNT on 10k nodes
    cargo run --release --example profile_crud -- --op count --nodes 10000

    # Profile MATCH with LIMIT on 50k nodes
    cargo run --release --example profile_crud -- --op match-limit --nodes 50000

    # Profile INSERT (recreates DB each iteration)
    cargo run --release --example profile_crud -- --op insert --nodes 1000 --iterations 50

    # Profile mixed workload
    cargo run --release --example profile_crud -- --op mixed --iterations 20
"#
    );
}

/// Create a database with N nodes for testing
fn setup_db(n: usize) -> Database {
    let db = Database::in_memory().expect("Failed to create database");

    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        let active = if i % 10 == 0 { "true" } else { "false" };
        parts.push(format!(
            "(n{}:Person {{id: {}, name: 'Person{}', active: {}}})",
            i, i, i, active
        ));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");

    db
}

fn profile_insert(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!(
        "Profiling INSERT of {} nodes x {} iterations...",
        nodes, iterations
    );

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let db = Database::in_memory().expect("Failed to create database");
        let mut parts = Vec::with_capacity(nodes);
        for i in 0..nodes {
            parts.push(format!("(n{}:Person {{id: {}}})", i, i));
        }
        let result = db.execute(&format!("CREATE {}", parts.join(", ")));
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let db = Database::in_memory().expect("Failed to create database");
        let mut parts = Vec::with_capacity(nodes);
        for i in 0..nodes {
            parts.push(format!("(n{}:Person {{id: {}}})", i, i));
        }
        let result = db.execute(&format!("CREATE {}", parts.join(", ")));
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_count(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!("Setting up {} nodes...", nodes);
    let db = setup_db(nodes);

    let query = "MATCH (n:Person) RETURN COUNT(n)";
    eprintln!("Query: {}", query);

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_match(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!("Setting up {} nodes...", nodes);
    let db = setup_db(nodes);

    let query = "MATCH (n:Person) RETURN n";
    eprintln!("Query: {}", query);

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_match_filtered(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!("Setting up {} nodes (10% with active=true)...", nodes);
    let db = setup_db(nodes);

    let query = "MATCH (n:Person) WHERE n.active = true RETURN n";
    eprintln!("Query: {}", query);

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_match_limit(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!("Setting up {} nodes...", nodes);
    let db = setup_db(nodes);

    let query = "MATCH (n:Person) RETURN n LIMIT 10";
    eprintln!("Query: {}", query);

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_delete(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!(
        "Profiling DELETE of {} nodes x {} iterations...",
        nodes, iterations
    );

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let db = setup_db(nodes);
        let result = db.execute("MATCH (n:Person) DELETE n");
        let _ = black_box(result);
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let db = setup_db(nodes);
        let result = db.execute("MATCH (n:Person) DELETE n");
        let _ = black_box(result);
    }

    write_flamegraph(guard, output);
}

fn profile_mixed(nodes: usize, iterations: usize, warmup: usize, output: &str) {
    eprintln!(
        "Profiling mixed workload: insert {}, count, match 10, delete x {} iterations...",
        nodes, iterations
    );

    // Warmup
    eprintln!("Warming up ({} iterations)...", warmup);
    for _ in 0..warmup {
        let db = Database::in_memory().expect("Failed to create database");

        // Insert
        let mut parts = Vec::with_capacity(nodes);
        for i in 0..nodes {
            parts.push(format!("(n{}:Person {{id: {}}})", i, i));
        }
        let _ = black_box(db.execute(&format!("CREATE {}", parts.join(", "))));

        // Count
        let _ = black_box(db.execute("MATCH (n:Person) RETURN COUNT(n)"));

        // Match with limit
        let _ = black_box(db.execute("MATCH (n:Person) RETURN n LIMIT 10"));

        // Delete
        let _ = black_box(db.execute("MATCH (n:Person) DELETE n"));
    }

    // Profile
    eprintln!("Profiling {} iterations...", iterations);

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    for _ in 0..iterations {
        let db = Database::in_memory().expect("Failed to create database");

        // Insert
        let mut parts = Vec::with_capacity(nodes);
        for i in 0..nodes {
            parts.push(format!("(n{}:Person {{id: {}}})", i, i));
        }
        let _ = black_box(db.execute(&format!("CREATE {}", parts.join(", "))));

        // Count
        let _ = black_box(db.execute("MATCH (n:Person) RETURN COUNT(n)"));

        // Match with limit
        let _ = black_box(db.execute("MATCH (n:Person) RETURN n LIMIT 10"));

        // Delete
        let _ = black_box(db.execute("MATCH (n:Person) DELETE n"));
    }

    write_flamegraph(guard, output);
}

fn write_flamegraph(guard: pprof::ProfilerGuard, output: &str) {
    if let Ok(report) = guard.report().build() {
        let file = File::create(output).expect("Failed to create output file");
        report.flamegraph(file).expect("Failed to write flamegraph");
        eprintln!("Flamegraph written to: {}", output);
    } else {
        eprintln!("Failed to generate report");
    }
}
