//! Profile the exact queries that are slow in e2e tests.
//!
//! The e2e tests show these CrustDB queries taking 1200-1400ms while Neo4j/FalkorDB
//! take ~50ms (HTTP overhead only). This profiler generates flamegraphs for:
//!
//! 1. `MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1` ("Query with relationship")
//! 2. `MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5` ("Query with type() function")
//!
//! The graph is built to match e2e data size: ~1500 nodes, ~13000 relationships.
//!
//! # Usage
//!
//! ```bash
//! cargo run --release --example profile_e2e_queries
//! cargo run --release --example profile_e2e_queries -- --op count-r
//! cargo run --release --example profile_e2e_queries -- --op type-r
//! ```

use crustdb::Database;
use pprof::ProfilerGuardBuilder;
use std::env;
use std::fs::File;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();

    let operation = args
        .iter()
        .position(|a| a == "--op")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("all");

    let iterations: usize = args
        .iter()
        .position(|a| a == "--iterations" || a == "-i")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let warmup: usize = args
        .iter()
        .position(|a| a == "--warmup" || a == "-w")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    // Build e2e-scale graph
    let db = Database::in_memory().expect("Failed to create database");
    let (nodes, rels) = build_e2e_graph(&db);
    eprintln!("Graph: {} nodes, {} relationships", nodes, rels);

    match operation {
        "count-r" => profile_count_r(&db, iterations, warmup),
        "type-r" => profile_type_r(&db, iterations, warmup),
        "all" => {
            profile_count_r(&db, iterations, warmup);
            profile_type_r(&db, iterations, warmup);
        }
        _ => {
            eprintln!(
                "Unknown operation: {}. Use: count-r, type-r, all",
                operation
            );
            std::process::exit(1);
        }
    }
}

/// Build a graph matching the e2e BloodHound dataset (~1500 nodes, ~13000 relationships).
fn build_e2e_graph(db: &Database) -> (usize, usize) {
    let start = Instant::now();

    let user_count = 500;
    let group_count = 300;
    let computer_count = 200;
    let ou_count = 50;
    let domain_count = 3;

    // Batch-create nodes
    let mut nodes: Vec<(Vec<String>, serde_json::Value)> = Vec::with_capacity(1100);

    for i in 0..user_count {
        nodes.push((
            vec!["User".to_string()],
            serde_json::json!({
                "objectid": format!("U_{}", i),
                "name": format!("User{}", i),
                "enabled": true,
            }),
        ));
    }
    for i in 0..group_count {
        let mut props = serde_json::json!({
            "objectid": format!("G_{}", i),
            "name": format!("Group{}", i),
        });
        if i < 5 {
            props["tier"] = serde_json::json!(0);
        }
        nodes.push((vec!["Group".to_string()], props));
    }
    for i in 0..computer_count {
        nodes.push((
            vec!["Computer".to_string()],
            serde_json::json!({
                "objectid": format!("C_{}", i),
                "name": format!("Computer{}", i),
            }),
        ));
    }
    for i in 0..ou_count {
        nodes.push((
            vec!["OU".to_string()],
            serde_json::json!({
                "objectid": format!("OU_{}", i),
                "name": format!("OrgUnit{}", i),
            }),
        ));
    }
    for i in 0..domain_count {
        nodes.push((
            vec!["Domain".to_string()],
            serde_json::json!({
                "objectid": format!("D_{}", i),
                "name": format!("Domain{}", i),
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

    // Build lookup by objectid
    let mut oid_to_id: std::collections::HashMap<String, i64> =
        std::collections::HashMap::with_capacity(node_ids.len());
    for (i, (_, props)) in nodes.iter().enumerate() {
        if let Some(oid) = props.get("objectid").and_then(|v| v.as_str()) {
            oid_to_id.insert(oid.to_string(), node_ids[i]);
        }
    }

    let edge_start = Instant::now();
    let mut edges: Vec<(i64, i64, String, serde_json::Value)> = Vec::new();

    // User -> Group MemberOf: ~1200
    for i in 0..user_count {
        for g in [i % group_count, (i * 7 + 13) % group_count] {
            edges.push((
                oid_to_id[&format!("U_{}", i)],
                oid_to_id[&format!("G_{}", g)],
                "MemberOf".to_string(),
                serde_json::json!({}),
            ));
        }
        if i % 3 == 0 {
            let g = (i * 11 + 37) % group_count;
            edges.push((
                oid_to_id[&format!("U_{}", i)],
                oid_to_id[&format!("G_{}", g)],
                "MemberOf".to_string(),
                serde_json::json!({}),
            ));
        }
    }

    // Group -> Group MemberOf: ~1800
    for i in 0..group_count {
        for offset in &[1usize, 3, 7, 15, 31, 67] {
            let target = (i + offset) % group_count;
            if target != i {
                edges.push((
                    oid_to_id[&format!("G_{}", i)],
                    oid_to_id[&format!("G_{}", target)],
                    "MemberOf".to_string(),
                    serde_json::json!({}),
                ));
            }
        }
    }

    // User -> Computer: HasSession + AdminTo: ~670
    for i in 0..user_count {
        let c = i % computer_count;
        edges.push((
            oid_to_id[&format!("U_{}", i)],
            oid_to_id[&format!("C_{}", c)],
            "HasSession".to_string(),
            serde_json::json!({}),
        ));
        if i % 3 == 0 {
            let c2 = (i * 3 + 7) % computer_count;
            edges.push((
                oid_to_id[&format!("U_{}", i)],
                oid_to_id[&format!("C_{}", c2)],
                "AdminTo".to_string(),
                serde_json::json!({}),
            ));
        }
    }

    // Computer -> Group MemberOf: ~300
    for i in 0..computer_count {
        let g = (i * 3) % group_count;
        edges.push((
            oid_to_id[&format!("C_{}", i)],
            oid_to_id[&format!("G_{}", g)],
            "MemberOf".to_string(),
            serde_json::json!({}),
        ));
        if i % 2 == 0 {
            let g2 = (i * 7 + 11) % group_count;
            edges.push((
                oid_to_id[&format!("C_{}", i)],
                oid_to_id[&format!("G_{}", g2)],
                "MemberOf".to_string(),
                serde_json::json!({}),
            ));
        }
    }

    // OU -> Group Contains: ~300
    for i in 0..group_count {
        let ou = i % ou_count;
        edges.push((
            oid_to_id[&format!("OU_{}", ou)],
            oid_to_id[&format!("G_{}", i)],
            "Contains".to_string(),
            serde_json::json!({}),
        ));
    }

    // Domain -> OU Contains: ~50
    for i in 0..ou_count {
        let d = i % domain_count;
        edges.push((
            oid_to_id[&format!("D_{}", d)],
            oid_to_id[&format!("OU_{}", i)],
            "Contains".to_string(),
            serde_json::json!({}),
        ));
    }

    // Group -> Computer cross-edges: ~1800
    for i in 0..group_count {
        for offset in &[1usize, 5, 11, 23, 47, 97] {
            let c = (i + offset) % computer_count;
            let rel_type = match offset % 4 {
                0 => "CanRDP",
                1 => "CanPSRemote",
                2 => "ExecuteDCOM",
                _ => "GenericAll",
            };
            edges.push((
                oid_to_id[&format!("G_{}", i)],
                oid_to_id[&format!("C_{}", c)],
                rel_type.to_string(),
                serde_json::json!({}),
            ));
        }
    }

    // User -> User CanRDP: ~100
    for i in (0..user_count).step_by(5) {
        let target = (i + 17) % user_count;
        if target != i {
            edges.push((
                oid_to_id[&format!("U_{}", i)],
                oid_to_id[&format!("U_{}", target)],
                "CanRDP".to_string(),
                serde_json::json!({}),
            ));
        }
    }

    db.insert_relationships_batch(&edges)
        .expect("Failed to insert relationships");
    eprintln!(
        "  Inserted {} relationships in {:?}",
        edges.len(),
        edge_start.elapsed()
    );
    eprintln!("  Total build time: {:?}", start.elapsed());

    let stats = db.stats().unwrap();
    (stats.node_count, stats.relationship_count)
}

fn profile_count_r(db: &Database, iterations: usize, warmup: usize) {
    let query = "MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1";
    eprintln!("\n--- Profiling: count(r) ---");
    eprintln!("Query: {}", query);

    // Warmup
    for _ in 0..warmup {
        let _ = black_box(db.execute(query));
    }

    // First, time a single run to see if it's actually slow
    let single_start = Instant::now();
    let result = db.execute(query).expect("Query failed");
    let single_ms = single_start.elapsed();
    eprintln!("  Single run: {:?} ({} rows)", single_ms, result.rows.len());

    // Profile
    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  {} iterations: {:?} ({:.2}ms/iter)",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, "flamegraph_e2e_count_r.svg");
}

fn profile_type_r(db: &Database, iterations: usize, warmup: usize) {
    let query = "MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5";
    eprintln!("\n--- Profiling: type(r) LIMIT 5 ---");
    eprintln!("Query: {}", query);

    // Warmup
    for _ in 0..warmup {
        let _ = black_box(db.execute(query));
    }

    // Single run timing
    let single_start = Instant::now();
    let result = db.execute(query).expect("Query failed");
    let single_ms = single_start.elapsed();
    eprintln!("  Single run: {:?} ({} rows)", single_ms, result.rows.len());

    // Profile
    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("Failed to build profiler");

    let start = Instant::now();
    for _ in 0..iterations {
        let result = db.execute(query).expect("Query failed");
        black_box(result);
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  {} iterations: {:?} ({:.2}ms/iter)",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    write_flamegraph(guard, "flamegraph_e2e_type_r.svg");
}

fn write_flamegraph(guard: pprof::ProfilerGuard, output: &str) {
    match guard.report().build() {
        Ok(report) => {
            let file = File::create(output).expect("Failed to create output file");
            report.flamegraph(file).expect("Failed to write flamegraph");
            eprintln!("  Flamegraph: {}", output);
        }
        Err(e) => {
            eprintln!("  Failed to build report: {}", e);
        }
    }
}
