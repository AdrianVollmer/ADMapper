//! Benchmark for basic CRUD operations.
//!
//! Run with: cargo run --release --example bench_crud

use crustdb::Database;
use std::time::Instant;

fn main() {
    println!("CRUD Operations Benchmark");
    println!("==========================\n");

    // Test different data sizes
    let sizes = [100, 500, 1_000, 5_000, 10_000, 50_000];

    // Benchmark: Batch INSERT (single CREATE with many nodes)
    println!("Batch INSERT (single CREATE statement):");
    println!("{:>8} {:>12} {:>12}", "nodes", "time (ms)", "nodes/sec");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    for &n in &sizes {
        let ms = bench_batch_insert(n);
        let rate = (n as f64) / (ms / 1000.0);
        println!("{:>8} {:>12.2} {:>12.0}", n, ms, rate);
    }

    // Benchmark: Individual INSERTs
    println!("\nIndividual INSERTs (one CREATE per node):");
    println!("{:>8} {:>12} {:>12}", "nodes", "time (ms)", "nodes/sec");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    let individual_sizes = [100, 500, 1_000, 2_000];
    for &n in &individual_sizes {
        let ms = bench_individual_insert(n);
        let rate = (n as f64) / (ms / 1000.0);
        println!("{:>8} {:>12.2} {:>12.0}", n, ms, rate);
    }

    // Benchmark: COUNT queries
    println!("\nCOUNT queries (after inserting N nodes):");
    println!("{:>8} {:>12} {:>12}", "nodes", "setup (ms)", "count (ms)");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    for &n in &sizes {
        let (setup_ms, count_ms) = bench_count(n);
        println!("{:>8} {:>12.2} {:>12.2}", n, setup_ms, count_ms);
    }

    // Benchmark: MATCH all nodes
    println!("\nMATCH (n) RETURN n (fetch all nodes):");
    println!("{:>8} {:>12} {:>12}", "nodes", "setup (ms)", "match (ms)");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    let match_sizes = [100, 500, 1_000, 5_000, 10_000];
    for &n in &match_sizes {
        let (setup_ms, match_ms) = bench_match_all(n);
        println!("{:>8} {:>12.2} {:>12.2}", n, setup_ms, match_ms);
    }

    // Benchmark: MATCH with WHERE filter
    println!("\nMATCH with WHERE filter (10% selectivity):");
    println!(
        "{:>8} {:>12} {:>12} {:>8}",
        "nodes", "setup (ms)", "match (ms)", "results"
    );
    println!("{:-<8} {:-<12} {:-<12} {:-<8}", "", "", "", "");

    for &n in &sizes {
        let (setup_ms, match_ms, result_count) = bench_match_filtered(n);
        println!(
            "{:>8} {:>12.2} {:>12.2} {:>8}",
            n, setup_ms, match_ms, result_count
        );
    }

    // Benchmark: MATCH with LIMIT
    println!("\nMATCH with LIMIT (first 10 of N nodes):");
    println!("{:>8} {:>12} {:>12}", "nodes", "setup (ms)", "match (ms)");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    for &n in &sizes {
        let (setup_ms, match_ms) = bench_match_limit(n);
        println!("{:>8} {:>12.2} {:>12.2}", n, setup_ms, match_ms);
    }

    // Benchmark: DELETE all nodes
    println!("\nDELETE all nodes:");
    println!("{:>8} {:>12} {:>12}", "nodes", "setup (ms)", "delete (ms)");
    println!("{:-<8} {:-<12} {:-<12}", "", "", "");

    for &n in &sizes {
        let (setup_ms, delete_ms) = bench_delete_all(n);
        println!("{:>8} {:>12.2} {:>12.2}", n, setup_ms, delete_ms);
    }

    // Benchmark: Mixed workload (insert, query, delete cycle)
    println!("\nMixed workload (insert 1000, count, match 10, delete all) x N:");
    println!("{:>8} {:>12}", "cycles", "total (ms)");
    println!("{:-<8} {:-<12}", "", "");

    let cycles = [1, 5, 10, 20];
    for &n in &cycles {
        let ms = bench_mixed_workload(n);
        println!("{:>8} {:>12.2}", n, ms);
    }
}

/// Benchmark batch insert using a single CREATE statement
fn bench_batch_insert(n: usize) -> f64 {
    let db = Database::in_memory().expect("Failed to create database");

    let start = Instant::now();

    // Build single CREATE with all nodes
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!(
            "(n{}:Person {{id: {}, name: 'Person{}'}})",
            i, i, i
        ));
    }
    let query = format!("CREATE {}", parts.join(", "));
    db.execute(&query).expect("INSERT failed");

    start.elapsed().as_secs_f64() * 1000.0
}

/// Benchmark individual inserts (one CREATE per node)
fn bench_individual_insert(n: usize) -> f64 {
    let db = Database::in_memory().expect("Failed to create database");

    let start = Instant::now();

    for i in 0..n {
        let query = format!("CREATE (n:Person {{id: {}, name: 'Person{}'}})", i, i);
        db.execute(&query).expect("INSERT failed");
    }

    start.elapsed().as_secs_f64() * 1000.0
}

/// Benchmark COUNT query
fn bench_count(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup
    let setup_start = Instant::now();
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!("(n{}:Person {{id: {}}})", i, i));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // COUNT query
    let query_start = Instant::now();
    let result = db
        .execute("MATCH (n:Person) RETURN COUNT(n)")
        .expect("COUNT failed");
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), 1);

    (setup_ms, query_ms)
}

/// Benchmark MATCH all nodes
fn bench_match_all(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup
    let setup_start = Instant::now();
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!("(n{}:Person {{id: {}}})", i, i));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // MATCH query
    let query_start = Instant::now();
    let result = db
        .execute("MATCH (n:Person) RETURN n")
        .expect("MATCH failed");
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), n);

    (setup_ms, query_ms)
}

/// Benchmark MATCH with WHERE filter (10% selectivity)
fn bench_match_filtered(n: usize) -> (f64, f64, usize) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup - give 10% of nodes a special property
    let setup_start = Instant::now();
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        let active = if i % 10 == 0 { "true" } else { "false" };
        parts.push(format!("(n{}:Person {{id: {}, active: {}}})", i, i, active));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // MATCH with WHERE
    let query_start = Instant::now();
    let result = db
        .execute("MATCH (n:Person) WHERE n.active = true RETURN n")
        .expect("MATCH failed");
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    (setup_ms, query_ms, result.rows.len())
}

/// Benchmark MATCH with LIMIT
fn bench_match_limit(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup
    let setup_start = Instant::now();
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!("(n{}:Person {{id: {}}})", i, i));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // MATCH with LIMIT
    let query_start = Instant::now();
    let result = db
        .execute("MATCH (n:Person) RETURN n LIMIT 10")
        .expect("MATCH failed");
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(result.rows.len(), 10);

    (setup_ms, query_ms)
}

/// Benchmark DELETE all nodes
fn bench_delete_all(n: usize) -> (f64, f64) {
    let db = Database::in_memory().expect("Failed to create database");

    // Setup
    let setup_start = Instant::now();
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!("(n{}:Person {{id: {}}})", i, i));
    }
    db.execute(&format!("CREATE {}", parts.join(", ")))
        .expect("Setup failed");
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // DELETE
    let delete_start = Instant::now();
    db.execute("MATCH (n:Person) DELETE n")
        .expect("DELETE failed");
    let delete_ms = delete_start.elapsed().as_secs_f64() * 1000.0;

    // Verify
    let result = db.execute("MATCH (n) RETURN n").expect("Verify failed");
    assert_eq!(result.rows.len(), 0);

    (setup_ms, delete_ms)
}

/// Benchmark mixed workload
fn bench_mixed_workload(cycles: usize) -> f64 {
    let db = Database::in_memory().expect("Failed to create database");

    let start = Instant::now();

    for c in 0..cycles {
        // Insert 1000 nodes
        let mut parts = Vec::with_capacity(1000);
        for i in 0..1000 {
            let id = c * 1000 + i;
            parts.push(format!("(n{}:Person {{id: {}}})", id, id));
        }
        db.execute(&format!("CREATE {}", parts.join(", ")))
            .expect("INSERT failed");

        // Count
        let _count = db
            .execute("MATCH (n:Person) RETURN COUNT(n)")
            .expect("COUNT failed");

        // Match first 10
        let _match = db
            .execute("MATCH (n:Person) RETURN n LIMIT 10")
            .expect("MATCH failed");

        // Delete all
        db.execute("MATCH (n:Person) DELETE n")
            .expect("DELETE failed");
    }

    start.elapsed().as_secs_f64() * 1000.0
}
