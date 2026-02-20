//! CrustDB Stress Test Benchmark
//!
//! A reusable benchmark harness for stress testing CrustDB with:
//! - Synthetic graph topologies at progressive scales
//! - Query workloads designed to expose bottlenecks
//! - Optional comparison against FalkorDB and Neo4j
//!
//! Run with: cargo run --release --example bench_stress -- --help
//!
//! Examples:
//!   cargo run --release --example bench_stress
//!   cargo run --release --example bench_stress -- --topology chain --scales 1000,10000
//!   cargo run --release --example bench_stress -- --topology all --compare

mod comparison;
mod generators;

use comparison::{load_falkordb, load_neo4j, run_falkordb_queries, run_neo4j_queries};
use crustdb::Database;
use generators::{generate, GeneratedGraph, Topology};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// =============================================================================
// Configuration
// =============================================================================

/// Benchmark configuration.
struct Config {
    /// Topologies to test.
    topologies: Vec<Topology>,
    /// Scales to test (number of nodes).
    scales: Vec<usize>,
    /// Which query workloads to run.
    queries: Vec<QueryType>,
    /// Also run against FalkorDB/Neo4j.
    compare: bool,
    /// Output file for results.
    output: PathBuf,
    /// Timeout per query in seconds.
    timeout_secs: u64,
    /// Number of warmup runs before measurement.
    warmup_runs: usize,
    /// Number of measurement runs for statistics.
    measure_runs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            topologies: vec![Topology::LongChain],
            scales: vec![1000, 10_000],
            queries: vec![
                QueryType::PointLookup,
                QueryType::SingleHop,
                QueryType::BoundedVarLength,
                QueryType::CountAll,
                QueryType::FullScanFilter,
                QueryType::DeepShortestPath,
                QueryType::UnboundedVarLength,
                QueryType::MultiPathBfs,
                QueryType::HighFanOut,
            ],
            compare: false,
            output: PathBuf::from("stress_results.json"),
            timeout_secs: 60,
            warmup_runs: 1,
            measure_runs: 3,
        }
    }
}

// =============================================================================
// Query Types
// =============================================================================

/// Types of queries to benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryType {
    // Baseline queries
    PointLookup,
    SingleHop,
    BoundedVarLength,
    CountAll,

    // Killer queries (stress tests)
    FullScanFilter,     // K1: Full scan with property filter
    DeepShortestPath,   // K2: Deep shortest path
    UnboundedVarLength, // K3: Variable-length unbounded
    MultiPathBfs,       // K4: Multi-path BFS (k > 1)
    HighFanOut,         // K5: High fan-out expansion
}

impl QueryType {
    fn name(&self) -> &'static str {
        match self {
            QueryType::PointLookup => "point_lookup",
            QueryType::SingleHop => "single_hop",
            QueryType::BoundedVarLength => "bounded_var_length",
            QueryType::CountAll => "count_all",
            QueryType::FullScanFilter => "K1_full_scan_filter",
            QueryType::DeepShortestPath => "K2_deep_shortest",
            QueryType::UnboundedVarLength => "K3_unbounded_var",
            QueryType::MultiPathBfs => "K4_multi_path_bfs",
            QueryType::HighFanOut => "K5_high_fanout",
        }
    }

    #[allow(dead_code)]
    fn is_killer(&self) -> bool {
        matches!(
            self,
            QueryType::FullScanFilter
                | QueryType::DeepShortestPath
                | QueryType::UnboundedVarLength
                | QueryType::MultiPathBfs
                | QueryType::HighFanOut
        )
    }
}

// =============================================================================
// Results
// =============================================================================

/// Result of a single query execution.
#[derive(Debug, Clone, serde::Serialize)]
struct QueryResult {
    query_name: String,
    scale: usize,
    topology: String,
    database: String,
    latency_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    memory_mb: f64,
    rows_returned: usize,
    success: bool,
    error: Option<String>,
}

/// Aggregate results for a benchmark run.
#[derive(Debug, serde::Serialize)]
struct BenchmarkResults {
    topology: String,
    scale: usize,
    database: String,
    queries: Vec<QueryResult>,
    setup_time_ms: f64,
    total_nodes: usize,
    total_edges: usize,
}

// =============================================================================
// Benchmark Runner
// =============================================================================

fn main() {
    let config = parse_args();

    println!("CrustDB Stress Test Benchmark");
    println!("==============================\n");

    if config.compare {
        println!("Comparison mode: Will run against FalkorDB and Neo4j (requires Docker)\n");
        if !comparison::docker_available() {
            eprintln!("Warning: Docker not available, skipping external comparisons");
        }
    }

    let mut all_results: Vec<BenchmarkResults> = Vec::new();

    for topology in &config.topologies {
        println!("\n{:=<60}", "");
        println!("Topology: {}", topology);
        println!("{:=<60}\n", "");

        for &scale in &config.scales {
            println!("  Scale: {} nodes", scale);

            // Generate graph
            let gen_start = Instant::now();
            let graph = generate(*topology, scale);
            let gen_time = gen_start.elapsed();
            println!(
                "    Generated: {} nodes, {} edges in {:.2}ms",
                graph.node_count(),
                graph.edge_count(),
                gen_time.as_secs_f64() * 1000.0
            );

            // Run CrustDB benchmark
            match run_crustdb_benchmark(&config, *topology, &graph) {
                Ok(results) => {
                    print_results_summary(&results);
                    all_results.push(results);
                }
                Err(e) => {
                    eprintln!("    CrustDB ERROR: {}", e);
                    // Record failure
                    all_results.push(BenchmarkResults {
                        topology: topology.to_string(),
                        scale,
                        database: "crustdb".to_string(),
                        queries: vec![QueryResult {
                            query_name: "setup".to_string(),
                            scale,
                            topology: topology.to_string(),
                            database: "crustdb".to_string(),
                            latency_ms: 0.0,
                            p50_ms: 0.0,
                            p95_ms: 0.0,
                            p99_ms: 0.0,
                            memory_mb: 0.0,
                            rows_returned: 0,
                            success: false,
                            error: Some(e),
                        }],
                        setup_time_ms: 0.0,
                        total_nodes: graph.node_count(),
                        total_edges: graph.edge_count(),
                    });
                }
            }

            // Run comparison benchmarks if requested
            if config.compare && comparison::docker_available() {
                // FalkorDB
                if comparison::falkordb_available() {
                    match run_falkordb_benchmark(&config, *topology, &graph) {
                        Ok(results) => {
                            print_results_summary(&results);
                            all_results.push(results);
                        }
                        Err(e) => {
                            eprintln!("    FalkorDB ERROR: {}", e);
                        }
                    }
                }

                // Neo4j
                if comparison::neo4j_available() {
                    match run_neo4j_benchmark(&config, *topology, &graph) {
                        Ok(results) => {
                            print_results_summary(&results);
                            all_results.push(results);
                        }
                        Err(e) => {
                            eprintln!("    Neo4j ERROR: {}", e);
                        }
                    }
                }
            }

            // Check if we should stop (OOM or timeout indication)
            if should_stop(&all_results) {
                println!("    Stopping scale progression (failures detected)");
                break;
            }
        }
    }

    // Write results to file
    write_results(&config.output, &all_results);

    println!("\n{:=<60}", "");
    println!("Results written to: {}", config.output.display());
}

// =============================================================================
// CrustDB Benchmark
// =============================================================================

fn run_crustdb_benchmark(
    config: &Config,
    topology: Topology,
    graph: &GeneratedGraph,
) -> Result<BenchmarkResults, String> {
    let db = Database::in_memory().map_err(|e| e.to_string())?;

    // Load graph
    let setup_start = Instant::now();
    load_crustdb(&db, graph)?;
    let setup_time_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    println!("    Loaded into CrustDB in {:.2}ms", setup_time_ms);

    // Run queries
    let mut query_results = Vec::new();
    for query_type in &config.queries {
        let result = run_crustdb_query(
            &db,
            *query_type,
            graph.node_count(),
            topology,
            config.warmup_runs,
            config.measure_runs,
            config.timeout_secs,
        );
        query_results.push(result);
    }

    Ok(BenchmarkResults {
        topology: topology.to_string(),
        scale: graph.node_count(),
        database: "crustdb".to_string(),
        queries: query_results,
        setup_time_ms,
        total_nodes: graph.node_count(),
        total_edges: graph.edge_count(),
    })
}

fn load_crustdb(db: &Database, graph: &GeneratedGraph) -> Result<(), String> {
    // Use batch insert for efficiency
    let node_ids = db
        .insert_nodes_batch(&graph.nodes)
        .map_err(|e| e.to_string())?;

    // Convert edges to use database IDs
    let edges: Vec<_> = graph
        .edges
        .iter()
        .map(|(src_idx, tgt_idx, edge_type, props)| {
            (
                node_ids[*src_idx],
                node_ids[*tgt_idx],
                edge_type.clone(),
                props.clone(),
            )
        })
        .collect();

    db.insert_edges_batch(&edges).map_err(|e| e.to_string())?;

    Ok(())
}

fn run_crustdb_query(
    db: &Database,
    query_type: QueryType,
    scale: usize,
    topology: Topology,
    warmup_runs: usize,
    measure_runs: usize,
    timeout_secs: u64,
) -> QueryResult {
    let query = build_crustdb_query(query_type, scale);

    // Warmup runs
    for _ in 0..warmup_runs {
        let _ = db.execute(&query);
    }

    // Measurement runs
    let mut latencies = Vec::with_capacity(measure_runs);
    let mut rows_returned = 0;
    let mut success = true;
    let mut error = None;

    for _ in 0..measure_runs {
        let start = Instant::now();
        match db.execute(&query) {
            Ok(result) => {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(timeout_secs) {
                    success = false;
                    error = Some("Timeout".to_string());
                    latencies.push(timeout_secs as f64 * 1000.0);
                } else {
                    latencies.push(elapsed.as_secs_f64() * 1000.0);
                    rows_returned = result.rows.len();
                }
            }
            Err(e) => {
                success = false;
                error = Some(e.to_string());
                latencies.push(0.0);
            }
        }
    }

    // Calculate percentiles
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);

    QueryResult {
        query_name: query_type.name().to_string(),
        scale,
        topology: topology.to_string(),
        database: "crustdb".to_string(),
        latency_ms: avg,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        memory_mb: 0.0, // Would need external measurement
        rows_returned,
        success,
        error,
    }
}

fn build_crustdb_query(query_type: QueryType, scale: usize) -> String {
    // Target node for path queries (typically last node)
    let target_id = scale.saturating_sub(1);

    match query_type {
        QueryType::PointLookup => {
            "MATCH (n:Node {id: 0}) RETURN n".to_string()
        }
        QueryType::SingleHop => {
            "MATCH (n:Node {id: 0})-[r]->(m) RETURN m LIMIT 10".to_string()
        }
        QueryType::BoundedVarLength => {
            // Note: CrustDB doesn't support COUNT(DISTINCT ...), so just count rows
            "MATCH (s:Node {id: 0})-[*1..3]->(t) RETURN t LIMIT 100".to_string()
        }
        QueryType::CountAll => {
            "MATCH (n) RETURN COUNT(n)".to_string()
        }
        QueryType::FullScanFilter => {
            // K1: Full scan with property filter (no index)
            "MATCH (n) WHERE n.value > 500 RETURN COUNT(n)".to_string()
        }
        QueryType::DeepShortestPath => {
            // K2: Deep shortest path
            format!(
                "MATCH p = SHORTEST 1 (s:Node {{id: 0}})-[:NEXT|:EDGE|:CONNECTED|:LINKED|:CHILD|:SHORTCUT]-+(t:Node {{id: {}}}) RETURN length(p)",
                target_id
            )
        }
        QueryType::UnboundedVarLength => {
            // K3: Variable-length unbounded (capped at reasonable depth)
            // Note: CrustDB doesn't support COUNT(DISTINCT ...), so just return count
            "MATCH (s:Node {id: 0})-[*1..50]->(t) RETURN t LIMIT 1000".to_string()
        }
        QueryType::MultiPathBfs => {
            // K4: Multi-path BFS (k > 1)
            format!(
                "MATCH p = SHORTEST 10 (s:Node {{id: 0}})-[*1..20]->(t:Node {{id: {}}}) RETURN p",
                target_id / 2
            )
        }
        QueryType::HighFanOut => {
            // K5: High fan-out expansion (count all neighbors of hub node 0)
            "MATCH (hub:Node {id: 0})-[]->(neighbor) RETURN COUNT(neighbor)".to_string()
        }
    }
}

// =============================================================================
// FalkorDB Benchmark
// =============================================================================

fn run_falkordb_benchmark(
    config: &Config,
    topology: Topology,
    graph: &GeneratedGraph,
) -> Result<BenchmarkResults, String> {
    let graph_name = "bench_graph";

    // Load graph
    let setup_start = Instant::now();
    load_falkordb(graph, graph_name)?;
    let setup_time_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    println!("    Loaded into FalkorDB in {:.2}ms", setup_time_ms);

    // Build queries
    let queries: Vec<_> = config
        .queries
        .iter()
        .map(|qt| {
            let crustdb_query = build_crustdb_query(*qt, graph.node_count());
            let falkor_query = comparison::translate_for_falkordb(&crustdb_query);
            (qt.name().to_string(), falkor_query)
        })
        .collect();

    // Run queries
    let comparison_results = run_falkordb_queries(graph_name, &queries);

    // Convert to QueryResult
    let query_results: Vec<_> = comparison_results
        .into_iter()
        .map(|cr| QueryResult {
            query_name: cr.query_name,
            scale: graph.node_count(),
            topology: topology.to_string(),
            database: "falkordb".to_string(),
            latency_ms: cr.latency_ms,
            p50_ms: cr.latency_ms,
            p95_ms: cr.latency_ms,
            p99_ms: cr.latency_ms,
            memory_mb: 0.0,
            rows_returned: cr.rows_returned.unwrap_or(0),
            success: cr.success,
            error: cr.error,
        })
        .collect();

    Ok(BenchmarkResults {
        topology: topology.to_string(),
        scale: graph.node_count(),
        database: "falkordb".to_string(),
        queries: query_results,
        setup_time_ms,
        total_nodes: graph.node_count(),
        total_edges: graph.edge_count(),
    })
}

// =============================================================================
// Neo4j Benchmark
// =============================================================================

fn run_neo4j_benchmark(
    config: &Config,
    topology: Topology,
    graph: &GeneratedGraph,
) -> Result<BenchmarkResults, String> {
    // Load graph
    let setup_start = Instant::now();
    load_neo4j(graph)?;
    let setup_time_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    println!("    Loaded into Neo4j in {:.2}ms", setup_time_ms);

    // Build queries
    let queries: Vec<_> = config
        .queries
        .iter()
        .map(|qt| {
            let crustdb_query = build_crustdb_query(*qt, graph.node_count());
            let neo4j_query = comparison::translate_for_neo4j(&crustdb_query);
            (qt.name().to_string(), neo4j_query)
        })
        .collect();

    // Run queries
    let comparison_results = run_neo4j_queries(&queries);

    // Convert to QueryResult
    let query_results: Vec<_> = comparison_results
        .into_iter()
        .map(|cr| QueryResult {
            query_name: cr.query_name,
            scale: graph.node_count(),
            topology: topology.to_string(),
            database: "neo4j".to_string(),
            latency_ms: cr.latency_ms,
            p50_ms: cr.latency_ms,
            p95_ms: cr.latency_ms,
            p99_ms: cr.latency_ms,
            memory_mb: 0.0,
            rows_returned: cr.rows_returned.unwrap_or(0),
            success: cr.success,
            error: cr.error,
        })
        .collect();

    Ok(BenchmarkResults {
        topology: topology.to_string(),
        scale: graph.node_count(),
        database: "neo4j".to_string(),
        queries: query_results,
        setup_time_ms,
        total_nodes: graph.node_count(),
        total_edges: graph.edge_count(),
    })
}

// =============================================================================
// Utilities
// =============================================================================

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn print_results_summary(results: &BenchmarkResults) {
    println!(
        "    {} results ({} nodes, {} edges):",
        results.database, results.total_nodes, results.total_edges
    );
    println!(
        "      {:30} {:>12} {:>12} {:>8} {:>8}",
        "Query", "Latency(ms)", "P99(ms)", "Rows", "Status"
    );
    println!("      {:->30} {:->12} {:->12} {:->8} {:->8}", "", "", "", "", "");

    for qr in &results.queries {
        let status = if qr.success { "OK" } else { "FAIL" };
        println!(
            "      {:30} {:>12.2} {:>12.2} {:>8} {:>8}",
            qr.query_name, qr.latency_ms, qr.p99_ms, qr.rows_returned, status
        );
    }
}

fn should_stop(results: &[BenchmarkResults]) -> bool {
    // Check if the last CrustDB result had failures
    results
        .last()
        .map(|r| r.database == "crustdb" && r.queries.iter().any(|q| !q.success))
        .unwrap_or(false)
}

fn write_results(path: &PathBuf, results: &[BenchmarkResults]) {
    let json = serde_json::to_string_pretty(results).unwrap_or_else(|e| {
        eprintln!("Failed to serialize results: {}", e);
        "[]".to_string()
    });

    match File::create(path) {
        Ok(mut file) => {
            if let Err(e) = file.write_all(json.as_bytes()) {
                eprintln!("Failed to write results: {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to create results file: {}", e);
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--topology" | "-t" => {
                i += 1;
                if i < args.len() {
                    if args[i] == "all" {
                        config.topologies = vec![
                            Topology::DenseCluster,
                            Topology::LongChain,
                            Topology::WideFanOut,
                            Topology::PowerLaw,
                        ];
                    } else {
                        config.topologies = args[i]
                            .split(',')
                            .filter_map(|s| s.parse().ok())
                            .collect();
                    }
                }
            }
            "--scales" | "-s" => {
                i += 1;
                if i < args.len() {
                    config.scales = args[i]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                }
            }
            "--compare" | "-c" => {
                config.compare = true;
            }
            "--output" | "-o" => {
                i += 1;
                if i < args.len() {
                    config.output = PathBuf::from(&args[i]);
                }
            }
            "--timeout" => {
                i += 1;
                if i < args.len() {
                    config.timeout_secs = args[i].parse().unwrap_or(60);
                }
            }
            "--baseline-only" => {
                config.queries = vec![
                    QueryType::PointLookup,
                    QueryType::SingleHop,
                    QueryType::BoundedVarLength,
                    QueryType::CountAll,
                ];
            }
            "--killer-only" => {
                config.queries = vec![
                    QueryType::FullScanFilter,
                    QueryType::DeepShortestPath,
                    QueryType::UnboundedVarLength,
                    QueryType::MultiPathBfs,
                    QueryType::HighFanOut,
                ];
            }
            _ => {}
        }
        i += 1;
    }

    config
}

fn print_help() {
    println!(
        r#"CrustDB Stress Test Benchmark

USAGE:
    cargo run --release --example bench_stress [OPTIONS]

OPTIONS:
    -t, --topology <TOPO>   Topology: dense_cluster, long_chain, wide_fanout, power_law, or "all"
    -s, --scales <SCALES>   Comma-separated scales (node counts), e.g., "1000,10000,100000"
    -c, --compare           Also benchmark FalkorDB and Neo4j (requires Docker)
    -o, --output <FILE>     Output JSON file (default: stress_results.json)
    --timeout <SECS>        Query timeout in seconds (default: 60)
    --baseline-only         Run only baseline queries (not killer queries)
    --killer-only           Run only killer queries (stress tests)
    -h, --help              Show this help message

TOPOLOGIES:
    dense_cluster   Near-clique (10% edge density) - tests BFS explosion
    long_chain      Linear path with shortcuts - tests deep traversals
    wide_fanout     Tree with branching=100 - tests high-degree node expansion
    power_law       Barabási-Albert scale-free - tests skewed degree distribution

QUERIES:
    Baseline:
        point_lookup        - Single node by property
        single_hop          - One-hop traversal
        bounded_var_length  - Variable-length 1..3 hops
        count_all           - COUNT(*) aggregation

    Killer (stress tests):
        K1_full_scan_filter - Full scan with property filter (no index)
        K2_deep_shortest    - Deep shortest path to last node
        K3_unbounded_var    - Variable-length up to 50 hops
        K4_multi_path_bfs   - SHORTEST 10 paths
        K5_high_fanout      - Count all neighbors of hub node

EXAMPLES:
    # Quick test with chain topology
    cargo run --release --example bench_stress

    # Full stress test with all topologies
    cargo run --release --example bench_stress -- --topology all --scales 1000,10000,100000

    # Compare against FalkorDB and Neo4j
    cargo run --release --example bench_stress -- --compare

    # Only run killer queries on large scale
    cargo run --release --example bench_stress -- --killer-only --scales 100000
"#
    );
}
