# CrustDB

An embedded graph database with SQLite backend and Cypher support, written in Rust.

CrustDB implements a subset of [openCypher 9](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf),
the vendor-neutral graph query language specification.

## Features

- **Embedded**: No separate server process, links directly into your application
- **SQLite Backend**: Battle-tested storage with ACID transactions
- **openCypher 9**: Vendor-neutral graph query language
- **Property Graph Model**: Nodes and relationships with labels, types, and arbitrary properties

## SQLite Schema

### Table `meta`

Schema versioning for migrations.

| Column | Type | Description |
|--------|------|-------------|
| key | TEXT PRIMARY KEY | Metadata key |
| value | TEXT | Metadata value |

### Table `node_labels`

Normalized storage for node label strings.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Label ID |
| name | TEXT UNIQUE | Label name (e.g., "Person", "Movie") |

### Table `rel_types`

Normalized storage for relationship type strings.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Type ID |
| name | TEXT UNIQUE | Type name (e.g., "KNOWS", "ACTED_IN") |

### Table `nodes`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Node ID |
| properties | TEXT | JSON-encoded properties |

### Table `node_label_map`

Many-to-many relationship between nodes and labels.

| Column | Type | Description |
|--------|------|-------------|
| node_id | INTEGER | FK -> nodes.id |
| label_id | INTEGER | FK -> node_labels.id |

### Table `relationships`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Relationship ID |
| source_id | INTEGER | FK -> nodes.id |
| target_id | INTEGER | FK -> nodes.id |
| type_id | INTEGER | FK -> rel_types.id |
| properties | TEXT | JSON-encoded properties |

### Indexes

- `idx_node_label_map_label` on node_label_map(label_id) - Fast label lookups
- `idx_node_label_map_node` on node_label_map(node_id) - Fast node label retrieval
- `idx_edges_source` on relationships(source_id) - Fast outgoing relationship traversal
- `idx_edges_target` on relationships(target_id) - Fast incoming relationship traversal
- `idx_edges_type` on relationships(type_id) - Fast relationship type filtering

## Milestones

### M1: Storage Layer [done]

Basic CRUD operations for nodes and relationships.

- [x] SQLite schema with migrations
- [x] Insert/get/delete nodes
- [x] Insert/get/delete relationships
- [x] Property serialization (JSON)
- [x] Database statistics

### M2: Simple CREATE Queries [done]

Parse and execute basic node/relationship creation.

```cypher
CREATE (n:Person {name: 'Alice', age: 30})

CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})

CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),
       (oliver:Person:Director {name: 'Oliver Stone'}),
       (wallStreet:Movie {title: 'Wall Street'}),
       (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),
       (oliver)-[:DIRECTED]->(wallStreet)
```

- [x] Cypher lexer/tokenizer (pest grammar)
- [x] AST builder (pest tree → clean AST)
- [x] CREATE clause parser
- [x] Node pattern parser (labels, properties)
- [x] Relationship pattern parser
- [x] CREATE executor

### M3: Simple MATCH Queries [done]

Parse and execute basic read queries.

```cypher
MATCH (n) RETURN n

MATCH (n:Person) RETURN n

MATCH (n:Person) RETURN n.name, n.age

MATCH (n:Person {name: 'Alice'}) RETURN n
```

- [x] MATCH clause parser
- [x] RETURN clause parser
- [x] Node scan operator
- [x] Label filter operator
- [x] Property filter operator
- [x] Projection operator

### M4: WHERE Clause [done]

Add filtering with WHERE conditions.

```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n

MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n

MATCH (n:Person) WHERE n.age >= 18 AND n.age <= 65 RETURN n
```

- [x] WHERE clause parser
- [x] Comparison operators (=, <>, <, <=, >, >=)
- [x] Logical operators (AND, OR, NOT)
- [x] String predicates (STARTS WITH, ENDS WITH, CONTAINS)
- [x] NULL checks (IS NULL, IS NOT NULL)

### M5: Single-Hop Traversal [done]

Navigate one relationship step.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name

MATCH (a)-[r]->(b) RETURN a, r, b

MATCH (m:Movie)<-[:ACTED_IN]-(a:Actor) RETURN m.title, a.name
```

- [x] Relationship pattern parser (direction, type, variable)
- [x] Expand operator (outgoing)
- [x] Expand operator (incoming)
- [x] Expand operator (undirected)

### M6: Multi-Hop Traversal [done]

Variable-length path queries.

```cypher
MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b

MATCH p = (a:Person)-[:KNOWS*]->(b:Person) RETURN p

MATCH (a)-[r:KNOWS*2]->(b) RETURN r
```

- [x] Variable-length pattern parser (`*`, `*n`, `*n..m`, `*n..`, `*..m`)
- [x] BFS traversal operator
- [x] Cycle avoidance (per-path visited tracking)
- [x] Path construction (`p = ...` syntax)
- [x] Relationship list binding (`[r:KNOWS*]` returning list)

### M7: Mutation Queries [done]

UPDATE and DELETE operations.

```cypher
MATCH (n:Person {name: 'Alice'}) SET n.age = 31

MATCH (n:Person {name: 'Alice'}) SET n:Employee

MATCH (n:Person {name: 'Bob'}) DELETE n

MATCH (n:Person {name: 'Charlie'}) DETACH DELETE n
```

- [x] SET clause parser
- [x] DELETE clause parser
- [x] Update operator
- [x] Delete operator (with DETACH)

### M8: Aggregation [done]

GROUP BY and aggregate functions.

```cypher
MATCH (n:Person) RETURN count(n)

MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, count(f) as friends

MATCH (n:Person) RETURN avg(n.age), min(n.age), max(n.age)
```

- [x] Aggregate function parser (count, sum, avg, min, max, collect)
- [x] GROUP BY detection (implicit, from non-aggregate RETURN expressions)
- [x] Aggregation operator

### M9: Functions [done]

Built-in Cypher functions.

```cypher
MATCH ()-[r]->() RETURN type(r)

MATCH (n) RETURN id(n), labels(n)

MATCH (n:Person) RETURN toLower(n.name), size(n.name)
```

- [x] type(r), id(n), labels(n)
- [x] size(), length()
- [x] toLower(), toUpper()
- [x] coalesce()

### M10: Query Optimization [open]

Basic query planning and optimization.

- [ ] Cost-based join ordering
- [ ] Index selection
- [ ] Predicate pushdown
- [ ] Projection pushdown

### M11: Advanced Features [open]

- [ ] MERGE (upsert)
- [ ] OPTIONAL MATCH
- [x] UNION ALL
- [ ] UNION (with deduplication)
- [ ] WITH clause (query chaining)
- [ ] ORDER BY
- [x] SKIP, LIMIT
- [x] DISTINCT
- [ ] CASE expressions
- [ ] List comprehensions

## Usage

```rust
use crustdb::Database;

// Open or create database
let db = Database::open("my_graph.db")?;

// Create nodes and relationships
db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;

// Query the graph
let results = db.execute("MATCH (n:Person) RETURN n.name")?;
for row in results.rows {
    println!("{:?}", row);
}
```

## Test cases

Test fixtures derived from [GrandCypher](https://github.com/aplbrain/grand-cypher)
are in `tests/fixtures/`, organized by milestone:

- `m2_create/` - CREATE clause tests
- `m3_match/` - Basic MATCH and RETURN
- `m4_where/` - WHERE clause filtering
- `m5_single_hop/` - Single relationship traversal
- `m6_multi_hop/` - Variable-length paths
- `m7_mutation/` - SET and DELETE operations
- `m8_aggregation/` - Aggregate functions
- `m9_functions/` - Built-in functions (type, id, labels, etc.)
- `m10_limit_skip/` - LIMIT and SKIP clauses

See `tests/fixtures/README.md` for the TOML format specification.

## Benchmarking

### CRUD Operations

Benchmark basic database operations (INSERT, COUNT, MATCH, DELETE):

```bash
cargo run --release --example bench_crud
```

Example output:
```
COUNT queries (after inserting N nodes):
   nodes   setup (ms)   count (ms)
--------  ------------  ------------
   10000       324.55         1.33
   50000      1690.56         6.46

MATCH with LIMIT (first 10 of N nodes):
   nodes   setup (ms)   match (ms)
--------  ------------  ------------
   50000      2253.85         1.02
```

### Shortest Path Queries

Benchmark `shortestPath()` queries across different graph structures:

```bash
cargo run --release --example bench_shortest
```

This tests on:
- **Linear chains**: Simple A→B→C→...→N paths
- **Grids**: NxN grids with diagonal shortcuts
- **Binary trees**: Trees of varying depths

### Stress Testing

Run stress tests with synthetic graph topologies designed to expose bottlenecks:

```bash
cargo run --release --example bench_stress -- --help
```

**Topologies:**
- `dense_cluster` - Near-clique (10% relationship density), tests BFS explosion
- `long_chain` - Linear path with shortcuts, tests deep traversals
- `wide_fanout` - Tree with branching=100, tests high-degree expansion
- `power_law` - Barabási-Albert scale-free, tests skewed degree distribution

**Query workloads:**
- Baseline: point lookup, single-hop, bounded variable-length, COUNT
- Killer queries: full scan filter, deep shortest path, unbounded traversal, multi-path BFS, high fan-out

Examples:
```bash
# Quick baseline test
cargo run --release --example bench_stress -- --baseline-only --scales 1000,5000

# Full stress test (use with caution - can exhaust memory)
cargo run --release --example bench_stress -- --topology all --scales 1000,10000

# Compare against FalkorDB and Neo4j (requires Docker)
cargo run --release --example bench_stress -- --compare
```

Results are written to `stress_results.json` for analysis.

## Profiling

Generate flamegraph SVGs to identify performance bottlenecks.

### CRUD Operations

```bash
cargo run --release --example profile_crud -- --op count --nodes 10000
```

Operations: `insert`, `count`, `match`, `match-filtered`, `match-limit`, `delete`, `mixed`

Options:
- `--op OP` - Operation to profile (default: count)
- `--nodes N` - Number of nodes (default: 10000)
- `--iterations N` - Number of iterations (default: 100)
- `--output FILE` - Output file (default: flamegraph.svg)

Examples:
```bash
# Profile COUNT on 50k nodes
cargo run --release --example profile_crud -- --op count --nodes 50000

# Profile MATCH with LIMIT
cargo run --release --example profile_crud -- --op match-limit --nodes 50000

# Profile mixed workload (insert, count, match, delete cycle)
cargo run --release --example profile_crud -- --op mixed --iterations 20
```

### Shortest Path Queries

```bash
cargo run --release --example profile_shortest -- --grid 20 --iterations 100
```

Options:
- `--grid N` - Profile NxN grid graph (default)
- `--chain N` - Profile linear chain of N nodes
- `--tree D` - Profile binary tree of depth D
- `--iterations N` - Number of query iterations (default: 100)
- `--output FILE` - Output file (default: flamegraph.svg)

## License

MIT
