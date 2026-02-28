# Graph Algorithms

CrustDB includes built-in graph algorithms for analysis tasks that operate on the entire graph structure.

## Edge Betweenness Centrality

Edge betweenness centrality measures how many shortest paths pass through each edge. Edges with high betweenness are "choke points" - removing them would disrupt many paths through the graph.

### Use Cases

- Identify critical permissions in security analysis
- Find high-impact remediation targets
- Detect structural vulnerabilities in networks
- Analyze information flow bottlenecks

### Algorithm

CrustDB uses Brandes' algorithm:

1. For each source node, perform BFS to compute shortest path distances and counts
2. Backtrack from leaves to accumulate dependency values
3. For each edge on a shortest path, add the dependency contribution

### Complexity

O(V * E) where V is the number of nodes and E is the number of edges.

### API

```rust
pub fn edge_betweenness_centrality(
    &self,
    edge_types: Option<&[&str]>,
    directed: bool,
) -> Result<EdgeBetweenness>
```

**Parameters:**

- `edge_types`: Optional filter to only consider specific edge types. Pass `None` for all edges.
- `directed`: Whether to treat edges as directed (`true`) or undirected (`false`). For permission graphs, directed is usually appropriate.

**Returns:**

```rust
pub struct EdgeBetweenness {
    pub scores: HashMap<i64, f64>,
    pub nodes_processed: usize,
    pub edges_count: usize,
}
```

### Example

```rust
let db = Database::open("graph.db")?;

// Compute betweenness for all edges (directed)
let result = db.edge_betweenness_centrality(None, true)?;

println!("Processed {} nodes, {} edges", result.nodes_processed, result.edges_count);

// Get top 10 choke points
for (edge_id, score) in result.top_k(10) {
    if let Some(edge) = db.get_edge(edge_id)? {
        println!(
            "{} -> {} via {}: betweenness = {:.2}",
            edge.source, edge.target, edge.edge_type, score
        );
    }
}
```

### Filtering by Edge Type

Analyze only specific relationship types:

```rust
// Only consider MemberOf and AdminTo edges
let result = db.edge_betweenness_centrality(
    Some(&["MemberOf", "AdminTo"]),
    true
)?;
```

### Getting Edges Above Threshold

```rust
let result = db.edge_betweenness_centrality(None, true)?;

// Get all edges with betweenness > 100
for (edge_id, score) in result.above_threshold(100.0) {
    println!("Edge {} has high betweenness: {}", edge_id, score);
}
```

### Directed vs Undirected

**Directed (recommended for permission graphs):**

Edges are treated as one-way. A->B and B->A are separate relationships.

```rust
let result = db.edge_betweenness_centrality(None, true)?;
```

**Undirected:**

Edges can be traversed in either direction. Use for symmetric relationships.

```rust
let result = db.edge_betweenness_centrality(None, false)?;
```

### Caching

Results are automatically cached. Subsequent calls with the same parameters return cached results. The cache is invalidated when graph data is modified.

```rust
// First call computes (slow)
let result1 = db.edge_betweenness_centrality(None, true)?;

// Second call uses cache (fast)
let result2 = db.edge_betweenness_centrality(None, true)?;

// Insert new data
db.execute("CREATE (n:Node {name: 'New'})")?;

// Cache invalidated, recomputes
let result3 = db.edge_betweenness_centrality(None, true)?;
```

## Shortest Path (via Cypher)

Shortest path queries are available through Cypher syntax.

### Single Shortest Path

```cypher
MATCH p = SHORTEST (src)-[:EDGE_TYPE]-+(dst)
WHERE src.id = 'start' AND dst.id = 'end'
RETURN p
```

### K Shortest Paths

```cypher
MATCH p = SHORTEST 5 (src)-[:EDGE_TYPE]-+(dst)
WHERE src.id = 'start' AND dst.id = 'end'
RETURN p
```

### Path Length

```cypher
MATCH p = SHORTEST (src)-[:EDGE_TYPE]-+(dst)
WHERE src.id = 'start' AND dst.id = 'end'
RETURN length(p) AS hops
```

### Implementation

Shortest path uses BFS with:

- Cycle avoidance per path
- Optional edge type filtering
- Configurable maximum depth (default 10,000)
- Early termination when target found

## EdgeBetweenness Methods

### `top_k`

Get the top k edges by betweenness score.

```rust
pub fn top_k(&self, k: usize) -> Vec<(i64, f64)>
```

Returns edge IDs sorted by betweenness score (highest first).

### `above_threshold`

Get edges with betweenness score above a threshold.

```rust
pub fn above_threshold(&self, threshold: f64) -> Vec<(i64, f64)>
```

### Resolving Edge IDs

Use `Database::get_edge` to get full edge details:

```rust
let result = db.edge_betweenness_centrality(None, true)?;

for (edge_id, score) in result.top_k(5) {
    if let Some(edge) = db.get_edge(edge_id)? {
        println!("Type: {}", edge.edge_type);
        println!("From node: {}", edge.source);
        println!("To node: {}", edge.target);
        println!("Properties: {:?}", edge.properties);
    }
}
```

## Performance Considerations

### Large Graphs

Edge betweenness has O(V * E) complexity. For large graphs:

- Filter by edge types to reduce the working set
- Run analysis during off-peak times
- Results are cached after first computation

### Memory Usage

The algorithm loads all nodes and edges into memory. Ensure sufficient RAM for large graphs.

### Incremental Updates

CrustDB does not support incremental betweenness updates. Any graph modification invalidates the cache and requires full recomputation.
