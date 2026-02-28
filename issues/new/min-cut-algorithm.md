# Min-Cut Algorithm for Attack Path Analysis

## Problem

In Active Directory security analysis, a critical question is: "What's the minimum set of permissions to remove to protect high-value targets (like Domain Admins) from all attack paths?"

This maps to the **Minimum Cut** problem in graph theory.

## Background

Given:
- **Sources**: Set of potentially compromised or low-privilege nodes
- **Sinks**: High-value targets (Domain Admins, Enterprise Admins, etc.)

Find the minimum set of edges (permissions/relationships) whose removal disconnects all sources from all sinks.

## Algorithms

### Ford-Fulkerson / Edmonds-Karp

Classic max-flow min-cut algorithm:
1. Find augmenting paths from source to sink
2. Push flow along paths until no more augmenting paths exist
3. The saturated edges form the minimum cut

**Complexity:** O(VE^2) for Edmonds-Karp

### Dinic's Algorithm

More efficient for unit-capacity graphs (which AD permission graphs often are):
1. Build level graph using BFS
2. Find blocking flows using DFS
3. Repeat until no path exists

**Complexity:** O(V^2 * E), but O(E * sqrt(V)) for unit capacity

## Proposed API

### Option A: Cypher Function

```cypher
MATCH (source:User)-[*]->(target:Group {name: 'Domain Admins'})
CALL algo.minCut(source, target) YIELD edge, score
RETURN edge.type, startNode(edge).name, endNode(edge).name
ORDER BY score DESC
```

### Option B: Direct Database Method

```rust
impl Database {
    /// Find minimum cut edges between source and target node sets.
    ///
    /// Returns edges in the minimum cut, ordered by flow contribution.
    pub fn min_cut(
        &self,
        source_ids: &[i64],
        target_ids: &[i64],
        edge_types: Option<&[&str]>,  // Filter to specific relationship types
        direction: Direction,
    ) -> Result<Vec<MinCutEdge>> {
        // ...
    }
}

pub struct MinCutEdge {
    pub edge: Edge,
    pub flow: f64,  // Flow through this edge (for weighted graphs)
}
```

## Implementation Considerations

### Edge Weights

BloodHound edges could be weighted by:
- Permission severity (e.g., GenericAll > ReadLAPSPassword)
- Likelihood of exploitation
- Detection risk

Unweighted (unit capacity) is simpler and often sufficient.

### Multi-Source Multi-Sink

AD analysis often involves:
- Multiple compromised accounts (sources)
- Multiple high-value targets (sinks)

Standard approach: Add super-source connected to all sources, super-sink connected to all sinks.

### Bidirectional Edges

Some AD relationships are effectively bidirectional. Need to handle this in the flow network representation.

## Use Cases

1. **Minimal Remediation**: "Remove the fewest permissions to protect Domain Admins"
2. **Choke Point Analysis**: "Which permissions, if removed, would have the biggest impact?"
3. **Defense Prioritization**: "Which fixes give the best security ROI?"

## Related

- `issues/new/edge-betweenness.md` - Related algorithm for ranking edges by path importance
- `src/crustdb/src/query/executor/pattern.rs` - Existing BFS/shortest path implementation

## References

- Max-Flow Min-Cut Theorem: https://en.wikipedia.org/wiki/Max-flow_min-cut_theorem
- Edmonds-Karp: https://en.wikipedia.org/wiki/Edmonds%E2%80%93Karp_algorithm
- Dinic's Algorithm: https://en.wikipedia.org/wiki/Dinic%27s_algorithm
