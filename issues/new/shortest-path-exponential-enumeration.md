# Shortest Path Exponential Enumeration for k>1

**Criticality: Medium** | **Complexity: Medium**

## Problem

When finding k shortest paths (where k > 1), the BFS enumeration can grow exponentially. For dense graphs or large k values, this causes performance degradation or memory exhaustion.

## Current Behavior

In `executor/pattern.rs`, the shortest path implementation:

```rust
pub fn find_k_shortest_paths(
    storage: &SqliteStorage,
    source: &Node,
    target: &Node,
    k: u32,
    // ...
) -> Result<Vec<Path>> {
    // BFS that enumerates ALL paths up to shortest length + some bound
    // Then takes the k shortest
}
```

The issue: to find k shortest paths, we may need to enumerate many more than k paths, especially when:
- Multiple paths have the same length
- The graph has high connectivity
- k is large

## Example

Consider a graph where nodes A and B are connected through 100 intermediate nodes, each with direct edges to both A and B:

```
    A ←→ N1 ←→ B
    A ←→ N2 ←→ B
    A ←→ N3 ←→ B
    ...
    A ←→ N100 ←→ B
```

Finding `SHORTEST 5` paths requires considering all 100 paths of length 2.

## Proposed Solutions

### Option A: Yen's Algorithm

Yen's algorithm finds k shortest loopless paths efficiently:

1. Find the shortest path P1
2. For i = 2 to k:
   - For each node in P(i-1), create a "spur path" by:
     - Removing edges used in previous paths at that spur point
     - Finding shortest path from spur to target
   - Add best spur path as Pi

**Complexity:** O(kn(m + n log n)) vs O(exponential) for naive enumeration

### Option B: Lazy Enumeration with Priority Queue

Instead of enumerating all paths then sorting:

```rust
struct PathCandidate {
    path: Vec<i64>,
    length: usize,
}

fn find_k_shortest_lazy(k: u32) -> Vec<Path> {
    let mut heap: BinaryHeap<Reverse<PathCandidate>> = BinaryHeap::new();
    let mut results: Vec<Path> = Vec::new();

    // Seed with paths from source
    heap.push(/* initial candidates */);

    while results.len() < k && !heap.is_empty() {
        let candidate = heap.pop().unwrap();
        if is_complete_path(&candidate) {
            results.push(candidate.into_path());
        } else {
            // Extend candidate, push extensions back to heap
            for extension in extend_path(&candidate) {
                heap.push(extension);
            }
        }
    }

    results
}
```

**Benefit:** Stops as soon as k paths found, doesn't enumerate unnecessary paths.

### Option C: Bounded Enumeration with Early Termination

Add configurable limits:

```rust
/// Maximum paths to enumerate before giving up
const MAX_PATH_CANDIDATES: usize = 10_000;

/// Maximum path length multiplier (shortest * this = max length to consider)
const MAX_LENGTH_FACTOR: f64 = 1.5;
```

Return partial results with a warning if limits hit.

## Implementation Recommendation

1. **Short term (Option C):** Add limits to prevent runaway enumeration
2. **Medium term (Option B):** Implement lazy enumeration with priority queue
3. **Long term (Option A):** Implement Yen's algorithm for optimal k-shortest-paths

## Configuration

Add to query options or as pragmas:

```rust
pub struct ShortestPathOptions {
    /// Maximum candidates to enumerate (default: 10000)
    pub max_candidates: usize,

    /// Maximum path length as multiple of shortest (default: 2.0)
    pub max_length_factor: f64,

    /// Algorithm to use
    pub algorithm: ShortestPathAlgorithm,
}

pub enum ShortestPathAlgorithm {
    BreadthFirst,  // Current
    Yen,           // Optimal for k-shortest
    Lazy,          // Priority queue based
}
```

## See Also

- `src/crustdb/src/query/executor/pattern.rs` - Current implementation
- Yen's algorithm: https://en.wikipedia.org/wiki/Yen%27s_algorithm
- `issues/new/code-review-crustdb.md` - Original identification
