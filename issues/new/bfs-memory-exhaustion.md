# BFS Memory Exhaustion in shortestPath and Variable-Length Expand

## Problem

Running `shortestPath` or variable-length relationship queries (e.g.
`[*1..5]`) on moderately large graphs causes the process to consume all
available memory almost instantly, making the system unresponsive until the
OOM killer terminates it.

Staying responsive is a hard requirement — the process must never lock up
the machine regardless of graph size or query shape.

## Root Cause

Three compounding issues in
`src/crustdb/src/query/executor/plan_exec/expand.rs`:

### 1. Path vector cloning at every frontier entry

Both `execute_shortest_path()` (line 374) and
`execute_variable_length_expand()` (line 244) clone the full path history
for every neighbor explored:

```rust
let mut new_path_nodes = path_nodes.clone();   // O(depth) allocation
new_path_nodes.push(next_id);
let mut new_path_rel_ids = path_rel_ids.clone(); // O(depth) allocation
new_path_rel_ids.push(relationship.id);
queue.push_back((next_id, new_path_nodes, new_path_rel_ids));
```

With average degree `d` and max depth `k`, the queue can hold up to `d^k`
entries, each carrying vectors of size up to `k`. For a typical AD graph
(degree ~50, depth 5): `50^5 × 5 × 8 bytes ≈ 125 GB`.

### 2. No bound on BFS queue size

The `VecDeque` used as the BFS queue grows without limit. There is no
check on intermediate memory usage — only final result count is tracked.

### 3. Memory safeguard does not apply to BFS internals

`ExecutionContext.max_bindings` (in `plan_exec/mod.rs:40`) counts **final
result bindings**, not intermediate queue entries. A shortestPath query
that returns 1 result tracks 1 binding while the queue silently exhausts
memory.

Additionally, `execute_variable_length_expand()` stores full
`Relationship` objects (with properties) per queue entry rather than IDs,
making each entry even heavier.

## Reproduction

Any shortestPath or variable-length query on a graph with moderate
connectivity (degree ≥ 20) and depth ≥ 4:

```cypher
MATCH p = shortestPath((a:User)-[r*1..5]->(b:Computer))
WHERE a.name = 'alice' AND b.highvalue = true
RETURN p
```

## Implementation Plan

### Phase 1: Hard safety cap on BFS queue (stops the bleeding)

**Goal:** The process never OOMs. If a query would exceed the cap, it
returns an error instead of killing the machine.

**Files:** `src/crustdb/src/query/executor/plan_exec/expand.rs`,
`src/crustdb/src/query/executor/plan_exec/mod.rs`

1. Add a `max_frontier_entries` field to `ExecutionContext` with a
   sensible default (e.g. 2,000,000 entries). This mirrors the existing
   `max_bindings` pattern but applies to intermediate BFS state.

2. In both `execute_shortest_path()` and
   `execute_variable_length_expand()`, check `queue.len()` after each
   `push_back`. If it exceeds the limit, return an error:

   ```rust
   if queue.len() > ctx.max_frontier_entries() {
       return Err(CrustError::QueryError(
           "BFS frontier exceeded memory limit; \
            try reducing path depth or adding filters".into()
       ));
   }
   ```

3. Expose the limit through `Database` the same way `max_bindings` is
   exposed (a setter method), so callers can tune it.

4. Add tests in `tests/memory_safeguard_test.rs`:
   - Verify the cap triggers on a dense graph with deep variable-length
     expand.
   - Verify shortestPath respects the cap.
   - Verify normal queries below the cap are unaffected.

### Phase 2: Parent-pointer BFS (eliminate path cloning)

**Goal:** Reduce per-queue-entry memory from `O(depth)` vectors to a
single `(i64, i64)` tuple. This is the single biggest memory win.

**Files:** `src/crustdb/src/query/executor/plan_exec/expand.rs`

1. Replace the path-carrying queue with a parent-pointer map:

   ```rust
   // Instead of: VecDeque<(i64, Vec<i64>, Vec<i64>)>
   // Use:
   let mut parent: HashMap<i64, (i64, i64)> = HashMap::new();
   // Maps: child_node_id -> (parent_node_id, relationship_id)
   let mut queue: VecDeque<i64> = VecDeque::new();
   ```

2. When a target is found, reconstruct the path by walking the parent
   chain backward. This is `O(depth)` work done once per result, instead
   of `O(depth)` cloning done per frontier entry.

3. **For `execute_shortest_path()`**: This is straightforward since
   shortestPath only needs one path per source→target pair, and the
   visited set already prevents revisits. The parent map naturally records
   the first (shortest) path.

4. **For `execute_variable_length_expand()`**: This is trickier because
   multiple paths to the same node may all be valid results. Two options:

   a. **Tree-structured parent tracking** — assign each queue entry a
      unique ID and store `parent_map: Vec<(i64, i64, usize)>` mapping
      `(node_id, rel_id, parent_entry_index)`. Queue entries carry only
      their entry index. Path reconstruction walks the tree. This still
      saves memory because the tree is append-only (no cloning), but
      entries are smaller.

   b. **Keep path cloning for variable-length expand** but combine with
      the Phase 1 frontier cap. Focus parent-pointer optimization on
      shortestPath only, where it's clean and gives the biggest win.

   Recommend option (b) for simplicity — shortestPath gets the full
   optimization, variable-length expand gets the safety cap.

5. Update tests in `tests/path_to_highvalue_test.rs` to verify identical
   results before and after the refactor.

### Phase 3: Bidirectional BFS for shortestPath

**Goal:** Reduce frontier size from `O(d^k)` to `O(2 × d^(k/2))`. For
degree 50 and depth 5: from 312M entries to ~14K.

**Files:** `src/crustdb/src/query/executor/plan_exec/expand.rs`,
`src/crustdb/src/query/planner/match_plan.rs`

1. Implement bidirectional BFS in `execute_shortest_path()`:

   ```
   forward_queue  starts from source, explores outgoing edges
   backward_queue starts from target, explores incoming edges
   forward_visited  maps node_id -> parent pointer (from source)
   backward_visited maps node_id -> parent pointer (from target)
   ```

   Alternate expanding one level from each direction. When the frontiers
   meet (a node appears in both visited sets), reconstruct the path by
   concatenating forward path + reversed backward path.

2. **Precondition: target must be known at plan time.** Check the planner
   in `match_plan.rs` — if the target is filtered by a WHERE clause (e.g.
   `b.name = 'DC01'`), resolve the target node(s) first, then run
   bidirectional BFS. If the target is unbound (find shortest path to
   *any* matching node), fall back to unidirectional BFS.

3. Add a `plan_bidirectional_shortest_path()` variant in the planner that
   resolves target nodes early and passes them into the executor.

4. Handle relationship type filters: both directions must respect the
   same type constraints, but the backward direction traverses incoming
   edges where forward traverses outgoing (or vice versa depending on
   pattern direction).

5. Add tests:
   - Bidirectional produces same results as unidirectional on existing
     test graphs.
   - Performance test on medium graph (1000+ nodes) showing frontier
     size reduction.
   - Edge cases: source == target, no path exists, multiple shortest
     paths of same length.

### Phase 4: Iterative deepening for shortestPath

**Goal:** Avoid building deep frontiers when a shorter path exists. Find
paths at depth 2 without ever exploring depth 5.

**Files:** `src/crustdb/src/query/executor/plan_exec/expand.rs`

1. Instead of a single BFS to `max_hops`, run successive depth-limited
   BFS passes: depth 1, then depth 2, ..., up to `max_hops`.

2. Stop as soon as a path is found at the current depth. The existing
   early termination logic (expand.rs line 325) already stops within a
   depth level — this extends it across levels.

3. This trades CPU (re-exploring shallow levels) for memory (never
   building the frontier beyond current depth). For shortestPath where we
   only need the first hit, the trade-off is strongly favorable.

4. Can be combined with bidirectional BFS (Phase 3) for maximum effect:
   iterative-deepening bidirectional BFS.

5. Add a test that verifies a depth-5 query with a path at depth 2 never
   explores beyond depth 2 (check via `QueryStats` or a counter).

### Phase 5: Streaming / lazy result production

**Goal:** Allow downstream operators (LIMIT, WHERE on results) to stop
exploration early by consuming results lazily.

**Files:** `src/crustdb/src/query/executor/plan_exec/expand.rs`,
`src/crustdb/src/query/executor/plan_exec/mod.rs`

1. Refactor `execute_variable_length_expand()` to return an iterator
   (or accept a callback) instead of `Vec<Binding>`. When a downstream
   LIMIT is satisfied, the iterator is dropped and exploration stops.

2. This is a larger refactor that touches the executor pipeline. It's
   most impactful for variable-length expand (which can produce millions
   of paths), less so for shortestPath (which typically produces few
   results). Prioritize accordingly.

3. As an intermediate step, check for `ctx.max_bindings` after each
   result is produced (not just at the end), which gives early termination
   without a full streaming refactor.

## Priority and Dependencies

```
Phase 1 (safety cap)          ← Do first. Immediate fix, prevents OOM.
  ↓
Phase 2 (parent pointers)     ← Biggest memory reduction. Independent.
  ↓
Phase 3 (bidirectional BFS)   ← Biggest algorithmic win. Builds on Phase 2.
  ↓
Phase 4 (iterative deepening) ← Complements Phase 3. Can be done in parallel.
  ↓
Phase 5 (streaming)           ← Larger refactor. Do last.
```

Phases 1 and 2 together solve the immediate problem. Phases 3–4 make
shortestPath performant on large graphs. Phase 5 is a broader
architectural improvement.

## Related

- `issues/new/shortest-path-exponential-enumeration.md` — covers the
  k-shortest-paths (k>1) enumeration problem, which is related but
  distinct. The parent-pointer and bidirectional BFS work here would
  benefit that case too.
