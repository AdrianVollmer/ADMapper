# Query Planner Implementation Plan

## Overview

Replace ad-hoc optimization logic scattered throughout the executor with a proper query planner that produces execution plans. This aligns with **M9: Query Optimization** from the roadmap.

## Current State

### Ad-hoc Optimizations in Executor

The executor currently contains optimization logic that should live in a planner:

1. **`try_optimized_count()`** (mod.rs:217-280) - Detects `MATCH (n:Label) RETURN count(n)` and pushes to SQL
2. **`get_pushable_limit()`** (mod.rs:282-322) - Detects when LIMIT can be pushed to SQL
3. **`extract_path_constraints()`** (mod.rs:103-182) - Extracts WHERE predicates for shortest path optimization
4. **Pattern type detection** - `is_single_node_pattern()`, `is_shortest_path_pattern()`, etc.

### Existing Planner Stub

`planner.rs` defines a well-designed operator hierarchy that's currently unused:

```rust
pub enum PlanOperator {
    Empty,
    NodeScan { variable: String, label: Option<String> },
    Filter { input: Box<PlanOperator>, predicate: FilterPredicate },
    Expand { input: Box<PlanOperator>, ... },
    Projection { input: Box<PlanOperator>, columns: Vec<ProjectionColumn> },
    Limit { input: Box<PlanOperator>, count: u64 },
    Aggregate { input: Box<PlanOperator>, ... },
    // etc.
}
```

## Implementation Plan

### Phase 1: Basic Plan Generation

**Goal**: Make `plan()` produce actual plans for simple queries.

#### 1.1 Single-Node Patterns
```cypher
MATCH (n:Person) RETURN n.name
```
Produces:
```
Projection { columns: [n.name] }
  └─ NodeScan { variable: "n", label: Some("Person") }
```

#### 1.2 Single-Node with WHERE
```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n
```
Produces:
```
Projection { columns: [n] }
  └─ Filter { predicate: n.age > 30 }
      └─ NodeScan { variable: "n", label: Some("Person") }
```

#### 1.3 Single-Hop Patterns
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b
```
Produces:
```
Projection { columns: [a, b] }
  └─ Expand { from: "a", rel: "r", to: "b", direction: Outgoing, types: ["KNOWS"] }
      └─ NodeScan { variable: "a", label: Some("Person") }
```

**Tasks**:
- [ ] Implement `plan_match()` for MatchClause → PlanOperator
- [ ] Implement `plan_pattern()` for Pattern → PlanOperator (scan + expands)
- [ ] Implement `plan_where()` to wrap with Filter
- [ ] Implement `plan_return()` to wrap with Projection
- [ ] Add unit tests for plan generation

### Phase 2: Plan Executor

**Goal**: Execute plans instead of re-analyzing AST.

#### 2.1 Create Plan Interpreter

```rust
// In executor/plan_exec.rs
pub fn execute_plan(plan: &PlanOperator, storage: &SqliteStorage) -> Result<Vec<Binding>> {
    match plan {
        PlanOperator::NodeScan { variable, label } => execute_node_scan(variable, label, storage),
        PlanOperator::Filter { input, predicate } => {
            let bindings = execute_plan(input, storage)?;
            filter_bindings(bindings, predicate)
        }
        PlanOperator::Expand { input, ... } => {
            let bindings = execute_plan(input, storage)?;
            expand_bindings(bindings, ...)
        }
        // etc.
    }
}
```

#### 2.2 Refactor Main Execute Function

```rust
pub fn execute(statement: &Statement, storage: &SqliteStorage) -> Result<QueryResult> {
    let plan = planner::plan(statement)?;
    let optimized_plan = planner::optimize(plan)?;
    executor::execute_plan(&optimized_plan, storage)
}
```

**Tasks**:
- [ ] Create `executor/plan_exec.rs` module
- [ ] Implement `execute_node_scan()`
- [ ] Implement `execute_filter()`
- [ ] Implement `execute_expand()` (reuse existing edge traversal logic)
- [ ] Implement `execute_projection()`
- [ ] Implement `execute_limit()`
- [ ] Wire up `execute()` to use planner
- [ ] Ensure all existing tests pass

### Phase 3: Optimization Passes

**Goal**: Move ad-hoc optimizations into structured planner passes.

#### 3.1 COUNT Pushdown

Transform:
```
Aggregate { func: COUNT, input: ... }
  └─ NodeScan { label: Some("Person") }
```
Into:
```
SqlCountPushdown { label: "Person" }
```

#### 3.2 LIMIT Pushdown

Transform:
```
Limit { count: 10 }
  └─ NodeScan { label: Some("Person") }
```
Into:
```
NodeScan { label: Some("Person"), limit: Some(10) }
```

#### 3.3 Predicate Pushdown

Transform:
```
Filter { predicate: n.name = 'Alice' }
  └─ NodeScan { variable: "n" }
```
Into:
```
NodeScan { variable: "n", property_filter: Some(("name", "Alice")) }
```

**Tasks**:
- [ ] Create `planner/optimize.rs` module
- [ ] Implement `optimize()` that runs passes in sequence
- [ ] Implement `pushdown_count()` pass
- [ ] Implement `pushdown_limit()` pass
- [ ] Implement `pushdown_predicates()` pass
- [ ] Remove ad-hoc optimization code from executor

### Phase 4: Advanced Patterns

**Goal**: Support multi-hop and variable-length patterns in planner.

#### 4.1 Multi-Hop Patterns
```cypher
MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, c
```
Produces:
```
Projection
  └─ Expand { to: "c" }
      └─ Expand { to: "b" }
          └─ NodeScan { variable: "a" }
```

#### 4.2 Variable-Length Patterns
```cypher
MATCH (a)-[*1..3]->(b) RETURN a, b
```
Produces:
```
Projection
  └─ VariableLengthExpand { min: 1, max: 3 }
      └─ NodeScan { variable: "a" }
```

#### 4.3 Shortest Path
```cypher
MATCH p = SHORTEST 1 (a)-[:KNOWS]-+(b) RETURN p
```
Produces:
```
ShortestPath { k: 1, algorithm: BFS }
  └─ NodeScan { variable: "a" }
```

**Tasks**:
- [ ] Add `VariableLengthExpand` operator
- [ ] Add `ShortestPath` operator
- [ ] Implement planning for these patterns
- [ ] Implement execution for these operators

### Phase 5: Cost-Based Optimization (Future)

**Goal**: Choose optimal join ordering based on statistics.

- [ ] Collect cardinality statistics (node counts per label, edge counts per type)
- [ ] Estimate selectivity of predicates
- [ ] Implement join reordering based on estimated costs
- [ ] Add index selection logic

## File Structure After Implementation

```
src/query/
├── mod.rs
├── parser.rs
├── planner/
│   ├── mod.rs          # plan() entry point, PlanOperator enum
│   ├── build.rs        # AST → unoptimized plan
│   └── optimize.rs     # optimization passes
└── executor/
    ├── mod.rs          # execute() entry point
    ├── plan_exec.rs    # plan interpreter
    ├── eval.rs         # expression evaluation (unchanged)
    ├── aggregate.rs    # aggregate functions (unchanged)
    └── ...
```

## Migration Strategy

1. **Phase 1-2**: Implement planner and plan executor in parallel with existing code
2. **Feature flag**: Add `USE_PLANNER` flag to switch between old and new paths
3. **Validation**: Run all tests with both paths, compare results
4. **Cutover**: Remove old ad-hoc code once planner is validated
5. **Cleanup**: Remove feature flag, delete dead code

## Success Criteria

- [ ] All 100+ existing tests pass with planner-based execution
- [ ] No ad-hoc optimization code remains in executor
- [ ] Plan structure is inspectable for debugging (`EXPLAIN` support possible)
- [ ] Adding new optimizations requires only planner changes
- [ ] Performance is equal or better than current implementation

## Estimated Effort

| Phase | Effort |
|-------|--------|
| Phase 1: Basic Plan Generation | 2-3 hours |
| Phase 2: Plan Executor | 3-4 hours |
| Phase 3: Optimization Passes | 2-3 hours |
| Phase 4: Advanced Patterns | 2-3 hours |
| Phase 5: Cost-Based (Future) | TBD |
| **Total (Phases 1-4)** | **~1 day** |

## References

- Current planner stub: `src/crustdb/src/query/planner.rs`
- Ad-hoc optimizations: `src/crustdb/src/query/executor/mod.rs:103-322`
- M9 milestone: README.md lines 219-227
