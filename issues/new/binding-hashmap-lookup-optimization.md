# Binding HashMap Lookup Optimization

## Problem

During expression evaluation, variable lookups in bindings use HashMap access repeatedly for the same variables. For expressions evaluated across many rows, this adds overhead.

## Current Behavior

In `executor/eval.rs`:

```rust
pub fn evaluate_expression_with_bindings(
    expr: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    match expr {
        Expression::Variable(name) => {
            // HashMap lookup every time
            if let Some(node) = binding.nodes.get(name) {
                Ok(PropertyValue::from(node))
            } else if let Some(edge) = binding.edges.get(name) {
                Ok(PropertyValue::from(edge))
            } else {
                Err(Error::Cypher(format!("Unknown variable: {}", name)))
            }
        }
        Expression::Property { base, property } => {
            // Recursive call, potentially more lookups
            let base_val = evaluate_expression_with_bindings(base, binding)?;
            // ...
        }
        // ...
    }
}
```

The `Binding` struct contains:
```rust
pub struct Binding {
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Edge>,
    pub paths: HashMap<String, Path>,
}
```

For an expression like `n.name = m.name`, we do 4 HashMap lookups per row:
1. Look up `n` in nodes
2. Look up `m` in nodes
3. (repeated if expression is re-evaluated)

## Proposed Solutions

### Option A: Variable Index Resolution

Pre-resolve variable names to indices during expression compilation:

```rust
struct BindingSchema {
    node_vars: Vec<String>,      // ["n", "m", ...]
    edge_vars: Vec<String>,      // ["r", ...]
    path_vars: Vec<String>,      // ["p", ...]
}

struct IndexedBinding {
    nodes: Vec<Node>,            // Indexed by position
    edges: Vec<Edge>,
    paths: Vec<Path>,
}

// In compiled expression:
enum Instruction {
    LoadNodeVar(usize),          // Index into nodes vec
    LoadEdgeVar(usize),          // Index into edges vec
    LoadPathVar(usize),          // Index into paths vec
    // ...
}
```

**Benefit:** O(1) vector index vs O(1) average HashMap (but with hashing overhead)

### Option B: Interned Variable Names

Use string interning to speed up HashMap lookups:

```rust
use string_interner::{StringInterner, Symbol};

struct Binding {
    nodes: HashMap<Symbol, Node>,
    edges: HashMap<Symbol, Edge>,
    paths: HashMap<Symbol, Path>,
}
```

**Benefit:** Smaller keys, faster hashing and comparison

### Option C: SmallVec for Small Bindings

Most queries have few variables (1-5). Use SmallVec with linear search:

```rust
use smallvec::SmallVec;

struct Binding {
    nodes: SmallVec<[(String, Node); 4]>,
    edges: SmallVec<[(String, Edge); 2]>,
    paths: SmallVec<[(String, Path); 1]>,
}

impl Binding {
    fn get_node(&self, name: &str) -> Option<&Node> {
        self.nodes.iter()
            .find(|(n, _)| n == name)
            .map(|(_, node)| node)
    }
}
```

**Benefit:** Cache-friendly, no hashing overhead for small N

### Option D: Flatten to Single Lookup

Combine nodes/edges/paths into single map:

```rust
enum BoundValue {
    Node(Node),
    Edge(Edge),
    Path(Path),
}

struct Binding {
    values: HashMap<String, BoundValue>,
}
```

**Benefit:** Single lookup instead of checking 3 maps

## Recommendation

**Option C (SmallVec)** is the best tradeoff:
- Simple implementation
- Handles the common case (few variables) efficiently
- Falls back gracefully for unusual queries with many variables
- No additional dependencies beyond smallvec (likely already in use)

Combined with **Option A** for hot paths (pre-resolve to indices during expression compilation).

## Benchmarking

Before implementing, benchmark current overhead:

```rust
#[bench]
fn bench_binding_lookup(b: &mut Bencher) {
    let binding = create_test_binding();
    b.iter(|| {
        binding.nodes.get("n")
    });
}
```

Compare HashMap vs SmallVec vs Vec with index.

## Implementation Steps

1. Benchmark current HashMap lookup overhead
2. If significant, implement SmallVec variant
3. For expression compilation (separate issue), use index resolution
4. Benchmark again to verify improvement

## Complexity

Low - SmallVec swap is straightforward. Index resolution ties into expression compilation.

## See Also

- `src/crustdb/src/query/executor/mod.rs` - `Binding` struct definition
- `issues/new/expression-evaluation-caching.md` - Related optimization
- `issues/new/code-review-crustdb.md` - Original identification
