# Expression Execution Optimization

## Overview

This issue combines two related optimizations for expression evaluation in CrustDB:
1. **Binding Lookup Optimization** - Faster variable lookups in bindings
2. **Expression Compilation** - Compile expressions to flat instruction sequences

Both address overhead in `executor/eval.rs` when evaluating expressions across many rows.

---

## Part 1: Binding Lookup Optimization

### Problem

During expression evaluation, variable lookups in bindings use HashMap access repeatedly for the same variables. For expressions evaluated across many rows, this adds overhead.

### Current Behavior

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

For an expression like `n.name = m.name`, we do 4 HashMap lookups per row.

### Proposed Solutions

#### Option A: SmallVec for Small Bindings (Recommended)

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

#### Option B: Variable Index Resolution

Pre-resolve variable names to indices during expression compilation:

```rust
struct BindingSchema {
    node_vars: Vec<String>,      // ["n", "m", ...]
    edge_vars: Vec<String>,      // ["r", ...]
}

struct IndexedBinding {
    nodes: Vec<Node>,            // Indexed by position
    edges: Vec<Edge>,
}

// In compiled expression:
enum Instruction {
    LoadNodeVar(usize),          // Index into nodes vec
    LoadEdgeVar(usize),          // Index into edges vec
}
```

**Benefit:** O(1) vector index vs O(1) average HashMap (but with hashing overhead)

#### Option C: Flatten to Single Lookup

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

---

## Part 2: Expression Compilation

### Problem

Expression evaluation re-walks the AST for every row in the result set. For complex expressions or large result sets, this adds overhead.

### Current Behavior

```rust
pub fn evaluate_expression_with_bindings(
    expr: &Expression,
    binding: &Binding,
) -> Result<PropertyValue> {
    match expr {
        Expression::Literal(lit) => Ok(literal_to_property_value(lit)),
        Expression::Variable(name) => { /* lookup */ },
        Expression::Property { base, property } => {
            let base_val = evaluate_expression_with_bindings(base, binding)?;
            // ...
        },
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression_with_bindings(left, binding)?;
            let right_val = evaluate_expression_with_bindings(right, binding)?;
            // ...
        },
        // ... recursive for all variants
    }
}
```

For each row, this:
1. Pattern matches on the expression enum
2. Recursively descends the tree
3. Allocates intermediate `PropertyValue` results

### Proposed Solution: Compile to Flat Instructions

```rust
enum Instruction {
    LoadLiteral(PropertyValue),
    LoadNodeVar(usize),          // Index into binding (ties into Part 1)
    LoadEdgeVar(usize),
    LoadProperty(String),
    BinaryOp(BinaryOperator),
    UnaryOp(UnaryOperator),
    Call(String, usize),         // function name, arg count
}

struct CompiledExpression {
    instructions: Vec<Instruction>,
}

impl CompiledExpression {
    fn compile(expr: &Expression, schema: &BindingSchema) -> Self {
        // Walk AST once, emit instructions, resolve variable indices
    }

    fn evaluate(&self, binding: &IndexedBinding) -> Result<PropertyValue> {
        let mut stack: Vec<PropertyValue> = Vec::new();
        for instr in &self.instructions {
            match instr {
                Instruction::LoadLiteral(v) => stack.push(v.clone()),
                Instruction::LoadNodeVar(idx) => stack.push(binding.nodes[*idx].into()),
                Instruction::BinaryOp(op) => {
                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    stack.push(apply_binary_op(op, left, right)?);
                },
                // ...
            }
        }
        Ok(stack.pop().unwrap())
    }
}
```

### Benefits

1. **No recursion** - flat loop over instructions
2. **No pattern matching per row** - instruction type encoded in enum variant
3. **No variable name hashing** - indices resolved at compile time
4. **Cache-friendly** - sequential memory access
5. **Reusable** - compile once, evaluate many times

### Integration Points

```rust
// In execute_match():
let schema = BindingSchema::from_pattern(&pattern);
let compiled_where = where_clause.map(|w| CompiledExpression::compile(&w.predicate, &schema));
let compiled_return: Vec<_> = return_items.iter()
    .map(|item| CompiledExpression::compile(&item.expression, &schema))
    .collect();

// For each binding:
if let Some(ref pred) = compiled_where {
    if !pred.evaluate(&binding)?.as_bool() {
        continue;
    }
}
```

---

## Implementation Plan

### Phase 1: SmallVec Binding (Low effort, immediate benefit)
1. Replace `HashMap` with `SmallVec` in `Binding` struct
2. Update all binding access to use linear search
3. Benchmark to verify improvement

### Phase 2: Expression Compilation (Medium effort)
1. Define `Instruction` enum covering all expression types
2. Define `BindingSchema` to map variable names to indices
3. Implement `compile()` that walks AST once and emits instructions
4. Implement stack-based `evaluate()`
5. Update `filter_bindings_by_where` and result building to use compiled form

### Phase 3: Full Integration
1. Create `IndexedBinding` that uses Vec instead of SmallVec
2. Resolve variable indices at compile time
3. Benchmark with varying result set sizes

## When This Matters

- Large result sets (1000+ rows)
- Complex expressions (multiple operations, function calls)
- Nested property access (`a.b.c.d`)
- Queries with WHERE clauses evaluated many times

For simple queries (`MATCH (n) RETURN n.name`), the overhead is negligible.

## Complexity

- Phase 1: Low - SmallVec swap is straightforward
- Phase 2-3: Medium-High - requires new module, careful handling of all expression types

## See Also

- `src/crustdb/src/query/executor/eval.rs` - Current implementation
- `src/crustdb/src/query/executor/mod.rs` - `Binding` struct definition
- `issues/new/query-planner-implementation.md` - Related optimization work
