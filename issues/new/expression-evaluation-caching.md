# Expression Evaluation Caching

## Problem

Expression evaluation in `executor/eval.rs` re-walks the AST for every row in the result set. For complex expressions or large result sets, this adds overhead.

## Current Behavior

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

## Proposed Solution

### Compile expressions to a flat instruction sequence

```rust
enum Instruction {
    LoadLiteral(PropertyValue),
    LoadVariable(String),
    LoadProperty(String),
    BinaryOp(BinaryOperator),
    UnaryOp(UnaryOperator),
    Call(String, usize), // function name, arg count
}

struct CompiledExpression {
    instructions: Vec<Instruction>,
}

impl CompiledExpression {
    fn compile(expr: &Expression) -> Self { /* ... */ }

    fn evaluate(&self, binding: &Binding) -> Result<PropertyValue> {
        let mut stack: Vec<PropertyValue> = Vec::new();
        for instr in &self.instructions {
            match instr {
                Instruction::LoadLiteral(v) => stack.push(v.clone()),
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
3. **Cache-friendly** - sequential memory access
4. **Reusable** - compile once, evaluate many times

### Integration Points

```rust
// In execute_match():
let compiled_where = where_clause.map(|w| CompiledExpression::compile(&w.predicate));
let compiled_return: Vec<_> = return_items.iter()
    .map(|item| CompiledExpression::compile(&item.expression))
    .collect();

// For each binding:
if let Some(ref pred) = compiled_where {
    if !pred.evaluate(&binding)?.as_bool() {
        continue;
    }
}
```

## When This Matters

- Large result sets (1000+ rows)
- Complex expressions (multiple operations, function calls)
- Nested property access (`a.b.c.d`)

For simple queries (`MATCH (n) RETURN n.name`), the overhead is negligible.

## Implementation Steps

1. Define `Instruction` enum covering all expression types
2. Implement `compile()` that walks AST once and emits instructions
3. Implement stack-based `evaluate()`
4. Update `filter_bindings_by_where` and result building to use compiled form
5. Benchmark with varying result set sizes

## Complexity

Medium-High - requires new module, careful handling of all expression types, thorough testing.

## Alternative: Partial Optimization

Instead of full compilation, optimize specific patterns:

1. **Property access caching**: Pre-resolve `n.prop` to a (variable_index, property_name) tuple
2. **Literal hoisting**: Evaluate constant subexpressions once
3. **Short-circuit evaluation**: Skip right side of AND/OR when possible

## See Also

- `src/crustdb/src/query/executor/eval.rs` - Current implementation
- `issues/new/query-planner-implementation.md` - Related optimization work
