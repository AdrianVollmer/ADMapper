# Implement UNWIND clause in CrustDB

## Context

`UNWIND` is part of the openCypher 9 specification and is supported by Neo4j
and FalkorDB. CrustDB already has the grammar rule in `cypher.pest` but the
parser and executor do not handle it.

## Semantics

`UNWIND` takes a list expression and produces one row per element, binding
each element to a variable:

```cypher
UNWIND [1, 2, 3] AS x
RETURN x
```

yields rows: `1`, `2`, `3`.

It is commonly chained with MATCH/CREATE/SET for batch operations:

```cypher
UNWIND [{id: 'A', name: 'Alice'}, {id: 'B', name: 'Bob'}] AS row
MATCH (n) WHERE n.objectid = row.id
SET n.name = row.name
```

This is the idiomatic way to batch-update heterogeneous data in Cypher
without issuing N individual queries.

## Current state

- **Grammar**: `Unwind` rule exists in `src/crustdb/src/query/cypher.pest`
  (line 53): `UNWIND ~ SP? ~ Expression ~ SP ~ AS ~ SP ~ Variable`
- **AST**: No `Unwind` variant in `Statement` or `ReadingClause`
- **Parser**: Not handled (the `Unwind` rule is parsed by pest but silently
  ignored by the Rust parser)
- **Executor**: No execution logic

## Implementation plan

### 1. AST (ast.rs)

Add an `UnwindClause` struct and wire it into `ReadingClause` or as a
standalone clause in the query pipeline:

```rust
pub struct UnwindClause {
    pub expression: Expression,  // the list to unwind
    pub variable: String,        // the AS binding name
}
```

### 2. Parser (parser/clause.rs)

Handle `Rule::Unwind` in the clause parser. Extract the expression and the
variable name. The expression parser already handles list literals
(`[1, 2, 3]`) and map literals (`{key: value}`), so this should plug in
directly.

### 3. Planner (planner/mod.rs)

The `UNWIND` operator is logically a flat-map: for each existing binding row,
expand the list into N rows. This can be modeled as a new `PlanOp::Unwind`
that sits in the pipeline between any preceding clauses (or as the initial
source if it's the first clause).

```rust
PlanOp::Unwind {
    expression: PlannedExpression,
    variable: String,
}
```

### 4. Executor (executor/)

The executor evaluates the list expression against each input binding, then
for each element produces a new binding with the variable bound to that
element. If the element is a map, property access (`row.name`) should resolve
via the existing map-property-access expression evaluation.

Key cases to handle:
- List of scalars: `UNWIND [1, 2, 3] AS x`
- List of maps: `UNWIND [{a: 1}, {a: 2}] AS row` (then `row.a` in SET/RETURN)
- Parameter lists (future): `UNWIND $list AS x`
- Empty list: produces zero rows (prunes the pipeline)
- NULL input: produces zero rows (per openCypher spec)

### 5. Tests

Add fixture tests in `tests/fixtures/m_unwind/`:

- Basic scalar unwind
- Map unwind with property access
- Chained with MATCH + SET (the primary use case for batch updates)
- Chained with CREATE (batch node creation)
- Empty list (zero rows)
- UNWIND after MATCH (flat-map over existing bindings)
- UNWIND + WITH for aggregation after expansion

## Priority

Medium. The workaround (CASE expressions in SET, or individual queries) works
but UNWIND would enable idiomatic batch operations and improve compatibility
with queries written for Neo4j.
