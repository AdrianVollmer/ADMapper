# EvalValue reinvents PropertyValue with redundant conversion layer

**Severity: MEDIUM** | **Category: bad-abstraction**

## Problem

In CrustDB, `plan_exec/eval.rs:8-18` defines `EvalValue`:

```rust
pub enum EvalValue {
    Null, Bool(bool), Int(i64), Float(f64), String(String),
    List(Vec<EvalValue>), Node(Node), Relationship(Relationship), Path(Path),
}
```

This duplicates `PropertyValue` (which has `Null, Bool, Integer, Float,
String, List, Map`) and adds graph entity variants. Then `convert.rs`
has ~68 lines of boilerplate converting between `EvalValue`,
`PropertyValue`, and `ResultValue`.

## Solution

Extend `PropertyValue` with `Node`/`Relationship`/`Path` variants (or create
a single unified `Value` enum), eliminating the separate `EvalValue` and the
conversion layer in `convert.rs`.
