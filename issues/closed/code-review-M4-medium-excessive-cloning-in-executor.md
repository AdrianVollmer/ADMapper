# Excessive cloning in query executor hot paths

## Severity: MEDIUM

## Problem

In `src/crustdb/src/query/executor/plan_exec/expand.rs`, multiple `.clone()`
calls appear in the path expansion hot loop:

- Line ~315: `relationships: path_rels.clone()` — clones entire Vec per
  iteration
- Line ~360: `let mut new_path_rels = path_rels.clone()` — in inner loop
- Line ~64: `nodes: vec![source_node.clone(), target_node]`

For large graphs with long paths, this creates significant memory pressure
and GC overhead.

## Solution

Use `Rc<Node>` / `Rc<Relationship>` for shared ownership in path expansion,
or restructure to pass references where possible. The path vectors can use
`Rc<Vec<Relationship>>` with copy-on-write semantics to avoid cloning until
mutation is actually needed.
