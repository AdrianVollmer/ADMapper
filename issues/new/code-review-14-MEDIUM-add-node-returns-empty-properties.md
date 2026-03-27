# add_node returns empty properties, discarding what was just inserted

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

In `api/core/mutation.rs:37-43`, `add_node` creates a `DbNode` with the
caller's properties, inserts it into the DB, then returns a NEW `DbNode`
with `properties: serde_json::json!({})` — throwing away the properties:

```rust
db.insert_node(node).map_err(|e| e.to_string())?;

Ok(DbNode {
    id,
    name,
    label,
    properties: serde_json::json!({}),  // <-- why empty?
})
```

The same pattern appears in the handler version at `handlers/mutation.rs:50-55`.

## Solution

Return the originally constructed `DbNode` with its properties, or fetch the
node back from the DB after insertion to return the canonical version.
