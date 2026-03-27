# Several methods take &self but never use it

**Severity: LOW** | **Category: vibe-coding-smell**

## Problem

Several methods accept `&self` but never reference it:

- `import/bloodhound/nodes.rs:194` — `normalize_type(&self, data_type: &str)`
- `import/bloodhound/mapping.rs` — `local_group_to_edge_type(&self, ...)`
  and `ace_to_edge_type(&self, ...)`

These should be standalone functions or associated functions.

## Solution

Change to `fn normalize_type(data_type: &str) -> String` (drop `&self`).
Same for the mapping functions.
