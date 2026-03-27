# normalize_node_type duplicated between CrustDB and importer

**Severity: LOW** | **Category: duplicate-code**

## Problem

Identical match logic mapping BloodHound type names to canonical labels
exists in two places:

- `db/crustdb/mod.rs:24-41` (`normalize_node_type`)
- `import/bloodhound/nodes.rs:194-211` (`normalize_type`)

Both do the same case-insensitive mapping (e.g., "computer" → "Computer",
"group" → "Group").

## Solution

Move to a shared function in a common module (e.g., `db/types.rs` or
a new `utils` module) and call it from both locations.
