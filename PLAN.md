# Tiering Model: Assigned Tier + Effective Tier + Enhanced Classification

## Overview

Introduce a proper two-tier concept: **assigned tier** (what the admin declares) and
**effective tier** (what the node can actually reach). A tier violation is
`effective_tier < assigned_tier`. Enhance the Edit Tiers modal with group-membership
and OU-containment filters. Add a new "Tiering" guide page to the docs.

---

## Phase 1: Enhanced Tier Assignment (Edit Tiers modal)

### 1a. Backend: Expand `BatchSetTierRequest` with new filter modes

**File: `src/backend/src/api/types.rs`**

Add new optional fields to `BatchSetTierRequest`:

```rust
pub struct BatchSetTierRequest {
    pub tier: i64,
    pub node_type: Option<String>,     // existing
    pub name_regex: Option<String>,    // existing
    pub group_id: Option<String>,      // NEW: assign to all (transitive) members of this group
    pub ou_id: Option<String>,         // NEW: assign to all objects contained in this OU (recursive)
    pub node_ids: Option<Vec<String>>, // NEW: assign to explicit list of node IDs (for "tag visible nodes")
}
```

### 1b. Backend: Implement group/OU expansion in `batch_set_tier` handler

**File: `src/backend/src/api/handlers.rs`**

In `batch_set_tier()`, after collecting `matching_ids` from name/type filters, add:

- **`group_id`**: Run a reverse BFS from the group node, following only `MemberOf`
  edges backwards (i.e., find all nodes `n` where `n -[MemberOf*]-> group`). Collect
  their IDs. Intersect with or add to the existing filter.
- **`ou_id`**: Run a reverse BFS from the OU node, following only `Contains` edges
  backwards. Collect their IDs.
- **`node_ids`**: Direct list of IDs — just union them in.

For group and OU expansion, add a helper function:

```rust
fn expand_transitive(
    nodes: &[DbNode],
    edges: &[DbEdge],
    root_id: &str,
    edge_type: &str, // "MemberOf" or "Contains"
) -> HashSet<String>
```

This does a reverse BFS from `root_id`, following edges of `edge_type` in reverse
(target → source), returning all reached node IDs.

### 1c. Frontend: Add group/OU/visible-nodes filters to Edit Tiers modal

**File: `src/frontend/components/edit-tiers.ts`**

Add to the filter section:
- **Group membership dropdown**: A search-enabled input where the user types a group
  name. On selection, calls the batch-set-tier API with `group_id`.
- **OU containment dropdown**: Same pattern, user types OU name. Calls with `ou_id`.
- **"Tag visible nodes" button**: Collects all currently visible node IDs from the
  graph renderer and sends them as `node_ids`.

For the group/OU inputs, use the existing `/api/graph/search` endpoint with a
type filter to populate suggestions as the user types.

### 1d. Frontend: Update Edit Tiers node listing to also filter by group/OU

The node listing table in the modal should also reflect group/OU filters, so the
user sees which nodes will be affected before clicking "Assign". Add corresponding
parameters to the frontend's filter state and pass them when fetching/filtering nodes.

---

## Phase 2: Effective Tier Computation

### 2a. Backend: Add `compute_effective_tiers` function

**File: `src/backend/src/api/handlers.rs`** (or a new `src/backend/src/core/tiers.rs`)

Implement multi-source reverse BFS as discussed:

```rust
pub fn compute_effective_tiers(
    nodes: &[DbNode],
    edges: &[DbEdge],
) -> HashMap<String, i64>
```

Algorithm:
1. Build reverse adjacency list from edges
2. Initialize `effective_tier` map: every node starts at tier 3
3. For tier in [0, 1, 2]:
   - Collect seed nodes: all nodes with `assigned_tier == tier`
   - Reverse BFS from seeds
   - For each reached node, set `effective_tier = min(current, tier)`
4. Return the map

This is O(V + E), single pass per tier level = O(3(V + E)) = O(V + E).

### 2b. Backend: Store effective tier as a node property

After computation, store `effective_tier` as a property on each node (alongside
the existing `tier` property in `properties`). Use the same `run_custom_query`
batch-update pattern as `batch_set_tier`.

### 2c. Backend: New API endpoint `POST /api/graph/compute-effective-tiers`

**File: `src/backend/src/api/handlers.rs`**

- Loads all nodes and edges
- Calls `compute_effective_tiers()`
- Stores results back to DB
- Returns summary: `{ computed: usize, violations: usize }` where violations =
  count of nodes where `effective_tier < tier`

### 2d. Backend: Update `tier_violations` endpoint

**File: `src/backend/src/api/handlers.rs`**

Refactor the existing `tier_violations()` handler to use the stored `effective_tier`
field instead of recomputing zones on every request. The violation is now simply:
nodes where `effective_tier < tier`. This makes the endpoint O(N) instead of
O(V + E).

If `effective_tier` is not yet computed (field missing), fall back to the current
BFS approach or return an error suggesting the user run the computation first.

### 2e. Frontend: Add "Compute Effective Tiers" button

**File: `src/frontend/components/insights.ts`**

In the Tier Violations tab, add a button "Analyze Tier Violations" that:
1. Calls `POST /api/graph/compute-effective-tiers`
2. Shows a spinner during computation
3. On completion, reloads the tier violations data
4. Shows summary: "Computed effective tiers for N nodes. Found M violations."

### 2f. Frontend: Show effective tier in node detail sidebar

When a node is selected, show both `tier` (assigned) and `effective_tier` (computed)
in the detail sidebar's properties, if the effective tier has been computed.

---

## Phase 3: Documentation

### 3a. New guide page: `docs/admapper/docs/guide/tiering.md`

Contents:

1. **Tiering Model Overview**
   - What is the Enterprise Access Model / tiering
   - Tier 0 = identity infrastructure (DCs, AD admins, PKI)
   - Tier 1 = servers and enterprise apps
   - Tier 2 = workstations and standard users
   - Tier 3 = unclassified (default)

2. **Assigning Tiers**
   - Recommended workflow: set all to Tier 3 (default), then work upward
   - Using name regex filters
   - Using group membership (e.g., "all members of Domain Admins → Tier 0")
   - Using OU containment (e.g., "all objects in Tier 0 Servers OU → Tier 0")
   - Using visible graph selection
   - Typical Tier 0 objects: Domain Admins, Enterprise Admins, DCs, KRBTGT,
     AdminSDHolder, PKI servers, AD CS templates

3. **Analyzing Tier Violations**
   - What is an effective tier
   - How the algorithm works (reverse BFS, O(V+E))
   - Reading the results: assigned vs. effective tier
   - What a violation means and how to remediate

### 3b. Update existing docs

- **`guide/insights.md`**: Add a Tier Violations section explaining the concept
  and linking to the new tiering guide
- **`api/graph.md`**: Document the new `POST /api/graph/compute-effective-tiers`
  endpoint and the updated `POST /api/graph/batch-set-tier` request format
- **`mkdocs.yml`**: Add the new tiering guide to the nav

---

## File Change Summary

| File | Changes |
|------|---------|
| `src/backend/src/api/types.rs` | Add fields to `BatchSetTierRequest`, add `ComputeEffectiveTiersResponse` |
| `src/backend/src/api/handlers.rs` | Expand `batch_set_tier`, add `compute_effective_tiers` endpoint, refactor `tier_violations` |
| `src/backend/src/lib.rs` | Register new route |
| `src/frontend/components/edit-tiers.ts` | Add group/OU/visible-nodes filter UI |
| `src/frontend/components/insights.ts` | Add "Analyze" button, show effective tier |
| `src/frontend/api/client.ts` | Add new endpoint mapping |
| `docs/admapper/docs/guide/tiering.md` | New tiering guide |
| `docs/admapper/docs/guide/insights.md` | Update with tier violation reference |
| `docs/admapper/docs/api/graph.md` | Document new/updated endpoints |
| `docs/admapper/mkdocs.yml` | Add tiering guide to nav |
