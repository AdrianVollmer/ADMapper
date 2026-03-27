# Various dead exports and unused code (LOW severity)

**Severity: LOW** | **Category: dead-code**

## Problem

Several exported symbols are never imported by any consumer:

### Frontend
- `NODE_SIZES` (`graph/theme.ts:78`, re-exported from `index.ts:79`) — never
  imported; actual node size uses `NODE_SIZE = 12` in `icons.ts`
- `normalizePositions` (`graph/layout.ts:855`, re-exported from `index.ts:60`)
  — never called; each layout handles normalization internally
- `getModeName` (`graph/label-visibility.ts:54`, re-exported from `index.ts:105`)
  — never imported; `getLabelVisibilityName` is used instead
- `QueryResponse` type (`api/types.ts:112-116`) — comment says "legacy"
- `api/validation.ts` (all 163 lines) — only used in test files, not production
- `BaseNodeImageProgram` alias (`ADGraphRenderer.ts:292`) — pointless rename
- `isTauri` alias (`api/client.ts:45`) — `isRunningInTauri` used directly elsewhere
- `halfThickness` in TaperedEdgeProgram fragment shader — computed but never used

### Backend
- `is_member_of` on `DatabaseBackend` trait (`db/backend.rs:216-253`) — never
  overridden, never called
- `extract_count` on `CrustDatabase` (`crustdb/mod.rs:136-149`) — marked
  `#[allow(dead_code)]`, never called
- `WELL_KNOWN_PRINCIPALS` / `DOMAIN_ADMIN_SID_SUFFIX` (`db/types.rs:71-81`)
  — marked `#[allow(dead_code)]`, only used behind feature gates
- `QueryLanguage` enum (`db/backend.rs:14-17`) — has only one variant (`Cypher`)
- `#[allow(unused_variables)]` on `properties` in `tauri_commands.rs:307` — the
  variable IS used; the allow is dead
- `_k` parameter in `execute_shortest_path` (`plan_exec/expand.rs:396`) — unused
- `_entity` parameter in `emit_wellknown_memberof` (`import/bloodhound/edges.rs:409`)

### Empty init functions
- `initAddNodeEdge()`, `initInsights()`, `initGenerateData()`, `initDbManager()`,
  `initSettings()` — all empty bodies with "created on demand" comments

## Solution

Delete unused exports and their re-exports. Remove dead `#[allow]` attributes.
Remove unused parameters (or use them if they should be used). Delete empty
init functions or add actual initialization logic.
