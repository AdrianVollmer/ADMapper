# Global mutable state scattered across frontend components

## Severity: CRITICAL

## Problem

40+ module-level `let` variables are scattered across frontend components
with no encapsulation:

- `insights.ts` (lines 115-126): 12+ mutable declarations
- `manage-queries.ts` (lines 74-97): 9 mutable state variables
- `add-node-edge.ts` (lines 49-55): 4+ modal/state variables
- `db-connect.ts` (lines 19-22, 387-388): 4 global state variables
- `run-query.ts` (lines 23-53): 12 mutable state variables

State is not reset on modal close/re-open. No type safety for state
transitions. Testing is impossible without global state pollution. Multiple
components mutate shared state without coordination.

## Solution

Encapsulate state per component in a state object with clear
initialization/reset methods:

```typescript
interface InsightsState {
  chokePointsPage: number;
  activeTab: TabId;
  daState: TabState<DAAnalysisData>;
  // ...
}

function createInitialState(): InsightsState { ... }

let state = createInitialState();

function resetState(): void {
  state = createInitialState();
}
```

Call `resetState()` on modal close. This makes state lifecycle explicit and
testable.
