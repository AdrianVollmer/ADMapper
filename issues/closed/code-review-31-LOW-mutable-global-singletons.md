# Scattered mutable global singletons in frontend graph modules

**Severity: LOW** | **Category: bad-abstraction**

## Problem

Several frontend modules use module-level mutable state:

- `label-visibility.ts:23`: `let currentMode: LabelVisibilityMode = "normal"`
- `magnifier.ts:35-49`: ~10 mutable module-level variables
- `collapse.ts`: two `Map` instances at module scope
- `insights.ts`: ~8 module-level state variables

This scattered mutable state makes reasoning about state changes difficult
and makes isolated testing impossible.

## Solution

This is a low-priority architectural concern. For now, document the mutable
state. If the frontend grows more complex, consider consolidating into a
single state store or passing state explicitly.
