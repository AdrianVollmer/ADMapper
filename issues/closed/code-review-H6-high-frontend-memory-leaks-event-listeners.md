# Memory leaks from unmanaged event listeners

## Severity: HIGH

## Problem

Event listeners are added without corresponding cleanup:

1. **sidebars.ts:1189** — `pathIndicator.addEventListener("click", ...)`
   added on every detail panel update. Listeners accumulate on the same
   element, each capturing `nodeId` in a closure.

2. **run-query.ts:56-60** — `document.addEventListener("keydown",
   handleKeydown)` added at init, never removed across modal open/close
   cycles.

3. **add-node-edge.ts:166-170** — Similar pattern with global keyboard
   listeners.

Over time, this causes memory bloat and potentially duplicate handler
invocations.

## Solution

- Use event delegation on stable parent elements instead of per-element
  listeners.
- For modal keyboard handlers, add on open and remove on close.
- For the path indicator, either replace the element (clearing listeners) or
  use `{ once: true }` / track and remove previous listeners.
