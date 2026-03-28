# setTimeout(0) used for DOM readiness in modal setup

## Severity: MEDIUM

## Problem

In `src/frontend/components/add-node-edge.ts` (lines 243-279),
`setupSearchInput()` uses `setTimeout(() => { ... }, 0)` to wait for the
modal to be appended to the DOM. This doesn't guarantee the element exists
when `getElementById` runs — it's a race condition.

Similarly, the blur handler uses `setTimeout(() => { results.hidden = true },
200)` as a hack to allow click events on results to fire before hiding.

## Solution

Call `setupSearchInput()` synchronously after `document.body.appendChild(modal)`,
eliminating the need for setTimeout. For the blur/click race, use
`mousedown` instead of `click` on results (mousedown fires before blur).
