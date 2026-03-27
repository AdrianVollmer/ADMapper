# Duplicate property rendering + timestamp formatting in sidebars.ts

**Severity: MEDIUM** | **Category: duplicate-code**

## Problem

Several pieces of logic are duplicated within `sidebars.ts`:

1. **Property sorting + HTML generation** (lines 912-949 vs 1317-1345):
   `updateDetailPanel()` and `fetchNodeProperties()` both sort by
   `PROPERTY_PRIORITY`, iterate entries, and generate identical
   `detail-prop` HTML with `escapeHtml(getPrettyLabel(key))`.

2. **Placeholder banner HTML** (lines 955-970 vs 1300-1314): The same
   placeholder warning SVG + text is duplicated verbatim.

3. **Timestamp formatting** (lines 756-834): `formatValue()` and
   `formatTimestamp()` both contain overlapping Windows FILETIME / Unix
   timestamp / JS millisecond conversion logic. The heuristic ranges
   differ slightly between the two (`value > 1e17 && value < 3e17` vs
   `value > 100000000000000000`), meaning the same number could be
   interpreted differently depending on which path is taken.

## Solution

1. Extract `renderPropertyList(entries)` helper for the property HTML.
2. Extract `renderPlaceholderBanner()` helper.
3. Have `formatValue` delegate to `formatTimestamp` for numeric timestamp
   fields, removing the inline heuristics from `formatValue`.
