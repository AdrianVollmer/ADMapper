# escapeHtml implemented 3 times

**Severity: MEDIUM** | **Category: duplicate-code**

## Problem

Three separate implementations of `escapeHtml`:

- `utils/html.ts:17` — canonical, string-replacement approach (widely imported)
- `utils/notifications.ts:138` — private, DOM-based (`document.createElement`)
- `components/graph-view.ts:220` — private, DOM-based (identical to notifications)

The notifications and graph-view files define their own private copies instead
of importing from `utils/html.ts`.

## Solution

Delete the private copies in `notifications.ts` and `graph-view.ts`, import
from `utils/html.ts` instead.
