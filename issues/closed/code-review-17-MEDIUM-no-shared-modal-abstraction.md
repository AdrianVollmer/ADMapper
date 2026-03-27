# No shared modal abstraction — same pattern copy-pasted ~15 times

**Severity: MEDIUM** | **Category: bad-abstraction**

## Problem

Every modal-bearing component repeats the same pattern:

1. Create a `div.modal-overlay`
2. Set innerHTML with `modal-content` / `modal-header` / `modal-body` / `modal-footer`
3. `addEventListener` for clicks (close on overlay, action buttons)
4. `addEventListener` for Escape key
5. `document.body.appendChild`

This appears in: `add-node-edge.ts` (3 modals), `db-connect.ts`,
`db-manager.ts`, `edit-tiers.ts`, `generate-data.ts`, `insights.ts`,
`keyboard.ts`, `licenses.ts`, `list-view.ts`, `manage-queries.ts`,
`query-history.ts`, `run-query.ts`, `settings.ts`.

## Solution

Create a `createModal(options)` utility that handles the boilerplate:
overlay creation, header/footer rendering, Escape handling, backdrop click,
expand/collapse. Each component only provides the body content and action
handlers.
