# No database connected message persists after connecting

## Problem

When connecting to a database from a previously disconnected state, the
"No database connected" message remained visible in the graph area even
though the connection was successful.

## Root cause

The `connectToDatabase()` and `connectToUrl()` functions in
`db-connect.ts` updated the connection status indicator in the header
via `updateConnectionStatus()`, but did not call
`updateGraphForConnectionState()` to update the graph area placeholder.

The function `updateGraphForConnectionState()` was only being called:
- On initial page load via `refreshConnectionStatus()`
- On disconnect (via `showNoConnectionPlaceholder()`)

## Fix

Added `updateGraphForConnectionState(result.connected)` call after
successful connection in both `connectToDatabase()` and `connectToUrl()`
functions in `src/frontend/components/db-connect.ts`.
