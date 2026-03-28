# Unresolved promise in SSE Tauri fallback

## Severity: HIGH

## Problem

In `src/frontend/api/transport.ts` (lines 191-200), when using the `sseOnly`
channel path, a promise is fired-and-forgotten:

```typescript
api.get<T>(url)
  .then((initial) => { handler(initial); })
  .catch((err) => { console.warn(...); });
```

The returned unsubscribe function has no way to cancel this pending fetch.
If the subscription is unsubscribed before the fetch completes, the handler
may fire on stale/unmounted state, causing memory leaks or errors.

## Solution

Store an `AbortController` and pass its signal to the fetch. Cancel it in
the unsubscribe function:

```typescript
const controller = new AbortController();
api.get<T>(url, { signal: controller.signal })
  .then(...)
  .catch(...);
return () => { controller.abort(); };
```
