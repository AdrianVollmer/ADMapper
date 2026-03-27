# ApiClient methods have duplicated error-handling boilerplate

**Severity: LOW** | **Category: duplicate-code**

## Problem

In `api/client.ts:268-384`, `get`, `post`, `put`, `postNoContent`, and
`delete` all repeat the same 3-line error handling block:

```typescript
if (!response.ok) {
  const text = await response.text().catch(() => "");
  throw new ApiClientError(
    response.status,
    text || response.statusText || `HTTP ${response.status}`
  );
}
```

This appears 5 times.

## Solution

Extract a private `assertOk(response: Response)` method and call it from
each HTTP method.
