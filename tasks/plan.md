# Implementation plan: automatic proxy gathering

## Capability map

| Capability | Responsibility | Depends on |
|---|---|---|
| `cobra-cli` | Preserve current root invocations while introducing Cobra. | — |
| `safe-manual-route` | Use only explicitly configured manual routes; honor destination limits/restrictions. | `cobra-cli` |
| `rotating-pool` | Lease every eligible supplied route exclusively with LRU rotation, evict dead routes, and stop the run (fail closed) on exhaustion. | `cobra-cli` |
| `proxy-filter` | Live-check a supplied list against a neutral endpoint (never job boards); write proven routes best-first and dead candidates marked not alive. | `rotating-pool` |
| `verification` | Update usage docs and verify CLI compatibility, route safety, and the full Go suite. | `rotating-pool` |

Build `cobra-cli` first, then integrate and verify `safe-manual-route`.

## Safety boundary

This implementation intentionally excludes automatic retrieval and use of public proxies in word-list or brute-force discovery. Those features would automate routing third-party enumeration through public egresses. The previously drafted source-gathering and auto-discovery tickets remain deferred; this plan covers only CLI compatibility and fail-closed manual routing.

## Implementation constraints

- `PRODUCT.md` owns observable behavior; `TECH.md` owns design and validation.
- `CURRENT_STATE.md` is the as-is reference; retain it as historical baseline and update README to describe the new behavior.
- Do not fan out probes to job-board endpoints. Destination signals stop provider work; rotation is for transport failures and evictions, never a way to switch egress after a destination restriction.
- Direct fallback stays opt-in and is selected only before target traffic starts; pool exhaustion stops the run instead of changing egress.
- Keep the aggregate target request budget unchanged.
- Do not add a source-waiting dispatch path.

## Integration checkpoints

1. Cobra root and existing CLI invocations remain testable.
2. Manual proxy input does not trigger destination probes or egress failover.
3. Destination rate-limit/access-control responses stop provider work.
4. Run focused tests, then `go test ./...` and `go vet ./...`.
