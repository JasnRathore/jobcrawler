# Crawler system design

This document describes the request scheduling, rate-limit behavior, and the
optional proxy pool of the crawler. It is a design reference, not setup
instructions — see `README.md` for usage.

## Scope and assumptions

Rate limiting on the destination job boards is applied per egress IP. This
crawler sends requests through a pool of proxies only on the user's
statement that the destination hosts authorize routing through proxies while
their public API layer is being built. If that authorization is withdrawn,
or you are not in a position to obtain it in writing, run with `--proxies
off`, which restores the pre-proxy behavior exactly.

Two invariants hold in every mode:

1. **Bounded request rate.** The global `--rps` limiter caps aggregate
   requests per second regardless of how many workers or proxies are in
   play. Adding proxies does not multiply the request rate by the pool size.
2. **Bounded retries.** Every fetch allows at most 4 attempts (same budget
   as before the pool existed) and honors context cancellation at every
   wait point.

## Architecture

```
                     ┌──────────────────────────────────────────────┐
                     │ providers.go / provider_*.go / jobs.go       │
                     │  fetch loops (attempt budget: 4)             │
                     └───────┬───────────────────────────▲──────────┘
                             │ acquireLease(ctx, pool,   │ response + status
                             │  client)                  │
                     ┌───────▼───────────────────────────┴──────────┐
                     │ requestLease                                 │
                     │   pool == nil → direct client (limiters etc.) │
                     │   otherwise   → peer.client (proxy-pinned)   │
                     └───────┬──────────────────────────────────────┘
                             │ release(outcome, retryAfter)
                     ┌───────▼──────────────────────────────────────┐
   free-proxy-list   │ proxyPool (proxy_pool.go)                    │
   JSON ─────────────►  filter alive/elite/http|socks5, dedupe,     │
                     │  sort by uptime, probe (TCP dial), evict     │
                     └──────────────────────────────────────────────┘
```

## Proxy pool

### Leasing

`acquire` grants an **exclusive lease**: a peer is only eligible when

- it is not currently in use by another worker,
- `now >= lastUsed + --proxy-reuse-min` (released proxies are not reused
  too soon), and
- `now >= cooldownUntil` (no fresh `429` cooldown).

Among eligible peers the least-recently-used wins, so traffic spreads
across the pool instead of hammering one proxy. If no peer is eligible but
some are leased, `acquire` polls every 5 ms. If no healthy peer remains at
all, `acquire` returns a "direct fallback" signal; the next request goes
out from the local address and the switch is logged at most once per 30 s
(every switch is auditable — never silently).

### Failure matrix

| Outcome on a leased request | Pool effect | Next attempt |
| --- | --- | --- |
| `2xx` | `failures = 0` | — (request done) |
| `429` | `failures++`, cooldown = `max(Retry-After, 1s floor)`, capped by `--cooldown` | rotate to a fresh peer, no backoff wait |
| `5xx` / transport error | `failures++` | rotate to a fresh peer, standard backoff |
| other status | request fails (no retry, as before) | — |
| consecutive failures ≥ `--proxy-failures` | peer evicted; idle connections closed | peer never leased again |
| pool drained to 0 healthy peers | direct fallback (logged) | direct client |

**Direct mode (`--proxies off`).** No rotation happens at all: a 429 keeps
the previous host-wide behavior — the shared limiter pauses all requests
for `--cooldown` — and the pool code is not executed.

### Startup probe

`loadProxyPool` reads the list (`free-proxy-list.json` in this repository is
a single-line JSON array of `proxy`/`protocol`/`alive`/`anonymity`/`uptime`
entries), keeps `alive` entries with `http` or `socks5` protocol and elite
anonymity, deduplicates, sorts by uptime (highest first), then **dials each
address once** with 64 concurrent probes and a `--proxy-probe-timeout`
budget (default 3 s), keeping only reachable addresses. The probe is a TCP
dial, not a full CONNECT handshake, so it validates reachability only.
Starting with a pool where already-dead proxies were not filtered is an
error.

### Wire format

Each peer gets its own `http.Transport` pinned to its proxy URL (the
`socks5` scheme is supported by `net/http` directly), so connection pooling
is per egress IP and the direct client remains untouched.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--proxies` | `off` | Path to a proxy list JSON, or `off` for the previous behavior |
| `--proxy-reuse-min` | `0` | Minimum gap before a released proxy may be leased again (on top of any cooldown) |
| `--proxy-failures` | `5` | Consecutive failures before a proxy is evicted |
| `--proxy-probe-timeout` | `3s` | Startup health-probe timeout per proxy |
| `--rps` | `50` | Global aggregate rate cap (unchanged; applies in pool mode too) |
| `--cooldown` | `2m` | Host-wide pause after 429 in direct mode; caps per-proxy cooldowns in pool mode |

## Data flow (job update)

1. `updateAllJobs` collects company slugs from the database per provider,
   skipping checkpointed companies.
2. Workers pull targets from a channel; each target runs
   `provider.FetchJobs` → `fetchProviderResponse` (or the Greenhouse
   specific loop), which performs up to 4 lease/request/report cycles.
3. Successful listings replace the company's stored jobs and save a
   crawl checkpoint, so interrupted updates resume where they stopped.

## Testing notes

`proxy_pool_test.go` covers lease exclusivity, LRU selection, reuse gaps,
`Retry-After` flooring/capping, eviction, direct fallback and context
cancellation at the state-machine level, plus list filtering/ordering and a
`429 → rotate → 200` integration test through `fetchProviderResponse`.
Existing tests keep passing with a nil pool, which pins down the
`--proxies off` behavior.
