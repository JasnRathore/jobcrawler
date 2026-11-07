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

### Interrupting a run

Ctrl+C unwinds gracefully: cleanup runs in order (proxy-list flush,
`failed.txt` saves, checkpoints), and workers never block past it —
after cancellation a worker delivers its result only if the results
buffer has room and drops it otherwise, so the command queue finishes
fast-failing and `close()` drains whatever is buffered into
`failed.txt`. A second Ctrl+C force-quits immediately with exit 130 no
matter where shutdown is. `SIGTERM` triggers the same graceful unwind
(Windows ignores it; the force-quit path makes the guarantee portable).

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
| `429` on a direct fallback lease (pool drained) | host-wide cooldown via the shared limiter, as in direct mode | wait the cooldown |
| `5xx` / transport error | `failures++` | rotate to a fresh peer, standard backoff |
| other status | request fails (no retry, as before) | — |
| consecutive failures ≥ `--proxy-failures` | peer evicted; idle connections closed | peer never leased again |
| pool drained to 0 healthy peers | direct fallback (logged) | direct client |

**Direct mode (`--proxies off`).** No rotation happens at all: a 429 keeps
the previous host-wide behavior — the shared limiter pauses all requests
for `--cooldown` — and the pool code is not executed.

### Startup probe

`loadProxyPool` reads the list (`free-proxy-list.json` in this repository is
a JSON object whose `proxies` field holds an array of
`proxy`/`protocol`/`alive`/`anonymity`/`uptime` records; a bare array is
accepted too), keeps `alive` entries with `http` or `socks5` protocol and
elite anonymity, deduplicates, sorts by uptime (highest first), then
**validates each peer** with 64 concurrent probes and a
`--proxy-probe-timeout` budget (default 3 s). With a provider endpoint
available, the probe sends one real GET through the peer's own proxy
client: **any HTTP status — 4xx and 5xx included — proves the CONNECT
tunnel and TLS handshake work**, and only a transport failure drops the
peer. A TCP open on the proxy address without a working tunnel is the most
common free-proxy failure, so the tunnel check is what keeps the pool
usable; the probe issues one small request to the provider per candidate
peer. Starting with a pool containing zero surviving peers is an error.

### Wire format

Each peer gets its own `http.Transport` pinned to its proxy URL (the
`socks5` scheme is supported by `net/http` directly), so connection pooling
is per egress IP and the direct client remains untouched.

### Remote lists

`--proxies` also accepts an `http(s)` URL. The list is fetched once at
startup with the run's `--timeout` budget and a browser user agent
(some list hosts reject default Go user agents), capped at 16 MB, and
decoded with the same rules as local files. The Geonode proxy-list API
(`https://proxylist.geonode.com/api/proxy-list?...`) maps its `data`
records onto the shared entry schema: one entry per advertised
`http`/`socks5` protocol (`socks4` and unknown schemes are ignored,
`anonymityLevel` filters as usual), with the record's `responseTime`
(milliseconds) inverted into `uptime` so the loader's descending priority
sort starts with the fastest proxies. Every record maps to `alive: true`
and its actual usability is left to the startup tunnel probe.

### Saving good peers (`--proxy-save`)

`--proxy-save FILE` writes every peer that was still leasable (never
evicted) to `FILE` in the same schema `loadProxyPool` reads — a bare JSON
array of `proxy`/`protocol`/`alive`/`anonymity`/`uptime` objects — so a
validated list can be fed straight back via `--proxies FILE`. Each entry
also carries the observed run stats: `requests`, `successes`, `failures`
(transport, 5xx and 429 combined) and `last_used` (RFC 3339). The list is
sorted by `successes` descending, and a peer's `uptime` field holds its
success count, so re-loading the file ranks proven winners first. Writes
are atomic (temp file + rename) and serialized: a flush every 5 minutes
keeps the file current during long runs, a deferred final save runs when
the crawl returns (including Ctrl+C unwinding), and only surviving peers
are saved — a peer evicted after `--proxy-failures` consecutive failures is
excluded even if it had earlier successes.

### Collect mode (`--collect-proxies`)

Curation and crawling are separate passes. `--collect-proxies FILE`
runs database-free: it merges every source (`--proxy-sources` wins,
otherwise `--proxies`; each element is a file path or an http(s) URL),
filters through `eligibleProxyEntries` — optionally collapsing to the
best entry per host with `--collect-unique-hosts` — then probes every
candidate `--collect-iterations` times (default 2) through the selected
provider's endpoint with the shared 64-way probe concurrency. Everything
that answered at least once is written to `FILE` in the same schema as
`--proxy-save`, with `response_ms` carrying the average measured probe
latency, ordered by successes descending, then response time, then URL.
A run with zero survivors is an error; a source that fails to load is
skipped with a log when other sources remain. Crawl afterwards from the
curated file: startup only re-validates the proven peers, so it is fast,
and `--proxy-save` during crawl sweeps keeps the file current.

Collect mode never touches the database, the request limiter, or the
crawl pipeline; sources and output reuse the decode/eligibility/probe/
save machinery of the regular pool, so Geonode URLs, wrapped lists and
saved pool files all work as sources.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--proxies` | `off` | Path to a proxy list JSON, an `http(s)` URL returning one (Geonode API supported), or `off` for the previous behavior |
| `--proxy-reuse-min` | `0` | Minimum gap before a released proxy may be leased again (on top of any cooldown) |
| `--proxy-failures` | `5` | Consecutive failures before a proxy is evicted |
| `--proxy-probe-timeout` | `3s` | Startup health-probe timeout per proxy |
| `--proxy-save` | `` | Path to write surviving proxies (never evicted) with request stats, in a schema `--proxies` can re-load |
| `--collect-proxies` | `` | Curation mode: probe every candidate from the sources and write survivors to this JSON file instead of crawling |
| `--proxy-sources` | `` | Comma-separated proxy list files or URLs to feed `--collect-proxies` (defaults to `--proxies`) |
| `--collect-iterations` | `2` | Probe rounds per candidate in collect mode; more rounds keep only stable proxies |
| `--collect-unique-hosts` | `off` | Keep only the best proxy per host in collect mode |
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

`bruteforce_test.go` includes the interrupt regression: a pool whose
workers are stuck inside checks is cancelled and `close()` must return
promptly with the buffered results, failing at a 5s watchdog the way the
pre-fix pool hung.

`proxy_pool_test.go` covers lease exclusivity, LRU selection, reuse gaps,
`Retry-After` flooring/capping, eviction, direct fallback and context
cancellation at the state-machine level, plus list filtering/ordering
(bare array and object-wrapped lists, the Geonode record mapping), the
tunnel-vs-TCP probe decision, URL fetch and decode error paths, the
`--proxy-save` stats counters and save/re-decode round trip (evicted
peers excluded, successes-descending ordering), the collect-mode
curation rounds (stable, flaky and dead candidates; per-host collapse;
full file-to-file flow) and a `429 → rotate → 200` integration test
through `fetchProviderResponse`. Existing tests keep
passing with a nil pool, which pins down the `--proxies off` behavior.

Failure diagnostics: when attempts are exhausted the error carries the
last failure cause, e.g. `fetch ashbyhq response after retries (last:
transport error: dial tcp 45.74.31.41:10867: i/o timeout)`, so dead-tunnel
proxies are visible in the per-slug failure log without extra tooling.
