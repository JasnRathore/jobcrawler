# Crawler request and proxy behavior

This document describes the current runtime behavior. The proposed automatic
proxy-gathering system remains deferred; see
`specs/automatic-proxy-gathering/PRODUCT.md` and `TECH.md` for that proposal.
The as-is baseline at the original commit is recorded in
`specs/automatic-proxy-gathering/CURRENT_STATE.md`.

## CLI and discovery

The Cobra root remains the discovery command, so existing root-level flags
continue to work without a `discover` subcommand. Greenhouse remains the
default provider except that `--words` requires an explicitly selected
provider. Word-list candidates are drained before brute-force candidates, and
the same in-run set suppresses duplicates. Only brute-only runs using
`--resume` have a persistent brute-force checkpoint.

The process-wide `--rps` limiter still applies to every target request and
retry. A retry of a transient transport or server error consumes another
limiter token and reacquires a lease, so it may rotate to another healthy
proxy route (direct mode retries use the direct client).

## Destination stop policy

Provider responses are checked for destination rate limits and
access-control signals. A `429` is congestion, not a block: it cools the
route that received it (from `Retry-After`, floored at one second and
capped by `--cooldown`) and the request retries on another healthy route,
so a rate limit never stops the run by itself. On Greenhouse, `403` and
`406` are rate limiting expressed as blocks and follow the same rule.
When rate-limit responses pile up across routes — at least 5 within a
minute — the provider instead pauses briefly (the longest observed
`Retry-After`, at most `--cooldown`) with its burst clamped, so the pool
breathes rather than strip-mining every route against a destination that
is signaling congestion, and then resumes normally.

HTTP 401, 407, 451 — and `403`/`406` from other providers — plus
recognized challenge or block headers are access-control signals: they
stop new work for that provider in the current run, and rotation never
treats them as route failures to bypass. Queued work is discarded,
in-flight work is canceled when possible, and brute-force checkpoints
are not advanced past unfinished candidates. Bad requests and other
unexpected statuses are per-candidate failures: they retry across routes
within the bounded attempt count, and exhausted candidates are recorded
in `failed.txt` without stopping the run.

Other transient transport failures and 5xx responses may be retried within
the existing bounded attempt count, but never by changing egress. A stop
signal is shared across workers and job-update tasks for the run.

## Manual proxy routing

`--proxies` accepts a local JSON list or an explicitly supplied HTTP(S) list
URL. The loader filters for entries marked alive with supported HTTP or
SOCKS5 protocols and acceptable anonymity metadata, deduplicates them, and
keeps every eligible entry as a leaseable route (stable order, best
source-reported uptime first). Source claims are not a transport-health
guarantee; the loader does not probe candidates against job-board endpoints.

Routes are leased exclusively, one request at a time, and handed out
least-recently-used first, so discovery and job updates rotate across the
whole list. Each route carries its own state:

- A transport error or 5xx response marks the route failed; the bounded
  retry loop then reacquires a lease, which rotates the retry to another
  healthy route.
- A 429 response sets the route's cooldown from its `Retry-After` (floored
  at one second, capped by `--cooldown`) so other routes serve while it
  recovers; the limiter counts `429`s across routes in a one-minute
  window and pauses the provider briefly once 5 pile up, then resumes.
- `--proxy-reuse-min` enforces a per-route gap before a route may handle
  another request.
- `--proxy-failures` consecutive failed outcomes evict a route; evicted
  routes are skipped and workers keep rotating through survivors, with
  pool health logged in the brute-force progress line.

Exhaustion and egress policy: when every route has been evicted, requests
stop with `errProxyExhausted`, which is recorded as a provider stop for the
run (cleanly ending work, preserving checkpoints and resumability) rather
than falling back to direct egress. `--proxies off` selects the configured
direct client before the run; the client may still use the process's
`HTTP_PROXY`/`HTTPS_PROXY` environment settings.

`--allow-direct-fallback` is rejected. Direct routing must be selected before
the run with `--proxies off`; rotation stays inside the routes the user
supplied. The legacy `--collect-proxies` option is also rejected because its
former behavior probed candidates against provider endpoints. Automatic public
source gathering is available through `crawler proxy refresh`, which fetches
the HProxy, Proxifly, ProxyScrape, and Geonode lists concurrently and
normalizes their JSON and text formats for review. It reports per-source
failures and retains successful results, writing one JSON object with a
`retrievedAt` timestamp, per-source `sources` report, and the `proxies`
array to stdout — or to an explicit file with `-o FILE` (same JSON, atomic
temp-file rename; `-o -` selects stdout). The documented reuse path is
reviewing that file and passing it to `--proxies` (with `--proxy-save`
writing surviving routes and their observed statistics back in the same
shape). A destination rate limit still stops provider work.

`proxy filter` live-checks a gathered or supplied list with real
connection attempts through each candidate's own CONNECT tunnel against
a neutral connectivity endpoint (never a job-board destination), and
writes survivors — best proven first, with measured attempt statistics —
plus an optional review file of dead candidates marked `alive: false`,
to files that `--proxies` can load directly.

## Proxy controls and legacy flags

| Option or command | Current behavior |
|---|---|
| `--proxies` | Manual list; every eligible entry rotates as a leaseable route |
| `--proxy-reuse-min` | Minimum delay before one route may be reused (per route) |
| `--proxy-failures` | Consecutive failures before a route is evicted from rotation |
| `--proxy-save` | Save surviving routes and observed run statistics for reuse |
| `proxy refresh` | Fetch and normalize four public sources; output for review, not auto-routing |
| `proxy filter` | Live-check a list against a neutral endpoint; write proven routes best-first, dead candidates marked not alive |
| `--proxy-probe-timeout` | Deprecated; no proxy health probes run |
| `--allow-direct-fallback` | Deprecated and rejected; select direct mode before the run |
| `--collect-proxies` | Disabled and rejected; no job-board probe collection |
| `--proxy-sources`, `--collect-iterations`, `--collect-unique-hosts` | Deprecated legacy collection flags |
| `--cooldown` | Caps per-route cooling and the provider pause after repeated `429`s |
| `--rps` | Aggregate target request rate cap, shared across workers and retries |

## Testing

Tests use local fake transports and databases. Proxy tests cover loading
every eligible route without destination probes, exclusive leases with
least-recently-used rotation, per-route reuse gaps and 429 cooldowns,
eviction plus worker reroute to survivors, exhaustion failing closed with a
provider stop, and refresh output feeding the pool directly. Source-refresh
tests cover filtering, HTTPS fetching, redirect rejection, and the absence
of candidate probes. Provider and job-update tests cover shared stop
signals and `Retry-After`; brute-force tests verify that a destination stop
is not retried or checkpointed past unfinished candidates. CLI tests cover
Cobra root compatibility, fresh options per invocation, and the disabled
legacy collection option.
