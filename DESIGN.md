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
retry. A retry of a transient transport or server error uses the same route
and consumes another limiter token.

## Destination stop policy

Provider responses are checked for destination rate limits and access-control
signals. HTTP 429, 401, 403, 407, 406, 451, and recognized challenge or block
headers stop new work for that provider in the current run. A 429's
`Retry-After` is reported as a recommendation for a later run; it does not
cause an automatic wait-and-retry. Queued work is discarded, in-flight work
is canceled when possible, and brute-force checkpoints are not advanced past
unfinished candidates.

Other transient transport failures and 5xx responses may be retried within
the existing bounded attempt count, but never by changing egress. A stop
signal is shared across workers and job-update tasks for the run.

## Manual proxy routing

`--proxies` accepts a local JSON list or an explicitly supplied HTTP(S) list
URL. The loader filters for entries marked alive with supported HTTP or
SOCKS5 protocols and acceptable anonymity metadata, deduplicates them, then
selects the highest source-reported uptime entry (stable input order breaks
ties). That single route is pinned for the run; source claims are not a
transport-health guarantee.

The loader does not probe candidates against job-board endpoints. The pinned
route is not replaced after transport failures or HTTP responses. If it is
evicted after repeated failures, the run fails closed. `--proxies off`
selects the configured direct client before the run; the client may still use
the process's `HTTP_PROXY`/`HTTPS_PROXY` environment settings.

The pool retains exclusive leases and `--proxy-reuse-min`, so requests
through a single proxy are serialized. `--proxy-failures` controls when the
pinned entry is evicted. `--proxy-save` atomically writes that entry and its
observed run statistics. It does not claim that a saved proxy is verified
for future use.

`--allow-direct-fallback` is rejected. Direct routing must be selected before
the run with `--proxies off`; a proxy failure never changes egress. The
legacy `--collect-proxies` option is also rejected because its former
behavior probed candidates against provider endpoints. Automatic public
source gathering and use in word-list or brute-force discovery are not
implemented.

## Legacy flags

| Flag | Current behavior |
|---|---|
| `--proxies` | Optional manual list; selects one eligible route and pins it |
| `--proxy-reuse-min` | Minimum delay before the pinned route can be reused |
| `--proxy-failures` | Consecutive failures before the pinned route is evicted |
| `--proxy-save` | Save the selected route and observed run statistics |
| `--proxy-probe-timeout` | Deprecated; no proxy health probes run |
| `--allow-direct-fallback` | Deprecated and rejected; select direct mode before the run |
| `--collect-proxies` | Disabled and rejected; no job-board probe collection |
| `--proxy-sources`, `--collect-iterations`, `--collect-unique-hosts` | Deprecated legacy collection flags |
| `--cooldown` | Deprecated; destination restriction responses stop provider work |
| `--rps` | Aggregate target request rate cap, shared across workers and retries |

## Testing

Tests use local fake transports and databases. Proxy tests cover deterministic
single-route selection, no destination probe fanout, pinned-route behavior,
fail-closed eviction, and the absence of rotation after 429 or transport
failures. Provider and job-update tests cover shared stop signals and
`Retry-After`; brute-force tests verify that a destination stop is not retried
or checkpointed past unfinished candidates. CLI tests cover Cobra root
compatibility, fresh options per invocation, and the disabled legacy
collection option.
