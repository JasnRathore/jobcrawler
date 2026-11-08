# Job Board Crawler

This glossary defines discovery and proxy-routing terms used by the crawler's specifications.

## Discovery

**Discovery run**:
One invocation that generates and checks company candidates using the selected provider and discovery modes.

**Job-board provider**:
A supported job-board platform the crawler queries for company or job data.

**Candidate**:
A company slug or name variant submitted for provider lookup.

**Word-list mode**:
Discovery that derives candidates from the lines of a user-provided file.

**Brute mode**:
Discovery that enumerates company slugs within the selected length range.
_Avoid_: brute-force attack

## Proxy routing

**Proxy source**:
A documented list provider or user-supplied file/URL that publishes proxy candidates.

**Proxy candidate**:
An endpoint published by a proxy source; publication does not establish that it is currently reachable or appropriate to use.

**Eligible proxy**:
A proxy candidate with a supported endpoint, a retrieval time within the configured freshness limit, and no known unavailable status.

**Transport-verified proxy**:
An eligible proxy through which an ordinary authorized request has completed successfully. Source-reported health is not transport verification.

**Run route**:
A direct or proxy egress usable for target requests during a run. With a supplied proxy list, every healthy entry becomes a route and workers lease routes exclusively, rotating least-recently-used first; a route that keeps failing is evicted, and exhaustion stops the run instead of changing egress to direct.

**Rotation**:
Handing out pool routes to workers so successive requests use different healthy proxies. Failed requests rotate the retry to another route; a `429` (and, on Greenhouse, `403`/`406`) cools that route and rotates instead of stopping the run (repeated rate limits across routes pause the provider briefly), while access-control signals (`401`, `407`, `451`, challenges — and other providers' `403`/`406`) stop the provider for the run rather than switching egress to bypass them.

**Proxy cache**:
The local, time-bounded record of normalized candidates and their source/retrieval metadata.

**Route policy**:
The rules deciding whether a target request uses direct or proxy transport, whether direct fallback is permitted, what happens when no route is eligible, and how destination signals affect the run.

**Target request budget**:
The process-wide request-rate limit and bounded-attempt allowance for requests to job-board providers, further limited by any lower provider-specific or published destination cap. Every retry consumes the same budget; proxy count never increases it.

**Source request budget**:
The separate, bounded request/time/concurrency limits for refreshing proxy-source data.

**Destination signal**:
A `429`, `403`, `406`, challenge, or other explicit restriction from a job-board provider. A `429` (and, on Greenhouse, `403`/`406`) is treated as per-route congestion — the route cools down and the request rotates to another route, with a brief provider pause when rate limits pile up across routes. Other access-control responses (`401`, `407`, `451`, challenges, and other providers' `403`/`406`) stop the provider for the run. None are treated as proxy transport failures.
