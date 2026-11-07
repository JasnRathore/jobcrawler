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
The single direct or proxy egress selected before target traffic begins and pinned for the run, except for a proven pre-send transport failure.

**Proxy cache**:
The local, time-bounded record of normalized candidates and their source/retrieval metadata.

**Route policy**:
The rules deciding whether a target request uses direct or proxy transport, whether direct fallback is permitted, what happens when no route is eligible, and how destination signals affect the run.

**Target request budget**:
The process-wide request-rate limit and bounded-attempt allowance for requests to job-board providers, further limited by any lower provider-specific or published destination cap. Every retry consumes the same budget; proxy count never increases it.

**Source request budget**:
The separate, bounded request/time/concurrency limits for refreshing proxy-source data.

**Destination signal**:
A `429`, `403`, `406`, challenge, or other explicit restriction from a job-board provider. It is handled across routes as a provider response, never as a proxy transport failure.
