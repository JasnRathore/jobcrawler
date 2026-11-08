# Implementation tasks

- [x] `cobra-cli`: Preserve root CLI compatibility with Cobra.
- [x] `safe-manual-route`: Use only explicitly configured manual routes; honor destination stops.
- [x] `rotating-pool`: Restore legacy rotation: every eligible `--proxies` entry is a leaseable route, failures evict routes and workers reroute, exhaustion stops the run (fail closed) instead of direct fallback.
- [x] `429-rotation`: Rate limits cool their route and rotate instead of stopping the run; repeated rate limits across routes pause the provider briefly, while `401`/`407`/`451`/challenge responses (and other providers' `403`/`406`) keep the provider stop.
- [x] `docs-help`: Expand root, `proxy`, and `proxy refresh` help with workflows and examples; document gathered-list storage and the `--proxies`/`--proxy-save` reuse loop in README/DESIGN.
- [x] `proxy-filter`: Add `proxy filter` subcommand that live-checks list entries against a neutral connectivity endpoint (never job boards), writes survivors best-first in the `--proxies`-loadable shape, and parks dead candidates in a not-alive review file.
- [x] `verification`: Update README and verify CLI compatibility, route safety, and the full Go suite.

Deferred for safety: automatic refresh of public-proxy source data inside runs and automatic use of gathered lists without an explicit `--proxies` reference.
