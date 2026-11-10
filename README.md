# Job Board Crawler

A command-line tool that discovers company job boards, indexes job listings from
supported providers, and serves a local dashboard for searching and bookmarking
those listings.

## What the project does

The crawler has three main jobs:

1. **Discover company boards.** Check company slugs against selected applicant
   tracking system (ATS) providers using a word list, brute force, or both.
2. **Index job listings.** Refresh listings for companies already in the
   database and retain searchable fields such as title, location, dates, and
   application links.
3. **Dashboard and save jobs.** Serve a local web interface over the indexed data
   so you can search, filter, open original listings, and bookmark jobs.

Supported discovery providers are Greenhouse, Lever, Lever EU, Workable, and
Ashby. A provider selection applies to company discovery; `--update --jobs`
refreshes all companies already stored across all providers.

### How a run works

- The selected provider checks candidate company slugs against its own public
  job-board API. Successful company discoveries are saved in the database.
- Word-list input is normalized into a small set of likely slug variants.
  Brute force, when explicitly enabled, enumerates lowercase alphabetic slugs
  within the requested length range.
- Requests are bounded by the worker count, request rate, and per-request
  timeout. A rate limit cools the affected route and retries on another
  healthy route (pausing the provider briefly when many routes hit it),
  while an access-control or recognized challenge response stops discovery
  for that provider for the run; neither switches egress to bypass it.
- Only query providers and company boards you are authorized to access. Prefer
  a word list for known companies; keep brute-force ranges small.
- Job refreshes fetch listings for saved companies and replace their current
  indexed jobs. The local dashboard reads from the database and does not fetch
  jobs from providers while you use it.

With no `--words`, `--brute`, `--update --jobs`, or `--dashboard` option, there is
no candidate source or other work to perform. Pick a workflow below rather
than expecting the default invocation to scan companies.

### Project map

| Path | Purpose |
|---|---|
| `main.go` | Cobra CLI, option validation, startup, discovery orchestration |
| `providers.go`, `provider_*.go` | Shared provider behavior and provider-specific API adapters |
| `bruteforce.go` | Concurrent candidate checks, failure output, and brute checkpoints |
| `jobs.go` | Job fetching/updating and shared job parsing |
| `dashboard.go` | Local job dashboard templates and HTTP handlers |
| `proxy_pool.go` | Loading the supplied proxy list and rotating checks across healthy routes |
| `proxy_refresh.go` | Gathering four public sources and normalizing their formats for review |
| `proxy_filter.go` | Live-checking proxy lists against a neutral endpoint; keeping proven routes |
| `database_config.go` | Local `.env` loading and local/remote database configuration |
| `db/` | SQLite/Turso database access, schema initialization, migrations, queries |
| `*_test.go` | CLI, provider, proxy, database, and workflow tests |
| `specs/` | Product and technical design documents, including proposed work not yet implemented |

## Requirements

- Go 1.27.3 or later
- Network access to the selected job-board provider

Run commands from the repository directory. Local mode uses `local.db` in the
current working directory by default.

## Install or build

From the repository root, run directly with Go:

```powershell
go run . --help
```

Or build a binary:

```powershell
go build -o crawler.exe .
.\crawler.exe --help
```

On macOS or Linux, replace `crawler.exe` with `crawler` and run it as
`./crawler`. No separate service is required for local database mode. The
first run initializes or migrates the database schema automatically.

## Database configuration

The crawler reads database settings from environment variables, or from `.env`
in the current directory when those variables are not already set:

```dotenv
# Optional: local mode is the default.
DB_MODE=local
LOCAL_DB_PATH=local.db

# For remote Turso mode, use these instead:
# DB_MODE=remote
# TURSO_DATABASE_URL=libsql://your-database-your-org.turso.io
# TURSO_AUTH_TOKEN=your-turso-auth-token
```

In remote mode, the crawler connects directly to Turso; each database
operation is sent to the remote database, with no local replica or sync step.
No `local.db` file is opened, so another process using a local database file
will not cause a file-lock conflict. In local mode, `LOCAL_DB_PATH` selects the
SQLite database file and defaults to `local.db`. Environment variables take
precedence over values in `.env`. Keep credentials private and do not commit
`.env` or database tokens.

The database contains provider-specific discovered companies, job records,
bookmarks, and progress checkpoints. It is initialized and migrated on
startup. The dashboard only listens on the configured address; by default that
is loopback (`127.0.0.1:8080`).

## Quick start

```powershell
# See all command-line flags
go run . -h

# Discover Greenhouse companies from a word list
go run . --greenhouse --words companies.txt

# Try a deliberately small slug range with a saved checkpoint
go run . --lever --brute --min 1 --max 1 --resume

# Allow slower HTTP responses up to 45 seconds per request
go run . --lever --words companies.txt --timeout 45s

# Keep request volume conservative
go run . --workable --words companies.txt --workers 8 --rps 2

# Fetch jobs for every discovered company, across all providers
go run . --update --jobs

# Route a run through a rotating proxy pool from a list you collected
go run . --greenhouse --words companies.txt --proxies proxies.json

# Open the job dashboard
go run . --dashboard
```

By default, discovery uses Greenhouse. The provider flags select exactly one
provider: `--greenhouse`, `--lever`, `--levereu`, `--workable`, or
`--ashbyhq`.

## Discover companies

### Word lists

Pass a text file with one company name or slug per line. Word-list discovery
requires an explicit provider flag:

```powershell
go run . --greenhouse --words companies.txt
go run . --lever --words companies.txt
go run . --levereu --words companies.txt
go run . --workable --words companies.txt
go run . --ashbyhq --words companies.txt
```

For each non-empty name, the crawler lowercases it, removes common company
suffixes, then checks likely slug variants: joined characters, hyphenated
characters, and the joined form with `hq`, `inc`, or `careers` appended.
Candidates outside the selected `--min` and `--max` lengths are skipped.
Duplicate candidates from the same run are checked once.

For example, `Acme Corp, Inc.` normalizes to `acme` and generates joined,
hyphenated, and suffix variants (`acme`, `acme`, `acmehq`, `acmeinc`, and
`acmecareers`); duplicate candidates are checked once per run. Use one line
per organization; blank or punctuation-only lines produce no candidates.
Since variants are generated automatically, a word list is a starting point
rather than a literal list of only the exact strings to check.

### Brute force

Brute force checks every lowercase alphabetic slug in the selected length
range. Use it only for authorized, narrowly scoped testing; the request count
grows exponentially, and the maximum supported length is a technical limit,
not a recommended setting. For example, the following deliberately checks
only one-character candidates:

```powershell
go run . --greenhouse --brute --min 1 --max 1
```

The default range is 4–5 characters. The maximum length is 13 when brute
force is enabled; the default range is already much larger than the example.
Use `--resume` to save progress in provider-specific checkpoints in
the configured database and continue that same provider and length range
later. `--resume` requires `--brute`.

Failed slugs from both word-list and brute-force checks are appended to
`failed.txt`, one per line, so they can be reviewed or retried. With `--resume`,
a checkpoint continues if it has at most five failed checks; more than five
failures stop that checkpoint without advancing its saved position. Word-list
mode generates variants from each line, so it may check additional variants
when retrying failed slugs.

You can combine `--words` and `--brute` in one run, but word-list mode requires
an explicit provider. Candidates already submitted from the word list are
skipped during brute force in that run.

## Refresh jobs

After discovering companies, fetch and refresh jobs for all known companies
across every provider:

```powershell
go run . --update --jobs
```

Both `--update` and `--jobs` are required together. The run snapshots the
companies currently stored for all providers. Each company's refreshed jobs,
bookmark snapshots, and completion marker are committed together.

If ordinary provider errors occur, other queued companies continue; the
command exits with an error and a later `--update --jobs` resumes only pending
companies from the original snapshot. Companies discovered after that snapshot
are picked up by the next fresh run. Once the full run succeeds, recovery state
is retired and a later invocation refreshes every company again.

A rate limit cools the affected route and retries on another healthy
route, pausing the provider briefly only when rate limits pile up across
routes. An access-control or recognized challenge response stops the
whole update run and cancels queued work. The command reports completed
and pending counts and any valid `Retry-After` deadline. It does not
automatically wait, retry the blocked request, or switch providers/proxies; rerun
`--update --jobs` after the reported deadline. If two update processes target
the same database, only one may own the active run. After an abrupt process
exit, the lease can take up to 60 seconds to expire before another invocation
can resume it.

## Job dashboard

Start the local dashboard:

```powershell
go run . --dashboard
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080). The dashboard searches jobs
across providers and supports:

- Search by job title, company, and location; optionally include descriptions
- Filter by location, posting date range, employment type, and workplace type
- Save jobs and switch between all jobs and saved jobs
- Select a role to view its description and properties in the dashboard panes
- Open the original listing and application links

To listen on another address or port, use `--addr`. The previous `--browse`
flag remains available as a deprecated alias for `--dashboard`:

```powershell
go run . --dashboard --addr 127.0.0.1:9090
```

## Benchmark requests

Measure discovery request performance without modifying `local.db`:

```powershell
go run . --bench --greenhouse --brute --min 1 --max 1
go run . --bench --lever --words companies.txt
go run . --bench --levereu --words companies.txt
go run . --bench --ashbyhq --words companies.txt
```

Benchmark mode uses a temporary database and prints request, retry, status,
latency, and company-check statistics when the run finishes. It cannot be
combined with `--update`/`--jobs`, `--dashboard`, or `--resume`.

## Proxy lists and rotation

The crawler only sends proxied traffic through a list you supply explicitly
with `--proxies`. A list is either a local JSON file or an HTTP(S) URL
returning a supported JSON list with this shape:

```json
[
  {
    "proxy": "http://proxy.example.net:8080",
    "protocol": "http",
    "alive": true,
    "anonymity": "elite",
    "uptime": 99.5
  }
]
```

Replace the example endpoint with a proxy you are authorized to use. Each
entry must be `alive`, use `http` or `socks5`, and carry an `elite` (or
absent) anonymity value when one is set; duplicate URLs are collapsed and
entries stay ordered by reported uptime (best first). This metadata is a
source claim, not a live health check or safety guarantee; the crawler
never probes candidate proxies against job-board endpoints.

```powershell
go run . --greenhouse --words companies.txt --proxies proxies.json
go run . --greenhouse --words companies.txt --proxies https://example.net/list.json
```

### How the pool rotates and evicts

Every eligible entry becomes a route in the run's proxy pool:

- Workers lease one route per request, exclusively, and always take the
  route that was used least recently, so requests spread across the whole
  list instead of pinning one proxy.
- A `429` applies the destination's `Retry-After` as a per-route cooldown
  (floored at one second, capped by `--cooldown`); the request is retried
  on another healthy route while the first one cools down, so a rate
  limit never stops the run. On Greenhouse, `403`/`406` behave the same
  (rate limiting expressed as blocks). When rate limits pile up across
  many routes (at least 5 within a minute), the provider pauses briefly
  — bounded by the longest observed `Retry-After`, at most `--cooldown` —
  and then resumes with fewer simultaneous attempts, instead of
  strip-mining the whole pool against a destination that is signaling
  congestion.
- Destination `401`, `407`, `451`, and recognized challenge responses
  are access-control signals: they still stop the provider for the run,
  and rotation never treats them as route failures to bypass. Other
  providers' `403`/`406` also still stop. Bad requests and other
  unexpected statuses retry across routes and, once exhausted, are
  logged to `failed.txt` for later retry — the run keeps going.
- Failures and destination policy are kept apart: transport errors and
  5xx responses mark the route failed and the immediate retry rotates to
  another route. `--proxy-reuse-min` enforces a per-route gap between
  requests.
- A route that reaches `--proxy-failures` (default `5`) consecutive
  failures is evicted and no longer leased; the run logs the eviction and
  the remaining healthy count, and workers keep rotating through
  survivors.
- If every route is evicted, the run stops for that provider (fail
  closed) instead of silently falling back to your direct connection.
  `--proxies off` selects direct egress before the run starts, and
  `--dashboard` only reads database content and never uses a proxy.

`proxy refresh` gathers lists from the four built-in public sources in
parallel and normalizes them for review; `proxy filter` live-checks a
list and keeps only routes that tunnel (see the next section). The legacy
`--collect-proxies` mode remains
disabled because it probed candidates against job-board endpoints, and
`--allow-direct-fallback` is rejected; choose direct mode before starting
with `--proxies off`. See `DESIGN.md` for routing behavior.

## How gathered proxies are stored and reused

`crawler proxy refresh` fetches the HProxy, Proxifly, ProxyScrape, and
Geonode lists concurrently, parses their JSON and text formats, filters to
eligible credential-free entries, and writes one normalized JSON object to
**stdout**:

```json
{
  "retrievedAt": "2026-01-01T00:00:00Z",
  "sources": [{"name": "HProxy", "count": 250}, {"name": "Geonode", "error": "..."}],
  "proxies": [{"proxy": "http://198.51.100.7:8080", "protocol": "http", "alive": true, "anonymity": "elite", "uptime": 99.1}]
}
```

Storage is explicit: the report goes to **stdout**, or to the file you
choose with `-o FILE` (the same JSON, written through a temp-file rename
so a crash cannot corrupt a previous gather). Nothing else — no database,
no cache — is stored:

1. **Capture the list.** Write the report to a file; the timestamped
   `retrievedAt` and per-source counts/errors travel with the proxies, so
   a failed source stays visible later:

   ```powershell
   go run . proxy refresh -o proxies.json
   go run . proxy refresh --source https://example.net/list.json -o proxies.json
   go run . proxy refresh --limit 2000 --timeout 30s -o proxies.json

   # The same JSON via stdout redirection, for piping
   go run . proxy refresh > proxies.json
   go run . proxy refresh -o -
   ```

2. **Review and edit.** The file is directly readable; remove entries you
   are not authorized to use. The `proxies` array is what `--proxies`
   loads, and the object wrapper (with `sources` metadata) is accepted as
   is, so you do not need to reformat the file after editing.

3. **Use the list in a run.** Point `--proxies` at the file; every
   eligible entry rotates, failing routes are evicted, and the run stops
   cleanly if all routes die:

   ```powershell
   go run . --greenhouse --words companies.txt --proxies proxies.json
   go run . --lever --brute --min 1 --max 1 --proxies proxies.json --resume
   go run . --update --jobs --proxies proxies.json
   ```

4. **Keep only proven routes.** Add `--proxy-save` to any proxied run;
   every five minutes and at shutdown it writes the routes that survived
   the run — with observed request, success, and failure counts — sorted
   best first. That saved file is the same JSON list shape, so later runs
   can use it directly:

   ```powershell
   go run . --greenhouse --words companies.txt --proxies proxies.json --proxy-save proven.json
   go run . --workable --words companies.txt --proxies proven.json
   ```

The refresh command has no scheduler and no automatic crawler integration,
by design: gather, review, then pass the file explicitly.

### Filter a list before a run

Gathered entries are unverified: source-reported `alive`/`elite` claims
leave dead ports, HTTP-only bots (which answer `400` to `CONNECT`), ACL
blocks, and captive-portal replies in the pool, and each of those surfaces
at run time as a `transport error` against a healthy destination. The
`proxy filter` command collapses that noise into a short proven list:

```powershell
go run . proxy filter -o proven.json proxies.json
go run . proxy filter --failures dead.json proxies.json > proven.json
go run . proxy filter --rounds 1 --concurrency 128 --timeout 5s -o proven.json proxies.json
```

Each eligible entry gets `--rounds` HTTPS requests to a neutral
connectivity endpoint through its own CONNECT tunnel (never a job-board
destination — verification proves transport health without touching
targets). A proxy whose tunnel or TLS fails, or that answers with its own
error status, counts as a failure. By default a candidate survives on its
first success; `--all-rounds` requires every attempt to pass.
`--concurrency` (default `64`) and `--timeout` (default `10s`) bound the
probe load, and `--unique-hosts` keeps only the best entry per host.

Survivors are written with measured attempt statistics (`requests`,
`successes`, `failures`, `response_ms`) in the same JSON list shape that
`--proxies` loads, strongest first, so the next run starts with the best
proven routes. Dead candidates can be parked with `--failures FILE` in a
review file whose entries are marked `alive: false` and can therefore
never be loaded as a run list. Only keep and use proxies you are
authorized to use.

## Flags

The command is a single Cobra root command. `go run . --help` groups these
options by workflow and includes runnable examples; this table is the longer
reference.

| Flag | Default | Description |
|---|---:|---|
| `--greenhouse` | Selected if no provider flag is given | Discover companies on Greenhouse |
| `--lever` | Off | Discover companies on Lever |
| `--levereu` | Off | Discover companies on Lever's EU API |
| `--workable` | Off | Discover companies on Workable |
| `--ashbyhq` | Off | Discover companies on Ashby |
| `--words FILE` | None | Read company names or slugs from a file, one per line; requires an explicit provider |
| `--brute` | Off | Brute-force slugs in the selected length range |
| `--resume` | Off | Save/resume brute-force checkpoints; requires `--brute` |
| `--min N` | `4` | Minimum candidate slug length |
| `--max N` | `5` | Maximum candidate slug length; brute force allows at most `13` |
| `--workers N` | `64` | Number of concurrent workers; must be at least `1` |
| `--rps N` | `50` | Maximum request rate; must be greater than `0` |
| `--timeout DURATION` | `10s` | Maximum duration of an individual HTTP request, including reading its response body; e.g. `30s` or `2m`; must be greater than `0` |
| `--cooldown DURATION` | `2m` | Caps per-route `Retry-After` cooling and the provider pause after repeated rate limits; `401`/`407`/`451` and challenge responses still stop the provider |
| `--proxies FILE\|URL` | `off` | Load every eligible entry as a route and rotate requests across them; `off` selects direct egress |
| `--proxy-reuse-min DURATION` | `0s` | Minimum gap before one proxy may handle another request (applies per route) |
| `--proxy-failures N` | `5` | Evict a proxy from the rotation after this many consecutive failed requests |
| `--proxy-probe-timeout DURATION` | `3s` | Deprecated; proxy health probes are disabled |
| `--proxy-save FILE` | Off | Save the proxies that survived the run, with observed stats, for reuse with `--proxies` |
| `--collect-proxies FILE` | Off | Disabled legacy option; returns an error and does not probe job-board endpoints |
| `--proxy-sources LIST` | Off | Deprecated legacy collection input; collection mode is disabled |
| `--collect-iterations N` | `2` | Deprecated legacy collection option |
| `--collect-unique-hosts` | Off | Deprecated legacy collection option |
| `--allow-direct-fallback` | Off | Disabled; select direct mode before the run with `--proxies off` |
| `--bench` | Off | Benchmark discovery without changing `local.db` |
| `--update --jobs` | Off | Refresh indexed jobs for every discovered company |
| `--dashboard` | Off | Start the local job dashboard |
| `--browse` | Off | Deprecated alias for `--dashboard` |
| `--addr ADDRESS` | `127.0.0.1:8080` | Dashboard listen address |

Only one provider flag may be selected. If none is provided, Greenhouse is
used for brute-force discovery. `--words` requires an explicit provider even
though Greenhouse is the default for other discovery modes. `--update` and
`--jobs` must be used together. Benchmark mode writes to a temporary database
and cannot be combined with update, the dashboard, or resume.

## Data and recovery

The database stores discovered companies, brute-force checkpoints, job-update
run/target checkpoints, indexed jobs, and bookmarks. Job-update targets retain
their original run snapshot until completion; brute-force checkpoint cadence
and resume behavior are separate and unchanged. The crawler creates/migrates
its schema on startup. In local mode, back up `LOCAL_DB_PATH` while the crawler
is stopped; if SQLite sidecar files such as `local.db-wal` or `local.db-shm`
exist, preserve them together with the database. Remote mode uses Turso as
described above.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `--words requires an explicit provider flag` | Add one provider flag, such as `--greenhouse` or `--lever`. |
| `choose only one provider flag` | Select exactly one of the five provider flags. |
| No companies are found from a word list | Confirm the provider, spellings, generated slug variants, and `--min`/`--max` range. |
| Brute force runs much longer than expected | Reduce the length range. Candidate count grows exponentially with each extra character. |
| A job update stops after a destination response | The run is resumable and completed work is retained. Rate limits rotate and pause briefly instead of stopping; `401`/`407`/`451`/challenge responses stop. Follow the destination's published access policy; if a `Retry-After` time is printed, wait until then before rerunning `--update --jobs`. |
| `no eligible proxy entries` | Check the JSON structure, `alive`, `protocol`, and `anonymity` values. Proxy source metadata is not verified by probing targets. |
| `no eligible proxy entries in FILE` | The list had entries, but none passed the eligibility filter; keep entries `alive`, `http`/`socks5`, and `elite` (or no anonymity value). |
| `greenhouse stopped: no healthy proxy route remains` | All `--proxies` routes failed repeatedly and the run stopped instead of going direct; collect fresh lists with `crawler proxy refresh`, review the file, and rerun (add `--resume` to continue brute-force checkpoints). |
| Remote database configuration fails | Set `DB_MODE=remote`, `TURSO_DATABASE_URL`, and `TURSO_AUTH_TOKEN`; environment values override `.env`. |
| Dashboard address is already in use | Choose another local port, for example `--dashboard --addr 127.0.0.1:9090`. |

## Development

Run the test suite:

```powershell
go test ./...
go vet ./...
```
