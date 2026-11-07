# Job Board Crawler

A command-line tool that discovers company job boards, indexes job listings from
supported providers, and serves a local browser for searching and bookmarking
those listings.

## What the project does

The crawler has three main jobs:

1. **Discover company boards.** Check company slugs against selected applicant
   tracking system (ATS) providers using a word list, brute force, or both.
2. **Index job listings.** Refresh listings for companies already in the
   database and retain searchable fields such as title, location, dates, and
   application links.
3. **Browse and save jobs.** Serve a local web interface over the indexed data
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
  timeout. A rate-limit, access-control, or recognized challenge response
  stops discovery for that provider for the run; it does not switch egress or
  continue brute force.
- Only query providers and company boards you are authorized to access. Prefer
  a word list for known companies; keep brute-force ranges small.
- Job refreshes fetch listings for saved companies and replace their current
  indexed jobs. The local browser reads from the database and does not fetch
  jobs from providers while you browse.

With no `--words`, `--brute`, `--update --jobs`, or `--browse` option, there is
no candidate source or other work to perform. Pick a workflow below rather
than expecting the default invocation to scan companies.

### Project map

| Path | Purpose |
|---|---|
| `main.go` | Cobra CLI, option validation, startup, discovery orchestration |
| `providers.go`, `provider_*.go` | Shared provider behavior and provider-specific API adapters |
| `bruteforce.go` | Concurrent candidate checks, failure output, and brute checkpoints |
| `jobs.go` | Job fetching/updating and the local browser server |
| `proxy_pool.go` | Loading and pinning one manually supplied proxy route |
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
startup. The browser only listens on the configured address; by default that
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

# Browse indexed jobs
go run . --browse
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

A rate-limit, access-control, or recognized challenge response stops the
whole update run and cancels queued work. The command reports completed and
pending counts and any valid `Retry-After` deadline. It does not automatically
wait, retry the blocked request, or switch providers/proxies; rerun
`--update --jobs` after the reported deadline. If two update processes target
the same database, only one may own the active run. After an abrupt process
exit, the lease can take up to 60 seconds to expire before another invocation
can resume it.

## Browse jobs

Start the local browser:

```powershell
go run . --browse
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080). The browser searches jobs
across providers and supports:

- Search by job title, company, and location; optionally include descriptions
- Filter by location, posting date range, employment type, and workplace type
- Save jobs and switch between all jobs and saved jobs
- Open the original listing and application links

To listen on another address or port, use `--addr`:

```powershell
go run . --browse --addr 127.0.0.1:9090
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
combined with `--update`/`--jobs`, `--browse`, or `--resume`.

## Manual proxy routing

For traffic you are authorized to send through a proxy, `--proxies` accepts
a local JSON list or an explicitly supplied HTTP(S) list URL. The crawler
selects one eligible entry using the list's reported uptime and pins that
route for the entire run. Source health claims are not verified; the crawler
does not probe proxy candidates against job-board endpoints, rotate egress,
or fall back to another route. A destination `429`, `403`, `406`, or
recognized challenge stops provider work for the run.

Automatic public-proxy gathering and automatic use in discovery are not
implemented. The legacy `--collect-proxies` mode is disabled because it
probed candidates against job-board endpoints. `--allow-direct-fallback` is
also disabled; choose direct mode before starting by leaving `--proxies` off.
See `DESIGN.md` for the current routing behavior.

For a local list, use this JSON shape:

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

Replace the example endpoint with a proxy you are authorized to use. The list
may also be supplied as an HTTP(S) URL returning a supported JSON list. At
least one entry must be marked alive and use `http` or `socks5`; when an
anonymity value is present, it must be `elite`. The highest source-reported
uptime is selected. This metadata is not a live health check or safety
guarantee.

```powershell
go run . --greenhouse --words companies.txt --proxies proxies.json
go run . --greenhouse --words companies.txt --proxies https://example.net/list.json
```

If that one route becomes unavailable or reaches `--proxy-failures`, the run
fails closed instead of selecting another proxy or sending traffic directly.
`--proxies off` disables the application-managed proxy list; Go may still
honor `HTTP_PROXY`/`HTTPS_PROXY` environment settings. `--browse` only reads
local or remote database content and does not use a proxy.

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
| `--cooldown DURATION` | `2m` | Deprecated compatibility option; destination restriction responses stop provider work instead of retrying |
| `--proxies FILE\|URL` | `off` | Select one eligible manual proxy entry and pin it for the run; no rotation |
| `--proxy-reuse-min DURATION` | `0s` | Minimum gap before the pinned proxy can handle another request |
| `--proxy-failures N` | `5` | Stop using the pinned route after this many consecutive failures |
| `--proxy-probe-timeout DURATION` | `3s` | Deprecated; proxy health probes are disabled |
| `--proxy-save FILE` | Off | Save the selected proxy and its run stats to `FILE` |
| `--collect-proxies FILE` | Off | Disabled legacy option; returns an error and does not probe job-board endpoints |
| `--proxy-sources LIST` | Off | Deprecated legacy collection input; collection mode is disabled |
| `--collect-iterations N` | `2` | Deprecated legacy collection option |
| `--collect-unique-hosts` | Off | Deprecated legacy collection option |
| `--allow-direct-fallback` | Off | Disabled; select direct mode before the run with `--proxies off` |
| `--bench` | Off | Benchmark discovery without changing `local.db` |
| `--update --jobs` | Off | Refresh indexed jobs for every discovered company |
| `--browse` | Off | Start the local job browser |
| `--addr ADDRESS` | `127.0.0.1:8080` | Browser listen address |

Only one provider flag may be selected. If none is provided, Greenhouse is
used for brute-force discovery. `--words` requires an explicit provider even
though Greenhouse is the default for other discovery modes. `--update` and
`--jobs` must be used together. Benchmark mode writes to a temporary database
and cannot be combined with update, browsing, or resume.

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
| A job update stops after a destination response | The run is resumable and completed work is retained. Follow the destination's published access policy; if a `Retry-After` time is printed, wait until then before rerunning `--update --jobs`. |
| `no eligible proxy entries` | Check the JSON structure, `alive`, `protocol`, and `anonymity` values. Proxy source metadata is not verified by probing targets. |
| Remote database configuration fails | Set `DB_MODE=remote`, `TURSO_DATABASE_URL`, and `TURSO_AUTH_TOKEN`; environment values override `.env`. |
| Browser address is already in use | Choose another local port, for example `--browse --addr 127.0.0.1:9090`. |

## Development

Run the test suite:

```powershell
go test ./...
go vet ./...
```
