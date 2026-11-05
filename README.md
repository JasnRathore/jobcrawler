# Job Board Crawler

A command-line tool that discovers company job boards, indexes job listings from
supported providers, and serves a local browser for searching and bookmarking
those listings.

## Requirements

- Go 1.27.3 or later
- Network access to the selected job-board provider

Run commands from the repository directory. Local mode uses `local.db` in the
current working directory by default.

## Database configuration

The crawler reads database settings from environment variables, or from `.env`
in the current directory when those variables are not already set:

```dotenv
DB_MODE=remote
TURSO_DATABASE_URL=libsql://your-database-your-org.turso.io
TURSO_AUTH_TOKEN=your-turso-auth-token
```

In remote mode, the crawler connects directly to Turso; each database
operation is sent to the remote database, with no local replica or sync step.
No `local.db` file is opened, so another process using a local database file
will not cause a file-lock conflict. In local mode, `LOCAL_DB_PATH` selects the
SQLite database file and defaults to `local.db`.

## Quick start

```powershell
# See all command-line flags
go run . -h

# Discover Greenhouse companies from a word list
go run . --greenhouse --words companies.txt

# Discover Lever companies by trying slugs from length 3 through 4
go run . --lever --brute --min 3 --max 4 --resume

# Allow slower HTTP responses up to 45 seconds per request
go run . --lever --words companies.txt --timeout 45s

# Pause for 3 minutes after a rate-limit response
go run . --workable --words companies.txt --cooldown 3m

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

### Brute force

Brute force checks every lowercase alphabetic slug in the selected length
range. It works with all supported providers:

```powershell
go run . --greenhouse --brute --min 3 --max 3
go run . --lever --brute --min 3 --max 4 --resume
go run . --levereu --brute --min 3 --max 4 --resume
go run . --workable --brute --min 2 --max 3 --resume
go run . --ashbyhq --brute --min 2 --max 3 --resume
```

The default range is 4–5 characters. The maximum length is 13 when brute
force is enabled; large ranges can take a very long time. Use `--resume` to
save progress in provider-specific checkpoints in `local.db` and continue that
same provider and length range later. `--resume` requires `--brute`.

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

Both `--update` and `--jobs` are required together. Failed company updates are
reported and cause the command to exit with an error. Successfully updated
companies are checkpointed in `local.db`; rerunning the same command skips
those companies and retries any that failed. Checkpoints are cleared after all
company updates succeed, so the next independent update run refreshes everyone.

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
go run . --bench --greenhouse --brute --min 2 --max 2
go run . --bench --lever --words companies.txt
go run . --bench --levereu --words companies.txt
go run . --bench --ashbyhq --words companies.txt
```

Benchmark mode uses a temporary database and prints request, retry, status,
latency, and company-check statistics when the run finishes. It cannot be
combined with `--update`/`--jobs`, `--browse`, or `--resume`.

## Flags

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
| `--cooldown DURATION` | `2m` | Pause all requests after an HTTP 429 before retrying; e.g. `3m` or `32s`; `0s` disables the cooldown |
| `--proxies FILE\|URL` | `off` | Route requests through a proxy list JSON, or an `http(s)` URL returning one (Geonode proxy-list API supported). Per-egress-IP rotation after 429s, exclusive leases, reuse-gap tracking. See `DESIGN.md`; use only where the destination hosts permit it |
| `--proxy-reuse-min DURATION` | `0s` | Minimum gap before a released proxy may be leased again |
| `--proxy-failures N` | `5` | Evict a proxy after this many consecutive failures |
| `--proxy-probe-timeout DURATION` | `3s` | Startup health-probe timeout per proxy |
| `--proxy-save FILE` | Off | Save proxies that survived the run (never evicted) to `FILE` with request stats, sorted by successes so the file can be fed back to `--proxies` |
| `--bench` | Off | Benchmark discovery without changing `local.db` |
| `--update --jobs` | Off | Refresh indexed jobs for every discovered company |
| `--browse` | Off | Start the local job browser |
| `--addr ADDRESS` | `127.0.0.1:8080` | Browser listen address |

Only one provider flag may be selected. If none is provided, Greenhouse is
used. `--words` requires an explicit provider even though Greenhouse is the
default for other discovery modes.

## Data and recovery

The database stores discovered companies, brute-force checkpoints, indexed
jobs, and bookmarks. The crawler creates/migrates its schema on startup. In
local mode, back up `LOCAL_DB_PATH` while the crawler is stopped; if SQLite
sidecar files such as `local.db-wal` or `local.db-shm` exist, preserve them
together with the database. Remote mode uses Turso as described above.

## Development

Run the test suite:

```powershell
go test ./...
```
