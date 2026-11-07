package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	libsql "github.com/tursodatabase/libsql-client-go/libsql"
	_ "turso.tech/database/tursogo"
)

type DB struct {
	conn    *sql.DB
	writeMu sync.Mutex
}

var jobSearchTerms = regexp.MustCompile(`[a-z0-9]+`)

// New opens or creates the local SQLite database and initializes the schema.
func New(ctx context.Context, path string) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		path = "local.db"
	}

	conn, err := sql.Open("turso", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return newWithConnection(ctx, conn)
}

// NewRemote connects directly to a remote Turso database without a local replica.
func NewRemote(ctx context.Context, remoteURL, authToken string) (*DB, error) {
	connector, err := libsql.NewConnector(remoteURL, libsql.WithAuthToken(authToken))
	if err != nil {
		return nil, fmt.Errorf("create remote database connector: %w", err)
	}
	return newWithConnection(ctx, sql.OpenDB(connector))
}

func newWithConnection(ctx context.Context, conn *sql.DB) (database *DB, err error) {
	initialized := false
	defer func() {
		if !initialized {
			if closeErr := conn.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close database after initialization failure: %w", closeErr))
			}
		}
	}()

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	const schema = `
		CREATE TABLE IF NOT EXISTS brute_checkpoints (
			provider TEXT NOT NULL,
			min_length INTEGER NOT NULL,
			max_length INTEGER NOT NULL,
			alphabet TEXT NOT NULL,
			next_index INTEGER NOT NULL,
			total INTEGER NOT NULL,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (provider, min_length, max_length, alphabet)
		);
		CREATE TABLE IF NOT EXISTS job_update_checkpoints (
			provider TEXT NOT NULL,
			company_slug TEXT NOT NULL,
			completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (provider, company_slug)
		);
		CREATE TABLE IF NOT EXISTS provider_companies (
			provider TEXT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (provider, name)
		);
		CREATE TABLE IF NOT EXISTS indexed_jobs (
			provider TEXT NOT NULL,
			company_slug TEXT NOT NULL,
			job_id TEXT NOT NULL,
			title TEXT NOT NULL,
			company_name TEXT NOT NULL,
			location_name TEXT NOT NULL,
			location_json TEXT NOT NULL DEFAULT '',
			details_url TEXT NOT NULL DEFAULT '',
			apply_url TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT '',
			first_published TEXT NOT NULL DEFAULT '',
			employment_type TEXT NOT NULL DEFAULT '',
			workplace_type TEXT NOT NULL DEFAULT '',
			department TEXT NOT NULL DEFAULT '',
			education TEXT NOT NULL DEFAULT '',
			experience TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			attributes_json TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (provider, company_slug, job_id)
		);
		CREATE TABLE IF NOT EXISTS indexed_bookmarks (
			provider TEXT NOT NULL,
			company_slug TEXT NOT NULL,
			job_id TEXT NOT NULL,
			title TEXT NOT NULL,
			company_name TEXT NOT NULL,
			location_name TEXT NOT NULL,
			location_json TEXT NOT NULL DEFAULT '',
			details_url TEXT NOT NULL DEFAULT '',
			apply_url TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT '',
			first_published TEXT NOT NULL DEFAULT '',
			employment_type TEXT NOT NULL DEFAULT '',
			workplace_type TEXT NOT NULL DEFAULT '',
			department TEXT NOT NULL DEFAULT '',
			education TEXT NOT NULL DEFAULT '',
			experience TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			attributes_json TEXT NOT NULL DEFAULT '{}',
			is_available INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (provider, company_slug, job_id)
		);
	`

	if _, err := conn.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	database = &DB{conn: conn}
	if err := database.migrateLegacySchema(ctx); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS indexed_jobs_title_idx ON indexed_jobs(title);
		CREATE INDEX IF NOT EXISTS indexed_jobs_company_idx ON indexed_jobs(company_name);
		CREATE INDEX IF NOT EXISTS indexed_jobs_location_idx ON indexed_jobs(location_name);
		CREATE INDEX IF NOT EXISTS indexed_jobs_date_idx ON indexed_jobs(first_published);
		CREATE INDEX IF NOT EXISTS indexed_jobs_employment_idx ON indexed_jobs(employment_type);
		CREATE INDEX IF NOT EXISTS indexed_jobs_workplace_idx ON indexed_jobs(workplace_type);
	`); err != nil {
		return nil, fmt.Errorf("create job search indexes: %w", err)
	}
	initialized = true
	return database, nil
}

func (d *DB) migrateLegacySchema(ctx context.Context) error {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer tx.Rollback()

	if err := migrateBruteCheckpoints(ctx, tx); err != nil {
		return err
	}
	if err := migrateJobUpdateRuns(ctx, tx); err != nil {
		return err
	}
	if err := migrateIndexedTable(ctx, tx, "indexed_jobs", false); err != nil {
		return err
	}
	if err := migrateIndexedTable(ctx, tx, "indexed_bookmarks", true); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO provider_companies(provider, name)
		SELECT 'greenhouse', name FROM company
		WHERE 1 = 1
		ON CONFLICT(provider, name) DO NOTHING
	`); err != nil && !isMissingTable(err) {
		return fmt.Errorf("migrate legacy companies: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO indexed_jobs (
			provider, company_slug, job_id, title, company_name, location_name,
			location_json, details_url, updated_at, first_published, description,
			attributes_json
		)
		SELECT 'greenhouse', company_slug, CAST(id AS TEXT), title, company_name,
			location_name, location_json, absolute_url, updated_at, first_published,
			content, metadata
		FROM greenhouse_jobs
		WHERE 1 = 1
		ON CONFLICT(provider, company_slug, job_id) DO NOTHING
	`); err != nil && !isMissingTable(err) {
		return fmt.Errorf("migrate legacy greenhouse jobs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO indexed_bookmarks (
			provider, company_slug, job_id, title, company_name, location_name,
			details_url, updated_at, first_published, description, is_available, created_at
		)
		SELECT 'greenhouse', company_slug, CAST(job_id AS TEXT), title, company_name,
			location_name, absolute_url, updated_at, first_published, content,
			CASE WHEN EXISTS (
				SELECT 1 FROM indexed_jobs j WHERE j.provider = 'greenhouse'
				AND j.company_slug = greenhouse_bookmarks.company_slug
				AND j.job_id = CAST(greenhouse_bookmarks.job_id AS TEXT)
			) THEN 1 ELSE 0 END,
			created_at
		FROM greenhouse_bookmarks
		WHERE 1 = 1
		ON CONFLICT(provider, company_slug, job_id) DO NOTHING
	`); err != nil && !isMissingTable(err) {
		return fmt.Errorf("migrate legacy greenhouse bookmarks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE indexed_bookmarks
		SET is_available = CASE WHEN EXISTS (
			SELECT 1 FROM indexed_jobs j
			WHERE j.provider = indexed_bookmarks.provider
				AND j.company_slug = indexed_bookmarks.company_slug
				AND j.job_id = indexed_bookmarks.job_id
		) THEN 1 ELSE 0 END
	`); err != nil {
		return fmt.Errorf("set migrated bookmark availability: %w", err)
	}

	for _, table := range []string{"company", "greenhouse_jobs", "greenhouse_bookmarks"} {
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop migrated legacy table %q: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migration: %w", err)
	}
	return nil
}

func migrateJobUpdateRuns(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create schema migration table: %w", err)
	}

	var applied int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1").Scan(&applied); err != nil {
		return fmt.Errorf("check job update run migration: %w", err)
	}
	if applied != 0 {
		return nil
	}

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE job_update_runs (
			run_id TEXT PRIMARY KEY,
			active_slot INTEGER UNIQUE,
			status TEXT NOT NULL,
			target_count INTEGER NOT NULL,
			owner_token TEXT NOT NULL DEFAULT '',
			fencing_token INTEGER NOT NULL DEFAULT 0,
			lease_expires_at INTEGER NOT NULL DEFAULT 0,
			stop_provider TEXT NOT NULL DEFAULT '',
			stop_status INTEGER NOT NULL DEFAULT 0,
			stop_reason TEXT NOT NULL DEFAULT '',
			retry_after_at INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE job_update_targets (
			run_id TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			provider TEXT NOT NULL,
			company_slug TEXT NOT NULL,
			status TEXT NOT NULL,
			last_error TEXT NOT NULL DEFAULT '',
			completed_at TEXT,
			PRIMARY KEY (run_id, provider, company_slug),
			UNIQUE (run_id, ordinal)
		);
		CREATE INDEX job_update_targets_pending_idx
			ON job_update_targets(run_id, status, ordinal);
		INSERT INTO schema_migrations(version) VALUES (1);
	`); err != nil {
		return fmt.Errorf("create job update run schema: %w", err)
	}
	return nil
}

func migrateBruteCheckpoints(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "brute_checkpoints")
	if err != nil {
		return err
	}
	if _, alreadyProviderScoped := columns["provider"]; alreadyProviderScoped {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "ALTER TABLE brute_checkpoints RENAME TO brute_checkpoints_legacy"); err != nil {
		return fmt.Errorf("rename legacy brute checkpoints: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
			CREATE TABLE brute_checkpoints (
				provider TEXT NOT NULL,
				min_length INTEGER NOT NULL,
				max_length INTEGER NOT NULL,
				alphabet TEXT NOT NULL,
				next_index INTEGER NOT NULL,
				total INTEGER NOT NULL,
				updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (provider, min_length, max_length, alphabet)
			)
	`); err != nil {
		return fmt.Errorf("create provider-scoped brute checkpoints: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO brute_checkpoints(provider, min_length, max_length, alphabet, next_index, total, updated_at)
			SELECT 'greenhouse', min_length, max_length, alphabet, next_index, total, updated_at
			FROM brute_checkpoints_legacy
	`); err != nil {
		return fmt.Errorf("migrate Greenhouse brute checkpoints: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE brute_checkpoints_legacy"); err != nil {
		return fmt.Errorf("drop legacy brute checkpoints: %w", err)
	}
	return nil
}

func isMissingTable(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no such table")
}

func migrateIndexedTable(ctx context.Context, tx *sql.Tx, table string, bookmarks bool) error {
	columns, err := tableColumns(ctx, tx, table)
	if err != nil {
		return err
	}
	if _, legacy := columns["raw_json"]; !legacy {
		return nil
	}
	legacyTable := table + "_legacy"
	if _, err := tx.ExecContext(ctx, "ALTER TABLE "+table+" RENAME TO "+legacyTable); err != nil {
		return fmt.Errorf("rename legacy table %q: %w", table, err)
	}
	create := `CREATE TABLE ` + table + ` (
		provider TEXT NOT NULL, company_slug TEXT NOT NULL, job_id TEXT NOT NULL,
		title TEXT NOT NULL, company_name TEXT NOT NULL, location_name TEXT NOT NULL,
		location_json TEXT NOT NULL DEFAULT '', details_url TEXT NOT NULL DEFAULT '',
		apply_url TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '',
		first_published TEXT NOT NULL DEFAULT '', employment_type TEXT NOT NULL DEFAULT '',
		workplace_type TEXT NOT NULL DEFAULT '', department TEXT NOT NULL DEFAULT '',
		education TEXT NOT NULL DEFAULT '', experience TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '', attributes_json TEXT NOT NULL DEFAULT '{}',`
	if bookmarks {
		create += " is_available INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
	}
	create += " PRIMARY KEY(provider, company_slug, job_id))"
	if _, err := tx.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("create normalized table %q: %w", table, err)
	}
	createdAt := "CURRENT_TIMESTAMP"
	if _, ok := columns["created_at"]; ok {
		createdAt = "created_at"
	}
	insert := `INSERT INTO ` + table + ` (
		provider, company_slug, job_id, title, company_name, location_name,
		location_json, details_url, updated_at, first_published, description,
		attributes_json`
	selectValues := `provider, company_slug, job_id, title, company_name, location_name,
		COALESCE(location_json, ''), COALESCE(absolute_url, ''), COALESCE(updated_at, ''),
		COALESCE(first_published, ''), COALESCE(content, ''), COALESCE(metadata, '')`
	if bookmarks {
		insert += ", is_available, created_at"
		selectValues += ", 0, " + createdAt
	}
	insert += `) SELECT ` + selectValues + ` FROM ` + legacyTable
	if _, err := tx.ExecContext(ctx, insert); err != nil {
		return fmt.Errorf("copy normalized data from %q: %w", legacyTable, err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+legacyTable); err != nil {
		return fmt.Errorf("drop migrated table %q: %w", legacyTable, err)
	}
	return nil
}

func tableColumns(ctx context.Context, tx *sql.Tx, table string) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("inspect table %q: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]struct{})
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("read columns for %q: %w", table, err)
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns for %q: %w", table, err)
	}
	return columns, nil
}

// AddCompany inserts a company into the company table.
func (d *DB) AddCompany(ctx context.Context, name string) error {
	return d.AddCompanyForProvider(ctx, "greenhouse", name)
}

// AddCompanyForProvider saves a company slug for its selected job-board provider.
func (d *DB) AddCompanyForProvider(ctx context.Context, provider, name string) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	name = strings.TrimSpace(name)
	if provider == "" || name == "" {
		return errors.New("provider and company slug cannot be empty")
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if _, err := d.conn.ExecContext(ctx,
		"INSERT INTO provider_companies (provider, name) VALUES (?, ?) ON CONFLICT(provider, name) DO NOTHING",
		provider, name,
	); err != nil {
		return fmt.Errorf("add %s company %q: %w", provider, name, err)
	}
	return nil
}

// StartBruteCheckpoint retains compatibility with the original Greenhouse checkpoint API.
func (d *DB) StartBruteCheckpoint(ctx context.Context, minLength, maxLength int, alphabet string, total int64) (int64, error) {
	return d.StartProviderBruteCheckpoint(ctx, "greenhouse", minLength, maxLength, alphabet, total)
}

// StartProviderBruteCheckpoint returns the provider-specific scan position or creates it.
func (d *DB) StartProviderBruteCheckpoint(ctx context.Context, provider string, minLength, maxLength int, alphabet string, total int64) (int64, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var nextIndex, savedTotal int64
	err := d.conn.QueryRowContext(ctx, `
		SELECT next_index, total FROM brute_checkpoints
		WHERE provider = ? AND min_length = ? AND max_length = ? AND alphabet = ?
	`, provider, minLength, maxLength, alphabet).Scan(&nextIndex, &savedTotal)
	if err == nil {
		if savedTotal != total || nextIndex < 0 || nextIndex > total {
			return 0, fmt.Errorf("saved brute checkpoint is invalid (next=%d total=%d; expected total=%d)", nextIndex, savedTotal, total)
		}
		return nextIndex, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read brute checkpoint: %w", err)
	}

	if _, err := d.conn.ExecContext(ctx, `
		INSERT INTO brute_checkpoints (provider, min_length, max_length, alphabet, next_index, total)
		VALUES (?, ?, ?, ?, 0, ?)
	`, provider, minLength, maxLength, alphabet, total); err != nil {
		return 0, fmt.Errorf("create brute checkpoint: %w", err)
	}
	return 0, nil
}

// SaveBruteCheckpoint retains compatibility with the original Greenhouse checkpoint API.
func (d *DB) SaveBruteCheckpoint(ctx context.Context, minLength, maxLength int, alphabet string, nextIndex, total int64) error {
	return d.SaveProviderBruteCheckpoint(ctx, "greenhouse", minLength, maxLength, alphabet, nextIndex, total)
}

// SaveProviderBruteCheckpoint advances one provider's position after a completed batch.
func (d *DB) SaveProviderBruteCheckpoint(ctx context.Context, provider string, minLength, maxLength int, alphabet string, nextIndex, total int64) error {
	if nextIndex < 0 || nextIndex > total {
		return fmt.Errorf("invalid brute checkpoint position %d of %d", nextIndex, total)
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	result, err := d.conn.ExecContext(ctx, `
		UPDATE brute_checkpoints
		SET next_index = ?, total = ?, updated_at = CURRENT_TIMESTAMP
		WHERE provider = ? AND min_length = ? AND max_length = ? AND alphabet = ?
			AND next_index <= ? AND total = ? AND ? <= total
	`, nextIndex, total, provider, minLength, maxLength, alphabet, nextIndex, total, nextIndex)
	if err != nil {
		return fmt.Errorf("save brute checkpoint: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm brute checkpoint: %w", err)
	}
	if rows != 1 {
		return errors.New("brute checkpoint did not advance; stored state changed unexpectedly")
	}
	return nil
}

// JobUpdateCheckpoints returns companies whose job updates completed in an unfinished run.
func (d *DB) JobUpdateCheckpoints(ctx context.Context) ([]JobUpdateCheckpoint, error) {
	rows, err := d.conn.QueryContext(ctx, `
		SELECT provider, company_slug FROM job_update_checkpoints
	`)
	if err != nil {
		return nil, fmt.Errorf("read job update checkpoints: %w", err)
	}
	defer rows.Close()

	var checkpoints []JobUpdateCheckpoint
	for rows.Next() {
		var checkpoint JobUpdateCheckpoint
		if err := rows.Scan(&checkpoint.Provider, &checkpoint.CompanySlug); err != nil {
			return nil, fmt.Errorf("scan job update checkpoint: %w", err)
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job update checkpoints: %w", err)
	}
	return checkpoints, nil
}

type JobUpdateCheckpoint struct {
	Provider    string
	CompanySlug string
}

// SaveJobUpdateCheckpoint marks one company's job update complete.
func (d *DB) SaveJobUpdateCheckpoint(ctx context.Context, provider, companySlug string) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	companySlug = strings.TrimSpace(companySlug)
	if provider == "" || companySlug == "" {
		return errors.New("provider and company slug cannot be empty")
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if _, err := d.conn.ExecContext(ctx, `
		INSERT INTO job_update_checkpoints(provider, company_slug)
		VALUES (?, ?)
		ON CONFLICT(provider, company_slug) DO UPDATE SET completed_at = CURRENT_TIMESTAMP
	`, provider, companySlug); err != nil {
		return fmt.Errorf("save job update checkpoint for %s/%s: %w", provider, companySlug, err)
	}
	return nil
}

// ClearJobUpdateCheckpoints removes progress after the full update run succeeds.
func (d *DB) ClearJobUpdateCheckpoints(ctx context.Context) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if _, err := d.conn.ExecContext(ctx, "DELETE FROM job_update_checkpoints"); err != nil {
		return fmt.Errorf("clear job update checkpoints: %w", err)
	}
	return nil
}

var (
	ErrJobUpdateRunActive        = errors.New("a job update run is already active")
	ErrJobUpdateRunOwnershipLost = errors.New("job update run ownership was lost")
)

type JobUpdateTarget struct {
	Provider    string
	CompanySlug string
}

type JobUpdateRun struct {
	ID             string
	Status         string
	Resumed        bool
	OwnerToken     string
	FencingToken   int64
	TargetCount    int
	CompletedCount int
	PendingCount   int
	FailedCount    int
	StopProvider   string
	StopStatus     int
	StopReason     string
	RetryAfterAt   time.Time
}

type JobUpdateRetryNotBeforeError struct {
	RetryAt time.Time
}

func (e *JobUpdateRetryNotBeforeError) Error() string {
	return fmt.Sprintf("job update is paused until %s", e.RetryAt.Format(time.RFC3339))
}

// BeginOrResumeJobUpdateRun creates a fixed target snapshot or takes over an
// interrupted run after its database-backed owner lease expires.
func (d *DB) BeginOrResumeJobUpdateRun(ctx context.Context, targets []JobUpdateTarget) (*JobUpdateRun, []JobUpdateTarget, error) {
	ownerToken, err := randomID()
	if err != nil {
		return nil, nil, fmt.Errorf("create job update owner token: %w", err)
	}

	normalized := make([]JobUpdateTarget, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		target.Provider = strings.ToLower(strings.TrimSpace(target.Provider))
		target.CompanySlug = strings.TrimSpace(target.CompanySlug)
		if target.Provider == "" || target.CompanySlug == "" {
			return nil, nil, errors.New("job update targets require a provider and company slug")
		}
		key := target.Provider + "\x00" + target.CompanySlug
		if _, ok := seen[key]; ok {
			return nil, nil, fmt.Errorf("duplicate job update target %s/%s", target.Provider, target.CompanySlug)
		}
		seen[key] = struct{}{}
		normalized[i] = target
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		if d.activeJobUpdateRunExists(ctx) {
			return nil, nil, ErrJobUpdateRunActive
		}
		return nil, nil, fmt.Errorf("begin job update run: %w", err)
	}
	defer tx.Rollback()

	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT CAST(strftime('%s', 'now') AS INTEGER)").Scan(&now); err != nil {
		return nil, nil, fmt.Errorf("read database time for job update run: %w", err)
	}

	var runID, status string
	var currentFence, leaseExpiresAt, retryAfterAt int64
	resumed := false
	err = tx.QueryRowContext(ctx, `
		SELECT run_id, status, fencing_token, lease_expires_at, retry_after_at
		FROM job_update_runs WHERE active_slot = 1
	`).Scan(&runID, &status, &currentFence, &leaseExpiresAt, &retryAfterAt)
	switch {
	case err == nil:
		resumed = true
		if status == "running" && leaseExpiresAt > now {
			return nil, nil, ErrJobUpdateRunActive
		}
		if retryAfterAt > now {
			return nil, nil, &JobUpdateRetryNotBeforeError{RetryAt: time.Unix(retryAfterAt, 0).UTC()}
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE job_update_runs
			SET status = 'running', owner_token = ?, fencing_token = fencing_token + 1,
				lease_expires_at = ? + 60, updated_at = CURRENT_TIMESTAMP
			WHERE run_id = ? AND active_slot = 1 AND fencing_token = ?
		`, ownerToken, now, runID, currentFence)
		if err != nil {
			return nil, nil, fmt.Errorf("take over interrupted job update run: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return nil, nil, fmt.Errorf("confirm job update run takeover: %w", err)
		}
		if changed != 1 {
			return nil, nil, ErrJobUpdateRunActive
		}
	case errors.Is(err, sql.ErrNoRows):
		runID, err = randomID()
		if err != nil {
			return nil, nil, fmt.Errorf("create job update run ID: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM job_update_targets
			WHERE run_id IN (SELECT run_id FROM job_update_runs WHERE status = 'completed')
		`); err != nil {
			return nil, nil, fmt.Errorf("remove old completed job update targets: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM job_update_runs WHERE status = 'completed'"); err != nil {
			return nil, nil, fmt.Errorf("remove old completed job update runs: %w", err)
		}
		legacyDone, err := readLegacyJobUpdateCheckpoints(ctx, tx)
		if err != nil {
			return nil, nil, err
		}
		resumed = len(legacyDone) > 0
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO job_update_runs (
				run_id, active_slot, status, target_count, owner_token,
				fencing_token, lease_expires_at
			) VALUES (?, 1, 'running', ?, ?, 1, ? + 60)
		`, runID, len(normalized), ownerToken, now); err != nil {
			tx.Rollback()
			if d.activeJobUpdateRunExists(ctx) {
				return nil, nil, ErrJobUpdateRunActive
			}
			return nil, nil, fmt.Errorf("create job update run: %w", err)
		}
		for ordinal, target := range normalized {
			targetStatus := "pending"
			if _, ok := legacyDone[target.Provider+"\x00"+target.CompanySlug]; ok {
				targetStatus = "completed"
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO job_update_targets(run_id, ordinal, provider, company_slug, status)
				VALUES (?, ?, ?, ?, ?)
			`, runID, ordinal, target.Provider, target.CompanySlug, targetStatus); err != nil {
				return nil, nil, fmt.Errorf("save job update target %s/%s: %w", target.Provider, target.CompanySlug, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM job_update_checkpoints"); err != nil {
			return nil, nil, fmt.Errorf("retire legacy job update checkpoints: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("read active job update run: %w", err)
	}

	run, err := readJobUpdateRun(ctx, tx, runID)
	if err != nil {
		return nil, nil, err
	}
	run.Resumed = resumed
	pending, err := readPendingJobUpdateTargets(ctx, tx, runID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit job update run ownership: %w", err)
	}
	return run, pending, nil
}

func (d *DB) activeJobUpdateRunExists(ctx context.Context) bool {
	var active int
	err := d.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM job_update_runs WHERE active_slot = 1
	`).Scan(&active)
	return err == nil && active > 0
}

// JobUpdateRunProgress returns counts derived from durable run and target state.
func (d *DB) JobUpdateRunProgress(ctx context.Context, runID string) (*JobUpdateRun, error) {
	tx, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin job update progress read: %w", err)
	}
	defer tx.Rollback()
	run, err := readJobUpdateRun(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("finish job update progress read: %w", err)
	}
	return run, nil
}

func readLegacyJobUpdateCheckpoints(ctx context.Context, tx *sql.Tx) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx, "SELECT provider, company_slug FROM job_update_checkpoints")
	if err != nil {
		return nil, fmt.Errorf("read legacy job update checkpoints: %w", err)
	}
	defer rows.Close()

	done := make(map[string]struct{})
	for rows.Next() {
		var provider, slug string
		if err := rows.Scan(&provider, &slug); err != nil {
			return nil, fmt.Errorf("scan legacy job update checkpoint: %w", err)
		}
		done[strings.ToLower(strings.TrimSpace(provider))+"\x00"+strings.TrimSpace(slug)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy job update checkpoints: %w", err)
	}
	return done, nil
}

func readJobUpdateRun(ctx context.Context, tx *sql.Tx, runID string) (*JobUpdateRun, error) {
	run := &JobUpdateRun{ID: runID}
	var retryAfterAt int64
	if err := tx.QueryRowContext(ctx, `
		SELECT status, owner_token, fencing_token, target_count, stop_provider,
			stop_status, stop_reason, retry_after_at
		FROM job_update_runs WHERE run_id = ?
	`, runID).Scan(
		&run.Status, &run.OwnerToken, &run.FencingToken, &run.TargetCount,
		&run.StopProvider, &run.StopStatus, &run.StopReason, &retryAfterAt,
	); err != nil {
		return nil, fmt.Errorf("read job update run %q: %w", runID, err)
	}
	if retryAfterAt > 0 {
		run.RetryAfterAt = time.Unix(retryAfterAt, 0).UTC()
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'pending' AND last_error <> '' THEN 1 ELSE 0 END), 0)
		FROM job_update_targets WHERE run_id = ?
	`, runID).Scan(&run.CompletedCount, &run.PendingCount, &run.FailedCount); err != nil {
		return nil, fmt.Errorf("read job update run progress: %w", err)
	}
	return run, nil
}

func readPendingJobUpdateTargets(ctx context.Context, tx *sql.Tx, runID string) ([]JobUpdateTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT provider, company_slug FROM job_update_targets
		WHERE run_id = ? AND status = 'pending' ORDER BY ordinal
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("read pending job update targets: %w", err)
	}
	defer rows.Close()

	var pending []JobUpdateTarget
	for rows.Next() {
		var target JobUpdateTarget
		if err := rows.Scan(&target.Provider, &target.CompanySlug); err != nil {
			return nil, fmt.Errorf("scan pending job update target: %w", err)
		}
		pending = append(pending, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending job update targets: %w", err)
	}
	return pending, nil
}

func randomID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

// RenewJobUpdateRunLease extends ownership using database time.
func (d *DB) RenewJobUpdateRunLease(ctx context.Context, run *JobUpdateRun) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	result, err := d.conn.ExecContext(ctx, `
		UPDATE job_update_runs
		SET lease_expires_at = CAST(strftime('%s', 'now') AS INTEGER) + 60,
			updated_at = CURRENT_TIMESTAMP
		WHERE run_id = ? AND status = 'running' AND owner_token = ?
			AND fencing_token = ?
			AND lease_expires_at > CAST(strftime('%s', 'now') AS INTEGER)
	`, run.ID, run.OwnerToken, run.FencingToken)
	if err != nil {
		return fmt.Errorf("renew job update run lease: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm job update run lease: %w", err)
	}
	if changed != 1 {
		return ErrJobUpdateRunOwnershipLost
	}
	return nil
}

// RecordJobUpdateTargetFailure retains a safe diagnostic for a pending target.
func (d *DB) RecordJobUpdateTargetFailure(ctx context.Context, run *JobUpdateRun, target JobUpdateTarget, category string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	result, err := d.conn.ExecContext(ctx, `
		UPDATE job_update_targets
		SET last_error = ?
		WHERE run_id = ? AND provider = ? AND company_slug = ? AND status = 'pending'
			AND EXISTS (
				SELECT 1 FROM job_update_runs
				WHERE run_id = ? AND status = 'running' AND owner_token = ? AND fencing_token = ?
					AND lease_expires_at > CAST(strftime('%s', 'now') AS INTEGER)
			)
	`, category, run.ID, target.Provider, target.CompanySlug, run.ID, run.OwnerToken, run.FencingToken)
	if err != nil {
		return fmt.Errorf("record failed job update target %s/%s: %w", target.Provider, target.CompanySlug, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm failed job update target: %w", err)
	}
	if changed != 1 {
		return ErrJobUpdateRunOwnershipLost
	}
	return nil
}

// ReplaceProviderJobsAndCompleteTarget commits refreshed jobs and target state atomically.
func (d *DB) ReplaceProviderJobsAndCompleteTarget(ctx context.Context, run *JobUpdateRun, target JobUpdateTarget, jobs []Job) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin atomic %s job update for %q: %w", target.Provider, target.CompanySlug, err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		UPDATE job_update_runs
		SET lease_expires_at = CAST(strftime('%s', 'now') AS INTEGER) + 60,
			updated_at = CURRENT_TIMESTAMP
		WHERE run_id = ? AND status = 'running' AND owner_token = ? AND fencing_token = ?
			AND lease_expires_at > CAST(strftime('%s', 'now') AS INTEGER)
	`, run.ID, run.OwnerToken, run.FencingToken)
	if err != nil {
		return fmt.Errorf("verify job update run ownership: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm job update run ownership: %w", err)
	}
	if changed != 1 {
		return ErrJobUpdateRunOwnershipLost
	}

	if err := replaceProviderJobsTx(ctx, tx, target.Provider, target.CompanySlug, jobs); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE job_update_targets
		SET status = 'completed', last_error = '', completed_at = CURRENT_TIMESTAMP
		WHERE run_id = ? AND provider = ? AND company_slug = ? AND status = 'pending'
	`, run.ID, target.Provider, target.CompanySlug)
	if err != nil {
		return fmt.Errorf("complete job update target %s/%s: %w", target.Provider, target.CompanySlug, err)
	}
	changed, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm job update target completion: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("job update target %s/%s is not pending", target.Provider, target.CompanySlug)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit atomic %s job update for %q: %w", target.Provider, target.CompanySlug, err)
	}
	return nil
}

// PauseJobUpdateRun releases ownership while retaining the resumable snapshot.
func (d *DB) PauseJobUpdateRun(ctx context.Context, run *JobUpdateRun, stopProvider string, stopStatus int, stopReason string, retryAfter time.Time) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	retryAt := int64(0)
	if !retryAfter.IsZero() {
		retryAt = retryAfter.UTC().Unix()
	}
	result, err := d.conn.ExecContext(ctx, `
		UPDATE job_update_runs
		SET status = 'paused', owner_token = '', lease_expires_at = 0,
			stop_provider = ?, stop_status = ?, stop_reason = ?, retry_after_at = ?,
			last_error = ?, updated_at = CURRENT_TIMESTAMP
		WHERE run_id = ? AND status = 'running' AND owner_token = ? AND fencing_token = ?
			AND lease_expires_at > CAST(strftime('%s', 'now') AS INTEGER)
	`, stopProvider, stopStatus, stopReason, retryAt, stopReason, run.ID, run.OwnerToken, run.FencingToken)
	if err != nil {
		return fmt.Errorf("pause job update run: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm job update run pause: %w", err)
	}
	if changed != 1 {
		return ErrJobUpdateRunOwnershipLost
	}
	return nil
}

// CompleteJobUpdateRun retires the active snapshot after every target commits.
func (d *DB) CompleteJobUpdateRun(ctx context.Context, run *JobUpdateRun) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job update run completion: %w", err)
	}
	defer tx.Rollback()
	var pending int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM job_update_targets
		WHERE run_id = ? AND status <> 'completed'
	`, run.ID).Scan(&pending); err != nil {
		return fmt.Errorf("check pending job update targets: %w", err)
	}
	if pending != 0 {
		return fmt.Errorf("cannot complete job update run with %d pending targets", pending)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE job_update_runs
		SET status = 'completed', active_slot = NULL, owner_token = '',
			lease_expires_at = 0, updated_at = CURRENT_TIMESTAMP
		WHERE run_id = ? AND status = 'running' AND owner_token = ? AND fencing_token = ?
			AND lease_expires_at > CAST(strftime('%s', 'now') AS INTEGER)
	`, run.ID, run.OwnerToken, run.FencingToken)
	if err != nil {
		return fmt.Errorf("complete job update run: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm job update run completion: %w", err)
	}
	if changed != 1 {
		return ErrJobUpdateRunOwnershipLost
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM job_update_targets WHERE run_id = ?", run.ID); err != nil {
		return fmt.Errorf("retire completed job update targets: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job update run completion: %w", err)
	}
	return nil
}

// Companies returns all known company slugs.
func (d *DB) Companies(ctx context.Context) ([]string, error) {
	return d.CompaniesForProvider(ctx, "greenhouse")
}

// CompaniesForProvider returns the stored slugs for a provider.
func (d *DB) CompaniesForProvider(ctx context.Context, provider string) ([]string, error) {
	rows, err := d.conn.QueryContext(ctx,
		"SELECT name FROM provider_companies WHERE provider = ? ORDER BY name", provider)
	if err != nil {
		return nil, fmt.Errorf("list %s companies: %w", provider, err)
	}
	defer rows.Close()

	var companies []string
	for rows.Next() {
		var slug string
		if err := rows.Scan(&slug); err != nil {
			return nil, fmt.Errorf("scan %s company: %w", provider, err)
		}
		companies = append(companies, slug)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s companies: %w", provider, err)
	}
	return companies, nil
}

// Job is the provider-neutral listing model persisted and returned by the DB.
type Job struct {
	Provider       string
	CompanySlug    string
	ID             int64
	JobKey         string
	IsBookmarked   bool
	IsAvailable    bool
	Title          string
	CompanyName    string
	LocationName   string
	LocationJSON   string
	DetailsURL     string
	ApplyURL       string
	UpdatedAt      string
	FirstPublished string
	EmploymentType string
	WorkplaceType  string
	Department     string
	Education      string
	Experience     string
	Description    string
	AttributesJSON string
}

type JobFilters struct {
	Query               string
	Location            string
	PostedAfter         string
	PostedBefore        string
	EmploymentType      string
	WorkplaceType       string
	IncludeDescriptions bool
}

// ReplaceCompanyJobs atomically replaces one company's indexed jobs.
func (d *DB) ReplaceCompanyJobs(ctx context.Context, slug string, jobs []Job) error {
	return d.ReplaceProviderJobs(ctx, "greenhouse", slug, jobs)
}

// ReplaceProviderJobs atomically replaces a company's indexed jobs and refreshes saved snapshots.
func (d *DB) ReplaceProviderJobs(ctx context.Context, provider, slug string, jobs []Job) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s job update for %q: %w", provider, slug, err)
	}
	defer tx.Rollback()

	if err := replaceProviderJobsTx(ctx, tx, provider, slug, jobs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s job update for %q: %w", provider, slug, err)
	}
	return nil
}

func replaceProviderJobsTx(ctx context.Context, tx *sql.Tx, provider, slug string, jobs []Job) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM indexed_jobs WHERE provider = ? AND company_slug = ?", provider, slug); err != nil {
		return fmt.Errorf("remove old %s jobs for %q: %w", provider, slug, err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO indexed_jobs (
			provider, company_slug, job_id, title, company_name, location_name,
			location_json, details_url, apply_url, updated_at, first_published,
			employment_type, workplace_type, department, education, experience,
			description, attributes_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare %s job update for %q: %w", provider, slug, err)
	}
	defer stmt.Close()

	for _, job := range jobs {
		jobID := job.JobKey
		if jobID == "" {
			jobID = strconv.FormatInt(job.ID, 10)
		}
		if jobID == "" || jobID == "0" {
			return fmt.Errorf("job for %s company %q has an empty id", provider, slug)
		}
		if _, err := stmt.ExecContext(
			ctx, provider, slug, jobID, job.Title, job.CompanyName,
			job.LocationName, job.LocationJSON, job.DetailsURL, job.ApplyURL,
			job.UpdatedAt, job.FirstPublished, job.EmploymentType, job.WorkplaceType,
			job.Department, job.Education, job.Experience, job.Description, job.AttributesJSON,
		); err != nil {
			return fmt.Errorf("save job %s for %s company %q: %w", jobID, provider, slug, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE indexed_bookmarks SET is_available = 0
		WHERE provider = ? AND company_slug = ?
	`, provider, slug); err != nil {
		return fmt.Errorf("mark removed %s bookmarks unavailable for %q: %w", provider, slug, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE indexed_bookmarks
		SET title = (SELECT title FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			company_name = (SELECT company_name FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			location_name = (SELECT location_name FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			location_json = (SELECT location_json FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			details_url = (SELECT details_url FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			apply_url = (SELECT apply_url FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			updated_at = (SELECT updated_at FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			first_published = (SELECT first_published FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			employment_type = (SELECT employment_type FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			workplace_type = (SELECT workplace_type FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			department = (SELECT department FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			education = (SELECT education FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			experience = (SELECT experience FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			description = (SELECT description FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			attributes_json = (SELECT attributes_json FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id),
			is_available = 1
		WHERE provider = ? AND company_slug = ?
			AND EXISTS (SELECT 1 FROM indexed_jobs j WHERE j.provider = indexed_bookmarks.provider AND j.company_slug = indexed_bookmarks.company_slug AND j.job_id = indexed_bookmarks.job_id)
	`, provider, slug); err != nil {
		return fmt.Errorf("refresh saved %s job snapshots for %q: %w", provider, slug, err)
	}

	return nil
}

// ToggleBookmark saves or removes a job and returns its new saved state.
func (d *DB) ToggleBookmark(ctx context.Context, companySlug string, jobID int64) (bool, error) {
	return d.ToggleProviderBookmark(ctx, "greenhouse", companySlug, strconv.FormatInt(jobID, 10))
}

// ToggleProviderBookmark saves or removes a provider job and returns its new state.
func (d *DB) ToggleProviderBookmark(ctx context.Context, provider, companySlug, jobID string) (bool, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var exists int
	err := d.conn.QueryRowContext(ctx,
		"SELECT 1 FROM indexed_bookmarks WHERE provider = ? AND company_slug = ? AND job_id = ?",
		provider, companySlug, jobID,
	).Scan(&exists)
	if err == nil {
		if _, err := d.conn.ExecContext(ctx,
			"DELETE FROM indexed_bookmarks WHERE provider = ? AND company_slug = ? AND job_id = ?",
			provider, companySlug, jobID,
		); err != nil {
			return false, fmt.Errorf("remove %s bookmark for %q job %s: %w", provider, companySlug, jobID, err)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check %s bookmark for %q job %s: %w", provider, companySlug, jobID, err)
	}

	result, err := d.conn.ExecContext(ctx, `
		INSERT INTO indexed_bookmarks (
			provider, company_slug, job_id, title, company_name, location_name,
			location_json, details_url, apply_url, updated_at, first_published,
			employment_type, workplace_type, department, education, experience,
			description, attributes_json, is_available
		)
		SELECT provider, company_slug, job_id, title, company_name, location_name,
			location_json, details_url, apply_url, updated_at, first_published,
			employment_type, workplace_type, department, education, experience,
			description, attributes_json, 1
		FROM indexed_jobs
		WHERE provider = ? AND company_slug = ? AND job_id = ?
	`, provider, companySlug, jobID)
	if err != nil {
		return false, fmt.Errorf("save %s bookmark for %q job %s: %w", provider, companySlug, jobID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("confirm %s bookmark for %q job %s: %w", provider, companySlug, jobID, err)
	}
	if rows == 0 {
		return false, fmt.Errorf("%s job %s for %q was not found", provider, jobID, companySlug)
	}
	return true, nil
}

// BookmarkCount returns the number of saved jobs across providers.
func (d *DB) BookmarkCount(ctx context.Context) (int, error) {
	var count int
	if err := d.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM indexed_bookmarks").Scan(&count); err != nil {
		return 0, fmt.Errorf("count bookmarks: %w", err)
	}
	return count, nil
}

// ProviderBookmarkCount returns the number of saved jobs for a provider.
func (d *DB) ProviderBookmarkCount(ctx context.Context, provider string) (int, error) {
	var count int
	if err := d.conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM indexed_bookmarks WHERE provider = ?", provider,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s bookmarks: %w", provider, err)
	}
	return count, nil
}

// ListJobs searches and paginates across all providers.
func (d *DB) ListJobs(ctx context.Context, filters JobFilters, savedOnly bool, limit, offset int) ([]Job, int, error) {
	return d.ListProviderJobs(ctx, "", filters, savedOnly, limit, offset)
}

// ListProviderJobs optionally limits results to one provider; empty means all providers.
func (d *DB) ListProviderJobs(ctx context.Context, provider string, filters JobFilters, savedOnly bool, limit, offset int) ([]Job, int, error) {
	conditions := []string{"1 = 1"}
	args := make([]any, 0)
	if provider != "" {
		conditions = append(conditions, "j.provider = ?")
		args = append(args, provider)
	}
	if filters.Query != "" {
		pattern := "%" + filters.Query + "%"
		search := "(j.title LIKE ? OR j.company_name LIKE ? OR j.location_name LIKE ?"
		args = append(args, pattern, pattern, pattern)
		if filters.IncludeDescriptions {
			var contentTerms []string
			for _, term := range jobSearchTerms.FindAllString(strings.ToLower(filters.Query), 8) {
				contentTerms = append(contentTerms, "(LOWER(j.description) GLOB ? OR LOWER(j.description) GLOB ? OR LOWER(j.description) GLOB ? OR LOWER(j.description) GLOB ?)")
				args = append(args,
					"*[^a-z0-9]"+term+"[^a-z0-9]*",
					term+"[^a-z0-9]*",
					"*[^a-z0-9]"+term,
					term,
				)
			}
			if len(contentTerms) > 0 {
				search += " OR (" + strings.Join(contentTerms, " AND ") + ")"
			}
		}
		conditions = append(conditions, search+")")
	}
	if filters.Location != "" {
		conditions = append(conditions, "j.location_name LIKE ?")
		args = append(args, "%"+filters.Location+"%")
	}
	if filters.PostedAfter != "" {
		conditions = append(conditions, "j.first_published >= ?")
		args = append(args, filters.PostedAfter)
	}
	if filters.PostedBefore != "" {
		conditions = append(conditions, "j.first_published < date(?, '+1 day')")
		args = append(args, filters.PostedBefore)
	}
	if filters.EmploymentType != "" {
		conditions = append(conditions, "LOWER(j.employment_type) LIKE ?")
		args = append(args, "%"+strings.ToLower(filters.EmploymentType)+"%")
	}
	if filters.WorkplaceType != "" {
		conditions = append(conditions, "LOWER(j.workplace_type) LIKE ?")
		args = append(args, "%"+strings.ToLower(filters.WorkplaceType)+"%")
	}
	from := "indexed_jobs j"
	selection := `j.provider, j.company_slug, j.job_id,
		EXISTS (SELECT 1 FROM indexed_bookmarks b
			WHERE b.provider = j.provider AND b.company_slug = j.company_slug AND b.job_id = j.job_id),
		1, j.title, j.company_name, j.location_name, j.location_json, j.details_url,
		j.apply_url, j.updated_at, j.first_published, j.employment_type,
		j.workplace_type, j.department, j.education, j.experience, j.description, j.attributes_json`
	if savedOnly {
		from = "indexed_bookmarks j"
		selection = `j.provider, j.company_slug, j.job_id, 1, j.is_available, j.title,
			j.company_name, j.location_name, j.location_json, j.details_url, j.apply_url,
			j.updated_at, j.first_published, j.employment_type, j.workplace_type,
			j.department, j.education, j.experience, j.description, j.attributes_json`
	}
	where := strings.Join(conditions, " AND ")

	var total int
	countArgs := append([]any(nil), args...)
	err := d.conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM `+from+`
		WHERE `+where,
		countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	listArgs := append([]any(nil), args...)
	listArgs = append(listArgs, limit, offset)
	rows, err := d.conn.QueryContext(ctx, `
		SELECT `+selection+`
		FROM `+from+`
		WHERE `+where+`
		ORDER BY j.first_published DESC, j.title COLLATE NOCASE
		LIMIT ? OFFSET ?
	`, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		var jobID string
		if err := rows.Scan(
			&job.Provider, &job.CompanySlug, &jobID, &job.IsBookmarked, &job.IsAvailable,
			&job.Title, &job.CompanyName, &job.LocationName, &job.LocationJSON,
			&job.DetailsURL, &job.ApplyURL, &job.UpdatedAt, &job.FirstPublished,
			&job.EmploymentType, &job.WorkplaceType, &job.Department, &job.Education,
			&job.Experience, &job.Description, &job.AttributesJSON,
		); err != nil {
			return nil, 0, fmt.Errorf("scan job: %w", err)
		}
		job.JobKey = jobID
		job.ID, _ = strconv.ParseInt(jobID, 10, 64)
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, total, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.conn.Close()
}
