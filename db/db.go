package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

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

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
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
		conn.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	database := &DB{conn: conn}
	if err := database.migrateLegacySchema(ctx); err != nil {
		conn.Close()
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
		conn.Close()
		return nil, fmt.Errorf("create job search indexes: %w", err)
	}
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s job update for %q: %w", provider, slug, err)
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
