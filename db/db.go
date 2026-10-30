package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
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
		CREATE TABLE IF NOT EXISTS company (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE
		);
		CREATE TABLE IF NOT EXISTS greenhouse_jobs (
			company_slug TEXT NOT NULL,
			id INTEGER NOT NULL,
			internal_job_id INTEGER,
			title TEXT NOT NULL,
			company_name TEXT NOT NULL,
			location_name TEXT NOT NULL,
			location_json TEXT NOT NULL,
			absolute_url TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			first_published TEXT NOT NULL,
			requisition_id TEXT NOT NULL,
			language TEXT NOT NULL,
			application_deadline TEXT NOT NULL,
			metadata TEXT NOT NULL,
			data_compliance TEXT NOT NULL,
			content TEXT NOT NULL,
			raw_json TEXT NOT NULL,
			PRIMARY KEY (company_slug, id)
		);
		CREATE INDEX IF NOT EXISTS greenhouse_jobs_title_idx ON greenhouse_jobs(title);
		CREATE INDEX IF NOT EXISTS greenhouse_jobs_company_idx ON greenhouse_jobs(company_slug);
		CREATE TABLE IF NOT EXISTS greenhouse_bookmarks (
			company_slug TEXT NOT NULL,
			job_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			company_name TEXT NOT NULL,
			location_name TEXT NOT NULL,
			absolute_url TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			first_published TEXT NOT NULL,
			requisition_id TEXT NOT NULL,
			content TEXT NOT NULL,
			raw_json TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (company_slug, job_id)
		);
		CREATE TABLE IF NOT EXISTS brute_checkpoints (
			min_length INTEGER NOT NULL,
			max_length INTEGER NOT NULL,
			alphabet TEXT NOT NULL,
			next_index INTEGER NOT NULL,
			total INTEGER NOT NULL,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (min_length, max_length, alphabet)
		);
	`

	if _, err := conn.ExecContext(ctx, schema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	return &DB{conn: conn}, nil
}

// AddCompany inserts a company into the company table.
func (d *DB) AddCompany(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("company name cannot be empty")
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_, err := d.conn.ExecContext(
		ctx,
		"INSERT INTO company (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
		name,
	)
	if err != nil {
		return fmt.Errorf("add company %q: %w", name, err)
	}

	return nil
}

// StartBruteCheckpoint returns the saved position or creates a new run checkpoint.
func (d *DB) StartBruteCheckpoint(ctx context.Context, minLength, maxLength int, alphabet string, total int64) (int64, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var nextIndex, savedTotal int64
	err := d.conn.QueryRowContext(ctx, `
		SELECT next_index, total FROM brute_checkpoints
		WHERE min_length = ? AND max_length = ? AND alphabet = ?
	`, minLength, maxLength, alphabet).Scan(&nextIndex, &savedTotal)
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
		INSERT INTO brute_checkpoints (min_length, max_length, alphabet, next_index, total)
		VALUES (?, ?, ?, 0, ?)
	`, minLength, maxLength, alphabet, total); err != nil {
		return 0, fmt.Errorf("create brute checkpoint: %w", err)
	}
	return 0, nil
}

// SaveBruteCheckpoint advances the next candidate position after a completed batch.
func (d *DB) SaveBruteCheckpoint(ctx context.Context, minLength, maxLength int, alphabet string, nextIndex, total int64) error {
	if nextIndex < 0 || nextIndex > total {
		return fmt.Errorf("invalid brute checkpoint position %d of %d", nextIndex, total)
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	result, err := d.conn.ExecContext(ctx, `
		UPDATE brute_checkpoints
		SET next_index = ?, total = ?, updated_at = CURRENT_TIMESTAMP
		WHERE min_length = ? AND max_length = ? AND alphabet = ?
			AND next_index <= ? AND total = ? AND ? <= total
	`, nextIndex, total, minLength, maxLength, alphabet, nextIndex, total, nextIndex)
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

// Companies returns all known company slugs.
func (d *DB) Companies(ctx context.Context) ([]string, error) {
	rows, err := d.conn.QueryContext(ctx, "SELECT name FROM company ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list companies: %w", err)
	}
	defer rows.Close()

	var companies []string
	for rows.Next() {
		var slug string
		if err := rows.Scan(&slug); err != nil {
			return nil, fmt.Errorf("scan company: %w", err)
		}
		companies = append(companies, slug)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate companies: %w", err)
	}
	return companies, nil
}

// GreenhouseJob contains searchable fields and the complete API job payload.
type GreenhouseJob struct {
	CompanySlug         string
	ID                  int64
	IsBookmarked        bool
	InternalJobID       *int64
	Title               string
	CompanyName         string
	LocationName        string
	LocationJSON        string
	AbsoluteURL         string
	UpdatedAt           string
	FirstPublished      string
	RequisitionID       string
	Language            string
	ApplicationDeadline string
	Metadata            string
	DataCompliance      string
	Content             string
	RawJSON             string
}

// ReplaceCompanyJobs atomically replaces one company's indexed jobs.
func (d *DB) ReplaceCompanyJobs(ctx context.Context, slug string, jobs []GreenhouseJob) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job update for %q: %w", slug, err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM greenhouse_jobs WHERE company_slug = ?", slug); err != nil {
		return fmt.Errorf("remove old jobs for %q: %w", slug, err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO greenhouse_jobs (
			company_slug, id, internal_job_id, title, company_name, location_name,
			location_json, absolute_url, updated_at, first_published, requisition_id,
			language, application_deadline, metadata, data_compliance, content, raw_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare job update for %q: %w", slug, err)
	}
	defer stmt.Close()

	for _, job := range jobs {
		if _, err := stmt.ExecContext(
			ctx, slug, job.ID, job.InternalJobID, job.Title, job.CompanyName,
			job.LocationName, job.LocationJSON, job.AbsoluteURL, job.UpdatedAt,
			job.FirstPublished, job.RequisitionID, job.Language, job.ApplicationDeadline,
			job.Metadata, job.DataCompliance, job.Content, job.RawJSON,
		); err != nil {
			return fmt.Errorf("save job %d for %q: %w", job.ID, slug, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE greenhouse_bookmarks
		SET title = (SELECT title FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			company_name = (SELECT company_name FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			location_name = (SELECT location_name FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			absolute_url = (SELECT absolute_url FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			updated_at = (SELECT updated_at FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			first_published = (SELECT first_published FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			requisition_id = (SELECT requisition_id FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			content = (SELECT content FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id),
			raw_json = (SELECT raw_json FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id)
		WHERE company_slug = ?
			AND EXISTS (SELECT 1 FROM greenhouse_jobs j WHERE j.company_slug = greenhouse_bookmarks.company_slug AND j.id = greenhouse_bookmarks.job_id)
	`, slug); err != nil {
		return fmt.Errorf("refresh saved job snapshots for %q: %w", slug, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job update for %q: %w", slug, err)
	}
	return nil
}

// ToggleBookmark saves or removes a job and returns its new saved state.
func (d *DB) ToggleBookmark(ctx context.Context, companySlug string, jobID int64) (bool, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var exists int
	err := d.conn.QueryRowContext(ctx,
		"SELECT 1 FROM greenhouse_bookmarks WHERE company_slug = ? AND job_id = ?",
		companySlug, jobID,
	).Scan(&exists)
	if err == nil {
		if _, err := d.conn.ExecContext(ctx,
			"DELETE FROM greenhouse_bookmarks WHERE company_slug = ? AND job_id = ?",
			companySlug, jobID,
		); err != nil {
			return false, fmt.Errorf("remove bookmark for %q job %d: %w", companySlug, jobID, err)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check bookmark for %q job %d: %w", companySlug, jobID, err)
	}

	result, err := d.conn.ExecContext(ctx, `
		INSERT INTO greenhouse_bookmarks (
			company_slug, job_id, title, company_name, location_name, absolute_url,
			updated_at, first_published, requisition_id, content, raw_json
		)
		SELECT company_slug, id, title, company_name, location_name, absolute_url,
			updated_at, first_published, requisition_id, content, raw_json
		FROM greenhouse_jobs
		WHERE company_slug = ? AND id = ?
	`, companySlug, jobID)
	if err != nil {
		return false, fmt.Errorf("save bookmark for %q job %d: %w", companySlug, jobID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("confirm bookmark for %q job %d: %w", companySlug, jobID, err)
	}
	if rows == 0 {
		return false, fmt.Errorf("job %d for %q was not found", jobID, companySlug)
	}
	return true, nil
}

// BookmarkCount returns the number of saved jobs.
func (d *DB) BookmarkCount(ctx context.Context) (int, error) {
	var count int
	if err := d.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM greenhouse_bookmarks").Scan(&count); err != nil {
		return 0, fmt.Errorf("count bookmarks: %w", err)
	}
	return count, nil
}

// ListJobs returns a page of matching jobs, searching descriptions only when requested.
func (d *DB) ListJobs(ctx context.Context, query string, includeDescriptions, savedOnly bool, limit, offset int) ([]GreenhouseJob, int, error) {
	pattern := "%" + query + "%"
	search := "title LIKE ?"
	args := []any{pattern}
	if includeDescriptions {
		search += " OR company_name LIKE ? OR company_slug LIKE ? OR location_name LIKE ?"
		args = append(args, pattern, pattern, pattern)
		var contentTerms []string
		for _, term := range jobSearchTerms.FindAllString(strings.ToLower(query), 8) {
			contentTerms = append(contentTerms, "(LOWER(content) GLOB ? OR LOWER(content) GLOB ? OR LOWER(content) GLOB ? OR LOWER(content) GLOB ?)")
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
	if savedOnly {
		search = "(" + search + ")"
	}

	var total int
	err := d.conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM `+jobSearchFrom(savedOnly)+`
		WHERE `+search, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count greenhouse jobs: %w", err)
	}

	listArgs := append([]any{}, args...)
	if !savedOnly {
		listArgs = append(listArgs, pattern)
	}
	listArgs = append(listArgs, limit, offset)
	rows, err := d.conn.QueryContext(ctx, `
		SELECT `+jobSelectColumns(savedOnly)+`
		FROM `+jobSearchFrom(savedOnly)+`
		WHERE `+search+`
		ORDER BY `+jobSearchOrder(savedOnly)+`
			first_published DESC, title COLLATE NOCASE
		LIMIT ? OFFSET ?
	`, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list greenhouse jobs: %w", err)
	}
	defer rows.Close()

	var jobs []GreenhouseJob
	for rows.Next() {
		var job GreenhouseJob
		var internalJobID sql.NullInt64
		if err := rows.Scan(
			&job.CompanySlug, &job.ID, &job.IsBookmarked, &internalJobID, &job.Title, &job.CompanyName,
			&job.LocationName, &job.LocationJSON, &job.AbsoluteURL, &job.UpdatedAt,
			&job.FirstPublished, &job.RequisitionID, &job.Language,
			&job.ApplicationDeadline, &job.Metadata, &job.DataCompliance,
			&job.Content, &job.RawJSON,
		); err != nil {
			return nil, 0, fmt.Errorf("scan greenhouse job: %w", err)
		}
		if internalJobID.Valid {
			job.InternalJobID = &internalJobID.Int64
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate greenhouse jobs: %w", err)
	}
	return jobs, total, nil
}

func jobSearchFrom(savedOnly bool) string {
	if savedOnly {
		return "greenhouse_bookmarks"
	}
	return "greenhouse_jobs"
}

func jobSearchColumns() string {
	return `company_slug, id, EXISTS (
		SELECT 1 FROM greenhouse_bookmarks b
		WHERE b.company_slug = greenhouse_jobs.company_slug AND b.job_id = greenhouse_jobs.id
	), internal_job_id, title, company_name, location_name, location_json, absolute_url,
		updated_at, first_published, requisition_id, language, application_deadline,
		metadata, data_compliance, content, raw_json`
}

func jobSelectColumns(savedOnly bool) string {
	if savedOnly {
		return `company_slug, job_id, 1, NULL, title, company_name, location_name,
			'', absolute_url, updated_at, first_published, requisition_id, '', '', '', '', content, raw_json`
	}
	return jobSearchColumns()
}

func jobSearchOrder(savedOnly bool) string {
	if savedOnly {
		return ""
	}
	return "CASE WHEN title LIKE ? THEN 0 ELSE 1 END, "
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.conn.Close()
}
