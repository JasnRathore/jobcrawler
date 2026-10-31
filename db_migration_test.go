package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"db"
)

func TestLegacyGreenhouseTablesMigrateAndAreDropped(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("turso", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`
		CREATE TABLE company (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
		CREATE TABLE greenhouse_jobs (
			company_slug TEXT NOT NULL, id INTEGER NOT NULL, internal_job_id INTEGER,
			title TEXT NOT NULL, company_name TEXT NOT NULL, location_name TEXT NOT NULL,
			location_json TEXT NOT NULL, absolute_url TEXT NOT NULL, updated_at TEXT NOT NULL,
			first_published TEXT NOT NULL, requisition_id TEXT NOT NULL, language TEXT NOT NULL,
			application_deadline TEXT NOT NULL, metadata TEXT NOT NULL,
			data_compliance TEXT NOT NULL, content TEXT NOT NULL, raw_json TEXT NOT NULL,
			PRIMARY KEY(company_slug, id)
		);
		CREATE TABLE greenhouse_bookmarks (
			company_slug TEXT NOT NULL, job_id INTEGER NOT NULL, title TEXT NOT NULL,
			company_name TEXT NOT NULL, location_name TEXT NOT NULL, absolute_url TEXT NOT NULL,
			updated_at TEXT NOT NULL, first_published TEXT NOT NULL,
			requisition_id TEXT NOT NULL, content TEXT NOT NULL, raw_json TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(company_slug, job_id)
		);
		CREATE TABLE indexed_jobs (
			provider TEXT NOT NULL, company_slug TEXT NOT NULL, job_id TEXT NOT NULL,
			title TEXT NOT NULL, company_name TEXT NOT NULL, location_name TEXT NOT NULL,
			location_json TEXT NOT NULL, absolute_url TEXT NOT NULL, updated_at TEXT NOT NULL,
			first_published TEXT NOT NULL, requisition_id TEXT NOT NULL, language TEXT NOT NULL,
			application_deadline TEXT NOT NULL, metadata TEXT NOT NULL,
			data_compliance TEXT NOT NULL, content TEXT NOT NULL, raw_json TEXT NOT NULL,
			PRIMARY KEY(provider, company_slug, job_id)
		);
		CREATE TABLE indexed_bookmarks (
			provider TEXT NOT NULL, company_slug TEXT NOT NULL, job_id TEXT NOT NULL,
			title TEXT NOT NULL, company_name TEXT NOT NULL, location_name TEXT NOT NULL,
			location_json TEXT NOT NULL, absolute_url TEXT NOT NULL, updated_at TEXT NOT NULL,
			first_published TEXT NOT NULL, requisition_id TEXT NOT NULL, language TEXT NOT NULL,
			application_deadline TEXT NOT NULL, metadata TEXT NOT NULL,
			data_compliance TEXT NOT NULL, content TEXT NOT NULL, raw_json TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(provider, company_slug, job_id)
		);
		CREATE TABLE provider_companies (
			provider TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY(provider, name)
		);
		CREATE TABLE brute_checkpoints (
			min_length INTEGER NOT NULL, max_length INTEGER NOT NULL, alphabet TEXT NOT NULL,
			next_index INTEGER NOT NULL, total INTEGER NOT NULL,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(min_length, max_length, alphabet)
		);
		INSERT INTO company(name) VALUES ('example');
		INSERT INTO brute_checkpoints(min_length, max_length, alphabet, next_index, total)
		VALUES (1, 1, 'abcdefghijklmnopqrstuvwxyz', 4, 26);
		INSERT INTO greenhouse_jobs VALUES (
			'example', 42, NULL, 'Research Intern', 'Example', 'Remote', '{"name":"Remote"}',
			'https://jobs.example.test/42', '', '2026-10-15', '', '', '', '{"team":"Research"}',
			'[]', 'Build research tools', '{}'
		);
		INSERT INTO greenhouse_bookmarks VALUES (
			'example', 42, 'Research Intern', 'Example', 'Remote',
			'https://jobs.example.test/42', '', '2026-10-15', '', 'Build research tools',
			'{}', '2026-10-16 12:00:00'
		);
		INSERT INTO greenhouse_bookmarks VALUES (
			'example', 99, 'Old Listing', 'Example', 'Boston',
			'https://jobs.example.test/99', '', '2026-09-01', '', 'Old description',
			'{}', '2026-09-02 12:00:00'
		);
		INSERT INTO indexed_jobs VALUES (
			'lever', 'leverco', 'lever-1', 'Research Analyst', 'Lever Example', 'Remote',
			'{}', 'https://jobs.example.test/lever-1', '', '2026-10-15', '', '', '',
			'{"team":"Research"}', '[]', 'Analyze data', '{}'
		);
		INSERT INTO indexed_bookmarks VALUES (
			'lever', 'leverco', 'lever-1', 'Research Analyst', 'Lever Example', 'Remote',
			'{}', 'https://jobs.example.test/lever-1', '', '2026-10-15', '', '', '',
			'{"team":"Research"}', '[]', 'Analyze data', '{}', '2026-10-16 12:00:00'
		);
	`)
	if err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 1, alphabet, 26)
	if err != nil || checkpoint != 4 {
		database.Close()
		t.Fatalf("migrated Greenhouse checkpoint=%d err=%v, want 4", checkpoint, err)
	}
	checkpoint, err = database.StartProviderBruteCheckpoint(ctx, providerLever, 1, 1, alphabet, 26)
	if err != nil || checkpoint != 0 {
		database.Close()
		t.Fatalf("new Lever checkpoint=%d err=%v, want 0", checkpoint, err)
	}
	jobs, total, err := database.ListJobs(ctx, db.JobFilters{}, false, 10, 0)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if total != 2 || len(jobs) != 2 {
		database.Close()
		t.Fatalf("migrated active jobs=%+v total=%d", jobs, total)
	}
	foundMigratedSource := false
	for _, job := range jobs {
		if job.Provider == "lever" && job.JobKey == "lever-1" &&
			job.DetailsURL == "https://jobs.example.test/lever-1" && job.Description == "Analyze data" {
			foundMigratedSource = true
		}
	}
	if !foundMigratedSource {
		database.Close()
		t.Fatalf("migrated indexed provider listing missing: %+v", jobs)
	}
	saved, savedTotal, err := database.ListJobs(ctx, db.JobFilters{}, true, 10, 0)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if savedTotal != 3 || len(saved) != 3 {
		database.Close()
		t.Fatalf("migrated saved jobs=%+v total=%d; want three", saved, savedTotal)
	}
	availability := map[string]bool{}
	for _, job := range saved {
		availability[job.JobKey] = job.IsAvailable
	}
	if !availability["42"] || availability["99"] || !availability["lever-1"] {
		database.Close()
		t.Fatalf("bookmark availability after migration=%v", availability)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	check, err := sql.Open("turso", path)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var remaining int
	err = check.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name IN ('company', 'greenhouse_jobs', 'greenhouse_bookmarks')
	`).Scan(&remaining)
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("legacy Greenhouse tables remaining after migration: %d", remaining)
	}
}
