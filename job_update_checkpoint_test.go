package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"db"
)

func TestJobUpdateRunResumesOnlyIncompleteCompaniesFromSnapshot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "job-updates.db")
	database, err := db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	targets := []companyTarget{
		{provider: providerGreenhouse, slug: "first"},
		{provider: providerLever, slug: "second"},
	}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets))
	if err != nil {
		t.Fatal(err)
	}
	firstAttempts := make(map[companyTarget]int)
	err = updateJobTargets(ctx, database, run, pending, 1, func(_ context.Context, target companyTarget) ([]db.Job, error) {
		firstAttempts[target]++
		if target.provider == providerLever {
			return nil, errors.New("temporary timeout")
		}
		return []db.Job{testUpdateJob(target)}, nil
	})
	if err == nil {
		t.Fatal("first update run succeeded despite a failed company")
	}
	if firstAttempts[targets[0]] != 1 || firstAttempts[targets[1]] != 1 {
		t.Fatalf("first run attempts = %v, want one attempt per company", firstAttempts)
	}
	if err := database.PauseJobUpdateRun(ctx, run, "", 0, "provider request failed", time.Time{}); err != nil {
		t.Fatal(err)
	}
	progress, err := database.JobUpdateRunProgress(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.CompletedCount != 1 || progress.PendingCount != 1 || progress.FailedCount != 1 {
		t.Fatalf("partial progress = %+v, want 1 complete, 1 failed and pending", progress)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	expandedTargets := append(targets, companyTarget{provider: providerGreenhouse, slug: "added-after-snapshot"})
	resumedRun, retryTargets, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(expandedTargets))
	if err != nil {
		t.Fatal(err)
	}
	if !resumedRun.Resumed || resumedRun.ID != run.ID {
		t.Fatalf("resumed run = %+v, want existing run %s", resumedRun, run.ID)
	}
	if len(retryTargets) != 1 || retryTargets[0].Provider != providerLever || retryTargets[0].CompanySlug != "second" {
		t.Fatalf("resume targets = %+v, want only lever/second", retryTargets)
	}

	retryAttempts := make(map[companyTarget]int)
	if err := updateJobTargets(ctx, database, resumedRun, retryTargets, 1, func(_ context.Context, target companyTarget) ([]db.Job, error) {
		retryAttempts[target]++
		return []db.Job{testUpdateJob(target)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if retryAttempts[targets[0]] != 0 || retryAttempts[targets[1]] != 1 {
		t.Fatalf("retry attempts = %v, want only lever/second retried", retryAttempts)
	}
	if err := database.CompleteJobUpdateRun(ctx, resumedRun); err != nil {
		t.Fatal(err)
	}

	freshRun, freshTargets, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(expandedTargets))
	if err != nil {
		t.Fatal(err)
	}
	if freshRun.Resumed || freshRun.ID == run.ID || len(freshTargets) != len(expandedTargets) {
		t.Fatalf("next invocation did not start a fresh snapshot: run=%+v targets=%+v", freshRun, freshTargets)
	}
}

func TestJobUpdateDestinationStopPausesRunAndHonorsRetryDeadline(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "job-stop.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	targets := []companyTarget{
		{provider: providerGreenhouse, slug: "restricted"},
		{provider: providerGreenhouse, slug: "pending"},
	}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets))
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(map[string]int)
	err = updateJobTargets(ctx, database, run, pending, 1, func(ctx context.Context, target companyTarget) ([]db.Job, error) {
		attempts[target.slug]++
		if target.slug == "restricted" {
			return nil, &providerStopError{
				provider: providerGreenhouse,
				status:   429,
				retryAt:  time.Now().Add(time.Hour).UTC(),
				reason:   "rate limit",
			}
		}
		return nil, ctx.Err()
	})
	if !errors.Is(err, errProviderStopped) {
		t.Fatalf("update error = %v, want provider stop", err)
	}
	if err := database.PauseJobUpdateRun(ctx, run, providerGreenhouse, 429, "rate limit", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if attempts["restricted"] != 1 || attempts["pending"] != 0 {
		t.Fatalf("target attempts = %v, want only restricted target attempted", attempts)
	}
	progress, err := database.JobUpdateRunProgress(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.CompletedCount != 0 || progress.PendingCount != 2 || progress.FailedCount != 1 {
		t.Fatalf("stopped run progress = %+v, want 0 complete, 2 pending, 1 failed", progress)
	}

	_, _, err = database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets))
	var retryErr *db.JobUpdateRetryNotBeforeError
	if !errors.As(err, &retryErr) {
		t.Fatalf("rerun error = %v, want retry deadline rejection", err)
	}
	if attempts["pending"] != 0 {
		t.Fatalf("pending target was requested before retry time: %v", attempts)
	}
}

func TestJobUpdateRunRejectsConcurrentOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "job-active.db")
	database, err := db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	otherDatabase, err := db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer otherDatabase.Close()

	targets := []companyTarget{{provider: providerGreenhouse, slug: "first"}}
	if _, _, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := otherDatabase.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets)); !errors.Is(err, db.ErrJobUpdateRunActive) {
		t.Fatalf("second owner error = %v, want active-run error", err)
	}
}

func TestJobUpdateCommitAndTargetCompletionAreAtomic(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "job-atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	target := companyTarget{provider: providerGreenhouse, slug: "company"}
	if err := database.ReplaceProviderJobs(ctx, target.provider, target.slug, []db.Job{
		{JobKey: "existing", Title: "Existing job"},
	}); err != nil {
		t.Fatal(err)
	}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets([]companyTarget{target}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ReplaceProviderJobsAndCompleteTarget(ctx, run, pending[0], []db.Job{
		{Title: "Invalid job without an ID"},
	}); err == nil {
		t.Fatal("invalid job update unexpectedly committed")
	}

	jobs, total, err := database.ListProviderJobs(ctx, target.provider, db.JobFilters{}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].JobKey != "existing" {
		t.Fatalf("jobs after failed transaction = %+v (total %d), want original job preserved", jobs, total)
	}
	progress, err := database.JobUpdateRunProgress(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.CompletedCount != 0 || progress.PendingCount != 1 {
		t.Fatalf("progress after failed transaction = %+v, want target still pending", progress)
	}

	if err := database.ReplaceProviderJobsAndCompleteTarget(ctx, run, pending[0], []db.Job{
		{JobKey: "replacement", Title: "Replacement job"},
	}); err != nil {
		t.Fatal(err)
	}
	jobs, total, err = database.ListProviderJobs(ctx, target.provider, db.JobFilters{}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].JobKey != "replacement" {
		t.Fatalf("jobs after successful transaction = %+v (total %d), want replacement", jobs, total)
	}
	progress, err = database.JobUpdateRunProgress(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.CompletedCount != 1 || progress.PendingCount != 0 {
		t.Fatalf("progress after successful transaction = %+v, want target complete", progress)
	}
}

func TestLegacyJobUpdateCheckpointIsImportedIntoRunSnapshot(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "job-legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.SaveJobUpdateCheckpoint(ctx, providerGreenhouse, "already-done"); err != nil {
		t.Fatal(err)
	}
	targets := []companyTarget{
		{provider: providerGreenhouse, slug: "already-done"},
		{provider: providerLever, slug: "still-pending"},
	}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, databaseTargets(targets))
	if err != nil {
		t.Fatal(err)
	}
	if !run.Resumed || run.CompletedCount != 1 || len(pending) != 1 ||
		pending[0].Provider != providerLever || pending[0].CompanySlug != "still-pending" {
		t.Fatalf("legacy checkpoint import = run %+v, pending %+v", run, pending)
	}
}

func databaseTargets(targets []companyTarget) []db.JobUpdateTarget {
	result := make([]db.JobUpdateTarget, len(targets))
	for i, target := range targets {
		result[i] = db.JobUpdateTarget{Provider: target.provider, CompanySlug: target.slug}
	}
	return result
}

func testUpdateJob(target companyTarget) db.Job {
	return db.Job{
		JobKey:      target.provider + "-" + target.slug,
		Title:       "Test job",
		CompanyName: target.slug,
	}
}
