package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestExpiredJobUpdateOwnerIsFencedOnTakeover(t *testing.T) {
	ctx := context.Background()
	database, err := New(ctx, filepath.Join(t.TempDir(), "lease.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	target := JobUpdateTarget{Provider: "greenhouse", CompanySlug: "company"}
	firstOwner, pending, err := database.BeginOrResumeJobUpdateRun(ctx, []JobUpdateTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("initial pending targets = %+v, want one", pending)
	}
	if _, err := database.conn.ExecContext(ctx, `
		UPDATE job_update_runs
		SET lease_expires_at = CAST(strftime('%s', 'now') AS INTEGER) - 1
		WHERE run_id = ?
	`, firstOwner.ID); err != nil {
		t.Fatal(err)
	}

	secondOwner, pending, err := database.BeginOrResumeJobUpdateRun(ctx, []JobUpdateTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	if secondOwner.ID != firstOwner.ID || secondOwner.FencingToken != firstOwner.FencingToken+1 || !secondOwner.Resumed {
		t.Fatalf("takeover = %+v, first owner %+v", secondOwner, firstOwner)
	}
	if err := database.ReplaceProviderJobsAndCompleteTarget(ctx, firstOwner, target, []Job{{JobKey: "stale"}}); !errors.Is(err, ErrJobUpdateRunOwnershipLost) {
		t.Fatalf("stale owner write error = %v, want ownership-lost error", err)
	}
	if err := database.ReplaceProviderJobsAndCompleteTarget(ctx, secondOwner, pending[0], []Job{{JobKey: "current"}}); err != nil {
		t.Fatalf("current owner could not commit target: %v", err)
	}
}

func TestJobUpdateRunMigrationAddsTablesToExistingDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	database, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AddCompanyForProvider(ctx, "greenhouse", "existing-company"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.conn.ExecContext(ctx, `
		DROP TABLE job_update_targets;
		DROP TABLE job_update_runs;
		DROP TABLE schema_migrations;
	`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	companies, err := migrated.CompaniesForProvider(ctx, "greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 || companies[0] != "existing-company" {
		t.Fatalf("companies after migration = %v, want existing company preserved", companies)
	}
	var migrationCount int
	if err := migrated.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM schema_migrations WHERE version = 1
	`).Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 1 {
		t.Fatalf("job update migration count = %d, want one", migrationCount)
	}
}
