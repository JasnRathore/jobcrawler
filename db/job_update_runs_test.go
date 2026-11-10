package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestJobUpdateRunProgressUsesSupportedTransaction(t *testing.T) {
	conn := sql.OpenDB(readOnlyRejectingConnector{})
	defer conn.Close()
	database := &DB{conn: conn}

	progress, err := database.JobUpdateRunProgress(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if progress.Status != "running" || progress.TargetCount != 2 || progress.CompletedCount != 1 || progress.PendingCount != 1 {
		t.Fatalf("progress = %+v, want running run with 1 complete and 1 pending target", progress)
	}
}

func TestRenewJobUpdateRunLeaseHonorsContextWhileWaitingForWriteLock(t *testing.T) {
	database := &DB{}
	database.writeMu.Lock()
	unlocked := make(chan struct{})
	go func() {
		time.Sleep(40 * time.Millisecond)
		database.writeMu.Unlock()
		close(unlocked)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := database.RenewJobUpdateRunLease(ctx, &JobUpdateRun{})
	<-unlocked
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lease renewal error = %v, want context deadline exceeded", err)
	}
}

func TestJobUpdateWritesWaitWhileLeaseRenewalIsPending(t *testing.T) {
	database := &DB{}
	database.leaseRenewalPending.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := database.lockWrite(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write lock error = %v, want context deadline exceeded", err)
	}
}

func TestReplaceProviderJobsAndCompleteTargetBatchesLargeJobSets(t *testing.T) {
	ctx := context.Background()
	database, err := New(ctx, filepath.Join(t.TempDir(), "job-batches.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	target := JobUpdateTarget{Provider: "greenhouse", CompanySlug: "batch-company"}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, []JobUpdateTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	jobs := make([]Job, 85)
	for i := range jobs {
		jobs[i] = Job{JobKey: fmt.Sprintf("job-%03d", i), Title: "Role"}
	}
	jobs[0].Title = "First occurrence"
	jobs[10] = Job{JobKey: jobs[0].JobKey, Title: "Duplicate in batch"}
	jobs[80] = Job{JobKey: jobs[0].JobKey, Title: "Duplicate in later batch"}
	if err := database.ReplaceProviderJobsAndCompleteTarget(ctx, run, pending[0], jobs); err != nil {
		t.Fatal(err)
	}

	var saved int
	if err := database.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM indexed_jobs WHERE provider = ? AND company_slug = ?
	`, target.Provider, target.CompanySlug).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	if saved != len(jobs)-2 {
		t.Fatalf("saved jobs = %d, want %d unique jobs", saved, len(jobs)-2)
	}
	var title string
	if err := database.conn.QueryRowContext(ctx, `
		SELECT title FROM indexed_jobs
		WHERE provider = ? AND company_slug = ? AND job_id = ?
	`, target.Provider, target.CompanySlug, jobs[0].JobKey).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "First occurrence" {
		t.Fatalf("duplicate job title = %q, want first occurrence to win", title)
	}
}

type readOnlyRejectingConnector struct{}

func (readOnlyRejectingConnector) Connect(context.Context) (driver.Conn, error) {
	return readOnlyRejectingConn{}, nil
}

func (readOnlyRejectingConnector) Driver() driver.Driver {
	return readOnlyRejectingDriver{}
}

type readOnlyRejectingDriver struct{}

func (readOnlyRejectingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("direct Open is not supported")
}

type readOnlyRejectingConn struct{}

func (readOnlyRejectingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not supported")
}

func (readOnlyRejectingConn) Close() error { return nil }

func (readOnlyRejectingConn) Begin() (driver.Tx, error) {
	return readOnlyRejectingTx{}, nil
}

func (readOnlyRejectingConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.ReadOnly {
		return nil, errors.New("read only transactions are not supported")
	}
	return readOnlyRejectingTx{}, nil
}

func (readOnlyRejectingConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "FROM job_update_runs WHERE run_id") {
		return &progressRows{
			columns: []string{"status", "owner_token", "fencing_token", "target_count", "stop_provider", "stop_status", "stop_reason", "retry_after_at"},
			values:  [][]driver.Value{{"running", "owner", int64(1), int64(2), "", int64(0), "", int64(0)}},
		}, nil
	}
	if strings.Contains(query, "FROM job_update_targets WHERE run_id") {
		return &progressRows{
			columns: []string{"completed_count", "pending_count", "failed_count"},
			values:  [][]driver.Value{{int64(1), int64(1), int64(0)}},
		}, nil
	}
	return nil, errors.New("unexpected progress query")
}

type readOnlyRejectingTx struct{}

func (readOnlyRejectingTx) Commit() error   { return nil }
func (readOnlyRejectingTx) Rollback() error { return nil }

type progressRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *progressRows) Columns() []string { return r.columns }
func (r *progressRows) Close() error      { return nil }

func (r *progressRows) Next(destination []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(destination, r.values[r.index])
	r.index++
	return nil
}

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
