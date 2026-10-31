package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"db"
)

func TestJobUpdateCheckpointsResumeOnlyIncompleteCompanies(t *testing.T) {
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
	firstAttempts := make(map[companyTarget]int)
	err = updateJobTargets(ctx, database, targets, 1, func(_ context.Context, target companyTarget) (int, error) {
		firstAttempts[target]++
		if target.provider == providerLever {
			return 0, errors.New("temporary timeout")
		}
		return 3, nil
	})
	if err == nil {
		t.Fatal("first update run succeeded despite a failed company")
	}
	if firstAttempts[targets[0]] != 1 || firstAttempts[targets[1]] != 1 {
		t.Fatalf("first run attempts = %v, want one attempt per company", firstAttempts)
	}
	checkpoints, err := database.JobUpdateCheckpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 || checkpoints[0].Provider != providerGreenhouse || checkpoints[0].CompanySlug != "first" {
		t.Fatalf("checkpoints after partial run = %+v, want only greenhouse/first", checkpoints)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = db.New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	retryAttempts := make(map[companyTarget]int)
	if err := updateJobTargets(ctx, database, targets, 1, func(_ context.Context, target companyTarget) (int, error) {
		retryAttempts[target]++
		return 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	if retryAttempts[targets[0]] != 0 || retryAttempts[targets[1]] != 1 {
		t.Fatalf("retry attempts = %v, want only lever/second retried", retryAttempts)
	}
	checkpoints, err = database.JobUpdateCheckpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 0 {
		t.Fatalf("checkpoints after successful completion = %+v, want none", checkpoints)
	}
}
