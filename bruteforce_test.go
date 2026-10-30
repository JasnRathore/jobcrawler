package main

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"db"
	"golang.org/x/time/rate"
)

func TestBruteSlugAtPreservesLexicographicSequence(t *testing.T) {
	tests := []struct {
		index int64
		want  string
	}{
		{index: 0, want: "a"},
		{index: 25, want: "z"},
		{index: 26, want: "aa"},
		{index: 27, want: "ab"},
		{index: 26 + 26*26 - 1, want: "zz"},
	}
	for _, test := range tests {
		if got := bruteSlugAt(1, 2, test.index); got != test.want {
			t.Errorf("bruteSlugAt(%d) = %q, want %q", test.index, got, test.want)
		}
	}
}

func TestBruteCheckpointResumesAndOnlyMovesForward(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "checkpoint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.AddCompany(ctx, "replay-safe"); err != nil {
		t.Fatal(err)
	}
	if err := database.AddCompany(ctx, "replay-safe"); err != nil {
		t.Fatalf("replayed company candidate was not idempotent: %v", err)
	}
	companies, err := database.Companies(ctx)
	if err != nil || len(companies) != 1 {
		t.Fatalf("company rows=%v err=%v, want one idempotent insert", companies, err)
	}

	const alphabet = "abc"
	position, err := database.StartBruteCheckpoint(ctx, 2, 3, alphabet, 36)
	if err != nil || position != 0 {
		t.Fatalf("new checkpoint position=%d err=%v, want 0", position, err)
	}
	if err := database.SaveBruteCheckpoint(ctx, 2, 3, alphabet, 12, 36); err != nil {
		t.Fatal(err)
	}
	position, err = database.StartBruteCheckpoint(ctx, 2, 3, alphabet, 36)
	if err != nil || position != 12 {
		t.Fatalf("resumed position=%d err=%v, want 12", position, err)
	}
	if err := database.SaveBruteCheckpoint(ctx, 2, 3, alphabet, 8, 36); err == nil {
		t.Fatal("checkpoint unexpectedly moved backward")
	}
	if _, err := database.StartBruteCheckpoint(ctx, 2, 3, alphabet, 37); err == nil {
		t.Fatal("checkpoint accepted a different search total")
	}
}

func TestRunBruteForceContinuesFromSavedPosition(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "continue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const startAt = int64(5)
	total := bruteCount(1, 1)
	if _, err := database.StartBruteCheckpoint(ctx, 1, 1, alphabet, total); err != nil {
		t.Fatal(err)
	}
	if err := database.SaveBruteCheckpoint(ctx, 1, 1, alphabet, startAt, total); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		requests++
		mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
	newPool := func() *checkPool {
		return newCheckPool(ctx, client, rate.NewLimiter(rate.Inf, 1), database, nil, 4)
	}

	pool := newPool()
	if err := runBruteForce(ctx, pool, nil, database, 1, 1, true); err != nil {
		t.Fatal(err)
	}
	pool.close()
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if want := int(total - startAt); gotRequests != want {
		t.Fatalf("resumed made %d requests, want %d", gotRequests, want)
	}
	position, err := database.StartBruteCheckpoint(ctx, 1, 1, alphabet, total)
	if err != nil || position != total {
		t.Fatalf("checkpoint position=%d err=%v, want %d", position, err, total)
	}

	pool = newPool()
	if err := runBruteForce(ctx, pool, nil, database, 1, 1, true); err != nil {
		t.Fatal(err)
	}
	pool.close()
	mu.Lock()
	defer mu.Unlock()
	if requests != gotRequests {
		t.Fatalf("completed checkpoint reran candidates: requests %d, want %d", requests, gotRequests)
	}
}

func TestCheckPoolUsesSharedCheckCompanyPathInBenchmarkMode(t *testing.T) {
	ctx := context.Background()
	database, cleanup, err := newBenchmarkDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	stats := newBenchmarkStats()
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
	pool := newCheckPool(ctx, client, rate.NewLimiter(rate.Inf, 1), database, stats, 1)
	if _, ok := pool.submit("example"); !ok {
		t.Fatal("benchmark pool rejected candidate")
	}
	if failures := pool.close(); failures != 0 {
		t.Fatalf("pool reported %d failures, want 0", failures)
	}
	companies, err := database.Companies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("benchmark database companies=%v, want [example]", companies)
	}
	stats.mu.Lock()
	checks, requests, errors := stats.checks, stats.requests, stats.transportError
	stats.mu.Unlock()
	if checks != 1 || requests != 1 || errors != 0 {
		t.Fatalf("benchmark stats checks=%d requests=%d transport-errors=%d; want 1, 1, 0", checks, requests, errors)
	}
}
