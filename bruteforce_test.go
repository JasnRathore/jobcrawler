package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestBruteCheckpointsAreProviderScoped(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "provider-checkpoint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const alphabet = "abc"
	position, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 2, alphabet, 12)
	if err != nil || position != 0 {
		t.Fatalf("new Greenhouse position=%d err=%v", position, err)
	}
	if err := database.SaveProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 2, alphabet, 7, 12); err != nil {
		t.Fatal(err)
	}
	position, err = database.StartProviderBruteCheckpoint(ctx, providerLever, 1, 2, alphabet, 12)
	if err != nil || position != 0 {
		t.Fatalf("new Lever position=%d err=%v; checkpoint must not inherit Greenhouse progress", position, err)
	}
	position, err = database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 2, alphabet, 12)
	if err != nil || position != 7 {
		t.Fatalf("resumed Greenhouse position=%d err=%v, want 7", position, err)
	}
}

func TestRunBruteForceSupportsAllProviders(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "all-providers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	for _, provider := range registeredProviders() {
		t.Run(provider, func(t *testing.T) {
			var mu sync.Mutex
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				requests++
				mu.Unlock()
				return providerResponse(req, http.StatusNotFound, ""), nil
			})}
			pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, provider, 4)
			if err := runBruteForce(ctx, pool, nil, database, 1, 1, true, filepath.Join(t.TempDir(), "failed.txt")); err != nil {
				t.Fatal(err)
			}
			if failures := pool.close(); failures != 0 {
				t.Fatalf("check pool reported %d failures", failures)
			}

			position, err := database.StartProviderBruteCheckpoint(ctx, provider, 1, 1, alphabet, bruteCount(1, 1))
			if err != nil || position != bruteCount(1, 1) {
				t.Fatalf("%s checkpoint position=%d err=%v, want %d", provider, position, err, bruteCount(1, 1))
			}
			mu.Lock()
			gotRequests := requests
			mu.Unlock()
			wantRequests := int(bruteCount(1, 1))
			if gotRequests != wantRequests {
				t.Fatalf("%s made %d requests, want %d", provider, gotRequests, wantRequests)
			}
		})
	}
}

func TestRunBruteForceSavesFailuresAndContinuesUpToFivePerCheckpoint(t *testing.T) {
	for _, failureCount := range []int{5, 6} {
		t.Run(strconv.Itoa(failureCount), func(t *testing.T) {
			ctx := context.Background()
			database, err := db.New(ctx, filepath.Join(t.TempDir(), "failed-checkpoint.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			failedSlugs := make(map[string]bool)
			for i := 0; i < failureCount; i++ {
				failedSlugs[bruteSlugAt(1, 1, int64(i))] = true
			}
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				status := http.StatusNotFound
				slug := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
				if failedSlugs[slug] {
					status = http.StatusBadRequest
				}
				return providerResponse(req, status, ""), nil
			})}
			pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 1)
			failedPath := filepath.Join(t.TempDir(), "failed.txt")
			runErr := runBruteForce(ctx, pool, nil, database, 1, 1, true, failedPath)
			if failures := pool.close(); failures != 0 {
				t.Fatalf("check pool reported %d failures", failures)
			}

			if failureCount == maxCheckpointFailures && runErr != nil {
				t.Fatalf("run stopped with %d failures: %v", failureCount, runErr)
			}
			if failureCount > maxCheckpointFailures && runErr == nil {
				t.Fatal("run continued despite exceeding the checkpoint failure limit")
			}
			position, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 1, alphabet, bruteCount(1, 1))
			if err != nil {
				t.Fatal(err)
			}
			wantPosition := int64(0)
			if failureCount <= maxCheckpointFailures {
				wantPosition = bruteCount(1, 1)
			}
			if position != wantPosition {
				t.Fatalf("checkpoint position=%d, want %d", position, wantPosition)
			}

			content, err := os.ReadFile(failedPath)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(content)), "\n")
			if len(lines) != failureCount {
				t.Fatalf("failed.txt contains %d entries, want %d: %q", len(lines), failureCount, content)
			}
			for i, slug := range lines {
				if want := bruteSlugAt(1, 1, int64(i)); slug != want {
					t.Errorf("failed.txt entry %d = %q, want %q", i, slug, want)
				}
			}
		})
	}
}

func TestWordlistCheckFailuresAreSaved(t *testing.T) {
	ctx := context.Background()
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return providerResponse(req, http.StatusBadRequest, ""), nil
	})}
	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), nil, nil, providerGreenhouse, 1)
	if _, ok := pool.submit("failed-slug"); !ok {
		t.Fatal("check pool rejected candidate")
	}
	pool.flush()
	path := filepath.Join(t.TempDir(), "failed.txt")
	if err := saveCheckFailures(pool, path); err != nil {
		t.Fatal(err)
	}
	if failures := pool.close(); failures != 0 {
		t.Fatalf("check pool reported %d remaining failures", failures)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "failed-slug\n"; got != want {
		t.Fatalf("failed.txt = %q, want %q", got, want)
	}
	if err := saveCheckFailures(pool, path); err != nil {
		t.Fatal(err)
	}
	content, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "failed-slug\n"; got != want {
		t.Fatalf("failure was written more than once: got %q, want %q", got, want)
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
		return newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 4)
	}

	pool := newPool()
	if err := runBruteForce(ctx, pool, nil, database, 1, 1, true, filepath.Join(t.TempDir(), "failed.txt")); err != nil {
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
	if err := runBruteForce(ctx, pool, nil, database, 1, 1, true, filepath.Join(t.TempDir(), "failed.txt")); err != nil {
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
	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, stats, providerGreenhouse, 1)
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

func TestRunBruteForceRequeuesThrottledCandidatesSuccessfully(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "requeue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var mu sync.Mutex
	attemptsPerSlug := make(map[string]int)

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		slug := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
		mu.Lock()
		attemptsPerSlug[slug]++
		attempt := attemptsPerSlug[slug]
		mu.Unlock()

		// First 4 attempts (across rounds) return 500 error, 5th attempt (via in-flight batch requeue) succeeds
		if attempt <= 4 {
			return providerResponse(req, http.StatusInternalServerError, ""), nil
		}
		return providerResponse(req, http.StatusNotFound, ""), nil
	})}

	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 1)
	failedPath := filepath.Join(t.TempDir(), "failed.txt")

	if err := runBruteForce(ctx, pool, nil, database, 1, 1, true, failedPath); err != nil {
		t.Fatalf("runBruteForce failed unexpectedly: %v", err)
	}
	if failures := pool.close(); failures != 0 {
		t.Fatalf("check pool reported %d failures", failures)
	}

	// Verify failed.txt has 0 entries (candidate was successfully requeued and processed)
	if _, err := os.Stat(failedPath); !os.IsNotExist(err) {
		content, _ := os.ReadFile(failedPath)
		if len(strings.TrimSpace(string(content))) > 0 {
			t.Fatalf("failed.txt is not empty: %s", content)
		}
	}

	// Checkpoint should have reached 26 (100% completed)
	position, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 1, alphabet, bruteCount(1, 1))
	if err != nil || position != bruteCount(1, 1) {
		t.Fatalf("checkpoint position = %d, want %d", position, bruteCount(1, 1))
	}
}

// Regression for the shutdown hang: after cancellation flush() stops
// draining the moment results stop flowing, while up to checkQueueSize
// checks are still in flight; workers blocked sending into the results
// buffer then made close()'s wg.Wait() wait forever, so Ctrl+C appeared
// to be ignored. close() must now return and collect what is buffered.
func TestCheckPoolCloseReturnsAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatal(err)
	}

	defer database.Close()

	// Every check blocks until the pool context is cancelled, so both
	// workers sit inside their checks with nothing sent or drained yet.
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 2)

	for i := 0; i < 6; i++ {
		if _, ok := pool.submit(bruteSlugAt(1, 1, int64(i))); !ok {
			t.Fatal("submit rejected a slug before cancellation")
		}
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	done := make(chan int, 1)
	go func() {
		done <- pool.close()
	}()
	select {
	case drained := <-done:
		if drained != 2 {
			t.Fatalf("close collected %d failures, want the 2 results buffered when the window stalled", drained)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pool.close did not finish after cancellation; worker result sends are stuck")
	}
}

func TestBruteForceDestinationStopIsNotRetriedOrCheckpointed(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "destination-stop.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     http.Header{"Retry-After": []string{"30"}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 1)

	err = runBruteForce(ctx, pool, make(map[string]struct{}), database, 1, 1, true, filepath.Join(t.TempDir(), "failed.txt"))
	if !errors.Is(err, errProviderStopped) {
		t.Fatalf("run error = %v, want provider stop", err)
	}
	pool.close()

	if requests != 1 {
		t.Fatalf("made %d requests after destination stop, want exactly 1", requests)
	}
	position, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 1, alphabet, bruteCount(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if position != 0 {
		t.Fatalf("checkpoint advanced to %d after a destination stop, want 0", position)
	}
}

func TestBruteForce429RotatesWithoutStopping(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "rate-limited.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Retry-After": []string{"1"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
	pool := newCheckPool(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, nil, providerGreenhouse, 1)

	if err := runBruteForce(ctx, pool, make(map[string]struct{}), database, 1, 1, true, filepath.Join(t.TempDir(), "failed.txt")); err != nil {
		t.Fatalf("run error = %v, want the 429 to rotate into a completed check", err)
	}
	pool.close()

	if requests != int(bruteCount(1, 1))+1 {
		t.Fatalf("made %d requests, want each of the %d candidates once plus the retried 429", requests, bruteCount(1, 1))
	}
	if pool.providerStop() != nil {
		t.Fatal("provider-wide stop was recorded for a 429")
	}
	position, err := database.StartProviderBruteCheckpoint(ctx, providerGreenhouse, 1, 1, alphabet, bruteCount(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if position != bruteCount(1, 1) {
		t.Fatalf("checkpoint advanced to %d, want %d: every candidate completed after the 429", position, bruteCount(1, 1))
	}
}
