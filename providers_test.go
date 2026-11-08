package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db"
	"golang.org/x/time/rate"
)

func TestLeverEUProviderChecksAndIndexesJobs(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "lever.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const body = `[{
		"id":"lever-job-1",
		"text":"Retail Associate",
		"categories":{"location":"North Attleboro, MA","team":"Retail"},
		"createdAt":1698876279719,
		"descriptionPlain":"Help customers.",
		"additionalPlain":"Apply today.",
		"lists":[{"text":"Responsibilities:","content":"<li>Serve customers</li>"}],
		"workplaceType":"hybrid",
		"hostedUrl":"https://jobs.lever.co/example/lever-job-1",
		"applyUrl":"https://jobs.lever.co/example/lever-job-1/apply"
	}]`
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "api.eu.lever.co" {
			t.Errorf("unexpected Lever EU host: %s", req.URL.Host)
		}
		return providerResponse(req, http.StatusOK, body), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	if err := checkProviderCompany(ctx, client, nil, limiter, database, providerLeverEU, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerLeverEU)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Lever companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, nil, limiter, providerLeverEU, "example")
	if err != nil || count != 1 {
		t.Fatalf("indexed count=%d err=%v, want one Lever job", count, err)
	}
	jobs, total, err := database.ListProviderJobs(ctx, providerLeverEU, db.JobFilters{Query: "Retail"}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].JobKey != "lever-job-1" ||
		jobs[0].LocationName != "North Attleboro, MA" || !strings.Contains(jobs[0].Description, "Help customers.") ||
		!strings.Contains(jobs[0].Description, "Serve customers") || jobs[0].WorkplaceType != "hybrid" ||
		jobs[0].DetailsURL != "https://jobs.lever.co/example/lever-job-1" ||
		jobs[0].ApplyURL != "https://jobs.lever.co/example/lever-job-1" {
		t.Fatalf("unexpected Lever job index: %+v", jobs)
	}
	saved, err := database.ToggleProviderBookmark(ctx, providerLeverEU, "example", "lever-job-1")
	if err != nil || !saved {
		t.Fatalf("save Lever bookmark = %t, %v", saved, err)
	}
}

func TestLeverProviderUsesUSAPIOnly(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "api.lever.co" {
			t.Errorf("unexpected Lever host: %s", req.URL.Host)
		}
		return providerResponse(req, http.StatusOK, `[]`), nil
	})}
	if err := checkProviderCompany(context.Background(), client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), nil, providerLever, "example", nil); err != nil {
		t.Fatal(err)
	}
}

func TestLeverJobUsesBaseURLWhenHostedURLIsMissing(t *testing.T) {
	jobs, err := parseLeverJobs("example", []byte(`[{
		"id":"lever-job-1",
		"text":"Retail Associate",
		"applyUrl":"https://jobs.lever.co/example/lever-job-1/apply"
	}]`))
	if err != nil {
		t.Fatal(err)
	}
	const wantURL = "https://jobs.lever.co/example/lever-job-1"
	if len(jobs) != 1 || jobs[0].DetailsURL != wantURL || jobs[0].ApplyURL != wantURL {
		t.Fatalf("Lever job URLs = details %q, apply %q; want %q", jobs[0].DetailsURL, jobs[0].ApplyURL, wantURL)
	}
}

func TestWorkableProviderChecksAndIndexesJobs(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "workable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	body := `{
		"name":"Example Incorporated",
		"description":"<p>Company overview</p>",
		"jobs":[{
			"title":"Client Advisor",
			"shortcode":"D646899FE3",
			"code":"REQ-7",
			"employment_type":"Full-time",
			"department":"Sales",
			"url":"https://apply.workable.com/j/D646899FE3",
			"application_url":"https://apply.workable.com/j/D646899FE3/apply",
			"published_on":"2026-10-15",
			"created_at":"2026-10-14",
			"country":"United States",
			"city":"Elmont",
			"state":"New York",
			"locations":[{"countryCode":"US","city":"Elmont"}]
		}]
	}`
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "apply.workable.com" {
			t.Errorf("unexpected Workable host: %s", req.URL.Host)
		}
		return providerResponse(req, http.StatusOK, body), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	if err := checkProviderCompany(ctx, client, nil, limiter, database, providerWorkable, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerWorkable)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Workable companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, nil, limiter, providerWorkable, "example")
	if err != nil || count != 1 {
		t.Fatalf("indexed count=%d err=%v, want one Workable job", count, err)
	}
	jobs, total, err := database.ListProviderJobs(ctx, providerWorkable, db.JobFilters{Query: "Client Advisor"}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].JobKey != "D646899FE3" ||
		jobs[0].CompanyName != "Example Incorporated" || jobs[0].LocationName != "Elmont, New York, United States" ||
		jobs[0].DetailsURL != "https://apply.workable.com/j/D646899FE3" ||
		jobs[0].ApplyURL != "https://apply.workable.com/j/D646899FE3/apply" ||
		jobs[0].EmploymentType != "Full-time" || jobs[0].Department != "Sales" ||
		jobs[0].WorkplaceType != "onsite" {
		t.Fatalf("unexpected Workable job index: %+v", jobs)
	}
}

func TestWorkable429RetriesWithoutStopping(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		return providerResponse(req, http.StatusTooManyRequests, ""), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 80*time.Millisecond)

	found, err := (workableProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
	if found {
		t.Fatal("Workable company was found after 429s")
	}
	if errors.Is(err, errProviderStopped) {
		t.Fatalf("check error = %v, want rate-limit retry failure, not a provider stop", err)
	}
	if !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("check error = %v, want it to mention rate limiting", err)
	}
	if requests != 4 {
		t.Fatalf("made %d requests, want 4 bounded attempts", requests)
	}
	if limiter.providerStop(providerWorkable) != nil {
		t.Fatal("provider-wide stop was recorded for a 429")
	}
	// Four hits stay below the cross-route breaker tolerance (5).
	if !limiter.cooldownUntil.IsZero() {
		t.Fatal("four 429s tripped the global pause, want rotation instead")
	}
}

func TestAshbyHQProviderChecksAndIndexesListedJobs(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "ashby.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const body = `{
		"jobs": [
			{
				"id": "ashby-job-1",
				"title": "Research Engineer",
				"department": "Research",
				"team": "AI",
				"employmentType": "FullTime",
				"location": "San Francisco",
				"secondaryLocations": [{"location":"London, UK"}],
				"publishedAt": "2026-10-15T12:00:00.000Z",
				"isListed": true,
				"isRemote": true,
				"workplaceType": null,
				"address": {"postalAddress": {
					"addressRegion": "California",
					"addressCountry": "United States",
					"addressLocality": "San Francisco"
				}},
				"jobUrl": "https://jobs.ashbyhq.com/example/ashby-job-1",
				"applyUrl": "https://jobs.ashbyhq.com/example/ashby-job-1/application",
				"descriptionHtml": "<p>Build research systems.</p>"
			},
			{
				"id": "unlisted-job",
				"title": "Unlisted Role",
				"isListed": false
			}
		]
	}`
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "api.ashbyhq.com" ||
			req.URL.Path != "/posting-api/job-board/example" {
			t.Errorf("unexpected Ashby request URL: %s", req.URL)
		}
		return providerResponse(req, http.StatusOK, body), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	if err := checkProviderCompany(ctx, client, nil, limiter, database, providerAshbyHQ, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerAshbyHQ)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Ashby companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, nil, limiter, providerAshbyHQ, "example")
	if err != nil || count != 1 {
		t.Fatalf("indexed count=%d err=%v, want one listed Ashby job", count, err)
	}
	jobs, total, err := database.ListProviderJobs(ctx, providerAshbyHQ, db.JobFilters{Query: "Research Engineer"}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].JobKey != "ashby-job-1" ||
		jobs[0].CompanySlug != "example" || jobs[0].CompanyName != "example" ||
		jobs[0].LocationName != "San Francisco" || jobs[0].EmploymentType != "FullTime" ||
		jobs[0].WorkplaceType != "remote" || jobs[0].Department != "Research" ||
		jobs[0].DetailsURL != "https://jobs.ashbyhq.com/example/ashby-job-1" ||
		jobs[0].ApplyURL != "https://jobs.ashbyhq.com/example/ashby-job-1/application" ||
		jobs[0].FirstPublished != "2026-10-15T12:00:00.000Z" ||
		jobs[0].Description != "Build research systems." ||
		!strings.Contains(jobs[0].AttributesJSON, "London, UK") {
		t.Fatalf("unexpected Ashby job index: %+v", jobs[0])
	}
}

func providerResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func TestGreenhouseUnauthorizedStopsWithoutRetry(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				return providerResponse(req, status, ""), nil
			})}
			limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

			found, err := (greenhouseProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
			if !errors.Is(err, errProviderStopped) {
				t.Fatalf("check error = %v, want provider stop", err)
			}
			if found {
				t.Fatal("Greenhouse company was found after a restriction response")
			}
			if requests != 1 {
				t.Fatalf("made %d requests, want 1", requests)
			}
			if limiter.providerStop(providerGreenhouse) == nil {
				t.Fatal("provider-wide stop was not recorded")
			}
		})
	}
}

func TestGreenhouseRateLimitsRetryWithoutStopping(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				return providerResponse(req, status, ""), nil
			})}
			limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

			found, err := (greenhouseProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
			if found {
				t.Fatalf("Greenhouse company was found after HTTP %d responses", status)
			}
			if errors.Is(err, errProviderStopped) {
				t.Fatalf("check error = %v, want rate-limit retry failure, not a provider stop", err)
			}
			if !strings.Contains(err.Error(), "rate limited") {
				t.Fatalf("check error = %v, want it to mention rate limiting", err)
			}
			if requests != 4 {
				t.Fatalf("made %d requests, want 4 bounded attempts", requests)
			}
			if limiter.providerStop(providerGreenhouse) != nil {
				t.Fatal("provider-wide stop was recorded for a rate limit")
			}
			// Four hits stay below the cross-route breaker tolerance (5):
			// rotation handles them, no global pause yet.
			if !limiter.cooldownUntil.IsZero() {
				t.Fatal("four rate limits tripped the global pause, want rotation instead")
			}
		})
	}
}

func TestGenericProvidersDoNotEnterCooldownOn403Or406(t *testing.T) {
	providers := []jobBoardProvider{
		workableProvider{},
		leverProvider{name: providerLever, apiHost: "api.lever.co"},
		leverProvider{name: providerLeverEU, apiHost: "api.eu.lever.co"},
		ashbyHQProvider{},
	}
	statuses := []int{http.StatusForbidden, http.StatusNotAcceptable}
	for _, p := range providers {
		for _, status := range statuses {
			t.Run(fmt.Sprintf("%s-%d", p.Name(), status), func(t *testing.T) {
				const cooldown = 200 * time.Millisecond
				requests := 0
				client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					requests++
					return providerResponse(req, status, "blocked"), nil
				})}
				limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), cooldown)
				started := time.Now()
				found, err := p.CheckCompany(context.Background(), client, nil, limiter, "example", nil)
				if !errors.Is(err, errProviderStopped) || found {
					t.Fatalf("found=%t err=%v; want provider stop for status %d", found, err, status)
				}
				if requests != 1 {
					t.Fatalf("made %d requests, want 1", requests)
				}
				if limiter.providerStop(p.Name()) == nil {
					t.Fatal("provider-wide stop was not recorded")
				}
				if elapsed := time.Since(started); elapsed >= cooldown {
					t.Fatalf("%s provider erroneously entered cooldown: took %s", p.Name(), elapsed)
				}
			})
		}
	}
}

func TestGenericProviderStopsOnExplicitChallengeHeader(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		res := providerResponse(req, http.StatusOK, "")
		res.Header.Set("CF-Mitigated", "challenge")
		return res, nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	found, err := (workableProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
	if !errors.Is(err, errProviderStopped) || found {
		t.Fatalf("found=%t err=%v; want provider stop", found, err)
	}
	if requests != 1 || limiter.providerStop(providerWorkable) == nil {
		t.Fatalf("requests=%d provider stop=%v, want one request and a recorded stop", requests, limiter.providerStop(providerWorkable))
	}
}

func TestGreenhouseCheckCompanySendsAcceptHeader(t *testing.T) {
	var gotAccept string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		gotAccept = req.Header.Get("Accept")
		return providerResponse(req, http.StatusOK, ""), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)
	found, err := (greenhouseProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected company to be found")
	}
	if gotAccept != "application/json" {
		t.Fatalf("Accept header = %q, want application/json", gotAccept)
	}
}

func TestRequestLimiterBurstClampedOnCooldown(t *testing.T) {
	rateLim := rate.NewLimiter(rate.Limit(100), 100)
	limiter := newRequestLimiter(rateLim, 50*time.Millisecond)
	if rateLim.Burst() != 100 {
		t.Fatalf("initial burst = %d, want 100", rateLim.Burst())
	}
	limiter.StartCooldownForStatus(providerGreenhouse, http.StatusForbidden)
	if rateLim.Burst() != 1 {
		t.Fatalf("clamped burst = %d, want 1", rateLim.Burst())
	}
}

func TestRequestLimiterHonorsRetryAfter(t *testing.T) {
	rateLim := rate.NewLimiter(rate.Limit(10), 1)
	limiter := newRequestLimiter(rateLim, 50*time.Millisecond)

	// Server says Retry-After 200ms, which exceeds the 50ms configured
	dur := limiter.StartCooldownForStatusAndRetryAfter(providerGreenhouse, http.StatusTooManyRequests, 200*time.Millisecond)
	if dur < 180*time.Millisecond {
		t.Fatalf("cooldown duration %s did not reflect 200ms Retry-After", dur)
	}
}

func TestProviderStopErrorReportsFixedRetryTime(t *testing.T) {
	retryAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	stop := &providerStopError{
		provider:   providerGreenhouse,
		status:     http.StatusTooManyRequests,
		retryAfter: time.Minute,
		retryAt:    retryAt,
		reason:     "rate limit",
	}

	if got := stop.Error(); !strings.Contains(got, retryAt.Format(time.RFC3339)) {
		t.Fatalf("provider stop error = %q, want fixed retry time %s", got, retryAt.Format(time.RFC3339))
	}
}

func TestGreenhouseBadStatusRetriesThenFailsIntoFailed(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		return providerResponse(req, http.StatusBadRequest, ""), nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	found, err := (greenhouseProvider{}).CheckCompany(context.Background(), client, nil, limiter, "example", nil)
	if found {
		t.Fatal("Greenhouse company was found after bad-request responses")
	}
	if errors.Is(err, errProviderStopped) {
		t.Fatalf("check error = %v, want a per-candidate failure that lands in failed.txt, not a stop", err)
	}
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("check error = %v, want it to report the exhausted bad status", err)
	}
	if requests != 4 {
		t.Fatalf("made %d requests, want 4 bounded attempts across routes before failing", requests)
	}
	if limiter.providerStop(providerGreenhouse) != nil {
		t.Fatal("provider-wide stop was recorded for bad requests")
	}
}

func TestRequestLimiterRateLimitBreakerPausesProvider(t *testing.T) {
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 30*time.Second)
	limiter.rateLimitTolerance = 2
	limiter.rateLimitWindow = time.Minute

	if limiter.recordRateLimit(providerWorkable, 0) {
		t.Fatal("first 429 tripped the global pause, want rotation instead")
	}
	if !limiter.cooldownUntil.IsZero() {
		t.Fatal("provider paused after a single 429")
	}
	if !limiter.recordRateLimit(providerWorkable, 0) {
		t.Fatal("second 429 within the window did not trip the global pause")
	}
	if limiter.cooldownUntil.Before(time.Now()) {
		t.Fatal("global pause did not engage after repeated 429s")
	}
	if limiter.providerStop(providerWorkable) != nil {
		t.Fatal("provider-wide stop was recorded for 429s")
	}
	// Hits reset after the trip: the next burst starts fresh.
	if limiter.recordRateLimit(providerWorkable, 0) {
		t.Fatal("429 right after the trip re-tripped the pause, want reset counter")
	}
}

func TestRequestLimiterRateLimitHitsExpireOutsideWindow(t *testing.T) {
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 30*time.Second)
	limiter.rateLimitTolerance = 2
	limiter.rateLimitWindow = 20 * time.Millisecond

	limiter.recordRateLimit(providerWorkable, 0)
	time.Sleep(30 * time.Millisecond)
	if limiter.recordRateLimit(providerWorkable, 0) {
		t.Fatal("expired 429 still counted toward the pause")
	}
}
