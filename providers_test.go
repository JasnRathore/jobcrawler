package main

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

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
	limiter := rate.NewLimiter(rate.Inf, 1)

	if err := checkProviderCompany(ctx, client, limiter, database, providerLeverEU, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerLeverEU)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Lever companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, limiter, providerLeverEU, "example")
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
		jobs[0].ApplyURL != "https://jobs.lever.co/example/lever-job-1/apply" {
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
	if err := checkProviderCompany(context.Background(), client, rate.NewLimiter(rate.Inf, 1), nil, providerLever, "example", nil); err != nil {
		t.Fatal(err)
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
	limiter := rate.NewLimiter(rate.Inf, 1)

	if err := checkProviderCompany(ctx, client, limiter, database, providerWorkable, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerWorkable)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Workable companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, limiter, providerWorkable, "example")
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
	limiter := rate.NewLimiter(rate.Inf, 1)

	if err := checkProviderCompany(ctx, client, limiter, database, providerAshbyHQ, "example", nil); err != nil {
		t.Fatal(err)
	}
	companies, err := database.CompaniesForProvider(ctx, providerAshbyHQ)
	if err != nil || len(companies) != 1 || companies[0] != "example" {
		t.Fatalf("Ashby companies=%v err=%v", companies, err)
	}

	count, err := updateProviderJobs(ctx, database, client, limiter, providerAshbyHQ, "example")
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
