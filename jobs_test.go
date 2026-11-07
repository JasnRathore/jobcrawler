package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"db"
	"golang.org/x/time/rate"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fixtureProvider struct {
	name string
	job  db.Job
}

func (p fixtureProvider) Name() string                   { return p.name }
func (fixtureProvider) CompanyEndpoints(string) []string { return nil }
func (fixtureProvider) FallbackOnNotFound() bool         { return false }
func (fixtureProvider) CheckCompany(context.Context, *http.Client, *proxyPool, *requestLimiter, string, *benchmarkStats) (bool, error) {
	return true, nil
}
func (p fixtureProvider) FetchJobs(context.Context, *http.Client, *proxyPool, *requestLimiter, string) ([]db.Job, error) {
	return []db.Job{p.job}, nil
}

func TestUpdateAllJobsIncludesEveryRegisteredProvider(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "all-providers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const fixtureName = "fixtureboard"
	const secondFixtureName = "secondfixture"
	registerProvider(fixtureProvider{name: fixtureName, job: db.Job{JobKey: "fixture-1", Title: "Fixture role"}})
	defer delete(providerRegistry, fixtureName)
	registerProvider(fixtureProvider{name: secondFixtureName, job: db.Job{JobKey: "fixture-2", Title: "Another fixture role"}})
	defer delete(providerRegistry, secondFixtureName)

	if err := database.AddCompanyForProvider(ctx, fixtureName, "fixtureco"); err != nil {
		t.Fatal(err)
	}
	if err := database.AddCompanyForProvider(ctx, secondFixtureName, "anotherco"); err != nil {
		t.Fatal(err)
	}
	if err := updateAllJobs(ctx, database, &http.Client{}, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), 2); err != nil {
		t.Fatal(err)
	}
	jobs, total, err := database.ListJobs(ctx, db.JobFilters{}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(jobs) != 2 {
		t.Fatalf("all-provider update indexed %d jobs: %+v", total, jobs)
	}
	foundProviders := make(map[string]bool)
	for _, job := range jobs {
		foundProviders[job.Provider] = true
	}
	if !foundProviders[fixtureName] || !foundProviders[secondFixtureName] {
		t.Fatalf("provider registered through adapter was not updated: %+v", jobs)
	}
}

func TestUpdateCompanyJobsReplacesStaleJobsOnlyAfterCompleteResponse(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}

	defer database.Close()
	if err := database.AddCompany(ctx, "mcecleanenergy"); err != nil {
		t.Fatal(err)
	}

	body := `{"jobs":[
		{"id":5256222007,"title":"Internship","company_name":"MCE","location":{"name":"Hybrid"},"metadata":{"team":"operations"},"content":"Job description"},
		{"id":5209945007,"title":"Administrator","company_name":"MCE","location":{"name":"Remote"}}
	],"meta":{"total":2}}`
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)

	count, err := updateCompanyJobs(ctx, database, client, nil, limiter, "mcecleanenergy")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("indexed %d jobs, want 2", count)
	}
	listed, total, err := database.ListJobs(ctx, db.JobFilters{}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(listed) != 2 {
		t.Fatalf("stored total=%d rows=%d, want 2", total, len(listed))
	}
	hasMetadata := false
	for _, job := range listed {
		if job.LocationName == "" {
			t.Fatalf("stored job lost its location: %+v", job)
		}
		if strings.Contains(job.AttributesJSON, `"metadata"`) {
			hasMetadata = true
		}
	}
	if !hasMetadata {
		t.Fatal("stored jobs lost raw metadata")
	}

	body = `{"jobs":[{"id":5256222007,"title":"Changed"}],"meta":{"total":2}}`
	if _, err := updateCompanyJobs(ctx, database, client, nil, limiter, "mcecleanenergy"); err == nil {
		t.Fatal("incomplete response unexpectedly replaced existing jobs")
	}
	_, total, err = database.ListJobs(ctx, db.JobFilters{}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("incomplete response left %d jobs, want existing 2", total)
	}

	body = `{"jobs":[{"id":5256222007,"title":"Changed"}],"meta":{"total":1}}`
	count, err = updateCompanyJobs(ctx, database, client, nil, limiter, "mcecleanenergy")
	if err != nil {
		t.Fatal(err)
	}
	listed, total, err = database.ListJobs(ctx, db.JobFilters{Query: "Changed"}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || total != 1 || len(listed) != 1 {
		t.Fatalf("successful replacement returned count=%d total=%d rows=%d, want 1", count, total, len(listed))
	}
}

func TestListJobsSearchesTitlesByDefault(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "search.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.ReplaceCompanyJobs(ctx, "example", []db.Job{
		{ID: 1, Title: "Summer Internship", Description: "Learn about our internal tools"},
		{ID: 2, Title: "Senior Product Owner", Description: "Build internal systems"},
		{ID: 3, Title: "Data Analyst", Description: "Join our intern program"},
	})
	if err != nil {
		t.Fatal(err)
	}

	titles, total, err := database.ListJobs(ctx, db.JobFilters{Query: "intern"}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(titles) != 1 || titles[0].Title != "Summer Internship" {
		t.Fatalf("title-only search returned total=%d jobs=%v, want only internship", total, titles)
	}

	broad, total, err := database.ListJobs(ctx, db.JobFilters{Query: "intern", IncludeDescriptions: true}, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(broad) != 2 {
		t.Fatalf("broader search returned total=%d rows=%d, want 2", total, len(broad))
	}
	for _, job := range broad {
		if job.Title == "Senior Product Owner" {
			t.Fatal("description search treated 'internal' as a match for the whole word 'intern'")
		}
	}

}

func TestJobsBrowseAcrossProvidersAndApplyCommonFilters(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "combined.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.ReplaceProviderJobs(ctx, providerGreenhouse, "green", []db.Job{
		{ID: 1, Title: "Research Engineer", CompanyName: "Green Example", LocationName: "Boston, MA",
			FirstPublished: "2026-10-15", EmploymentType: "Full-time", WorkplaceType: "remote"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.ReplaceProviderJobs(ctx, providerLever, "leverco", []db.Job{
		{JobKey: "lever-1", Title: "Research Engineer", CompanyName: "Lever Example",
			LocationName: "Boston, MA", FirstPublished: "2026-10-15T00:00:00Z",
			EmploymentType: "Full-time or part-time", WorkplaceType: "remote"},
		{JobKey: "lever-2", Title: "Research Associate", CompanyName: "Lever Example",
			LocationName: "New York", FirstPublished: "2026-09-01", EmploymentType: "Contract",
			WorkplaceType: "onsite"},
	}); err != nil {
		t.Fatal(err)
	}

	filters := db.JobFilters{
		Query: "Research", Location: "Boston", PostedAfter: "2026-10-14",
		PostedBefore: "2026-10-15", EmploymentType: "full-time", WorkplaceType: "remote",
	}
	jobs, total, err := database.ListJobs(ctx, filters, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(jobs) != 2 {
		t.Fatalf("filtered combined listing returned %d jobs: %+v", total, jobs)
	}
	providers := map[string]bool{}
	for _, job := range jobs {
		providers[job.Provider] = true
	}
	if !providers[providerGreenhouse] || !providers[providerLever] {
		t.Fatalf("combined results missing provider listings: %v", providers)
	}
}

func TestCheckCompanyTreatsNoContentAsFound(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "no-content.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
	if err := checkCompany(ctx, client, nil, newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0), database, "foundcompany", nil); err != nil {
		t.Fatalf("checkCompany returned error for 204: %v", err)
	}
	companies, err := database.Companies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 || companies[0] != "foundcompany" {
		t.Fatalf("companies=%v, want [foundcompany]", companies)
	}
}

func TestBookmarksPersistWhenJobsAreRemoved(t *testing.T) {
	ctx := context.Background()
	database, err := db.New(ctx, filepath.Join(t.TempDir(), "bookmarks.db"))
	if err != nil {
		t.Fatal(err)
	}

	defer database.Close()

	if err := database.ReplaceCompanyJobs(ctx, "example", []db.Job{
		{ID: 42, Title: "Research Intern", CompanyName: "Example", LocationName: "Remote", DetailsURL: "https://example.test/jobs/42"},
	}); err != nil {
		t.Fatal(err)
	}
	saved, err := database.ToggleBookmark(ctx, "example", 42)
	if err != nil || !saved {
		t.Fatalf("first toggle saved=%t err=%v, want saved", saved, err)
	}
	count, err := database.BookmarkCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("bookmark count=%d err=%v, want 1", count, err)
	}

	if err := database.ReplaceCompanyJobs(ctx, "example", []db.Job{
		{ID: 42, Title: "Research Intern - Updated", CompanyName: "Example", LocationName: "Hybrid", DetailsURL: "https://example.test/jobs/42"},
	}); err != nil {
		t.Fatal(err)
	}
	jobs, total, err := database.ListJobs(ctx, db.JobFilters{}, true, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].Title != "Research Intern - Updated" || jobs[0].LocationName != "Hybrid" {
		t.Fatalf("bookmark snapshot was not refreshed: %+v", jobs)
	}

	if err := database.ReplaceCompanyJobs(ctx, "example", nil); err != nil {
		t.Fatal(err)
	}
	jobs, total, err = database.ListJobs(ctx, db.JobFilters{Query: "intern"}, true, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != 42 || !jobs[0].IsBookmarked || jobs[0].IsAvailable {
		t.Fatalf("saved jobs after refresh = total %d, jobs %+v; want saved snapshot", total, jobs)
	}
	removed, err := database.ToggleBookmark(ctx, "example", 42)
	if err != nil || removed {
		t.Fatalf("remove toggle returned saved=%t err=%v, want removed", removed, err)
	}
	count, err = database.BookmarkCount(ctx)
	if err != nil || count != 0 {
		t.Fatalf("bookmark count=%d err=%v, want 0", count, err)
	}
}

func TestBookmarkButtonTemplateReflectsSavedState(t *testing.T) {
	job := db.Job{
		Provider:     providerGreenhouse,
		CompanySlug:  "example",
		ID:           42,
		JobKey:       "42",
		Title:        "Research Intern",
		IsBookmarked: true,
		IsAvailable:  true,
	}
	var rendered bytes.Buffer
	err := browseResultsTemplate.Execute(&rendered, browsePage{
		SavedOnly:  true,
		Page:       1,
		TotalPages: 1,
		Total:      1,
		Jobs:       []db.Job{job},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	for _, expected := range []string{
		"♥ Saved",
		"hx-post=\"/bookmarks/toggle?provider=greenhouse&amp;company=example&amp;id=42&amp;saved=true\"",
		"hx-target=\"closest .job\"",
	} {
		if !strings.Contains(html, expected) {
			t.Errorf("rendered saved-job list missing %q:\n%s", expected, html)
		}
	}
}

func TestGreenhouseFetchJobsStopsOn403406And429(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				return &http.Response{
					StatusCode: status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}
			limiter := newRequestLimiter(rate.NewLimiter(rate.Inf, 1), 0)
			jobs, err := (greenhouseProvider{}).FetchJobs(context.Background(), client, nil, limiter, "example")
			if !errors.Is(err, errProviderStopped) {
				t.Fatalf("fetch error = %v, want provider stop", err)
			}
			if len(jobs) != 0 {
				t.Fatalf("unexpected jobs after restriction response: %+v", jobs)
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
