package main

import (
	"bytes"
	"context"
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
	limiter := rate.NewLimiter(rate.Inf, 1)

	count, err := updateCompanyJobs(ctx, database, client, limiter, "mcecleanenergy")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("indexed %d jobs, want 2", count)
	}
	listed, total, err := database.ListJobs(ctx, "", false, false, 10, 0)
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
		if strings.Contains(job.RawJSON, `"metadata"`) {
			hasMetadata = true
		}
	}
	if !hasMetadata {
		t.Fatal("stored jobs lost raw metadata")
	}

	body = `{"jobs":[{"id":5256222007,"title":"Changed"}],"meta":{"total":2}}`
	if _, err := updateCompanyJobs(ctx, database, client, limiter, "mcecleanenergy"); err == nil {
		t.Fatal("incomplete response unexpectedly replaced existing jobs")
	}
	_, total, err = database.ListJobs(ctx, "", false, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("incomplete response left %d jobs, want existing 2", total)
	}

	body = `{"jobs":[{"id":5256222007,"title":"Changed"}],"meta":{"total":1}}`
	count, err = updateCompanyJobs(ctx, database, client, limiter, "mcecleanenergy")
	if err != nil {
		t.Fatal(err)
	}
	listed, total, err = database.ListJobs(ctx, "Changed", false, false, 10, 0)
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

	err = database.ReplaceCompanyJobs(ctx, "example", []db.GreenhouseJob{
		{ID: 1, Title: "Summer Internship", Content: "Learn about our internal tools"},
		{ID: 2, Title: "Senior Product Owner", Content: "Build internal systems"},
		{ID: 3, Title: "Data Analyst", Content: "Join our intern program"},
	})
	if err != nil {
		t.Fatal(err)
	}

	titles, total, err := database.ListJobs(ctx, "intern", false, false, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(titles) != 1 || titles[0].Title != "Summer Internship" {
		t.Fatalf("title-only search returned total=%d jobs=%v, want only internship", total, titles)
	}

	broad, total, err := database.ListJobs(ctx, "intern", true, false, 10, 0)
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
	if err := checkCompany(ctx, client, rate.NewLimiter(rate.Inf, 1), database, "foundcompany", nil); err != nil {
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

	if err := database.ReplaceCompanyJobs(ctx, "example", []db.GreenhouseJob{
		{ID: 42, Title: "Research Intern", CompanyName: "Example", LocationName: "Remote", AbsoluteURL: "https://example.test/jobs/42"},
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

	if err := database.ReplaceCompanyJobs(ctx, "example", []db.GreenhouseJob{
		{ID: 42, Title: "Research Intern - Updated", CompanyName: "Example", LocationName: "Hybrid", AbsoluteURL: "https://example.test/jobs/42"},
	}); err != nil {
		t.Fatal(err)
	}
	jobs, total, err := database.ListJobs(ctx, "", false, true, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].Title != "Research Intern - Updated" || jobs[0].LocationName != "Hybrid" {
		t.Fatalf("bookmark snapshot was not refreshed: %+v", jobs)
	}

	if err := database.ReplaceCompanyJobs(ctx, "example", nil); err != nil {
		t.Fatal(err)
	}
	jobs, total, err = database.ListJobs(ctx, "intern", false, true, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != 42 || !jobs[0].IsBookmarked {
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
	job := db.GreenhouseJob{
		CompanySlug:  "example",
		ID:           42,
		Title:        "Research Intern",
		IsBookmarked: true,
	}
	var rendered bytes.Buffer
	err := browseResultsTemplate.Execute(&rendered, browsePage{
		SavedOnly:  true,
		Page:       1,
		TotalPages: 1,
		Total:      1,
		Jobs:       []db.GreenhouseJob{job},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	for _, expected := range []string{
		"♥ Saved",
		"hx-post=\"/bookmarks/toggle?company=example&amp;id=42&amp;saved=true\"",
		"hx-target=\"closest .job\"",
	} {
		if !strings.Contains(html, expected) {
			t.Errorf("rendered saved-job list missing %q:\n%s", expected, html)
		}
	}
}
