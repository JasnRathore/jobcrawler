package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhtml "html"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"db"
	"golang.org/x/time/rate"
)

const (
	jobListURL  = baseURL + "/jobs?content=true"
	jobPageSize = 50
	maxJobsBody = 64 << 20
)

var htmlTagPattern = regexp.MustCompile(`<[^>]*>`)

type greenhouseResponse struct {
	Jobs json.RawMessage `json:"jobs"`
	Meta struct {
		Total *int `json:"total"`
	} `json:"meta"`
}

type greenhouseJobPayload struct {
	AbsoluteURL         string          `json:"absolute_url"`
	InternalJobID       *int64          `json:"internal_job_id"`
	Location            json.RawMessage `json:"location"`
	Metadata            json.RawMessage `json:"metadata"`
	ID                  int64           `json:"id"`
	UpdatedAt           string          `json:"updated_at"`
	RequisitionID       string          `json:"requisition_id"`
	Title               string          `json:"title"`
	CompanyName         string          `json:"company_name"`
	FirstPublished      string          `json:"first_published"`
	Language            string          `json:"language"`
	ApplicationDeadline string          `json:"application_deadline"`
	DataCompliance      json.RawMessage `json:"data_compliance"`
	Content             string          `json:"content"`
}

func updateAllJobs(
	ctx context.Context,
	database *db.DB,
	client *http.Client,
	limiter *rate.Limiter,
	workers int,
) error {
	companies, err := database.Companies(ctx)
	if err != nil {
		return err
	}
	if len(companies) == 0 {
		log.Printf("No companies to update; discover companies first")
		return nil
	}

	jobs := make(chan string)
	errs := make(chan error, len(companies))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slug := range jobs {
				count, err := updateCompanyJobs(ctx, database, client, limiter, slug)
				if err != nil {
					log.Printf("%s: job update failed: %v", slug, err)
					errs <- err
					continue
				}
				log.Printf("%s: indexed %d jobs", slug, count)
			}
		}()
	}

sendCompanies:
	for _, slug := range companies {
		select {
		case jobs <- slug:
		case <-ctx.Done():
			break sendCompanies
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)

	failed := 0
	var firstErr error
	for err := range errs {
		failed++
		if firstErr == nil {
			firstErr = err
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d company job updates failed: %w", failed, len(companies), firstErr)
	}
	return nil
}

func updateCompanyJobs(
	ctx context.Context,
	database *db.DB,
	client *http.Client,
	limiter *rate.Limiter,
	slug string,
) (int, error) {
	requestURL := fmt.Sprintf(jobListURL, url.PathEscape(slug))
	backoff := 500 * time.Millisecond
	var response *greenhouseResponse

	for attempt := 0; attempt < 4; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return 0, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return 0, fmt.Errorf("create jobs request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		res, err := client.Do(req)
		if err != nil && res != nil && res.Body != nil {
			res.Body.Close()
		}
		if err == nil {
			if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
				res.Body.Close()
				err = fmt.Errorf("jobs endpoint returned status %d", res.StatusCode)
			} else if res.StatusCode != http.StatusOK {
				res.Body.Close()
				return 0, fmt.Errorf("jobs endpoint returned status %d", res.StatusCode)
			} else {
				var parsed greenhouseResponse
				decoder := json.NewDecoder(io.LimitReader(res.Body, maxJobsBody))
				decodeErr := decoder.Decode(&parsed)
				if decodeErr == nil {
					var extra json.RawMessage
					if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
						if err == nil {
							decodeErr = errors.New("jobs response contains multiple JSON values")
						} else {
							decodeErr = err
						}
					}
				}
				closeErr := res.Body.Close()
				if decodeErr != nil {
					return 0, fmt.Errorf("decode jobs response: %w", decodeErr)
				}
				if closeErr != nil {
					return 0, fmt.Errorf("close jobs response: %w", closeErr)
				}
				response = &parsed
				break
			}
		}

		if attempt == 3 {
			return 0, fmt.Errorf("fetch jobs after retries: %w", err)
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	if response == nil {
		return 0, errors.New("jobs request completed without a response")
	}
	if len(response.Jobs) == 0 || !strings.HasPrefix(strings.TrimSpace(string(response.Jobs)), "[") {
		return 0, errors.New("jobs response did not contain a jobs array; existing jobs were kept")
	}
	var responseJobs []json.RawMessage
	if err := json.Unmarshal(response.Jobs, &responseJobs); err != nil {
		return 0, fmt.Errorf("decode jobs array: %w", err)
	}
	if response.Meta.Total == nil || *response.Meta.Total < 0 {
		return 0, errors.New("jobs response did not contain a valid meta.total; existing jobs were kept")
	}
	if *response.Meta.Total != len(responseJobs) {
		return 0, fmt.Errorf("incomplete jobs response: meta.total=%d but received %d; existing jobs were kept",
			*response.Meta.Total, len(responseJobs))
	}

	parsedJobs := make([]db.GreenhouseJob, 0, len(responseJobs))
	for _, raw := range responseJobs {
		var payload greenhouseJobPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return 0, fmt.Errorf("decode job: %w", err)
		}
		if payload.ID <= 0 {
			return 0, errors.New("job response contains an invalid id; existing jobs were kept")
		}
		var location struct {
			Name string `json:"name"`
		}
		if len(payload.Location) > 0 && string(payload.Location) != "null" {
			if err := json.Unmarshal(payload.Location, &location); err != nil {
				return 0, fmt.Errorf("decode location for job %d: %w", payload.ID, err)
			}
		}
		parsedJobs = append(parsedJobs, db.GreenhouseJob{
			ID:                  payload.ID,
			InternalJobID:       payload.InternalJobID,
			Title:               payload.Title,
			CompanyName:         payload.CompanyName,
			LocationName:        location.Name,
			LocationJSON:        jsonOrNull(payload.Location),
			AbsoluteURL:         payload.AbsoluteURL,
			UpdatedAt:           payload.UpdatedAt,
			FirstPublished:      payload.FirstPublished,
			RequisitionID:       payload.RequisitionID,
			Language:            payload.Language,
			ApplicationDeadline: payload.ApplicationDeadline,
			Metadata:            jsonOrNull(payload.Metadata),
			DataCompliance:      jsonOrNull(payload.DataCompliance),
			Content:             payload.Content,
			RawJSON:             string(raw),
		})
	}

	if err := database.ReplaceCompanyJobs(ctx, slug, parsedJobs); err != nil {
		return 0, err
	}
	return len(parsedJobs), nil
}

func jsonOrNull(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	return string(raw)
}

func plainText(content string) string {
	content = stdhtml.UnescapeString(content)
	content = htmlTagPattern.ReplaceAllString(content, " ")
	content = stdhtml.UnescapeString(content)
	return strings.Join(strings.Fields(content), " ")
}

type browsePage struct {
	Query               string
	IncludeDescriptions bool
	SavedOnly           bool
	Page                int
	TotalPages          int
	Total               int
	Jobs                []db.GreenhouseJob
}

type browseHome struct {
	SavedOnly     bool
	BookmarkCount int
}

var browsePageTemplate = template.Must(template.New("browse").Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>Greenhouse Jobs</title>
	<script src="https://unpkg.com/htmx.org@2.0.4"></script>
	<style>
		:root { color-scheme: light; --ink: #172033; --muted: #64748b; --line: #e2e8f0; --accent: #2457d6; --surface: #fff; --canvas: #f5f7fb; }
		* { box-sizing: border-box; }
		body { margin: 0; background: var(--canvas); color: var(--ink); font: 16px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
		header { background: var(--surface); border-bottom: 1px solid var(--line); }
		.header-inner, main { width: min(1050px, calc(100% - 2rem)); margin: 0 auto; }
		.header-inner { padding: 2.4rem 0 1.8rem; }
		h1 { margin: 0; font-size: clamp(1.8rem, 4vw, 2.5rem); letter-spacing: -.04em; }
		header p { margin: .5rem 0 0; color: var(--muted); }
		main { padding: 1.5rem 0 4rem; }
		.search-panel { padding: 1.2rem; background: var(--surface); border: 1px solid var(--line); border-radius: 12px; box-shadow: 0 2px 8px #17203308; }
		.tabs { display: flex; gap: .5rem; margin-bottom: 1rem; }
		.tab { display: inline-flex; gap: .5rem; align-items: center; padding: .55rem .85rem; border-radius: 8px; color: var(--muted); font-weight: 600; text-decoration: none; }
		.tab:hover { background: white; }
		.tab.active { background: #e8efff; color: var(--accent); }
		.count { min-width: 1.5rem; padding: .05rem .4rem; border-radius: 1rem; background: #e2e8f0; color: #475569; text-align: center; font-size: .8rem; }
		.search-label { display: block; margin-bottom: .45rem; font-weight: 650; }
		.search-row { display: flex; gap: .6rem; }
		input[type=search] { min-width: 0; flex: 1; padding: .8rem .9rem; border: 1px solid #cbd5e1; border-radius: 8px; font: inherit; }
		input[type=search]:focus { outline: 3px solid #2457d633; border-color: var(--accent); }
		button, .button { display: inline-flex; align-items: center; justify-content: center; gap: .35rem; padding: .65rem .95rem; border: 1px solid var(--line); border-radius: 8px; background: white; color: var(--ink); font: inherit; font-weight: 600; text-decoration: none; cursor: pointer; }
		button.primary, .button.primary { border-color: var(--accent); background: var(--accent); color: white; }
		button:hover, .button:hover { filter: brightness(.96); }
		.search-help, .muted { color: var(--muted); font-size: .9rem; }
		.search-options { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-top: .8rem; }
		.check-option { display: flex; align-items: center; gap: .5rem; color: #475569; font-size: .92rem; }
		#results { min-height: 10rem; }
		.results-heading { display: flex; justify-content: space-between; align-items: baseline; gap: 1rem; margin: 1.5rem 0 .75rem; }
		.results-heading p { margin: 0; }
		.job { margin: .8rem 0; padding: 1.2rem; background: var(--surface); border: 1px solid var(--line); border-radius: 12px; box-shadow: 0 2px 6px #17203308; }
		.job-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; }
		.job h2 { margin: 0; font-size: 1.15rem; line-height: 1.35; }
		.job h2 a { color: var(--ink); text-decoration: none; }
		.job h2 a:hover { color: var(--accent); text-decoration: underline; }
		.job-meta { display: flex; flex-wrap: wrap; gap: .35rem .8rem; margin: .7rem 0 0; color: var(--muted); font-size: .9rem; }
		.job-meta span + span:before { content: "·"; margin-right: .8rem; color: #94a3b8; }
		.job-actions { display: flex; flex-wrap: wrap; gap: .6rem; align-items: center; margin-top: 1rem; }
		.bookmark-toggle { min-width: 7rem; }
		.bookmark-toggle[aria-label="Remove bookmark"] { color: #be123c; border-color: #fecdd3; background: #fff1f2; }
		details { margin-top: 1rem; border-top: 1px solid var(--line); padding-top: .8rem; }
		summary { color: #334155; font-weight: 600; cursor: pointer; }
		.description { max-height: 22rem; overflow: auto; margin: .7rem 0 0; color: #334155; white-space: pre-wrap; overflow-wrap: anywhere; font: .9rem/1.6 system-ui, sans-serif; }
		.raw-json { max-height: 22rem; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: .8rem; }
		nav { display: flex; justify-content: space-between; align-items: center; padding: 1rem 0; }
		.empty { padding: 3rem 1rem; text-align: center; color: var(--muted); background: white; border: 1px dashed #cbd5e1; border-radius: 12px; }
		.htmx-indicator { display: none; color: var(--muted); }
		.htmx-request .htmx-indicator, .htmx-request.htmx-indicator { display: inline; }
		@media (max-width: 600px) { .header-inner { padding: 1.6rem 0 1.3rem; } .search-panel { padding: .9rem; } .search-row { flex-direction: column; } .search-options { align-items: flex-start; flex-direction: column; } .job-top { display: block; } .job-top .button { margin-top: .8rem; } .results-heading { align-items: flex-start; flex-direction: column; gap: .15rem; } }
	</style>
</head>
<body>
	<header><div class="header-inner">
		<h1>Greenhouse Jobs</h1>
		<p>Explore open roles indexed from company career boards.</p>
	</div></header>
	<main>
		<nav class="tabs" aria-label="Job lists">
			<a class="tab {{if not .SavedOnly}}active{{end}}" href="/">All jobs</a>
			<a class="tab {{if .SavedOnly}}active{{end}}" href="/?saved=true">Saved <span id="bookmark-count" class="count"
				hx-get="/bookmarks/count" hx-trigger="load, bookmarksChanged from:body" hx-swap="innerHTML">{{.BookmarkCount}}</span></a>
		</nav>
		<form id="search-form" class="search-panel" action="/jobs" method="get"
			hx-get="/jobs" hx-trigger="submit, keyup changed delay:350ms from:input, change from:input"
			hx-target="#results" hx-swap="innerHTML" hx-push-url="true">
			<input type="hidden" name="saved" value="{{.SavedOnly}}">
			<label class="search-label" for="job-search">Find a role</label>
			<div class="search-row">
				<input id="job-search" type="search" name="q" placeholder="Try “intern”, “designer”, or “data analyst”"
					autocomplete="off">
				<button class="primary" type="submit">Search</button>
			</div>
			<div class="search-options">
				<label class="check-option"><input id="include-descriptions" type="checkbox" name="details" value="true">
					Include company, location, and description text</label>
				<span class="search-help">By default, matching is limited to job titles.</span>
			</div>
		</form>
		<div id="results" hx-get="/jobs" hx-include="#search-form"
			hx-trigger="load, bookmarksChanged from:body" hx-target="this" hx-swap="innerHTML">
			<p class="muted">Loading jobs…</p>
		</div>
	</main>
</body>
</html>`))

var browseResultsTemplate = template.Must(template.New("results").Funcs(template.FuncMap{
	"add":      func(a, b int) int { return a + b },
	"subtract": func(a, b int) int { return a - b },
	"urlquery": url.QueryEscape,
	"date": func(value string) string {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return value
		}
		return parsed.Format("Jan 2, 2006")
	},
	"bookmarkButton": func(companySlug string, jobID int64, isBookmarked, savedOnly bool) template.HTML {
		var label, action string
		if isBookmarked {
			label, action = "♥ Saved", "Remove bookmark"
		} else {
			label, action = "♡ Save job", "Save job"
		}
		target, swap, savedQuery := "closest .bookmark-toggle", "outerHTML", "false"
		if savedOnly {
			target, swap, savedQuery = "closest .job", "outerHTML", "true"
		}
		return template.HTML(fmt.Sprintf(
			`<button class="bookmark-toggle" type="button" aria-label="%s" title="%s"
				hx-post="/bookmarks/toggle?company=%s&amp;id=%d&amp;saved=%s"
				hx-target="%s" hx-swap="%s">%s</button>`,
			template.HTMLEscapeString(action), template.HTMLEscapeString(action),
			url.QueryEscape(companySlug), jobID, savedQuery, target, swap,
			template.HTMLEscapeString(label),
		))
	},
}).Parse(`{{if .Jobs}}
	<div class="results-heading">
		<p><strong>{{.Total}}</strong> {{if eq .Total 1}}job{{else}}jobs{{end}} found{{if .Query}} for <strong>“{{.Query}}”</strong>{{end}}</p>
		<p class="muted">Page {{.Page}} of {{.TotalPages}}</p>
	</div>
	{{range .Jobs}}
	<article class="job">
		<div class="job-top">
			<h2><a href="{{.AbsoluteURL}}" target="_blank" rel="noopener noreferrer">{{if .Title}}{{.Title}}{{else}}Untitled job{{end}}</a></h2>
			<div class="job-actions">
				{{bookmarkButton .CompanySlug .ID .IsBookmarked $.SavedOnly}}
				<a class="button primary" href="{{.AbsoluteURL}}" target="_blank" rel="noopener noreferrer">View job ↗</a>
			</div>
		</div>
		<div class="job-meta">
			<span>{{if .CompanyName}}{{.CompanyName}}{{else}}{{.CompanySlug}}{{end}}</span>
			{{if .LocationName}}<span>{{.LocationName}}</span>{{end}}
			{{if .FirstPublished}}<span>Posted {{date .FirstPublished}}</span>{{end}}
			{{if .RequisitionID}}<span>Req {{.RequisitionID}}</span>{{end}}
		</div>
		{{if .Content}}<details><summary>Preview job description</summary><div class="description">{{.Content}}</div></details>{{end}}
		<details><summary>Job data</summary><pre class="raw-json">{{.RawJSON}}</pre></details>
	</article>
	{{end}}
	<nav>
		{{if gt .Page 1}}<a class="button" href="/jobs?q={{urlquery .Query}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{subtract .Page 1}}" hx-get="/jobs?q={{urlquery .Query}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{subtract .Page 1}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">← Previous</a>{{else}}<span></span>{{end}}
		{{if lt .Page .TotalPages}}<a class="button" href="/jobs?q={{urlquery .Query}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{add .Page 1}}" hx-get="/jobs?q={{urlquery .Query}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{add .Page 1}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">Next →</a>{{end}}
	</nav>
{{else}}
	<div class="empty"><strong>{{if .SavedOnly}}No saved jobs yet{{else}}No matching jobs{{end}}</strong><p>{{if .SavedOnly}}Save a job from the All jobs list and it will appear here.{{else}}Try a different title or enable description search for broader results.{{end}}</p></div>
{{end}}`))

func serveJobBrowser(ctx context.Context, database *db.DB, address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		count, err := database.BookmarkCount(r.Context())
		if err != nil {
			http.Error(w, "could not load bookmarks", http.StatusInternalServerError)
			log.Printf("count bookmarks: %v", err)
			return
		}
		if err := browsePageTemplate.Execute(w, browseHome{
			SavedOnly:     r.URL.Query().Get("saved") == "true",
			BookmarkCount: count,
		}); err != nil {
			log.Printf("render browser page: %v", err)
		}
	})
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		includeDescriptions := r.URL.Query().Get("details") == "true"
		savedOnly := r.URL.Query().Get("saved") == "true"
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			page = 1
		}
		if page > 1_000_000 {
			page = 1_000_000
		}
		jobs, total, err := database.ListJobs(r.Context(), query, includeDescriptions, savedOnly, jobPageSize, (page-1)*jobPageSize)
		if err != nil {
			http.Error(w, "could not load jobs", http.StatusInternalServerError)
			log.Printf("load jobs: %v", err)
			return
		}
		totalPages := (total + jobPageSize - 1) / jobPageSize
		if totalPages == 0 {
			totalPages = 1
		}
		if page > totalPages {
			page = totalPages
			jobs, _, err = database.ListJobs(r.Context(), query, includeDescriptions, savedOnly, jobPageSize, (page-1)*jobPageSize)
			if err != nil {
				http.Error(w, "could not load jobs", http.StatusInternalServerError)
				log.Printf("load jobs page: %v", err)
				return
			}
		}
		for i := range jobs {
			jobs[i].Content = plainText(jobs[i].Content)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := browseResultsTemplate.Execute(w, browsePage{
			Query: query, IncludeDescriptions: includeDescriptions, SavedOnly: savedOnly,
			Page: page, TotalPages: totalPages, Total: total, Jobs: jobs,
		}); err != nil {
			log.Printf("render jobs: %v", err)
		}
	})
	mux.HandleFunc("/bookmarks/toggle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		company := strings.TrimSpace(r.URL.Query().Get("company"))
		jobID, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
		if company == "" || err != nil || jobID <= 0 {
			http.Error(w, "invalid bookmark request", http.StatusBadRequest)
			return
		}
		saved, err := database.ToggleBookmark(r.Context(), company, jobID)
		if err != nil {
			http.Error(w, "could not update bookmark", http.StatusInternalServerError)
			log.Printf("toggle bookmark: %v", err)
			return
		}
		w.Header().Set("HX-Trigger", "bookmarksChanged")
		if r.URL.Query().Get("saved") == "true" && !saved {
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		action, label := "Save job", "♡ Save job"
		if saved {
			action, label = "Remove bookmark", "♥ Saved"
		}
		target := "closest .bookmark-toggle"
		if r.URL.Query().Get("saved") == "true" {
			target = "closest .job"
		}
		if _, err := fmt.Fprintf(w, `<button class="bookmark-toggle" type="button" aria-label="%s" title="%s"
			hx-post="/bookmarks/toggle?company=%s&amp;id=%d" hx-target="%s" hx-swap="outerHTML">%s</button>`,
			template.HTMLEscapeString(action), template.HTMLEscapeString(action),
			url.QueryEscape(company), jobID, target, template.HTMLEscapeString(label)); err != nil {
			log.Printf("write bookmark response: %v", err)
		}
	})
	mux.HandleFunc("/bookmarks/count", func(w http.ResponseWriter, r *http.Request) {
		count, err := database.BookmarkCount(r.Context())
		if err != nil {
			http.Error(w, "could not load bookmark count", http.StatusInternalServerError)
			log.Printf("count bookmarks: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, count)
	})

	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("stop browser: %v", err)
		}
	}()

	log.Printf("Job browser listening at http://%s", address)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
		return nil
	}
	return err
}
