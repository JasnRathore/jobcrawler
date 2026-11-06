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
)

const (
	jobListURL  = baseURL + "/jobs?content=true"
	jobPageSize = 50
	maxJobsBody = 64 << 20
)

var htmlTagPattern = regexp.MustCompile(`<[^>]*>`)

type companyTarget struct {
	provider string
	slug     string
}

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
	Department          string          `json:"department"`
	EmploymentType      string          `json:"employment_type"`
	WorkplaceType       string          `json:"workplace_type"`
	Education           string          `json:"education"`
	Experience          string          `json:"experience"`
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
	pool *proxyPool,
	limiter *requestLimiter,
	workers int,
) error {
	targets := make([]companyTarget, 0)
	for _, provider := range registeredProviders() {
		companies, err := database.CompaniesForProvider(ctx, provider)
		if err != nil {
			return err
		}
		for _, slug := range companies {
			targets = append(targets, companyTarget{provider: provider, slug: slug})
		}
	}
	return updateJobTargets(ctx, database, targets, workers, func(ctx context.Context, target companyTarget) (int, error) {
		return updateProviderCompanyJobs(ctx, database, client, pool, limiter, target.provider, target.slug)
	})
}

func updateJobTargets(
	ctx context.Context,
	database *db.DB,
	targets []companyTarget,
	workers int,
	update func(context.Context, companyTarget) (int, error),
) error {
	completed, err := database.JobUpdateCheckpoints(ctx)
	if err != nil {
		return err
	}
	completedSet := make(map[companyTarget]struct{}, len(completed))
	for _, checkpoint := range completed {
		completedSet[companyTarget{provider: checkpoint.Provider, slug: checkpoint.CompanySlug}] = struct{}{}
	}

	pending := make([]companyTarget, 0, len(targets))
	for _, target := range targets {
		if _, done := completedSet[target]; !done {
			pending = append(pending, target)
		}
	}
	if len(pending) == 0 {
		if len(targets) == 0 {
			log.Printf("No companies to update; discover companies first")
		} else {
			log.Printf("All %d company job updates were already checkpointed", len(targets))
		}
		return database.ClearJobUpdateCheckpoints(ctx)
	}

	jobs := make(chan companyTarget)
	errs := make(chan error, len(pending))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				count, err := update(ctx, target)
				if err != nil {
					log.Printf("%s/%s: job update failed: %v", target.provider, target.slug, err)
					errs <- err
					continue
				}
				if err := database.SaveJobUpdateCheckpoint(ctx, target.provider, target.slug); err != nil {
					log.Printf("%s/%s: could not save job update checkpoint: %v", target.provider, target.slug, err)
					errs <- err
					continue
				}
				log.Printf("%s/%s: indexed %d jobs", target.provider, target.slug, count)
			}
		}()
	}

sendCompanies:
	for _, target := range pending {
		select {
		case jobs <- target:
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
		return fmt.Errorf("%d of %d company job updates failed: %w", failed, len(pending), firstErr)
	}
	if err := database.ClearJobUpdateCheckpoints(ctx); err != nil {
		return err
	}
	return nil
}

func updateProviderCompanyJobs(
	ctx context.Context,
	database *db.DB,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	provider, slug string,
) (int, error) {
	return updateProviderJobs(ctx, database, client, pool, limiter, provider, slug)
}

func updateCompanyJobs(
	ctx context.Context,
	database *db.DB,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	slug string,
) (int, error) {
	parsedJobs, err := fetchGreenhouseJobs(ctx, client, pool, limiter, slug)
	if err != nil {
		return 0, err
	}
	if err := database.ReplaceCompanyJobs(ctx, slug, parsedJobs); err != nil {
		return 0, err
	}
	return len(parsedJobs), nil
}

func fetchGreenhouseJobs(
	ctx context.Context,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	slug string,
) ([]db.Job, error) {
	requestURL := fmt.Sprintf(jobListURL, url.PathEscape(slug))
	backoff := 500 * time.Millisecond
	var response *greenhouseResponse

	transientAttempts := 0
	rateLimitAttempts := 0
	const maxTransientAttempts = 4
	const maxRateLimitAttempts = 4

	for {
		lease, leaseErr := acquireLease(ctx, pool, client)
		if leaseErr != nil {
			return nil, leaseErr
		}
		if err := limiter.Wait(ctx); err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return nil, fmt.Errorf("create jobs request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		res, err := lease.client.Do(req)
		if err != nil {
			if res != nil && res.Body != nil {
				res.Body.Close()
			}
			lease.release(proxyOutcomeFailed, 0)
			transientAttempts++
			if transientAttempts >= maxTransientAttempts {
				return nil, fmt.Errorf("fetch jobs after retries: %w", err)
			}
		} else {
			if isGreenhouseRateLimited(res.StatusCode) {
				res.Body.Close()
				retryAfter := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
				lease.release(proxyOutcomeRateLimited, retryAfter)
				err = fmt.Errorf("jobs endpoint returned status %d", res.StatusCode)
				rateLimitAttempts++
				if pool != nil && !lease.directFallback {
					// Rate limiting is per egress IP; rotate to a fresh peer.
					if rateLimitAttempts >= maxRateLimitAttempts {
						return nil, fmt.Errorf("fetch jobs after retries: %w", err)
					}
					continue
				}
				cooldownDuration := limiter.StartCooldownForStatusAndRetryAfter("greenhouse", res.StatusCode, retryAfter)
				if rateLimitAttempts >= maxRateLimitAttempts {
					return nil, fmt.Errorf("fetch jobs after retries: %w", err)
				}
				if cooldownDuration > 0 {
					continue
				}
			} else if res.StatusCode >= 500 {
				res.Body.Close()
				lease.release(proxyOutcomeFailed, 0)
				err = fmt.Errorf("jobs endpoint returned status %d", res.StatusCode)
				transientAttempts++
				if transientAttempts >= maxTransientAttempts {
					return nil, fmt.Errorf("fetch jobs after retries: %w", err)
				}
			} else if res.StatusCode != http.StatusOK {
				res.Body.Close()
				lease.release(proxyOutcomeFailed, 0)
				return nil, fmt.Errorf("jobs endpoint returned status %d", res.StatusCode)
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
					lease.release(proxyOutcomeFailed, 0)
					return nil, fmt.Errorf("decode jobs response: %w", decodeErr)
				}
				if closeErr != nil {
					lease.release(proxyOutcomeFailed, 0)
					return nil, fmt.Errorf("close jobs response: %w", closeErr)
				}
				lease.release(proxyOutcomeOK, 0)
				response = &parsed
				break
			}
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if response == nil {
		return nil, errors.New("jobs request completed without a response")
	}
	if len(response.Jobs) == 0 || !strings.HasPrefix(strings.TrimSpace(string(response.Jobs)), "[") {
		return nil, errors.New("jobs response did not contain a jobs array; existing jobs were kept")
	}
	var responseJobs []json.RawMessage
	if err := json.Unmarshal(response.Jobs, &responseJobs); err != nil {
		return nil, fmt.Errorf("decode jobs array: %w", err)
	}
	if response.Meta.Total == nil || *response.Meta.Total < 0 {
		return nil, errors.New("jobs response did not contain a valid meta.total; existing jobs were kept")
	}
	if *response.Meta.Total != len(responseJobs) {
		return nil, fmt.Errorf("incomplete jobs response: meta.total=%d but received %d; existing jobs were kept",
			*response.Meta.Total, len(responseJobs))
	}

	parsedJobs := make([]db.Job, 0, len(responseJobs))
	for _, raw := range responseJobs {
		var payload greenhouseJobPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("decode job: %w", err)
		}
		if payload.ID <= 0 {
			return nil, errors.New("job response contains an invalid id; existing jobs were kept")
		}
		var location struct {
			Name string `json:"name"`
		}
		if len(payload.Location) > 0 && string(payload.Location) != "null" {
			if err := json.Unmarshal(payload.Location, &location); err != nil {
				return nil, fmt.Errorf("decode location for job %d: %w", payload.ID, err)
			}
		}
		attributes, err := json.Marshal(map[string]any{
			"metadata": payload.Metadata, "data_compliance": payload.DataCompliance,
			"requisition_id": payload.RequisitionID, "language": payload.Language,
			"application_deadline": payload.ApplicationDeadline, "internal_job_id": payload.InternalJobID,
		})
		if err != nil {
			return nil, fmt.Errorf("normalize attributes for job %d: %w", payload.ID, err)
		}
		parsedJobs = append(parsedJobs, db.Job{
			ID:             payload.ID,
			Title:          payload.Title,
			CompanyName:    payload.CompanyName,
			LocationName:   location.Name,
			LocationJSON:   jsonOrNull(payload.Location),
			DetailsURL:     payload.AbsoluteURL,
			UpdatedAt:      payload.UpdatedAt,
			FirstPublished: payload.FirstPublished,
			EmploymentType: payload.EmploymentType,
			WorkplaceType:  normalizeWorkplaceType(payload.WorkplaceType),
			Department:     payload.Department,
			Education:      payload.Education,
			Experience:     payload.Experience,
			Description:    plainText(payload.Content),
			AttributesJSON: string(attributes),
		})
	}
	return parsedJobs, nil
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
	Location            string
	PostedAfter         string
	PostedBefore        string
	EmploymentType      string
	WorkplaceType       string
	IncludeDescriptions bool
	SavedOnly           bool
	Page                int
	TotalPages          int
	Total               int
	Jobs                []db.Job
}

type browseHome struct {
	SavedOnly           bool
	BookmarkCount       int
	Query               string
	Location            string
	PostedAfter         string
	PostedBefore        string
	EmploymentType      string
	WorkplaceType       string
	IncludeDescriptions bool
}

var browsePageTemplate = template.Must(template.New("browse").Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>Job Browser</title>
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
		.filter-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .7rem; margin-top: .9rem; }
		.filter-row label { display: grid; gap: .3rem; color: #475569; font-size: .88rem; font-weight: 600; }
		.filter-row input, .filter-row select { width: 100%; padding: .65rem; border: 1px solid #cbd5e1; border-radius: 8px; background: white; color: var(--ink); font: inherit; }
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
		.unavailable { color: #b91c1c; font-weight: 650; }
		.job-actions { display: flex; flex-wrap: wrap; gap: .6rem; align-items: center; margin-top: 1rem; }
		.bookmark-toggle { min-width: 7rem; }
		.bookmark-toggle[aria-label="Remove bookmark"] { color: #be123c; border-color: #fecdd3; background: #fff1f2; }
		details { margin-top: 1rem; border-top: 1px solid var(--line); padding-top: .8rem; }
		summary { color: #334155; font-weight: 600; cursor: pointer; }
		.description { max-height: 22rem; overflow: auto; margin: .7rem 0 0; color: #334155; white-space: pre-wrap; overflow-wrap: anywhere; font: .9rem/1.6 system-ui, sans-serif; }
		.job-attributes { max-height: 22rem; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: .8rem; }
		nav { display: flex; justify-content: space-between; align-items: center; padding: 1rem 0; }
		.empty { padding: 3rem 1rem; text-align: center; color: var(--muted); background: white; border: 1px dashed #cbd5e1; border-radius: 12px; }
		.htmx-indicator { display: none; color: var(--muted); }
		.htmx-request .htmx-indicator, .htmx-request.htmx-indicator { display: inline; }
		@media (max-width: 600px) { .header-inner { padding: 1.6rem 0 1.3rem; } .search-panel { padding: .9rem; } .search-row { flex-direction: column; } .search-options { align-items: flex-start; flex-direction: column; } .job-top { display: block; } .job-top .button { margin-top: .8rem; } .results-heading { align-items: flex-start; flex-direction: column; gap: .15rem; } }
	</style>
</head>
<body>
	<header><div class="header-inner">
		<h1>Browse jobs</h1>
		<p>Explore indexed openings across all supported job boards.</p>
	</div></header>
	<main>
		<nav class="tabs" aria-label="Job lists">
			<a class="tab {{if not .SavedOnly}}active{{end}}" href="/?saved=false">All jobs</a>
			<a class="tab {{if .SavedOnly}}active{{end}}" href="/?saved=true">Saved <span id="bookmark-count" class="count"
				hx-get="/bookmarks/count" hx-trigger="load, bookmarksChanged from:body" hx-swap="innerHTML">{{.BookmarkCount}}</span></a>
		</nav>
		<form id="search-form" class="search-panel" action="/jobs" method="get"
			hx-get="/jobs" hx-trigger="submit, keyup changed delay:350ms from:input, change from:input"
			hx-target="#results" hx-swap="innerHTML" hx-push-url="true">
			<input type="hidden" name="saved" value="{{.SavedOnly}}">
			<label class="search-label" for="job-search">Find a role</label>
			<div class="search-row">
				<input id="job-search" type="search" name="q" value="{{.Query}}" placeholder="Try “intern”, “designer”, or “data analyst”"
					autocomplete="off">
				<button class="primary" type="submit">Search</button>
			</div>
			<div class="filter-row">
				<label>Location <input name="location" value="{{.Location}}" placeholder="City, state, or country"></label>
				<label>Posted after <input type="date" name="posted_after" value="{{.PostedAfter}}"></label>
				<label>Posted before <input type="date" name="posted_before" value="{{.PostedBefore}}"></label>
				<label>Employment
					<select name="employment">
						<option value="">Any type</option>
						<option value="full-time" {{if eq .EmploymentType "full-time"}}selected{{end}}>Full-time</option>
						<option value="part-time" {{if eq .EmploymentType "part-time"}}selected{{end}}>Part-time</option>
						<option value="contract" {{if eq .EmploymentType "contract"}}selected{{end}}>Contract</option>
						<option value="internship" {{if eq .EmploymentType "internship"}}selected{{end}}>Internship</option>
					</select>
				</label>
				<label>Workplace
					<select name="workplace">
						<option value="">Any workplace</option>
						<option value="remote" {{if eq .WorkplaceType "remote"}}selected{{end}}>Remote</option>
						<option value="hybrid" {{if eq .WorkplaceType "hybrid"}}selected{{end}}>Hybrid</option>
						<option value="onsite" {{if eq .WorkplaceType "onsite"}}selected{{end}}>On-site</option>
					</select>
				</label>
			</div>
			<div class="search-options">
				<label class="check-option"><input id="include-descriptions" type="checkbox" name="details" value="true" {{if .IncludeDescriptions}}checked{{end}}>
					Also search job descriptions</label>
				<span class="search-help">Search matches title, company, and location by default.</span>
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
		for _, layout := range []string{time.RFC3339, "2006-01-02"} {
			parsed, err := time.Parse(layout, value)
			if err == nil {
				return parsed.Format("Jan 2, 2006")
			}
		}
		return value
	},
	"bookmarkButton": func(provider, companySlug, jobKey string, isBookmarked, savedOnly bool) template.HTML {
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
				hx-post="/bookmarks/toggle?provider=%s&amp;company=%s&amp;id=%s&amp;saved=%s"
				hx-target="%s" hx-swap="%s">%s</button>`,
			template.HTMLEscapeString(action), template.HTMLEscapeString(action),
			url.QueryEscape(provider), url.QueryEscape(companySlug), url.QueryEscape(jobKey), savedQuery, target, swap,
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
			<h2>{{if .DetailsURL}}<a href="{{.DetailsURL}}" target="_blank" rel="noopener noreferrer">{{end}}{{if .Title}}{{.Title}}{{else}}Untitled job{{end}}{{if .DetailsURL}}</a>{{end}}</h2>
			<div class="job-actions">
				{{bookmarkButton .Provider .CompanySlug .JobKey .IsBookmarked $.SavedOnly}}
				{{if .DetailsURL}}<a class="button primary" href="{{.DetailsURL}}" target="_blank" rel="noopener noreferrer">View job details ↗</a>{{end}}
			</div>
		</div>
		<div class="job-meta">
			<span>{{if .CompanyName}}{{.CompanyName}}{{else}}{{.CompanySlug}}{{end}}</span>
			{{if .LocationName}}<span>{{.LocationName}}</span>{{end}}
			{{if .FirstPublished}}<span>Posted {{date .FirstPublished}}</span>{{end}}
				{{if .EmploymentType}}<span>{{.EmploymentType}}</span>{{end}}
				{{if .WorkplaceType}}<span>{{.WorkplaceType}}</span>{{end}}
				{{if not .IsAvailable}}<span class="unavailable">No longer listed</span>{{end}}
			</div>
			{{if .Description}}<details><summary>Preview job description</summary><div class="description">{{.Description}}</div></details>{{end}}
			<details><summary>Listing details</summary>
				<p>Source: {{.Provider}} / {{.CompanySlug}}</p>
				{{if .ApplyURL}}<p><a href="{{.ApplyURL}}" target="_blank" rel="noopener noreferrer">Apply for this job ↗</a></p>{{end}}
				{{if .Department}}<p>Department: {{.Department}}</p>{{end}}
				{{if .Education}}<p>Education: {{.Education}}</p>{{end}}
				{{if .Experience}}<p>Experience: {{.Experience}}</p>{{end}}
				{{if .AttributesJSON}}				<pre class="job-attributes">{{.AttributesJSON}}</pre>{{end}}
			</details>
		</article>
		{{end}}
		<nav>
			{{if gt .Page 1}}<a class="button" href="/jobs?q={{urlquery .Query}}&amp;location={{urlquery .Location}}&amp;posted_after={{.PostedAfter}}&amp;posted_before={{.PostedBefore}}&amp;employment={{urlquery .EmploymentType}}&amp;workplace={{urlquery .WorkplaceType}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{subtract .Page 1}}" hx-get="/jobs?q={{urlquery .Query}}&amp;location={{urlquery .Location}}&amp;posted_after={{.PostedAfter}}&amp;posted_before={{.PostedBefore}}&amp;employment={{urlquery .EmploymentType}}&amp;workplace={{urlquery .WorkplaceType}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{subtract .Page 1}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">← Previous</a>{{else}}<span></span>{{end}}
			{{if lt .Page .TotalPages}}<a class="button" href="/jobs?q={{urlquery .Query}}&amp;location={{urlquery .Location}}&amp;posted_after={{.PostedAfter}}&amp;posted_before={{.PostedBefore}}&amp;employment={{urlquery .EmploymentType}}&amp;workplace={{urlquery .WorkplaceType}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{add .Page 1}}" hx-get="/jobs?q={{urlquery .Query}}&amp;location={{urlquery .Location}}&amp;posted_after={{.PostedAfter}}&amp;posted_before={{.PostedBefore}}&amp;employment={{urlquery .EmploymentType}}&amp;workplace={{urlquery .WorkplaceType}}&amp;details={{.IncludeDescriptions}}&amp;saved={{.SavedOnly}}&amp;page={{add .Page 1}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">Next →</a>{{end}}
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
			SavedOnly:           r.URL.Query().Get("saved") == "true",
			BookmarkCount:       count,
			Query:               r.URL.Query().Get("q"),
			Location:            r.URL.Query().Get("location"),
			PostedAfter:         r.URL.Query().Get("posted_after"),
			PostedBefore:        r.URL.Query().Get("posted_before"),
			EmploymentType:      r.URL.Query().Get("employment"),
			WorkplaceType:       r.URL.Query().Get("workplace"),
			IncludeDescriptions: r.URL.Query().Get("details") == "true",
		}); err != nil {
			log.Printf("render browser page: %v", err)
		}
	})
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		includeDescriptions := r.URL.Query().Get("details") == "true"
		savedOnly := r.URL.Query().Get("saved") == "true"
		filters := db.JobFilters{
			Query: query, Location: strings.TrimSpace(r.URL.Query().Get("location")),
			PostedAfter: r.URL.Query().Get("posted_after"), PostedBefore: r.URL.Query().Get("posted_before"),
			EmploymentType:      strings.TrimSpace(r.URL.Query().Get("employment")),
			WorkplaceType:       strings.TrimSpace(r.URL.Query().Get("workplace")),
			IncludeDescriptions: includeDescriptions,
		}
		if r.Header.Get("HX-Request") != "true" {
			count, err := database.BookmarkCount(r.Context())
			if err != nil {
				http.Error(w, "could not load bookmarks", http.StatusInternalServerError)
				log.Printf("count bookmarks: %v", err)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if err := browsePageTemplate.Execute(w, browseHome{
				SavedOnly: savedOnly, BookmarkCount: count, Query: query,
				Location: filters.Location, PostedAfter: filters.PostedAfter,
				PostedBefore: filters.PostedBefore, EmploymentType: filters.EmploymentType,
				WorkplaceType: filters.WorkplaceType, IncludeDescriptions: includeDescriptions,
			}); err != nil {
				log.Printf("render browser page: %v", err)
			}
			return
		}
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			page = 1
		}
		if page > 1_000_000 {
			page = 1_000_000
		}
		jobs, total, err := database.ListJobs(r.Context(), filters, savedOnly, jobPageSize, (page-1)*jobPageSize)
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
			jobs, _, err = database.ListJobs(r.Context(), filters, savedOnly, jobPageSize, (page-1)*jobPageSize)
			if err != nil {
				http.Error(w, "could not load jobs", http.StatusInternalServerError)
				log.Printf("load jobs page: %v", err)
				return
			}
		}
		for i := range jobs {
			jobs[i].Description = plainText(jobs[i].Description)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := browseResultsTemplate.Execute(w, browsePage{
			Query: query, Location: filters.Location, PostedAfter: filters.PostedAfter, PostedBefore: filters.PostedBefore,
			EmploymentType: filters.EmploymentType, WorkplaceType: filters.WorkplaceType,
			IncludeDescriptions: includeDescriptions, SavedOnly: savedOnly,
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
		jobID := strings.TrimSpace(r.URL.Query().Get("id"))
		provider := strings.TrimSpace(r.URL.Query().Get("provider"))
		if company == "" || jobID == "" || !supportedProvider(provider) {
			http.Error(w, "invalid bookmark request", http.StatusBadRequest)
			return
		}
		saved, err := database.ToggleProviderBookmark(r.Context(), provider, company, jobID)
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
		savedQuery := "false"
		if r.URL.Query().Get("saved") == "true" {
			target, savedQuery = "closest .job", "true"
		}
		if _, err := fmt.Fprintf(w, `<button class="bookmark-toggle" type="button" aria-label="%s" title="%s"
			hx-post="/bookmarks/toggle?provider=%s&amp;company=%s&amp;id=%s&amp;saved=%s" hx-target="%s" hx-swap="outerHTML">%s</button>`,
			template.HTMLEscapeString(action), template.HTMLEscapeString(action),
			url.QueryEscape(provider), url.QueryEscape(company), url.QueryEscape(jobID), savedQuery, target, template.HTMLEscapeString(label)); err != nil {
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

func providerDisplayName(provider string) string {
	switch provider {
	case providerAshbyHQ:
		return "Ashby"
	case providerGreenhouse:
		return "Greenhouse"
	case providerLever:
		return "Lever"
	case providerLeverEU:
		return "Lever EU"
	case providerWorkable:
		return "Workable"
	default:
		return provider
	}
}
