package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"db"
)

type dashboardPage struct {
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
	SelectedJob         db.Job
	HasSelectedJob      bool
	SelectedProvider    string
	SelectedCompany     string
	SelectedJobKey      string
}

type dashboardHome struct {
	SavedOnly           bool
	BookmarkCount       int
	Query               string
	Location            string
	PostedAfter         string
	PostedBefore        string
	EmploymentType      string
	WorkplaceType       string
	IncludeDescriptions bool
	SelectedProvider    string
	SelectedCompany     string
	SelectedJobKey      string
}

var dashboardPageTemplate = template.Must(template.New("dashboard").Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>Job Dashboard</title>
	<script src="https://unpkg.com/htmx.org@2.0.4"></script>
	<style>
		:root { color-scheme: dark; --ink: #e6e8eb; --muted: #92959d; --line: #292a2e; --accent: #91a0ff; --accent-soft: #1d2030; --surface: #111214; --surface-raised: #17181b; --surface-hover: #1c1d21; --canvas: #090a0c; --danger: #f08b98; }
		* { box-sizing: border-box; }
		html, body { width: 100%; height: 100%; overflow: hidden; }
		body { margin: 0; background: var(--canvas); color: var(--ink); font: 14px/1.5 Inter, "Segoe UI", system-ui, sans-serif; -webkit-font-smoothing: antialiased; }
		a { color: inherit; }
		.dashboard-shell { display: grid; width: 100%; height: 100vh; height: 100dvh; min-height: 0; grid-template-columns: 190px minmax(0, 1fr); overflow: hidden; }
		.sidebar { display: flex; min-height: 0; flex-direction: column; gap: 1.5rem; overflow: hidden; padding: 1rem .7rem; background: #0b0c0e; border-right: 1px solid var(--line); }
		.sidebar-label { margin: 0 .55rem .45rem; color: #71747c; font-size: .68rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }
		.sidebar-nav { display: grid; gap: .2rem; }
		.sidebar-link { display: flex; align-items: center; gap: .65rem; min-width: 0; padding: .55rem .6rem; border-radius: 6px; color: var(--muted); text-decoration: none; transition: background .15s, color .15s; }
		.sidebar-link:hover { background: var(--surface-hover); color: var(--ink); }
		.sidebar-link.active { background: #202125; color: var(--ink); }
		.sidebar-icon { width: 1rem; color: #858891; text-align: center; }
		.sidebar-link.active .sidebar-icon { color: var(--accent); }
		.count { min-width: 1.4rem; margin-left: auto; padding: .05rem .35rem; border-radius: 1rem; background: var(--surface-raised); color: var(--muted); text-align: center; font-size: .75rem; }
		.sidebar-footer { margin-top: auto; padding: .6rem; color: #71747c; font-size: .75rem; }
		.dashboard-main { display: flex; min-width: 0; min-height: 0; height: 100vh; height: 100dvh; flex-direction: column; overflow: hidden; }
		.search-panel { flex: 0 0 auto; padding: 1.25rem 1.25rem 1rem; border: 0; border-radius: 0; }
		.search-row { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto; gap: .5rem; }
		.search-row input { min-width: 0; }
		.filter-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); align-items: end; gap: .55rem; margin-top: .7rem; }
		.filter-row label { display: grid; gap: .25rem; color: var(--muted); font-size: .78rem; font-weight: 550; }
		.filter-row input, .filter-row select, input[type=search] { width: 100%; padding: .55rem .65rem; border: 1px solid var(--line); border-radius: 6px; background: #0d0e10; color: var(--ink); font: inherit; }
		input[type=search]::placeholder, .filter-row input::placeholder { color: #70737b; }
		input[type=search]:focus, .filter-row input:focus, .filter-row select:focus { outline: 2px solid #91a0ff55; border-color: var(--accent); }
		button, .button { display: inline-flex; align-items: center; justify-content: center; gap: .35rem; padding: .55rem .8rem; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); color: var(--ink); font: inherit; font-weight: 550; text-decoration: none; cursor: pointer; transition: background .15s, border-color .15s, color .15s; }
		button.primary, .button.primary { border-color: var(--accent); background: var(--accent); color: #101116; }
		button:hover, .button:hover { background: var(--surface-hover); border-color: #3a3b42; }
		button.primary:hover, .button.primary:hover { background: #a4afff; border-color: #a4afff; }
		:where(button, a, input, select, summary):focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
		input[type=checkbox] { accent-color: var(--accent); }
		.check-option, .search-help { color: var(--muted); font-size: .8rem; }
		.check-option { display: flex !important; min-height: 2.45rem; align-items: center; gap: .45rem !important; }
		.check-option input { width: auto; }
		.dashboard-workspace { display: grid; min-height: 0; flex: 1; grid-template-columns: minmax(220px, 280px) minmax(320px, 1fr) 230px; overflow: hidden; border-top: 1px solid var(--line); }
		.job-list-pane, .job-detail-pane, .properties-pane { min-width: 0; }
		.job-list-pane { display: flex; min-height: 0; flex-direction: column; overflow: hidden; border-right: 1px solid var(--line); background: #0d0e10; }
		.pane-heading { display: flex; min-height: 3.4rem; align-items: center; justify-content: space-between; gap: .6rem; padding: .65rem .85rem; border-bottom: 1px solid var(--line); }
		.pane-heading h2 { margin: 0; font-size: .85rem; font-weight: 600; }
		.pane-heading p { margin: .1rem 0 0; color: var(--muted); font-size: .72rem; }
		.page-indicator { color: var(--muted); font-size: .7rem; white-space: nowrap; }
		.job-list { min-height: 0; flex: 1; overflow-y: auto; overscroll-behavior: contain; }
		.job-row { display: grid; gap: .18rem; padding: .75rem .85rem; border-bottom: 1px solid #202125; color: var(--muted); text-decoration: none; transition: background .15s, color .15s; }
		.job-row:hover { background: #17181b; color: var(--ink); }
		.job-row.active { background: #1b1c20; box-shadow: inset 2px 0 var(--accent); color: var(--ink); }
		.job-row-title { overflow: hidden; color: var(--ink); font-size: .82rem; font-weight: 600; text-overflow: ellipsis; white-space: nowrap; }
		.job-row-company { overflow: hidden; font-size: .76rem; text-overflow: ellipsis; white-space: nowrap; }
		.job-row-meta { overflow: hidden; color: #777a82; font-size: .7rem; text-overflow: ellipsis; white-space: nowrap; }
		.job-row-status { color: var(--danger); font-size: .7rem; }
		.pagination { display: flex; align-items: center; justify-content: space-between; gap: .4rem; padding: .65rem; border-top: 1px solid var(--line); }
		.pagination .button { padding: .4rem .55rem; font-size: .75rem; }
		.pagination-label { color: var(--muted); font-size: .7rem; }
		.job-detail-pane { min-height: 0; overflow-y: auto; overscroll-behavior: contain; padding: 1.35rem 1.5rem 2rem; background: #101113; }
		.detail-kicker { display: flex; align-items: center; gap: .5rem; margin-bottom: .85rem; color: var(--muted); font-size: .73rem; }
		.detail-dot { width: .42rem; height: .42rem; border-radius: 50%; background: var(--accent); }
		.detail-heading { display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem; }
		.detail-heading h2 { margin: 0; font-size: 1.35rem; font-weight: 600; letter-spacing: -.03em; line-height: 1.3; }
		.detail-company { margin: .4rem 0 1.2rem; color: var(--muted); font-size: .85rem; }
		.detail-actions { display: flex; flex-wrap: wrap; gap: .45rem; margin-bottom: 1.25rem; }
		.detail-section { border-top: 1px solid var(--line); padding-top: 1rem; }
		.detail-section h3 { margin: 0 0 .7rem; font-size: .8rem; font-weight: 600; }
		.description { color: #b8bac1; font-size: .85rem; line-height: 1.7; white-space: pre-wrap; overflow-wrap: anywhere; }
		.job-attributes { max-height: 22rem; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: .75rem; }
		.properties-pane { min-height: 0; overflow-y: auto; overscroll-behavior: contain; padding: .9rem .8rem; background: #131416; }
		.properties-heading { margin: .15rem .35rem .8rem; color: var(--ink); font-size: .8rem; font-weight: 600; }
		.property { padding: .55rem .35rem; border-bottom: 1px solid #242529; }
		.property:last-child { border-bottom: 0; }
		.property-label { display: block; margin-bottom: .15rem; color: #777a82; font-size: .68rem; }
		.property-value { color: #d7d8dc; font-size: .76rem; overflow-wrap: anywhere; }
		.property-value.unavailable { color: var(--danger); }
		.empty { padding: 2.5rem 1rem; color: var(--muted); text-align: center; }
		.empty strong { display: block; margin-bottom: .3rem; color: var(--ink); font-size: .85rem; }
		.empty p { margin: 0; font-size: .78rem; }
		.empty-main { display: grid; min-height: 15rem; place-items: center; padding: 1rem; }
		.htmx-indicator { display: none; color: var(--muted); }
		.htmx-request .htmx-indicator, .htmx-request.htmx-indicator { display: inline; }
		@media (max-width: 1120px) {
			.dashboard-workspace { grid-template-columns: minmax(220px, 280px) minmax(0, 1fr); grid-template-rows: minmax(0, 1fr) minmax(5rem, 9rem); }
			.properties-pane { display: grid; grid-column: 1 / -1; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .25rem .8rem; overflow-y: auto; border-top: 1px solid var(--line); }
			.properties-heading { grid-column: 1 / -1; }
		}
		@media (max-width: 760px) {
			.dashboard-shell { display: block; }
			.sidebar { height: 3.5rem; min-height: 3.5rem; flex-direction: row; align-items: center; justify-content: space-between; gap: .5rem; padding: .5rem .85rem; border-right: 0; border-bottom: 1px solid var(--line); }
			.sidebar-group { display: block; }
			.sidebar-label { display: none; }
			.sidebar-footer { display: none; }
			.sidebar-nav { display: flex; gap: .2rem; }
			.sidebar-link { padding: .4rem .55rem; font-size: .78rem; }
			.dashboard-main { height: calc(100vh - 3.5rem); height: calc(100dvh - 3.5rem); min-height: 0; }
			.dashboard-workspace { grid-template-columns: minmax(190px, .75fr) minmax(0, 1.25fr); }
		}
		@media (max-width: 560px) {
			.search-panel { padding: .8rem; }
			.filter-row { grid-template-columns: repeat(2, minmax(0, 1fr)); }
			.dashboard-workspace { grid-template-columns: minmax(0, 1fr); grid-template-rows: minmax(0, .35fr) minmax(0, .65fr) minmax(0, .25fr); }
			.job-list-pane { border-right: 0; border-bottom: 1px solid var(--line); }
			.job-detail-pane { padding: 1rem; }
			.properties-pane { display: block; border-top: 1px solid var(--line); }
			.detail-heading { display: block; }
			.detail-actions { margin-top: .85rem; }
		}
	</style>
</head>
<body>
	<div class="dashboard-shell">
		<aside class="sidebar">
			<div class="sidebar-group">
				<p class="sidebar-label">Workspace</p>
				<nav class="sidebar-nav" aria-label="Job views">
					<a class="sidebar-link {{if not .SavedOnly}}active{{end}}" href="/?saved=false"><span class="sidebar-icon">▦</span><span>All jobs</span></a>
					<a class="sidebar-link {{if .SavedOnly}}active{{end}}" href="/?saved=true"><span class="sidebar-icon">☆</span><span>Saved</span><span id="bookmark-count" class="count"
						hx-get="/bookmarks/count" hx-trigger="load, bookmarksChanged from:body" hx-swap="innerHTML">{{.BookmarkCount}}</span></a>
				</nav>
			</div>
			<div class="sidebar-footer">Indexed roles · Local workspace</div>
		</aside>
		<main class="dashboard-main">
		<form id="search-form" class="search-panel" action="/jobs" method="get"
			hx-get="/jobs" hx-trigger="submit, keyup changed delay:350ms from:input, change from:input"
			hx-target="#results" hx-swap="innerHTML" hx-push-url="true">
			<input type="hidden" name="saved" value="{{.SavedOnly}}">
			<div id="selection-fields">
				<input type="hidden" name="selected_provider" value="{{.SelectedProvider}}">
				<input type="hidden" name="selected_company" value="{{.SelectedCompany}}">
				<input type="hidden" name="selected_job" value="{{.SelectedJobKey}}">
			</div>
			<div class="search-row">
				<input id="job-search" type="search" name="q" value="{{.Query}}" aria-label="Role title, keywords, or company" placeholder="Role title, keywords, or company..."
					autocomplete="off">
				<input id="job-location" type="search" name="location" value="{{.Location}}" aria-label="City, country, or remote" placeholder="City, country, or remote..."
					autocomplete="off">
				<button class="primary" type="submit">Search</button>
			</div>
			<div class="filter-row">
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
				<label class="check-option"><input id="include-descriptions" type="checkbox" name="details" value="true" {{if .IncludeDescriptions}}checked{{end}}>
					Search descriptions</label>
			</div>
		</form>
		<section id="results" class="dashboard-workspace" aria-label="Job dashboard results" hx-get="/jobs" hx-include="#search-form"
			hx-trigger="load, bookmarksChanged from:body" hx-target="this" hx-swap="innerHTML">
			<div class="job-list-pane"><p class="empty">Loading jobs…</p></div>
			<section class="job-detail-pane" aria-live="polite"><p class="empty">Loading selected role…</p></section>
			<aside class="properties-pane"><p class="muted">Job properties</p></aside>
		</section>
		</main>
		</div>
</body>
</html>`))

var dashboardResultsTemplate = template.Must(template.New("dashboard-results").Funcs(template.FuncMap{
	"add":      func(a, b int) int { return a + b },
	"subtract": func(a, b int) int { return a - b },
	"date": func(value string) string {
		for _, layout := range []string{time.RFC3339, "2006-01-02"} {
			parsed, err := time.Parse(layout, value)
			if err == nil {
				return parsed.Format("Jan 2, 2006")
			}
		}
		return value
	},
	"providerName":      providerDisplayName,
	"selectURL":         dashboardSelectionURL,
	"pageURL":           dashboardPageURL,
	"dashboardApplyURL": dashboardApplyURL,
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
	<div class="job-list-pane">
		<div class="pane-heading">
			<div><h2>{{if .SavedOnly}}Saved jobs{{else}}All jobs{{end}}</h2><p>{{.Total}} matching roles</p></div>
			<span class="page-indicator">{{.Page}} / {{.TotalPages}}</span>
		</div>
		<div class="job-list">
			{{range .Jobs}}
			<a class="job-row {{if and (eq .Provider $.SelectedProvider) (eq .CompanySlug $.SelectedCompany) (eq .JobKey $.SelectedJobKey)}}active{{end}}"
				href="{{selectURL $ .}}" hx-get="{{selectURL $ .}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true"
				{{if and (eq .Provider $.SelectedProvider) (eq .CompanySlug $.SelectedCompany) (eq .JobKey $.SelectedJobKey)}}aria-current="true"{{end}}>
				<strong class="job-row-title">{{if .Title}}{{.Title}}{{else}}Untitled role{{end}}</strong>
				<span class="job-row-company">{{if .CompanyName}}{{.CompanyName}}{{else}}{{.CompanySlug}}{{end}}</span>
				<span class="job-row-meta">{{if .LocationName}}{{.LocationName}}{{else}}Location not listed{{end}}{{if .FirstPublished}} · {{date .FirstPublished}}{{end}}</span>
				{{if not .IsAvailable}}<span class="job-row-status">No longer listed</span>{{end}}
			</a>
			{{end}}
		</div>
		<nav class="pagination" aria-label="Job result pages">
			{{if gt .Page 1}}<a class="button" href="{{pageURL $ (subtract .Page 1)}}" hx-get="{{pageURL $ (subtract .Page 1)}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">← Prev</a>{{else}}<span></span>{{end}}
			<span class="pagination-label">{{.Total}} roles</span>
			{{if lt .Page .TotalPages}}<a class="button" href="{{pageURL $ (add .Page 1)}}" hx-get="{{pageURL $ (add .Page 1)}}" hx-target="#results" hx-swap="innerHTML" hx-push-url="true">Next →</a>{{else}}<span></span>{{end}}
		</nav>
	</div>
	<section class="job job-detail-pane" aria-label="Selected job">
		<div class="detail-kicker"><span class="detail-dot"></span>{{providerName .SelectedJob.Provider}}</div>
		<div class="detail-heading"><h2>{{if .SelectedJob.Title}}{{.SelectedJob.Title}}{{else}}Untitled role{{end}}</h2></div>
		<p class="detail-company">{{if .SelectedJob.CompanyName}}{{.SelectedJob.CompanyName}}{{else}}{{.SelectedJob.CompanySlug}}{{end}}{{if .SelectedJob.LocationName}} · {{.SelectedJob.LocationName}}{{end}}</p>
		<div class="detail-actions">
			{{bookmarkButton .SelectedJob.Provider .SelectedJob.CompanySlug .SelectedJob.JobKey .SelectedJob.IsBookmarked .SavedOnly}}
			{{if dashboardApplyURL .SelectedJob}}<a class="button primary" href="{{dashboardApplyURL .SelectedJob}}" target="_blank" rel="noopener noreferrer">Apply ↗</a>{{end}}
		</div>
		<div class="detail-section">
			<h3>Description</h3>
			{{if .SelectedJob.Description}}<div class="description">{{.SelectedJob.Description}}</div>{{else}}<p class="search-help">No description is available for this role.</p>{{end}}
		</div>
		{{if .SelectedJob.AttributesJSON}}<details class="detail-section"><summary>Additional listing data</summary><pre class="job-attributes">{{.SelectedJob.AttributesJSON}}</pre></details>{{end}}
	</section>
	<aside class="properties-pane" aria-label="Job properties">
		<h2 class="properties-heading">Properties</h2>
		<div class="property"><span class="property-label">Status</span><span class="property-value {{if not .SelectedJob.IsAvailable}}unavailable{{end}}">{{if .SelectedJob.IsAvailable}}Open{{else}}No longer listed{{end}}</span></div>
		<div class="property"><span class="property-label">Company</span><span class="property-value">{{if .SelectedJob.CompanyName}}{{.SelectedJob.CompanyName}}{{else}}{{.SelectedJob.CompanySlug}}{{end}}</span></div>
		<div class="property"><span class="property-label">Location</span><span class="property-value">{{if .SelectedJob.LocationName}}{{.SelectedJob.LocationName}}{{else}}Not listed{{end}}</span></div>
		<div class="property"><span class="property-label">Employment</span><span class="property-value">{{if .SelectedJob.EmploymentType}}{{.SelectedJob.EmploymentType}}{{else}}Not listed{{end}}</span></div>
		<div class="property"><span class="property-label">Workplace</span><span class="property-value">{{if .SelectedJob.WorkplaceType}}{{.SelectedJob.WorkplaceType}}{{else}}Not listed{{end}}</span></div>
		<div class="property"><span class="property-label">Posted</span><span class="property-value">{{if .SelectedJob.FirstPublished}}{{date .SelectedJob.FirstPublished}}{{else}}Not listed{{end}}</span></div>
		<div class="property"><span class="property-label">Source</span><span class="property-value">{{providerName .SelectedJob.Provider}}</span></div>
		{{if .SelectedJob.Department}}<div class="property"><span class="property-label">Department</span><span class="property-value">{{.SelectedJob.Department}}</span></div>{{end}}
		{{if .SelectedJob.Education}}<div class="property"><span class="property-label">Education</span><span class="property-value">{{.SelectedJob.Education}}</span></div>{{end}}
		{{if .SelectedJob.Experience}}<div class="property"><span class="property-label">Experience</span><span class="property-value">{{.SelectedJob.Experience}}</span></div>{{end}}
	</aside>
	<div id="selection-fields" hx-swap-oob="innerHTML">
		<input type="hidden" name="selected_provider" value="{{.SelectedProvider}}">
		<input type="hidden" name="selected_company" value="{{.SelectedCompany}}">
		<input type="hidden" name="selected_job" value="{{.SelectedJobKey}}">
	</div>
{{else}}
	<div class="job-list-pane">
		<div class="pane-heading"><div><h2>{{if .SavedOnly}}Saved jobs{{else}}All jobs{{end}}</h2><p>0 matching roles</p></div></div>
		<div class="empty"><strong>{{if .SavedOnly}}No saved jobs yet{{else}}No matching jobs{{end}}</strong><p>{{if .SavedOnly}}Save a role and it will appear here.{{else}}Try adjusting the search or filters.{{end}}</p></div>
	</div>
	<section class="job-detail-pane empty-main" aria-label="Selected job"><div class="empty"><strong>No role selected</strong><p>Matching roles will appear here when available.</p></div></section>
	<aside class="properties-pane" aria-label="Job properties"><h2 class="properties-heading">Properties</h2><p class="search-help">Select a role to view its details.</p></aside>
	<div id="selection-fields" hx-swap-oob="innerHTML">
		<input type="hidden" name="selected_provider" value="">
		<input type="hidden" name="selected_company" value="">
		<input type="hidden" name="selected_job" value="">
	</div>
{{end}}`))

func dashboardQueryValues(page dashboardPage) url.Values {
	values := url.Values{
		"q":             {page.Query},
		"location":      {page.Location},
		"posted_after":  {page.PostedAfter},
		"posted_before": {page.PostedBefore},
		"employment":    {page.EmploymentType},
		"workplace":     {page.WorkplaceType},
		"saved":         {strconv.FormatBool(page.SavedOnly)},
	}
	if page.IncludeDescriptions {
		values.Set("details", "true")
	}
	return values
}

func dashboardSelectionURL(page dashboardPage, job db.Job) string {
	values := dashboardQueryValues(page)
	values.Set("page", strconv.Itoa(page.Page))
	values.Set("selected_provider", job.Provider)
	values.Set("selected_company", job.CompanySlug)
	values.Set("selected_job", job.JobKey)
	return "/jobs?" + values.Encode()
}

func dashboardPageURL(page dashboardPage, pageNumber int) string {
	values := dashboardQueryValues(page)
	values.Set("page", strconv.Itoa(pageNumber))
	return "/jobs?" + values.Encode()
}

func dashboardApplyURL(job db.Job) string {
	rawURL := job.ApplyURL
	if rawURL == "" {
		rawURL = job.DetailsURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	switch {
	case strings.HasSuffix(parsed.Path, "/application"):
		parsed.Path = strings.TrimSuffix(parsed.Path, "/application")
	case strings.HasSuffix(parsed.Path, "/apply"):
		parsed.Path = strings.TrimSuffix(parsed.Path, "/apply")
	}
	return parsed.String()
}

func selectDashboardJob(jobs []db.Job, query url.Values) (db.Job, bool) {
	if len(jobs) == 0 {
		return db.Job{}, false
	}
	provider := query.Get("selected_provider")
	company := query.Get("selected_company")
	jobKey := query.Get("selected_job")
	if provider != "" && company != "" && jobKey != "" {
		for _, job := range jobs {
			if job.Provider == provider && job.CompanySlug == company && job.JobKey == jobKey {
				return job, true
			}
		}
	}
	return jobs[0], true
}

func serveDashboard(ctx context.Context, database *db.DB, address string) error {
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
		if err := dashboardPageTemplate.Execute(w, dashboardHome{
			SavedOnly:           r.URL.Query().Get("saved") == "true",
			BookmarkCount:       count,
			Query:               r.URL.Query().Get("q"),
			Location:            r.URL.Query().Get("location"),
			PostedAfter:         r.URL.Query().Get("posted_after"),
			PostedBefore:        r.URL.Query().Get("posted_before"),
			EmploymentType:      r.URL.Query().Get("employment"),
			WorkplaceType:       r.URL.Query().Get("workplace"),
			IncludeDescriptions: r.URL.Query().Get("details") == "true",
			SelectedProvider:    r.URL.Query().Get("selected_provider"),
			SelectedCompany:     r.URL.Query().Get("selected_company"),
			SelectedJobKey:      r.URL.Query().Get("selected_job"),
		}); err != nil {
			log.Printf("render dashboard page: %v", err)
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
			if err := dashboardPageTemplate.Execute(w, dashboardHome{
				SavedOnly: savedOnly, BookmarkCount: count, Query: query,
				Location: filters.Location, PostedAfter: filters.PostedAfter,
				PostedBefore: filters.PostedBefore, EmploymentType: filters.EmploymentType,
				WorkplaceType: filters.WorkplaceType, IncludeDescriptions: includeDescriptions,
				SelectedProvider: r.URL.Query().Get("selected_provider"),
				SelectedCompany:  r.URL.Query().Get("selected_company"),
				SelectedJobKey:   r.URL.Query().Get("selected_job"),
			}); err != nil {
				log.Printf("render dashboard page: %v", err)
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
		selectedJob, hasSelectedJob := selectDashboardJob(jobs, r.URL.Query())
		selectedProvider, selectedCompany, selectedJobKey := "", "", ""
		if hasSelectedJob {
			selectedProvider = selectedJob.Provider
			selectedCompany = selectedJob.CompanySlug
			selectedJobKey = selectedJob.JobKey
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := dashboardResultsTemplate.Execute(w, dashboardPage{
			Query: query, Location: filters.Location, PostedAfter: filters.PostedAfter, PostedBefore: filters.PostedBefore,
			EmploymentType: filters.EmploymentType, WorkplaceType: filters.WorkplaceType,
			IncludeDescriptions: includeDescriptions, SavedOnly: savedOnly,
			Page: page, TotalPages: totalPages, Total: total, Jobs: jobs,
			SelectedJob: selectedJob, HasSelectedJob: hasSelectedJob,
			SelectedProvider: selectedProvider, SelectedCompany: selectedCompany, SelectedJobKey: selectedJobKey,
		}); err != nil {
			log.Printf("render dashboard jobs: %v", err)
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
			log.Printf("stop dashboard: %v", err)
		}
	}()

	log.Printf("Job dashboard listening at http://%s", address)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
		return nil
	}
	return err
}
