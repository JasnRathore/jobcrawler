package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"db"
)

type workableProvider struct{}

func init() {
	registerProvider(workableProvider{})
}

func (workableProvider) Name() string { return providerWorkable }

func (workableProvider) CompanyEndpoints(slug string) []string {
	return []string{"https://apply.workable.com/api/v1/widget/accounts/" + url.PathEscape(slug)}
}

func (workableProvider) FallbackOnNotFound() bool { return false }

func (p workableProvider) CheckCompany(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string, stats *benchmarkStats,
) (bool, error) {
	response, err := fetchProviderResponse(ctx, client, pool, limiter, p, slug, stats)
	if err != nil || !response.ok {
		return false, err
	}
	var payload workableResponse
	if err := json.Unmarshal(response.body, &payload); err != nil {
		return false, fmt.Errorf("decode Workable response: %w", err)
	}
	if strings.TrimSpace(payload.Name) == "" || payload.Jobs == nil {
		return false, errors.New("Workable response did not contain a company name and jobs array")
	}
	return true, nil
}

func (p workableProvider) FetchJobs(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string,
) ([]db.Job, error) {
	response, err := fetchProviderResponse(ctx, client, pool, limiter, p, slug, nil)
	if err != nil {
		return nil, err
	}
	if !response.ok {
		return nil, fmt.Errorf("Workable company %q was not found", slug)
	}
	return parseWorkableJobs(response.body)
}

type workableResponse struct {
	Name string            `json:"name"`
	Jobs []json.RawMessage `json:"jobs"`
}

type workablePosting struct {
	Title          string          `json:"title"`
	Shortcode      string          `json:"shortcode"`
	Code           json.RawMessage `json:"code"`
	EmploymentType json.RawMessage `json:"employment_type"`
	Telecommuting  bool            `json:"telecommuting"`
	Department     json.RawMessage `json:"department"`
	Education      json.RawMessage `json:"education"`
	Experience     json.RawMessage `json:"experience"`
	Function       json.RawMessage `json:"function"`
	Industry       json.RawMessage `json:"industry"`
	URL            string          `json:"url"`
	Shortlink      string          `json:"shortlink"`
	ApplicationURL string          `json:"application_url"`
	PublishedOn    string          `json:"published_on"`
	CreatedAt      string          `json:"created_at"`
	Country        string          `json:"country"`
	City           string          `json:"city"`
	State          string          `json:"state"`
	Locations      json.RawMessage `json:"locations"`
}

func parseWorkableJobs(body []byte) ([]db.Job, error) {
	var response workableResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode Workable account: %w", err)
	}
	if strings.TrimSpace(response.Name) == "" || response.Jobs == nil {
		return nil, errors.New("Workable account is missing name or jobs")
	}
	jobs := make([]db.Job, 0, len(response.Jobs))
	for _, raw := range response.Jobs {
		var posting workablePosting
		if err := json.Unmarshal(raw, &posting); err != nil {
			return nil, fmt.Errorf("decode Workable job: %w", err)
		}
		if posting.Shortcode == "" || posting.Title == "" {
			return nil, errors.New("Workable job is missing shortcode or title")
		}
		attributes, err := json.Marshal(map[string]any{
			"code": stringValue(posting.Code), "department": stringValue(posting.Department),
			"function": stringValue(posting.Function), "industry": stringValue(posting.Industry),
			"locations": json.RawMessage(jsonOrNull(posting.Locations)),
		})
		if err != nil {
			return nil, fmt.Errorf("normalize Workable attributes: %w", err)
		}
		detailsURL := posting.URL
		if detailsURL == "" {
			detailsURL = posting.Shortlink
		}
		if detailsURL == "" {
			detailsURL = posting.ApplicationURL
		}
		location := strings.Join(nonEmpty(posting.City, posting.State, posting.Country), ", ")
		workplace := "onsite"
		if posting.Telecommuting {
			workplace = "remote"
		}
		jobs = append(jobs, db.Job{
			JobKey: posting.Shortcode, Title: posting.Title, CompanyName: response.Name,
			LocationName: location, LocationJSON: stringValue(posting.Locations),
			DetailsURL: detailsURL, ApplyURL: posting.ApplicationURL,
			FirstPublished: posting.PublishedOn, UpdatedAt: posting.CreatedAt,
			EmploymentType: stringValue(posting.EmploymentType), WorkplaceType: normalizeWorkplaceType(workplace),
			Department: stringValue(posting.Department), Education: stringValue(posting.Education),
			Experience: stringValue(posting.Experience), AttributesJSON: string(attributes),
		})
	}
	return jobs, nil
}

func stringValue(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return string(raw)
}

func nonEmpty(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}
