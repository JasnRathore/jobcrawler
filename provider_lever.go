package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"db"
	"golang.org/x/time/rate"
)

type leverProvider struct{}

func init() {
	registerProvider(leverProvider{})
}

func (leverProvider) Name() string { return providerLever }

func (leverProvider) CompanyEndpoints(slug string) []string {
	escaped := url.PathEscape(slug)
	return []string{
		"https://api.lever.co/v0/postings/" + escaped + "?mode=json",
		"https://api.eu.lever.co/v0/postings/" + escaped + "?mode=json",
	}
}

func (leverProvider) FallbackOnNotFound() bool { return true }

func (p leverProvider) CheckCompany(
	ctx context.Context, client *http.Client, limiter *rate.Limiter, slug string, stats *benchmarkStats,
) (bool, error) {
	response, err := fetchProviderResponse(ctx, client, limiter, p, slug, stats)
	if err != nil || !response.ok {
		return false, err
	}
	if err := validateLeverResponse(response.body); err != nil {
		return false, err
	}
	return true, nil
}

func (p leverProvider) FetchJobs(
	ctx context.Context, client *http.Client, limiter *rate.Limiter, slug string,
) ([]db.Job, error) {
	response, err := fetchProviderResponse(ctx, client, limiter, p, slug, nil)
	if err != nil {
		return nil, err
	}
	if !response.ok {
		return nil, fmt.Errorf("Lever company %q was not found", slug)
	}
	return parseLeverJobs(slug, response.body)
}

func validateLeverResponse(body []byte) error {
	if !strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		var failure struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &failure); err == nil && !failure.OK {
			return fmt.Errorf("Lever returned an error: %s", failure.Error)
		}
		return errors.New("Lever response was not a postings array")
	}
	var postings []json.RawMessage
	if err := json.Unmarshal(body, &postings); err == nil {
		return nil
	}
	return errors.New("Lever response was not a postings array")
}

type leverPosting struct {
	ID         string `json:"id"`
	Text       string `json:"text"`
	Categories struct {
		Commitment   string   `json:"commitment"`
		Location     string   `json:"location"`
		Team         string   `json:"team"`
		AllLocations []string `json:"allLocations"`
	} `json:"categories"`
	CreatedAt        int64  `json:"createdAt"`
	DescriptionPlain string `json:"descriptionPlain"`
	DescriptionBody  string `json:"descriptionBody"`
	AdditionalPlain  string `json:"additionalPlain"`
	Additional       string `json:"additional"`
	Lists            []struct {
		Text    string `json:"text"`
		Content string `json:"content"`
	} `json:"lists"`
	WorkplaceType string `json:"workplaceType"`
	HostedURL     string `json:"hostedUrl"`
	ApplyURL      string `json:"applyUrl"`
}

func parseLeverJobs(slug string, body []byte) ([]db.Job, error) {
	var postings []leverPosting
	if err := json.Unmarshal(body, &postings); err != nil {
		return nil, fmt.Errorf("decode Lever postings array: %w", err)
	}
	jobs := make([]db.Job, 0, len(postings))
	for _, posting := range postings {
		if posting.ID == "" || posting.Text == "" {
			return nil, errors.New("Lever posting is missing id or title")
		}
		published := ""
		if posting.CreatedAt > 0 {
			published = time.UnixMilli(posting.CreatedAt).UTC().Format(time.RFC3339)
		}
		description := strings.TrimSpace(posting.DescriptionPlain + "\n\n" + posting.AdditionalPlain)
		if description == "" {
			description = plainText(posting.DescriptionBody + " " + posting.Additional)
		}
		for _, list := range posting.Lists {
			description += "\n\n" + strings.TrimSpace(list.Text+" "+plainText(list.Content))
		}
		attributes, err := json.Marshal(map[string]any{
			"team": posting.Categories.Team, "commitment": posting.Categories.Commitment,
			"all_locations": posting.Categories.AllLocations,
		})
		if err != nil {
			return nil, fmt.Errorf("normalize Lever attributes: %w", err)
		}
		detailsURL := posting.HostedURL
		if detailsURL == "" {
			detailsURL = posting.ApplyURL
		}
		jobs = append(jobs, db.Job{
			JobKey: posting.ID, Title: posting.Text, CompanyName: slug,
			LocationName: posting.Categories.Location, EmploymentType: posting.Categories.Commitment,
			WorkplaceType: normalizeWorkplaceType(posting.WorkplaceType),
			Department:    posting.Categories.Team, DetailsURL: detailsURL, ApplyURL: posting.ApplyURL,
			UpdatedAt: published, FirstPublished: published, Description: strings.TrimSpace(description),
			AttributesJSON: string(attributes),
		})
	}
	return jobs, nil
}
