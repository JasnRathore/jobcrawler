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
	"golang.org/x/time/rate"
)

type ashbyHQProvider struct{}

func init() {
	registerProvider(ashbyHQProvider{})
}

func (ashbyHQProvider) Name() string { return providerAshbyHQ }

func (ashbyHQProvider) CompanyEndpoints(slug string) []string {
	return []string{"https://api.ashbyhq.com/posting-api/job-board/" + url.PathEscape(slug)}
}

func (ashbyHQProvider) FallbackOnNotFound() bool { return false }

func (p ashbyHQProvider) CheckCompany(
	ctx context.Context, client *http.Client, limiter *rate.Limiter, slug string, stats *benchmarkStats,
) (bool, error) {
	response, err := fetchProviderResponse(ctx, client, limiter, p, slug, stats)
	if err != nil || !response.ok {
		return false, err
	}
	var payload ashbyHQResponse
	if err := json.Unmarshal(response.body, &payload); err != nil {
		return false, fmt.Errorf("decode Ashby response: %w", err)
	}
	if payload.Jobs == nil {
		return false, errors.New("Ashby response did not contain a jobs array")
	}
	return true, nil
}

func (p ashbyHQProvider) FetchJobs(
	ctx context.Context, client *http.Client, limiter *rate.Limiter, slug string,
) ([]db.Job, error) {
	response, err := fetchProviderResponse(ctx, client, limiter, p, slug, nil)
	if err != nil {
		return nil, err
	}
	if !response.ok {
		return nil, fmt.Errorf("Ashby company %q was not found", slug)
	}
	return parseAshbyHQJobs(slug, response.body)
}

type ashbyHQResponse struct {
	Jobs []ashbyHQPosting `json:"jobs"`
}

type ashbyHQPosting struct {
	ID                 string `json:"id"`
	Title              string `json:"title"`
	Department         string `json:"department"`
	Team               string `json:"team"`
	EmploymentType     string `json:"employmentType"`
	Location           string `json:"location"`
	SecondaryLocations []struct {
		Location string `json:"location"`
	} `json:"secondaryLocations"`
	PublishedAt   string `json:"publishedAt"`
	IsListed      bool   `json:"isListed"`
	IsRemote      *bool  `json:"isRemote"`
	WorkplaceType string `json:"workplaceType"`
	Address       struct {
		PostalAddress struct {
			AddressRegion   string `json:"addressRegion"`
			AddressCountry  string `json:"addressCountry"`
			AddressLocality string `json:"addressLocality"`
		} `json:"postalAddress"`
	} `json:"address"`
	JobURL           string `json:"jobUrl"`
	ApplyURL         string `json:"applyUrl"`
	DescriptionHTML  string `json:"descriptionHtml"`
	DescriptionPlain string `json:"descriptionPlain"`
}

func parseAshbyHQJobs(slug string, body []byte) ([]db.Job, error) {
	var response ashbyHQResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode Ashby job board: %w", err)
	}
	if response.Jobs == nil {
		return nil, errors.New("Ashby job board is missing jobs")
	}

	jobs := make([]db.Job, 0, len(response.Jobs))
	for _, posting := range response.Jobs {
		if !posting.IsListed {
			continue
		}
		if strings.TrimSpace(posting.ID) == "" || strings.TrimSpace(posting.Title) == "" {
			return nil, errors.New("Ashby job is missing id or title")
		}

		postalAddress := posting.Address.PostalAddress
		location := strings.TrimSpace(posting.Location)
		if location == "" {
			location = strings.Join(nonEmpty(postalAddress.AddressLocality, postalAddress.AddressRegion, postalAddress.AddressCountry), ", ")
		}
		secondaryLocations := make([]string, 0, len(posting.SecondaryLocations))
		for _, secondary := range posting.SecondaryLocations {
			if secondary.Location = strings.TrimSpace(secondary.Location); secondary.Location != "" {
				secondaryLocations = append(secondaryLocations, secondary.Location)
			}
		}

		workplace := posting.WorkplaceType
		if posting.IsRemote != nil && *posting.IsRemote {
			workplace = "remote"
		}
		attributes, err := json.Marshal(map[string]any{
			"team": posting.Team, "secondary_locations": secondaryLocations,
		})
		if err != nil {
			return nil, fmt.Errorf("normalize Ashby attributes: %w", err)
		}
		detailsURL := posting.JobURL
		if detailsURL == "" {
			detailsURL = "https://jobs.ashbyhq.com/" + url.PathEscape(slug) + "/" + url.PathEscape(posting.ID)
		}
		applyURL := posting.ApplyURL
		if applyURL == "" {
			applyURL = detailsURL + "/application"
		}
		description := strings.TrimSpace(posting.DescriptionPlain)
		if description == "" {
			description = plainText(posting.DescriptionHTML)
		}
		jobs = append(jobs, db.Job{
			JobKey: posting.ID, Title: posting.Title, CompanyName: slug,
			LocationName: location, EmploymentType: posting.EmploymentType,
			WorkplaceType: normalizeWorkplaceType(workplace),
			Department:    posting.Department, DetailsURL: detailsURL, ApplyURL: applyURL,
			UpdatedAt: posting.PublishedAt, FirstPublished: posting.PublishedAt,
			Description: description, AttributesJSON: string(attributes),
		})
	}
	return jobs, nil
}
