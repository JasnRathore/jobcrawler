package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"db"
	"golang.org/x/time/rate"
)

const (
	providerGreenhouse = "greenhouse"
	providerLever      = "lever"
	providerWorkable   = "workable"
)

type jobBoardProvider interface {
	Name() string
	CompanyEndpoints(string) []string
	FallbackOnNotFound() bool
	CheckCompany(context.Context, *http.Client, *rate.Limiter, string, *benchmarkStats) (bool, error)
	FetchJobs(context.Context, *http.Client, *rate.Limiter, string) ([]db.Job, error)
}

var providerRegistry = map[string]jobBoardProvider{}

func registerProvider(provider jobBoardProvider) {
	name := provider.Name()
	if name == "" {
		panic("job-board provider has an empty name")
	}
	if _, exists := providerRegistry[name]; exists {
		panic("duplicate job-board provider: " + name)
	}
	providerRegistry[name] = provider
}

func supportedProvider(provider string) bool {
	_, exists := providerRegistry[provider]
	return exists
}

func registeredProviders() []string {
	names := make([]string, 0, len(providerRegistry))
	for name := range providerRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func getProvider(name string) (jobBoardProvider, error) {
	provider, ok := providerRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unsupported provider %q", name)
	}
	return provider, nil
}

func checkProviderCompany(
	ctx context.Context,
	client *http.Client,
	limiter *rate.Limiter,
	database *db.DB,
	name, slug string,
	stats *benchmarkStats,
) error {
	provider, err := getProvider(name)
	if err != nil {
		return err
	}
	found, err := provider.CheckCompany(ctx, client, limiter, slug, stats)
	if err != nil {
		return err
	}
	if !found || database == nil {
		return nil
	}
	if err := database.AddCompanyForProvider(ctx, name, slug); err != nil {
		return fmt.Errorf("saving company: %w", err)
	}
	logFoundCompany(name, slug)
	return nil
}

func updateProviderJobs(
	ctx context.Context,
	database *db.DB,
	client *http.Client,
	limiter *rate.Limiter,
	name, slug string,
) (int, error) {
	provider, err := getProvider(name)
	if err != nil {
		return 0, err
	}
	jobs, err := provider.FetchJobs(ctx, client, limiter, slug)
	if err != nil {
		return 0, err
	}
	if err := database.ReplaceProviderJobs(ctx, name, slug, jobs); err != nil {
		return 0, err
	}
	return len(jobs), nil
}

type providerHTTPResponse struct {
	body []byte
	ok   bool
}

func fetchProviderResponse(
	ctx context.Context,
	client *http.Client,
	limiter *rate.Limiter,
	provider jobBoardProvider,
	slug string,
	stats *benchmarkStats,
) (providerHTTPResponse, error) {
	name := provider.Name()
	endpoints := provider.CompanyEndpoints(slug)
	if len(endpoints) == 0 {
		return providerHTTPResponse{}, fmt.Errorf("%s provider returned no request endpoints", name)
	}
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		retry := false
		for i, endpoint := range endpoints {
			if err := limiter.Wait(ctx); err != nil {
				return providerHTTPResponse{}, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
			if err != nil {
				return providerHTTPResponse{}, fmt.Errorf("create %s request: %w", name, err)
			}
			req.Header.Set("Accept", "application/json")
			started := time.Now()
			res, requestErr := client.Do(req)
			status := 0
			if res != nil {
				status = res.StatusCode
			}
			if stats != nil {
				stats.recordRequest(time.Since(started), status, requestErr != nil, attempt > 0 || i > 0)
			}
			if requestErr != nil {
				if res != nil && res.Body != nil {
					res.Body.Close()
				}
				retry = true
				break
			}
			if res.StatusCode == http.StatusNotFound && provider.FallbackOnNotFound() && i+1 < len(endpoints) {
				res.Body.Close()
				continue
			}
			if res.StatusCode == http.StatusNotFound {
				res.Body.Close()
				return providerHTTPResponse{ok: false}, nil
			}
			if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
				res.Body.Close()
				retry = true
				break
			}
			if res.StatusCode < 200 || res.StatusCode >= 300 {
				res.Body.Close()
				return providerHTTPResponse{}, fmt.Errorf("%s endpoint returned status %d", name, res.StatusCode)
			}
			body, err := io.ReadAll(io.LimitReader(res.Body, maxJobsBody+1))
			closeErr := res.Body.Close()
			if err != nil {
				return providerHTTPResponse{}, fmt.Errorf("read %s response: %w", name, err)
			}
			if closeErr != nil {
				return providerHTTPResponse{}, fmt.Errorf("close %s response: %w", name, closeErr)
			}
			if len(body) > maxJobsBody {
				return providerHTTPResponse{}, fmt.Errorf("%s response exceeds %d bytes", name, maxJobsBody)
			}
			return providerHTTPResponse{body: body, ok: true}, nil
		}
		if !retry {
			return providerHTTPResponse{ok: false}, nil
		}
		if attempt == 3 {
			return providerHTTPResponse{}, fmt.Errorf("fetch %s response after retries", name)
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return providerHTTPResponse{}, ctx.Err()
		}
	}
	return providerHTTPResponse{}, fmt.Errorf("fetch %s response after retries", name)
}

func normalizeWorkplaceType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "remote":
		return "remote"
	case "hybrid":
		return "hybrid"
	case "onsite", "on-site", "office":
		return "onsite"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}
