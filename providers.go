package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"db"
	"golang.org/x/time/rate"
)

const (
	providerAshbyHQ    = "ashbyhq"
	providerGreenhouse = "greenhouse"
	providerLever      = "lever"
	providerLeverEU    = "levereu"
	providerWorkable   = "workable"
)

type requestLimiter struct {
	*rate.Limiter
	cooldown time.Duration

	mu            sync.Mutex
	cooldownUntil time.Time
}

func newRequestLimiter(limiter *rate.Limiter, cooldown time.Duration) *requestLimiter {
	return &requestLimiter{Limiter: limiter, cooldown: cooldown}
}

func (l *requestLimiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		wait := time.Until(l.cooldownUntil)
		l.mu.Unlock()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			}
			continue
		}

		if err := l.Limiter.Wait(ctx); err != nil {
			return err
		}

		l.mu.Lock()
		coolingDown := time.Now().Before(l.cooldownUntil)
		l.mu.Unlock()
		if !coolingDown {
			return nil
		}
	}
}

func (l *requestLimiter) StartCooldown(provider string) time.Duration {
	if l.cooldown <= 0 {
		return 0
	}
	until := time.Now().Add(l.cooldown)
	l.mu.Lock()
	if until.After(l.cooldownUntil) {
		l.cooldownUntil = until
	}
	wait := time.Until(l.cooldownUntil)
	l.mu.Unlock()
	log.Printf("%s returned HTTP 429; pausing requests for %s", provider, wait.Round(time.Second))
	return wait
}

type jobBoardProvider interface {
	Name() string
	CompanyEndpoints(string) []string
	FallbackOnNotFound() bool
	CheckCompany(context.Context, *http.Client, *proxyPool, *requestLimiter, string, *benchmarkStats) (bool, error)
	FetchJobs(context.Context, *http.Client, *proxyPool, *requestLimiter, string) ([]db.Job, error)
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
	pool *proxyPool,
	limiter *requestLimiter,
	database *db.DB,
	name, slug string,
	stats *benchmarkStats,
) error {
	provider, err := getProvider(name)
	if err != nil {
		return err
	}
	found, err := provider.CheckCompany(ctx, client, pool, limiter, slug, stats)
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
	pool *proxyPool,
	limiter *requestLimiter,
	name, slug string,
) (int, error) {
	provider, err := getProvider(name)
	if err != nil {
		return 0, err
	}
	jobs, err := provider.FetchJobs(ctx, client, pool, limiter, slug)
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
	pool *proxyPool,
	limiter *requestLimiter,
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
		skipBackoff := false
		for i, endpoint := range endpoints {
			lease, leaseErr := acquireLease(ctx, pool, client)
			if leaseErr != nil {
				return providerHTTPResponse{}, leaseErr
			}
			if err := limiter.Wait(ctx); err != nil {
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
			if err != nil {
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, fmt.Errorf("create %s request: %w", name, err)
			}
			req.Header.Set("Accept", "application/json")
			started := time.Now()
			res, requestErr := lease.client.Do(req)
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
				lease.release(proxyOutcomeFailed, 0)
				retry = true
				break
			}
			if res.StatusCode == http.StatusNotFound && provider.FallbackOnNotFound() && i+1 < len(endpoints) {
				res.Body.Close()
				lease.release(proxyOutcomeOK, 0)
				continue
			}
			if res.StatusCode == http.StatusNotFound {
				res.Body.Close()
				lease.release(proxyOutcomeOK, 0)
				return providerHTTPResponse{ok: false}, nil
			}
			if res.StatusCode == http.StatusTooManyRequests {
				res.Body.Close()
				retryAfter := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
				lease.release(proxyOutcomeRateLimited, retryAfter)
				if pool == nil {
					skipBackoff = limiter.StartCooldown(name) > 0
				} else {
					// Rate limiting is per egress IP; the leased peer cools
					// down and the next attempt rotates to a fresh one.
					skipBackoff = true
				}
				retry = true
				break
			}
			if res.StatusCode >= 500 {
				res.Body.Close()
				lease.release(proxyOutcomeFailed, 0)
				retry = true
				break
			}
			if res.StatusCode < 200 || res.StatusCode >= 300 {
				res.Body.Close()
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, fmt.Errorf("%s endpoint returned status %d", name, res.StatusCode)
			}
			body, err := io.ReadAll(io.LimitReader(res.Body, maxJobsBody+1))
			closeErr := res.Body.Close()
			if err != nil {
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, fmt.Errorf("read %s response: %w", name, err)
			}
			if closeErr != nil {
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, fmt.Errorf("close %s response: %w", name, closeErr)
			}
			if len(body) > maxJobsBody {
				lease.release(proxyOutcomeFailed, 0)
				return providerHTTPResponse{}, fmt.Errorf("%s response exceeds %d bytes", name, maxJobsBody)
			}
			lease.release(proxyOutcomeOK, 0)
			return providerHTTPResponse{body: body, ok: true}, nil
		}
		if !retry {
			return providerHTTPResponse{ok: false}, nil
		}
		if attempt == 3 {
			return providerHTTPResponse{}, fmt.Errorf("fetch %s response after retries", name)
		}
		if skipBackoff {
			continue
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
