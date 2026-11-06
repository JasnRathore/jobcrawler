package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"db"
)

type greenhouseProvider struct{}

func init() {
	registerProvider(greenhouseProvider{})
}

func (greenhouseProvider) Name() string { return providerGreenhouse }

func (greenhouseProvider) CompanyEndpoints(slug string) []string {
	return []string{fmt.Sprintf(baseURL, url.PathEscape(slug))}
}

func (greenhouseProvider) FallbackOnNotFound() bool { return false }

func isGreenhouseRateLimited(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusForbidden || status == http.StatusNotAcceptable
}

func (greenhouseProvider) CheckCompany(
	ctx context.Context,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	slug string,
	stats *benchmarkStats,
) (bool, error) {
	requestURL := fmt.Sprintf(baseURL, url.PathEscape(slug))
	backoff := 500 * time.Millisecond
	lastFailure := ""
	transientAttempts := 0
	rateLimitAttempts := 0
	const maxTransientAttempts = 4
	const maxRateLimitAttempts = 4

	for {
		lease, leaseErr := acquireLease(ctx, pool, client)
		if leaseErr != nil {
			return false, leaseErr
		}
		if err := limiter.Wait(ctx); err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return false, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, requestURL, nil)
		if err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return false, err
		}
		req.Header.Set("Accept", "application/json")
		started := time.Now()
		res, requestErr := lease.client.Do(req)
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		status := 0
		if res != nil {
			status = res.StatusCode
		}
		if stats != nil {
			stats.recordRequest(time.Since(started), status, requestErr != nil, transientAttempts > 0 || rateLimitAttempts > 0)
		}
		switch {
		case requestErr != nil:
			lease.release(proxyOutcomeFailed, 0)
			lastFailure = fmt.Sprintf("transport error: %v", requestErr)
			transientAttempts++
			if transientAttempts >= maxTransientAttempts {
				return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
			}
		case status == http.StatusOK || status == http.StatusNoContent:
			lease.release(proxyOutcomeOK, 0)
			return true, nil
		case status == http.StatusNotFound:
			lease.release(proxyOutcomeOK, 0)
			return false, nil
		case isGreenhouseRateLimited(status):
			retryAfter := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
			lease.release(proxyOutcomeRateLimited, retryAfter)
			lastFailure = fmt.Sprintf("status %d (Retry-After %s)", status, retryAfter)
			rateLimitAttempts++
			if pool != nil && !lease.directFallback {
				// Rate limiting is per egress IP; rotate to a fresh peer.
				if rateLimitAttempts >= maxRateLimitAttempts {
					return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
				}
				continue
			}
			cooldownDuration := limiter.StartCooldownForStatusAndRetryAfter(providerGreenhouse, status, retryAfter)
			if cooldownDuration > 0 {
				if rateLimitAttempts >= maxRateLimitAttempts {
					return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
				}
				// Wait for the shared cooldown to elapse so this worker doesn't hammer the API during the pause
				if err := limiter.Wait(ctx); err != nil {
					return false, err
				}
				continue
			}
			if rateLimitAttempts >= maxRateLimitAttempts {
				return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
			}
		case status >= 500:
			lease.release(proxyOutcomeFailed, 0)
			lastFailure = fmt.Sprintf("status %d", status)
			transientAttempts++
			if transientAttempts >= maxTransientAttempts {
				return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
			}
		default:
			lease.release(proxyOutcomeFailed, 0)
			return false, fmt.Errorf("unexpected status %d", status)
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return false, fmt.Errorf("gave up checking Greenhouse company after retries")
}

func (greenhouseProvider) FetchJobs(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string,
) ([]db.Job, error) {
	return fetchGreenhouseJobs(ctx, client, pool, limiter, slug)
}
