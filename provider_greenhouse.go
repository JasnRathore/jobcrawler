package main

import (
	"context"
	"errors"
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
	for attempt := 0; attempt < 4; attempt++ {
		if stop := limiter.providerStop(providerGreenhouse); stop != nil {
			return false, stop
		}
		lease, leaseErr := acquireLease(ctx, pool, client)
		if leaseErr != nil {
			if errors.Is(leaseErr, errProxyExhausted) {
				leaseErr = stopProviderForProxyExhaustion(limiter, providerGreenhouse)
			}
			return false, leaseErr
		}
		if err := limiter.waitForProvider(ctx, providerGreenhouse); err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return false, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, requestURL, nil)
		if err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return false, err
		}
		setStandardHeaders(req)
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
			stats.recordRequest(time.Since(started), status, requestErr != nil, attempt > 0)
		}
		switch {
		case requestErr != nil:
			lease.release(proxyOutcomeFailed, 0)
			lastFailure = fmt.Sprintf("transport error: %v", requestErr)
		case status == http.StatusOK || status == http.StatusNoContent:
			lease.release(proxyOutcomeOK, 0)
			return true, nil
		case status == http.StatusNotFound:
			lease.release(proxyOutcomeOK, 0)
			return false, nil
		case status == http.StatusTooManyRequests:
			retryAfter := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
			lease.release(proxyOutcomeRateLimited, retryAfter)
			limiter.recordRateLimit(providerGreenhouse, retryAfter)
			lastFailure = "rate limited (HTTP 429)"
		case isDestinationStopResponse(res):
			stop := destinationStopError(providerGreenhouse, res)
			lease.release(proxyOutcomeFailed, 0)
			return false, limiter.stopProvider(stop)
		case status >= 500:
			lease.release(proxyOutcomeFailed, 0)
			lastFailure = fmt.Sprintf("status %d", status)
		default:
			lease.release(proxyOutcomeFailed, 0)
			return false, fmt.Errorf("unexpected status %d", status)
		}
		if attempt == 3 {
			return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return false, fmt.Errorf("gave up checking Greenhouse company after retries (last: %s)", lastFailure)
}

func (greenhouseProvider) FetchJobs(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string,
) ([]db.Job, error) {
	return fetchGreenhouseJobs(ctx, client, pool, limiter, slug)
}
