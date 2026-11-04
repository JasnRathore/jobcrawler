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

func (greenhouseProvider) CheckCompany(
	ctx context.Context,
	client *http.Client,
	limiter *requestLimiter,
	slug string,
	stats *benchmarkStats,
) (bool, error) {
	requestURL := fmt.Sprintf(baseURL, slug)
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return false, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, requestURL, nil)
		if err != nil {
			return false, err
		}
		started := time.Now()
		res, requestErr := client.Do(req)
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
		cooldownStarted := requestErr == nil && status == http.StatusTooManyRequests &&
			limiter.StartCooldown(providerGreenhouse) > 0
		if requestErr == nil {
			switch {
			case status == http.StatusOK || status == http.StatusNoContent:
				return true, nil
			case status == http.StatusNotFound:
				return false, nil
			case status != http.StatusTooManyRequests && status < 500:
				return false, fmt.Errorf("unexpected status %d", status)
			}
		}
		if attempt == 3 {
			return false, fmt.Errorf("gave up checking Greenhouse company after retries")
		}
		if cooldownStarted {
			continue
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
	ctx context.Context, client *http.Client, limiter *requestLimiter, slug string,
) ([]db.Job, error) {
	return fetchGreenhouseJobs(ctx, client, limiter, slug)
}
