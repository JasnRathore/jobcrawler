package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhtml "html"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"db"
)

const (
	jobListURL  = baseURL + "/jobs?content=true"
	jobPageSize = 50
	maxJobsBody = 64 << 20

	jobUpdateHeartbeatInterval      = 10 * time.Second
	// Two attempts leave margin before the database's 60-second lease expires.
	jobUpdateDBOperationTimeout     = 20 * time.Second
	jobUpdateDBOperationMaxAttempts = 2
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

	runTargets := make([]db.JobUpdateTarget, len(targets))
	for i, target := range targets {
		runTargets[i] = db.JobUpdateTarget{Provider: target.provider, CompanySlug: target.slug}
	}
	run, pending, err := database.BeginOrResumeJobUpdateRun(ctx, runTargets)
	if err != nil {
		var retry *db.JobUpdateRetryNotBeforeError
		switch {
		case errors.Is(err, db.ErrJobUpdateRunActive):
			return fmt.Errorf("job update already in progress: %w", err)
		case errors.As(err, &retry):
			return fmt.Errorf("%w; no provider requests were sent", err)
		default:
			return err
		}
	}

	action := "started"
	if run.Resumed {
		action = "resuming"
	}
	log.Printf("Job update %s (run %s): %d/%d companies complete, %d pending",
		action, run.ID, run.CompletedCount, run.TargetCount, run.PendingCount)
	if run.TargetCount == 0 {
		log.Printf("No companies to update; discover companies first")
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	go heartbeatJobUpdateRun(heartbeatCtx, cancelRun, database, run, heartbeatDone, heartbeatErr)

	updateErr := updateJobTargets(runCtx, database, run, pending, workers, func(ctx context.Context, target companyTarget) ([]db.Job, error) {
		return fetchProviderCompanyJobs(ctx, client, pool, limiter, target.provider, target.slug)
	})
	cancelHeartbeat()
	<-heartbeatDone
	select {
	case err := <-heartbeatErr:
		if updateErr == nil || errors.Is(updateErr, context.Canceled) {
			updateErr = fmt.Errorf("job update lease lost: %w", err)
		}
	default:
	}

	progress, progressErr := database.JobUpdateRunProgress(ctx, run.ID)
	if progressErr != nil {
		updateErr = errors.Join(updateErr, progressErr)
	}
	if updateErr != nil || progressErr != nil {
		var stop *providerStopError
		stopProvider, stopStatus, stopReason := "", 0, "update interrupted; pending targets remain"
		var retryAt time.Time
		if errors.As(updateErr, &stop) {
			stopProvider, stopStatus, stopReason, retryAt = stop.provider, stop.status, stop.reason, stop.retryAt
		}
		pauseCtx, cancelPause := context.WithTimeout(context.Background(), 10*time.Second)
		pauseErr := database.PauseJobUpdateRun(pauseCtx, run, stopProvider, stopStatus, stopReason, retryAt)
		cancelPause()
		if progress != nil {
			log.Printf("Job update paused (run %s): %d/%d companies complete, %d pending, %d failed",
				run.ID, progress.CompletedCount, progress.TargetCount, progress.PendingCount, progress.FailedCount)
		}
		if stop != nil {
			if !retryAt.IsZero() {
				log.Printf("Provider %s stopped after HTTP %d; retry no earlier than %s. Rerun --update --jobs after that time to resume.",
					stopProvider, stopStatus, retryAt.Format(time.RFC3339))
			} else {
				log.Printf("Provider %s stopped after HTTP %d; Retry-After is unavailable. Follow the provider's access policy, then rerun --update --jobs to resume.",
					stopProvider, stopStatus)
			}
		}
		return errors.Join(updateErr, pauseErr)
	}
	if progress.PendingCount != 0 {
		err := fmt.Errorf("job update stopped with %d pending targets but no worker error", progress.PendingCount)
		pauseCtx, cancelPause := context.WithTimeout(context.Background(), 10*time.Second)
		pauseErr := database.PauseJobUpdateRun(pauseCtx, run, "", 0, "update stopped with pending targets", time.Time{})
		cancelPause()
		return errors.Join(err, pauseErr)
	}
	if err := database.CompleteJobUpdateRun(ctx, run); err != nil {
		pauseCtx, cancelPause := context.WithTimeout(context.Background(), 10*time.Second)
		pauseErr := database.PauseJobUpdateRun(pauseCtx, run, "", 0, "could not finalize job update run", time.Time{})
		cancelPause()
		return errors.Join(fmt.Errorf("complete job update run: %w", err), pauseErr)
	}
	log.Printf("Job update completed (run %s): %d/%d companies complete", run.ID, progress.CompletedCount, progress.TargetCount)
	return nil
}

func heartbeatJobUpdateRun(
	ctx context.Context,
	cancelRun context.CancelFunc,
	database *db.DB,
	run *db.JobUpdateRun,
	done chan<- struct{},
	failures chan<- error,
) {
	defer close(done)
	ticker := time.NewTicker(jobUpdateHeartbeatInterval)
	defer ticker.Stop()
	lastCompleted := run.CompletedCount
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := retryJobUpdateDBOperation(ctx, run.ID, "lease renewal", func(operationCtx context.Context) error {
				return database.RenewJobUpdateRunLease(operationCtx, run)
			})
			if err == nil {
				var progress *db.JobUpdateRun
				progressErr := retryJobUpdateDBOperation(ctx, run.ID, "progress read", func(operationCtx context.Context) error {
					var err error
					progress, err = database.JobUpdateRunProgress(operationCtx, run.ID)
					return err
				})
				if progressErr != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Job update progress read delayed (run %s); lease was renewed: %v", run.ID, progressErr)
					continue
				}
				if progress.CompletedCount != lastCompleted {
					lastCompleted = progress.CompletedCount
					log.Printf("Job update progress (run %s): %d/%d companies complete, %d pending",
						run.ID, progress.CompletedCount, progress.TargetCount, progress.PendingCount)
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case failures <- err:
				default:
				}
				cancelRun()
				return
			}
		}
	}
}

func retryJobUpdateDBOperation(ctx context.Context, runID, operation string, run func(context.Context) error) error {
	var err error
	for attempt := 1; attempt <= jobUpdateDBOperationMaxAttempts; attempt++ {
		operationCtx, cancel := context.WithTimeout(ctx, jobUpdateDBOperationTimeout)
		err = run(operationCtx)
		cancel()
		if err == nil || !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || attempt == jobUpdateDBOperationMaxAttempts {
			return err
		}
		log.Printf("Job update %s timed out; retrying (run %s)", operation, runID)
	}
	return err
}

func updateJobTargets(
	ctx context.Context,
	database *db.DB,
	run *db.JobUpdateRun,
	pending []db.JobUpdateTarget,
	workers int,
	update func(context.Context, companyTarget) ([]db.Job, error),
) error {
	if workers < 1 {
		return errors.New("job update worker count must be at least one")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan db.JobUpdateTarget)
	errs := make(chan error, len(pending))
	var wg sync.WaitGroup
	var stopMu sync.Mutex
	var stopErr error
	var fatalErr error
	recordFatal := func(err error) {
		stopMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		stopMu.Unlock()
		cancel()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				if runCtx.Err() != nil {
					continue
				}
				company := companyTarget{provider: target.Provider, slug: target.CompanySlug}
				updatedJobs, err := update(runCtx, company)
				if err != nil {
					var stop *providerStopError
					if errors.As(err, &stop) {
						if recordErr := recordJobUpdateTargetFailure(database, run, target, "destination stop"); recordErr != nil {
							recordFatal(fmt.Errorf("persist destination stop progress: %w", recordErr))
							continue
						}
						errs <- err
						stopMu.Lock()
						if stopErr == nil {
							stopErr = stop
							cancel()
						}
						stopMu.Unlock()
						continue
					}
					if runCtx.Err() != nil {
						if recordErr := recordJobUpdateTargetFailure(database, run, target, "request interrupted"); recordErr != nil {
							recordFatal(fmt.Errorf("persist interrupted target progress: %w", recordErr))
						}
						continue
					}
					if recordErr := recordJobUpdateTargetFailure(database, run, target, jobUpdateErrorCategory(err)); recordErr != nil {
						recordFatal(fmt.Errorf("persist failed target progress: %w", recordErr))
						continue
					}
					log.Printf("%s/%s: job update failed: %v", target.Provider, target.CompanySlug, err)
					errs <- err
					continue
				}
				if runCtx.Err() != nil {
					continue
				}
				if err := database.ReplaceProviderJobsAndCompleteTarget(runCtx, run, target, updatedJobs); err != nil {
					recordFatal(fmt.Errorf("save %s/%s job update atomically: %w", target.Provider, target.CompanySlug, err))
					continue
				}
				log.Printf("%s/%s: indexed %d jobs", target.Provider, target.CompanySlug, len(updatedJobs))
			}
		}()
	}

sendCompanies:
	for _, target := range pending {
		select {
		case jobs <- target:
		case <-runCtx.Done():
			break sendCompanies
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)

	var failed int
	var firstErr error
	for err := range errs {
		failed++
		if firstErr == nil {
			firstErr = err
		}
	}
	stopMu.Lock()
	stopped := stopErr
	fatal := fatalErr
	stopMu.Unlock()
	if fatal != nil {
		return fatal
	}
	if stopped != nil {
		return stopped
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d company job updates failed: %w", failed, len(pending), firstErr)
	}
	return nil
}

func jobUpdateErrorCategory(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "request timed out"
	}
	return "provider request failed"
}

func recordJobUpdateTargetFailure(database *db.DB, run *db.JobUpdateRun, target db.JobUpdateTarget, category string) error {
	return retryJobUpdateDBOperation(context.Background(), run.ID, "failed target progress write", func(ctx context.Context) error {
		return database.RecordJobUpdateTargetFailure(ctx, run, target, category)
	})
}

func fetchProviderCompanyJobs(
	ctx context.Context,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	provider, slug string,
) ([]db.Job, error) {
	implementation, err := getProvider(provider)
	if err != nil {
		return nil, err
	}
	return implementation.FetchJobs(ctx, client, pool, limiter, slug)
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

	lastFailure := ""
	for attempt := 0; attempt < 4; attempt++ {
		if stop := limiter.providerStop(providerGreenhouse); stop != nil {
			return nil, stop
		}
		lease, leaseErr := acquireLease(ctx, pool, client)
		if leaseErr != nil {
			if errors.Is(leaseErr, errProxyExhausted) {
				leaseErr = stopProviderForProxyExhaustion(limiter, providerGreenhouse)
			}
			return nil, leaseErr
		}
		if err := limiter.waitForProvider(ctx, providerGreenhouse); err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			lease.release(proxyOutcomeFailed, 0)
			return nil, fmt.Errorf("create jobs request: %w", err)
		}
		setStandardHeaders(req)
		res, err := lease.client.Do(req)
		if err != nil {
			if res != nil && res.Body != nil {
				res.Body.Close()
			}
			lease.release(proxyOutcomeFailed, 0)
			lastFailure = fmt.Sprintf("transport error: %v", err)
		} else {
			if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusNotAcceptable {
				// On Greenhouse 403/406 are rate limiting expressed as
				// blocks: cool this route and retry on another one.
				res.Body.Close()
				retryAfter := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
				lease.release(proxyOutcomeRateLimited, retryAfter)
				limiter.recordRateLimit(providerGreenhouse, retryAfter)
				lastFailure = fmt.Sprintf("rate limited (HTTP %d)", res.StatusCode)
			} else if isDestinationStopResponse(res) {
				res.Body.Close()
				stop := destinationStopError(providerGreenhouse, res)
				lease.release(proxyOutcomeFailed, 0)
				return nil, limiter.stopProvider(stop)
			} else if res.StatusCode >= 500 {
				res.Body.Close()
				lease.release(proxyOutcomeFailed, 0)
				lastFailure = fmt.Sprintf("status %d", res.StatusCode)
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
		if attempt == 3 {
			return nil, fmt.Errorf("fetch jobs after retries (last: %s)", lastFailure)
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
