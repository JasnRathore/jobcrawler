package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"db"
)

const (
	checkQueueSize        = 1024
	bruteCheckpointBatch  = int64(10000)
	maxCheckpointFailures = 5
	progressInterval      = 5 * time.Second
)

type checkResult struct {
	slug string
	err  error
}

type checkPool struct {
	jobs    chan string
	results chan checkResult
	wg      sync.WaitGroup
	pending int

	ctx      context.Context
	client   *http.Client
	proxies  *proxyPool
	limiter  *requestLimiter
	database *db.DB
	stats    *benchmarkStats
	provider string
	failures []checkResult
}

func newCheckPool(ctx context.Context, client *http.Client, proxies *proxyPool, limiter *requestLimiter, database *db.DB, stats *benchmarkStats, provider string, workers int) *checkPool {
	pool := &checkPool{
		jobs:     make(chan string, checkQueueSize),
		results:  make(chan checkResult, workers),
		ctx:      ctx,
		client:   client,
		proxies:  proxies,
		limiter:  limiter,
		database: database,
		stats:    stats,
		provider: provider,
	}
	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for slug := range pool.jobs {
				err := checkProviderCompany(pool.ctx, pool.client, pool.proxies, pool.limiter, pool.database, pool.provider, slug, pool.stats)
				if pool.stats != nil {
					pool.stats.recordCheck(err != nil)
				}
				pool.results <- checkResult{slug: slug, err: err}
			}
		}()
	}
	return pool
}

// submit queues a check and drains periodically so producers cannot outrun result collection.
func (p *checkPool) submit(slug string) (int, bool) {
	select {
	case p.jobs <- slug:
		p.pending++
	case <-p.ctx.Done():
		return 0, false
	}
	if p.pending >= checkQueueSize {
		return p.flush(), true
	}
	return 0, true
}

func (p *checkPool) flush() int {
	failures := 0
	for i := 0; i < p.pending; i++ {
		result := <-p.results
		if result.err != nil {
			failures++
			log.Printf("%s: %v", result.slug, result.err)
			p.failures = append(p.failures, result)
		}
	}
	p.pending = 0
	return failures
}

func (p *checkPool) takeFailures() []checkResult {
	failures := p.failures
	p.failures = nil
	return failures
}

func saveCheckFailures(pool *checkPool, path string) error {
	failures := pool.takeFailures()
	if len(failures) == 0 {
		return nil
	}
	return appendFailedCandidates(path, failures)
}

func (p *checkPool) close() int {
	failures := p.flush()
	close(p.jobs)
	p.wg.Wait()
	return failures
}

func runBruteForce(
	ctx context.Context,
	pool *checkPool,
	seen map[string]struct{},
	database *db.DB,
	minLength, maxLength int,
	resume bool,
	failedPath string,
) error {
	total := bruteCount(minLength, maxLength)
	next := int64(0)
	if resume {
		var err error
		next, err = database.StartProviderBruteCheckpoint(ctx, pool.provider, minLength, maxLength, alphabet, total)
		if err != nil {
			return err
		}
		if next == total {
			log.Printf("Brute-force run already complete (%d candidates); use a different length range for a new run", total)
			return nil
		}
		log.Printf("Resuming brute force at candidate %d of %d", next, total)
	}

	started := time.Now()
	lastProgress := started
	lastSaved := next
	var processed int64
	for next < total && ctx.Err() == nil {
		batchEnd := next + bruteCheckpointBatch
		if batchEnd > total {
			batchEnd = total
		}
		batchStart := next
		for index := batchStart; index < batchEnd; index++ {
			slug := bruteSlugAt(minLength, maxLength, index)
			if len(seen) > 0 {
				if _, duplicate := seen[slug]; duplicate {
					continue
				}
			}
			_, submitted := pool.submit(slug)
			if !submitted {
				break
			}
		}
		pool.flush()
		failed := pool.takeFailures()

		// Tier 3: In-flight batch requeue.
		// If any candidate failed (e.g. temporary throttling or rate-limiting),
		// retry them within the batch up to maxBatchRetries rounds after waiting for cooldown.
		const maxBatchRetries = 3
		for retryRound := 0; retryRound < maxBatchRetries && len(failed) > 0 && ctx.Err() == nil; retryRound++ {
			toRetry := failed
			failed = nil
			for _, failure := range toRetry {
				_, submitted := pool.submit(failure.slug)
				if !submitted {
					break
				}
			}
			pool.flush()
			failed = pool.takeFailures()
		}

		if len(failed) > 0 {
			if err := appendFailedCandidates(failedPath, failed); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			break
		}
		if resume && len(failed) > maxCheckpointFailures {
			return fmt.Errorf("%d checks failed in candidates %d-%d; checkpoint remains at %d and failures were saved to %s",
				len(failed), batchStart, batchEnd-1, lastSaved, failedPath)
		}

		next = batchEnd
		processed += batchEnd - batchStart
		if resume {
			const maxRetries = 3
			backoff := time.Second
			var saveErr error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				saveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				saveErr = database.SaveProviderBruteCheckpoint(saveCtx, pool.provider, minLength, maxLength, alphabet, next, total)
				cancel()
				if saveErr == nil {
					lastSaved = next
					break
				}
				if attempt == maxRetries {
					log.Printf("Warning: could not save brute-force checkpoint for %s at candidate %d of %d after %d attempts; continuing: %v",
						pool.provider, next, total, maxRetries+1, saveErr)
					break
				}

				jitter := time.Duration(rand.Int63n(int64(backoff/5*2)+1)) - backoff/5
				time.Sleep(backoff + jitter)
				backoff *= 2
			}
		}

		now := time.Now()
		if now.Sub(lastProgress) >= progressInterval || next == total {
			elapsed := now.Sub(started)
			rate := float64(processed) / elapsed.Seconds()
			remaining := total - next
			eta := time.Duration(0)
			if rate > 0 {
				eta = time.Duration(float64(remaining)/rate) * time.Second
			}
			log.Printf("Brute-force progress: %d/%d candidates (%.2f%%), %.0f candidates/sec, ETA %s%s",
				next, total, float64(next)*100/float64(total), rate, eta.Round(time.Second), pool.proxies.progressNote())
			lastProgress = now
		}
	}

	if ctx.Err() != nil {
		if resume {
			log.Printf("Interrupted; saved checkpoint is at candidate %d of %d. Rerun with --brute --resume", lastSaved, total)
		} else {
			log.Printf("Interrupted at candidate %d of %d; rerun with --brute --resume to enable checkpoints", next, total)
		}
	}
	return nil
}

func appendFailedCandidates(path string, failures []checkResult) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open failed-candidates file %q: %w", path, err)
	}
	for _, failure := range failures {
		if _, err := fmt.Fprintln(file, failure.slug); err != nil {
			file.Close()
			return fmt.Errorf("write failed candidate %q to %q: %w", failure.slug, path, err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close failed-candidates file %q: %w", path, err)
	}
	return nil
}

// bruteSlugAt maps a zero-based position to the same lexicographic sequence as generate.
func bruteSlugAt(minLength, maxLength int, index int64) string {
	for length := minLength; length <= maxLength; length++ {
		count := int64(1)
		for i := 0; i < length; i++ {
			count *= int64(len(alphabet))
		}
		if index >= count {
			index -= count
			continue
		}

		slug := make([]byte, length)
		for i := length - 1; i >= 0; i-- {
			slug[i] = alphabet[index%int64(len(alphabet))]
			index /= int64(len(alphabet))
		}
		return string(slug)
	}
	return ""
}
