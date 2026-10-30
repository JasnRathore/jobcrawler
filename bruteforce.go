package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"db"
	"golang.org/x/time/rate"
)

const (
	checkQueueSize       = 1024
	bruteCheckpointBatch = int64(10000)
	progressInterval     = 5 * time.Second
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
	limiter  *rate.Limiter
	database *db.DB
	stats    *benchmarkStats
}

func newCheckPool(ctx context.Context, client *http.Client, limiter *rate.Limiter, database *db.DB, stats *benchmarkStats, workers int) *checkPool {
	pool := &checkPool{
		jobs:     make(chan string, checkQueueSize),
		results:  make(chan checkResult, workers),
		ctx:      ctx,
		client:   client,
		limiter:  limiter,
		database: database,
		stats:    stats,
	}
	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for slug := range pool.jobs {
				err := checkCompany(pool.ctx, pool.client, pool.limiter, pool.database, slug, pool.stats)
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
		}
	}
	p.pending = 0
	return failures
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
) error {
	total := bruteCount(minLength, maxLength)
	next := int64(0)
	if resume {
		var err error
		next, err = database.StartBruteCheckpoint(ctx, minLength, maxLength, alphabet, total)
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
	var pendingFailures int
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
			failures, submitted := pool.submit(slug)
			pendingFailures += failures
			if !submitted {
				break
			}
		}
		pendingFailures += pool.flush()
		if ctx.Err() != nil {
			break
		}
		if resume && pendingFailures > 0 {
			return fmt.Errorf("%d checks failed in candidates %d-%d; checkpoint remains at %d, rerun with --resume",
				pendingFailures, batchStart, batchEnd-1, lastSaved)
		}

		next = batchEnd
		processed += batchEnd - batchStart
		if resume {
			saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := database.SaveBruteCheckpoint(saveCtx, minLength, maxLength, alphabet, next, total)
			cancel()
			if err != nil {
				return err
			}
			lastSaved = next
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
			log.Printf("Brute-force progress: %d/%d candidates (%.2f%%), %.0f candidates/sec, ETA %s",
				next, total, float64(next)*100/float64(total), rate, eta.Round(time.Second))
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
