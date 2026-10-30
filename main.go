package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"db"
)

const baseURL = "https://boards-api.greenhouse.io/v1/boards/%s"

// Global slug length bounds, used for brute-force generation and
// to filter wordlist-derived candidates. Overridable via flags.
var (
	minSlugLength = 4
	maxSlugLength = 5
)

var (
	alphabet = "abcdefghijklmnopqrstuvwxyz"
	nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)
)

func main() {
	words := flag.String("words", "", "file with company names or slugs, one per line")
	brute := flag.Bool("brute", false, "also brute-force slugs (low priority)")
	bench := flag.Bool("bench", false, "benchmark requests without modifying local.db")
	resume := flag.Bool("resume", false, "resume or checkpoint brute-force progress (requires --brute)")
	update := flag.Bool("update", false, "update indexed data")
	updateJobsFlag := flag.Bool("jobs", false, "fetch and refresh jobs for all known companies")
	browse := flag.Bool("browse", false, "launch the local job browser")
	browseAddress := flag.String("addr", "127.0.0.1:8080", "job browser listen address")
	workers := flag.Int("workers", 64, "concurrent workers")
	rps := flag.Float64("rps", 50, "max requests per second")
	flag.IntVar(&minSlugLength, "min", minSlugLength, "minimum slug length")
	flag.IntVar(&maxSlugLength, "max", maxSlugLength, "maximum slug length")
	flag.Parse()

	if minSlugLength < 1 || maxSlugLength < minSlugLength || (*brute && maxSlugLength > 13) {
		log.Fatalf("invalid length range: min=%d max=%d", minSlugLength, maxSlugLength)
	}
	if *workers < 1 {
		log.Fatal("workers must be at least 1")
	}
	if *rps <= 0 {
		log.Fatal("rps must be greater than 0")
	}
	if *resume && (!*brute || *bench || *words != "" || *browse || *update) {
		log.Fatal("--resume requires --brute and cannot be combined with --bench, --words, --browse, or --update")
	}
	if *update != *updateJobsFlag {
		log.Fatal("--update and --jobs must be used together")
	}
	if *bench && *update {
		log.Fatal("--bench cannot be combined with --update or --jobs")
	}
	if *browse && (*bench || *update) {
		log.Fatal("--browse cannot be combined with --bench or --update")
	}
	if *brute && maxSlugLength >= 6 {
		log.Printf("warning: brute force up to length %d means %d candidates",
			maxSlugLength, bruteCount(minSlugLength, maxSlugLength))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var (
		database       *db.DB
		err            error
		benchmarkClean func()
	)
	if *bench {
		database, benchmarkClean, err = newBenchmarkDatabase(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if benchmarkClean != nil {
				benchmarkClean()
			}
		}()
	} else {
		database, err = db.New(ctx, "local.db")
		if err != nil {
			log.Fatal(err)
		}
		defer database.Close()
	}

	client := newClient(*workers)
	burst := int(*rps)
	if burst < 1 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(*rps), burst)
	var stats *benchmarkStats
	if *bench {
		stats = newBenchmarkStats()
	}
	if *browse {
		if err := serveJobBrowser(ctx, database, *browseAddress); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *update && *updateJobsFlag {
		client.Timeout = 30 * time.Second
		if err := updateAllJobs(ctx, database, client, limiter, *workers); err != nil {
			log.Fatal(err)
		}
		return
	}

	pool := newCheckPool(ctx, client, limiter, database, stats, *workers)

	seen := make(map[string]struct{})
	emit := func(slug string) bool {
		if len(slug) < minSlugLength || len(slug) > maxSlugLength {
			return true // out of range, skip but keep going
		}
		if _, ok := seen[slug]; ok {
			return true
		}
		_, submitted := pool.submit(slug)
		if submitted {
			seen[slug] = struct{}{}
			return true
		}
		return false
	}

	started := time.Now()
	if *words != "" {
		if err := feedWordlist(*words, emit); err != nil {
			log.Printf("wordlist: %v", err)
		}
	}
	pool.flush()
	var runErr error
	if *brute {
		runErr = runBruteForce(ctx, pool, seen, database, minSlugLength, maxSlugLength, *resume)
	}

	pool.close()
	if stats != nil {
		stats.print(time.Since(started))
	}
	if runErr != nil {
		if benchmarkClean != nil {
			benchmarkClean()
			benchmarkClean = nil
		}
		log.Fatal(runErr)
	}
}

// bruteCount returns how many candidates brute force would generate.
func bruteCount(min, max int) int64 {
	total := int64(0)
	for l := min; l <= max; l++ {
		n := int64(1)
		for i := 0; i < l; i++ {
			n *= int64(len(alphabet))
		}
		total += n
	}
	return total
}

func newBenchmarkDatabase(ctx context.Context) (*db.DB, func(), error) {
	dir, err := os.MkdirTemp("", "crawler-benchmark-")
	if err != nil {
		return nil, nil, fmt.Errorf("create temporary benchmark database directory: %w", err)
	}
	database, err := db.New(ctx, filepath.Join(dir, "benchmark.db"))
	if err != nil {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			return nil, nil, fmt.Errorf("open temporary benchmark database: %w (cleanup failed: %v)", err, removeErr)
		}
		return nil, nil, fmt.Errorf("open temporary benchmark database: %w", err)
	}
	cleanup := func() {
		if err := database.Close(); err != nil {
			log.Printf("close temporary benchmark database: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("remove temporary benchmark database: %v", err)
		}
	}
	return database, cleanup, nil
}

func newClient(workers int) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        workers * 2,
		MaxIdleConnsPerHost: workers * 2,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 3 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: 8 * time.Second}
}

func feedWordlist(path string, emit func(string) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		for _, v := range variants(sc.Text()) {
			if !emit(v) {
				return nil
			}
		}
	}
	return sc.Err()
}

// variants turns "Acme Corp, Inc." into likely slugs.
func variants(name string) []string {
	n := strings.ToLower(strings.TrimSpace(name))
	for _, suf := range []string{", inc.", ", inc", " inc.", " inc", " llc", " ltd", " corp", " corporation", " co"} {
		n = strings.TrimSuffix(n, suf)
	}
	joined := nonAlnum.ReplaceAllString(n, "")
	hyphen := strings.Trim(nonAlnum.ReplaceAllString(n, "-"), "-")
	if joined == "" {
		return nil
	}
	return []string{joined, hyphen, joined + "hq", joined + "inc", joined + "careers"}
}

func checkCompany(
	ctx context.Context,
	client *http.Client,
	limiter *rate.Limiter,
	database *db.DB,
	slug string,
	stats *benchmarkStats,
) error {
	url := fmt.Sprintf(baseURL, slug)
	backoff := 500 * time.Millisecond

	for attempt := 0; attempt < 4; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return err
		}

		requestStarted := time.Now()
		res, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
		}
		if stats != nil {
			status := 0
			if res != nil {
				status = res.StatusCode
			}
			stats.recordRequest(time.Since(requestStarted), status, err != nil, attempt > 0)
		}
		if err == nil {
			switch {
			case res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNoContent:
				if database != nil {
					if err := database.AddCompany(ctx, slug); err != nil {
						return fmt.Errorf("saving company: %w", err)
					}
					log.Printf("Found company: %s", slug)
				}
				return nil
			case res.StatusCode == http.StatusNotFound:
				return nil
			case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
				// fall through to backoff and retry
			default:
				return fmt.Errorf("unexpected status %d", res.StatusCode)
			}
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("gave up after retries")
}

var benchmarkLatencyBuckets = []time.Duration{
	time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	8 * time.Second,
}

type benchmarkStats struct {
	mu             sync.Mutex
	requests       uint64
	retries        uint64
	transportError uint64
	ok             uint64
	notFound       uint64
	rateLimited    uint64
	serverError    uint64
	otherStatus    uint64
	checks         uint64
	checkFailures  uint64
	totalLatency   time.Duration
	minLatency     time.Duration
	maxLatency     time.Duration
	latencyBuckets []uint64
}

func newBenchmarkStats() *benchmarkStats {
	return &benchmarkStats{latencyBuckets: make([]uint64, len(benchmarkLatencyBuckets)+1)}
}

func (s *benchmarkStats) recordRequest(latency time.Duration, status int, transportError, retry bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests++
	if retry {
		s.retries++
	}
	if transportError {
		s.transportError++
	} else {
		switch {
		case status >= 200 && status < 300:
			s.ok++
		case status == http.StatusNotFound:
			s.notFound++
		case status == http.StatusTooManyRequests:
			s.rateLimited++
		case status >= 500:
			s.serverError++
		default:
			s.otherStatus++
		}
	}

	s.totalLatency += latency
	if s.requests == 1 || latency < s.minLatency {
		s.minLatency = latency
	}
	if latency > s.maxLatency {
		s.maxLatency = latency
	}
	bucket := len(benchmarkLatencyBuckets)
	for i, upperBound := range benchmarkLatencyBuckets {
		if latency <= upperBound {
			bucket = i
			break
		}
	}
	s.latencyBuckets[bucket]++
}

func (s *benchmarkStats) recordCheck(failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checks++
	if failed {
		s.checkFailures++
	}
}

func (s *benchmarkStats) print(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	requestsPerSecond := float64(0)
	if elapsed > 0 {
		requestsPerSecond = float64(s.requests) / elapsed.Seconds()
	}
	average := time.Duration(0)
	if s.requests > 0 {
		average = s.totalLatency / time.Duration(s.requests)
	}

	log.Printf("Benchmark results:")
	log.Printf("  Duration: %s", elapsed.Round(time.Millisecond))
	log.Printf("  Candidate checks: %d (%d failed)", s.checks, s.checkFailures)
	log.Printf("  Requests: %d (%d retries), %.2f requests/sec", s.requests, s.retries, requestsPerSecond)
	log.Printf("  HTTP responses: 2xx=%d 404=%d 429=%d 5xx=%d other=%d transport-errors=%d",
		s.ok, s.notFound, s.rateLimited, s.serverError, s.otherStatus, s.transportError)
	log.Printf("  Latency: min=%s avg=%s p50=%s p95=%s p99=%s max=%s",
		s.minLatency, average, s.percentile(0.50), s.percentile(0.95), s.percentile(0.99), s.maxLatency)
}

func (s *benchmarkStats) percentile(percent float64) string {
	if s.requests == 0 {
		return "n/a"
	}

	target := uint64(float64(s.requests)*percent + 0.999999)
	var cumulative uint64
	for i, count := range s.latencyBuckets {
		cumulative += count
		if cumulative >= target {
			if i == len(benchmarkLatencyBuckets) {
				return ">" + benchmarkLatencyBuckets[i-1].String()
			}
			return "<=" + benchmarkLatencyBuckets[i].String()
		}
	}
	return "n/a"
}
