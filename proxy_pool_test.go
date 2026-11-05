package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	golangRate "golang.org/x/time/rate"

	"db"
)

func newTestPool(cfg proxyPoolConfig, peers ...string) *proxyPool {
	pool := &proxyPool{cfg: cfg}
	for _, raw := range peers {
		pool.peers = append(pool.peers, &proxyPeer{raw: raw, client: &http.Client{}})
	}
	pool.healthy = len(pool.peers)
	return pool
}

func freeConfig() proxyPoolConfig {
	return proxyPoolConfig{
		minReuseGap:     0,
		maxFailures:     0,
		retryAfterFloor: 0,
		maxCooldown:     0,
	}
}

func testLimiter() *requestLimiter {
	return newRequestLimiter(golangRate.NewLimiter(golangRate.Inf, 1), 0)
}

func TestProxyPoolAcquiresExclusiveLeases(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80", "http://b:80")

	first, direct, err := pool.acquire(context.Background())
	if err != nil || direct {
		t.Fatalf("first acquire: peer=%v direct=%t err=%v", first, direct, err)
	}
	second, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.raw == second.raw {
		t.Fatalf("acquired the same proxy twice: %s", first.raw)
	}

	leased := make(chan *proxyPeer, 1)
	acquireCtx, cancelAcquire := context.WithCancel(context.Background())
	defer cancelAcquire()
	go func() {
		peer, _, err := pool.acquire(acquireCtx)
		if err == nil {
			leased <- peer
			return
		}
		_ = len(leased)
	}()
	select {
	case peer := <-leased:
		t.Fatalf("third acquire leased %s while all peers were in use", peer.raw)
	case <-time.After(50 * time.Millisecond):
	}
	// Releasing one peer lets the blocked acquire through.
	pool.release(first, proxyOutcomeOK, 0)
	select {
	case peer := <-leased:
		if peer.raw != first.raw {
			t.Fatalf("third acquire leased %s, want the released %s", peer.raw, first.raw)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("blocked acquire did not resolve after a release")
	}
}

func TestProxyPoolPrefersLeastRecentlyUsed(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80", "http://b:80")

	a, _, _ := pool.acquire(context.Background())
	pool.release(a, proxyOutcomeOK, 0)
	b, _, _ := pool.acquire(context.Background())
	pool.release(b, proxyOutcomeOK, 0)

	c, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.raw != a.raw {
		t.Fatalf("acquired %s, want least-recently-used %s", c.raw, a.raw)
	}
}

func TestProxyPoolHonorsReuseGap(t *testing.T) {
	cfg := freeConfig()
	cfg.minReuseGap = 20 * time.Millisecond
	pool := newTestPool(cfg, "http://a:80", "http://b:80")

	a, _, _ := pool.acquire(context.Background())
	pool.release(a, proxyOutcomeOK, 0)

	next, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.raw == a.raw {
		t.Fatalf("reused %s during its reuse gap", a.raw)
	}
	pool.release(next, proxyOutcomeOK, 0)
	time.Sleep(cfg.minReuseGap + 10*time.Millisecond)

	again, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// a became eligible first; b is still gap blocked.
	if again.raw != a.raw {
		t.Fatalf("acquired %s after the gap, want %s", again.raw, a.raw)
	}
}

func TestProxyPool429CooldownFloorsAndCapsRetryAfter(t *testing.T) {
	cfg := freeConfig()
	cfg.retryAfterFloor = 100 * time.Millisecond
	cfg.maxCooldown = 200 * time.Millisecond
	pool := newTestPool(cfg, "http://a:80", "http://b:80")

	a, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeRateLimited, 50*time.Millisecond)
	cooled := time.Until(a.cooldownUntil)
	if cooled > cfg.maxCooldown+30*time.Millisecond {
		t.Fatalf("cooldown %s exceeded the cap %s", cooled, cfg.maxCooldown)
	}
	if cooled < cfg.retryAfterFloor-30*time.Millisecond {
		t.Fatalf("cooldown %s was below the floor %s", cooled, cfg.retryAfterFloor)
	}
	if pool.healthyPeers() != 2 {
		t.Fatalf("healthy peers = %d after a rate limit, want 2", pool.healthyPeers())
	}

	next, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.raw == a.raw {
		t.Fatalf("reused %s while its cooldown was active", a.raw)
	}

	// After the capped cooldown expires the peer becomes eligible again.
	time.Sleep(cfg.maxCooldown + 50*time.Millisecond)
	again, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again.raw != a.raw {
		t.Fatalf("acquired %s after the capped cooldown expired, want %s", again.raw, a.raw)
	}
}

func TestProxyPoolEvictsAfterConsecutiveFailures(t *testing.T) {
	cfg := freeConfig()
	cfg.maxFailures = 2
	pool := newTestPool(cfg, "http://a:80")

	a, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeFailed, 0)
	a2, _, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a2.raw != a.raw {
		t.Fatalf("acquired %s, want the single failing peer %s", a2.raw, a.raw)
	}
	pool.release(a2, proxyOutcomeFailed, 0)

	if pool.healthyPeers() != 0 {
		t.Fatalf("healthy peers = %d after eviction, want 0", pool.healthyPeers())
	}
	_, direct, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !direct {
		t.Fatal("drained pool did not request the direct fallback")
	}
}

func TestProxyPoolSuccessResetsFailureCount(t *testing.T) {
	cfg := freeConfig()
	cfg.maxFailures = 2
	pool := newTestPool(cfg, "http://a:80")

	a, _, _ := pool.acquire(context.Background())
	pool.release(a, proxyOutcomeFailed, 0)
	pool.release(a, proxyOutcomeOK, 0)
	if pool.healthyPeers() != 1 {
		t.Fatal("success did not reset the failure count")
	}
	a2, _, _ := pool.acquire(context.Background())
	pool.release(a2, proxyOutcomeFailed, 0)
	if pool.healthyPeers() != 1 {
		t.Fatalf("evicted after only %d consecutive failures, want no eviction", a2.failures)
	}
}

func TestProxyPoolDirectFallbackWithNoHealthyPeers(t *testing.T) {
	pool := newTestPool(freeConfig())

	_, direct, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !direct {
		t.Fatal("empty pool did not request the direct fallback")
	}
}

func TestProxyPoolAcquireHonorsContextCancellation(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80")
	leased, _, _ := pool.acquire(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, err := pool.acquire(ctx)
		finished <- err
	}()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("acquired a leased peer")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("acquire ignored context cancellation")
	}
	pool.release(leased, proxyOutcomeOK, 0)
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Now()
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"12", 12 * time.Second},
		{" 5 ", 5 * time.Second},
		{"0", 0},
		{"-3", 0},
		{"", 0},
		{"not-a-number", 0},
		{now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 89 * time.Second},
		{now.Add(-30 * time.Second).UTC().Format(http.TimeFormat), 0},
	}
	for _, testCase := range cases {
		got := parseRetryAfter(testCase.header, now)
		lower := testCase.want
		upper := testCase.want + time.Second
		if got < lower || got > upper {
			t.Errorf("parseRetryAfter(%q) = %s, want in [%s, %s]", testCase.header, got, lower, upper)
		}
	}
}

func TestLoadProxyPoolFiltersAndKeepsUptimeOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	socksURL := strings.Replace(server.URL, "http://", "socks5://", 1)
	entries := []string{
		fmt.Sprintf(`{"proxy":%q,"protocol":"http","alive":true,"anonymity":"elite","uptime":5}`, server.URL),
		`{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"elite","uptime":9}`,
		`{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":false,"anonymity":"elite","uptime":9}`,
		`{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"transparent","uptime":9}`,
		`{"proxy":"http://127.0.0.1:1937","protocol":"ftp","alive":true,"anonymity":"elite","uptime":9}`,
		fmt.Sprintf(`{"proxy":%q,"protocol":"socks5","alive":true,"anonymity":"elite","uptime":7}`, socksURL),
		fmt.Sprintf(`{"proxy":%q,"protocol":"http","alive":true,"anonymity":"elite","uptime":1}`, server.URL),
	}
	path := filepath.Join(t.TempDir(), "proxies.json")
	if err := os.WriteFile(path, []byte("["+strings.Join(entries, ",")+"]"), 0o600); err != nil {
		t.Fatal(err)
	}

	pool, err := loadProxyPool(context.Background(), path, freeConfig(), time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if pool.healthyPeers() != 2 {
		t.Fatalf("healthy peers = %d, want 2 (duplicate and invalid entries removed)", pool.healthyPeers())
	}
	if pool.peers[0].raw != socksURL {
		t.Fatalf("first peer = %s, want the highest uptime survivor %s", pool.peers[0].raw, socksURL)
	}
	if pool.peers[1].raw != server.URL {
		t.Fatalf("second peer = %s, want the remaining survivor %s", pool.peers[1].raw, server.URL)
	}
}

func TestDecodeProxyListAcceptsBareArrayAndWrappedObject(t *testing.T) {
	bare := `[{"proxy":"http://127.0.0.1:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":5}]`
	entries, err := decodeProxyList([]byte(bare))
	if err != nil || len(entries) != 1 || entries[0].Proxy != "http://127.0.0.1:8080" {
		t.Fatalf("bare array: entries=%v err=%v", entries, err)
	}

	wrapped := `{"shown_records":1,"nextpage":false,"proxies":[{"proxy":"socks5://127.0.0.1:1080","protocol":"socks5","alive":true,"anonymity":"elite","uptime":9}]}`
	entries, err = decodeProxyList([]byte(wrapped))
	if err != nil || len(entries) != 1 || entries[0].Proxy != "socks5://127.0.0.1:1080" {
		t.Fatalf("wrapped object: entries=%v err=%v", entries, err)
	}

	empty := `[]`
	entries, err = decodeProxyList([]byte(empty))
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty array: entries=%v err=%v, want no error", entries, err)
	}

	wrappedWithoutProxies := `{"shown_records":0,"nextpage":false,"proxies":[{}]}`
	if _, err := decodeProxyList([]byte(wrappedWithoutProxies)); err == nil {
		t.Fatal("object without proxy entries accepted")
	}
	if _, err := decodeProxyList([]byte(`not json`)); err == nil {
		t.Fatal("non-JSON input accepted")
	}
}

func TestLoadProxyPoolFailsWithoutAliveProxy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxies.json")
	content := `{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"elite","uptime":9}`
	if err := os.WriteFile(path, []byte("["+content+"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProxyPool(context.Background(), path, freeConfig(), 300*time.Millisecond, 5*time.Second); err == nil {
		t.Fatal("expected an error when no proxy passes the health probe")
	}
}

func TestFetchProviderResponseRotatesProxyOn429(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}

	limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		hits["limited"]++
		mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     "429 Too Many Requests",
			Header:     http.Header{"Retry-After": []string{"60"}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}}
	healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		hits["healthy"]++
		mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}}
	pool := &proxyPool{
		peers:   []*proxyPeer{limitedPeer, healthyPeer},
		healthy: 2,
		cfg:     freeConfig(),
	}
	provider := rotationTestProvider{endpoints: []string{"https://boards.example/boards/example"}}

	response, err := fetchProviderResponse(
		context.Background(), &http.Client{}, pool, testLimiter(), provider, "example", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !response.ok {
		t.Fatal("rotation did not end with a successful response")
	}

	mu.Lock()
	defer mu.Unlock()
	if hits["limited"] != 1 || hits["healthy"] != 1 {
		t.Fatalf("hit counts = %v, want exactly one request per proxy", hits)
	}
	if limitedPeer.failures != 1 {
		t.Fatalf("rate limited peer failures = %d, want 1", limitedPeer.failures)
	}
	if time.Until(limitedPeer.cooldownUntil) <= 0 {
		t.Fatal("Retry-After did not set a cooldown on the rate limited peer")
	}
	if !healthyPeer.cooldownUntil.IsZero() {
		t.Fatal("healthy peer received an unexpected cooldown")
	}
}

// rotationTestProvider is a minimal jobBoardProvider used to exercise
// fetchProviderResponse directly.
type rotationTestProvider struct {
	endpoints []string
}

func (p rotationTestProvider) Name() string                     { return "rotationtest" }
func (p rotationTestProvider) CompanyEndpoints(string) []string { return p.endpoints }
func (rotationTestProvider) FallbackOnNotFound() bool           { return false }

func (p rotationTestProvider) CheckCompany(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string, stats *benchmarkStats,
) (bool, error) {
	response, err := fetchProviderResponse(ctx, client, pool, limiter, p, slug, stats)
	if err != nil || !response.ok {
		return false, err
	}
	return true, nil
}

func (p rotationTestProvider) FetchJobs(
	ctx context.Context, client *http.Client, pool *proxyPool, limiter *requestLimiter, slug string,
) ([]db.Job, error) {
	response, err := fetchProviderResponse(ctx, client, pool, limiter, p, slug, nil)
	if err != nil {
		return nil, err
	}
	if !response.ok {
		return nil, fmt.Errorf("rotation test provider: not found")
	}
	return []db.Job{}, nil
}
