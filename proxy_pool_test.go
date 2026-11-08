package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func TestProxyPoolLeasesExclusivelyAndRotates(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80", "http://b:80")

	first, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if first.raw == second.raw {
		t.Fatalf("both leases used %s; concurrent workers must rotate", first.raw)
	}

	// A third acquire has no idle peer and must block instead of
	// double-booking a leased route.
	leased := make(chan *proxyPeer, 1)
	acquireCtx, cancelAcquire := context.WithCancel(context.Background())
	defer cancelAcquire()
	go func() {
		if peer, err := pool.acquire(acquireCtx); err == nil {
			leased <- peer
		}
	}()
	select {
	case peer := <-leased:
		t.Fatalf("third acquire leased %s while all peers were in use", peer.raw)
	case <-time.After(50 * time.Millisecond):
	}
	// Releasing one route lets the waiting request continue on exactly
	// that route.
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

func TestProxyPoolRotatesAcrossPeers(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80", "http://b:80", "http://c:80")

	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		peer, err := pool.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		seen[peer.raw]++
		pool.release(peer, proxyOutcomeOK, 0)
	}
	if len(seen) != 3 {
		t.Fatalf("leases visited %+v; expected each of the three peers", seen)
	}

	// Sequential leases on two peers keep alternating instead of pinning
	// the first route.
	pool = newTestPool(freeConfig(), "http://a:80", "http://b:80")
	seen = map[string]int{}
	for i := 0; i < 4; i++ {
		peer, err := pool.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		seen[peer.raw]++
		pool.release(peer, proxyOutcomeOK, 0)
	}
	if len(seen) != 2 {
		t.Fatalf("leases visited %+v; expected rotation across both peers", seen)
	}
}

func TestProxyPoolHonorsPerProxyReuseGap(t *testing.T) {
	cfg := freeConfig()
	cfg.minReuseGap = 20 * time.Millisecond
	pool := newTestPool(cfg, "http://a:80", "http://b:80")

	a, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeOK, 0)

	// The gap applies per route: the other route serves right away, so
	// rotation avoids waiting out the gap.
	started := time.Now()
	next, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.raw == a.raw {
		t.Fatalf("reused %s inside its %s reuse gap", a.raw, cfg.minReuseGap)
	}
	if waited := time.Since(started); waited >= cfg.minReuseGap {
		t.Fatalf("lease waited %s; healthy routes must serve during the gap", waited)
	}
	pool.release(next, proxyOutcomeOK, 0)

	// A single-route pool has no alternative and must wait out the gap.
	solo := newTestPool(cfg, "http://a:80")
	first, err := solo.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	solo.release(first, proxyOutcomeOK, 0)
	started = time.Now()
	again, err := solo.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(started); waited < cfg.minReuseGap {
		t.Fatalf("single-peer pool reused after %s, before the %s gap", waited, cfg.minReuseGap)
	}
	solo.release(again, proxyOutcomeOK, 0)
}

func TestProxyPool429CooldownFloorsCapsAndRotates(t *testing.T) {
	cfg := freeConfig()
	cfg.retryAfterFloor = 100 * time.Millisecond
	cfg.maxCooldown = 200 * time.Millisecond
	pool := newTestPool(cfg, "http://a:80", "http://b:80")

	a, err := pool.acquire(context.Background())
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

	// The cooled route is skipped; a healthy route serves without waiting
	// for the cooldown to expire.
	started := time.Now()
	next, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.raw == a.raw {
		t.Fatalf("acquired cooling %s while %s was healthy", next.raw, a.raw)
	}
	if waited := time.Since(started); waited >= cfg.retryAfterFloor {
		t.Fatalf("lease waited %s; healthy routes must serve during a cooldown", waited)
	}
	pool.release(next, proxyOutcomeOK, 0)
}

func TestProxyPoolEvictsAfterConsecutiveFailures(t *testing.T) {
	cfg := freeConfig()
	cfg.maxFailures = 2
	pool := newTestPool(cfg, "http://a:80")

	a, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeFailed, 0)
	a2, err := pool.acquire(context.Background())
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
	if _, err := pool.acquire(context.Background()); !errors.Is(err, errProxyExhausted) {
		t.Fatalf("acquire after eviction = %v, want errProxyExhausted", err)
	}
}

func TestEvictedProxyRotatesTrafficToSurvivors(t *testing.T) {
	cfg := freeConfig()
	cfg.maxFailures = 2
	pool := newTestPool(cfg, "http://a:80", "http://b:80")

	// a fails one request. Rotation sends the next request to b because
	// it is now the least-recently-used route, and nothing is evicted yet
	// because a has not reached two consecutive failures.
	a, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeFailed, 0)
	if pool.healthyPeers() != 2 {
		t.Fatalf("healthy = %d after one failure, want 2", pool.healthyPeers())
	}

	lease, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.raw != "http://b:80" {
		t.Fatalf("acquired %s after a failed request on a, want rotation to b", lease.raw)
	}
	pool.release(lease, proxyOutcomeOK, 0)

	// Rotation returns to a, whose second consecutive failure evicts it;
	// workers keep serving through the surviving route.
	lease, err = pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.raw != "http://a:80" {
		t.Fatalf("acquired %s, want the least-recently-used route a", lease.raw)
	}
	pool.release(lease, proxyOutcomeFailed, 0)
	if pool.healthyPeers() != 1 {
		t.Fatalf("healthy = %d after a was evicted, want 1", pool.healthyPeers())
	}

	lease, err = pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.raw != "http://b:80" {
		t.Fatalf("acquired %s after eviction, want surviving route b", lease.raw)
	}
	pool.release(lease, proxyOutcomeOK, 0)
}

func TestProxyPoolSuccessResetsFailureCount(t *testing.T) {
	cfg := freeConfig()
	cfg.maxFailures = 2
	pool := newTestPool(cfg, "http://a:80")

	a, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a, proxyOutcomeFailed, 0)
	pool.release(a, proxyOutcomeOK, 0)
	if pool.healthyPeers() != 1 {
		t.Fatal("success did not reset the failure count")
	}
	a2, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.release(a2, proxyOutcomeFailed, 0)
	if pool.healthyPeers() != 1 {
		t.Fatalf("evicted after only %d consecutive failures, want no eviction", a2.failures)
	}
}

func TestProxyPoolExhaustedPoolFailsClosed(t *testing.T) {
	pool := newTestPool(freeConfig())

	if _, err := pool.acquire(context.Background()); !errors.Is(err, errProxyExhausted) {
		t.Fatalf("empty pool acquire = %v, want errProxyExhausted", err)
	}
}

func TestProxyPoolAcquireHonorsContextCancellation(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80")
	leased, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := pool.acquire(ctx)
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

func TestLoadProxyPoolKeepsEveryEligibleRouteWithoutTargetProbe(t *testing.T) {
	var targetRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
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

	pool, err := loadProxyPool(context.Background(), path, freeConfig(), time.Second, 5*time.Second, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Every eligible entry becomes a rotating route: the duplicate-URL and
	// http duplicate entries collapse, leaving the highest-uptime route
	// first.
	if pool.healthyPeers() != 3 || len(pool.peers) != 3 {
		t.Fatalf("healthy peers = %d, len(peers) = %d; want all three eligible routes", pool.healthyPeers(), len(pool.peers))
	}
	if pool.peers[0].raw != "http://127.0.0.1:1937" {
		t.Fatalf("first route = %s, want highest-uptime eligible proxy", pool.peers[0].raw)
	}
	first, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.raw == second.raw {
		t.Fatalf("both leases used %s; loaded routes must rotate", first.raw)
	}
	pool.release(first, proxyOutcomeOK, 0)
	pool.release(second, proxyOutcomeOK, 0)
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("startup sent %d target probe(s), want none", got)
	}
}

func TestReleaseTracksPeerStats(t *testing.T) {
	pool := newTestPool(freeConfig(), "http://a:80")
	peer := pool.peers[0]
	pool.release(peer, proxyOutcomeOK, 0)
	pool.release(peer, proxyOutcomeOK, 0)
	pool.release(peer, proxyOutcomeRateLimited, time.Second)
	pool.release(peer, proxyOutcomeFailed, 0)

	entries := pool.snapshot()
	if len(entries) != 1 {
		t.Fatalf("snapshot has %d entries, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Successes != 2 || entry.Failures != 2 || entry.Requests != 4 {
		t.Fatalf("stats: successes=%d failures=%d requests=%d, want 2/2/4", entry.Successes, entry.Failures, entry.Requests)
	}
	if entry.LastUsed == "" {
		t.Fatal("last_used missing from snapshot")
	}

	// Consecutive failures still reset on a success.
	evictor := newTestPool(proxyPoolConfig{maxFailures: 2}, "http://x:80")
	target := evictor.peers[0]
	evictor.release(target, proxyOutcomeOK, 0)
	evictor.release(target, proxyOutcomeFailed, 0)
	if evictor.healthyPeers() != 1 {
		t.Fatalf("healthy=%d after one failure, want 1", evictor.healthyPeers())
	}
	evictor.release(target, proxyOutcomeFailed, 0)
	if evictor.healthyPeers() != 0 {
		t.Fatalf("healthy=%d after two consecutive failures, want 0", evictor.healthyPeers())
	}
	if _, err := evictor.acquire(context.Background()); !errors.Is(err, errProxyExhausted) {
		t.Fatalf("acquire after eviction = %v, want errProxyExhausted", err)
	}

	if note := pool.progressNote(); note != ", 1/1 proxy peers healthy" {
		t.Fatalf("progressNote = %q", note)
	}
	if note := (*proxyPool)(nil).progressNote(); note != "" {
		t.Fatalf("nil pool progressNote = %q, want empty", note)
	}
}

func TestSaveRoundTripExcludesEvictedAndOrdersBySuccesses(t *testing.T) {
	pool := newTestPool(proxyPoolConfig{maxFailures: 2}, "http://a:80", "http://b:80", "http://c:80")
	first, second, burned := pool.peers[0], pool.peers[1], pool.peers[2]

	// second proves itself three times, first once plus a rate limit,
	// and burned fails twice straight away and is evicted.
	pool.release(burned, proxyOutcomeFailed, 0)
	pool.release(burned, proxyOutcomeFailed, 0)
	pool.release(first, proxyOutcomeOK, 0)
	pool.release(second, proxyOutcomeOK, 0)
	pool.release(second, proxyOutcomeOK, 0)
	pool.release(second, proxyOutcomeOK, 0)
	pool.release(first, proxyOutcomeRateLimited, 0)

	path := filepath.Join(t.TempDir(), "ips.json")
	saved, err := pool.save(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved != 2 {
		t.Fatalf("saved=%d, want 2 (evicted peer excluded)", saved)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProxyList(data)
	if err != nil {
		t.Fatalf("saved file is not loadable: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded %d entries, want 2", len(decoded))
	}
	if decoded[0].Proxy != "http://b:80" || decoded[1].Proxy != "http://a:80" {
		t.Fatalf("order: %s before %s, want b (3 successes) before a (1)", decoded[0].Proxy, decoded[1].Proxy)
	}
	if decoded[0].Uptime != 3 || decoded[1].Uptime != 1 {
		t.Fatalf("uptimes %v then %v, want 3 then 1", decoded[0].Uptime, decoded[1].Uptime)
	}

	var entries []proxySaveEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatal(err)
	}
	if entries[0].Protocol != "http" || !entries[0].Alive || entries[0].Anonymity != "elite" {
		t.Fatalf("entry metadata: %+v", entries[0])
	}
	if entries[0].Requests != 3 || entries[0].Failures != 0 {
		t.Fatalf("second stats: %+v", entries[0])
	}
	if entries[1].Requests != 2 || entries[1].Successes != 1 || entries[1].Failures != 1 {
		t.Fatalf("first stats: %+v", entries[1])
	}

	drained := newTestPool(proxyPoolConfig{maxFailures: 2}, "http://nobody:80")
	drained.release(drained.peers[0], proxyOutcomeFailed, 0)
	drained.release(drained.peers[0], proxyOutcomeFailed, 0)
	drainedSaved, err := drained.save(filepath.Join(t.TempDir(), "empty.json"))
	if err != nil || drainedSaved != 0 {
		t.Fatalf("save with only evicted peers: saved=%d err=%v", drainedSaved, err)
	}
}

func TestDecodeGeonodeListMapsRecords(t *testing.T) {
	payload := `{"data":[` +
		`{"ip":"178.218.119.29","port":"10808","protocols":["socks5","http"],"anonymityLevel":"elite","responseTime":84,"upTime":98.7},` +
		`{"ip":"49.148.16.50","port":"8080","protocols":["http","socks4"],"anonymityLevel":"transparent","responseTime":1503},` +
		`{"ip":"203.0.113.9","port":8844,"protocols":["http"],"anonymityLevel":"elite"},` +
		`{"ip":"","port":"1","protocols":["http"]}` +
		`],"total":4}`

	entries, err := decodeProxyList([]byte(payload))
	if err != nil {
		t.Fatalf("decode geonode list: %v", err)
	}
	if len(entries) != 4 { // socks5 + http from record 1, http from record 2 (socks4 dropped), http from record 3
		t.Fatalf("decoded %d entries, want 4", len(entries))
	}
	if entries[0].Proxy != "socks5://178.218.119.29:10808" || entries[0].Uptime != -84 || entries[0].Anonymity != "elite" {
		t.Fatalf("entry 0: %+v, want socks5://178.218.119.29:10808 elite with uptime -84", entries[0])
	}
	if entries[1].Proxy != "http://178.218.119.29:10808" || entries[1].Uptime != -84 {
		t.Fatalf("entry 1: %+v", entries[1])
	}
	if entries[2].Proxy != "http://49.148.16.50:8080" || entries[2].Anonymity != "transparent" || entries[2].Uptime != -1503 {
		t.Fatalf("entry 2: %+v (transparent records are kept for the loader's elite filter)", entries[2])
	}
	// socks4 is dropped; an unquoted port number still maps cleanly.
	if entries[3].Proxy != "http://203.0.113.9:8844" || entries[3].Uptime != 0 {
		t.Fatalf("entry 3: %+v", entries[3])
	}
}

func TestLoadProxyPoolFetchesURLSource(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	serverHost := strings.TrimPrefix(target.URL, "http://") // host:port
	serverAddr, port, _ := strings.Cut(serverHost, ":")
	payload := fmt.Sprintf(`{"data":[{"ip":%q,"port":%q,"protocols":["socks5","http"],"anonymityLevel":"elite","responseTime":120,"upTime":98},`+
		`{"ip":"203.0.113.9","port":"9","protocols":["http"],"anonymityLevel":"transparent","responseTime":20}]}`,
		serverAddr, port)
	var requests int32
	listServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(payload))
	}))
	defer listServer.Close()

	peerPool, err := loadProxyPool(context.Background(), listServer.URL, freeConfig(), time.Second, 5*time.Second, "")
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&requests) != 1 {
		t.Fatalf("list fetches = %d, want 1", requests)
	}
	// The transparent record is filtered; both eligible routes are loaded
	// (the socks5 and http tunnel for the list host) without contacting
	// their hosts.
	if peerPool.healthyPeers() != 2 || len(peerPool.peers) != 2 {
		t.Fatalf("healthy peers = %d with %d peers, want two eligible routes", peerPool.healthyPeers(), len(peerPool.peers))
	}
	if peerPool.peers[0].raw != "socks5://"+serverHost {
		t.Fatalf("first peer = %s, want socks5://%s", peerPool.peers[0].raw, serverHost)
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("loaded route triggered %d target probe(s), want none", got)
	}
}

func TestFetchProxyListEntriesReportsErrors(t *testing.T) {
	denied := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer denied.Close()
	if _, err := fetchProxyListEntries(context.Background(), denied.URL, time.Second); err == nil || !strings.Contains(err.Error(), "status 403") {
		t.Fatalf("expected 403 error, got %v", err)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer broken.Close()
	if _, err := fetchProxyListEntries(context.Background(), broken.URL, time.Second); err == nil {
		t.Fatal("expected decode error for non-JSON body")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loadProxyPool(ctx, "https://127.0.0.1:1/list", freeConfig(), time.Second, 5*time.Second, ""); err == nil {
		t.Fatal("expected cancellation error for canceled context")
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

func TestLoadProxyPoolDoesNotRequireHealthProbe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxies.json")
	content := `{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"elite","uptime":9}`
	if err := os.WriteFile(path, []byte("["+content+"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	pool, err := loadProxyPool(context.Background(), path, freeConfig(), 300*time.Millisecond, 5*time.Second, "https://boards.example/probe")
	if err != nil {
		t.Fatalf("manual route was rejected without a health probe: %v", err)
	}
	if len(pool.peers) != 1 || pool.peers[0].raw != "http://127.0.0.1:1937" {
		t.Fatalf("unexpected route set: %+v", pool.peers)
	}
}

func TestCurateProxiesDoesNotProbeCandidates(t *testing.T) {
	candidates := []*collectCandidate{
		{entry: proxyListEntry{Proxy: "http://b:80", Protocol: "http", Alive: true, Anonymity: "elite"}},
		{entry: proxyListEntry{Proxy: "http://a:80", Protocol: "http", Alive: true, Anonymity: "elite"}},
		{entry: proxyListEntry{Proxy: "http://dead:80", Protocol: "http", Alive: true, Anonymity: "elite"}},
	}

	saved, err := curateProxies(context.Background(), candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) != 3 {
		t.Fatalf("curated %d entries, want all 3 eligible source entries", len(saved))
	}
	if saved[0].Proxy != "http://a:80" || saved[1].Proxy != "http://b:80" || saved[2].Proxy != "http://dead:80" {
		t.Fatalf("candidates not sorted deterministically: %+v", saved)
	}
	for _, entry := range saved {
		if entry.Requests != 0 || entry.Successes != 0 || entry.Failures != 0 {
			t.Fatalf("unverified candidate includes fabricated transport stats: %+v", entry)
		}
	}
}

func TestEligibleProxyEntriesFiltersAndCollapsesHosts(t *testing.T) {
	entries := []proxyListEntry{
		{Proxy: "http://45.74.31.41:7002", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 5},
		{Proxy: "http://45.74.31.41:7190", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 9},
		{Proxy: "socks5://2.2.2.2:1080", Protocol: "socks5", Alive: true, Anonymity: "elite", Uptime: 1},
		{Proxy: "http://3.3.3.3:8080", Protocol: "http", Alive: false, Anonymity: "elite", Uptime: 9},
		{Proxy: "http://4.4.4.4:8080", Protocol: "http", Alive: true, Anonymity: "transparent", Uptime: 9},
		{Proxy: "http://5.5.5.5:8080", Protocol: "ftp", Alive: true, Anonymity: "elite", Uptime: 9},
		{Proxy: "http://45.74.31.41:7002", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 7}, // duplicate URL
	}

	byURL := eligibleProxyEntries(entries, false)
	if len(byURL) != 3 {
		t.Fatalf("by-URL filtering kept %d entries, want 3: %+v", len(byURL), byURL)
	}
	// The duplicate URL (uptime 7) is dropped before sorting, leaving 9, 5, 1.
	if byURL[0].Uptime != 9 || byURL[1].Uptime != 5 || byURL[2].Uptime != 1 {
		t.Fatalf("by-URL order wrong: %+v", byURL)
	}

	byHost := eligibleProxyEntries(entries, true)
	if len(byHost) != 2 {
		t.Fatalf("by-host filtering kept %d entries, want 2: %+v", len(byHost), byHost)
	}
	if byHost[0].Proxy != "http://45.74.31.41:7190" {
		t.Fatalf("by-host kept %s, want the highest-uptime port", byHost[0].Proxy)
	}
	if byHost[1].Proxy != "socks5://2.2.2.2:1080" {
		t.Fatalf("by-host kept %s, want the other host", byHost[1].Proxy)
	}
}

func TestCollectProxiesWritesCuratedFile(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	targetHost := strings.TrimPrefix(target.URL, "http://")

	listPath := filepath.Join(t.TempDir(), "sources.json")
	entries := "[" +
		fmt.Sprintf(`{"proxy":"socks5://%s","protocol":"socks5","alive":true,"anonymity":"elite","uptime":5},`, targetHost) +
		fmt.Sprintf(`{"proxy":"http://%s","protocol":"http","alive":true,"anonymity":"elite","uptime":3},`, targetHost) +
		`{"proxy":"http://127.0.0.1:1","protocol":"http","alive":true,"anonymity":"elite","uptime":99}` +
		"]"
	if err := os.WriteFile(listPath, []byte(entries), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "ips.json")

	if err := collectProxies(context.Background(), output, []string{listPath}, 2, false, time.Second, 5*time.Second, ""); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProxyList(data)
	if err != nil {
		t.Fatalf("curated file is not loadable: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("curated %d entries, want all 3 eligible source candidates", len(decoded))
	}
	var saved []proxySaveEntry
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved[0].Proxy != "http://127.0.0.1:1" ||
		saved[1].Proxy != "http://"+targetHost ||
		saved[2].Proxy != "socks5://"+targetHost {
		t.Fatalf("saved candidates not sorted deterministically: %+v", saved)
	}
	for _, entry := range saved {
		if entry.Successes != 0 || entry.Requests != 0 || entry.Failures != 0 {
			t.Fatalf("unverified candidate has transport stats: %+v", entry)
		}
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("collection sent %d target probe(s), want none", got)
	}
}

func TestFetchProviderResponseRotatesOn429WithoutStopping(t *testing.T) {
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

	limiter := testLimiter()
	response, err := fetchProviderResponse(
		context.Background(), &http.Client{}, pool, limiter, provider, "example", nil)
	if err != nil {
		t.Fatalf("fetch error = %v, want rotation onto the healthy route", err)
	}
	if !response.ok || len(response.body) == 0 {
		t.Fatalf("response = %+v, want the healthy route's body", response)
	}

	mu.Lock()
	defer mu.Unlock()
	if hits["limited"] != 1 || hits["healthy"] != 1 {
		t.Fatalf("hit counts = %v, want one request per route: the 429 cools its route and the retry uses another", hits)
	}
	if limitedPeer.failures != 1 {
		t.Fatalf("rate limited peer failures = %d, want 1", limitedPeer.failures)
	}
	if limiter.providerStop(provider.Name()) != nil {
		t.Fatal("provider-wide stop was recorded for a 429; rate limits should rotate, not stop")
	}
	if !limiter.cooldownUntil.IsZero() {
		t.Fatal("a single 429 tripped the global pause, want route rotation instead")
	}
	if time.Until(limitedPeer.cooldownUntil) <= 0 {
		t.Fatal("Retry-After did not set a cooldown on the rate limited route")
	}
}

func TestRefreshOutputFeedsRotatingPool(t *testing.T) {
	// The documented stdout storage of "crawler proxy refresh" must be
	// directly usable as a --proxies list for later runs.
	gathered := `{"retrievedAt":"2026-01-01T00:00:00Z","sources":[{"name":"HProxy","count":2}],"proxies":[` +
		`{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"elite","uptime":90},` +
		`{"proxy":"http://127.0.0.1:1938","protocol":"http","alive":true,"anonymity":"elite","uptime":70}]}`
	path := filepath.Join(t.TempDir(), "gathered.json")
	if err := os.WriteFile(path, []byte(gathered), 0o600); err != nil {
		t.Fatal(err)
	}

	pool, err := loadProxyPool(context.Background(), path, freeConfig(), time.Second, 5*time.Second, "")
	if err != nil {
		t.Fatalf("refresh output was not usable as a --proxies list: %v", err)
	}
	if pool.healthyPeers() != 2 || len(pool.peers) != 2 {
		t.Fatalf("loaded %d peers (healthy %d), want both gathered entries", len(pool.peers), pool.healthyPeers())
	}
	first, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.raw == second.raw {
		t.Fatalf("both leases used %s; gathered routes did not rotate", first.raw)
	}
}

func TestFetchProviderResponseStopsWhenProxyPoolIsExhausted(t *testing.T) {
	exhausted := &proxyPool{peers: []*proxyPeer{{raw: "http://dead:80", client: &http.Client{}}}, healthy: 0, cfg: freeConfig()}
	provider := rotationTestProvider{endpoints: []string{"https://boards.example/boards/example"}}
	limiter := testLimiter()

	_, err := fetchProviderResponse(context.Background(), &http.Client{}, exhausted, limiter, provider, "example", nil)
	var stop *providerStopError
	if !errors.As(err, &stop) || !strings.Contains(err.Error(), "no healthy proxy route remains") {
		t.Fatalf("fetch error = %v, want a provider stop on proxy exhaustion", err)
	}
	if limiter.providerStop(provider.Name()) == nil {
		t.Fatal("provider-wide stop was not recorded on proxy exhaustion")
	}
}

func TestFetchProviderRetriesServerErrorOnAnotherRoute(t *testing.T) {
	firstRequests := 0
	alternateRequests := 0
	first := &proxyPeer{raw: "http://first:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		firstRequests++
		if firstRequests == 1 {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}}
	alternate := &proxyPeer{raw: "http://alternate:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		alternateRequests++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}}
	pool := &proxyPool{peers: []*proxyPeer{first, alternate}, healthy: 2, cfg: freeConfig()}
	provider := rotationTestProvider{endpoints: []string{"https://boards.example/boards/example"}}

	response, err := fetchProviderResponse(context.Background(), &http.Client{}, pool, testLimiter(), provider, "example", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !response.ok {
		t.Fatal("expected the retried request to succeed")
	}
	// A failed attempt rotates the retry to the other leased route.
	if firstRequests != 1 || alternateRequests != 1 {
		t.Fatalf("first proxy requests=%d alternate proxy requests=%d, want 1 and 1", firstRequests, alternateRequests)
	}
}

func TestFetchProviderRetriesTransportFailureOnAnotherRoute(t *testing.T) {
	firstRequests := 0
	alternateRequests := 0
	first := &proxyPeer{raw: "http://first:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		firstRequests++
		if firstRequests == 1 {
			return nil, errors.New("connection reset")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}}
	alternate := &proxyPeer{raw: "http://alternate:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		alternateRequests++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}}
	pool := &proxyPool{peers: []*proxyPeer{first, alternate}, healthy: 2, cfg: freeConfig()}
	provider := rotationTestProvider{endpoints: []string{"https://boards.example/boards/example"}}

	response, err := fetchProviderResponse(context.Background(), &http.Client{}, pool, testLimiter(), provider, "example", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !response.ok {
		t.Fatal("expected the retried request to succeed")
	}
	// A transport failure rotates the retry to the other leased route.
	if firstRequests != 1 || alternateRequests != 1 {
		t.Fatalf("first proxy requests=%d alternate proxy requests=%d, want 1 and 1", firstRequests, alternateRequests)
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

func TestGreenhouseStopsProviderOnUnauthorized(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var restrictedHits, alternateHits int
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				restrictedHits++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				alternateHits++
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			pool := &proxyPool{
				peers:   []*proxyPeer{limitedPeer, healthyPeer},
				healthy: 2,
				cfg:     freeConfig(),
			}
			limiter := testLimiter()
			found, err := (greenhouseProvider{}).CheckCompany(context.Background(), &http.Client{}, pool, limiter, "example", nil)
			if !errors.Is(err, errProviderStopped) || found {
				t.Fatalf("found=%t err=%v; want a provider stop", found, err)
			}
			if restrictedHits != 1 || alternateHits != 0 {
				t.Fatalf("restricted requests=%d alternate requests=%d, want 1 and 0", restrictedHits, alternateHits)
			}
			if limiter.providerStop(providerGreenhouse) == nil {
				t.Fatal("provider stop was not recorded")
			}
		})
	}
}

func TestGreenhouseFetchJobsStopsOnUnauthorized(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var restrictedHits, alternateHits int
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				restrictedHits++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				alternateHits++
				body := `{"jobs":[{"id":456,"title":"Backend Dev"}],"meta":{"total":1}}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Request:    req,
				}, nil
			})}}
			pool := &proxyPool{
				peers:   []*proxyPeer{limitedPeer, healthyPeer},
				healthy: 2,
				cfg:     freeConfig(),
			}
			limiter := testLimiter()
			jobs, err := (greenhouseProvider{}).FetchJobs(context.Background(), &http.Client{}, pool, limiter, "example")
			if !errors.Is(err, errProviderStopped) || len(jobs) != 0 {
				t.Fatalf("jobs=%+v err=%v; want a provider stop", jobs, err)
			}
			if restrictedHits != 1 || alternateHits != 0 {
				t.Fatalf("restricted requests=%d alternate requests=%d, want 1 and 0", restrictedHits, alternateHits)
			}
			if limiter.providerStop(providerGreenhouse) == nil {
				t.Fatal("provider stop was not recorded")
			}
		})
	}
}

func TestGreenhouseCheckRotatesOn403406(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var restrictedHits, alternateHits int
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				restrictedHits++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				alternateHits++
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			pool := &proxyPool{
				peers:   []*proxyPeer{limitedPeer, healthyPeer},
				healthy: 2,
				cfg:     freeConfig(),
			}
			limiter := testLimiter()
			found, err := (greenhouseProvider{}).CheckCompany(context.Background(), &http.Client{}, pool, limiter, "example", nil)
			if err != nil || !found {
				t.Fatalf("found=%t err=%v; want the check to rotate onto the healthy route", found, err)
			}
			if restrictedHits != 1 || alternateHits != 1 {
				t.Fatalf("restricted requests=%d alternate requests=%d, want 1 and 1", restrictedHits, alternateHits)
			}
			if limiter.providerStop(providerGreenhouse) != nil {
				t.Fatal("provider-wide stop was recorded for a Greenhouse rate limit")
			}
			if limitedPeer.failures != 1 || time.Until(limitedPeer.cooldownUntil) <= 0 {
				t.Fatalf("limited peer failures=%d cooldown=%s, want 1 failure and a cooling route",
					limitedPeer.failures, time.Until(limitedPeer.cooldownUntil))
			}
		})
	}
}

func TestGreenhouseFetchJobsRotatesOn403406(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var restrictedHits, alternateHits int
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				restrictedHits++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				alternateHits++
				body := `{"jobs":[{"id":456,"title":"Backend Dev"}],"meta":{"total":1}}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Request:    req,
				}, nil
			})}}
			pool := &proxyPool{
				peers:   []*proxyPeer{limitedPeer, healthyPeer},
				healthy: 2,
				cfg:     freeConfig(),
			}
			limiter := testLimiter()
			jobs, err := (greenhouseProvider{}).FetchJobs(context.Background(), &http.Client{}, pool, limiter, "example")
			if err != nil || len(jobs) == 0 {
				t.Fatalf("jobs=%+v err=%v; want the fetch to rotate onto the healthy route", jobs, err)
			}
			if restrictedHits != 1 || alternateHits != 1 {
				t.Fatalf("restricted requests=%d alternate requests=%d, want 1 and 1", restrictedHits, alternateHits)
			}
			if limiter.providerStop(providerGreenhouse) != nil {
				t.Fatal("provider-wide stop was recorded for a Greenhouse rate limit")
			}
		})
	}
}

