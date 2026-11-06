package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

	pool, err := loadProxyPool(context.Background(), path, freeConfig(), time.Second, 5*time.Second, "")
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

	// Consecutive failures still reset on a success: two failures after
	// one success evict, while failure,failure,success continues.
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
	if _, direct, err := evictor.acquire(context.Background()); err != nil || !direct {
		t.Fatalf("expected direct fallback after eviction, direct=%t err=%v", direct, err)
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
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	// The transparent record is dropped by the elite filter and the same
	// host serves two peers (one per advertised protocol).
	if peerPool.healthyPeers() != 2 {
		t.Fatalf("healthy peers = %d, want 2", peerPool.healthyPeers())
	}
	if peerPool.peers[0].raw != "socks5://"+serverHost {
		t.Fatalf("first peer = %s, want socks5://%s", peerPool.peers[0].raw, serverHost)
	}
	if peerPool.peers[1].raw != "http://"+serverHost {
		t.Fatalf("second peer = %s, want http://%s", peerPool.peers[1].raw, serverHost)
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

func TestLoadProxyPoolFailsWithoutAliveProxy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxies.json")
	content := `{"proxy":"http://127.0.0.1:1937","protocol":"http","alive":true,"anonymity":"elite","uptime":9}`
	if err := os.WriteFile(path, []byte("["+content+"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProxyPool(context.Background(), path, freeConfig(), 300*time.Millisecond, 5*time.Second, ""); err == nil {
		t.Fatal("expected an error when no proxy passes the health probe")
	}
}

func probeResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusNotFound,
		Status:     "404 Not Found",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
	}
}

func TestCurateProxiesKeepsAnsweringCandidates(t *testing.T) {
	good := &collectCandidate{
		entry:  proxyListEntry{Proxy: "http://good:80", Protocol: "http", Alive: true, Anonymity: "elite"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return probeResponse(), nil })},
	}
	calls := 0
	flaky := &collectCandidate{
		entry: proxyListEntry{Proxy: "http://flaky:80", Protocol: "http", Alive: true, Anonymity: "elite"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("connection reset")
			}
			return probeResponse(), nil
		})},
	}
	dead := &collectCandidate{
		entry:  proxyListEntry{Proxy: "http://dead:80", Protocol: "http", Alive: true, Anonymity: "elite"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("tunnel refused") })},
	}

	saved, err := curateProxies(context.Background(), []*collectCandidate{good, flaky, dead}, 2, time.Second, "https://boards.example/probe")
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) != 2 {
		t.Fatalf("curated %d entries, want 2", len(saved))
	}
	if saved[0].Proxy != "http://good:80" || saved[0].Successes != 2 || saved[0].Requests != 2 || saved[0].Failures != 0 {
		t.Fatalf("first entry: %+v", saved[0])
	}
	if saved[1].Proxy != "http://flaky:80" || saved[1].Successes != 1 || saved[1].Failures != 1 || saved[1].Requests != 2 {
		t.Fatalf("second entry: %+v", saved[1])
	}
	if saved[0].ResponseMs < 0 {
		t.Fatalf("response_ms must not be negative: %+v", saved[0])
	}
	if saved[0].LastUsed == "" {
		t.Fatal("last_used missing")
	}

	none := []*collectCandidate{{
		entry:  proxyListEntry{Proxy: "http://dead:80", Protocol: "http", Alive: true},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("dead") })},
	}}
	if _, err := curateProxies(context.Background(), none, 2, time.Second, "https://boards.example/probe"); err == nil {
		t.Fatal("expected an error when no candidate ever answers")
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
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if len(decoded) != 2 {
		t.Fatalf("curated %d entries, want 2 (unreachable candidate dropped)", len(decoded))
	}
	var saved []proxySaveEntry
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	// Equal successes hand ordering to the latency tie-break, which is
	// intentionally nondeterministic; accept either order.
	if !((saved[0].Proxy == "http://"+targetHost && saved[1].Proxy == "socks5://"+targetHost) ||
		(saved[0].Proxy == "socks5://"+targetHost && saved[1].Proxy == "http://"+targetHost)) {
		t.Fatalf("saved proxies: %s and %s, want the two target entries", saved[0].Proxy, saved[1].Proxy)
	}
	for _, entry := range saved {
		if entry.Successes != 2 {
			t.Fatalf("probe rounds not counted: %+v", entry)
		}
	}
}

func TestProbePeerTunnelCheckDropsDeadTunnels(t *testing.T) {
	alivePeer := &proxyPeer{raw: "http://alive:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Status:     "404 Not Found",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}}
	deadTunnelPeer := &proxyPeer{raw: "http://dead:80", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("proxy: CONNECT tunnel to target refused")
	})}}

	dialer := &net.Dialer{Timeout: 300 * time.Millisecond}
	if !probePeer(context.Background(), dialer, alivePeer, time.Second, "https://boards.example/boards/probe") {
		t.Fatal("tunnel-capable peer was dropped")
	}
	if probePeer(context.Background(), dialer, deadTunnelPeer, time.Second, "https://boards.example/boards/probe") {
		t.Fatal("transport-error proxy passed the tunnel probe")
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

func TestGreenhouseRotatesPeerOn403And406(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var mu sync.Mutex
			hits := map[string]int{}
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				hits["limited"]++
				mu.Unlock()
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
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
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			pool := &proxyPool{
				peers:   []*proxyPeer{limitedPeer, healthyPeer},
				healthy: 2,
				cfg:     freeConfig(),
			}
			found, err := (greenhouseProvider{}).CheckCompany(
				context.Background(), &http.Client{}, pool, testLimiter(), "example", nil)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("expected company to be found after rotation")
			}
			mu.Lock()
			defer mu.Unlock()
			if hits["limited"] != 1 || hits["healthy"] != 1 {
				t.Fatalf("hit counts = %v, want 1 limited and 1 healthy", hits)
			}
			if limitedPeer.failures != 1 {
				t.Fatalf("rate limited peer failures = %d, want 1", limitedPeer.failures)
			}
			if time.Until(limitedPeer.cooldownUntil) <= 0 {
				t.Fatal("Retry-After did not set cooldown on limited peer")
			}
		})
	}
}

func TestGreenhouseFetchJobsRotatesPeerOn403And406(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var mu sync.Mutex
			hits := map[string]int{}
			limitedPeer := &proxyPeer{raw: "http://limited:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				hits["limited"]++
				mu.Unlock()
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			})}}
			healthyPeer := &proxyPeer{raw: "http://healthy:80", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				hits["healthy"]++
				mu.Unlock()
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
			jobs, err := (greenhouseProvider{}).FetchJobs(
				context.Background(), &http.Client{}, pool, testLimiter(), "example")
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 || jobs[0].ID != 456 {
				t.Fatalf("unexpected jobs after rotation: %+v", jobs)
			}
			mu.Lock()
			defer mu.Unlock()
			if hits["limited"] != 1 || hits["healthy"] != 1 {
				t.Fatalf("hit counts = %v, want 1 limited and 1 healthy", hits)
			}
			if limitedPeer.failures != 1 {
				t.Fatalf("rate limited peer failures = %d, want 1", limitedPeer.failures)
			}
			if time.Until(limitedPeer.cooldownUntil) <= 0 {
				t.Fatal("Retry-After did not set cooldown on limited peer")
			}
		})
	}
}
