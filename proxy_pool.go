package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// proxyInUsePollInterval is how often acquire rechecks the pool when a
// healthy peer is currently leased by another worker.
const proxyInUsePollInterval = 5 * time.Millisecond

// proxyListMaxBytes caps a downloaded proxy list to a sane size.
const proxyListMaxBytes = 16 << 20

// geonodeProxyEntry mirrors the proxylist.geonode.com API record. The
// port is a string there, but json.Number accepts quoted and unquoted
// numbers alike; responseTime is milliseconds.
type geonodeProxyEntry struct {
	IP        string      `json:"ip"`
	Port      json.Number `json:"port"`
	Protocols []string    `json:"protocols"`
	Anonymity string      `json:"anonymityLevel"`
	Latency   float64     `json:"responseTime"`
	UpTime    float64     `json:"upTime"`
}

// toEntries maps one Geonode record onto the shared entry schema, one
// entry per supported protocol. Uptime carries the negative response
// time so the loader's descending sort prefers source-claimed records;
// this metadata does not verify transport health.
func (entry geonodeProxyEntry) toEntries() []proxyListEntry {
	if entry.IP == "" || entry.Port == "" {
		return nil
	}
	mapped := make([]proxyListEntry, 0, len(entry.Protocols))
	for _, protocol := range entry.Protocols {
		if protocol != "http" && protocol != "socks5" {
			continue
		}
		mapped = append(mapped, proxyListEntry{
			Proxy:     protocol + "://" + entry.IP + ":" + entry.Port.String(),
			Protocol:  protocol,
			Alive:     true,
			Anonymity: entry.Anonymity,
			Uptime:    -entry.Latency,
		})
	}
	if len(mapped) == 0 {
		return nil
	}
	return mapped
}

// decodeProxyList accepts either a bare JSON array of proxy entries or a
// JSON object carrying one (for example a "proxies" field next to list
// metadata, or the Geonode API's "data" field); the first object field
// decoding to entries with proxy URLs wins.
func decodeProxyList(data []byte) ([]proxyListEntry, error) {
	var entries []proxyListEntry
	if err := json.Unmarshal(data, &entries); err == nil {
		if len(entries) == 0 {
			return entries, nil
		}
		for _, entry := range entries {
			if entry.Proxy != "" {
				return entries, nil
			}
		}
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("expected a proxy list array (or an object wrapping one): %w", err)
	}
	// Prefer the "proxies" key (local list files), then "data" (Geonode),
	// then any other field with entries. Each candidate tries our own
	// entry schema first, then the Geonode record shape.
	readEntries := func(raw json.RawMessage) ([]proxyListEntry, bool) {
		var candidate []proxyListEntry
		if err := json.Unmarshal(raw, &candidate); err == nil {
			for _, entry := range candidate {
				if entry.Proxy != "" {
					return candidate, true
				}
			}
		}
		var geonode []geonodeProxyEntry
		if err := json.Unmarshal(raw, &geonode); err == nil {
			var mapped []proxyListEntry
			for _, record := range geonode {
				mapped = append(mapped, record.toEntries()...)
			}
			for _, entry := range mapped {
				if entry.Proxy != "" {
					return mapped, true
				}
			}
		}
		return nil, false
	}
	if raw, ok := object["proxies"]; ok {
		if candidate, ok := readEntries(raw); ok {
			return candidate, nil
		}
	}
	if raw, ok := object["data"]; ok {
		if candidate, ok := readEntries(raw); ok {
			return candidate, nil
		}
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "proxies" || key == "data" {
			continue
		}
		if candidate, ok := readEntries(object[key]); ok {
			return candidate, nil
		}
	}
	return nil, errors.New("no field contained an array of proxy entries")
}

type proxyListEntry struct {
	Proxy     string  `json:"proxy"`
	Protocol  string  `json:"protocol"`
	Alive     bool    `json:"alive"`
	Anonymity string  `json:"anonymity"`
	Uptime    float64 `json:"uptime"`
}

type proxyPeer struct {
	raw    string // proxy URL as written in the list
	client *http.Client

	inUse         bool
	evicted       bool
	failures      int // consecutive failures, reset on success
	successes     int // observed successful requests since pool load
	failuresTotal int // observed transport/5xx/429 failures since pool load
	lastUsed      time.Time
	cooldownUntil time.Time
}

type proxyPoolConfig struct {
	// minReuseGap is the minimum gap after release before a peer may be
	// leased again, independent of cooldowns.
	minReuseGap time.Duration
	// maxFailures evicts a peer once consecutive failures reach this many.
	// Values below 1 disable eviction.
	maxFailures int
	// retryAfterFloor floors the cooldown set from a Retry-After header.
	retryAfterFloor time.Duration
	// maxCooldown caps any cooldown a peer can be given. Zero disables the cap.
	maxCooldown time.Duration
}

type proxyPool struct {
	mu      sync.Mutex
	saveMu  sync.Mutex // serializes save() so flushes never race
	peers   []*proxyPeer
	pinned  *proxyPeer
	healthy int
	cfg     proxyPoolConfig
}

type proxyOutcome int

const (
	proxyOutcomeOK proxyOutcome = iota
	proxyOutcomeRateLimited
	proxyOutcomeFailed
)

// isProxyListURL reports whether the source looks like a remote proxy
// list (an http or https URL) rather than a local file path.
func isProxyListURL(source string) bool {
	return strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://")
}

// loadProxyListFile reads and decodes a proxy list from disk.
func loadProxyListFile(path string) ([]proxyListEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read proxy list: %w", err)
	}
	return decodeProxyList(data)
}

// fetchProxyListEntries downloads a proxy list from an http(s) URL and
// decodes it with the same rules as a local file. Some list hosts reject
// default Go user agents, so the request presents a browser one.
func fetchProxyListEntries(ctx context.Context, source string, timeout time.Duration) ([]proxyListEntry, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, fmt.Errorf("build proxy list request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36")
	res, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch proxy list: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch proxy list: status %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, proxyListMaxBytes))
	if err != nil {
		return nil, fmt.Errorf("read proxy list: %w", err)
	}
	entries, err := decodeProxyList(data)
	if err != nil {
		return nil, fmt.Errorf("decode proxy list: %w", err)
	}
	return entries, nil
}

// eligibleProxyEntries filters the entries worth validating: alive, http
// or socks5, without a downgraded anonymity, deduplicated by proxy URL —
// or by host when uniqueHosts collapses multi-port providers to their
// best entry — and returns them ordered by uptime (best candidates
// first). Entries whose URL does not parse are dropped.
func eligibleProxyEntries(entries []proxyListEntry, uniqueHosts bool) []proxyListEntry {
	kept := make([]proxyListEntry, 0, len(entries))
	seen := make(map[string]struct{})
	best := make(map[string]int) // hostname -> index into kept
	for _, entry := range entries {
		if !entry.Alive {
			continue
		}
		if entry.Protocol != "http" && entry.Protocol != "socks5" {
			continue
		}
		if entry.Anonymity != "" && entry.Anonymity != "elite" {
			continue
		}
		if _, ok := seen[entry.Proxy]; ok {
			continue
		}
		seen[entry.Proxy] = struct{}{}
		proxyURL, err := url.Parse(entry.Proxy)
		if err != nil {
			continue
		}
		if uniqueHosts {
			host := proxyURL.Hostname()
			if index, ok := best[host]; ok {
				if entry.Uptime > kept[index].Uptime {
					kept[index] = entry
				}
				continue
			}
			best[host] = len(kept)
		}
		kept = append(kept, entry)
	}
	sort.SliceStable(kept, func(i, j int) bool {
		return kept[i].Uptime > kept[j].Uptime
	})
	return kept
}

// loadProxyPool reads a proxy list and pins the highest-ranked eligible
// entry for the run. Route eligibility is determined from the supplied
// list only; target endpoints are never contacted to probe proxy candidates.
func loadProxyPool(
	ctx context.Context,
	source string,
	cfg proxyPoolConfig,
	probeTimeout time.Duration,
	requestTimeout time.Duration,
	probeURL string,
) (*proxyPool, error) {
	_ = probeTimeout
	_ = probeURL
	var entries []proxyListEntry
	var err error
	if isProxyListURL(source) {
		entries, err = fetchProxyListEntries(ctx, source, requestTimeout)
	} else {
		entries, err = loadProxyListFile(source)
	}
	if err != nil {
		return nil, err
	}

	eligible := eligibleProxyEntries(entries, false)
	if len(eligible) == 0 {
		return nil, fmt.Errorf("no eligible proxy entries in %s", source)
	}

	chosen := eligible[0]
	proxyURL, err := url.Parse(chosen.Proxy)
	if err != nil {
		return nil, fmt.Errorf("parse selected proxy: %w", err)
	}
	// A selected manual route stays fixed. Pool exhaustion is fail-closed;
	// never change egress mid-run.
	peer := &proxyPeer{raw: chosen.Proxy, client: newPeerClient(proxyURL, requestTimeout)}
	log.Printf("selected one of %d eligible proxies from %s; route is pinned for this run", len(eligible), source)
	return &proxyPool{peers: []*proxyPeer{peer}, pinned: peer, healthy: 1, cfg: cfg}, nil
}

// newPeerClient builds an HTTP client pinned to a single proxy URL. The
// transport settings mirror the direct client in main.newClient so pooled
// and direct requests behave the same. The socks5 scheme is supported by
// net/http without extra dependencies.
func newPeerClient(proxyURL *url.URL, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyURL(proxyURL),
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 3 * time.Second,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			if len(via) > 0 && req.URL.Host != via[0].URL.Host {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// acquire leases the one pinned peer exclusively. It blocks until that
// route is available or the context ends. It never rotates to another peer
// or falls back to direct egress.
func (p *proxyPool) acquire(ctx context.Context) (*proxyPeer, bool, error) {
	for {
		p.mu.Lock()
		if p.pinned == nil {
			for _, peer := range p.peers {
				if !peer.evicted {
					p.pinned = peer
					break
				}
			}
		}
		peer := p.pinned
		if peer == nil || peer.evicted {
			p.mu.Unlock()
			return nil, false, errors.New("pinned proxy route is unavailable; refusing to change egress")
		}
		now := time.Now()
		if !peer.inUse {
			ready := peer.lastUsed.Add(p.cfg.minReuseGap)
			if peer.cooldownUntil.After(ready) {
				ready = peer.cooldownUntil
			}
			wait := ready.Sub(now)
			if wait <= 0 {
				peer.inUse = true
				p.mu.Unlock()
				return peer, false, nil
			}
			p.mu.Unlock()
			if err := waitContext(ctx, wait); err != nil {
				return nil, false, err
			}
			continue
		}
		p.mu.Unlock()

		if err := waitContext(ctx, proxyInUsePollInterval); err != nil {
			return nil, false, err
		}
	}
}

func waitContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release reports the outcome of the request made on a lease.
//
// OK resets consecutive failures; RateLimited sets a cooldown derived from
// the Retry-After duration (floored and capped) and counts a failure;
// Failed counts a failure. A peer reaching maxFailures consecutive
// failures is evicted and can no longer be leased.
func (p *proxyPool) release(peer *proxyPeer, outcome proxyOutcome, retryAfter time.Duration) {
	if p == nil || peer == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	peer.inUse = false
	peer.lastUsed = now
	switch outcome {
	case proxyOutcomeOK:
		peer.failures = 0
		peer.successes++
	case proxyOutcomeRateLimited:
		peer.failures++
		peer.failuresTotal++
		wait := retryAfter
		if wait < p.cfg.retryAfterFloor {
			wait = p.cfg.retryAfterFloor
		}
		if p.cfg.maxCooldown > 0 && wait > p.cfg.maxCooldown {
			wait = p.cfg.maxCooldown
		}
		if wait > 0 {
			if until := now.Add(wait); until.After(peer.cooldownUntil) {
				peer.cooldownUntil = until
			}
		}
	case proxyOutcomeFailed:
		peer.failures++
		peer.failuresTotal++
	}
	if outcome != proxyOutcomeRateLimited && !peer.evicted && p.cfg.maxFailures > 0 && peer.failures >= p.cfg.maxFailures {
		peer.evicted = true
		p.healthy--
		log.Printf("proxy %s evicted after %d consecutive failures (%d healthy left)",
			peer.raw, peer.failures, p.healthy)
		peer.client.CloseIdleConnections()
	}
}

// healthyPeers returns how many peers can still be leased.
func (p *proxyPool) healthyPeers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.healthy
}

type proxySaveEntry struct {
	Proxy      string  `json:"proxy"`
	Protocol   string  `json:"protocol"`
	Anonymity  string  `json:"anonymity"`
	Alive      bool    `json:"alive"`
	Uptime     float64 `json:"uptime"`
	Requests   int     `json:"requests"`
	Successes  int     `json:"successes"`
	Failures   int     `json:"failures"`
	ResponseMs float64 `json:"response_ms,omitempty"`
	LastUsed   string  `json:"last_used,omitempty"`
}

// snapshot returns the surviving (never evicted) peers as save entries
// under the pool lock, ordered by proven successes descending so the
// next run starts with the best performers.
func (p *proxyPool) snapshot() []proxySaveEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := make([]proxySaveEntry, 0, len(p.peers))
	for _, peer := range p.peers {
		if peer.evicted {
			continue
		}
		proxyURL, err := url.Parse(peer.raw)
		protocol := "http"
		if err == nil && proxyURL.Scheme != "" {
			protocol = proxyURL.Scheme
		}
		entries = append(entries, proxySaveEntry{
			Proxy:     peer.raw,
			Protocol:  protocol,
			Anonymity: "elite",
			Alive:     true,
			// Reuse the source schema: the loader sorts by uptime, so
			// observed successes rank proven peers first.
			Uptime:    float64(peer.successes),
			Requests:  peer.successes + peer.failuresTotal,
			Successes: peer.successes,
			Failures:  peer.failuresTotal,
			LastUsed:  peer.lastUsed.Round(time.Second).UTC().Format(time.RFC3339),
		})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Successes != entries[j].Successes {
			return entries[i].Successes > entries[j].Successes
		}
		return entries[i].Proxy < entries[j].Proxy
	})
	return entries
}

// save writes surviving peers to path in the same JSON shape loadProxyPool
// accepts, sorted by observed successes, through a temp-file rename so a
// crash mid-write cannot corrupt the previous list. Calls are serialized
// so a periodic flush and the final save never race. Returns the number
// of peers written.
func (p *proxyPool) save(path string) (int, error) {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()
	entries := p.snapshot()
	if err := writeProxyList(path, entries); err != nil {
		return 0, err
	}
	return len(entries), nil
}

// writeProxyList serializes the entries as a loadable proxy list file,
// through a temp-file rename so a crash mid-write never corrupts the
// previous list.
func writeProxyList(path string, entries []proxySaveEntry) error {
	data, err := json.MarshalIndent(entries, "", " ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temp := path + ".tmp"
	if err := os.WriteFile(temp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		os.Remove(temp)
		return err
	}
	return nil
}

// proxySaveInterval is how often --proxy-save rewrites the list during a
// long crawl; the deferred final save still runs when the crawl ends.
const proxySaveInterval = 5 * time.Minute

// saveProxyList writes surviving peers with their request stats to path.
// Safe with a nil pool or empty path.
func saveProxyList(pool *proxyPool, path string) {
	if pool == nil || path == "" {
		return
	}
	saved, err := pool.save(path)
	if err != nil {
		log.Printf("save proxy list %s: %v", path, err)
		return
	}
	log.Printf("saved %d proxies with request stats to %s", saved, path)
}

// flushProxyList rewrites the save file every proxySaveInterval until the
// context ends, so an interrupted crawl keeps the stats collected so far.
func flushProxyList(ctx context.Context, pool *proxyPool, path string) {
	ticker := time.NewTicker(proxySaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			saveProxyList(pool, path)
		}
	}
}

type collectCandidate struct {
	entry proxyListEntry
}

// collectProxies reads and normalizes user-supplied proxy lists without
// probing candidate routes against a destination provider.
func collectProxies(
	ctx context.Context,
	output string,
	sources []string,
	iterations int,
	uniqueHosts bool,
	probeTimeout time.Duration,
	requestTimeout time.Duration,
	probeURL string,
) error {
	if probeTimeout <= 0 {
		return errors.New("proxy probe timeout must be greater than 0")
	}
	_ = iterations
	_ = probeURL
	entries := make([]proxyListEntry, 0)
	for _, source := range sources {
		var sourceEntries []proxyListEntry
		var err error
		if isProxyListURL(source) {
			sourceEntries, err = fetchProxyListEntries(ctx, source, requestTimeout)
		} else {
			sourceEntries, err = loadProxyListFile(source)
		}
		if err != nil {
			if len(sources) > 1 {
				log.Printf("skipping proxy source %s: %v", source, err)
				continue
			}
			return err
		}
		log.Printf("loaded %d proxy candidates from %s", len(sourceEntries), source)
		entries = append(entries, sourceEntries...)
	}
	if len(entries) == 0 {
		return errors.New("no proxy candidates found in the given sources")
	}

	candidates := make([]*collectCandidate, 0, len(entries))
	for _, entry := range eligibleProxyEntries(entries, uniqueHosts) {
		candidates = append(candidates, &collectCandidate{
			entry: entry,
		})
	}
	if len(candidates) == 0 {
		return errors.New("no eligible proxy candidates after filtering the given sources")
	}

	curated, err := curateProxies(ctx, candidates)
	if err != nil {
		return err
	}
	if err := writeProxyList(output, curated); err != nil {
		return fmt.Errorf("write proxy list %s: %w", output, err)
	}
	log.Printf("saved %d eligible proxy candidates (not transport-verified) -> %s",
		len(curated), output)
	return nil
}

// curateProxies serializes eligible source entries without sending target
// requests. Source health fields remain claims, not transport verification.
func curateProxies(ctx context.Context, candidates []*collectCandidate) ([]proxySaveEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, errors.New("no eligible proxy candidates")
	}
	sorted := append([]*collectCandidate(nil), candidates...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].entry.Proxy < sorted[j].entry.Proxy
	})

	saved := make([]proxySaveEntry, 0, len(sorted))
	for _, candidate := range sorted {
		saved = append(saved, proxySaveEntry{
			Proxy:     candidate.entry.Proxy,
			Protocol:  candidate.entry.Protocol,
			Anonymity: candidate.entry.Anonymity,
			Alive:     candidate.entry.Alive,
			Uptime:    candidate.entry.Uptime,
		})
	}
	return saved, nil
}

// progressNote summarizes pool health for the brute-force progress line,
// counting only peers that can still be leased. Empty without a pool.
func (p *proxyPool) progressNote() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	evicted := 0
	for _, peer := range p.peers {
		if peer.evicted {
			evicted++
		}
	}
	return fmt.Sprintf(", %d/%d proxy peers healthy", p.healthy, len(p.peers)-evicted)
}

// requestLease ties one HTTP request to the peer that serves it. Proxies
// off (nil pool) yields a lease using the configured direct client.
type requestLease struct {
	pool   *proxyPool
	peer   *proxyPeer
	client *http.Client
}

func acquireLease(ctx context.Context, pool *proxyPool, client *http.Client) (requestLease, error) {
	if pool == nil {
		return requestLease{client: client}, nil
	}
	peer, direct, err := pool.acquire(ctx)
	if err != nil {
		return requestLease{}, err
	}
	if direct {
		return requestLease{client: client}, nil
	}
	return requestLease{pool: pool, peer: peer, client: peer.client}, nil
}

// release reports the request outcome. Safe on a direct lease.
func (l requestLease) release(outcome proxyOutcome, retryAfter time.Duration) {
	l.pool.release(l.peer, outcome, retryAfter)
}

// parseRetryAfter reads a Retry-After header value: delay seconds, an HTTP
// date, or anything invalid returning 0. Past dates return 0.
func parseRetryAfter(header string, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if wait := when.Sub(now); wait > 0 {
			return wait
		}
	}
	return 0
}
