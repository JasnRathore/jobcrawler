package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// proxyProbeConcurrency bounds simultaneous probe dials at startup.
const proxyProbeConcurrency = 64

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
// time so the loader's descending sort puts the fastest proxies first;
// every record is marked alive and left for the health probe to judge.
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
	healthy int
	cfg     proxyPoolConfig

	lastFallbackLog time.Time
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

// loadProxyPool reads a proxy list file, keeps alive elite http/socks5
// entries ordered by uptime (best first), validates each peer, and keeps
// only peers that answered within probeTimeout. With a non-empty probeURL
// each peer receives one real GET through its own proxy client and any
// HTTP status proves the tunnel reaches the target; with an empty probeURL
// the probe falls back to a plain TCP dial, which only proves address
// reachability.
func loadProxyPool(
	ctx context.Context,
	source string,
	cfg proxyPoolConfig,
	probeTimeout time.Duration,
	requestTimeout time.Duration,
	probeURL string,
) (*proxyPool, error) {
	if probeTimeout <= 0 {
		return nil, errors.New("proxy probe timeout must be greater than 0")
	}
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

	ordered := make([]*proxyPeer, 0, len(entries))
	for _, entry := range eligibleProxyEntries(entries, false) {
		proxyURL, err := url.Parse(entry.Proxy)
		if err != nil {
			continue
		}
		ordered = append(ordered, &proxyPeer{
			raw:    entry.Proxy,
			client: newPeerClient(proxyURL, requestTimeout),
		})
	}

	probed := probeProxyPeers(ctx, ordered, probeTimeout, probeURL)
	if len(probed) == 0 {
		return nil, fmt.Errorf("no proxy passed the health probe from %s", source)
	}
	log.Printf("loaded %d proxies from %s (%d of %d entries passed the health probe)",
		len(probed), source, len(probed), len(entries))

	return &proxyPool{peers: probed, healthy: len(probed), cfg: cfg}, nil
}

// probeProxyPeers validates every peer concurrently and returns only the
// peers that answered, preserving the original order.
func probeProxyPeers(ctx context.Context, peers []*proxyPeer, timeout time.Duration, probeURL string) []*proxyPeer {
	dialer := &net.Dialer{Timeout: timeout}
	jobs := make(chan *proxyPeer)
	survivors := make([]bool, len(peers))
	var wg sync.WaitGroup
	for i := 0; i < proxyProbeConcurrency && i < len(peers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for peer := range jobs {
				if probePeer(ctx, dialer, peer, timeout, probeURL) {
					survivors[indexOfPeer(peers, peer)] = true
				}
			}
		}()
	}
	for _, peer := range peers {
		jobs <- peer
	}
	close(jobs)
	wg.Wait()

	kept := make([]*proxyPeer, 0, len(peers))
	for i, peer := range peers {
		if survivors[i] {
			kept = append(kept, peer)
		}
	}
	return kept
}

// probePeer validates one peer. A non-empty probeURL sends a real GET
// through the proxy and any HTTP status (4xx and 5xx included) proves the
// CONNECT tunnel and TLS handshake work; a transport failure drops the
// peer. An empty probeURL only dials the proxy address over TCP.
func probePeer(ctx context.Context, dialer *net.Dialer, peer *proxyPeer, timeout time.Duration, probeURL string) bool {
	if probeURL == "" {
		return dialProxyPeer(ctx, dialer, peer.raw) == nil
	}
	probeClient := &http.Client{Transport: peer.client.Transport, Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return false
	}
	res, err := probeClient.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, io.LimitReader(res.Body, 512))
	res.Body.Close()
	return true
}

func indexOfPeer(peers []*proxyPeer, peer *proxyPeer) int {
	for i, candidate := range peers {
		if candidate == peer {
			return i
		}
	}
	return -1
}

func dialProxyPeer(ctx context.Context, dialer *net.Dialer, raw string) error {
	proxyURL, err := url.Parse(raw)
	if err != nil {
		return err
	}
	addr := proxyURL.Host
	if proxyURL.Port() == "" {
		if proxyURL.Scheme == "https" {
			addr += ":443"
		} else {
			addr += ":80"
		}
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

// newPeerClient builds an HTTP client pinned to a single proxy URL. The
// transport settings mirror the direct client in main.newClient so pooled
// and direct requests behave the same. The socks5 scheme is supported by
// net/http without extra dependencies.
func newPeerClient(proxyURL *url.URL, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyURL(proxyURL),
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 3 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

// acquire leases a peer exclusively. It blocks until a peer is eligible
// (idle, past its reuse gap and cooldown) or the context ends. If no
// healthy peer remains it reports directFallback so callers can send from
// the local address, and logs the switch.
func (p *proxyPool) acquire(ctx context.Context) (*proxyPeer, bool, error) {
	for {
		p.mu.Lock()
		if p.healthy == 0 {
			p.mu.Unlock()
			p.warnDirectFallback()
			return nil, true, nil
		}
		now := time.Now()
		var best *proxyPeer
		next := time.Duration(0)
		nextSet := false
		anyLeased := false
		for _, peer := range p.peers {
			if peer.evicted {
				continue
			}
			if peer.inUse {
				anyLeased = true
				continue
			}
			ready := peer.lastUsed.Add(p.cfg.minReuseGap)
			if peer.cooldownUntil.After(ready) {
				ready = peer.cooldownUntil
			}
			if wait := ready.Sub(now); wait > 0 {
				if !nextSet || wait < next {
					next, nextSet = wait, true
				}
				continue
			}
			if best == nil || peer.lastUsed.Before(best.lastUsed) {
				best = peer
			}
		}
		if best != nil {
			best.inUse = true
			p.mu.Unlock()
			return best, false, nil
		}
		wait := next
		if anyLeased {
			wait = proxyInUsePollInterval
		}
		p.mu.Unlock()

		if wait <= 0 {
			wait = proxyInUsePollInterval
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, false, ctx.Err()
		}
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
	if !peer.evicted && p.cfg.maxFailures > 0 && peer.failures >= p.cfg.maxFailures {
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

// collectCandidate is one proxy under consideration in collect mode with
// the client used for probe rounds and the stats observed so far.
type collectCandidate struct {
	entry     proxyListEntry
	client    *http.Client
	successes int
	failures  int
	latencyMS float64
	latencyN  int
}

// probe once through the candidate. A non-empty probeURL sends a GET
// through the proxy — any HTTP status proves the tunnel — otherwise a
// TCP dial is timed instead. Returns success and the elapsed duration.
func (c *collectCandidate) probe(ctx context.Context, probeURL string, timeout time.Duration) (bool, time.Duration) {
	if probeURL == "" {
		start := time.Now()
		err := dialProxyPeer(ctx, &net.Dialer{Timeout: timeout}, c.entry.Proxy)
		return err == nil, time.Since(start)
	}
	client := &http.Client{Transport: c.client.Transport, Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return false, 0
	}
	start := time.Now()
	res, err := client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return false, elapsed
	}
	io.Copy(io.Discard, io.LimitReader(res.Body, 512))
	res.Body.Close()
	return true, elapsed
}

// collectProxies curation mode: pull candidates from every source (file
// or http(s) URL), probe each one `iterations` times through probeURL
// with the shared probe concurrency, drop everything that never
// answered, and write the curated list to output. With uniqueHosts only
// the best entry per host survives, which saves probes when one provider
// exposes the same egress IP on many ports.
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
		proxyURL, err := url.Parse(entry.Proxy)
		if err != nil {
			continue
		}
		candidates = append(candidates, &collectCandidate{
			entry:  entry,
			client: newPeerClient(proxyURL, probeTimeout),
		})
	}
	if len(candidates) == 0 {
		return errors.New("no eligible proxy candidates after filtering the given sources")
	}

	curated, err := curateProxies(ctx, candidates, iterations, probeTimeout, probeURL)
	if err != nil {
		return err
	}
	if err := writeProxyList(output, curated); err != nil {
		return fmt.Errorf("write proxy list %s: %w", output, err)
	}
	log.Printf("collected %d proxies from %d candidate(s) after %d probe round(s) -> %s",
		len(curated), len(candidates), iterations, output)
	return nil
}

// curateProxies runs the probe rounds over the candidates, keeps anything
// that answered at least once, and returns save entries ordered by
// successes descending, then measured response time, then proxy URL.
// Candidates are mutated, so callers must not share them concurrently.
func curateProxies(ctx context.Context, candidates []*collectCandidate, iterations int, probeTimeout time.Duration, probeURL string) ([]proxySaveEntry, error) {
	for round := 1; round <= iterations; round++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		collectSweep(ctx, candidates, probeTimeout, probeURL)
		answered := 0
		for _, candidate := range candidates {
			if candidate.successes > 0 {
				answered++
			}
		}
		log.Printf("probe round %d/%d done: %d/%d answered so far", round, iterations, answered, len(candidates))
	}

	survivors := make([]*collectCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.successes > 0 {
			survivors = append(survivors, candidate)
		}
	}
	if len(survivors) == 0 {
		return nil, fmt.Errorf("no proxy answered any of %d probe round(s)", iterations)
	}
	sort.SliceStable(survivors, func(i, j int) bool {
		a, b := survivors[i], survivors[j]
		if a.successes != b.successes {
			return a.successes > b.successes
		}
		if a.averageLatency() != b.averageLatency() {
			return a.averageLatency() < b.averageLatency()
		}
		return a.entry.Proxy < b.entry.Proxy
	})

	saved := make([]proxySaveEntry, 0, len(survivors))
	now := time.Now().UTC().Format(time.RFC3339)
	for _, candidate := range survivors {
		if candidate.latencyN > 0 {
			candidate.latencyMS = candidate.latencyMS / float64(candidate.latencyN)
		}
		saved = append(saved, proxySaveEntry{
			Proxy:      candidate.entry.Proxy,
			Protocol:   candidate.entry.Protocol,
			Anonymity:  candidate.entry.Anonymity,
			Alive:      true,
			Uptime:     float64(candidate.successes),
			Requests:   candidate.successes + candidate.failures,
			Successes:  candidate.successes,
			Failures:   candidate.failures,
			ResponseMs: candidate.latencyMS,
			LastUsed:   now,
		})
	}
	return saved, nil
}

// collectSweep probes every candidate once, concurrently.
func collectSweep(ctx context.Context, candidates []*collectCandidate, probeTimeout time.Duration, probeURL string) {
	jobs := make(chan *collectCandidate)
	var wg sync.WaitGroup
	for i := 0; i < proxyProbeConcurrency && i < len(candidates); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for candidate := range jobs {
				ok, elapsed := candidate.probe(ctx, probeURL, probeTimeout)
				if ok {
					candidate.successes++
					candidate.latencyMS += float64(elapsed) / float64(time.Millisecond)
					candidate.latencyN++
				} else {
					candidate.failures++
				}
			}
		}()
	}
	for _, candidate := range candidates {
		jobs <- candidate
	}
	close(jobs)
	wg.Wait()
}

// averageLatency returns the mean probe latency in milliseconds.
func (c *collectCandidate) averageLatency() float64 {
	if c.latencyN == 0 {
		return 0
	}
	return c.latencyMS / float64(c.latencyN)
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

// warnDirectFallback logs switching to the direct connection, at most once
// per 30 seconds, so switches stay auditable without flooding the log.
func (p *proxyPool) warnDirectFallback() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.lastFallbackLog) < 30*time.Second {
		return
	}
	p.lastFallbackLog = time.Now()
	log.Printf("proxy pool has no healthy peers; falling back to direct connection")
}

// requestLease ties one HTTP request to the peer that serves it. Proxies
// off (nil pool) and direct fallback both yield a lease whose client is
// the direct client; directFallback records which case, so callers can
// keep direct-mode rate-limit handling when the pool is drained.
type requestLease struct {
	pool           *proxyPool
	peer           *proxyPeer
	client         *http.Client
	directFallback bool
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
		return requestLease{client: client, directFallback: true}, nil
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
