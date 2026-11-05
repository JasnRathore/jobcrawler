package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	failures      int
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

// loadProxyPool reads a proxy list file, keeps alive elite http/socks5
// entries ordered by uptime (best first), dials each address once, and
// keeps only peers that accepted a connection within probeTimeout.
func loadProxyPool(
	ctx context.Context,
	path string,
	cfg proxyPoolConfig,
	probeTimeout time.Duration,
	requestTimeout time.Duration,
) (*proxyPool, error) {
	if probeTimeout <= 0 {
		return nil, errors.New("proxy probe timeout must be greater than 0")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read proxy list: %w", err)
	}
	var entries []proxyListEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("decode proxy list: %w", err)
	}

	type candidate struct {
		peer   *proxyPeer
		uptime float64
	}
	candidates := make([]candidate, 0, len(entries))
	seen := make(map[string]struct{})
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
		candidates = append(candidates, candidate{
			peer: &proxyPeer{
				raw:    entry.Proxy,
				client: newPeerClient(proxyURL, requestTimeout),
			},
			uptime: entry.Uptime,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].uptime > candidates[j].uptime
	})

	ordered := make([]*proxyPeer, 0, len(candidates))
	for _, candidate := range candidates {
		ordered = append(ordered, candidate.peer)
	}

	probed := probeProxyPeers(ctx, ordered, probeTimeout)
	if len(probed) == 0 {
		return nil, fmt.Errorf("no proxy passed the health probe from %s", path)
	}
	log.Printf("loaded %d proxies from %s (%d of %d entries passed the health probe)",
		len(probed), path, len(probed), len(entries))

	return &proxyPool{peers: probed, healthy: len(probed), cfg: cfg}, nil
}

// probeProxyPeers dials every peer address concurrently and returns only
// the peers whose address accepted a TCP connection in time, preserving
// the original order.
func probeProxyPeers(ctx context.Context, peers []*proxyPeer, timeout time.Duration) []*proxyPeer {
	dialer := &net.Dialer{Timeout: timeout}
	jobs := make(chan *proxyPeer)
	survivors := make([]bool, len(peers))
	var wg sync.WaitGroup
	for i := 0; i < proxyProbeConcurrency && i < len(peers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for peer := range jobs {
				if dialProxyPeer(ctx, dialer, peer.raw) == nil {
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
	case proxyOutcomeRateLimited:
		peer.failures++
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
// the direct client.
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
