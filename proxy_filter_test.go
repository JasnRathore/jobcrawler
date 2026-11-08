package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// probeOKClient returns a probe client that answers every attempt with
// the given status after an optional delay.
func probeOKClient(status int, delay time.Duration) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}
}

func probeErrorClient(err error) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, err
	})}
}

const testProbeURL = "https://probe.example/204"

func TestFilterProxiesSplitsSurvivorsAndDeadWithStatsAndOrder(t *testing.T) {
	build := func(proxyURL *url.URL, _ time.Duration) *http.Client {
		switch proxyURL.Hostname() {
		case "fast.example":
			return probeOKClient(http.StatusNoContent, 0)
		case "slow.example":
			return probeOKClient(http.StatusNoContent, 20*time.Millisecond)
		default:
			return probeErrorClient(errors.New("dial failed"))
		}
	}
	entries := []proxyListEntry{
		{Proxy: "http://slow.example:8080", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 99},
		{Proxy: "http://b-dead.example:8080", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 98},
		{Proxy: "http://fast.example:8080", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 97},
		{Proxy: "http://a-dead.example:8080", Protocol: "http", Alive: true, Anonymity: "elite", Uptime: 88},
	}
	cfg := proxyFilterConfig{rounds: 1, concurrency: 2, timeout: 2 * time.Second, buildClient: build, probeURL: testProbeURL}

	survivors, dead, err := filterProxies(context.Background(), entries, cfg)
	if err != nil {
		t.Fatal(err)
	}

	if len(survivors) != 2 || len(dead) != 2 {
		t.Fatalf("survivors = %d, dead = %d, want 2 and 2\nsurvivors: %+v\ndead: %+v", len(survivors), len(dead), survivors, dead)
	}
	// Equal successes: the faster route sorts first.
	if survivors[0].Proxy != "http://fast.example:8080" || survivors[1].Proxy != "http://slow.example:8080" {
		t.Fatalf("survivor order = [%s, %s], want fast before slow by response time",
			survivors[0].Proxy, survivors[1].Proxy)
	}
	for _, survivor := range survivors {
		if !survivor.Alive || survivor.Requests != 1 || survivor.Successes != 1 || survivor.Failures != 0 {
			t.Fatalf("survivor stats = %+v, want alive with 1 request and 1 success", survivor)
		}
		if survivor.Uptime != 1 {
			t.Fatalf("survivor uptime = %v, want observed successes as the load-time sort key", survivor.Uptime)
		}
		if survivor.ResponseMs < 0 {
			t.Fatalf("survivor response_ms = %v, want a non-negative mean latency", survivor.ResponseMs)
		}
	}
	// The slow survivor slept 20ms per round trip, so it must show a
	// measurable latency while the fast one ordered ahead of it.
	if survivors[1].ResponseMs <= 0 {
		t.Fatalf("slow survivor response_ms = %v, want measured latency from the 20ms delay", survivors[1].ResponseMs)
	}
	// Dead candidates sort alphabetically and keep source uptime, but are
	// marked not alive so a review file can never load as a run list.
	if dead[0].Proxy != "http://a-dead.example:8080" || dead[1].Proxy != "http://b-dead.example:8080" {
		t.Fatalf("dead order = [%s, %s], want alphabetical", dead[0].Proxy, dead[1].Proxy)
	}
	if dead[0].Alive || dead[1].Alive {
		t.Fatalf("dead entries must not be alive: %+v", dead)
	}
	if dead[0].Requests != 1 || dead[0].Successes != 0 || dead[0].Failures != 1 {
		t.Fatalf("dead stats = %+v, want 1 failed attempt", dead[0])
	}
	if dead[0].Uptime != 88 || dead[0].ResponseMs != 0 {
		t.Fatalf("dead entry = %+v, want source uptime preserved and no response_ms", dead[0])
	}
}

func TestFilterProxiesAllRoundsRequiresEveryAttempt(t *testing.T) {
	mkBuilder := func(flakyAttempts *int) buildProbeClient {
		return func(proxyURL *url.URL, _ time.Duration) *http.Client {
			if proxyURL.Hostname() == "steady.example" {
				return probeOKClient(http.StatusOK, 0)
			}
			return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				*flakyAttempts++
				if *flakyAttempts == 1 {
					return &http.Response{
						StatusCode: http.StatusOK,
						Status:     "200 OK",
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader("")),
						Request:    req,
					}, nil
				}
				return nil, errors.New("dial failed")
			})}
		}
	}
	entries := []proxyListEntry{
		{Proxy: "http://steady.example:8080", Protocol: "http", Alive: true, Anonymity: "elite"},
		{Proxy: "http://flaky.example:8080", Protocol: "http", Alive: true, Anonymity: "elite"},
	}

	t.Run("best of rounds survives a single success", func(t *testing.T) {
		var flakyAttempts int
		cfg := proxyFilterConfig{rounds: 2, concurrency: 1, timeout: 2 * time.Second, buildClient: mkBuilder(&flakyAttempts), probeURL: testProbeURL}
		survivors, dead, err := filterProxies(context.Background(), entries, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(survivors) != 2 || len(dead) != 0 {
			t.Fatalf("survivors = %d, dead = %d, want 2 and 0\nsurvivors: %+v\ndead: %+v", len(survivors), len(dead), survivors, dead)
		}
		var flaky *proxySaveEntry
		for i := range survivors {
			if strings.Contains(survivors[i].Proxy, "flaky") {
				flaky = &survivors[i]
			}
		}
		if flaky == nil || flaky.Successes != 1 || flaky.Requests != 1 {
			t.Fatalf("flaky stats = %+v, want early break after 1 attempt", flaky)
		}
		if flakyAttempts != 1 {
			t.Fatalf("flaky attempts = %d, want 1 (no extra round after success)", flakyAttempts)
		}
	})

	t.Run("all rounds must pass to survive", func(t *testing.T) {
		var flakyAttempts int
		cfg := proxyFilterConfig{rounds: 2, concurrency: 1, timeout: 2 * time.Second, allRounds: true, buildClient: mkBuilder(&flakyAttempts), probeURL: testProbeURL}
		survivors, dead, err := filterProxies(context.Background(), entries, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(survivors) != 1 || survivors[0].Proxy != "http://steady.example:8080" {
			t.Fatalf("survivors = %+v, want only the steady route", survivors)
		}
		if survivors[0].Successes != 2 || survivors[0].Requests != 2 {
			t.Fatalf("steady stats = %+v, want 2 successes over 2 attempts", survivors[0])
		}
		if len(dead) != 1 || dead[0].Proxy != "http://flaky.example:8080" || dead[0].Alive {
			t.Fatalf("dead = %+v, want the flaky route marked dead", dead)
		}
		if dead[0].Successes != 1 || dead[0].Failures != 1 || dead[0].Requests != 2 {
			t.Fatalf("flaky stats = %+v, want 1 success and 1 failure over 2 attempts", dead[0])
		}
		if flakyAttempts != 2 {
			t.Fatalf("flaky attempts = %d, want 2 under --all-rounds", flakyAttempts)
		}
	})
}

func TestFilterProxiesTreatsHTTPErrorStatusAsFailure(t *testing.T) {
	build := func(proxyURL *url.URL, _ time.Duration) *http.Client {
		if strings.Contains(proxyURL.Hostname(), "server-error") {
			return probeOKClient(http.StatusInternalServerError, 0)
		}
		return probeOKClient(http.StatusNotFound, 0)
	}
	entries := []proxyListEntry{
		{Proxy: "http://server-error.example:8080", Protocol: "http", Alive: true, Anonymity: "elite"},
		{Proxy: "http://not-found.example:8080", Protocol: "http", Alive: true, Anonymity: "elite"},
	}
	cfg := proxyFilterConfig{rounds: 1, concurrency: 2, timeout: 2 * time.Second, buildClient: build, probeURL: testProbeURL}

	survivors, dead, err := filterProxies(context.Background(), entries, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(survivors) != 0 || len(dead) != 2 {
		t.Fatalf("survivors = %+v, dead = %+v, want both error statuses dead", survivors, dead)
	}
	for _, deadEntry := range dead {
		if deadEntry.Failures != 1 || deadEntry.Successes != 0 || deadEntry.Alive {
			t.Fatalf("dead entry = %+v, want 1 failure and not alive", deadEntry)
		}
	}
}

func TestFilterProxiesRejectsInvalidConfig(t *testing.T) {
	okBuilder := func(_ *url.URL, _ time.Duration) *http.Client { return probeOKClient(http.StatusOK, 0) }
	entries := []proxyListEntry{{Proxy: "http://example.com:8080", Protocol: "http", Alive: true}}
	valid := proxyFilterConfig{rounds: 1, concurrency: 1, timeout: time.Second, buildClient: okBuilder, probeURL: testProbeURL}
	tests := []struct {
		name   string
		mutate func(*proxyFilterConfig)
		want   string
	}{
		{"rounds zero", func(cfg *proxyFilterConfig) { cfg.rounds = 0 }, "--rounds"},
		{"rounds too many", func(cfg *proxyFilterConfig) { cfg.rounds = 11 }, "--rounds"},
		{"concurrency zero", func(cfg *proxyFilterConfig) { cfg.concurrency = 0 }, "--concurrency"},
		{"concurrency too many", func(cfg *proxyFilterConfig) { cfg.concurrency = 513 }, "--concurrency"},
		{"timeout zero", func(cfg *proxyFilterConfig) { cfg.timeout = 0 }, "--timeout"},
		{"nil builder", func(cfg *proxyFilterConfig) { cfg.buildClient = nil }, "builder"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if _, _, err := filterProxies(context.Background(), entries, cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFilterProxiesRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := proxyFilterConfig{rounds: 2, concurrency: 1, timeout: time.Second,
		buildClient: func(_ *url.URL, _ time.Duration) *http.Client { return probeOKClient(http.StatusOK, 0) },
		probeURL:    testProbeURL,
	}
	entries := []proxyListEntry{{Proxy: "http://example.com:8080", Protocol: "http", Alive: true}}
	if _, _, err := filterProxies(ctx, entries, cfg); err == nil {
		t.Fatal("cancelled context returned no error")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestProxyFilterCommandWritesVerifiedListAndFailuresRoundTrip(t *testing.T) {
	input := `[
  {"proxy":"http://good-b.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":90},
  {"proxy":"http://good-a.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":95},
  {"proxy":"http://dead.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":92},
  {"proxy":"http://stale.example:8080","protocol":"http","alive":false,"anonymity":"elite","uptime":99}
]`
	inputDir := t.TempDir()
	inputPath := filepath.Join(inputDir, "proxies.json")
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	provenPath := filepath.Join(inputDir, "proven.json")
	deadPath := filepath.Join(inputDir, "dead.json")

	build := func(proxyURL *url.URL, _ time.Duration) *http.Client {
		switch proxyURL.Hostname() {
		case "good-a.example":
			return probeOKClient(http.StatusNoContent, 0)
		case "good-b.example":
			return probeOKClient(http.StatusNoContent, 20*time.Millisecond)
		default:
			return probeErrorClient(errors.New("dial failed"))
		}
	}
	command := newProxyFilterCommandWithBuilder(build, testProbeURL)
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stdout)
	command.SetArgs([]string{inputPath, "-o", provenPath, "--failures", deadPath})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	provenData, err := os.ReadFile(provenPath)
	if err != nil {
		t.Fatal(err)
	}
	var proven []proxySaveEntry
	if err := json.Unmarshal(provenData, &proven); err != nil {
		t.Fatalf("decode proven list: %v\n%s", err, provenData)
	}
	if len(proven) != 2 {
		t.Fatalf("proven proxies = %d, want 2\n%s", len(proven), provenData)
	}
	if proven[0].Proxy != "http://good-a.example:8080" || proven[1].Proxy != "http://good-b.example:8080" {
		t.Fatalf("proven order = [%s, %s], want best-proven first", proven[0].Proxy, proven[1].Proxy)
	}
	for _, entry := range proven {
		if !entry.Alive || entry.Successes != 1 || entry.Requests != 1 {
			t.Fatalf("proven entry = %+v, want alive with measured success", entry)
		}
	}

	deadData, err := os.ReadFile(deadPath)
	if err != nil {
		t.Fatal(err)
	}
	var dead []proxySaveEntry
	if err := json.Unmarshal(deadData, &dead); err != nil {
		t.Fatalf("decode dead review file: %v\n%s", err, deadData)
	}
	if len(dead) != 1 || dead[0].Proxy != "http://dead.example:8080" || dead[0].Alive {
		t.Fatalf("dead entries = %+v, want only the dead route marked not alive", dead)
	}

	// The proven file must feed a run directly, dead file must not.
	pool, err := loadProxyPool(context.Background(), provenPath, freeConfig(), time.Second, 5*time.Second, "")
	if err != nil {
		t.Fatalf("proven list is not usable with --proxies: %v", err)
	}
	if len(pool.peers) != 2 {
		t.Fatalf("proven pool peers = %d, want 2", len(pool.peers))
	}
	if _, err := loadProxyPool(context.Background(), deadPath, freeConfig(), time.Second, 5*time.Second, ""); err == nil {
		t.Fatal("dead review file loaded as a run list; dead entries must never be loadable")
	}
	// No temp file left behind.
	if _, err := os.Stat(provenPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary proven file left behind: %v", err)
	}
}

func TestProxyFilterCommandWritesSurvivorsToStdout(t *testing.T) {
	input := `[{"proxy":"http://good.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":90}]`
	inputPath := filepath.Join(t.TempDir(), "proxies.json")
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	build := func(_ *url.URL, _ time.Duration) *http.Client { return probeOKClient(http.StatusNoContent, 0) }
	command := newProxyFilterCommandWithBuilder(build, testProbeURL)
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stdout)
	command.SetArgs([]string{inputPath})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var proven []proxySaveEntry
	if err := json.Unmarshal(stdout.Bytes(), &proven); err != nil {
		t.Fatalf("decode stdout survivors: %v\n%s", err, stdout.String())
	}
	if len(proven) != 1 || proven[0].Proxy != "http://good.example:8080" || !proven[0].Alive {
		t.Fatalf("stdout survivors = %+v, want the single verified route", proven)
	}
}

func TestProxyFilterCommandRejectsListWithoutEligibleEntries(t *testing.T) {
	input := `[{"proxy":"http://stale.example:8080","protocol":"http","alive":false,"anonymity":"elite","uptime":99}]`
	inputPath := filepath.Join(t.TempDir(), "proxies.json")
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	build := func(_ *url.URL, _ time.Duration) *http.Client { return probeOKClient(http.StatusOK, 0) }
	command := newProxyFilterCommandWithBuilder(build, testProbeURL)
	command.SetArgs([]string{inputPath})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "no eligible proxy entries") {
		t.Fatalf("err = %v, want 'no eligible proxy entries'", err)
	}
}

func TestProxyFilterCommandHelpDocumentsNeutralProbeAndOutput(t *testing.T) {
	build := func(_ *url.URL, _ time.Duration) *http.Client { return probeOKClient(http.StatusOK, 0) }
	command := newProxyFilterCommandWithBuilder(build, testProbeURL)
	var help bytes.Buffer
	command.SetOut(&help)
	command.SetErr(&help)
	command.SetArgs([]string{"--help"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"Live-check", "neutral", "connectivity", "--rounds", "--concurrency",
		"--timeout", "--all-rounds", "--failures", "Examples:", "filter",
	} {
		if !strings.Contains(help.String(), expected) {
			t.Errorf("filter help missing %q:\n%s", expected, help.String())
		}
	}
	for _, line := range strings.Split(help.String(), "\n") {
		if len(line) > 80 {
			t.Errorf("filter help line exceeds 80 columns (%d): %q", len(line), line)
		}
	}
}
