package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyRefreshFetchesOneHTTPSListAndFiltersWithoutProbing(t *testing.T) {
	var sourceRequests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceRequests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/list.json" {
			t.Errorf("source request = %s %s, want GET /list.json", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Accept"); !strings.Contains(got, "application/json") {
			t.Errorf("Accept = %q, want a JSON-capable accept header", got)
		}
		_, _ = w.Write([]byte(`[
			{"proxy":"http://proxy.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":91},
			{"proxy":"socks5://proxy.example:1080","protocol":"socks5","alive":true,"anonymity":"elite","uptime":99},
			{"proxy":"http://user:secret@credentialed.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":100},
			{"proxy":"http://dead.example:8080","protocol":"http","alive":false,"anonymity":"elite","uptime":100},
			{"proxy":"http://anonymous.example:8080","protocol":"http","alive":true,"anonymity":"anonymous","uptime":100},
			{"proxy":"http://broken.example:port","protocol":"http","alive":true,"anonymity":"elite","uptime":100}
		]`))
	}))
	defer server.Close()

	entries, err := fetchProxySourceEntries(context.Background(), server.URL+"/list.json", time.Second, server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	filtered := filterProxySourceEntries(entries, 2)
	if len(filtered) != 2 {
		t.Fatalf("eligible entries = %+v, want two", filtered)
	}
	if filtered[0].Proxy != "socks5://proxy.example:1080" || filtered[1].Proxy != "http://proxy.example:8080" {
		t.Fatalf("filtered entries not sorted by source uptime: %+v", filtered)
	}
	if sourceRequests.Load() != 1 {
		t.Fatalf("source requests = %d, want exactly one; proxy candidates must not be probed", sourceRequests.Load())
	}
}

func TestProxyRefreshCommandPrintsFilteredListForReview(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"proxy":"http://proxy.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":90},
			{"proxy":"http://private:secret@bad.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":99}
		]`))
	}))
	defer server.Close()

	command := newProxyRefreshCommandWithTransport(server.Client().Transport)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--source", server.URL + "/list.json"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var result proxyRefreshOutput
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode refresh output: %v\n%s", err, output.String())
	}
	if result.RetrievedAt.IsZero() || len(result.Sources) != 1 || len(result.Proxies) != 1 {
		t.Fatalf("refresh output = %+v, want one filtered proxy and source metadata", result)
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatal("proxy credentials were emitted")
	}
}

func TestProxyRefreshOutputFlagWritesLoadableFileInsteadOfStdout(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"proxy":"socks5://proxy.example:1080","protocol":"socks5","alive":true,"anonymity":"elite","uptime":90},
			{"proxy":"http://private:secret@bad.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":99},
			{"proxy":"http://dead.example:8080","protocol":"http","alive":false,"anonymity":"elite","uptime":99}
		]`))
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "gathered.json")
	command := newProxyRefreshCommandWithTransport(server.Client().Transport)
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stdout)
	command.SetArgs([]string{"--source", server.URL + "/list.json", "--output", path})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	// File mode keeps the report out of stdout (logs go to stderr).
	if strings.Contains(stdout.String(), "proxy") {
		t.Fatalf("stdout should stay empty in file mode: %q", stdout.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report proxyRefreshOutput
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode persisted report: %v\n%s", err, data)
	}
	if report.RetrievedAt.IsZero() || len(report.Sources) != 1 || len(report.Proxies) != 1 {
		t.Fatalf("persisted report = %+v, want source metadata and one eligible proxy", report)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatal("proxy credentials were written to the file")
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temporary report file was left behind: %v", err)
	}

	// The same file must be directly usable as a --proxies list.
	pool, err := loadProxyPool(context.Background(), path, freeConfig(), time.Second, 5*time.Second, "")
	if err != nil {
		t.Fatalf("persisted report is not usable with --proxies: %v", err)
	}
	if len(pool.peers) != 1 || pool.peers[0].raw != "socks5://proxy.example:1080" {
		t.Fatalf("loaded routes from persisted report: %+v", pool.peers)
	}
}

func TestProxyRefreshOutputDashKeepsStdoutReport(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"proxy":"http://proxy.example:8080","protocol":"http","alive":true,"anonymity":"elite","uptime":90}]`))
	}))
	defer server.Close()

	command := newProxyRefreshCommandWithTransport(server.Client().Transport)
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stdout)
	command.SetArgs([]string{"--source", server.URL + "/list.json", "--output", "-"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var report proxyRefreshOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode stdout report: %v\n%s", err, stdout.String())
	}
	if len(report.Proxies) != 1 {
		t.Fatalf("stdout proxies = %+v, want one eligible entry", report.Proxies)
	}
}

func TestDefaultProxyRefreshSourcesCoverAllFourFormats(t *testing.T) {
	sources := defaultProxyRefreshSources(100)
	if len(sources) != 5 {
		t.Fatalf("default source requests = %d, want five requests across four providers", len(sources))
	}
	formats := make(map[proxyRefreshSourceFormat]int)
	names := make(map[string]bool)
	for _, source := range sources {
		formats[source.format]++
		names[source.name] = true
		if _, err := validateProxySourceURL(source.rawURL); err != nil {
			t.Errorf("%s URL is invalid: %v", source.name, err)
		}
	}
	for _, name := range []string{"HProxy", "Proxifly", "ProxyScrape http", "ProxyScrape socks5", "Geonode"} {
		if !names[name] {
			t.Errorf("built-in sources missing %q", name)
		}
	}
	if formats[proxyRefreshHProxyJSON] != 1 || formats[proxyRefreshURLLines] != 1 ||
		formats[proxyRefreshAddressLines] != 2 || formats[proxyRefreshJSON] != 1 {
		t.Fatalf("built-in source formats = %+v", formats)
	}
}

func TestProxyRefreshGatherNormalizesSourcesAndKeepsPartialResults(t *testing.T) {
	responses := map[string]struct {
		status int
		body   string
	}{
		"/hproxy": {
			status: http.StatusOK,
			body:   `[{"ip":"192.0.2.1","port":8080,"protocols":["http"],"anonymity":"elite","status":"alive","uptime_pct":95}]`,
		},
		"/proxifly": {
			status: http.StatusOK,
			body:   "socks5://192.0.2.2:1080\nsocks4://192.0.2.3:1080\n",
		},
		"/proxyscrape": {
			status: http.StatusServiceUnavailable,
			body:   "temporarily unavailable",
		},
		"/geonode": {
			status: http.StatusOK,
			body:   `{"data":[{"ip":"192.0.2.4","port":"3128","protocols":["http"],"anonymityLevel":"elite","upTime":88,"responseTime":30}]}`,
		},
	}
	var requests atomic.Int32
	transport := proxyRefreshRoundTripper(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		response, exists := responses[request.URL.Path]
		if !exists {
			return nil, fmt.Errorf("unexpected request to %s", request.URL.Path)
		}
		return &http.Response{
			StatusCode: response.status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(response.body)),
			Request:    request,
		}, nil
	})
	sources := []proxyRefreshSource{
		{name: "HProxy", rawURL: "https://sources.example/hproxy", format: proxyRefreshHProxyJSON},
		{name: "Proxifly", rawURL: "https://sources.example/proxifly", format: proxyRefreshURLLines},
		{name: "ProxyScrape http", rawURL: "https://sources.example/proxyscrape", format: proxyRefreshAddressLines, protocol: "http"},
		{name: "Geonode", rawURL: "https://sources.example/geonode", format: proxyRefreshJSON},
	}
	entries, reports := gatherProxyRefreshSources(context.Background(), sources, time.Second, transport)
	filtered := filterProxySourceEntries(entries, 10)
	if requests.Load() != int32(len(sources)) {
		t.Fatalf("source requests = %d, want %d", requests.Load(), len(sources))
	}
	if len(filtered) != 3 {
		t.Fatalf("normalized entries = %+v, want three supported proxies", filtered)
	}
	if filtered[0].Proxy != "http://192.0.2.1:8080" ||
		filtered[1].Proxy != "socks5://192.0.2.2:1080" ||
		filtered[2].Proxy != "http://192.0.2.4:3128" {
		t.Fatalf("normalized entries = %+v, want merged source results sorted by uptime", filtered)
	}
	if len(reports) != len(sources) || reports[2].Error == "" || reports[2].Count != 0 {
		t.Fatalf("source reports = %+v, want failed source recorded alongside successful results", reports)
	}
}

func TestDecodeProxyRefreshTextFormats(t *testing.T) {
	addressEntries, err := decodeProxyAddressLines([]byte("203.0.113.1:8080\n"), "http")
	if err != nil {
		t.Fatal(err)
	}
	if len(addressEntries) != 1 || addressEntries[0].Proxy != "http://203.0.113.1:8080" {
		t.Fatalf("plain address entries = %+v", addressEntries)
	}
	urlEntries, err := decodeProxyAddressLines([]byte("socks5://203.0.113.2:1080\nhttp://203.0.113.3:80\n%://invalid\n"), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(urlEntries) != 2 || urlEntries[0].Protocol != "socks5" || urlEntries[1].Protocol != "http" {
		t.Fatalf("URL line entries = %+v", urlEntries)
	}
	emptyGeonode, err := decodeProxyList([]byte(`{"data":[]}`))
	if err != nil || len(emptyGeonode) != 0 {
		t.Fatalf("empty Geonode list = %+v, %v; want a valid empty result", emptyGeonode, err)
	}
}

func TestProxyRefreshRejectsInsecureSourceAndCrossHostRedirects(t *testing.T) {
	if _, err := validateProxySourceURL("http://source.example/list.json"); err == nil {
		t.Fatal("HTTP source was accepted; HTTPS is required")
	}
	if _, err := validateProxySourceURL("https://user:secret@source.example/list.json"); err == nil {
		t.Fatal("source URL userinfo was accepted")
	}

	var redirectTargetRequests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectTargetRequests.Add(1)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	_, err := fetchProxySourceEntries(context.Background(), source.URL, time.Second, source.Client().Transport)
	if err == nil || !strings.Contains(err.Error(), "request to proxy source") {
		t.Fatalf("cross-host redirect error = %v, want safe source request error", err)
	}
	if redirectTargetRequests.Load() != 0 {
		t.Fatal("proxy source redirect sent a request to an unconfigured host")
	}
}

func TestFilterProxySourceEntriesRejectsMalformedURLsAndAppliesLimit(t *testing.T) {
	entries := []proxyListEntry{
		{Proxy: "http://one.example:80", Protocol: "http", Alive: true, Uptime: 10},
		{Proxy: "http://one.example:80", Protocol: "http", Alive: true, Uptime: 11},
		{Proxy: "http://two.example:70000", Protocol: "http", Alive: true, Uptime: 12},
		{Proxy: "http://three.example:8080/path", Protocol: "http", Alive: true, Uptime: 13},
		{Proxy: "http://four.example:8080", Protocol: "http", Alive: true, Uptime: 14},
	}

	filtered := filterProxySourceEntries(entries, 2)
	if len(filtered) != 2 || filtered[0].Proxy != "http://four.example:8080" ||
		filtered[1].Proxy != "http://one.example:80" || filtered[1].Uptime != 11 {
		t.Fatalf("filtered proxy list = %+v, want top two valid unique entries", filtered)
	}
	if len(filterProxySourceEntries(entries, 0)) != 0 {
		t.Fatal("zero result limit returned proxy entries")
	}
}

type proxyRefreshRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper proxyRefreshRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}
