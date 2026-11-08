package main

import (
	"bufio"
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

	"github.com/spf13/cobra"
)

const maxProxyRefreshEntries = 2000
const proxyRefreshDefaultTimeout = 10 * time.Second

type proxyRefreshOutput struct {
	RetrievedAt time.Time                  `json:"retrievedAt"`
	Sources     []proxyRefreshSourceReport `json:"sources"`
	Proxies     []proxyListEntry           `json:"proxies"`
}

type proxyRefreshSourceReport struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	Error string `json:"error,omitempty"`
}

type proxyRefreshSourceFormat int

const (
	proxyRefreshJSON proxyRefreshSourceFormat = iota
	proxyRefreshHProxyJSON
	proxyRefreshURLLines
	proxyRefreshAddressLines
)

type proxyRefreshSource struct {
	name     string
	rawURL   string
	format   proxyRefreshSourceFormat
	protocol string
}

func newProxyCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "proxy",
		Short: "Gather, review, and filter proxy-list sources",
	}
	command.AddCommand(newProxyRefreshCommand(), newProxyFilterCommand())
	command.SetHelpFunc(func(command *cobra.Command, _ []string) {
		out := command.OutOrStdout()
		fmt.Fprintf(out, `%s

Lists are managed separately from crawling: gathering writes a reviewable
file, and crawler traffic only ever uses a list you pass explicitly with
--proxies.

Usage:
  %s <command>

Commands:
  refresh  Gather and normalize the four built-in public proxy lists
  filter   Live-check a list; keep routes that tunnel

Workflow:
  1. crawler proxy refresh -o proxies.json
     gathers HProxy, Proxifly, ProxyScrape, and Geonode into one file
  2. crawler proxy filter -o proven.json proxies.json
     verifies each entry against a neutral endpoint; survivors load
     first, dead entries go to --failures if set
  3. Review proven.json and keep the routes you are authorized to use.
  4. crawler --greenhouse --words companies.txt --proxies proven.json
     runs rotate across healthy entries; failing routes are evicted.
  5. Add --proxy-save final.json to a run to keep the routes that
     survived it, then reuse that file the same way later.
`, command.Short, command.CommandPath())
	})
	return command
}

func newProxyRefreshCommand() *cobra.Command {
	return newProxyRefreshCommandWithTransport(nil)
}

func newProxyRefreshCommandWithTransport(transport http.RoundTripper) *cobra.Command {
	return newProxyRefreshCommandWithSources(transport, nil)
}

func newProxyRefreshCommandWithSources(
	transport http.RoundTripper,
	sources []proxyRefreshSource,
) *cobra.Command {
	var source string
	var limit int
	var timeout time.Duration
	var outputPath string

	command := &cobra.Command{
		Use:   "refresh",
		Short: "Gather, normalize, and filter public proxy lists",
		Long: `Fetches the built-in HProxy, Proxifly, ProxyScrape, and Geonode lists
in parallel, parses their different JSON and text formats, and writes one
normalized JSON object. Failed sources are reported under "sources" while
results from successful sources are kept under "proxies", so one broken
list cannot erase the others.

Storage and reuse:
  The report goes to stdout, or to a file with -o FILE (the same JSON).
  Nothing else is stored. Review the file, keep the entries you are
  authorized to use, and pass the same file to a run with --proxies FILE.
  Adding --proxy-save FILE2 to that run writes the proxies that survived
  it, with observed request statistics, for reuse.

  # Gather, review, then use in one place
  crawler proxy refresh -o proxies.json
  (review and edit proxies.json)
  crawler --greenhouse --words companies.txt --proxies proxies.json

Gathered entries are not probed: source-reported uptime is a claim, not
a verified transport check, and no job-board endpoint is contacted here.
Destination rate limits and access restrictions still stop crawler work
during runs.`,
		Example: `  # Gather from all four built-in sources straight into a file
  crawler proxy refresh -o proxies.json

  # Gather to stdout instead, for piping or reviewing in the terminal
  crawler proxy refresh > proxies.json
  crawler proxy refresh -o -

  # Fetch one explicit HTTPS JSON source instead of the built-in four
  crawler proxy refresh --source https://example.com/list.json -o proxies.json

  # Raise the per-source fetch budget and emit up to 2000 entries
  crawler proxy refresh --limit 2000 --timeout 30s -o proxies.json`,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if limit < 1 || limit > maxProxyRefreshEntries {
				return fmt.Errorf("--limit must be between 1 and %d", maxProxyRefreshEntries)
			}
			if timeout <= 0 {
				return errors.New("--timeout must be greater than zero")
			}

			var entries []proxyListEntry
			var reports []proxyRefreshSourceReport
			if strings.TrimSpace(source) != "" {
				parsedSource, err := validateProxySourceURL(source)
				if err != nil {
					return err
				}
				entries, err = fetchProxySourceEntries(cmd.Context(), parsedSource.String(), timeout, transport)
				if err != nil {
					return err
				}
				reports = []proxyRefreshSourceReport{{Name: "custom", Count: len(entries)}}
			} else {
				configuredSources := sources
				if configuredSources == nil {
					configuredSources = defaultProxyRefreshSources(limit)
				}
				entries, reports = gatherProxyRefreshSources(cmd.Context(), configuredSources, timeout, transport)
			}

			eligible := filterProxySourceEntries(entries, limit)
			report := proxyRefreshOutput{
				RetrievedAt: time.Now().UTC(),
				Sources:     reports,
				Proxies:     eligible,
			}
			if strings.TrimSpace(outputPath) != "" && outputPath != "-" {
				if err := writeProxyRefreshReport(outputPath, report); err != nil {
					return err
				}
				log.Printf("saved %d eligible proxies from %d source report(s) to %s; a run can use it with --proxies %s",
					len(eligible), len(reports), outputPath, outputPath)
			} else {
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(report); err != nil {
					return err
				}
			}
			if len(eligible) == 0 {
				return errors.New("no eligible credential-free proxies were returned; inspect the source report for errors or retry later")
			}
			return nil
		},
	}
	command.Flags().StringVar(&source, "source", "", "fetch one HTTPS JSON source, not the built-in four")
	command.Flags().StringVarP(&outputPath, "output", "o", "", "write report to this .json file instead of stdout")
	command.Flags().IntVar(&limit, "limit", 500, "max eligible entries to emit (1-2000)")
	command.Flags().DurationVar(&timeout, "timeout", proxyRefreshDefaultTimeout, "per-source HTTP timeout")
	command.SetHelpFunc(writeProxyRefreshHelp)
	return command
}

func writeProxyRefreshHelp(command *cobra.Command, _ []string) {
	out := command.OutOrStdout()
	fmt.Fprintf(out, "%s\n\n%s\n\nUsage:\n  %s [flags]\n\nFlags:\n%s",
		command.Short, strings.TrimSpace(command.Long), command.CommandPath(), command.Flags().FlagUsages())
	// Trim only newlines so the example's leading indentation survives.
	if example := strings.Trim(command.Example, "\n"); example != "" {
		fmt.Fprintf(out, "\nExamples:\n%s\n", example)
	}
}

func defaultProxyRefreshSources(limit int) []proxyRefreshSource {
	params := url.Values{
		"format":    {"json"},
		"protocol":  {"http,socks5"},
		"anonymity": {"elite"},
		"sort":      {"uptime"},
		"limit":     {strconv.Itoa(limit)},
	}
	hproxyURL := (&url.URL{
		Scheme:   "https",
		Host:     "hproxy.com",
		Path:     "/api/proxy-list",
		RawQuery: params.Encode(),
	}).String()

	geoLimit := limit
	if geoLimit > 500 {
		geoLimit = 500
	}
	geonodeParams := url.Values{
		"limit":     {strconv.Itoa(geoLimit)},
		"page":      {"1"},
		"sort_by":   {"lastChecked"},
		"sort_type": {"desc"},
	}
	geonodeURL := (&url.URL{
		Scheme:   "https",
		Host:     "proxylist.geonode.com",
		Path:     "/api/proxy-list",
		RawQuery: geonodeParams.Encode(),
	}).String()

	proxyscrapeSources := make([]proxyRefreshSource, 0, 2)
	for _, protocol := range []string{"http", "socks5"} {
		proxyscrapeParams := url.Values{
			"request":  {"getproxies"},
			"protocol": {protocol},
			"limit":    {strconv.Itoa(limit)},
		}
		proxyscrapeURL := (&url.URL{
			Scheme:   "https",
			Host:     "api.proxyscrape.com",
			Path:     "/v4/free-proxy-list/get",
			RawQuery: proxyscrapeParams.Encode(),
		}).String()
		proxyscrapeSources = append(proxyscrapeSources, proxyRefreshSource{
			name:     "ProxyScrape " + protocol,
			rawURL:   proxyscrapeURL,
			format:   proxyRefreshAddressLines,
			protocol: protocol,
		})
	}

	sources := []proxyRefreshSource{
		{
			name:   "HProxy",
			rawURL: hproxyURL,
			format: proxyRefreshHProxyJSON,
		},
		{
			name:   "Proxifly",
			rawURL: "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt",
			format: proxyRefreshURLLines,
		},
	}
	sources = append(sources, proxyscrapeSources...)
	sources = append(sources, proxyRefreshSource{
		name:   "Geonode",
		rawURL: geonodeURL,
		format: proxyRefreshJSON,
	})
	return sources
}

func validateProxySourceURL(source string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(source))
	if err != nil {
		return nil, errors.New("--source must be a valid HTTPS URL")
	}
	if !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("--source must be an HTTPS URL without userinfo or a fragment")
	}
	parsed.Scheme = "https"
	return parsed, nil
}

func fetchProxySourceEntries(
	ctx context.Context,
	source string,
	timeout time.Duration,
	transport http.RoundTripper,
) ([]proxyListEntry, error) {
	parsedSource, err := validateProxySourceURL(source)
	if err != nil {
		return nil, err
	}
	data, err := fetchProxySourcePayload(ctx, parsedSource, timeout, transport)
	if err != nil {
		return nil, err
	}
	entries, err := decodeProxyList(data)
	if err != nil {
		return nil, fmt.Errorf("decode proxy source response: %w", err)
	}
	return entries, nil
}

func fetchProxySourcePayload(
	ctx context.Context,
	parsedSource *url.URL,
	timeout time.Duration,
	transport http.RoundTripper,
) ([]byte, error) {
	if timeout <= 0 {
		timeout = proxyRefreshDefaultTimeout
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 3 ||
				request.URL.Scheme != "https" ||
				!strings.EqualFold(request.URL.Host, parsedSource.Host) ||
				request.URL.User != nil {
				return errors.New("proxy source redirect must remain on the configured HTTPS host")
			}
			return nil
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedSource.String(), nil)
	if err != nil {
		return nil, errors.New("build proxy source request")
	}
	request.Header.Set("Accept", "application/json, text/plain;q=0.9, */*;q=0.8")
	request.Header.Set("User-Agent", "jobcrawler-proxy-source-refresh")
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request to proxy source %s failed", parsedSource.Host)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy source %s returned HTTP %d; refresh was not retried", parsedSource.Host, response.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, proxyListMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read proxy source response: %w", err)
	}
	if len(data) > proxyListMaxBytes {
		return nil, fmt.Errorf("proxy source response exceeds %d bytes", proxyListMaxBytes)
	}
	return data, nil
}

func gatherProxyRefreshSources(
	ctx context.Context,
	sources []proxyRefreshSource,
	timeout time.Duration,
	transport http.RoundTripper,
) ([]proxyListEntry, []proxyRefreshSourceReport) {
	type sourceResult struct {
		index   int
		entries []proxyListEntry
		report  proxyRefreshSourceReport
	}

	results := make(chan sourceResult, len(sources))
	var workers sync.WaitGroup
	for index, source := range sources {
		workers.Add(1)
		go func(index int, source proxyRefreshSource) {
			defer workers.Done()
			report := proxyRefreshSourceReport{Name: source.name}
			parsedSource, err := validateProxySourceURL(source.rawURL)
			if err == nil {
				var data []byte
				data, err = fetchProxySourcePayload(ctx, parsedSource, timeout, transport)
				if err == nil {
					var entries []proxyListEntry
					entries, err = parseProxyRefreshSource(source, data)
					if err == nil {
						report.Count = len(entries)
						results <- sourceResult{index: index, entries: entries, report: report}
						return
					}
				}
			}
			report.Error = err.Error()
			results <- sourceResult{index: index, report: report}
		}(index, source)
	}
	workers.Wait()
	close(results)

	orderedResults := make([]sourceResult, len(sources))
	for result := range results {
		orderedResults[result.index] = result
	}
	var entries []proxyListEntry
	reports := make([]proxyRefreshSourceReport, 0, len(sources))
	for _, result := range orderedResults {
		entries = append(entries, result.entries...)
		reports = append(reports, result.report)
	}
	return entries, reports
}

func parseProxyRefreshSource(source proxyRefreshSource, data []byte) ([]proxyListEntry, error) {
	switch source.format {
	case proxyRefreshJSON:
		return decodeProxyList(data)
	case proxyRefreshHProxyJSON:
		return decodeHProxyList(data)
	case proxyRefreshURLLines:
		return decodeProxyAddressLines(data, "")
	case proxyRefreshAddressLines:
		return decodeProxyAddressLines(data, source.protocol)
	default:
		return nil, errors.New("unsupported proxy source format")
	}
}

type hproxyProxyEntry struct {
	IP        string      `json:"ip"`
	Port      json.Number `json:"port"`
	Protocols []string    `json:"protocols"`
	Anonymity string      `json:"anonymity"`
	Status    string      `json:"status"`
	Uptime    float64     `json:"uptime_pct"`
}

func decodeHProxyList(data []byte) ([]proxyListEntry, error) {
	var records []hproxyProxyEntry
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("decode HProxy JSON response: %w", err)
	}
	var entries []proxyListEntry
	for _, record := range records {
		if record.IP == "" || record.Port == "" {
			continue
		}
		hostPort := net.JoinHostPort(record.IP, record.Port.String())
		for _, protocol := range record.Protocols {
			protocol = strings.ToLower(strings.TrimSpace(protocol))
			if protocol != "http" && protocol != "socks5" {
				continue
			}
			entries = append(entries, proxyListEntry{
				Proxy:     protocol + "://" + hostPort,
				Protocol:  protocol,
				Alive:     strings.EqualFold(record.Status, "alive"),
				Anonymity: record.Anonymity,
				Uptime:    record.Uptime,
			})
		}
	}
	return entries, nil
}

func decodeProxyAddressLines(data []byte, defaultProtocol string) ([]proxyListEntry, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 4096), 64*1024)
	var entries []proxyListEntry
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\uFEFF"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		proxyURL := line
		if !strings.Contains(proxyURL, "://") {
			if defaultProtocol == "" {
				continue
			}
			proxyURL = defaultProtocol + "://" + proxyURL
		}
		parsed, err := url.Parse(proxyURL)
		if err != nil {
			continue
		}
		protocol := strings.ToLower(parsed.Scheme)
		if protocol != "http" && protocol != "socks5" {
			continue
		}
		entries = append(entries, proxyListEntry{
			Proxy:    parsed.String(),
			Protocol: protocol,
			Alive:    true,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read proxy list text: %w", err)
	}
	return entries, nil
}

func filterProxySourceEntries(entries []proxyListEntry, limit int) []proxyListEntry {
	if limit <= 0 {
		return nil
	}
	eligible := make([]proxyListEntry, 0, len(entries))
	seen := make(map[string]int, len(entries))
	for _, entry := range entries {
		if !entry.Alive {
			continue
		}
		protocol := strings.ToLower(strings.TrimSpace(entry.Protocol))
		if protocol != "http" && protocol != "socks5" {
			continue
		}
		anonymity := strings.ToLower(strings.TrimSpace(entry.Anonymity))
		if anonymity != "" && anonymity != "elite" {
			continue
		}
		proxyURL, err := url.Parse(strings.TrimSpace(entry.Proxy))
		if err != nil ||
			proxyURL.User != nil ||
			proxyURL.Hostname() == "" ||
			proxyURL.Port() == "" ||
			proxyURL.RawQuery != "" ||
			proxyURL.Fragment != "" ||
			(proxyURL.Path != "" && proxyURL.Path != "/") ||
			!strings.EqualFold(proxyURL.Scheme, protocol) {
			continue
		}
		port, err := parsePort(proxyURL.Port())
		if err != nil || port < 1 || port > 65535 {
			continue
		}
		proxyURL.Path = ""
		proxyURL.RawPath = ""
		entry.Proxy = proxyURL.String()
		entry.Protocol = protocol
		entry.Anonymity = anonymity
		key := strings.ToLower(proxyURL.String())
		if index, exists := seen[key]; exists {
			if entry.Uptime > eligible[index].Uptime {
				eligible[index] = entry
			}
			continue
		}
		seen[key] = len(eligible)
		eligible = append(eligible, entry)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		return eligible[i].Uptime > eligible[j].Uptime
	})
	if len(eligible) > limit {
		eligible = eligible[:limit]
	}
	return eligible
}

// writeProxyRefreshReport writes the full refresh report (retrievedAt,
// sources, proxies) as indented JSON through a .tmp sibling plus rename,
// so a crash mid-write cannot corrupt a previously gathered file.
func writeProxyRefreshReport(path string, report proxyRefreshOutput) error {
	data, err := json.MarshalIndent(report, "", "  ")
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

func parsePort(value string) (int, error) {
	return strconv.Atoi(value)
}
