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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// probeConnectivityURL is the neutral endpoint every filter probe runs
// through a candidate's tunnel: a tiny HTTPS 204 request that requires a
// working CONNECT plus TLS but no credentials or content. Job-board
// destinations are never contacted to verify proxy candidates, so a
// filtered list proves transport health without touching the crawler's
// targets.
const probeConnectivityURL = "https://cp.cloudflare.com/generate_204"

// buildProbeClient creates the HTTP client used to probe one candidate
// route. Production uses the same per-peer transport as crawl runs.
type buildProbeClient func(proxyURL *url.URL, timeout time.Duration) *http.Client

func defaultProbeClient(proxyURL *url.URL, timeout time.Duration) *http.Client {
	return newPeerClient(proxyURL, timeout)
}

type proxyFilterConfig struct {
	rounds      int
	concurrency int
	timeout     time.Duration
	allRounds   bool // require every round to pass, not just one
	buildClient buildProbeClient
	probeURL    string // test seam; always probeConnectivityURL in production
}

func (cfg proxyFilterConfig) validate() error {
	if cfg.rounds < 1 || cfg.rounds > 10 {
		return errors.New("--rounds must be between 1 and 10")
	}
	if cfg.concurrency < 1 || cfg.concurrency > 512 {
		return errors.New("--concurrency must be between 1 and 512")
	}
	if cfg.timeout <= 0 {
		return errors.New("--timeout must be greater than zero")
	}
	if cfg.buildClient == nil {
		return errors.New("filter needs a probe client builder")
	}
	return nil
}

type proxyFilterResult struct {
	stats  proxySaveEntry
	source proxyListEntry
}

// filterProxies live-checks each eligible candidate with --rounds
// connection attempts through its own tunnel and splits the entries into
// measured survivors and dead candidates. Only a valid HTTP response in
// the 2xx/3xx range counts as success: intercepting or broken proxies
// fail with transport or status errors, exactly like they would during a
// discovery run. Survivors carry probe statistics and are ordered so the
// best proven routes load first with --proxies.
func filterProxies(
	ctx context.Context,
	entries []proxyListEntry,
	cfg proxyFilterConfig,
) ([]proxySaveEntry, []proxySaveEntry, error) {
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}

	// Probe with best-of-rounds by default: the loop breaks after the
	// first success, and only --all-rounds makes every round mandatory.
	// A status-based failure (like a proxy's own 500 CONNECT answer) and
	// a transport error are counted the same way; both poison a survivor.

	tests := make([]*proxyFilterResult, len(entries))
	jobs := make(chan int)
	var jobsWG sync.WaitGroup
	var mu sync.Mutex
	var drainErr error

	for worker := 0; worker < cfg.concurrency; worker++ {
		jobsWG.Add(1)
		go func() {
			defer jobsWG.Done()
			for index := range jobs {
				entry := entries[index]
				proxyURL, err := url.Parse(entry.Proxy)
				if err != nil {
					mu.Lock()
					drainErr = errors.Join(drainErr, fmt.Errorf("parse %s: %w", entry.Proxy, err))
					mu.Unlock()
					continue
				}
				client := cfg.buildClient(proxyURL, cfg.timeout)
				result := &proxyFilterResult{source: entry}
				for round := 0; round < cfg.rounds; round++ {
					if ctx.Err() != nil {
						break
					}
					started := time.Now()
					req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.probeURL, nil)
					if err != nil {
						break
					}
					res, requestErr := client.Do(req)
					elapsed := time.Since(started)
					success := false
					if requestErr == nil && res != nil {
						// Drain a little so keep-alive connections stay healthy.
						_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
						success = res.StatusCode >= http.StatusOK && res.StatusCode < 400
					}
					if res != nil && res.Body != nil {
						res.Body.Close()
					}
					if success {
						result.stats.Successes++
						result.stats.Requests++
						result.stats.ResponseMs += float64(elapsed.Microseconds()) / 1000
						if !cfg.allRounds {
							break
						}
						continue
					}
					result.stats.Failures++
					result.stats.Requests++
				}
				client.CloseIdleConnections()
				mu.Lock()
				tests[index] = result
				mu.Unlock()
			}
		}()
	}
	dispatchErr := func() error {
		defer close(jobs)
		for index := range entries {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}()
	jobsWG.Wait()

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if dispatchErr != nil {
		return nil, nil, dispatchErr
	}
	if drainErr != nil {
		return nil, nil, drainErr
	}

	var survivors, dead []proxySaveEntry
	for _, test := range tests {
		if test == nil {
			continue
		}
		entry := proxySaveEntry{
			Proxy:     test.source.Proxy,
			Protocol:  test.source.Protocol,
			Anonymity: test.source.Anonymity,
			Alive:     true,
			// Reuse the loader's sort key: observed successes rank the
			// strongest transports first on the next --proxies load.
			Uptime:    float64(test.stats.Successes),
			Requests:  test.stats.Requests,
			Successes: test.stats.Successes,
			Failures:  test.stats.Failures,
		}
		if entry.Successes > 0 {
			entry.ResponseMs = test.stats.ResponseMs / float64(entry.Successes)
		}
		if entry.Successes > 0 && (!cfg.allRounds || entry.Failures == 0) {
			survivors = append(survivors, entry)
			continue
		}
		// Dead entries keep their source metadata but are marked not
		// alive so a review file can never be loaded as a run list.
		entry.Alive = false
		entry.Uptime = test.source.Uptime
		entry.ResponseMs = 0
		dead = append(dead, entry)
	}

	sort.SliceStable(survivors, func(i, j int) bool {
		if survivors[i].Successes != survivors[j].Successes {
			return survivors[i].Successes > survivors[j].Successes
		}
		if survivors[i].ResponseMs != survivors[j].ResponseMs {
			return survivors[i].ResponseMs < survivors[j].ResponseMs
		}
		return survivors[i].Proxy < survivors[j].Proxy
	})
	sort.SliceStable(dead, func(i, j int) bool {
		return dead[i].Proxy < dead[j].Proxy
	})
	return survivors, dead, nil
}

func newProxyFilterCommand() *cobra.Command {
	return newProxyFilterCommandWithBuilder(defaultProbeClient, probeConnectivityURL)
}

func newProxyFilterCommandWithBuilder(build buildProbeClient, probeURL string) *cobra.Command {
	var (
		outputPath  string
		failuresPath string
		rounds      int
		concurrency int
		timeout     time.Duration
		allRounds   bool
		uniqueHosts bool
	)

	command := &cobra.Command{
		Use:   "filter <list-file-or-URL>",
		Short: "Live-check a proxy list and keep the routes that tunnel",
		Long: `Checks every eligible entry of a proxy list with real connection
attempts: each candidate gets --rounds HTTPS requests to a neutral
connectivity endpoint through its own CONNECT tunnel, so what the filter
verifies is exactly what a discovery run needs (a working tunnel plus
TLS). Survivors are written with measured attempt statistics, best
proven first, in the same JSON list shape that --proxies loads; dead
candidates can be written to a review file with --failures.

The check endpoint is a neutral connectivity probe, and job-board
destinations are never contacted for verification. Source-reported
"alive"/"elite" metadata is only used to select candidates. Only keep
and use proxies you are authorized to use.`,
		Example: `  # Verify a gathered list and keep a runs-ready file
  crawler proxy filter -o proven.json proxies.json

  # Survivors to stdout for piping; dead candidates to a review file
  crawler proxy filter --failures dead.json proxies.json > proven.json

  # Quick pass: one attempt per candidate at high parallelism
  crawler proxy filter --rounds 1 --concurrency 128 -o proven.json proxies.json
  crawler proxy filter --timeout 5s -o proven.json proxies.json`,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			input := args[0]
			cfg := proxyFilterConfig{
				rounds:      rounds,
				concurrency: concurrency,
				timeout:     timeout,
				allRounds:   allRounds,
				buildClient: build,
				probeURL:    probeURL,
			}
			if err := cfg.validate(); err != nil {
				return err
			}

			var entries []proxyListEntry
			var err error
			if isProxyListURL(input) {
				entries, err = fetchProxyListEntries(cmd.Context(), input, timeout)
			} else {
				entries, err = loadProxyListFile(input)
			}
			if err != nil {
				return err
			}
			candidates := eligibleProxyEntries(entries, uniqueHosts)
			if len(candidates) == 0 {
				return fmt.Errorf("no eligible proxy entries in %s", input)
			}

			survivors, dead, err := filterProxies(cmd.Context(), candidates, cfg)
			if err != nil {
				return err
			}

			if strings.TrimSpace(outputPath) != "" && outputPath != "-" {
				if err := writeProxyList(outputPath, survivors); err != nil {
					return err
				}
				log.Printf("saved %d verified proxies to %s; run with --proxies %s", len(survivors), outputPath, outputPath)
			} else {
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(survivors); err != nil {
					return err
				}
			}
			if strings.TrimSpace(failuresPath) != "" && failuresPath != "-" {
				if err := writeProxyList(failuresPath, dead); err != nil {
					return err
				}
				log.Printf("saved %d dead candidates for review to %s", len(dead), failuresPath)
			}
			log.Printf("filter checked %d candidates with %d attempt(s) each: %d usable, %d dead",
				len(candidates), rounds, len(survivors), len(dead))
			return nil
		},
	}
	command.Flags().StringVarP(&outputPath, "output", "o", "", "write survivors to this .json file instead of stdout")
	command.Flags().StringVar(&failuresPath, "failures", "", "write dead candidates to this .json file for review")
	command.Flags().IntVar(&rounds, "rounds", 2, "connection attempts per candidate (1-10)")
	command.Flags().IntVar(&concurrency, "concurrency", 64, "parallel probes, between 1 and 512")
	command.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "per-attempt timeout, e.g. 5s or 15s")
	command.Flags().BoolVar(&allRounds, "all-rounds", false, "survive only candidates that pass every attempt")
	command.Flags().BoolVar(&uniqueHosts, "unique-hosts", false, "keep only the best entry per host before checking")
	command.SetHelpFunc(func(command *cobra.Command, _ []string) {
		out := command.OutOrStdout()
		fmt.Fprintf(out, "%s\n\n%s\n\nUsage:\n  %s [flags]\n\nFlags:\n%s",
			command.Short, strings.TrimSpace(command.Long), command.CommandPath(), command.Flags().FlagUsages())
		if example := strings.Trim(command.Example, "\n"); example != "" {
			fmt.Fprintf(out, "\nExamples:\n%s\n", example)
		}
	})
	return command
}
