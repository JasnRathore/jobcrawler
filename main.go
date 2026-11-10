package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	"db"
)

const baseURL = "https://boards-api.greenhouse.io/v1/boards/%s"

var (
	alphabet = "abcdefghijklmnopqrstuvwxyz"
	nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)
)

type cliOptions struct {
	words               string
	brute               bool
	bench               bool
	providerFlags       map[string]*bool
	resume              bool
	update              bool
	updateJobs          bool
	dashboard           bool
	dashboardAddress    string
	workers             int
	rps                 float64
	requestTimeout      time.Duration
	cooldown            time.Duration
	proxiesPath         string
	proxyReuseGap       time.Duration
	proxyFailures       int
	proxyProbeTimeout   time.Duration
	proxySavePath       string
	allowDirectFallback bool
	collectPath         string
	proxySources        string
	collectIterations   int
	collectUniqueHosts  bool
	minSlugLength       int
	maxSlugLength       int
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	return newRootCommand().Execute()
}

func newRootCommand() *cobra.Command {
	return newRootCommandWithRunner(executeCLI)
}

func newRootCommandWithRunner(runner func(*cliOptions, string) error) *cobra.Command {
	opts := &cliOptions{
		dashboardAddress:  "127.0.0.1:8080",
		workers:           64,
		rps:               50,
		requestTimeout:    10 * time.Second,
		cooldown:          2 * time.Minute,
		proxiesPath:       "off",
		proxyFailures:     5,
		proxyProbeTimeout: 3 * time.Second,
		collectIterations: 2,
		minSlugLength:     4,
		maxSlugLength:     5,
		providerFlags:     make(map[string]*bool),
	}

	root := &cobra.Command{
		Use:   "crawler",
		Short: "Discover and update job-board data",
		Long: `Crawler checks company slugs against applicant-tracking (ATS) providers,
saves discovered boards to the local database, refreshes their job
listings, and serves a dashboard for searching and bookmarking those jobs.

Pick exactly one provider with --greenhouse (default), --lever,
--levereu, --workable, or --ashbyhq, and run one work mode at a time:

  --words FILE     check slugs generated from a company-name list
  --brute          check every lowercase slug in a length range
  --update --jobs  refresh stored listings for every known company
  --dashboard      serve the local dashboard for indexed jobs

Word-list candidates are checked before brute-force candidates, and both
honor --workers, --rps, and --timeout. A rate limit (429, or Greenhouse
403/406) cools the route that received it — retrying on another healthy
route — and, once many routes hit it, pauses the provider briefly; an
access-control or recognized challenge response still stops the provider
for the run. Brute-force checkpoints stay resumable with --resume.

Proxy routing is opt-in: pass --proxies FILE (or an HTTPS list URL) with
a list you collected, and every healthy entry becomes a route. Workers
rotate across the following routes (least-recently-used first), routes
that keep failing are evicted after --proxy-failures consecutive
failures, and the run stops when no route remains instead of falling
back to your direct connection. Use "crawler proxy refresh" to gather
the four built-in public lists into one reviewable file for --proxies,
or supply your own list. Use "crawler proxy filter" to live-check a
list against a neutral endpoint and keep only routes that tunnel.
Only send traffic you are authorized to send,
and keep brute-force ranges small. See README.md for workflows, the
proxy-list format, and safety notes.`,
		Example: `  # Discover company boards from a word list
  crawler --greenhouse --words companies.txt

  # Route the run through a rotating proxy pool from a list you collected
  crawler --greenhouse --words companies.txt --proxies proxies.json

  # Keep the routes that proved healthy, then reuse them later
  crawler --brute --max 1 --proxies proxies.json --proxy-save proven.json

  # Gather the four built-in public proxy lists into one file
  crawler proxy refresh > proxies.json

  # Live-check a gathered list; survivors are in the same JSON shape
  crawler proxy filter -o proven.json proxies.json

  # Try a deliberately small slug range and checkpoint progress for later
  crawler --lever --brute --min 1 --max 1 --resume

  # Refresh jobs for every discovered company across all providers
  crawler --update --jobs

  # Open the dashboard and bookmark indexed jobs
  crawler --dashboard`,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			provider, err := validateCLIOptions(opts)
			if err != nil {
				return err
			}
			return runner(opts, provider)
		},
	}
	flags := root.Flags()
	flags.StringVar(&opts.words, "words", "", "file with company names or slugs, one per line (generates variants)")
	flags.BoolVar(&opts.brute, "brute", false, "also brute-force slugs in the --min/--max length range (low priority)")
	flags.BoolVar(&opts.bench, "bench", false, "benchmark requests without modifying local.db")
	for _, name := range registeredProviders() {
		help := "select " + providerDisplayName(name) + " for company discovery"
		if name == providerGreenhouse {
			help += " (default)"
		}
		selected := false
		opts.providerFlags[name] = &selected
		flags.BoolVar(opts.providerFlags[name], name, false, help)
	}
	flags.BoolVar(&opts.resume, "resume", false, "resume or checkpoint brute-force progress (requires --brute)")
	flags.BoolVar(&opts.update, "update", false, "update indexed data (always pair with --jobs)")
	flags.BoolVar(&opts.updateJobs, "jobs", false, "fetch and refresh jobs for all known companies (requires --update)")
	flags.BoolVar(&opts.dashboard, "dashboard", false, "launch the local job dashboard")
	flags.BoolVar(&opts.dashboard, "browse", false, "deprecated alias for --dashboard")
	flags.Lookup("browse").Deprecated = "use --dashboard"
	flags.StringVar(&opts.dashboardAddress, "addr", opts.dashboardAddress, "dashboard listen address, for example 127.0.0.1:9090")
	flags.IntVar(&opts.workers, "workers", opts.workers, "concurrent workers shared by all request modes")
	flags.Float64Var(&opts.rps, "rps", opts.rps, "maximum aggregate requests per second across all workers")
	flags.DurationVar(&opts.requestTimeout, "timeout", opts.requestTimeout, "per-request HTTP timeout (for example 10s or 1m)")
	flags.DurationVar(&opts.cooldown, "cooldown", opts.cooldown, "caps cooling and the provider pause after repeated rate limits; 401 stops")
	flags.StringVar(&opts.proxiesPath, "proxies", opts.proxiesPath, "JSON proxy list file or HTTPS URL whose healthy entries rotate; \"off\" uses direct egress")
	flags.DurationVar(&opts.proxyReuseGap, "proxy-reuse-min", 0, "minimum gap before one proxy may handle another request (applies per route)")
	flags.IntVar(&opts.proxyFailures, "proxy-failures", opts.proxyFailures, "evict a proxy from the rotation after this many consecutive failed requests")
	flags.DurationVar(&opts.proxyProbeTimeout, "proxy-probe-timeout", opts.proxyProbeTimeout, "deprecated; proxy health probes are disabled")
	flags.StringVar(&opts.proxySavePath, "proxy-save", "", "write surviving proxies and their run stats to this JSON file for reuse with --proxies")
	flags.BoolVar(&opts.allowDirectFallback, "allow-direct-fallback", false, "deprecated; direct egress is selected with --proxies off")
	flags.StringVar(&opts.collectPath, "collect-proxies", "", "disabled legacy option; proxy collection no longer probes job-board endpoints")
	flags.StringVar(&opts.proxySources, "proxy-sources", "", "deprecated legacy collection input; collection mode is disabled")
	flags.IntVar(&opts.collectIterations, "collect-iterations", opts.collectIterations, "deprecated legacy collection option")
	flags.BoolVar(&opts.collectUniqueHosts, "collect-unique-hosts", false, "deprecated legacy collection option")
	flags.IntVar(&opts.minSlugLength, "min", opts.minSlugLength, "minimum brute-force slug length")
	flags.IntVar(&opts.maxSlugLength, "max", opts.maxSlugLength, "maximum brute-force slug length (13 technical maximum)")

	for name, message := range map[string]string{
		"cooldown":              "caps rate-limit cooling and pause; 401 still stops",
		"proxy-probe-timeout":   "proxy health probes have been removed",
		"allow-direct-fallback": "direct egress is chosen with --proxies off; rotation stays inside the supplied list",
		"proxy-sources":         "legacy collection mode is disabled",
		"collect-iterations":    "legacy collection mode is disabled",
		"collect-unique-hosts":  "legacy collection mode is disabled",
	} {
		flags.Lookup(name).Deprecated = message
	}

	root.SetHelpFunc(writeRootHelp)
	root.AddCommand(newProxyCommand())

	return root
}

type cliHelpFlagGroup struct {
	title string
	names []string
}

var cliHelpFlagGroups = []cliHelpFlagGroup{
	{title: "Provider selection", names: []string{"greenhouse", "lever", "levereu", "workable", "ashbyhq"}},
	{title: "Company discovery", names: []string{"words", "brute", "min", "max", "resume"}},
	{title: "Request controls", names: []string{"workers", "rps", "timeout"}},
	{title: "Manual proxy routing", names: []string{"proxies", "proxy-reuse-min", "proxy-failures", "proxy-save"}},
	{title: "Other modes and dashboard", names: []string{"bench", "update", "jobs", "dashboard", "addr"}},
	{title: "Legacy and disabled options", names: []string{
		"cooldown", "proxy-probe-timeout", "allow-direct-fallback", "collect-proxies",
		"proxy-sources", "collect-iterations", "collect-unique-hosts",
	}},
	{title: "Help", names: []string{"help"}},
}

func writeRootHelp(cmd *cobra.Command, _ []string) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "%s\n\n%s\n\nUsage:\n  %s [flags]\n", cmd.Short, strings.TrimSpace(cmd.Long), cmd.CommandPath())
	for _, group := range cliHelpFlagGroups {
		writeCLIHelpFlagGroup(out, cmd, group)
	}
	var commands []*cobra.Command
	for _, subcommand := range cmd.Commands() {
		if !subcommand.Hidden && subcommand.Name() != "help" && subcommand.Name() != "completion" {
			commands = append(commands, subcommand)
		}
	}
	if len(commands) > 0 {
		fmt.Fprintln(out, "\nCommands:")
		for _, subcommand := range commands {
			fmt.Fprintf(out, "  %-12s %s\n", subcommand.Name(), subcommand.Short)
		}
	}
	if example := strings.TrimSpace(cmd.Example); example != "" {
		fmt.Fprintf(out, "\nExamples:\n%s\n", strings.TrimRight(cmd.Example, "\n"))
	}
}

func writeCLIHelpFlagGroup(out io.Writer, cmd *cobra.Command, group cliHelpFlagGroup) {
	type helpEntry struct {
		label       string
		description string
	}
	entries := make([]helpEntry, 0, len(group.names))
	labelWidth := 0
	for _, name := range group.names {
		flag := cmd.Flags().Lookup(name)
		if flag == nil || flag.Hidden {
			continue
		}
		label := "--" + flag.Name
		if flag.Shorthand != "" {
			label = "-" + flag.Shorthand + ", " + label
		}
		if flag.Value.Type() != "bool" {
			label += " " + flag.Value.Type()
		}
		description := flag.Usage
		if flag.Value.Type() != "bool" && flag.DefValue != "" {
			description += fmt.Sprintf(" (default %q)", flag.DefValue)
		}
		if flag.Deprecated != "" {
			description += " (DEPRECATED: " + flag.Deprecated + ")"
		}
		entries = append(entries, helpEntry{label: label, description: description})
		if len(label) > labelWidth {
			labelWidth = len(label)
		}
	}
	if len(entries) == 0 {
		return
	}

	fmt.Fprintf(out, "\n%s:\n", group.title)
	descriptionWidth := 80 - labelWidth - 4
	for _, entry := range entries {
		lines := wrapCLIHelpText(entry.description, descriptionWidth)
		fmt.Fprintf(out, "  %-*s  %s\n", labelWidth, entry.label, lines[0])
		for _, line := range lines[1:] {
			fmt.Fprintf(out, "  %*s  %s\n", labelWidth, "", line)
		}
	}
}

func wrapCLIHelpText(text string, width int) []string {
	words := strings.Fields(text)
	lines := make([]string, 0, 1)
	var line string
	for _, word := range words {
		if line == "" {
			line = word
			continue
		}
		if len(line)+1+len(word) > width {
			lines = append(lines, line)
			line = word
			continue
		}
		line += " " + word
	}
	if line != "" {
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return []string{""}
	}
	return lines
}

func validateCLIOptions(opts *cliOptions) (string, error) {
	minSlugLength, maxSlugLength := opts.minSlugLength, opts.maxSlugLength
	if minSlugLength < 1 || maxSlugLength < minSlugLength || (opts.brute && maxSlugLength > 13) {
		return "", fmt.Errorf("invalid length range: min=%d max=%d", minSlugLength, maxSlugLength)
	}
	if opts.workers < 1 {
		return "", errors.New("workers must be at least 1")
	}
	if opts.rps <= 0 {
		return "", errors.New("rps must be greater than 0")
	}
	if opts.requestTimeout <= 0 {
		return "", errors.New("timeout must be greater than 0")
	}
	if opts.cooldown < 0 {
		return "", errors.New("cooldown must not be negative")
	}
	if opts.proxiesPath == "" {
		return "", errors.New("proxies must be \"off\" or a proxy list file path")
	}
	if opts.allowDirectFallback {
		return "", errors.New("--allow-direct-fallback is disabled; select direct mode before the run with --proxies off")
	}
	if opts.proxyFailures < 1 {
		return "", errors.New("proxy failures must be at least 1")
	}
	if opts.proxyProbeTimeout <= 0 {
		return "", errors.New("proxy probe timeout must be greater than 0")
	}
	selectedPortals := 0
	selectedProvider := ""
	for name, selected := range opts.providerFlags {
		if *selected {
			selectedPortals++
			selectedProvider = name
		}
	}
	if selectedPortals > 1 {
		return "", errors.New("choose only one provider flag")
	}
	provider := providerGreenhouse
	if selectedProvider != "" {
		provider = selectedProvider
	}
	if opts.words != "" && selectedProvider == "" {
		options := make([]string, 0)
		for _, name := range registeredProviders() {
			options = append(options, "--"+name)
		}
		return "", fmt.Errorf("--words requires an explicit provider flag: %s", strings.Join(options, ", "))
	}
	if opts.resume && (!opts.brute || opts.bench || opts.words != "" || opts.dashboard || opts.update) {
		return "", errors.New("--resume requires --brute and cannot be combined with --bench, --words, --dashboard, or --update")
	}
	if opts.update != opts.updateJobs {
		return "", errors.New("--update and --jobs must be used together")
	}
	if opts.bench && opts.update {
		return "", errors.New("--bench cannot be combined with --update or --jobs")
	}
	if opts.dashboard && (opts.bench || opts.update) {
		return "", errors.New("--dashboard cannot be combined with --bench or --update")
	}
	if opts.brute && maxSlugLength >= 6 {
		log.Printf("warning: brute force up to length %d means %d candidates",
			maxSlugLength, bruteCount(minSlugLength, maxSlugLength))
	}
	if opts.collectPath != "" {
		if opts.brute || opts.words != "" || opts.bench || opts.update || opts.dashboard || opts.resume {
			return "", errors.New("--collect-proxies cannot be combined with --brute, --words, --bench, --update/--jobs, --dashboard, or --resume")
		}
		if opts.collectIterations < 1 {
			return "", errors.New("collect iterations must be at least 1")
		}
	}
	return provider, nil
}

func executeCLI(opts *cliOptions, provider string) (returnErr error) {
	if opts.collectPath != "" {
		return errors.New("legacy --collect-proxies mode is disabled because it probes candidates against job-board endpoints")
	}

	words := &opts.words
	brute := &opts.brute
	bench := &opts.bench
	resume := &opts.resume
	update := &opts.update
	updateJobsFlag := &opts.updateJobs
	dashboard := &opts.dashboard
	dashboardAddress := &opts.dashboardAddress
	workers := &opts.workers
	rps := &opts.rps
	requestTimeout := &opts.requestTimeout
	cooldown := &opts.cooldown
	proxiesPath := &opts.proxiesPath
	proxyReuseGap := &opts.proxyReuseGap
	proxyFailures := &opts.proxyFailures
	proxyProbeTimeout := &opts.proxyProbeTimeout
	proxySavePath := &opts.proxySavePath
	minSlugLength := &opts.minSlugLength
	maxSlugLength := &opts.maxSlugLength

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The first interrupt unwinds gracefully through checkpoints and
	// result saving; a second one force-quits in case shutdown stalls.
	// signal.Notify fans every press out to all registrations, so count
	// them to keep the first press graceful.
	var interrupts int32
	quit := make(chan os.Signal, 4)
	signal.Notify(quit, os.Interrupt)
	go func() {
		for range quit {
			if atomic.AddInt32(&interrupts, 1) == 2 {
				log.Printf("second interrupt; exiting immediately")
				os.Exit(130)
			}
		}
	}()

	var (
		database       *db.DB
		err            error
		benchmarkClean func()
	)
	if *bench {
		database, benchmarkClean, err = newBenchmarkDatabase(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if benchmarkClean != nil {
				benchmarkClean()
			}
		}()
	} else {
		config, configErr := loadDatabaseConfig(".env")
		if configErr != nil {
			return fmt.Errorf("load database configuration: %w", configErr)
		}
		if config.mode == "remote" {
			database, err = db.NewRemote(ctx, config.tursoURL, config.tursoToken)
			if err == nil {
				log.Print("connected directly to remote Turso database")
			}
		} else {
			database, err = db.New(ctx, config.localPath)
		}
		if err != nil {
			return err
		}
		defer func() {
			if err := database.Close(); err != nil {
				returnErr = errors.Join(returnErr, err)
			}
		}()
	}

	client := newClient(*workers, *requestTimeout)
	burst := int(*rps)
	if burst < 1 {
		burst = 1
	}
	limiter := newRequestLimiter(rate.NewLimiter(rate.Limit(*rps), burst), *cooldown)
	var proxies *proxyPool
	if *dashboard {
		// The dashboard serves local data; no remote requests, no pool needed.
		if *proxiesPath != "off" {
			log.Printf("--proxies has no effect with --dashboard")
		}
		if *proxySavePath != "" {
			log.Printf("--proxy-save has no effect with --dashboard")
		}
	} else if *proxiesPath != "off" {
		// Select one manual route without probing job-board endpoints. Its
		// transport health is learned only from the run's ordinary requests.
		proxies, err = loadProxyPool(ctx, *proxiesPath, proxyPoolConfig{
			minReuseGap:     *proxyReuseGap,
			maxFailures:     *proxyFailures,
			retryAfterFloor: time.Second,
			maxCooldown:     *cooldown,
		}, *proxyProbeTimeout, *requestTimeout, "")
		if err != nil {
			return err
		}
		if *proxySavePath != "" {
			// Periodic flush catches a hard crash; the deferred save
			// covers both normal completion and Ctrl+C (ctx cancellation
			// unwinds this function, so the defer runs before exit).
			defer saveProxyList(proxies, *proxySavePath)
			go flushProxyList(ctx, proxies, *proxySavePath)
		}
	} else {
		if *proxySavePath != "" {
			log.Printf("--proxy-save has no effect without --proxies")
		}
		if *proxyReuseGap != 0 {
			log.Printf("--proxy-reuse-min has no effect without --proxies")
		}
	}
	var stats *benchmarkStats
	if *bench {
		stats = newBenchmarkStats()
	}
	if *dashboard {
		if err := serveDashboard(ctx, database, *dashboardAddress); err != nil {
			return err
		}
		return nil
	}
	if *update && *updateJobsFlag {
		if err := updateAllJobs(ctx, database, client, proxies, limiter, *workers); err != nil {
			return err
		}
		return nil
	}

	pool := newCheckPool(ctx, client, proxies, limiter, database, stats, provider, *workers)

	seen := make(map[string]struct{})
	emit := func(slug string) bool {
		if len(slug) < *minSlugLength || len(slug) > *maxSlugLength {
			return true // out of range, skip but keep going
		}
		if _, ok := seen[slug]; ok {
			return true
		}
		_, submitted := pool.submit(slug)
		if submitted {
			seen[slug] = struct{}{}
			return true
		}
		return false
	}

	started := time.Now()
	if *words != "" {
		if err := feedWordlist(*words, emit); err != nil {
			log.Printf("wordlist: %v", err)
		}
	}
	pool.flush()
	providerStopErr := pool.providerStop()
	var runErr error
	if *words != "" {
		runErr = saveCheckFailures(pool, "failed.txt")
	}
	runErr = runBruteUnlessStopped(*brute, runErr, providerStopErr, func() error {
		return runBruteForce(ctx, pool, seen, database, *minSlugLength, *maxSlugLength, *resume, "failed.txt")
	})

	pool.close()
	// Interrupted runs leave checks whose results were only collected by
	// close(); persist them so failed.txt stays complete.
	if err := saveCheckFailures(pool, "failed.txt"); err != nil {
		log.Printf("save check failures: %v", err)
	}
	if stopErr := pool.providerStop(); errors.Is(stopErr, errProviderStopped) {
		runErr = stopErr
	}
	if stats != nil {
		stats.print(time.Since(started))
	}
	if runErr != nil {
		return runErr
	}
	return nil
}

func runBruteUnlessStopped(brute bool, runErr, providerStopErr error, runBrute func() error) error {
	if errors.Is(providerStopErr, errProviderStopped) {
		return providerStopErr
	}
	if brute && runErr == nil {
		return runBrute()
	}
	return runErr
}

// bruteCount returns how many candidates brute force would generate.
func bruteCount(min, max int) int64 {
	total := int64(0)
	for l := min; l <= max; l++ {
		n := int64(1)
		for i := 0; i < l; i++ {
			n *= int64(len(alphabet))
		}
		total += n
	}
	return total
}

func newBenchmarkDatabase(ctx context.Context) (*db.DB, func(), error) {
	dir, err := os.MkdirTemp("", "crawler-benchmark-")
	if err != nil {
		return nil, nil, fmt.Errorf("create temporary benchmark database directory: %w", err)
	}
	database, err := db.New(ctx, filepath.Join(dir, "benchmark.db"))
	if err != nil {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			return nil, nil, fmt.Errorf("open temporary benchmark database: %w (cleanup failed: %v)", err, removeErr)
		}
		return nil, nil, fmt.Errorf("open temporary benchmark database: %w", err)
	}
	cleanup := func() {
		if err := database.Close(); err != nil {
			log.Printf("close temporary benchmark database: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("remove temporary benchmark database: %v", err)
		}
	}
	return database, cleanup, nil
}

func newClient(workers int, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        workers * 2,
		MaxIdleConnsPerHost: workers * 2,
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

// buildProxySources resolves the candidate sources for --collect-proxies:
// the --proxy-sources list wins, otherwise --proxies is used. Each
// element is a file path or an http(s) URL.
func buildProxySources(proxySources, proxiesPath string) ([]string, error) {
	raw := proxySources
	if raw == "" {
		if proxiesPath == "off" {
			return nil, errors.New("--collect-proxies needs --proxy-sources or --proxies pointing at candidate lists")
		}
		raw = proxiesPath
	}
	sources := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			sources = append(sources, part)
		}
	}
	if len(sources) == 0 {
		return nil, errors.New("proxy sources must be comma-separated files or URLs")
	}
	return sources, nil
}

func feedWordlist(path string, emit func(string) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		for _, v := range variants(sc.Text()) {
			if !emit(v) {
				return nil
			}
		}
	}
	return sc.Err()
}

// variants turns "Acme Corp, Inc." into likely slugs.
func variants(name string) []string {
	n := strings.ToLower(strings.TrimSpace(name))
	for _, suf := range []string{", inc.", ", inc", " inc.", " inc", " llc", " ltd", " corp", " corporation", " co"} {
		n = strings.TrimSuffix(n, suf)
	}
	joined := nonAlnum.ReplaceAllString(n, "")
	hyphen := strings.Trim(nonAlnum.ReplaceAllString(n, "-"), "-")
	if joined == "" {
		return nil
	}
	return []string{joined, hyphen, joined + "hq", joined + "inc", joined + "careers"}
}

func checkCompany(
	ctx context.Context,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	database *db.DB,
	slug string,
	stats *benchmarkStats,
) error {
	return checkProviderCompany(ctx, client, pool, limiter, database, providerGreenhouse, slug, stats)
}

func checkGreenhouseCompany(
	ctx context.Context,
	client *http.Client,
	pool *proxyPool,
	limiter *requestLimiter,
	database *db.DB,
	slug string,
	stats *benchmarkStats,
) error {
	return checkProviderCompany(ctx, client, pool, limiter, database, providerGreenhouse, slug, stats)
}

func logFoundCompany(provider, slug string) {
	log.Printf("Found %s company: %s", provider, slug)
}

var benchmarkLatencyBuckets = []time.Duration{
	time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	8 * time.Second,
}

type benchmarkStats struct {
	mu             sync.Mutex
	requests       uint64
	retries        uint64
	transportError uint64
	ok             uint64
	notFound       uint64
	rateLimited    uint64
	serverError    uint64
	otherStatus    uint64
	checks         uint64
	checkFailures  uint64
	totalLatency   time.Duration
	minLatency     time.Duration
	maxLatency     time.Duration
	latencyBuckets []uint64
}

func newBenchmarkStats() *benchmarkStats {
	return &benchmarkStats{latencyBuckets: make([]uint64, len(benchmarkLatencyBuckets)+1)}
}

func (s *benchmarkStats) recordRequest(latency time.Duration, status int, transportError, retry bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests++
	if retry {
		s.retries++
	}
	if transportError {
		s.transportError++
	} else {
		switch {
		case status >= 200 && status < 300:
			s.ok++
		case status == http.StatusNotFound:
			s.notFound++
		case status == http.StatusTooManyRequests:
			s.rateLimited++
		case status >= 500:
			s.serverError++
		default:
			s.otherStatus++
		}
	}

	s.totalLatency += latency
	if s.requests == 1 || latency < s.minLatency {
		s.minLatency = latency
	}
	if latency > s.maxLatency {
		s.maxLatency = latency
	}
	bucket := len(benchmarkLatencyBuckets)
	for i, upperBound := range benchmarkLatencyBuckets {
		if latency <= upperBound {
			bucket = i
			break
		}
	}
	s.latencyBuckets[bucket]++
}

func (s *benchmarkStats) recordCheck(failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checks++
	if failed {
		s.checkFailures++
	}
}

func (s *benchmarkStats) print(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	requestsPerSecond := float64(0)
	if elapsed > 0 {
		requestsPerSecond = float64(s.requests) / elapsed.Seconds()
	}
	average := time.Duration(0)
	if s.requests > 0 {
		average = s.totalLatency / time.Duration(s.requests)
	}

	log.Printf("Benchmark results:")
	log.Printf("  Duration: %s", elapsed.Round(time.Millisecond))
	log.Printf("  Candidate checks: %d (%d failed)", s.checks, s.checkFailures)
	log.Printf("  Requests: %d (%d retries), %.2f requests/sec", s.requests, s.retries, requestsPerSecond)
	log.Printf("  HTTP responses: 2xx=%d 404=%d 429=%d 5xx=%d other=%d transport-errors=%d",
		s.ok, s.notFound, s.rateLimited, s.serverError, s.otherStatus, s.transportError)
	log.Printf("  Latency: min=%s avg=%s p50=%s p95=%s p99=%s max=%s",
		s.minLatency, average, s.percentile(0.50), s.percentile(0.95), s.percentile(0.99), s.maxLatency)
}

func (s *benchmarkStats) percentile(percent float64) string {
	if s.requests == 0 {
		return "n/a"
	}

	target := uint64(float64(s.requests)*percent + 0.999999)
	var cumulative uint64
	for i, count := range s.latencyBuckets {
		cumulative += count
		if cumulative >= target {
			if i == len(benchmarkLatencyBuckets) {
				return ">" + benchmarkLatencyBuckets[i-1].String()
			}
			return "<=" + benchmarkLatencyBuckets[i].String()
		}
	}
	return "n/a"
}
