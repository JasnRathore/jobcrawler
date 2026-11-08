package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestRootCommandHelpAndNoArgumentDefaults(t *testing.T) {
	var calls int
	var got *cliOptions
	var gotProvider string
	root := newRootCommandWithRunner(func(opts *cliOptions, provider string) error {
		calls++
		got = opts
		gotProvider = provider
		return nil
	})

	var help bytes.Buffer
	root.SetOut(&help)
	root.SetErr(&help)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute help: %v", err)
	}
	if calls != 0 {
		t.Fatal("help unexpectedly ran discovery")
	}
	for _, want := range []string{
		"Usage:", "Provider selection:", "Company discovery:", "Request controls:",
		"Manual proxy routing:", "Other modes and browser:", "Legacy and disabled options:",
		"Commands:", "proxy", "Examples:", "crawler --greenhouse --words companies.txt",
		"crawler proxy refresh > proxies.json",
		"--words string", "--brute", "--greenhouse", "--collect-proxies",
		// Rotating pool guidance in the overview and examples.
		"Workers", "rotate across", "--proxy-failures",
		"--proxies proxies.json --proxy-save proven.json",
		"Refresh jobs for every discovered company",
	} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help output missing %q", want)
		}
	}
	for _, group := range cliHelpFlagGroups {
		for _, name := range group.names {
			if root.Flags().Lookup(name) == nil {
				t.Errorf("help group %q refers to unregistered flag %q", group.title, name)
			}
			if !strings.Contains(help.String(), "--"+name) {
				t.Errorf("help output is missing categorized flag --%s", name)
			}
		}
	}
	for _, line := range strings.Split(help.String(), "\n") {
		if len(line) > 80 {
			t.Errorf("help line exceeds 80 columns (%d): %q", len(line), line)
		}
	}

	root = newRootCommandWithRunner(func(opts *cliOptions, provider string) error {
		calls++
		got = opts
		gotProvider = provider
		return nil
	})
	root.SetArgs(nil)
	if err := root.Execute(); err != nil {
		t.Fatalf("execute no-argument discovery: %v", err)
	}
	if calls != 1 {
		t.Fatalf("runner called %d times, want 1", calls)
	}
	if gotProvider != providerGreenhouse {
		t.Errorf("default provider = %q, want %q", gotProvider, providerGreenhouse)
	}
	if got.words != "" || got.brute || got.bench || got.update || got.browse {
		t.Errorf("no-argument invocation unexpectedly enabled a mode: %+v", got)
	}
	if got.workers != 64 || got.rps != 50 || got.proxiesPath != "off" {
		t.Errorf("unexpected CLI defaults: workers=%d rps=%g proxies=%q", got.workers, got.rps, got.proxiesPath)
	}
}

func TestProxyRefreshCommandHelpDocumentsBuiltInSourcesAndNoRouting(t *testing.T) {
	root := newRootCommand()
	var help bytes.Buffer
	root.SetOut(&help)
	root.SetErr(&help)
	root.SetArgs([]string{"proxy", "refresh", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"HProxy", "Proxifly", "ProxyScrape", "Geonode", "--source", "--limit",
		"stdout", "not probed", "Destination rate limits",
		// Storage and reuse guidance.
		"Storage and reuse", "--proxies FILE", "--proxy-save", "Examples:",
	} {
		if !strings.Contains(help.String(), expected) {
			t.Errorf("proxy refresh help missing %q:\n%s", expected, help.String())
		}
	}
}

func TestRootCommandProviderValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "words requires explicit provider",
			args: []string{"--words", "companies.txt"},
			want: "--words requires an explicit provider flag",
		},
		{
			name: "only one provider",
			args: []string{"--greenhouse", "--lever"},
			want: "choose only one provider flag",
		},
		{
			name: "direct fallback disabled",
			args: []string{"--allow-direct-fallback"},
			want: "--allow-direct-fallback is disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCommandWithRunner(func(*cliOptions, string) error {
				t.Fatal("runner called despite invalid options")
				return nil
			})
			root.SetArgs(tt.args)
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("execute error = %v, want substring %q", err, tt.want)
			}
		})
	}

	root := newRootCommandWithRunner(func(_ *cliOptions, provider string) error {
		if provider != providerLever {
			t.Errorf("provider = %q, want %q", provider, providerLever)
		}
		return nil
	})
	root.SetArgs([]string{"--words", "companies.txt", "--lever"})
	if err := root.Execute(); err != nil {
		t.Fatalf("valid words invocation: %v", err)
	}
}

func TestRootCommandOptionsDoNotLeakBetweenInvocations(t *testing.T) {
	first := newRootCommandWithRunner(func(opts *cliOptions, _ string) error {
		if !opts.brute || opts.minSlugLength != 6 || opts.maxSlugLength != 6 {
			t.Errorf("first invocation options = %+v", opts)
		}
		return nil
	})
	first.SetArgs([]string{"--brute", "--min", "6", "--max", "6"})
	if err := first.Execute(); err != nil {
		t.Fatalf("execute first invocation: %v", err)
	}

	second := newRootCommandWithRunner(func(opts *cliOptions, _ string) error {
		if opts.brute || opts.minSlugLength != 4 || opts.maxSlugLength != 5 {
			t.Errorf("second invocation inherited state: %+v", opts)
		}
		return nil
	})
	second.SetArgs(nil)
	if err := second.Execute(); err != nil {
		t.Fatalf("execute second invocation: %v", err)
	}
}

func TestLegacyCollectProxiesDoesNotProbeTargets(t *testing.T) {
	root := newRootCommand()
	root.SetArgs([]string{"--collect-proxies", "proxies.json", "--proxy-sources", "input.json"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "legacy --collect-proxies mode is disabled") ||
		!strings.Contains(err.Error(), "job-board endpoints") {
		t.Fatalf("legacy collection error = %v, want explicit safety/migration error", err)
	}
}

func TestProviderStopAfterWordFlushPreventsBrute(t *testing.T) {
	stopErr := errors.Join(errors.New("provider stopped"), errProviderStopped)
	bruteStarted := false
	err := runBruteUnlessStopped(true, nil, stopErr, func() error {
		bruteStarted = true
		return nil
	})

	if bruteStarted {
		t.Fatal("brute force started after a provider stop")
	}
	if !errors.Is(err, errProviderStopped) {
		t.Fatalf("returned error = %v, want provider stop", err)
	}
}
