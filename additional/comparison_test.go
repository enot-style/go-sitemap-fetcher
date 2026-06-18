//go:build integration

package additional

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	mega "github.com/MegaBytee/sitemap-go"
	"github.com/MegaBytee/sitemap-go/config"
	aafeher "github.com/aafeher/go-sitemap-parser"
	gositemapfetcher "github.com/enot-style/go-sitemap-fetcher"
	gopher "github.com/mrehanabbasi/gopher-parse-sitemap"
)

func TestComparison_RealWebsites(t *testing.T) {
	if os.Getenv("GO_SITEMAP_FETCHER_INTEGRATION") == "" {
		t.Skip("set GO_SITEMAP_FETCHER_INTEGRATION=1 to run")
	}

	var summaries []comparisonSummary
	t.Cleanup(func() {
		printComparisonSummary(t, summaries)
	})

	sites := []string{
		"https://www.apple.com/sitemap.xml",
		"https://www.jetbrains.com/sitemap.xml",
		"https://www.djangoproject.com/sitemap.xml",
	}

	for _, site := range sites {
		site := site
		t.Run(site, func(t *testing.T) {
			ours, metrics, err := measureFetch(func() (map[string]struct{}, error) {
				return fetchWithFetcher(site)
			})
			if err != nil {
				t.Fatalf("fetcher failed: %v", err)
			}
			summaries = append(summaries, comparisonSummary{
				Site:    site,
				Tool:    "★go-sitemap-fetcher",
				URLs:    len(ours),
				Metrics: metrics,
			})

			parserURLs, metrics, err := measureFetch(func() (map[string]struct{}, error) {
				return fetchWithAafeher(site)
			})
			if err != nil {
				t.Fatalf("go-sitemap-parser failed: %v", err)
			}
			recordComparison(t, &summaries, site, "go-sitemap-parser", ours, parserURLs, metrics)

			gopherURLs, metrics, err := measureFetch(func() (map[string]struct{}, error) {
				return fetchWithGopher(site)
			})
			if err != nil {
				t.Fatalf("gopher-parse-sitemap failed: %v", err)
			}
			recordComparison(t, &summaries, site, "gopher-parse-sitemap", ours, gopherURLs, metrics)

			megaURLs, metrics, err := measureFetch(func() (map[string]struct{}, error) {
				return fetchWithMega(site)
			})
			if err != nil {
				t.Fatalf("sitemap-go failed: %v", err)
			}
			recordComparison(t, &summaries, site, "sitemap-go", ours, megaURLs, metrics)
		})
	}
}

type comparisonSummary struct {
	Site    string
	Tool    string
	URLs    int
	Missing int
	Extra   int
	Metrics fetchMetrics
}

type fetchMetrics struct {
	Elapsed     time.Duration
	AllocKB     uint64
	HeapInuseKB int64
	SysKB       int64
	HeapObjects uint64
}

func fetchWithFetcher(site string) (map[string]struct{}, error) {
	parsed, err := url.Parse(site)
	if err != nil {
		return nil, err
	}
	fetcher := gositemapfetcher.New(gositemapfetcher.Options{
		UserAgent:         "go-sitemap-fetcher/compare",
		PerRequestTimeout: 15 * time.Second,
		SkipNon200:        false,
	})
	results := make(map[string]struct{})
	err = fetcher.Walk(context.Background(), parsed, func(item gositemapfetcher.Item) error {
		loc := normalizeURLString(item.Loc.String())
		if loc != "" {
			results[loc] = struct{}{}
		}
		return nil
	})
	if err != nil {
		var maxErr *gositemapfetcher.ErrMaxURLs
		if !errors.As(err, &maxErr) {
			return nil, err
		}
	}
	return results, nil
}

func fetchWithAafeher(site string) (map[string]struct{}, error) {
	parser := aafeher.New()
	parsed, err := parser.Parse(site, nil)
	if err != nil {
		return nil, err
	}
	results := make(map[string]struct{})
	for _, item := range parsed.GetURLs() {
		loc := normalizeURLString(item.Loc)
		if loc != "" {
			results[loc] = struct{}{}
		}
	}
	return results, nil
}

func fetchWithGopher(site string) (map[string]struct{}, error) {
	results := make(map[string]struct{})
	err := gopher.ParseFromSite(site, func(entry gopher.Entry) error {
		loc := normalizeURLString(entry.GetLocation())
		if loc != "" {
			results[loc] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func fetchWithMega(site string) (map[string]struct{}, error) {
	scanner := mega.NewScanner(&config.Config{})
	if scanner == nil {
		return nil, errors.New("failed to initialize sitemap-go scanner")
	}
	defer scanner.Close()

	links := scanner.GetLinksFromSitemapIndex(site)
	results := make(map[string]struct{})
	for _, loc := range links {
		norm := normalizeURLString(loc)
		if norm != "" {
			results[norm] = struct{}{}
		}
	}
	return results, nil
}

func normalizeURLString(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	parsed.Fragment = ""
	return parsed.String()
}

func recordComparison(t *testing.T, summaries *[]comparisonSummary, site, label string, ours, other map[string]struct{}, metrics fetchMetrics) {
	t.Helper()

	missing := diffSet(ours, other)
	extra := diffSet(other, ours)
	*summaries = append(*summaries, comparisonSummary{
		Site:    site,
		Tool:    label,
		URLs:    len(other),
		Missing: len(missing),
		Extra:   len(extra),
		Metrics: metrics,
	})

	if len(missing) == 0 && len(extra) == 0 {
		return
	}

	missingSample := sampleStrings(missing, 5)
	extraSample := sampleStrings(extra, 5)

	t.Errorf("comparison mismatch for %s: missing=%d extra=%d missing_sample=%v extra_sample=%v", label, len(missing), len(extra), missingSample, extraSample)
}

func measureFetch(fetch func() (map[string]struct{}, error)) (map[string]struct{}, fetchMetrics, error) {
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()
	urls, err := fetch()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(urls)

	metrics := fetchMetrics{
		Elapsed:     time.Since(start),
		AllocKB:     bytesToKB(after.TotalAlloc - before.TotalAlloc),
		HeapInuseKB: signedBytesToKB(after.HeapInuse, before.HeapInuse),
		SysKB:       signedBytesToKB(after.Sys, before.Sys),
	}
	if after.HeapObjects >= before.HeapObjects {
		metrics.HeapObjects = after.HeapObjects - before.HeapObjects
	}
	return urls, metrics, err
}

func printComparisonSummary(t *testing.T, summaries []comparisonSummary) {
	t.Helper()

	if len(summaries) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintln(&b, "comparison summary:")
	fmt.Fprintf(&b, "%-43s  %-22s  %10s  %10s  %10s  %10s  %11s  %10s  %8s  %12s\n", "site", "tool", "urls", "missing", "extra", "elapsed", "alloc_kb", "heap_kb", "sys_kb", "heap_objects")
	for _, summary := range summaries {
		fmt.Fprintf(
			&b,
			"%-43s  %-22s  %10d  %10d  %10d  %10s  %11d  %10d  %8d  %12d\n",
			summary.Site,
			summary.Tool,
			summary.URLs,
			summary.Missing,
			summary.Extra,
			summary.Metrics.Elapsed.Truncate(time.Millisecond),
			summary.Metrics.AllocKB,
			summary.Metrics.HeapInuseKB,
			summary.Metrics.SysKB,
			summary.Metrics.HeapObjects,
		)
	}
	t.Log("\n" + strings.TrimRight(b.String(), "\n"))
}

func bytesToKB(bytes uint64) uint64 {
	return bytes / 1024
}

func signedBytesToKB(after, before uint64) int64 {
	if after >= before {
		return int64((after - before) / 1024)
	}
	return -int64((before - after) / 1024)
}

func diffSet(left, right map[string]struct{}) []string {
	out := make([]string, 0)
	for key := range left {
		if _, ok := right[key]; !ok {
			out = append(out, key)
		}
	}
	return out
}

func sampleStrings(items []string, max int) []string {
	if len(items) == 0 {
		return nil
	}
	sort.Strings(items)
	if len(items) <= max {
		return items
	}
	return items[:max]
}
