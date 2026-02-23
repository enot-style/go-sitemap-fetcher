//go:build integration

package additional

import (
	"context"
	"errors"
	"net/url"
	"os"
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

	sites := []string{
		"https://www.apple.com/sitemap.xml",
		"https://www.jetbrains.com/sitemap.xml",
		"https://www.djangoproject.com/sitemap.xml",
	}

	for _, site := range sites {
		site := site
		t.Run(site, func(t *testing.T) {
			ours, err := fetchWithFetcher(site)
			if err != nil {
				t.Fatalf("fetcher failed: %v", err)
			}

			parserURLs, err := fetchWithAafeher(site)
			if err != nil {
				t.Fatalf("go-sitemap-parser failed: %v", err)
			}
			compareSets(t, "go-sitemap-parser", ours, parserURLs)

			gopherURLs, err := fetchWithGopher(site)
			if err != nil {
				t.Fatalf("gopher-parse-sitemap failed: %v", err)
			}
			compareSets(t, "gopher-parse-sitemap", ours, gopherURLs)

			megaURLs, err := fetchWithMega(site)
			if err != nil {
				t.Fatalf("sitemap-go failed: %v", err)
			}
			compareSets(t, "sitemap-go", ours, megaURLs)
		})
	}
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

func compareSets(t *testing.T, label string, ours, other map[string]struct{}) {
	missing := diffSet(ours, other)
	extra := diffSet(other, ours)
	if len(missing) == 0 && len(extra) == 0 {
		return
	}

	missingSample := sampleStrings(missing, 5)
	extraSample := sampleStrings(extra, 5)

	t.Fatalf("comparison mismatch for %s: missing=%d extra=%d missing_sample=%v extra_sample=%v", label, len(missing), len(extra), missingSample, extraSample)
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
