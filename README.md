# go-sitemap-fetcher
[![Go Reference](https://pkg.go.dev/badge/github.com/kotylevskiy/go-sitemap-fetcher.svg)](https://pkg.go.dev/github.com/kotylevskiy/go-sitemap-fetcher)
[![Go Report Card](https://goreportcard.com/badge/github.com/kotylevskiy/go-sitemap-fetcher)](https://goreportcard.com/report/github.com/kotylevskiy/go-sitemap-fetcher)
[![License](https://img.shields.io/github/license/kotylevskiy/go-sitemap-fetcher)](LICENSE)


Fast, streaming sitemap walker for Go. It handles sitemap indexes (including nested indexes), gzip-compressed XML, robots.txt rules, and URL filtering **without loading entire sitemaps into memory**, even when they are gzipped.

It is designed for speed and low memory usage. For example, processing the full wikipedia.org sitemap index stays around ~10 MB of RAM in the long test.

## Why this fetcher

Common advantages compared to other sitemap parsers:

- Streaming XML parsing: avoids loading full sitemap documents into memory, which keeps memory flat even for very large sitemaps.
- Unified traversal: handles sitemap indexes and nested sitemaps in one walk.
- Optional robots.txt enforcement: useful when you need to respect site policies.
- URL filtering and limits: include/exclude patterns and hard caps for depth, sitemap count, and URLs.
- 429 handling: requests that return HTTP 429 are retried up to 3 times, honoring `Retry-After` when present (or a short backoff when not).
- Typed errors: easier error handling in higher-level code.
- CLI included: convenient for quick checks or piping URLs into other tools.

Other parsers are great for specific tasks, but many focus on parsing a single sitemap file or load entire documents into memory. This fetcher prioritizes streaming and end-to-end traversal with configurable controls.

## Install

```bash
go get github.com/kotylevskiy/go-sitemap-fetcher
```

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/url"

	gositemapfetcher "github.com/kotylevskiy/go-sitemap-fetcher"
)

func main() {
	website, _ := url.Parse("https://www.apple.com/sitemap.xml")
	fetcher := gositemapfetcher.New(gositemapfetcher.Options{})

	err := fetcher.Walk(context.Background(), website, func(item gositemapfetcher.Item) error {
		// Put your URL handling logic here.
		fmt.Println(item.Loc.String())
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Options

Defaults are safe and permissive, with minimal surprises:

- `HTTPClient`: uses `http.DefaultClient` when nil.
- `MaxDepth`, `MaxSitemaps`, `MaxURLs`: `0` means no limit.
- `PerRequestTimeout`: `0` means no per-request timeout (caller’s context still applies).
- `UserAgent`: browser-like user agent when empty.
- `IgnoreRobots`: disabled by default (robots.txt respected).
- `Include`/`Exclude`: nil means include all / exclude none.
- `Logger`: silenced by default. Set to `slog.New(slog.NewTextHandler(os.Stderr, nil))` to enable logging.

Typed errors are returned for common failure modes, including `ErrInvalidURL`, `ErrHTTPStatus`, `ErrSitemapParse`, `ErrMaxDepth`, `ErrMaxSitemaps`, `ErrMaxURLs`, and `ErrYield`.

## Examples

### Filter URLs

```go
fetcher := gositemapfetcher.New(gositemapfetcher.Options{
	Include: []*regexp.Regexp{regexp.MustCompile(`/blog/`)},
	Exclude: []*regexp.Regexp{regexp.MustCompile(`/blog/drafts/`)},
})
```

### Apply limits

```go
fetcher := gositemapfetcher.New(gositemapfetcher.Options{
	MaxDepth:    2,
	MaxSitemaps: 100,
	MaxURLs:     50000,
})
```

### Custom HTTP client and logger

```go
client := &http.Client{Timeout: 10 * time.Second}
logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

fetcher := gositemapfetcher.New(gositemapfetcher.Options{
	HTTPClient: client,
	Logger:     logger,
})
```

### Ignore robots.txt

```go
fetcher := gositemapfetcher.New(gositemapfetcher.Options{
	IgnoreRobots: true,
})
```

## Tests

Run unit tests:

```bash
go test ./...
```

### Large sitemap performance test

Generates a 10M-URL sitemap stream and validates that parsing stays efficient.

```bash
GO_SITEMAP_FETCHER_LONG=1 go test -tags long ./...
```

### Integration comparisons with other tools

The `additional` package compares this fetcher against other popular sitemap parsers on real websites. These tests require network access and may take a while (some dependencies introduce throttling delays).

```bash
GO_SITEMAP_FETCHER_INTEGRATION=1 go test -tags integration ./additional
```

Interpretation:

- Each site is fetched with all tools and URL sets are compared.
- A failure means a mismatch between tools. This can indicate a bug, different canonicalization rules, or external website changes.
- Re-run to confirm, and inspect the reported sample diffs.

### Sample long-test results

Example output from a local run (macOS, 10M URLs):

```
urls=1000000 elapsed=3.849s alloc_mb=2 heap_inuse_mb=3 sys_mb=13
urls=2000000 elapsed=4.09s alloc_mb=2 heap_inuse_mb=3 sys_mb=17
urls=3000000 elapsed=3.754s alloc_mb=3 heap_inuse_mb=4 sys_mb=17
urls=4000000 elapsed=3.896s alloc_mb=3 heap_inuse_mb=4 sys_mb=17
urls=5000000 elapsed=3.678s alloc_mb=3 heap_inuse_mb=3 sys_mb=17
urls=6000000 elapsed=3.669s alloc_mb=2 heap_inuse_mb=3 sys_mb=17
urls=7000000 elapsed=3.686s alloc_mb=2 heap_inuse_mb=2 sys_mb=17
urls=8000000 elapsed=3.68s alloc_mb=3 heap_inuse_mb=4 sys_mb=17
urls=9000000 elapsed=3.672s alloc_mb=1 heap_inuse_mb=2 sys_mb=17
urls=10000000 elapsed=3.671s alloc_mb=3 heap_inuse_mb=3 sys_mb=17
urls=10000000 elapsed=0s alloc_mb=3 heap_inuse_mb=3 sys_mb=17
```

This shows memory usage staying flat while processing 10M URLs, which indicates streaming behavior.

## CLI

Fetch a sitemap and print URLs line by line:

```bash
go run ./cmd/go-sitemap-fetcher https://www.apple.com/sitemap.xml
```

Flags:

- `--max-depth`, `--max-sitemaps`, `--max-urls`
- `--allow-non-200`
- `--user-agent`
- `--timeout` (per-request, e.g. `5s`)
- `--log-level` (`debug`, `info`, `warn`, `error`)
- `--ignore-robots`

Environment:

- `GO_SITEMAP_FETCHER_LOG_LEVEL` sets the default log level (same values as `--log-level`, default `error`).
