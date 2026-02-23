package gositemapfetcher

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSitemapFetcher_Walk_URLSet(t *testing.T) {
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/page-a</loc>
    <lastmod>2024-01-02</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.7</priority>
  </url>
  <url>
    <loc>https://example.com/page-b</loc>
  </url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sitemap.xml" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(sitemap))
	}))
	defer server.Close()

	sitemapURL, err := url.Parse(server.URL + "/sitemap.xml")
	if err != nil {
		t.Fatalf("failed to parse sitemap URL: %v", err)
	}

	fetcher := New(Options{})
	items, err := collectItems(fetcher, sitemapURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if got := items[0].Loc.String(); !strings.HasSuffix(got, "/page-a") {
		t.Fatalf("expected relative loc to resolve, got %s", got)
	}
	if items[0].LastMod == nil {
		t.Fatalf("expected lastmod to be parsed")
	}
	if items[0].ChangeFreq != "daily" {
		t.Fatalf("expected changefreq daily, got %q", items[0].ChangeFreq)
	}
	if items[0].Priority == nil || *items[0].Priority != 0.7 {
		t.Fatalf("expected priority 0.7, got %v", items[0].Priority)
	}
}

func TestSitemapFetcher_Walk_IndexAndNested(t *testing.T) {
	const index = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>/nested.xml</loc>
  </sitemap>
</sitemapindex>`
	const nested = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/nested-page</loc>
  </url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index.xml":
			_, _ = w.Write([]byte(index))
		case "/nested.xml":
			_, _ = w.Write([]byte(nested))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	indexURL, err := url.Parse(server.URL + "/index.xml")
	if err != nil {
		t.Fatalf("failed to parse index URL: %v", err)
	}

	fetcher := New(Options{})
	items, err := collectItems(fetcher, indexURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Sitemap == nil || !strings.HasSuffix(items[0].Sitemap.String(), "/nested.xml") {
		t.Fatalf("expected sitemap to be nested.xml, got %v", items[0].Sitemap)
	}
}

func TestSitemapFetcher_Walk_Gzip(t *testing.T) {
	const nested = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/gzip-page</loc>
  </url>
</urlset>`

	var gzipped bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipped)
	_, _ = gzipWriter.Write([]byte(nested))
	_ = gzipWriter.Close()

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sitemap.xml.gz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(gzipped.Bytes())
	}))
	defer server.Close()

	sitemapURL, err := url.Parse(server.URL + "/sitemap.xml.gz")
	if err != nil {
		t.Fatalf("failed to parse sitemap URL: %v", err)
	}

	fetcher := New(Options{})
	items, err := collectItems(fetcher, sitemapURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestSitemapFetcher_RespectRobots_Default(t *testing.T) {
	const robots = "User-agent: *\nDisallow: /sitemap.xml\n"
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/blocked</loc>
  </url>
</urlset>`

	var sitemapRequests int32
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			_, _ = w.Write([]byte(robots))
		case "/sitemap.xml":
			atomic.AddInt32(&sitemapRequests, 1)
			_, _ = w.Write([]byte(sitemap))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	fetcher := New(Options{})
	items, err := collectItems(fetcher, baseURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no items, got %d", len(items))
	}
	if got := atomic.LoadInt32(&sitemapRequests); got != 0 {
		t.Fatalf("expected sitemap not to be fetched, got %d requests", got)
	}
}

func TestSitemapFetcher_IgnoreRobots(t *testing.T) {
	const robots = "User-agent: *\nDisallow: /sitemap.xml\n"
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/allowed</loc>
  </url>
</urlset>`

	var sitemapRequests int32
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			_, _ = w.Write([]byte(robots))
		case "/sitemap.xml":
			atomic.AddInt32(&sitemapRequests, 1)
			_, _ = w.Write([]byte(sitemap))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	fetcher := New(Options{IgnoreRobots: true})
	items, err := collectItems(fetcher, baseURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if got := atomic.LoadInt32(&sitemapRequests); got == 0 {
		t.Fatalf("expected sitemap to be fetched, got %d requests", got)
	}
}

func TestSitemapFetcher_IncludeExclude(t *testing.T) {
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/keep</loc>
  </url>
  <url>
    <loc>/skip</loc>
  </url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sitemap.xml" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(sitemap))
	}))
	defer server.Close()

	sitemapURL, err := url.Parse(server.URL + "/sitemap.xml")
	if err != nil {
		t.Fatalf("failed to parse sitemap URL: %v", err)
	}

	fetcher := New(Options{
		Include: []*regexp.Regexp{regexp.MustCompile("keep")},
		Exclude: []*regexp.Regexp{regexp.MustCompile("skip")},
	})
	items, err := collectItems(fetcher, sitemapURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if !strings.HasSuffix(items[0].Loc.String(), "/keep") {
		t.Fatalf("expected /keep URL, got %s", items[0].Loc.String())
	}
}

func TestSitemapFetcher_MaxURLs(t *testing.T) {
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/one</loc>
  </url>
  <url>
    <loc>/two</loc>
  </url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sitemap.xml" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(sitemap))
	}))
	defer server.Close()

	sitemapURL, err := url.Parse(server.URL + "/sitemap.xml")
	if err != nil {
		t.Fatalf("failed to parse sitemap URL: %v", err)
	}

	fetcher := New(Options{MaxURLs: 1})
	items, err := collectItems(fetcher, sitemapURL)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var maxErr *ErrMaxURLs
	if !errors.As(err, &maxErr) {
		t.Fatalf("expected ErrMaxURLs, got %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func collectItems(fetcher *SitemapFetcher, sitemapURL *url.URL) ([]Item, error) {
	var items []Item
	err := fetcher.Walk(context.Background(), sitemapURL, func(item Item) error {
		items = append(items, item)
		return nil
	})
	return items, err
}

func newTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping test that requires network listener: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}

func TestSitemapFetcher_DefaultDiscovery(t *testing.T) {
	const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>/auto</loc>
  </url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sitemap.xml":
			_, _ = w.Write([]byte(sitemap))
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	fetcher := New(Options{})
	items, err := collectItems(fetcher, baseURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestSitemapFetcher_PerRequestTimeout(t *testing.T) {
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sitemap.xml" {
			time.Sleep(50 * time.Millisecond)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><urlset></urlset>`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	sitemapURL, err := url.Parse(server.URL + "/sitemap.xml")
	if err != nil {
		t.Fatalf("failed to parse sitemap URL: %v", err)
	}

	fetcher := New(Options{PerRequestTimeout: 10 * time.Millisecond})
	_, err = collectItems(fetcher, sitemapURL)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}

func TestSitemapFetcher_SkipNon200_WarnsAndSkips(t *testing.T) {
	const index = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>/ok.xml</loc></sitemap>
  <sitemap><loc>/bad.xml</loc></sitemap>
</sitemapindex>`
	const ok = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>/page-ok</loc></url>
</urlset>`

	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index.xml":
			_, _ = w.Write([]byte(index))
		case "/ok.xml":
			_, _ = w.Write([]byte(ok))
		case "/bad.xml":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	indexURL, err := url.Parse(server.URL + "/index.xml")
	if err != nil {
		t.Fatalf("failed to parse index URL: %v", err)
	}

	handler := &captureHandler{}
	logger := slog.New(handler)
	fetcher := New(Options{
		SkipNon200: true,
		Logger:     logger,
	})

	items, err := collectItems(fetcher, indexURL)
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if !strings.HasSuffix(items[0].Loc.String(), "/page-ok") {
		t.Fatalf("expected /page-ok, got %s", items[0].Loc.String())
	}
	if !handler.hasWarningContaining("skipping sitemap due to non-200 response") {
		t.Fatalf("expected warning about skipped non-200 sitemap")
	}
}

type captureHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

type capturedRecord struct {
	level slog.Level
	msg   string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedRecord{
		level: record.Level,
		msg:   record.Message,
	})
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *captureHandler) hasWarningContaining(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.level == slog.LevelWarn && strings.Contains(record.msg, message) {
			return true
		}
	}
	return false
}
