package gositemapfetcher

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/temoto/robotstxt"
)

const (
	defaultUserAgent  = "Mozilla/5.0 (Macintosh; Intel Mac OS X 26_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
	defaultBufSize    = 64 * 1024
	maxRetryAttempts  = 3
	defaultRetryDelay = 5 * time.Second
	maxRetryDelay     = 30 * time.Second
)

// ===================== Configuration =====================

// Options configures sitemap fetching, limits, and filtering behavior.
type Options struct {
	HTTPClient        *http.Client
	MaxDepth          int
	MaxSitemaps       int
	MaxURLs           int
	SkipNon200        bool
	IgnoreRobots      bool
	UserAgent         string
	PerRequestTimeout time.Duration
	Logger            *slog.Logger

	Include []*regexp.Regexp // nil => include all
	Exclude []*regexp.Regexp // nil => exclude none
}

// SitemapFetcher streams sitemap URLs and implements SitemapWalker.
type SitemapFetcher struct {
	opts   Options
	client *http.Client
	logger *slog.Logger
}

// ===================== Public API =====================

// New builds a SitemapFetcher with safe defaults applied.
func New(opts Options) *SitemapFetcher {
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.UserAgent == "" {
		opts.UserAgent = defaultUserAgent
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &SitemapFetcher{
		opts:   opts,
		client: opts.HTTPClient,
		logger: opts.Logger,
	}
}

// Walk traverses sitemaps discovered from the given website or sitemap URL.
func (f *SitemapFetcher) Walk(ctx context.Context, website *url.URL, yield func(Item) error) error {
	if yield == nil {
		return &ErrNilYield{}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	inputURL, baseURL, err := normalizeInputURL(website)
	if err != nil {
		return err
	}

	robotsCache := map[string]*robotsRules{}
	var baseRobots *robotsRules
	if !f.opts.IgnoreRobots && !isLikelySitemapURL(inputURL) {
		baseRobots, _ = f.getRobots(ctx, baseURL, robotsCache)
	}

	initial := f.initialSitemaps(inputURL, baseURL, baseRobots)
	if len(initial) == 0 {
		return &ErrNoSitemaps{URL: baseURL}
	}

	queue := make([]sitemapTask, 0, len(initial))
	for _, task := range initial {
		queue = append(queue, task)
	}

	seen := make(map[string]struct{}, len(initial))
	var sitemapCount int
	var urlCount int

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := queue[0]
		queue = queue[1:]

		if f.opts.MaxDepth > 0 && current.depth > f.opts.MaxDepth {
			return &ErrMaxDepth{MaxDepth: f.opts.MaxDepth, URL: current.loc}
		}

		key := canonicalURLKey(current.loc)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		if !f.opts.IgnoreRobots {
			allowed, err := f.allowedByRobots(ctx, current.loc, robotsCache)
			if err != nil {
				return err
			}
			if !allowed {
				f.logger.Debug(fmt.Sprintf("robots.txt disallows sitemap %s", current.loc))
				continue
			}
		}

		if f.opts.MaxSitemaps > 0 && sitemapCount >= f.opts.MaxSitemaps {
			return &ErrMaxSitemaps{MaxSitemaps: f.opts.MaxSitemaps}
		}
		sitemapCount++

		reader, err := f.fetchSitemap(ctx, current.loc, current.allowMissing)
		if err != nil {
			return err
		}
		if reader == nil {
			continue
		}

		err = parseSitemap(ctx, reader, func(entry xmlURLEntry) error {
			loc, err := resolveLocation(current.loc, entry.Loc)
			if err != nil {
				f.logger.Debug(fmt.Sprintf("invalid URL %q in %s: %v", entry.Loc, current.loc, err))
				return nil
			}
			if !f.opts.IgnoreRobots {
				allowed, err := f.allowedByRobots(ctx, loc, robotsCache)
				if err != nil {
					return err
				}
				if !allowed {
					f.logger.Debug(fmt.Sprintf("robots.txt disallows URL %s", loc))
					return nil
				}
			}
			if !f.shouldInclude(loc) {
				return nil
			}
			if f.opts.MaxURLs > 0 && urlCount >= f.opts.MaxURLs {
				return &ErrMaxURLs{MaxURLs: f.opts.MaxURLs}
			}
			item := Item{
				Loc:        loc,
				LastMod:    parseTimeValue(entry.LastMod),
				ChangeFreq: strings.TrimSpace(entry.ChangeFreq),
				Priority:   parsePriority(entry.Priority),
				Sitemap:    cloneURL(current.loc),
			}
			if err := yield(item); err != nil {
				return &ErrYield{Err: err}
			}
			urlCount++
			return nil
		}, func(entry xmlSitemapEntry) error {
			loc, err := resolveLocation(current.loc, entry.Loc)
			if err != nil {
				f.logger.Debug(fmt.Sprintf("invalid sitemap URL %q in %s: %v", entry.Loc, current.loc, err))
				return nil
			}
			queue = append(queue, sitemapTask{loc: loc, depth: current.depth + 1})
			return nil
		})
		reader.Close()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			var maxURLs *ErrMaxURLs
			if errors.As(err, &maxURLs) {
				return err
			}
			var yieldErr *ErrYield
			if errors.As(err, &yieldErr) {
				return err
			}
			return &ErrSitemapParse{URL: current.loc, Err: err}
		}
	}

	return nil
}

// ===================== Internal Types =====================

type sitemapTask struct {
	loc   *url.URL
	depth int
	// allowMissing treats 404 responses as a non-fatal probe miss.
	allowMissing bool
}

type robotsRules struct {
	group    *robotstxt.Group
	sitemaps []*url.URL
}

type xmlURLEntry struct {
	Loc        string `xml:"loc"`
	LastMod    string `xml:"lastmod"`
	ChangeFreq string `xml:"changefreq"`
	Priority   string `xml:"priority"`
}

type xmlSitemapEntry struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

type readCloser struct {
	reader io.Reader
	close  func() error
}

func (r *readCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *readCloser) Close() error {
	if r.close == nil {
		return nil
	}
	return r.close()
}

type cancelCloser struct {
	cancel context.CancelFunc
}

func (c cancelCloser) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func (m *multiCloser) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m *multiCloser) Close() error {
	var firstErr error
	for _, closer := range m.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ===================== Sitemap Discovery =====================

func normalizeInputURL(website *url.URL) (*url.URL, *url.URL, error) {
	if website == nil {
		return nil, nil, &ErrInvalidURL{Err: errors.New("nil URL")}
	}
	input := *website
	if input.Scheme == "" {
		input.Scheme = "https"
	}
	if input.Host == "" {
		return nil, nil, &ErrInvalidURL{URL: website.String(), Err: errors.New("missing host")}
	}
	input.Fragment = ""
	base := &url.URL{Scheme: input.Scheme, Host: input.Host}
	return &input, base, nil
}

func (f *SitemapFetcher) initialSitemaps(input, base *url.URL, robots *robotsRules) []sitemapTask {
	if isLikelySitemapURL(input) {
		return []sitemapTask{{loc: cloneURL(input), depth: 0}}
	}
	if robots != nil && len(robots.sitemaps) > 0 {
		tasks := make([]sitemapTask, 0, len(robots.sitemaps))
		for _, loc := range robots.sitemaps {
			tasks = append(tasks, sitemapTask{loc: loc, depth: 0})
		}
		return tasks
	}
	paths := defaultSitemaps(base)
	tasks := make([]sitemapTask, 0, len(paths))
	for _, loc := range paths {
		tasks = append(tasks, sitemapTask{loc: loc, depth: 0, allowMissing: true})
	}
	return tasks
}

func isLikelySitemapURL(u *url.URL) bool {
	if u == nil {
		return false
	}
	path := strings.ToLower(u.Path)
	return strings.HasSuffix(path, ".xml") || strings.HasSuffix(path, ".xml.gz")
}

func defaultSitemaps(base *url.URL) []*url.URL {
	candidates := []string{
		"/sitemap.xml",
		"/sitemap_index.xml",
		"/sitemap-index.xml",
		"/sitemap.xml.gz",
		"/sitemap_index.xml.gz",
		"/sitemap-index.xml.gz",
	}
	out := make([]*url.URL, 0, len(candidates))
	for _, path := range candidates {
		out = append(out, base.ResolveReference(&url.URL{Path: path}))
	}
	return out
}

// ===================== Filtering =====================

func (f *SitemapFetcher) shouldInclude(u *url.URL) bool {
	if u == nil {
		return false
	}
	candidate := u.String()
	if len(f.opts.Include) > 0 {
		matched := false
		for _, re := range f.opts.Include {
			if re != nil && re.MatchString(candidate) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, re := range f.opts.Exclude {
		if re != nil && re.MatchString(candidate) {
			return false
		}
	}
	return true
}

// ===================== HTTP Helpers =====================

func (f *SitemapFetcher) newRequest(ctx context.Context, method string, u *url.URL) (*http.Request, context.CancelFunc, error) {
	if f.opts.PerRequestTimeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, f.opts.PerRequestTimeout)
		req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("User-Agent", f.opts.UserAgent)
		return req, cancel, nil
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", f.opts.UserAgent)
	return req, func() {}, nil
}

func (f *SitemapFetcher) fetchSitemap(ctx context.Context, loc *url.URL, allowMissing bool) (io.ReadCloser, error) {
	for attempt := 0; attempt <= maxRetryAttempts; attempt++ {
		req, cancel, err := f.newRequest(ctx, http.MethodGet, loc)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			return nil, err
		}

		resp, err := f.client.Do(req)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			return nil, err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryAfterDelay(resp)
			resp.Body.Close()
			if cancel != nil {
				cancel()
			}
			if attempt == maxRetryAttempts {
				return nil, &ErrHTTPStatus{URL: loc, StatusCode: resp.StatusCode, Status: resp.Status}
			}
			if delay <= 0 {
				delay = defaultRetryDelay
			}
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}
			f.logger.Debug(fmt.Sprintf("received 429 for %s, retrying in %s", loc, delay))
			if err := sleepWithContext(ctx, delay); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			resp.Body.Close()
			if cancel != nil {
				cancel()
			}
			if f.opts.SkipNon200 {
				f.logger.Warn(
					"skipping sitemap due to non-200 response",
					"sitemap", loc.String(),
					"status", resp.Status,
				)
				return nil, nil
			}
			if allowMissing && resp.StatusCode == http.StatusNotFound {
				f.logger.Debug(fmt.Sprintf("sitemap not found (probe) %s", loc))
				return nil, nil
			}
			return nil, &ErrHTTPStatus{URL: loc, StatusCode: resp.StatusCode, Status: resp.Status}
		}

		reader, err := wrapReader(resp, cancel)
		if err != nil {
			resp.Body.Close()
			if cancel != nil {
				cancel()
			}
			return nil, err
		}
		return reader, nil
	}

	return nil, &ErrHTTPStatus{URL: loc, StatusCode: http.StatusTooManyRequests, Status: http.StatusText(http.StatusTooManyRequests)}
}

func (f *SitemapFetcher) getRobots(ctx context.Context, base *url.URL, cache map[string]*robotsRules) (*robotsRules, error) {
	key := base.Scheme + "://" + base.Host
	if rules, ok := cache[key]; ok {
		return rules, nil
	}

	robotsURL := base.ResolveReference(&url.URL{Path: "/robots.txt"})
	req, cancel, err := f.newRequest(ctx, http.MethodGet, robotsURL)
	if err != nil {
		return nil, err
	}
	defer cancel()

	resp, err := f.client.Do(req)
	if err != nil {
		rules := &robotsRules{}
		cache[key] = rules
		return rules, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		rules := &robotsRules{}
		cache[key] = rules
		return rules, nil
	}

	data, err := robotstxt.FromResponse(resp)
	if err != nil {
		rules := &robotsRules{}
		cache[key] = rules
		return rules, nil
	}

	rules := &robotsRules{group: data.FindGroup(f.opts.UserAgent)}
	for _, loc := range data.Sitemaps {
		parsed, err := url.Parse(strings.TrimSpace(loc))
		if err != nil {
			f.logger.Debug(fmt.Sprintf("invalid sitemap URL %q in robots.txt %s: %v", loc, robotsURL, err))
			continue
		}
		if !parsed.IsAbs() {
			parsed = base.ResolveReference(parsed)
		}
		rules.sitemaps = append(rules.sitemaps, parsed)
	}

	cache[key] = rules
	return rules, nil
}

func (f *SitemapFetcher) allowedByRobots(ctx context.Context, loc *url.URL, cache map[string]*robotsRules) (bool, error) {
	base := &url.URL{Scheme: loc.Scheme, Host: loc.Host}
	rules, err := f.getRobots(ctx, base, cache)
	if err != nil {
		return true, nil
	}
	if rules == nil || rules.group == nil {
		return true, nil
	}
	path := loc.EscapedPath()
	if loc.RawQuery != "" {
		path += "?" + loc.RawQuery
	}
	return rules.group.Test(path), nil
}

func wrapReader(resp *http.Response, cancel context.CancelFunc) (io.ReadCloser, error) {
	reader := bufio.NewReaderSize(resp.Body, defaultBufSize)
	peek, err := reader.Peek(2)
	if err == nil && len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return &multiCloser{reader: gz, closers: []io.Closer{gz, resp.Body, cancelCloser{cancel: cancel}}}, nil
	}
	return &readCloser{
		reader: reader,
		close: func() error {
			if cancel != nil {
				cancel()
			}
			return resp.Body.Close()
		},
	}, nil
}

func retryAfterDelay(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		return time.Until(t)
	}
	return 0
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ===================== XML Parsing =====================

func parseSitemap(ctx context.Context, reader io.Reader, onURL func(xmlURLEntry) error, onSitemap func(xmlSitemapEntry) error) error {
	decoder := xml.NewDecoder(reader)
	decoder.Strict = false

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tok, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "url":
			var entry xmlURLEntry
			if err := decoder.DecodeElement(&entry, &start); err != nil {
				return err
			}
			if onURL != nil {
				if err := onURL(entry); err != nil {
					return err
				}
			}
		case "sitemap":
			var entry xmlSitemapEntry
			if err := decoder.DecodeElement(&entry, &start); err != nil {
				return err
			}
			if onSitemap != nil {
				if err := onSitemap(entry); err != nil {
					return err
				}
			}
		}
	}
}

func resolveLocation(base *url.URL, loc string) (*url.URL, error) {
	trimmed := strings.TrimSpace(loc)
	if trimmed == "" {
		return nil, errors.New("empty loc")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, err
	}
	if parsed.IsAbs() {
		parsed.Fragment = ""
		return parsed, nil
	}
	resolved := base.ResolveReference(parsed)
	resolved.Fragment = ""
	return resolved, nil
}

func parseTimeValue(value string) *time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
		"2006-01-02T15:04:05",
		time.RFC1123,
		time.RFC1123Z,
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return &parsed
		}
	}
	return nil
}

func parsePriority(value string) *float64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func canonicalURLKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	clone := *u
	clone.Fragment = ""
	return clone.String()
}

func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	copy := *u
	return &copy
}
