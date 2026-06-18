package gositemapfetcher

import (
	"context"
	"net/url"
	"time"
)

// SitemapWalker walks through sitemap URLs and yields parsed items.
type SitemapWalker interface {
	// Walk streams sitemap entries discovered from the provided website or sitemap URL.
	Walk(ctx context.Context, website *url.URL, yield func(Item) error) error
}

// Item is what you yield while walking sitemaps.
type Item struct {
	Loc        *url.URL
	LastMod    *time.Time
	ChangeFreq string
	Priority   *float64
	Sitemap    *url.URL
}

// SkippedSitemap describes a sitemap fetch/open failure skipped by SkipNon200 or SkipFetchErrors.
type SkippedSitemap struct {
	URL string
	Err error
}
