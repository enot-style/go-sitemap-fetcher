package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	gositemapfetcher "github.com/enot-style/go-sitemap-fetcher"
	"github.com/spf13/cobra"
)

func main() {
	var (
		maxDepth          int
		maxSitemaps       int
		maxURLs           int
		skipNon200        bool
		ignoreRobots      bool
		userAgent         string
		perRequestTimeout time.Duration
		logLevel          string
	)

	cmd := &cobra.Command{
		Use:          "go-sitemap-fetcher [flags] <site or sitemap URL>",
		Short:        "Fetch sitemaps and print URLs line by line",
		SilenceUsage: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				return nil
			}
			return errors.New("missing URL argument")
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			for _, arg := range os.Args[1:] {
				if arg == "--" {
					return nil
				}
				if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") && arg != "-h" {
					return fmt.Errorf("invalid flag %q (use --)", arg)
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := url.Parse(args[0])
			if err != nil {
				return fmt.Errorf("invalid URL %q: %w", args[0], err)
			}

			level, err := resolveLogLevel(logLevel)
			if err != nil {
				return err
			}
			if skipNon200 && strings.TrimSpace(logLevel) == "" && strings.TrimSpace(os.Getenv("GO_SITEMAP_FETCHER_LOG_LEVEL")) == "" {
				level = slog.LevelWarn
			}
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

			fetcher := gositemapfetcher.New(gositemapfetcher.Options{
				MaxDepth:          maxDepth,
				MaxSitemaps:       maxSitemaps,
				MaxURLs:           maxURLs,
				SkipNon200:        skipNon200,
				IgnoreRobots:      ignoreRobots,
				UserAgent:         userAgent,
				PerRequestTimeout: perRequestTimeout,
				Logger:            logger,
			})

			return fetcher.Walk(context.Background(), parsed, func(item gositemapfetcher.Item) error {
				_, err := fmt.Fprintln(os.Stdout, item.Loc.String())
				return err
			})
		},
	}

	flags := cmd.Flags()
	flags.IntVar(&maxDepth, "max-depth", 0, "Maximum sitemap index depth (0 = no limit)")
	flags.IntVar(&maxSitemaps, "max-sitemaps", 0, "Maximum number of sitemaps to fetch (0 = no limit)")
	flags.IntVar(&maxURLs, "max-urls", 0, "Maximum number of URLs to yield (0 = no limit)")
	flags.BoolVar(&skipNon200, "skip-non-200", false, "Skip non-200 sitemaps instead of failing")
	flags.BoolVar(&ignoreRobots, "ignore-robots", false, "Ignore robots.txt disallow rules")
	flags.StringVar(&userAgent, "user-agent", "", "User-Agent for HTTP requests")
	flags.DurationVar(&perRequestTimeout, "timeout", 0, "Per-request timeout (e.g. 5s, 500ms)")
	flags.StringVar(&logLevel, "log-level", "", "Log level (debug, info, warn, error)")

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func resolveLogLevel(flagValue string) (slog.Level, error) {
	value := strings.TrimSpace(flagValue)
	if value == "" {
		value = strings.TrimSpace(os.Getenv("GO_SITEMAP_FETCHER_LOG_LEVEL"))
	}
	if value == "" {
		return slog.LevelError, nil
	}
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid log level %q (use debug, info, warn, error)", value)
	}
}
