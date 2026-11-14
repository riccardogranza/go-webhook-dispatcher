package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/riccardogranza/go-webhook-dispatcher/internal/dispatcher"
)

func main() {
	cfg := dispatcher.DefaultConfig()

	flag.StringVar(&cfg.ListenAddr, "addr", cfg.ListenAddr, "HTTP listen address")
	flag.StringVar(&cfg.DBPath, "db", cfg.DBPath, "SQLite WAL database file path")
	flag.IntVar(&cfg.WorkerCount, "workers", cfg.WorkerCount, "Bounded worker pool concurrency")
	flag.IntVar(&cfg.QueueCapacity, "queue-capacity", cfg.QueueCapacity, "Bounded channel capacity before HTTP 429 load shedding")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "Maximum delivery retries before routing to DLQ")
	flag.DurationVar(&cfg.BaseBackoff, "base-backoff", cfg.BaseBackoff, "Base exponential backoff duration")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", cfg.MaxBackoff, "Maximum backoff cap duration")
	flag.DurationVar(&cfg.DeliveryTimeout, "delivery-timeout", cfg.DeliveryTimeout, "Per-attempt outbound webhook HTTP timeout")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "Graceful shutdown drain timeout")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	eng, err := dispatcher.NewEngine(cfg)
	if err != nil {
		logger.Error("failed to initialize webhook dispatcher engine", "error", err)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           eng.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("webhook dispatcher listening",
			"addr", cfg.ListenAddr,
			"db_path", cfg.DBPath,
			"workers", cfg.WorkerCount,
			"queue_capacity", cfg.QueueCapacity,
			"max_retries", cfg.MaxRetries,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
		close(serverErrCh)
	}()

	select {
	case sig := <-sigCh:
		logger.Info("received termination signal; initiating graceful shutdown",
			"signal", sig.String(),
			"shutdown_timeout", cfg.ShutdownTimeout.String(),
		)
	case err := <-serverErrCh:
		if err != nil {
			logger.Error("http server terminated unexpectedly", "error", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// 1. Stop accepting new HTTP connections and wait for active ingestion handlers to finish.
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http server shutdown timeout exceeded", "error", err)
	}

	// 2. Drain bounded worker pool and flush state to SQLite WAL.
	if err := eng.Shutdown(shutdownCtx); err != nil {
		logger.Warn("engine worker drain completed with warning", "error", err)
	}

	snap := eng.Metrics().Snapshot(0, cfg.QueueCapacity)
	logger.Info("graceful shutdown complete",
		"enqueued", snap.Enqueued,
		"delivered", snap.Delivered,
		"retried", snap.Retried,
		"dlq_count", snap.DLQCount,
		"shed_count", snap.ShedCount,
		"peak_rss_mb", snap.PeakRSSMB,
	)
}
