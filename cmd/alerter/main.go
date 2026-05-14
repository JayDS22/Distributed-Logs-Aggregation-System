// Command alerter watches the logs collection for ERROR/FATAL events via
// change streams and dispatches notifications to Slack/PagerDuty.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/JayDS22/logstream/internal/alert"
	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/storage"
	"github.com/JayDS22/logstream/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to YAML config")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	log, err := logger.New(cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	collectors := metrics.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := storage.New(ctx, &cfg.MongoDB, log, collectors)
	if err != nil {
		log.Fatal("mongo init failed", zap.Error(err))
	}
	defer store.Close(context.Background())

	disp := alert.New(cfg.Alerter, store, log, collectors)

	// Metrics endpoint for the alerter
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.MetricsPort),
		Handler: mux,
	}
	go func() {
		log.Info("alerter metrics listening", zap.String("addr", srv.Addr))
		_ = srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() { errCh <- disp.Run(ctx) }()

	select {
	case <-sigCh:
		log.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			log.Error("alerter exited with error", zap.Error(err))
		}
	}
	cancel()
	_ = srv.Shutdown(context.Background())
}
