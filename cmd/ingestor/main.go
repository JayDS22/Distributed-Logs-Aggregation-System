// Command ingestor runs the logstream ingestion HTTP API and background
// pipeline. It binds a JSON ingestion endpoint, query endpoints, and a
// Prometheus /metrics endpoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/ingestion"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/middleware"
	"github.com/JayDS22/logstream/internal/processor"
	"github.com/JayDS22/logstream/internal/storage"
	"github.com/JayDS22/logstream/pkg/logger"
	"github.com/gorilla/mux"
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
		log.Fatal("mongo store init failed", zap.Error(err))
	}
	defer store.Close(context.Background())

	pipeline := ingestion.New(cfg.Ingestor, store, log, collectors)
	pipeline.Start(ctx)

	handlers := &processor.Handlers{
		Pipeline: pipeline,
		Store:    store,
		Logger:   log,
		Started:  time.Now(),
	}

	r := mux.NewRouter()
	r.Use(middleware.Recover(log))
	r.Use(middleware.Metrics(collectors))
	r.Use(middleware.CORS)

	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/logs", handlers.Ingest).Methods(http.MethodPost, http.MethodOptions)
	api.HandleFunc("/logs/single", handlers.IngestSingle).Methods(http.MethodPost, http.MethodOptions)
	api.HandleFunc("/logs", handlers.Query).Methods(http.MethodGet, http.MethodOptions)
	api.HandleFunc("/stats", handlers.Stats).Methods(http.MethodGet, http.MethodOptions)
	api.HandleFunc("/throughput", handlers.Throughput).Methods(http.MethodGet, http.MethodOptions)
	api.HandleFunc("/pipeline", handlers.PipelineStats).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/healthz", handlers.Health).Methods(http.MethodGet)
	r.HandleFunc("/readyz", handlers.Ready).Methods(http.MethodGet)

	// Static demo dashboard
	r.PathPrefix("/demo/").Handler(http.StripPrefix("/demo/", http.FileServer(http.Dir("./demo"))))
	r.Handle("/", http.RedirectHandler("/demo/", http.StatusFound))

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.MetricsPort),
		Handler: metricsMux,
	}

	// Run both servers
	go func() {
		log.Info("api server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("api server error", zap.Error(err))
		}
	}()
	go func() {
		log.Info("metrics server listening", zap.String("addr", metricsSrv.Addr))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	_ = srv.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	cancel()
	pipeline.Stop()
	log.Info("clean shutdown complete")
}
