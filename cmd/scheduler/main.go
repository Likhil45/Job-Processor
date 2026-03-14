// Scheduler periodically generates fake jobs and enqueues them via Kafka + Postgres.
// Requires POSTGRES_DSN and KAFKA_BROKERS. Optional: SCHEDULER_INTERVAL (e.g. 2m), SCHEDULER_HTTP_ADDR (e.g. :9091).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/scheduler"
	"github.com/savvients/sip-core/internal/store"
)

func main() {
	postgresDSN := getEnv("POSTGRES_DSN", "")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "")
	kafkaJobsTopic := getEnv("KAFKA_JOBS_TOPIC", "job.requests")
	intervalStr := getEnv("SCHEDULER_INTERVAL", "2m")
	httpAddr := getEnv("SCHEDULER_HTTP_ADDR", ":9091")

	if postgresDSN == "" {
		slog.Error("POSTGRES_DSN is required")
		os.Exit(1)
	}
	if kafkaBrokers == "" {
		slog.Error("KAFKA_BROKERS is required")
		os.Exit(1)
	}

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		slog.Error("invalid SCHEDULER_INTERVAL", "value", intervalStr, "err", err)
		os.Exit(1)
	}

	pgStore, err := store.NewPostgresStore(postgresDSN)
	if err != nil {
		slog.Error("postgres store", "err", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	brokers := splitTrim(kafkaBrokers, ",")
	jobProducer, err := kafkaqueue.NewProducer(kafkaqueue.Config{Brokers: brokers, Topic: kafkaJobsTopic})
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer jobProducer.Close()

	submit := func(ctx context.Context, jobType string, payload []byte, maxRetry int32) (string, error) {
		jobID := uuid.New().String()
		queue := "default"
		if err := pgStore.Create(ctx, &store.JobRecord{
			ID:          jobID,
			Type:        jobType,
			Payload:     payload,
			Queue:       queue,
			Status:      "pending",
			AsynqTaskID: jobID,
		}); err != nil {
			return "", err
		}
		if _, err := jobProducer.Enqueue(ctx, jobID, jobType, payload, queue, maxRetry, 0, 0); err != nil {
			return "", err
		}
		metrics.JobsScheduledTotal.WithLabelValues(jobType).Inc()
		return jobID, nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := pgStore.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		slog.Info("scheduler http listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go scheduler.Run(ctx, interval, submit)
	go scheduler.RunPromoter(ctx, pgStore, jobProducer, 15*time.Second)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitTrim(s, sep string) []string {
	var out []string
	for _, p := range strings.Split(s, sep) {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
