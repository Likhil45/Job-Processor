package main

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/savvients/sip-core/internal/api"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	restAddr := getEnv("REST_ADDR", ":8080")
	metricsAddr := getEnv("METRICS_ADDR", ":9090")
	postgresDSN := getEnv("POSTGRES_DSN", "")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "")
	kafkaTopic := getEnv("KAFKA_TOPIC", "job.events")
	kafkaJobsTopic := getEnv("KAFKA_JOBS_TOPIC", "job.requests")

	if postgresDSN == "" {
		slog.Error("POSTGRES_DSN is required")
		os.Exit(1)
	}
	if kafkaBrokers == "" {
		slog.Error("KAFKA_BROKERS is required")
		os.Exit(1)
	}

	brokers := splitTrim(kafkaBrokers, ",")

	pgStore, err := store.NewPostgresStore(postgresDSN)
	if err != nil {
		slog.Error("postgres store", "err", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	jobProducer, err := kafkaqueue.NewProducer(kafkaqueue.Config{Brokers: brokers, Topic: kafkaJobsTopic})
	if err != nil {
		slog.Error("kafka job producer", "err", err)
		os.Exit(1)
	}
	defer jobProducer.Close()

	var eventProducer events.Producer = events.NoopProducer{}
	kp, err := events.NewKafkaProducer(events.KafkaConfig{Brokers: brokers, Topic: kafkaTopic})
	if err != nil {
		slog.Warn("kafka events producer", "err", err, "msg", "continuing without events")
	} else {
		defer kp.Close()
		eventProducer = kp
	}

	backend := api.NewKafkaPostgresBackend(pgStore, jobProducer, eventProducer)
	restHandler := api.NewRESTHandler(backend)

	mux := http.NewServeMux()
	mux.Handle("/jobs", restHandler)
	mux.Handle("/jobs/", restHandler)
	mux.Handle("/admin", restHandler)
	mux.Handle("/admin/", restHandler)
	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		slog.Info("REST server", "addr", restAddr)
		if err := http.ListenAndServe(restAddr, mux); err != nil && err != http.ErrServerClosed {
			slog.Warn("rest listen", "err", err)
		}
	}()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		slog.Info("metrics server", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, metricsMux); err != nil && err != http.ErrServerClosed {
			slog.Warn("metrics listen", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
