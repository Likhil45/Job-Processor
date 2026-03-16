package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/savvients/sip-core/internal/api"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/store"
	"github.com/savvients/sip-core/internal/telemetry"
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

	// #region agent log
	debugLogPath := getEnv("DEBUG_LOG_PATH", "debug-3a3d06.log")
	// #endregion
	shutdownTracer, err := telemetry.InitTracer("job-api", getEnv, debugLogPath)
	// #region agent log
	if debugLogPath != "" {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		f, _ := os.OpenFile(debugLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if f != nil {
			enc := map[string]interface{}{
				"sessionId": "3a3d06", "timestamp": time.Now().UnixMilli(), "location": "cmd/api/main.go",
				"message": "api InitTracer result", "data": map[string]interface{}{"err": errStr, "endpoint": getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")}, "hypothesisId": "A,E",
			}
			json.NewEncoder(f).Encode(enc)
			f.Close()
		}
	}
	// #endregion
	if err != nil {
		slog.Warn("tracing init", "err", err, "msg", "continuing without tracing")
	} else {
		defer shutdownTracer()
	}

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
	if perQueue, global := api.ParseRateLimitConfig(getEnv); perQueue > 0 || global > 0 {
		restHandler.SetSubmitRateLimiter(api.NewTokenBucketLimiter(perQueue, global))
	}

	mux := http.NewServeMux()
	mux.Handle("/jobs", restHandler)
	mux.Handle("/jobs/", restHandler)
	mux.Handle("/schedules", restHandler)
	mux.Handle("/schedules/", restHandler)
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
