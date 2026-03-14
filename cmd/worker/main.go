package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/jobs"
	"github.com/savvients/sip-core/internal/queue"
	"github.com/savvients/sip-core/internal/store"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	concurrency, _ := strconv.Atoi(getEnv("WORKER_CONCURRENCY", "5"))
	postgresDSN := getEnv("POSTGRES_DSN", "")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "")
	kafkaTopic := getEnv("KAFKA_TOPIC", "job.events")

	registry := jobs.NewRegistry()
	registry.Register(jobs.Hello{})
	registry.Register(jobs.NewEmailHandler(getEnv("SMTP_ADDR", ""), getEnv("EMAIL_FROM", "noreply@localhost")))
	registry.Register(&jobs.Image{OutputDir: getEnv("IMAGE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Invoice{OutputDir: getEnv("INVOICE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Report{OutputDir: getEnv("REPORT_OUTPUT_DIR", "")})

	var jobStore store.Store
	if postgresDSN != "" {
		pgStore, err := store.NewPostgresStore(postgresDSN)
		if err != nil {
			slog.Error("postgres store", "err", err)
			os.Exit(1)
		}
		defer pgStore.Close()
		jobStore = pgStore
	}

	var eventProducer events.Producer = events.NoopProducer{}
	if kafkaBrokers != "" {
		brokers := splitTrim(kafkaBrokers, ",")
		kp, err := events.NewKafkaProducer(events.KafkaConfig{Brokers: brokers, Topic: kafkaTopic})
		if err != nil {
			slog.Warn("kafka producer", "err", err, "msg", "continuing without events")
		} else {
			defer kp.Close()
			eventProducer = kp
		}
	}

	mux := queue.NewServeMux(registry, jobStore, eventProducer)

	redisOpts := asynq.RedisClientOpt{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	}

	// Priority queues: higher weight = more workers on that queue. high:3, default:2, low:1.
	srv := asynq.NewServer(redisOpts, asynq.Config{
		Concurrency: concurrency,
		Queues: map[string]int{
			"high":    3,
			"default": 2,
			"low":     1,
		},
	})

	// Health/readiness HTTP server for Kubernetes probes
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPassword, DB: redisDB})
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if rdb.Ping(ctx).Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		if err := http.ListenAndServe(":9090", nil); err != nil && err != http.ErrServerClosed {
			slog.Warn("health server", "err", err)
		}
	}()

	go func() {
		slog.Info("worker starting", "redis", redisAddr, "concurrency", concurrency)
		if err := srv.Run(mux); err != nil {
			slog.Error("worker stopped", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down worker")
	srv.Shutdown()
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
