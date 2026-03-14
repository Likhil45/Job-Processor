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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/jobs"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/store"
)

func main() {
	postgresDSN := getEnv("POSTGRES_DSN", "")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "")
	kafkaTopic := getEnv("KAFKA_TOPIC", "job.events")
	kafkaJobsTopic := getEnv("KAFKA_JOBS_TOPIC", "job.requests")
	kafkaGroupID := getEnv("KAFKA_CONSUMER_GROUP", "job-worker")

	if postgresDSN == "" {
		slog.Error("POSTGRES_DSN is required")
		os.Exit(1)
	}
	if kafkaBrokers == "" {
		slog.Error("KAFKA_BROKERS is required")
		os.Exit(1)
	}

	brokers := splitTrim(kafkaBrokers, ",")

	registry := jobs.NewRegistry()
	registry.Register(jobs.Hello{})
	registry.Register(jobs.NewEmailHandler(getEnv("SMTP_ADDR", ""), getEnv("EMAIL_FROM", "noreply@localhost")))
	registry.Register(&jobs.Image{OutputDir: getEnv("IMAGE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Invoice{OutputDir: getEnv("INVOICE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Report{OutputDir: getEnv("REPORT_OUTPUT_DIR", "")})

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

	process := func(ctx context.Context, req *kafkaqueue.JobRequest) error {
		rec, err := pgStore.GetByID(ctx, req.JobID)
		if err != nil {
			return err
		}
		if rec != nil && rec.Status == "cancelled" {
			return nil
		}
		_ = pgStore.UpdateStatus(ctx, req.JobID, "processing", "", req.Attempt, nil)
		eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventStarted, Queue: req.Queue, Attempt: req.Attempt})

		h := registry.Get(req.Type)
		if h == nil {
			metrics.JobsProcessedTotal.WithLabelValues(req.Type, "failure").Inc()
			_ = pgStore.UpdateStatus(ctx, req.JobID, "failed", "unknown task type", req.Attempt, nil)
			eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventFailed, LastError: "unknown task type"})
			return nil
		}
		err = h.Handle(ctx, req.Payload)
		if err != nil {
			metrics.JobsProcessedTotal.WithLabelValues(req.Type, "failure").Inc()
			attempt := req.Attempt + 1
			maxRetry := int32(0)
			if req.Options != nil {
				maxRetry = req.Options.MaxRetry
			}
			_ = pgStore.UpdateStatus(ctx, req.JobID, "failed", err.Error(), attempt, nil)
			if attempt <= maxRetry {
				_, _ = jobProducer.Enqueue(ctx, req.JobID, req.Type, req.Payload, req.Queue, maxRetry, 0, attempt)
			} else {
				_ = pgStore.UpdateStatus(ctx, req.JobID, "archived", err.Error(), attempt, nil)
				eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventArchived, LastError: err.Error(), Attempt: attempt})
			}
			eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventFailed, LastError: err.Error(), Attempt: attempt})
			return nil
		}
		metrics.JobsProcessedTotal.WithLabelValues(req.Type, "success").Inc()
		now := time.Now()
		_ = pgStore.UpdateStatus(ctx, req.JobID, "completed", "", req.Attempt, &now)
		eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventCompleted, Queue: req.Queue})
		return nil
	}

	// Health/readiness HTTP server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if pgStore.Ping(ctx) != nil {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kafkaqueue.RunConsumer(ctx, brokers, kafkaJobsTopic, kafkaGroupID, process)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down worker")
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
