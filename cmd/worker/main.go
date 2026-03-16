package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/jobs"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/store"
	"github.com/savvients/sip-core/internal/telemetry"
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

	// #region agent log
	debugLogPath := getEnv("DEBUG_LOG_PATH", "debug-3a3d06.log")
	// #endregion
	shutdownTracer, err := telemetry.InitTracer("job-worker", getEnv, debugLogPath)
	// #region agent log
	if debugLogPath != "" {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		f, _ := os.OpenFile(debugLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if f != nil {
			enc := map[string]interface{}{
				"sessionId": "3a3d06", "timestamp": time.Now().UnixMilli(), "location": "cmd/worker/main.go",
				"message": "worker InitTracer result", "data": map[string]interface{}{"err": errStr, "endpoint": getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")}, "hypothesisId": "A,E",
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

	registry := jobs.NewRegistry()
	registry.Register(jobs.Hello{})
	registry.Register(jobs.NewEmailHandler(getEnv("SMTP_ADDR", ""), getEnv("EMAIL_FROM", "noreply@localhost")))
	registry.Register(&jobs.Image{OutputDir: getEnv("IMAGE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Invoice{OutputDir: getEnv("INVOICE_OUTPUT_DIR", "")})
	registry.Register(&jobs.Report{OutputDir: getEnv("REPORT_OUTPUT_DIR", "")})
	registry.Register(jobs.Sleep{})

	maxConcurrentPerType := 0
	if s := getEnv("WORKER_MAX_CONCURRENT_PER_TYPE", "0"); s != "" {
		maxConcurrentPerType, _ = strconv.Atoi(s)
	}
	slaSec := 0
	if s := getEnv("WORKER_SLA_SEC", "0"); s != "" {
		slaSec, _ = strconv.Atoi(s)
	}
	var concurrencyLimiter *concurrencyLimiter
	if maxConcurrentPerType > 0 {
		concurrencyLimiter = newConcurrencyLimiter(maxConcurrentPerType)
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

	process := func(ctx context.Context, req *kafkaqueue.JobRequest) error {
		start := time.Now()
		defer func() {
			metrics.JobProcessingDurationSeconds.WithLabelValues(req.Type).Observe(time.Since(start).Seconds())
		}()
		queue := req.Queue
		if queue == "" {
			queue = "default"
		}
		paused, err := pgStore.GetQueue(ctx, queue)
		if err != nil {
			return err
		}
		if paused {
			return nil
		}
		if concurrencyLimiter != nil {
			concurrencyLimiter.Acquire(req.Type)
			defer concurrencyLimiter.Release(req.Type)
		}
		rec, err := pgStore.GetByID(ctx, req.JobID)
		if err != nil {
			return err
		}
		if rec != nil && rec.Status == "cancelled" {
			return nil
		}
		_ = pgStore.UpdateStatus(ctx, req.JobID, "processing", "", req.Attempt, nil)
		eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventStarted, Queue: req.Queue, Attempt: req.Attempt})

		// Email pilot: validate and write to outbox in same tx as job completion; actual send is in outbox processor.
		if req.Type == jobs.EmailType {
			if err := jobs.ValidateEmailPayload(req.Payload); err != nil {
				metrics.JobsProcessedTotal.WithLabelValues(req.Type, "failure").Inc()
				attempt := req.Attempt + 1
				maxRetry := int32(0)
				if req.Options != nil {
					maxRetry = req.Options.MaxRetry
				}
				_ = pgStore.UpdateStatus(ctx, req.JobID, "failed", err.Error(), attempt, nil)
				if attempt <= maxRetry {
					pri := int32(0)
					if req.Options != nil {
						pri = req.Options.Priority
					}
					_, _ = jobProducer.Enqueue(ctx, req.JobID, req.Type, req.Payload, req.Queue, maxRetry, 0, attempt, pri)
					metrics.JobsRetriedTotal.WithLabelValues(req.Type, queue).Inc()
				} else {
					_ = pgStore.UpdateStatus(ctx, req.JobID, "archived", err.Error(), attempt, nil)
					metrics.JobsArchivedTotal.WithLabelValues(req.Type, queue).Inc()
					eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventArchived, LastError: err.Error(), Attempt: attempt})
				}
				eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventFailed, LastError: err.Error(), Attempt: attempt})
				return nil
			}
			metrics.JobsProcessedTotal.WithLabelValues(req.Type, "success").Inc()
			now := time.Now()
			if slaSec > 0 && rec != nil && now.Sub(rec.CreatedAt) > time.Duration(slaSec)*time.Second {
				metrics.JobsSLABreachedTotal.WithLabelValues(req.Type, queue).Inc()
			}
			outbox := &store.OutboxRecord{JobID: req.JobID, Type: jobs.EmailType, PayloadJSON: req.Payload}
			if err := pgStore.CompleteJobWithOutbox(ctx, req.JobID, "completed", "", req.Attempt, &now, outbox); err != nil {
				return err
			}
			eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventCompleted, Queue: req.Queue})
			return nil
		}

		h := registry.Get(req.Type)
		if h == nil {
			metrics.JobsProcessedTotal.WithLabelValues(req.Type, "failure").Inc()
			_ = pgStore.UpdateStatus(ctx, req.JobID, "failed", "unknown task type", req.Attempt, nil)
			eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventFailed, LastError: "unknown task type"})
			return nil
		}
		attemptCtx := context.WithValue(ctx, jobs.AttemptContextKey, req.Attempt)
		err = h.Handle(attemptCtx, req.Payload)
		if err != nil {
			metrics.JobsProcessedTotal.WithLabelValues(req.Type, "failure").Inc()
			attempt := req.Attempt + 1
			maxRetry := int32(0)
			if req.Options != nil {
				maxRetry = req.Options.MaxRetry
			}
			_ = pgStore.UpdateStatus(ctx, req.JobID, "failed", err.Error(), attempt, nil)
			if attempt <= maxRetry {
				pri := int32(0)
				if req.Options != nil {
					pri = req.Options.Priority
				}
				_, _ = jobProducer.Enqueue(ctx, req.JobID, req.Type, req.Payload, req.Queue, maxRetry, 0, attempt, pri)
				metrics.JobsRetriedTotal.WithLabelValues(req.Type, queue).Inc()
			} else {
				_ = pgStore.UpdateStatus(ctx, req.JobID, "archived", err.Error(), attempt, nil)
				metrics.JobsArchivedTotal.WithLabelValues(req.Type, queue).Inc()
				eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventArchived, LastError: err.Error(), Attempt: attempt})
			}
			eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventFailed, LastError: err.Error(), Attempt: attempt})
			return nil
		}
		metrics.JobsProcessedTotal.WithLabelValues(req.Type, "success").Inc()
		now := time.Now()
		if slaSec > 0 && rec != nil && now.Sub(rec.CreatedAt) > time.Duration(slaSec)*time.Second {
			metrics.JobsSLABreachedTotal.WithLabelValues(req.Type, queue).Inc()
		}
		_ = pgStore.UpdateStatus(ctx, req.JobID, "completed", "", req.Attempt, &now)
		eventProducer.Emit(ctx, events.JobEvent{JobID: req.JobID, Type: req.Type, Event: events.EventCompleted, Queue: req.Queue})
		return nil
	}

	var drainRequested atomic.Bool
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
	http.HandleFunc("/admin/drain", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		drainRequested.Store(true)
		slog.Info("drain requested; consumer will stop fetching and exit after current work")
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		if err := http.ListenAndServe(":9090", nil); err != nil && err != http.ErrServerClosed {
			slog.Warn("health server", "err", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainDone := make(chan struct{})
	go func() {
		kafkaqueue.RunConsumer(ctx, brokers, kafkaJobsTopic, kafkaGroupID, process, func() bool { return drainRequested.Load() })
		close(drainDone)
	}()

	// Outbox processor: perform side-effects (e.g. send email) for pending outbox rows.
	emailHandler := registry.Get(jobs.EmailType)
	go runOutboxProcessor(ctx, pgStore, emailHandler)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
		slog.Info("shutdown signal received")
	case <-drainDone:
		slog.Info("drain complete; exiting")
	}
	cancel()
}

// runOutboxProcessor polls pending outbox rows and performs the side-effect (e.g. email send).
func runOutboxProcessor(ctx context.Context, st store.Store, emailHandler jobs.Handler) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			runOutboxBatch(context.Background(), st, emailHandler)
		}
	}
}

// concurrencyLimiter limits concurrent jobs per type (channel-based semaphore per type).
type concurrencyLimiter struct {
	limit int
	mu    sync.Mutex
	chs   map[string]chan struct{}
}

func newConcurrencyLimiter(perTypeLimit int) *concurrencyLimiter {
	return &concurrencyLimiter{limit: perTypeLimit, chs: make(map[string]chan struct{})}
}

func (c *concurrencyLimiter) getCh(typ string) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.chs[typ]; ok {
		return ch
	}
	ch := make(chan struct{}, c.limit)
	c.chs[typ] = ch
	return ch
}

func (c *concurrencyLimiter) Acquire(typ string) {
	ch := c.getCh(typ)
	ch <- struct{}{}
}

func (c *concurrencyLimiter) Release(typ string) {
	ch := c.getCh(typ)
	<-ch
}

func runOutboxBatch(ctx context.Context, st store.Store, emailHandler jobs.Handler) {
	pending, err := st.ListPendingOutbox(ctx, 10)
	if err != nil {
		slog.Warn("outbox list", "err", err)
		return
	}
	for _, o := range pending {
		var err error
		switch o.Type {
		case jobs.EmailType:
			if emailHandler != nil {
				err = emailHandler.Handle(ctx, o.PayloadJSON)
			} else {
				err = nil
				slog.Info("outbox email skipped (no handler)", "job_id", o.JobID)
			}
		default:
			slog.Warn("outbox unknown type", "type", o.Type, "job_id", o.JobID)
			err = nil
		}
		now := time.Now()
		if err != nil {
			_ = st.UpdateOutboxStatus(ctx, o.ID, "failed", nil)
			slog.Warn("outbox side-effect failed", "id", o.ID, "type", o.Type, "err", err)
		} else {
			_ = st.UpdateOutboxStatus(ctx, o.ID, "sent", &now)
		}
	}
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
