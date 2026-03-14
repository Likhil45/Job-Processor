package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	jobv1 "github.com/savvients/sip-core/api/proto"
	"github.com/savvients/sip-core/internal/api"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/ingest"
	"github.com/savvients/sip-core/internal/queue"
	"github.com/savvients/sip-core/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	grpcAddr := getEnv("GRPC_ADDR", ":50051")
	metricsAddr := getEnv("METRICS_ADDR", ":9090")
	restAddr := getEnv("REST_ADDR", ":8080")
	postgresDSN := getEnv("POSTGRES_DSN", "")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "")
	kafkaTopic := getEnv("KAFKA_TOPIC", "job.events")
	kafkaConsumerTopic := getEnv("KAFKA_CONSUMER_TOPIC", "")
	kafkaConsumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "job-api-ingest")

	redisOpts := queue.RedisOpts{Addr: redisAddr, Password: redisPassword, DB: redisDB}
	client, err := queue.NewClient(redisOpts)
	if err != nil {
		slog.Error("queue client", "err", err)
		os.Exit(1)
	}
	defer client.Close()

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

	jobServer, err := api.NewJobServer(client, redisOpts, []string{"high", "default", "low"}, jobStore, eventProducer)
	if err != nil {
		slog.Error("job server", "err", err)
		os.Exit(1)
	}
	defer jobServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if kafkaConsumerTopic != "" && kafkaBrokers != "" {
		brokers := splitTrim(kafkaBrokers, ",")
		go ingest.RunConsumer(ctx, brokers, kafkaConsumerTopic, kafkaConsumerGroup, func(ctx context.Context, jobType string, payload []byte) (string, error) {
			resp, err := jobServer.SubmitJob(ctx, &jobv1.SubmitJobRequest{Type: jobType, Payload: payload})
			if err != nil {
				return "", err
			}
			return resp.JobId, nil
		})
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("listen", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}
	defer lis.Close()

	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, jobServer)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)
	reflection.Register(srv)

	// REST API (jobs + admin)
	restHandler := api.NewRESTHandler(jobServer)
	mux := http.NewServeMux()
	mux.Handle("/jobs", restHandler)
	mux.Handle("/jobs/", restHandler)
	mux.Handle("/admin", restHandler)
	mux.Handle("/admin/", restHandler)
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

	go func() {
		slog.Info("gRPC server listening", "addr", grpcAddr)
		if err := srv.Serve(lis); err != nil {
			slog.Error("grpc serve", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down gRPC server")
	srv.GracefulStop()
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
