package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

const defaultTopic = "job.events"

// Producer is the interface for publishing job lifecycle events.
type Producer interface {
	Emit(ctx context.Context, ev JobEvent)
	Close() error
}

// NoopProducer is a no-op producer when Kafka is not configured.
type NoopProducer struct{}

func (NoopProducer) Emit(context.Context, JobEvent) {}
func (NoopProducer) Close() error                   { return nil }

// KafkaProducer publishes job lifecycle events to Kafka.
type KafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

// KafkaConfig holds Kafka producer configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// NewKafkaProducer creates a Kafka producer. Call Close when done.
func NewKafkaProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9092"}
	}
	if cfg.Topic == "" {
		cfg.Topic = defaultTopic
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	return &KafkaProducer{writer: writer, topic: cfg.Topic}, nil
}

// Emit sends a job event to Kafka. Non-blocking; logs errors.
func (p *KafkaProducer) Emit(ctx context.Context, ev JobEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		slog.Warn("events: marshal", "err", err)
		return
	}
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(ev.JobID),
		Value: payload,
	})
	if err != nil {
		slog.Warn("events: kafka write", "job_id", ev.JobID, "event", ev.Event, "err", err)
	}
}

// Close closes the Kafka writer.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
