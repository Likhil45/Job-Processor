package kafkaqueue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const defaultJobsTopic = "job.requests"

// JobRequest is the message value produced to job.requests.
type JobRequest struct {
	JobID   string `json:"job_id"`
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
	Queue   string `json:"queue,omitempty"`
	Attempt int32  `json:"attempt,omitempty"`
	Options *struct {
		MaxRetry     int32 `json:"max_retry,omitempty"`
		RunAtUnixSec int64 `json:"run_at_unix_sec,omitempty"`
		Priority     int32 `json:"priority,omitempty"`
	} `json:"options,omitempty"`
}

// Producer produces job request messages to Kafka.
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// Config for the Kafka job producer.
type Config struct {
	Brokers []string
	Topic   string
}

// NewProducer creates a producer. Call Close when done.
func NewProducer(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9092"}
	}
	if cfg.Topic == "" {
		cfg.Topic = defaultJobsTopic
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	return &Producer{writer: writer, topic: cfg.Topic}, nil
}

// Enqueue produces a job request. If jobID is empty, a new UUID is generated. attempt is used for retries (0 for new jobs). priority: 0=default, higher=processed first.
// Returns the job ID.
func (p *Producer) Enqueue(ctx context.Context, jobID, jobType string, payload []byte, queue string, maxRetry int32, runAtUnixSec int64, attempt int32, priority int32) (string, error) {
	if jobID == "" {
		jobID = uuid.New().String()
	}
	req := JobRequest{
		JobID:   jobID,
		Type:    jobType,
		Payload: payload,
		Queue:   queue,
		Attempt: attempt,
	}
	if maxRetry > 0 || runAtUnixSec > 0 || priority != 0 {
		req.Options = &struct {
			MaxRetry     int32 `json:"max_retry,omitempty"`
			RunAtUnixSec int64 `json:"run_at_unix_sec,omitempty"`
			Priority     int32 `json:"priority,omitempty"`
		}{MaxRetry: maxRetry, RunAtUnixSec: runAtUnixSec, Priority: priority}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	msg := kafka.Message{Key: []byte(jobID), Value: body}
	if prop := otel.GetTextMapPropagator(); prop != nil {
		carrier := make(propagation.MapCarrier)
		prop.Inject(ctx, carrier)
		for k, v := range carrier {
			msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
		}
	}
	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		slog.Warn("kafkaqueue: write failed", "job_id", jobID, "err", err)
		return "", err
	}
	return jobID, nil
}

// Close closes the writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
