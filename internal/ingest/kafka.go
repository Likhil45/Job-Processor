package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// SubmitFunc submits a job (type + payload bytes) and returns job ID or error.
type SubmitFunc func(ctx context.Context, jobType string, payload []byte) (string, error)

// RunConsumer reads from the given Kafka topic and calls submit for each message.
// It runs until ctx is cancelled.
func RunConsumer(ctx context.Context, brokers []string, topic, groupID string, submit SubmitFunc) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	if groupID == "" {
		groupID = "job-api-ingest"
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		CommitInterval: 0,
	})
	defer r.Close()
	slog.Info("kafka consumer started", "topic", topic, "group", groupID)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("kafka read", "err", err)
			continue
		}
		var req struct {
			Type    string          `json:"type"`
			Payload json.RawMessage  `json:"payload"`
		}
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			slog.Warn("ingest: invalid json", "err", err)
			continue
		}
		if req.Type == "" {
			slog.Warn("ingest: missing type")
			continue
		}
		payload := req.Payload
		if payload == nil {
			payload = []byte("{}")
		}
		id, err := submit(ctx, req.Type, payload)
		if err != nil {
			slog.Warn("ingest: submit failed", "type", req.Type, "err", err)
			continue
		}
		slog.Info("ingest: job submitted", "job_id", id, "type", req.Type)
	}
}
