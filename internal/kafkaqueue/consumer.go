package kafkaqueue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProcessFunc processes a job request. Return nil on success; non-nil to indicate failure (caller may retry or DLQ).
type ProcessFunc func(ctx context.Context, req *JobRequest) error

// RunConsumer reads from the job.requests topic and calls process for each message.
// Commits offset after process returns. On process error the message is not retried here (caller may re-produce inside process).
func RunConsumer(ctx context.Context, brokers []string, topic, groupID string, process ProcessFunc) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	if topic == "" {
		topic = defaultJobsTopic
	}
	if groupID == "" {
		groupID = "job-worker"
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
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
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("fetch message", "err", err)
			continue
		}
		var req JobRequest
		if err := json.Unmarshal(m.Value, &req); err != nil {
			slog.Warn("invalid message", "err", err, "offset", m.Offset)
			_ = r.CommitMessages(ctx, m)
			continue
		}
		if err := process(ctx, &req); err != nil {
			slog.Warn("process job failed", "job_id", req.JobID, "type", req.Type, "err", err)
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			slog.Warn("commit failed", "err", err)
		}
	}
}
