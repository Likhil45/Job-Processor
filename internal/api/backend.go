package api

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/store"
)

// JobBackend is the REST-only backend: Kafka for queue, Postgres for metadata.
type JobBackend interface {
	Submit(ctx context.Context, jobType string, payload []byte, queue string, maxRetry int32, runAtUnixSec int64) (jobID string, err error)
	GetStatus(ctx context.Context, jobID string) (status, lastError string, attempt int32, err error)
	ListJobs(ctx context.Context, queue, statusFilter string, limit, offset int) ([]JobInfo, error)
	CancelJob(ctx context.Context, jobID string) error
	ListArchivedJobs(ctx context.Context, queue string, limit int) ([]ArchivedJobInfo, error)
	RetryArchivedJob(ctx context.Context, jobID, queue string) error
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	PauseQueue(ctx context.Context, queue string) error
	UnpauseQueue(ctx context.Context, queue string) error
}

// JobInfo is a single job for list response.
type JobInfo struct {
	JobID             string
	Type              string
	Queue             string
	Status            string
	Attempt           int32
	LastError         string
	CreatedAtUnixSec  int64
	UpdatedAtUnixSec  int64
	CompletedAtUnixSec int64
}

// ArchivedJobInfo for DLQ list.
type ArchivedJobInfo struct {
	JobID     string
	Queue     string
	Type      string
	Payload   []byte
	Attempt   int32
	LastError string
}

// QueueInfo for admin (stubbed when using Kafka only).
type QueueInfo struct {
	Name     string
	Pending  int64
	Active   int64
	Scheduled int64
	Retry    int64
	Archived int64
	Paused   bool
}

// KafkaPostgresBackend implements JobBackend using Kafka and Postgres.
type KafkaPostgresBackend struct {
	store    store.Store
	producer *kafkaqueue.Producer
	events   events.Producer
}

// NewKafkaPostgresBackend returns a backend that requires Postgres and Kafka.
func NewKafkaPostgresBackend(store store.Store, producer *kafkaqueue.Producer, eventProducer events.Producer) *KafkaPostgresBackend {
	if eventProducer == nil {
		eventProducer = events.NoopProducer{}
	}
	return &KafkaPostgresBackend{store: store, producer: producer, events: eventProducer}
}

func (b *KafkaPostgresBackend) Submit(ctx context.Context, jobType string, payload []byte, queue string, maxRetry int32, runAtUnixSec int64) (string, error) {
	if jobType == "" {
		return "", fmt.Errorf("type required")
	}
	if queue == "" {
		queue = "default"
	}
	jobID := uuid.New().String()
	statusStr := "pending"
	if runAtUnixSec > 0 {
		statusStr = "scheduled"
	}
	if err := b.store.Create(ctx, &store.JobRecord{
		ID:           jobID,
		Type:         jobType,
		Payload:      payload,
		Queue:        queue,
		Status:       statusStr,
		AsynqTaskID:  jobID,
		RunAtUnixSec: runAtUnixSec,
	}); err != nil {
		return "", err
	}
	// Delayed jobs: do not enqueue until run_at; the scheduler promoter will enqueue them when due.
	now := time.Now().Unix()
	if runAtUnixSec > 0 && runAtUnixSec > now {
		b.events.Emit(ctx, events.JobEvent{JobID: jobID, Type: jobType, Event: events.EventSubmitted, Queue: queue, Payload: payload})
		return jobID, nil
	}
	if _, err := b.producer.Enqueue(ctx, jobID, jobType, payload, queue, maxRetry, runAtUnixSec, 0); err != nil {
		return "", err
	}
	metrics.JobsEnqueuedTotal.WithLabelValues(jobType, queue).Inc()
	b.events.Emit(ctx, events.JobEvent{JobID: jobID, Type: jobType, Event: events.EventSubmitted, Queue: queue, Payload: payload})
	return jobID, nil
}

func (b *KafkaPostgresBackend) GetStatus(ctx context.Context, jobID string) (status, lastError string, attempt int32, err error) {
	if jobID == "" {
		return "", "", 0, fmt.Errorf("job_id required")
	}
	rec, err := b.store.GetByID(ctx, jobID)
	if err != nil {
		return "", "", 0, err
	}
	if rec == nil {
		return "unknown", "", 0, nil
	}
	return rec.Status, rec.LastError, rec.Attempt, nil
}

func (b *KafkaPostgresBackend) ListJobs(ctx context.Context, queue, statusFilter string, limit, offset int) ([]JobInfo, error) {
	recs, err := b.store.List(ctx, queue, statusFilter, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]JobInfo, 0, len(recs))
	for _, r := range recs {
		var completedAt int64
		if r.CompletedAt != nil {
			completedAt = r.CompletedAt.Unix()
		}
		out = append(out, JobInfo{
			JobID:             r.ID,
			Type:              r.Type,
			Queue:             r.Queue,
			Status:            r.Status,
			Attempt:           r.Attempt,
			LastError:         r.LastError,
			CreatedAtUnixSec:  r.CreatedAt.Unix(),
			UpdatedAtUnixSec:  r.UpdatedAt.Unix(),
			CompletedAtUnixSec: completedAt,
		})
	}
	return out, nil
}

func (b *KafkaPostgresBackend) CancelJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("job_id required")
	}
	rec, err := b.store.GetByID(ctx, jobID)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("task not found or already running/completed (cannot cancel)")
	}
	if rec.Status == "completed" || rec.Status == "processing" {
		return fmt.Errorf("task not found or already running/completed (cannot cancel)")
	}
	if err := b.store.UpdateStatus(ctx, jobID, "cancelled", "", 0, nil); err != nil {
		return err
	}
	b.events.Emit(ctx, events.JobEvent{JobID: jobID, Type: rec.Type, Event: events.EventCancelled, Queue: rec.Queue})
	return nil
}

func (b *KafkaPostgresBackend) ListArchivedJobs(ctx context.Context, queue string, limit int) ([]ArchivedJobInfo, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	recs, err := b.store.List(ctx, queue, "archived", limit, 0)
	if err != nil {
		return nil, err
	}
	out := make([]ArchivedJobInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, ArchivedJobInfo{
			JobID:     r.ID,
			Queue:     r.Queue,
			Type:      r.Type,
			Payload:   r.Payload,
			Attempt:   r.Attempt,
			LastError: r.LastError,
		})
	}
	return out, nil
}

func (b *KafkaPostgresBackend) RetryArchivedJob(ctx context.Context, jobID, queue string) error {
	if jobID == "" {
		return fmt.Errorf("job_id required")
	}
	if queue == "" {
		queue = "default"
	}
	rec, err := b.store.GetByID(ctx, jobID)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("job not found")
	}
	if rec.Status != "archived" {
		return fmt.Errorf("job is not archived")
	}
	newID, err := b.producer.Enqueue(ctx, "", rec.Type, rec.Payload, queue, 0, 0, 0)
	if err != nil {
		return err
	}
	return b.store.Create(ctx, &store.JobRecord{
		ID:          newID,
		Type:        rec.Type,
		Payload:     rec.Payload,
		Queue:       queue,
		Status:      "pending",
		AsynqTaskID: newID,
	})
}

func (b *KafkaPostgresBackend) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	// Kafka-only: no Redis queues. Return empty or stub.
	return []QueueInfo{}, nil
}

func (b *KafkaPostgresBackend) PauseQueue(ctx context.Context, queue string) error {
	return fmt.Errorf("pause queue not supported in Kafka-only mode")
}

func (b *KafkaPostgresBackend) UnpauseQueue(ctx context.Context, queue string) error {
	return fmt.Errorf("unpause queue not supported in Kafka-only mode")
}
