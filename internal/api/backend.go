package api

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/store"
)

// JobBackend is the REST-only backend: Kafka for queue, Postgres for metadata.
type JobBackend interface {
	Submit(ctx context.Context, jobType string, payload []byte, queue string, maxRetry int32, runAtUnixSec int64, idempotencyKey string, priority int32) (jobID string, err error)
	GetStatus(ctx context.Context, jobID string) (status, lastError string, attempt int32, err error)
	ListJobs(ctx context.Context, queue, statusFilter string, limit, offset int) ([]JobInfo, error)
	CancelJob(ctx context.Context, jobID string) error
	ListArchivedJobs(ctx context.Context, queue, jobType string, limit, offset int) ([]ArchivedJobInfo, error)
	GetArchivedJob(ctx context.Context, jobID string) (*ArchivedJobInfo, error)
	RetryArchivedJob(ctx context.Context, jobID, queue string) error
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	PauseQueue(ctx context.Context, queue string) error
	UnpauseQueue(ctx context.Context, queue string) error

	CreateSchedule(ctx context.Context, name, jobType string, payload []byte, cronExpr, queue string, maxRetry int32) (id int64, err error)
	ListSchedules(ctx context.Context) ([]ScheduleInfo, error)
	DeleteSchedule(ctx context.Context, id int64) error
}

// ScheduleInfo is a schedule for list response.
type ScheduleInfo struct {
	ID         int64
	Name       string
	Type       string
	CronExpr   string
	Queue      string
	MaxRetry   int32
	NextRunAt  time.Time
	CreatedAt  time.Time
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
	JobID            string
	Queue            string
	Type             string
	Payload          []byte
	Attempt          int32
	LastError        string
	CreatedAtUnixSec int64
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

func (b *KafkaPostgresBackend) Submit(ctx context.Context, jobType string, payload []byte, queue string, maxRetry int32, runAtUnixSec int64, idempotencyKey string, priority int32) (string, error) {
	if jobType == "" {
		return "", fmt.Errorf("type required")
	}
	if queue == "" {
		queue = "default"
	}
	if idempotencyKey != "" {
		existing, err := b.store.GetByIdempotencyKey(ctx, jobType, queue, idempotencyKey)
		if err != nil {
			return "", err
		}
		if existing != nil {
			return existing.ID, nil
		}
	}
	jobID := uuid.New().String()
	statusStr := "pending"
	if runAtUnixSec > 0 {
		statusStr = "scheduled"
	}
	if err := b.store.Create(ctx, &store.JobRecord{
		ID:             jobID,
		Type:           jobType,
		Payload:        payload,
		Queue:          queue,
		Status:         statusStr,
		AsynqTaskID:    jobID,
		RunAtUnixSec:   runAtUnixSec,
		IdempotencyKey: idempotencyKey,
		Priority:       priority,
	}); err != nil {
		return "", err
	}
	// Delayed jobs: do not enqueue until run_at; the scheduler promoter will enqueue them when due.
	now := time.Now().Unix()
	if runAtUnixSec > 0 && runAtUnixSec > now {
		b.events.Emit(ctx, events.JobEvent{JobID: jobID, Type: jobType, Event: events.EventSubmitted, Queue: queue, Payload: payload})
		return jobID, nil
	}
	if _, err := b.producer.Enqueue(ctx, jobID, jobType, payload, queue, maxRetry, runAtUnixSec, 0, priority); err != nil {
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

func (b *KafkaPostgresBackend) ListArchivedJobs(ctx context.Context, queue, jobType string, limit, offset int) ([]ArchivedJobInfo, error) {
	recs, err := b.store.ListArchived(ctx, queue, jobType, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]ArchivedJobInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, ArchivedJobInfo{
			JobID:            r.ID,
			Queue:            r.Queue,
			Type:             r.Type,
			Payload:          r.Payload,
			Attempt:          r.Attempt,
			LastError:        r.LastError,
			CreatedAtUnixSec: r.CreatedAt.Unix(),
		})
	}
	return out, nil
}

func (b *KafkaPostgresBackend) GetArchivedJob(ctx context.Context, jobID string) (*ArchivedJobInfo, error) {
	rec, err := b.store.GetByID(ctx, jobID)
	if err != nil || rec == nil {
		return nil, nil
	}
	if rec.Status != "archived" {
		return nil, nil
	}
	return &ArchivedJobInfo{
		JobID:            rec.ID,
		Queue:            rec.Queue,
		Type:             rec.Type,
		Payload:          rec.Payload,
		Attempt:          rec.Attempt,
		LastError:        rec.LastError,
		CreatedAtUnixSec: rec.CreatedAt.Unix(),
	}, nil
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
	newID, err := b.producer.Enqueue(ctx, "", rec.Type, rec.Payload, queue, 0, 0, 0, rec.Priority)
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
		Priority:    rec.Priority,
	})
}

func (b *KafkaPostgresBackend) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	infos, err := b.store.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]QueueInfo, 0, len(infos))
	for _, q := range infos {
		out = append(out, QueueInfo{
			Name:      q.Name,
			Pending:   q.Pending,
			Active:    q.Active,
			Scheduled: q.Scheduled,
			Retry:     q.Retry,
			Archived:  q.Archived,
			Paused:    q.Paused,
		})
	}
	return out, nil
}

func (b *KafkaPostgresBackend) PauseQueue(ctx context.Context, queue string) error {
	if queue == "" {
		return fmt.Errorf("queue name required")
	}
	return b.store.SetQueuePaused(ctx, queue, true)
}

func (b *KafkaPostgresBackend) UnpauseQueue(ctx context.Context, queue string) error {
	if queue == "" {
		return fmt.Errorf("queue name required")
	}
	return b.store.SetQueuePaused(ctx, queue, false)
}

func (b *KafkaPostgresBackend) CreateSchedule(ctx context.Context, name, jobType string, payload []byte, cronExpr, queue string, maxRetry int32) (int64, error) {
	if name == "" || jobType == "" || cronExpr == "" {
		return 0, fmt.Errorf("name, type, and cron_expr required")
	}
	if queue == "" {
		queue = "default"
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return 0, fmt.Errorf("invalid cron_expr: %w", err)
	}
	now := time.Now()
	sch := &store.ScheduleRecord{
		Name:        name,
		Type:        jobType,
		PayloadJSON: payload,
		CronExpr:    cronExpr,
		Queue:       queue,
		MaxRetry:    maxRetry,
		NextRunAt:   sched.Next(now),
	}
	if err := b.store.CreateSchedule(ctx, sch); err != nil {
		return 0, err
	}
	return sch.ID, nil
}

func (b *KafkaPostgresBackend) ListSchedules(ctx context.Context) ([]ScheduleInfo, error) {
	recs, err := b.store.ListSchedules(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, ScheduleInfo{
			ID:        r.ID,
			Name:      r.Name,
			Type:      r.Type,
			CronExpr:  r.CronExpr,
			Queue:     r.Queue,
			MaxRetry:  r.MaxRetry,
			NextRunAt: r.NextRunAt,
			CreatedAt: r.CreatedAt,
		})
	}
	return out, nil
}

func (b *KafkaPostgresBackend) DeleteSchedule(ctx context.Context, id int64) error {
	return b.store.DeleteSchedule(ctx, id)
}
