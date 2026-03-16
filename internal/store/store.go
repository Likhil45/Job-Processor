package store

import (
	"context"
	"time"
)

// JobRecord holds job metadata for persistence.
type JobRecord struct {
	ID               string
	Type             string
	Payload          []byte
	Queue            string
	Status           string // pending, scheduled, processing, completed, failed, archived, cancelled
	Attempt          int32
	LastError        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      *time.Time
	AsynqTaskID      string // same as ID when using Asynq; for correlation
	RunAtUnixSec     int64  // when to run (0 = immediate); used for delayed/scheduled jobs
	IdempotencyKey   string // optional; duplicate submit with same type+queue+key returns existing job
	Priority         int32  // 0=default, higher=processed first by worker
}

// Store is the interface for job metadata persistence.
type Store interface {
	// Create persists a new job record (e.g. on submit).
	Create(ctx context.Context, job *JobRecord) error
	// GetByID returns a job by ID, or nil if not found.
	GetByID(ctx context.Context, id string) (*JobRecord, error)
	// List returns jobs with optional filters, ordered by created_at desc.
	List(ctx context.Context, queue, status string, limit, offset int) ([]*JobRecord, error)
	// UpdateStatus updates status, attempt, last_error, updated_at; optionally completed_at.
	UpdateStatus(ctx context.Context, id, status, lastError string, attempt int32, completedAt *time.Time) error
	// ListScheduledDue returns jobs with status=scheduled and run_at_unix_sec <= runAtBeforeUnix (for promoter).
	ListScheduledDue(ctx context.Context, runAtBeforeUnix int64, limit int) ([]*JobRecord, error)
	// GetByIdempotencyKey returns the job with the given type, queue, and idempotency key, or nil.
	GetByIdempotencyKey(ctx context.Context, jobType, queue, idempotencyKey string) (*JobRecord, error)

	// CompleteJobWithOutbox updates the job status and optionally appends an outbox row in one transaction.
	CompleteJobWithOutbox(ctx context.Context, jobID, status, lastError string, attempt int32, completedAt *time.Time, outbox *OutboxRecord) error
	// ListPendingOutbox returns outbox rows with status=pending, ordered by created_at asc, limited.
	ListPendingOutbox(ctx context.Context, limit int) ([]*OutboxRecord, error)
	// UpdateOutboxStatus sets status and sent_at for an outbox row.
	UpdateOutboxStatus(ctx context.Context, outboxID int64, status string, sentAt *time.Time) error

	// GetQueue returns whether the queue is paused. If the queue is not in the table, returns false, nil.
	GetQueue(ctx context.Context, name string) (paused bool, err error)
	// SetQueuePaused sets the queue's paused flag (creates queue row if not exists).
	SetQueuePaused(ctx context.Context, name string, paused bool) error
	// ListQueues returns all queues with job counts and paused flag (from jobs + queues tables).
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	// ListArchived returns archived jobs with optional queue and type filter, limit and offset.
	ListArchived(ctx context.Context, queue, jobType string, limit, offset int) ([]*JobRecord, error)

	// Schedules for recurring (cron) jobs.
	CreateSchedule(ctx context.Context, s *ScheduleRecord) error
	ListSchedules(ctx context.Context) ([]*ScheduleRecord, error)
	GetScheduleByID(ctx context.Context, id int64) (*ScheduleRecord, error)
	ListSchedulesDue(ctx context.Context, atOrBefore time.Time, limit int) ([]*ScheduleRecord, error)
	UpdateScheduleNextRun(ctx context.Context, id int64, nextRunAt time.Time) error
	DeleteSchedule(ctx context.Context, id int64) error
}

// ScheduleRecord is a single recurring schedule.
type ScheduleRecord struct {
	ID         int64
	Name       string
	Type       string
	PayloadJSON []byte
	CronExpr   string
	Queue      string
	MaxRetry   int32
	NextRunAt  time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// QueueInfo holds queue name, counts by status, and paused flag.
type QueueInfo struct {
	Name      string
	Pending   int64
	Active    int64
	Scheduled int64
	Retry     int64
	Archived  int64
	Paused    bool
}

// OutboxRecord is a single outbox row for exactly-once side-effects (e.g. send email).
type OutboxRecord struct {
	ID          int64
	JobID       string
	Type        string
	PayloadJSON []byte
	Status      string // pending, sent, failed
	CreatedAt   time.Time
	SentAt      *time.Time
}
