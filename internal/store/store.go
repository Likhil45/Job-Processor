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
}
