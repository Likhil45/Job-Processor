package store

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

// PostgresStore implements Store using PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore opens a connection and returns a PostgresStore. Call Close when done.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

// Close closes the database connection.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// Ping verifies the database connection is alive (for readiness probes).
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Create inserts a new job record.
func (s *PostgresStore) Create(ctx context.Context, job *JobRecord) error {
	query := `
		INSERT INTO jobs (id, type, payload, queue, status, attempt, last_error, created_at, updated_at, asynq_task_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	_, err := s.db.ExecContext(ctx, query,
		job.ID, job.Type, job.Payload, job.Queue, job.Status, job.Attempt, job.LastError,
		job.CreatedAt, job.UpdatedAt, job.AsynqTaskID,
	)
	return err
}

// GetByID returns a job by ID.
func (s *PostgresStore) GetByID(ctx context.Context, id string) (*JobRecord, error) {
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id
		FROM jobs WHERE id = $1
	`
	var j JobRecord
	var completedAt sql.NullTime
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	return &j, nil
}

// List returns jobs with optional queue/status filters.
func (s *PostgresStore) List(ctx context.Context, queue, status string, limit, offset int) ([]*JobRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id
		FROM jobs
		WHERE ($1 = '' OR queue = $1) AND ($2 = '' OR jobs.status = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4
	`
	rows, err := s.db.QueryContext(ctx, query, queue, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*JobRecord
	for rows.Next() {
		var j JobRecord
		var completedAt sql.NullTime
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
			&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID,
		); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		out = append(out, &j)
	}
	return out, rows.Err()
}

// UpdateStatus updates job status and related fields.
func (s *PostgresStore) UpdateStatus(ctx context.Context, id, status, lastError string, attempt int32, completedAt *time.Time) error {
	query := `
		UPDATE jobs SET status = $1, last_error = $2, attempt = $3, updated_at = $4, completed_at = $5
		WHERE id = $6
	`
	now := time.Now()
	var ca interface{}
	if completedAt != nil {
		ca = *completedAt
	} else {
		ca = nil
	}
	_, err := s.db.ExecContext(ctx, query, status, lastError, attempt, now, ca, id)
	return err
}
