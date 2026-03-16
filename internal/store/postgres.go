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
		INSERT INTO jobs (id, type, payload, queue, status, attempt, last_error, created_at, updated_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	runAt := job.RunAtUnixSec
	if runAt < 0 {
		runAt = 0
	}
	idempKey := emptyToNull(job.IdempotencyKey)
	_, err := s.db.ExecContext(ctx, query,
		job.ID, job.Type, job.Payload, job.Queue, job.Status, job.Attempt, job.LastError,
		job.CreatedAt, job.UpdatedAt, job.AsynqTaskID, runAt, idempKey, job.Priority,
	)
	return err
}

func emptyToNull(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// GetByID returns a job by ID.
func (s *PostgresStore) GetByID(ctx context.Context, id string) (*JobRecord, error) {
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority
		FROM jobs WHERE id = $1
	`
	var j JobRecord
	var completedAt sql.NullTime
	var idempKey sql.NullString
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID, &j.RunAtUnixSec, &idempKey, &j.Priority,
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
	if idempKey.Valid {
		j.IdempotencyKey = idempKey.String
	}
	return &j, nil
}

// GetByIdempotencyKey returns the job with the given type, queue, and idempotency key, or nil.
func (s *PostgresStore) GetByIdempotencyKey(ctx context.Context, jobType, queue, idempotencyKey string) (*JobRecord, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority
		FROM jobs WHERE type = $1 AND queue = $2 AND idempotency_key = $3
	`
	var j JobRecord
	var completedAt sql.NullTime
	var idempKey sql.NullString
	err := s.db.QueryRowContext(ctx, query, jobType, queue, idempotencyKey).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID, &j.RunAtUnixSec, &idempKey, &j.Priority,
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
	if idempKey.Valid {
		j.IdempotencyKey = idempKey.String
	}
	return &j, nil
}

// List returns jobs with optional queue/status filters.
func (s *PostgresStore) List(ctx context.Context, queue, status string, limit, offset int) ([]*JobRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority
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
		var idempKey sql.NullString
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
			&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID, &j.RunAtUnixSec, &idempKey, &j.Priority,
		); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		if idempKey.Valid {
			j.IdempotencyKey = idempKey.String
		}
		out = append(out, &j)
	}
	return out, rows.Err()
}

// ListScheduledDue returns jobs with status=scheduled and run_at_unix_sec <= runAtBeforeUnix.
func (s *PostgresStore) ListScheduledDue(ctx context.Context, runAtBeforeUnix int64, limit int) ([]*JobRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority
		FROM jobs
		WHERE status = 'scheduled' AND run_at_unix_sec > 0 AND run_at_unix_sec <= $1
		ORDER BY run_at_unix_sec ASC
		LIMIT $2
	`
	rows, err := s.db.QueryContext(ctx, query, runAtBeforeUnix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*JobRecord
	for rows.Next() {
		var j JobRecord
		var completedAt sql.NullTime
		var idempKey sql.NullString
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
			&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID, &j.RunAtUnixSec, &idempKey, &j.Priority,
		); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		if idempKey.Valid {
			j.IdempotencyKey = idempKey.String
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

// CompleteJobWithOutbox updates the job and optionally inserts an outbox row in one transaction.
func (s *PostgresStore) CompleteJobWithOutbox(ctx context.Context, jobID, status, lastError string, attempt int32, completedAt *time.Time, outbox *OutboxRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now()
	var ca interface{}
	if completedAt != nil {
		ca = *completedAt
	} else {
		ca = nil
	}
	_, err = tx.ExecContext(ctx,
		`UPDATE jobs SET status = $1, last_error = $2, attempt = $3, updated_at = $4, completed_at = $5 WHERE id = $6`,
		status, lastError, attempt, now, ca, jobID)
	if err != nil {
		return err
	}
	if outbox != nil {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO job_outbox (job_id, type, payload_json, status) VALUES ($1, $2, $3, 'pending')`,
			outbox.JobID, outbox.Type, outbox.PayloadJSON)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListPendingOutbox returns outbox rows with status=pending, ordered by created_at asc.
func (s *PostgresStore) ListPendingOutbox(ctx context.Context, limit int) ([]*OutboxRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	query := `SELECT id, job_id, type, payload_json, status, created_at, sent_at FROM job_outbox WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*OutboxRecord
	for rows.Next() {
		var o OutboxRecord
		var sentAt sql.NullTime
		if err := rows.Scan(&o.ID, &o.JobID, &o.Type, &o.PayloadJSON, &o.Status, &o.CreatedAt, &sentAt); err != nil {
			return nil, err
		}
		if sentAt.Valid {
			o.SentAt = &sentAt.Time
		}
		out = append(out, &o)
	}
	return out, rows.Err()
}

// UpdateOutboxStatus sets status and sent_at for an outbox row.
func (s *PostgresStore) UpdateOutboxStatus(ctx context.Context, outboxID int64, status string, sentAt *time.Time) error {
	var sa interface{}
	if sentAt != nil {
		sa = *sentAt
	} else {
		sa = nil
	}
	_, err := s.db.ExecContext(ctx, `UPDATE job_outbox SET status = $1, sent_at = $2 WHERE id = $3`, status, sa, outboxID)
	return err
}

// ListArchived returns archived jobs with optional queue and type filter.
func (s *PostgresStore) ListArchived(ctx context.Context, queue, jobType string, limit, offset int) ([]*JobRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	query := `
		SELECT id, type, payload, queue, status, attempt, last_error, created_at, updated_at, completed_at, asynq_task_id, run_at_unix_sec, idempotency_key, priority
		FROM jobs
		WHERE status = 'archived' AND ($1 = '' OR queue = $1) AND ($2 = '' OR type = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4
	`
	rows, err := s.db.QueryContext(ctx, query, queue, jobType, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*JobRecord
	for rows.Next() {
		var j JobRecord
		var completedAt sql.NullTime
		var idempKey sql.NullString
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Queue, &j.Status, &j.Attempt, &j.LastError,
			&j.CreatedAt, &j.UpdatedAt, &completedAt, &j.AsynqTaskID, &j.RunAtUnixSec, &idempKey, &j.Priority,
		); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		if idempKey.Valid {
			j.IdempotencyKey = idempKey.String
		}
		out = append(out, &j)
	}
	return out, rows.Err()
}

// GetQueue returns whether the queue is paused.
func (s *PostgresStore) GetQueue(ctx context.Context, name string) (bool, error) {
	var paused bool
	err := s.db.QueryRowContext(ctx, `SELECT paused FROM queues WHERE name = $1`, name).Scan(&paused)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return paused, err
}

// SetQueuePaused sets the queue's paused flag.
func (s *PostgresStore) SetQueuePaused(ctx context.Context, name string, paused bool) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO queues (name, paused) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET paused = $2`, name, paused)
	return err
}

// ListQueues returns all queues with job counts and paused flag.
func (s *PostgresStore) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue, status, count(*)::bigint FROM jobs GROUP BY queue, status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type key struct{ queue, status string }
	counts := make(map[string]map[string]int64)
	for rows.Next() {
		var queue, status string
		var c int64
		if err := rows.Scan(&queue, &status, &c); err != nil {
			return nil, err
		}
		if counts[queue] == nil {
			counts[queue] = make(map[string]int64)
		}
		counts[queue][status] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	pausedRows, err := s.db.QueryContext(ctx, `SELECT name, paused FROM queues`)
	if err != nil {
		return nil, err
	}
	defer pausedRows.Close()
	paused := make(map[string]bool)
	for pausedRows.Next() {
		var name string
		var p bool
		if err := pausedRows.Scan(&name, &p); err != nil {
			return nil, err
		}
		paused[name] = p
		if counts[name] == nil {
			counts[name] = make(map[string]int64)
		}
	}
	if err := pausedRows.Err(); err != nil {
		return nil, err
	}
	var out []QueueInfo
	for name, st := range counts {
		info := QueueInfo{
			Name:      name,
			Pending:   st["pending"],
			Active:    st["processing"],
			Scheduled: st["scheduled"],
			Retry:     st["failed"],
			Archived:   st["archived"],
			Paused:     paused[name],
		}
		out = append(out, info)
	}
	return out, nil
}

// CreateSchedule inserts a schedule. ID and NextRunAt must be set (or zero for next_run_at = now).
func (s *PostgresStore) CreateSchedule(ctx context.Context, sch *ScheduleRecord) error {
	query := `
		INSERT INTO schedules (name, type, payload_json, cron_expr, queue, max_retry, next_run_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`
	now := time.Now()
	if sch.CreatedAt.IsZero() {
		sch.CreatedAt = now
	}
	if sch.UpdatedAt.IsZero() {
		sch.UpdatedAt = now
	}
	nextRun := sch.NextRunAt
	if nextRun.IsZero() {
		nextRun = now
	}
	payload := sch.PayloadJSON
	if payload == nil {
		payload = []byte("{}")
	}
	return s.db.QueryRowContext(ctx, query,
		sch.Name, sch.Type, payload, sch.CronExpr, sch.Queue, sch.MaxRetry, nextRun, sch.CreatedAt, sch.UpdatedAt,
	).Scan(&sch.ID)
}

// ListSchedules returns all schedules.
func (s *PostgresStore) ListSchedules(ctx context.Context) ([]*ScheduleRecord, error) {
	query := `SELECT id, name, type, payload_json, cron_expr, queue, max_retry, next_run_at, created_at, updated_at FROM schedules ORDER BY name`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ScheduleRecord
	for rows.Next() {
		var sch ScheduleRecord
		if err := rows.Scan(&sch.ID, &sch.Name, &sch.Type, &sch.PayloadJSON, &sch.CronExpr, &sch.Queue, &sch.MaxRetry, &sch.NextRunAt, &sch.CreatedAt, &sch.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &sch)
	}
	return out, rows.Err()
}

// GetScheduleByID returns a schedule by ID.
func (s *PostgresStore) GetScheduleByID(ctx context.Context, id int64) (*ScheduleRecord, error) {
	query := `SELECT id, name, type, payload_json, cron_expr, queue, max_retry, next_run_at, created_at, updated_at FROM schedules WHERE id = $1`
	var sch ScheduleRecord
	err := s.db.QueryRowContext(ctx, query, id).Scan(&sch.ID, &sch.Name, &sch.Type, &sch.PayloadJSON, &sch.CronExpr, &sch.Queue, &sch.MaxRetry, &sch.NextRunAt, &sch.CreatedAt, &sch.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sch, nil
}

// ListSchedulesDue returns schedules with next_run_at <= atOrBefore, ordered by next_run_at asc.
func (s *PostgresStore) ListSchedulesDue(ctx context.Context, atOrBefore time.Time, limit int) ([]*ScheduleRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	query := `SELECT id, name, type, payload_json, cron_expr, queue, max_retry, next_run_at, created_at, updated_at FROM schedules WHERE next_run_at <= $1 ORDER BY next_run_at ASC LIMIT $2`
	rows, err := s.db.QueryContext(ctx, query, atOrBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ScheduleRecord
	for rows.Next() {
		var sch ScheduleRecord
		if err := rows.Scan(&sch.ID, &sch.Name, &sch.Type, &sch.PayloadJSON, &sch.CronExpr, &sch.Queue, &sch.MaxRetry, &sch.NextRunAt, &sch.CreatedAt, &sch.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &sch)
	}
	return out, rows.Err()
}

// UpdateScheduleNextRun sets next_run_at and updated_at for a schedule.
func (s *PostgresStore) UpdateScheduleNextRun(ctx context.Context, id int64, nextRunAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE schedules SET next_run_at = $1, updated_at = $2 WHERE id = $3`, nextRunAt, time.Now(), id)
	return err
}

// DeleteSchedule deletes a schedule by ID.
func (s *PostgresStore) DeleteSchedule(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	return err
}
