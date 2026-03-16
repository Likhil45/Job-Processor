-- Job metadata and execution history for the distributed job processing system.
-- Run against your Postgres database (e.g. psql -f schema.sql).

CREATE TABLE IF NOT EXISTS jobs (
  id                TEXT PRIMARY KEY,
  type              TEXT NOT NULL,
  payload           BYTEA,
  queue             TEXT NOT NULL DEFAULT 'default',
  status            TEXT NOT NULL DEFAULT 'pending',
  attempt           INT NOT NULL DEFAULT 0,
  last_error        TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at      TIMESTAMPTZ,
  asynq_task_id     TEXT NOT NULL DEFAULT '',
  run_at_unix_sec   BIGINT NOT NULL DEFAULT 0,
  idempotency_key   TEXT,
  priority          INT NOT NULL DEFAULT 0
);

-- Backfill run_at for existing rows (optional; new deployments skip).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS run_at_unix_sec BIGINT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs (type, queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON jobs(status, run_at_unix_sec) WHERE status = 'scheduled';

-- Optional: job_events for full audit trail (Phase 2 can populate via Kafka consumer).
CREATE TABLE IF NOT EXISTS job_events (
  id         BIGSERIAL PRIMARY KEY,
  job_id     TEXT NOT NULL,
  event      TEXT NOT NULL,
  payload    JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);
CREATE INDEX IF NOT EXISTS idx_job_events_created_at ON job_events(created_at DESC);

-- Outbox for exactly-once side-effects (e.g. send email after job completes).
CREATE TABLE IF NOT EXISTS job_outbox (
  id           BIGSERIAL PRIMARY KEY,
  job_id       TEXT NOT NULL,
  type         TEXT NOT NULL,
  payload_json BYTEA NOT NULL,
  status       TEXT NOT NULL DEFAULT 'pending',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sent_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_job_outbox_status ON job_outbox(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_outbox_created_at ON job_outbox(created_at ASC);

-- Queues metadata for pause/unpause and visibility.
CREATE TABLE IF NOT EXISTS queues (
  name   TEXT PRIMARY KEY,
  paused BOOLEAN NOT NULL DEFAULT false
);
INSERT INTO queues (name, paused) VALUES ('default', false) ON CONFLICT (name) DO NOTHING;

-- Recurring (cron-style) job schedules.
CREATE TABLE IF NOT EXISTS schedules (
  id           BIGSERIAL PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  type         TEXT NOT NULL,
  payload_json BYTEA NOT NULL DEFAULT '{}',
  cron_expr    TEXT NOT NULL,
  queue        TEXT NOT NULL DEFAULT 'default',
  max_retry    INT NOT NULL DEFAULT 0,
  next_run_at  TIMESTAMPTZ NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at ON schedules(next_run_at);
