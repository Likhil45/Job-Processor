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
  asynq_task_id     TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);

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
