# Fake Job Scheduler

The **job scheduler** is a lightweight microservice that periodically generates fake jobs (email, image, invoice, report) and enqueues them into the same Kafka topic and Postgres schema used by the Job API. Existing workers consume and process these jobs; no changes to the worker or API are required.

## Architecture

- **Postgres**: Creates job records (same `jobs` table as the API) so workers can load job metadata by ID.
- **Kafka**: Produces to `job.requests` (same topic the API uses). Workers consume from this topic.
- **No REST**: The scheduler does not call the Job API; it writes directly to Postgres and Kafka.

## Running the scheduler

### Prerequisites

- Postgres and Kafka running (same as for the Job API and worker).
- Postgres schema applied (`deploy/postgres/schema.sql`).

### Environment variables

| Variable              | Default       | Description                          |
| --------------------- | ------------- | ------------------------------------ |
| POSTGRES_DSN          | (required)    | Postgres connection string           |
| KAFKA_BROKERS         | (required)    | Comma-separated Kafka broker list    |
| KAFKA_JOBS_TOPIC      | job.requests  | Kafka topic for job requests         |
| SCHEDULER_INTERVAL    | 2m            | How often to generate a round of jobs |
| SCHEDULER_HTTP_ADDR   | :9091         | HTTP server for health and metrics   |

### Local (go run)

```bash
set POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
set KAFKA_BROKERS=localhost:9092
go run ./cmd/scheduler
```

Optional: set `SCHEDULER_INTERVAL=1m` for more frequent runs.

### Docker Compose (demo stack)

The demo stack includes a `job-scheduler` service. After bringing up the stack:

```bash
docker compose -f deploy/demo/docker-compose.yml up -d
# Apply schema if not done yet, then scheduler will run and create jobs every 2m
```

Scheduler HTTP (health/metrics) is on port **9091**.

## Endpoints

- **GET /health** — Always 200 (liveness).
- **GET /ready** — 200 if Postgres is reachable, else 503 (readiness).
- **GET /metrics** — Prometheus metrics (includes `jobs_scheduled_total` by type).

## Job types generated

Each tick generates one fake job per type:

| Type   | Payload content (fake)                    |
| ------ | ----------------------------------------- |
| email  | Random to, subject, body                  |
| image  | Source URL or path, width/height          |
| invoice| Template + random ID, total, customer     |
| report | Headers + a few rows of fake data         |

Payload shapes match the handlers in `internal/jobs` (email, image, invoice, report). Retries: each job is submitted with `max_retry=2`.

## Verifying that jobs are processed

1. Start Postgres, Kafka, Job API, and worker (and optionally the scheduler).
2. Run the scheduler (or wait for the next interval if it is already running).
3. List recent jobs or check status via the Job API:

   ```bash
   curl "http://localhost:8083/jobs?limit=10"
   ```

4. Filter by status:

   ```bash
   curl "http://localhost:8083/jobs?status=completed&limit=5"
   ```

5. After an email job runs, check MailHog (e.g. http://localhost:8025) if SMTP is configured. Report jobs write to the worker’s output directory (e.g. `./out` in the demo).

Fake jobs are processed by the same worker and handlers as API-submitted jobs; see `cmd/worker` and `internal/jobs`.
