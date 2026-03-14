## Distributed Job Processing (Minikube)

This repo includes a phased implementation of a **distributed job processing system** for local development on Minikube.

**→ [How to use this repo](docs/HOW_TO_USE.md)** — quick start (local or Docker), submitting jobs, checking status, and admin.  
**→ [How user-service and billing-service are used](docs/UPSTREAM_SERVICES.md)** — demo upstream services that submit jobs to the Job API.  
**→ [Fake job scheduler](docs/SCHEDULER.md)** — periodic fake job generator (email, image, invoice, report) that enqueues to Kafka + Postgres.

### Features

- **Job queue**: Kafka-only (topic `job.requests`); no Redis
- **REST-only API**: Submit jobs, get status, list jobs, cancel, retry archived (DLQ); Admin: list queues (stubbed), pause/unpause (not supported in Kafka-only mode)
- **PostgreSQL**: Required for job metadata, status, and execution history
- **Kafka**: Job requests (topic `job.requests`), lifecycle events (topic `job.events`). Runs in **KRaft mode** (no Zookeeper; Zookeeper is deprecated).
- **Retries**: Configurable max retries; failed jobs re-enqueued to Kafka; exhausted jobs archived
- **Job types**: hello, email, image (resize), invoice (template), report (CSV)
- **Observability**: Structured logging (slog), Prometheus metrics (`/metrics`), health probes
- **Upstream demos**: user-service (register, password-reset), billing-service (invoice, report, email)

### Quick start (local)

1. **Start Postgres and Kafka** (Kafka in KRaft mode — no Zookeeper). Example with Docker:

   ```bash
   docker run -d --name postgres -e POSTGRES_USER=jobs -e POSTGRES_PASSWORD=jobs -e POSTGRES_DB=jobs -p 5432:5432 postgres:15-alpine
   # KRaft: single node as controller+broker (Zookeeper deprecated)
   docker run -d --name kafka -p 9092:9092 -e KAFKA_CFG_NODE_ID=0 -e KAFKA_CFG_PROCESS_ROLES=controller,broker -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 bitnami/kafka:latest
   ```

2. **Apply Postgres schema** (once):

   ```bash
   psql -U jobs -h localhost jobs -f deploy/postgres/schema.sql
   ```

3. **Run API and worker** (require `POSTGRES_DSN` and `KAFKA_BROKERS`):

   ```bash
   set POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
   set KAFKA_BROKERS=localhost:9092
   go run ./cmd/api &
   go run ./cmd/worker &
   ```

4. **Submit a job** (REST or enqueue CLI):

   ```bash
   curl -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d "{\"type\":\"hello\",\"payload\":\"world\"}"
   # Or: set JOB_API_URL=http://localhost:8080 && go run ./cmd/enqueue hello "world"
   ```

### Deploy to Minikube

Minikube manifests in `deploy/minikube` reference Redis and need to be updated for Kafka + Postgres. Use the Docker Compose demo stack for a full local run (Kafka, Postgres, API, worker).

### Demo (one-command run)

Run the full stack locally (Kafka, Postgres, MailHog, Job API, worker, scheduler, dashboard). Requires [Docker](https://docs.docker.com/get-docker/).

1. **Start everything**:

   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```

2. **Apply Postgres schema** (once):

   ```bash
   docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
   ```

3. **Run the demo script** (submits hello, email, report jobs; prints job IDs):
   ```bash
   ./scripts/demo.sh
   ```
   On Windows use the dashboard or Git Bash for `demo.sh`.

4. **See results**
   - **Dashboard:** http://localhost:8080 — submit jobs, check status, and run **demo scenarios (A–E)** to see normal flow, retry/recovery, delayed scheduling, DLQ, and worker crash recovery.
   - **REST API:** http://localhost:8083 — POST/GET /jobs, GET /jobs/:id, POST /jobs/:id/retry, POST /jobs/:id/cancel, GET /admin/queues.
   - **User service:** http://localhost:8081 — POST /register, POST /password-reset.
   - **Billing service:** http://localhost:8082 — POST /invoice, POST /report, POST /invoice-ready.
   - **Scheduler:** Runs in the stack; generates fake jobs every 2m. Health/metrics: http://localhost:9091. See [docs/SCHEDULER.md](docs/SCHEDULER.md).
   - **Logs (Loki):** Loki at http://localhost:3100; Promtail ships container logs to Loki. Add Grafana (e.g. port 3000) and add a Loki datasource `http://loki:3100` to query logs in Explore.
   - **Emails:** http://localhost:8025 (MailHog).
   - **Report:** `./out/demo-report.csv` (after report job runs).

5. **Stop the stack**

   ```bash
   docker compose -f deploy/demo/docker-compose.yml down
   ```

### Project layout

- `cmd/api` – REST-only server (Kafka + Postgres backend): submit, status, list, cancel, retry, admin
- `cmd/worker` – Kafka consumer for `job.requests`; runs job handlers; updates Postgres and emits to `job.events`
- `cmd/enqueue` – CLI to enqueue a single job via REST (requires `JOB_API_URL`)
- `cmd/demo-server` – Demo dashboard (HTTP UI + proxy to Job API REST)
- `cmd/user-service` – Demo upstream: POST /register, /password-reset
- `cmd/billing-service` – Demo upstream: POST /invoice, /report, /invoice-ready
- `cmd/scheduler` – Periodic fake job generator; writes to Kafka + Postgres (same topic/table as API). See [docs/SCHEDULER.md](docs/SCHEDULER.md).
- `scripts/demo.sh` – Demo script: submits hello, email, report jobs
- `deploy/demo` – Docker Compose (Kafka, Postgres, MailHog, API, worker, user-service, billing-service, dashboard)
- `deploy/postgres` – Postgres schema for job metadata
- `internal/kafkaqueue` – Kafka producer (job.requests) and consumer for workers
- `internal/jobs` – Job handlers (hello, email, image, invoice, report)
- `internal/api` – REST handlers and JobBackend (Kafka + Postgres)
- `internal/store` – Job metadata store (Postgres)
- `internal/events` – Kafka job lifecycle event producer
- `internal/metrics` – Prometheus counters/histograms
- `api/proto` – Protobuf (retained for reference; API is REST-only)
- `deploy/minikube` – Kubernetes manifests (to be updated for Kafka)

### Environment variables

| Variable             | Default     | Description                                           |
| -------------------- | ----------- | ----------------------------------------------------- |
| REST_ADDR            | :8080       | REST API listen address (API)                         |
| METRICS_ADDR         | :9090       | HTTP metrics listen address (API)                     |
| POSTGRES_DSN         | (required)  | Postgres DSN for job metadata (API and worker)        |
| KAFKA_BROKERS        | (required)  | Kafka brokers (API: job.requests producer; worker: consumer + job.events) |
| KAFKA_TOPIC          | job.events  | Kafka topic for lifecycle events                      |
| KAFKA_JOBS_TOPIC     | job.requests| Kafka topic for job queue (API produces, worker consumes) |
| KAFKA_CONSUMER_GROUP | job-worker  | Consumer group for worker                              |
| SMTP_ADDR            | (empty)     | SMTP for email jobs (e.g. mailhog:1025)              |
| EMAIL_FROM           | noreply@localhost | From address for email jobs                        |
| JOB_API_URL          | (empty)     | Job API REST base URL (enqueue, demo-server, user-service, billing-service) |
| SCHEDULER_INTERVAL   | 2m          | How often the scheduler generates a round of fake jobs (scheduler only) |
| SCHEDULER_HTTP_ADDR  | :9091       | Health and metrics listen address (scheduler only) |

### cURL examples

See [docs/CURL_EXAMPLES.md](docs/CURL_EXAMPLES.md) for curl commands for all REST endpoints: submit job, get status, list jobs, retry, cancel, and admin (list queues, pause/unpause), plus user-service and billing-service.

### Proto regeneration

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/job_service.proto
```

Requires: `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` (install via `go install`).
