## Distributed Job Processing (Minikube)

This repo includes a phased implementation of a **distributed job processing system** for local development on Minikube.

**→ [How to use this repo](docs/HOW_TO_USE.md)** — quick start (local or Docker), submitting jobs, checking status, and admin.  
**→ [How user-service and billing-service are used](docs/UPSTREAM_SERVICES.md)** — demo upstream services that submit jobs to the Job API.

### Features

- **Job queues**: FIFO and priority (high / default / low)
- **Retries**: Exponential backoff via [Asynq](https://github.com/hibiken/asynq)
- **Delayed jobs**: Schedule with `run_at_unix_sec` in `JobOptions`
- **Worker pool**: Configurable concurrency and horizontal scaling (replicas)
- **gRPC and REST API**: Submit jobs, get status, list jobs, cancel, retry archived (DLQ); Admin: list queues, pause/unpause
- **PostgreSQL**: Optional job metadata and execution history
- **Kafka**: Optional job lifecycle events (topic `job.events`) and job request ingestion (topic `job.requests`)
- **Job types**: hello, email, image (resize), invoice (template), report (CSV)
- **Observability**: Structured logging (slog), Prometheus metrics (`/metrics`), health probes
- **Upstream demos**: user-service (register, password-reset), billing-service (invoice, report, email)

### Quick start (local)

1. **Start Minikube and Redis**

   ```bash
   minikube start --cpus=4 --memory=8192 --driver=docker
   minikube addons enable metrics-server
   kubectl apply -f deploy/minikube/redis.yaml
   kubectl port-forward svc/redis 6379:6379 &
   ```

2. **Run API and worker on host**

   ```bash
 export REDIS_ADDR=localhost:6379
   go run ./cmd/api &
   go run ./cmd/worker &  
   ```

3. **Enqueue a job**

   ```bash
   go run ./cmd/enqueue hello "world"
   ```

4. **Submit job via REST** (Job API must be running with REST on :8080)
   ```bash
   curl -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'
   ```

5. **Submit email job via gRPC** (requires [grpcurl](https://github.com/fullstorydev/grpcurl))
   ```bash
   grpcurl -plaintext -d '{"type":"email","payload":"{\"to\":\"dev@localhost\",\"subject\":\"Test\",\"body\":\"Hello\"}"}' localhost:50051 job.v1.JobService/SubmitJob
   ```

### Deploy to Minikube

1. Build images inside Minikube Docker:

   ```bash
   eval $(minikube docker-env)
   docker build -f Dockerfile.api -t job-api:latest .
   docker build -f Dockerfile.worker -t job-worker:latest .
   ```

2. Apply manifests:

   ```bash
   kubectl apply -f deploy/minikube/redis.yaml
   kubectl apply -f deploy/minikube/mailhog.yaml
   kubectl apply -f deploy/minikube/job-api.yaml
   kubectl apply -f deploy/minikube/job-worker.yaml
   ```

3. Optional: Prometheus for metrics
   ```bash
   kubectl apply -f deploy/minikube/prometheus.yaml
   kubectl port-forward svc/prometheus 9090:9090
   ```

### Demo (one-command run)

Run the full stack locally and see results in a browser. No Minikube required. Requires [Docker](https://docs.docker.com/get-docker/) and [grpcurl](https://github.com/fullstorydev/grpcurl).

1. **Start everything** (Redis, MailHog, API, worker, dashboard):

   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```

2. **Apply Postgres schema** (once, for job metadata):
   ```bash
   docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
   ```

3. **Run the demo script** (submits hello, email, and report jobs; prints job IDs and links):
   ```bash
   ./scripts/demo.sh
   ```
   On Windows (PowerShell) you can submit jobs manually via the dashboard (step 4) or use Git Bash for `demo.sh`.

4. **See results**
   - **Dashboard:** http://localhost:8080 — submit jobs and check status.
   - **REST API:** http://localhost:8083 — POST/GET /jobs, GET /jobs/:id, POST /jobs/:id/retry, POST /jobs/:id/cancel, GET /admin/queues, POST /admin/queues/:name/pause|unpause.
   - **User service:** http://localhost:8081 — POST /register, POST /password-reset (submit email jobs).
   - **Billing service:** http://localhost:8082 — POST /invoice, POST /report, POST /invoice-ready.
   - **Emails:** http://localhost:8025 (MailHog).
   - **Report:** `./out/demo-report.csv` (created after the report job runs).

5. **Stop the stack**

   ```bash
   docker compose -f deploy/demo/docker-compose.yml down
   ```

### Project layout

- `cmd/api` – gRPC + REST server (SubmitJob, GetJobStatus, ListJobs, CancelJob, ListArchivedJobs, RetryArchivedJob, ListQueues, PauseQueue, UnpauseQueue)
- `cmd/worker` – Asynq worker (processes jobs from Redis; optional Postgres store and Kafka events)
- `cmd/enqueue` – CLI to enqueue a single job (testing)
- `cmd/demo-server` – Demo dashboard (HTTP UI + proxy to gRPC API)
- `cmd/user-service` – Demo upstream: POST /register, /password-reset (submits email jobs)
- `cmd/billing-service` – Demo upstream: POST /invoice, /report, /invoice-ready
- `cmd/scheduler` – Stub (Asynq handles delayed tasks internally)
- `scripts/demo.sh` – Demo script: submits hello, email, report jobs
- `deploy/demo` – Docker Compose (Redis, Postgres, MailHog, API, worker, user-service, billing-service, dashboard)
- `deploy/postgres` – Postgres schema for job metadata
- `internal/queue` – Queue client and worker mux
- `internal/jobs` – Job handlers (hello, email, image, invoice, report)
- `internal/api` – gRPC and REST service implementation
- `internal/store` – Job metadata store (Postgres)
- `internal/events` – Kafka job lifecycle event producer
- `internal/ingest` – Kafka consumer for job requests (optional)
- `internal/metrics` – Prometheus counters/histograms
- `api/proto` – Protobuf and generated gRPC code
- `deploy/minikube` – Kubernetes manifests (Redis, MailHog, API, worker, Prometheus, MinIO)

### Environment variables

| Variable                 | Default           | Description                                              |
| ------------------------ | ----------------- | -------------------------------------------------------- |
| REDIS_ADDR               | localhost:6379    | Redis address                                            |
| REDIS_PASSWORD           | (empty)           | Redis password                                           |
| REDIS_DB                 | 0                 | Redis DB index                                           |
| GRPC_ADDR                | :50051            | gRPC listen address (API)                                |
| REST_ADDR                | :8080             | REST API listen address (API)                             |
| METRICS_ADDR             | :9090             | HTTP metrics listen address (API)                        |
| POSTGRES_DSN             | (empty)           | Postgres DSN for job metadata (optional)                 |
| KAFKA_BROKERS            | (empty)           | Kafka brokers for job.events producer (optional)          |
| KAFKA_TOPIC              | job.events        | Kafka topic for lifecycle events                         |
| KAFKA_CONSUMER_TOPIC     | (empty)           | If set, API consumes job requests from this topic        |
| KAFKA_CONSUMER_GROUP     | job-api-ingest    | Consumer group for job request ingestion                 |
| WORKER_CONCURRENCY       | 5                 | Number of concurrent jobs per worker                     |
| SMTP_ADDR                | (empty)           | SMTP server for email jobs (e.g. mailhog:1025)           |
| EMAIL_FROM               | noreply@localhost | From address for email jobs                              |
| JOB_API_URL              | (empty)           | Job API REST base URL (for user-service, billing-service)|

### cURL examples

See [docs/CURL_EXAMPLES.md](docs/CURL_EXAMPLES.md) for curl commands for all REST endpoints: submit job, get status, list jobs, retry, cancel, and admin (list queues, pause/unpause), plus user-service and billing-service.

### Proto regeneration

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/job_service.proto
```

Requires: `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` (install via `go install`).
