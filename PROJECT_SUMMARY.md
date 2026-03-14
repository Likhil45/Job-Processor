# Distributed Job Processing System — Project Summary

## 1. Project Overview

This is a **distributed job processing system** built in Go for local development (Minikube) and demo (Docker Compose). It uses **Redis** as the queue backend and **Asynq** for reliable, retriable job processing with FIFO and priority queues.

**Purpose:** Accept job submissions via a gRPC API, enqueue them in Redis, and process them asynchronously with configurable workers. Supports multiple job types (hello, email, image resize, invoice, report), retries with exponential backoff, delayed/scheduled jobs, and observability (Prometheus metrics, structured logging, health probes).

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CLIENTS / USERS                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│  grpcurl / enqueue CLI          Demo Dashboard (HTTP)         External Services  │
│  (SubmitJob, GetStatus)         (Submit + Status UI)          (gRPC clients)     │
└────────────────────────────┬───────────────────────┬────────────────────────────┘
                              │                       │
                              ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           JOB API (cmd/api)                                       │
│  • gRPC server (:50051) — SubmitJob, GetJobStatus, ListArchivedJobs, RetryArchived│
│  • HTTP metrics (:9090) — /metrics (Prometheus)                                   │
│  • Uses queue.Client to enqueue; Asynq Inspector for status/archived/retry       │
└────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              │  Enqueue tasks (type + payload + options)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              REDIS                                                │
│  • Asynq queues: high (weight 3), default (2), low (1)                             │
│  • Stores: pending, scheduled, active, completed, archived (DLQ)                  │
└────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              │  Poll & claim tasks
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        JOB WORKER(S) (cmd/worker)                                 │
│  • Asynq server: N concurrent tasks per process                                   │
│  • ServeMux routes by task type → Registry → Handler (hello, email, image, etc.)   │
│  • HTTP :9090 — /health, /ready, /metrics                                         │
└────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│ Hello        │    │ Email        │    │ Report / Invoice  │
│ (log only)   │    │ (SMTP e.g.   │    │ (CSV / template   │
│              │    │  MailHog)    │    │  → file)          │
└──────────────┘    └──────────────┘    └──────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│ Image        │    │ (optional)   │    │ Observability     │
│ (resize,     │    │ MailHog UI   │    │ Prometheus, slog  │
│  thumbnail)  │    │ :8025        │    │ health probes     │
└──────────────┘    └──────────────┘    └──────────────────┘
```

### Data flow (high level)

1. **Submit:** Client → gRPC API → `queue.Client.Enqueue()` → Redis (Asynq list).
2. **Process:** Worker polls Redis → gets task → Registry lookup by type → Handler runs → success/failure (retry or archive).
3. **Status:** Client → gRPC GetJobStatus → API uses Asynq Inspector → Redis → returns state (pending/scheduled/processing/completed/failed/archived).
4. **DLQ:** Failed jobs (after max retries) go to archive; ListArchivedJobs / RetryArchivedJob operate on them.

---

## 3. Component Reference

| Component | Location | Role |
|-----------|----------|------|
| **Job API** | `cmd/api` | gRPC server exposing SubmitJob, GetJobStatus, ListArchivedJobs, RetryArchivedJob; HTTP /metrics; talks to Redis via queue client + Asynq Inspector. |
| **Worker** | `cmd/worker` | Asynq server; registers all job handlers, runs ServeMux; exposes /health, /ready, /metrics. |
| **Queue client** | `internal/queue/client.go` | Wraps Asynq client; Enqueue(ctx, type, payload, opts). |
| **Queue worker mux** | `internal/queue/worker.go` | Implements Asynq Handler; dispatches by task type to `jobs.Registry`. |
| **Job registry** | `internal/jobs/registry.go` | Map of job type string → Handler; Register/Get/Types. |
| **Job handlers** | `internal/jobs/*.go` | hello, email, image, invoice, report; each implements Handler (Type, Handle). |
| **gRPC service impl** | `internal/api/server.go` | JobServer: enqueue with options (queue, max_retry, run_at), status via Inspector, list/retry archived. |
| **Metrics** | `internal/metrics/metrics.go` | Prometheus: jobs_enqueued_total, jobs_processed_total, job_processing_duration_seconds. |
| **Proto / gRPC** | `api/proto/` | job_service.proto and generated Go; defines API contract. |
| **Enqueue CLI** | `cmd/enqueue` | Small CLI to enqueue one job (e.g. `go run ./cmd/enqueue hello "world"`). |
| **Demo server** | `cmd/demo-server` | HTTP dashboard (submit job, get status); proxies to gRPC Job API. |
| **Scheduler** | `cmd/scheduler` | Stub; Asynq handles delayed/scheduled tasks via options. |
| **Deploy (demo)** | `deploy/demo/` | Docker Compose: Redis, MailHog, job-api, job-worker, dashboard. |
| **Deploy (Minikube)** | `deploy/minikube/` | K8s manifests: Redis, MailHog, job-api, job-worker, Prometheus, MinIO. |
| **Demo script** | `scripts/demo.sh` | Submits hello, email, report jobs via grpcurl; prints job IDs and links. |

---

## 4. Job Types and Handlers

| Type | Handler | Payload (JSON) | Behavior |
|------|---------|----------------|----------|
| **hello** | `jobs.Hello` | Any (e.g. `"world"`) | Logs payload and succeeds. |
| **email** | `jobs.Email` | `to`, `subject`, `body` | Sends via SMTP (e.g. MailHog in dev). |
| **image** | `jobs.Image` | `source_url` or `source_path`, `width`, `height`, optional `out_path` | Fetches/loads image, resizes (thumbnail), writes PNG to path. |
| **invoice** | `jobs.Invoice` | `template` (Go template), `data`, optional `out_path` | Renders template with data, writes to file. |
| **report** | `jobs.Report` | `headers`, `rows`, optional `out_path` | Writes CSV to file. |

Handlers use optional `*OutputDir` (e.g. `REPORT_OUTPUT_DIR`, `INVOICE_OUTPUT_DIR`, `IMAGE_OUTPUT_DIR`) so relative `out_path` is under that directory.

---

## 5. Real-Time Scenario Usage

### Scenario A: Quick local run (no Docker)

1. Start Redis (e.g. `docker run -d -p 6379:6379 redis:7-alpine` or Minikube + port-forward).
2. Run API and worker on host:
   ```bash
   export REDIS_ADDR=localhost:6379
   go run ./cmd/api &
   go run ./cmd/worker &
   ```
3. Enqueue a hello job:
   ```bash
   go run ./cmd/enqueue hello "world"
   ```
4. Submit via gRPC and check status (grpcurl):
   ```bash
   grpcurl -plaintext -d '{"type":"hello","payload":"<base64>"}' localhost:50051 job.v1.JobService/SubmitJob
   grpcurl -plaintext -d '{"job_id":"<id>"}' localhost:50051 job.v1.JobService/GetJobStatus
   ```

**Real-time use:** Validation that jobs flow API → Redis → worker and status is visible.

---

### Scenario B: Full demo stack (Docker Compose)

1. Start stack:
   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```
2. Run demo script (submits hello, email, report):
   ```bash
   ./scripts/demo.sh
   ```
3. **Real-time checks:**
   - **Dashboard:** http://localhost:8080 — submit jobs (hello, email, report), paste job ID, get status.
   - **MailHog:** http://localhost:8025 — see email sent by the email job.
   - **Report:** `./out/demo-report.csv` — created after report job runs (worker writes to `/out` in container, bind-mounted to host).

**Real-time use:** End-to-end: submit from UI or script → worker processes → see email in MailHog and CSV on disk; status via dashboard or gRPC.

---

### Scenario C: Priority and delayed jobs

1. **High-priority email** (queue `high`):
   ```bash
   grpcurl -plaintext -d '{"type":"email","payload":"<base64>","options":{"queue":"high"}}' localhost:50051 job.v1.JobService/SubmitJob
   ```
2. **Delayed job** (run at Unix time):
   ```bash
   # run_at_unix_sec = now + 60
   grpcurl -plaintext -d '{"type":"hello","payload":"<base64>","options":{"run_at_unix_sec":'$(($(date +%s)+60))'}}' localhost:50051 job.v1.JobService/SubmitJob
   ```

**Real-time use:** High-priority tasks get more worker attention (queue weight 3); delayed job appears as “scheduled” until run_at.

---

### Scenario D: Failures and dead-letter queue (DLQ)

1. Submit a job that will fail (e.g. invalid email payload or unknown type).
2. After max retries, task moves to **archived** (DLQ).
3. **List archived:**
   ```bash
   grpcurl -plaintext -d '{"limit":50}' localhost:50051 job.v1.JobService/ListArchivedJobs
   ```
4. **Retry one:**
   ```bash
   grpcurl -plaintext -d '{"job_id":"<id>","queue":"default"}' localhost:50051 job.v1.JobService/RetryArchivedJob
   ```

**Real-time use:** Inspect failed jobs and re-queue them after fixing payload or system.

---

### Scenario E: Minikube (production-like)

1. Start Minikube, enable metrics-server, apply Redis (and optionally MailHog, Prometheus).
2. Build images in Minikube Docker, apply `job-api` and `job-worker` manifests.
3. Workers run as Deployment (e.g. 2 replicas); API as Deployment; both use health/readiness probes.
4. **Real-time use:** Scale workers (`kubectl scale deployment job-worker --replicas=4`), hit API via port-forward or Ingress, scrape /metrics with Prometheus.

---

### Scenario F: Observability

- **Metrics (API):** `GET http://localhost:9090/metrics` — `jobs_enqueued_total`, `jobs_processed_total`, `job_processing_duration_seconds`.
- **Metrics (worker):** Worker exposes its own /metrics on :9090 (or :9091 in demo compose).
- **Logs:** Structured slog in API and worker (job_id, type, duration, errors).
- **Health:** API uses gRPC health; worker uses HTTP /health and /ready (Redis ping).

**Real-time use:** Monitor queue growth, processing rate, latency, and failures; alert on readiness/health.

---

## 6. Environment Variables (reference)

| Variable | Default | Description |
|----------|---------|-------------|
| REDIS_ADDR | localhost:6379 | Redis address for queue and inspector. |
| REDIS_PASSWORD | (empty) | Redis password. |
| REDIS_DB | 0 | Redis DB index. |
| GRPC_ADDR | :50051 | gRPC listen address (API). |
| METRICS_ADDR | :9090 | HTTP metrics listen (API). |
| WORKER_CONCURRENCY | 5 | Concurrent jobs per worker process. |
| SMTP_ADDR | (empty) | SMTP server (e.g. mailhog:1025). |
| EMAIL_FROM | noreply@localhost | From address for email jobs. |
| REPORT_OUTPUT_DIR | (empty) | Base directory for report output paths. |
| INVOICE_OUTPUT_DIR | (empty) | Base directory for invoice output. |
| IMAGE_OUTPUT_DIR | (empty) | Base directory for image output. |
| JOB_API_ADDR | localhost:50051 | Used by demo-server to call gRPC API. |

---

## 7. Summary

- **What it is:** A distributed job queue with gRPC API, Redis/Asynq backend, and pluggable job handlers (hello, email, image, invoice, report).
- **Architecture:** Clients → Job API (gRPC + metrics) → Redis (queues) → Workers (ServeMux → Registry → Handlers); optional demo dashboard and MailHog for emails.
- **Real-time scenarios:** Local run, full demo (Docker), priority/delayed jobs, DLQ list/retry, Minikube deployment, and observability via metrics and health checks. Each component’s role is documented in the component table and job-type table above.
