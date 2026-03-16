# Working Flow and How to Demo

This doc explains **how jobs flow** through the system and **how to run a demo** end-to-end.

---

## 1. Working flow (end-to-end)

### Overview

1. A **client** submits a job to the **REST API**.
2. The **API** writes the job to **PostgreSQL** and publishes a message to **Kafka** (`job.requests`).
3. A **worker** consumes the message from Kafka, runs the right **handler** (hello, email, report, etc.), then updates **PostgreSQL** and optionally emits to **Kafka** (`job.events`).
4. The client can **get status** or **list jobs** from the API, which reads from PostgreSQL.

```
┌──────────┐     POST /jobs      ┌─────────────┐     write      ┌───────────┐
│  Client  │ ─────────────────► │   Job API    │ ─────────────► │ Postgres  │
│(Dashboard│                    │  (REST :8080)│                │  (jobs)   │
│ curl CLI)│                    └──────┬──────┘                └───────────┘
└──────────┘                           │
                                       │ produce
                                       ▼
                                ┌─────────────┐
                                │   Kafka     │
                                │job.requests │
                                └──────┬──────┘
                                       │ consume
                                       ▼
┌──────────┐     GET /jobs/:id   ┌─────────────┐     read       ┌───────────┐
│  Client  │ ◄───────────────── │   Job API   │ ◄───────────── │ Postgres  │
└──────────┘                    └─────────────┘                └───────────┘
                                       ▲
                                ┌──────┴──────┐
                                │   Worker    │  update status
                                │ (handlers)  │  + job.events
                                └──────┬──────┘
                                       │
         ┌─────────────────────────────┼─────────────────────────────┐
         ▼                             ▼                             ▼
   ┌──────────┐                 ┌──────────┐                 ┌──────────┐
   │  hello   │                 │  email   │                 │  report   │
   │  (log)   │                 │ (SMTP)   │                 │ (CSV)    │
   └──────────┘                 └──────────┘                 └──────────┘
```

### Step-by-step flow

| Step | What happens |
|------|----------------|
| 1 | **Submit** — Client sends `POST /jobs` with `type` and `payload`. |
| 2 | **API** — Validates request, inserts a row in Postgres (`jobs` table), produces a message to Kafka topic `job.requests` with job id, type, payload, queue, etc. Returns `201` with `job_id`. |
| 3 | **Worker** — Consumer loop reads from `job.requests`; for each message, loads job metadata from Postgres (checks cancelled, queue paused, etc.), acquires concurrency semaphore if configured. |
| 4 | **Handler** — Worker looks up handler by `type` (hello, email, report, invoice, image), runs `Handle(ctx, payload)`. Handler may call SMTP (email), write files (report, invoice), resize image, etc. |
| 5 | **Completion** — On success: worker updates job status in Postgres (e.g. completed), may produce to `job.events`; releases semaphore. On failure: worker retries (re-enqueue to Kafka) up to `max_retry`; after exhaustion, job is archived (DLQ). |
| 6 | **Status** — Client calls `GET /jobs/<job_id>`. API reads from Postgres and returns status (pending, processing, completed, failed, archived). |

### Optional pieces

- **Scheduler** — Periodically creates jobs (e.g. fake email/report) and writes to Postgres + Kafka (same as API).
- **User-service / Billing-service** — Demo upstream services that call the Job API (e.g. register → send email job; invoice → submit invoice job).
- **DLQ** — Archived jobs can be listed with `GET /jobs/archived` and retried with `POST /jobs/<id>/retry`.

---

## 2. How to demo this repo

### Prerequisites

- **Docker** (and Docker Compose)
- **Git Bash** on Windows (for `demo.sh`), or use the dashboard only

### Step 1: Start the stack

From the **repo root**:

```bash
docker compose -f deploy/demo/docker-compose.yml up -d
```

This starts: Kafka, Postgres, MailHog, Job API, Worker, Dashboard, Scheduler, and optionally user-service, billing-service, Jaeger, Loki, Grafana, Promtail (see `deploy/demo/docker-compose.yml`).

Wait until services are healthy (e.g. 30–60 seconds). Check:

```bash
docker compose -f deploy/demo/docker-compose.yml ps
```

All listed services should be “Up”.

### Step 2: Apply Postgres schema (once per DB)

```bash
docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
```

Required so the API and worker can read/write job metadata.

### Step 3: Run the demo script (optional)

Submits a **hello**, **email**, and **report** job and prints their job IDs:

```bash
export JOB_API_URL=http://localhost:8083
./scripts/demo.sh
```

On Windows (PowerShell) you can set `$env:JOB_API_URL="http://localhost:8083"` then run the script in **Git Bash**. If the script fails (e.g. API not reachable), use the dashboard in Step 4 instead.

### Step 4: Use the dashboard and see results

| What | URL / Where |
|------|-------------|
| **Dashboard** | http://localhost:8080 — Submit jobs (Submit Hello / Email / Report), paste job ID, click “Get status”. Use **demo scenarios (A–E)** to see success, retry, delayed, DLQ, crash recovery. |
| **REST API** | http://localhost:8083 — e.g. `GET http://localhost:8083/jobs`, `POST http://localhost:8083/jobs` (see [CURL_EXAMPLES.md](CURL_EXAMPLES.md)). |
| **Emails** | http://localhost:8025 (MailHog) — All email jobs are captured here; no real SMTP. |
| **Report output** | `./out/demo-report.csv` — Created after a report job runs (worker writes to `/out` in container; Compose mounts `./out`). |
| **Traces** | http://localhost:16686 (Jaeger) — Search by service `job-api` or `job-worker` after submitting a job. |
| **Logs** | Grafana http://localhost:3000 (admin/admin) — Explore → Loki datasource; query e.g. `{container=~"job-api|job-worker"}`. |
| **User service** | http://localhost:8081 — POST /register, /password-reset (submits jobs to Job API). |
| **Billing service** | http://localhost:8082 — POST /invoice, /report, /invoice-ready (submits jobs). |

### Step 5: Stop the stack

```bash
docker compose -f deploy/demo/docker-compose.yml down
```

---

## 3. Minimal demo (no script)

If you only want to see the flow without the demo script:

1. **Start stack** — `docker compose -f deploy/demo/docker-compose.yml up -d`
2. **Apply schema** — `docker exec -i ... psql ... < deploy/postgres/schema.sql` (see Step 2 above)
3. **Open dashboard** — http://localhost:8080
4. **Submit** — Click “Submit Hello” (or Email / Report). Copy the job ID shown.
5. **Status** — Paste the job ID and click “Get status”. You should see `pending` then (after the worker runs it) `completed` or `processing` if you poll quickly.
6. **Email** — After “Submit Email”, open http://localhost:8025 to see the email in MailHog.
7. **Report** — After “Submit Report”, check `./out/demo-report.csv` on your machine.

---

## 4. Quick reference

| Action | How |
|--------|-----|
| **Submit job** | Dashboard → Submit Hello/Email/Report, or `curl -X POST http://localhost:8083/jobs -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'` |
| **Get status** | Dashboard → paste job ID → Get status, or `curl http://localhost:8083/jobs/<job_id>` |
| **List jobs** | `curl http://localhost:8083/jobs?queue=default&limit=20` |
| **List archived (DLQ)** | `curl http://localhost:8083/jobs/archived?limit=20` |
| **Retry archived** | `curl -X POST http://localhost:8083/jobs/<job_id>/retry -H "Content-Type: application/json" -d '{"queue":"default"}'` |
| **Cancel job** | `curl -X POST http://localhost:8083/jobs/<job_id>/cancel` |

More examples: [CURL_EXAMPLES.md](CURL_EXAMPLES.md). Full workflow options (local run, K8s): [WORKFLOW.md](WORKFLOW.md).
