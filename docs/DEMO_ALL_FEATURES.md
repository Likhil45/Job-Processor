# How to Demo All Features

This guide walks through **every feature** in the project so you can demo the system end-to-end. Use the **Docker Compose** stack so all components (API, worker, dashboard, MailHog, user-service, billing-service, scheduler, Jaeger, Grafana, Loki) are available.

---

## Prerequisites

- **Docker** and Docker Compose
- **Repo root** as working directory
- **Git Bash** on Windows (for `demo.sh` and curl), or use the dashboard + PowerShell where noted

---

## 0. Start the stack (once)

```bash
docker compose -f deploy/demo/docker-compose.yml up -d
```

Apply Postgres schema (once per DB):

```bash
docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
```

Set the API base URL for curl (use **8083** for Docker):

```bash
export API="http://localhost:8083"
# Windows PowerShell: $env:API="http://localhost:8083"
```

---

## 1. Job types (submit and see result)

| Feature | How to demo | Where to see result |
|--------|--------------|---------------------|
| **hello** | **Dashboard:** http://localhost:8080 → “Submit Hello”. Or: `curl -s -X POST "$API/jobs" -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'` | Worker logs; status → completed |
| **email** | **Dashboard:** “Submit Email”. Or curl (see [CURL_EXAMPLES.md](CURL_EXAMPLES.md)). | **MailHog:** http://localhost:8025 — message appears after worker runs |
| **report** | **Dashboard:** “Submit Report”. Or curl with `type":"report"` and `out_path":"/out/demo-report.csv"`. | **File:** `./out/demo-report.csv` on host |
| **invoice** | `curl -s -X POST "$API/jobs" -H "Content-Type: application/json" -d '{"type":"invoice","payload":{"template":"Invoice #{{.ID}}","data":{"ID":"INV-1","Total":"99"},"out_path":"/out/inv.txt"}}'` | **File:** `./out/inv.txt` |
| **image** | Submit with valid payload (e.g. `source_url` or `source_path` + `width`/`height`). See [CURL_EXAMPLES.md](CURL_EXAMPLES.md). | **File:** path in `out_path` |

For each: copy the returned `job_id`, then **get status** (dashboard “Get status” or `curl -s "$API/jobs/<job_id>"`) to see pending → processing → completed.

---

## 2. Get status and list jobs

| Feature | How to demo |
|--------|--------------|
| **Get status** | **Dashboard:** Paste job ID → “Get status”. Or: `curl -s "$API/jobs/<job_id>"`. Shows `status` (pending, processing, completed, failed, archived), `attempt`, `last_error`. |
| **List jobs** | `curl -s "$API/jobs"` or `curl -s "$API/jobs?queue=default&status=pending&limit=20"`. Requires Postgres. |

---

## 3. Job options (queue, retries, delayed run)

| Feature | How to demo |
|--------|--------------|
| **Queue** | `curl -s -X POST "$API/jobs" -H "Content-Type: application/json" -d '{"type":"hello","payload":"x","options":{"queue":"high"}}'`. Then list queues (see § 7) to see counts per queue. |
| **Max retry** | Use **Scenario B** (dashboard) or submit an invoice job with `options":{"max_retry":2}`; trigger a failure (e.g. demo payload with `_demo_fail_until_attempt`) and watch status/attempt. |
| **Delayed run** | Use **Scenario C** (dashboard) or: `RUN_AT=$(($(date +%s) + 60)); curl -s -X POST "$API/jobs" -H "Content-Type: application/json" -d "{\"type\":\"hello\",\"payload\":\"later\",\"options\":{\"run_at_unix_sec\":$RUN_AT}}"`. Get status immediately → scheduled; after 60s → completed. |

---

## 4. Demo scenarios (dashboard) — retries, DLQ, crash recovery

Open **http://localhost:8080** and use the **“Demo scenarios”** section:

| Scenario | What it shows | What to do |
|----------|----------------|------------|
| **A: Normal success** | Email job runs once and completes. | Click “A: Normal success (email)”. Check status → completed; open MailHog to see the email. |
| **B: Retry & recovery** | Invoice job fails attempt 1, retries, succeeds on attempt 2. | Click “B: Retry & recovery (invoice)”. Poll status; you should see attempt increase then status completed. |
| **C: Delayed 60s** | Report job is scheduled 60 seconds in the future. | Click “C: Delayed 60s (report)”. Status stays scheduled; after ~60s worker runs it. Check `./out/delayed-report.csv`. |
| **D: Permanent failure → DLQ** | Image job with invalid payload fails all retries and is archived. | Click “D: Permanent failure → DLQ”. Wait for max retries; get status → archived. Then use “List archived” and “Retry” (see § 5). |
| **E: Worker crash recovery** | Long-running sleep job; you kill the worker; after restart, Kafka redelivers and job completes. | Click “E: Worker crash recovery”. While job is “processing”, stop the worker: `docker compose -f deploy/demo/docker-compose.yml stop job-worker`. Wait a few seconds, then start it again: `docker compose -f deploy/demo/docker-compose.yml start job-worker`. Job should complete after recovery. |

---

## 5. Cancel, list archived, retry (DLQ)

| Feature | How to demo |
|--------|--------------|
| **Cancel** | Submit a job (e.g. hello or a delayed one). Before the worker picks it up (or while scheduled): `curl -s -X POST "$API/jobs/<job_id>/cancel" -H "Content-Type: application/json" -d '{}'`. Then get status; job should be cancelled. |
| **List archived** | After running **Scenario D**, or any job that exhausted retries: `curl -s "$API/jobs/archived?limit=20"`. Shows job_id, type, queue, last_error, attempt. |
| **Retry archived** | Pick an archived job ID from the list above. `curl -s -X POST "$API/jobs/<job_id>/retry" -H "Content-Type: application/json" -d '{"queue":"default"}'`. Job is re-queued; worker will process it again (fix payload if it was invalid to avoid re-archiving). |

---

## 6. Upstream services (user-service, billing-service)

These services **call the Job API** for you; workers do the actual work.

| Service | Endpoint | Job created | How to demo |
|---------|----------|-------------|-------------|
| **User-service** (:8081) | POST /register | Email (welcome) | `curl -s -X POST http://localhost:8081/register -H "Content-Type: application/json" -d '{"email":"newuser@example.com"}'`. Check MailHog. |
| **User-service** | POST /password-reset | Email (password reset) | `curl -s -X POST http://localhost:8081/password-reset -H "Content-Type: application/json" -d '{"email":"user@example.com"}'`. Check MailHog. |
| **Billing-service** (:8082) | POST /invoice | Invoice | `curl -s -X POST http://localhost:8082/invoice -H "Content-Type: application/json" -d '{"id":"INV-002","total":"150.00"}'`. Check `./out/` for generated file. |
| **Billing-service** | POST /report | Report | `curl -s -X POST http://localhost:8082/report`. Check `./out/` for CSV. |
| **Billing-service** | POST /invoice-ready | Email | `curl -s -X POST http://localhost:8082/invoice-ready -H "Content-Type: application/json" -d '{"email":"billing@example.com"}'`. Check MailHog. |

Each response includes a `job_id`; use **Get status** to confirm completion.

---

## 7. Admin (list queues, pause / unpause)

| Feature | How to demo |
|--------|--------------|
| **List queues** | `curl -s "$API/admin/queues"`. Returns queues with counts (pending, active, scheduled, retry, archived) and `paused` flag. |
| **Pause queue** | `curl -s -X POST "$API/admin/queues/default/pause"`. New jobs in that queue are not processed until unpaused (worker checks queue state). |
| **Unpause queue** | `curl -s -X POST "$API/admin/queues/default/unpause"`. |

*Note: In Kafka-only mode, list queues may be empty or stubbed; pause/unpause may not be supported (see README).*

---

## 8. Scheduler (periodic fake jobs)

The **job-scheduler** runs inside the stack and enqueues fake jobs (email, image, invoice, report) every **2 minutes** (configurable via `SCHEDULER_INTERVAL`).

| How to demo |
|-------------|
| Leave the stack running. After 2 minutes, check **MailHog** (new emails), **List jobs** (new job IDs), or **Grafana** for scheduler metrics. Scheduler health: `curl -s http://localhost:9091/health`. Metrics: `curl -s http://localhost:9091/metrics` (e.g. `jobs_scheduled_total`). |

See [SCHEDULER.md](SCHEDULER.md) for details.

---

## 9. Observability (traces, logs, metrics)

| Feature | How to demo |
|--------|--------------|
| **Jaeger (traces)** | Open http://localhost:16686. Submit a job from the dashboard or curl. In Jaeger, select service `job-api` or `job-worker`, click “Find Traces”. You should see a trace from API submit through Kafka to worker. See [TRACING.md](TRACING.md). |
| **Grafana (logs)** | Open http://localhost:3000 (admin/admin). Go to **Explore** → choose **Loki**. Query: `{container=~"job-api|job-worker|dashboard"}` or `{stack="demo"}`, time range “Last 15 minutes”. See worker and API log lines. |
| **Prometheus metrics** | API: `curl -s http://localhost:9090/metrics` (or the mapped port). Worker: `curl -s http://localhost:9091/metrics` (in compose). Look for `jobs_enqueued_total`, `job_processing_duration_seconds`, etc. |

---

## 10. Demo script (batch submit)

Runs **hello**, **email**, and **report** in one go and prints job IDs and links:

```bash
export JOB_API_URL=http://localhost:8083
./scripts/demo.sh
```

Then open **MailHog** (email), **Dashboard** (paste job ID → Get status), and **./out/demo-report.csv** (report).

---

## Feature checklist (quick reference)

| # | Feature | Where |
|---|--------|--------|
| 1 | Job types: hello, email, report, invoice, image | Dashboard or curl → § 1 |
| 2 | Get status, list jobs | Dashboard or curl → § 2 |
| 3 | Options: queue, max_retry, run_at_unix_sec | curl or Scenarios B/C → § 3 |
| 4 | Demo scenarios A–E (success, retry, delayed, DLQ, crash) | Dashboard → § 4 |
| 5 | Cancel, list archived, retry DLQ | curl → § 5 |
| 6 | User-service (register, password-reset) | curl :8081 → § 6 |
| 7 | Billing-service (invoice, report, invoice-ready) | curl :8082 → § 6 |
| 8 | Admin: list queues, pause/unpause | curl → § 7 |
| 9 | Scheduler (periodic fake jobs) | Wait 2m or check :9091 → § 8 |
| 10 | Jaeger, Grafana/Loki, metrics | Browser :16686, :3000, curl :9090/:9091 → § 9 |
| 11 | Demo script (batch) | ./scripts/demo.sh → § 10 |

---

## Suggested demo order (for a live walkthrough)

1. **Start stack** + apply schema (§ 0).
2. **Dashboard** — Submit Hello → Get status → show completed (§ 1, 2).
3. **Email** — Submit Email → open MailHog and show the message (§ 1).
4. **Report** — Submit Report → show `./out/demo-report.csv` (§ 1).
5. **Scenario A** — Normal success (quick repeat) (§ 4).
6. **Scenario B** — Retry & recovery; show status/attempt changing (§ 4).
7. **Scenario C** — Delayed 60s; show status “scheduled” then completed after wait (§ 4).
8. **Scenario D** — DLQ; show archived, then List archived + Retry (§ 4, 5).
9. **User-service** — POST /register → show MailHog (§ 6).
10. **Billing-service** — POST /invoice or /report → show output file (§ 6).
11. **Jaeger** — Submit a job, then open Jaeger and show trace (§ 9).
12. **Scheduler** — Mention it runs every 2m; show metrics or MailHog after a couple of minutes (§ 8).

To stop: `docker compose -f deploy/demo/docker-compose.yml down`.
