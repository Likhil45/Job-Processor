# How to Use This Repo

This repo is a **distributed job processing system**: you submit jobs (email, report, invoice, etc.) via a **REST API**. Workers consume from **Kafka** (`job.requests`), run the job handlers, and update **PostgreSQL** for status. No Redis or gRPC.

---

## Prerequisites

- **Go 1.24+** (for local run)
- **Docker** (for full demo)
- **PostgreSQL** and **Kafka** (required for API and worker — use Docker Compose for the demo)

---

## Option A: Fastest local run (with Docker for Postgres + Kafka)

1. **Start Postgres and Kafka** (Kafka runs in **KRaft mode** — no Zookeeper; Zookeeper is deprecated):

   ```bash
   docker run -d --name postgres -e POSTGRES_USER=jobs -e POSTGRES_PASSWORD=jobs -e POSTGRES_DB=jobs -p 5432:5432 postgres:15-alpine
   # KRaft: controller+broker in one (no Zookeeper)
   docker run -d --name kafka -p 9092:9092 -e KAFKA_CFG_NODE_ID=0 -e KAFKA_CFG_PROCESS_ROLES=controller,broker -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 bitnami/kafka:latest
   ```

   Apply schema once: `psql -U jobs -h localhost jobs -f deploy/postgres/schema.sql`

2. **Start the API and worker**:

   ```bash
   set POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
   set KAFKA_BROKERS=localhost:9092
   go run ./cmd/api &
   go run ./cmd/worker &
   ```

3. **Submit a job**:

   ```bash
   curl -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d "{\"type\":\"hello\",\"payload\":\"world\"}"

   # Or CLI (set JOB_API_URL=http://localhost:8080)
   go run ./cmd/enqueue hello "world"
   ```

4. **Check status** (use the `job_id` from step 3):

   ```bash
   curl http://localhost:8080/jobs/<job_id>
   ```

**Ports:** REST `:8080`, metrics `:9090`. Worker health `:9090`.

---

## Option B: Full stack with Docker (recommended for demo)

Runs Kafka, Postgres, MailHog, Job API, worker, scheduler, dashboard, user-service, billing-service, Loki, and Promtail.

1. **Start all services**:

   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```

2. **Apply Postgres schema once** (for job list and metadata):

   ```bash
   docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
   ```

3. **Run the demo script** (submits hello, email, report jobs). Run from the **repo root**; the script waits up to ~60s for the API:

   ```bash
   export JOB_API_URL=http://localhost:8083
   ./scripts/demo.sh
   ```

   On Windows use Git Bash. If the API is not reachable, check `docker compose -f deploy/demo/docker-compose.yml logs job-api`. Or submit jobs via the dashboard (http://localhost:8080) or curl.

4. **Use the system**:

   | What              | URL / Action |
   |-------------------|--------------|
   | Web dashboard     | http://localhost:8080 |
   | REST API          | http://localhost:8083 |
   | View emails       | http://localhost:8025 (MailHog) |
   | User service      | http://localhost:8081 |
   | Billing service   | http://localhost:8082 |
   | Logs (Loki)       | http://localhost:3100 — Promtail ships container logs to Loki; use Grafana with datasource `http://loki:3100` to query (Explore) |
   | Report output     | `./out/demo-report.csv` (after report job runs) |

5. **Stop**:

   ```bash
   docker compose -f deploy/demo/docker-compose.yml down
   ```

---

## Day-to-day usage

### Submit jobs

- **REST**: `POST http://localhost:8080/jobs` (or `:8083` in Docker) with JSON body:
  - `type`: `hello` | `email` | `report` | `invoice` | `image`
  - `payload`: type-specific JSON (see [docs/CURL_EXAMPLES.md](CURL_EXAMPLES.md))
  - Optional `options`: `queue`, `max_retry`, `run_at_unix_sec`

- **Enqueue CLI**: `JOB_API_URL=http://localhost:8080 go run ./cmd/enqueue <type> "<payload>"`

- **Upstream services**: user-service (register, password-reset) or billing-service (invoice, report, invoice-ready) submit jobs to the Job API via REST.

### Check job status

- **REST**: `GET http://localhost:8080/jobs/<job_id>` (or `:8083` in Docker).
- **Dashboard**: open http://localhost:8080, paste job ID, click “Get status”.

### List jobs

- **REST**: `GET http://localhost:8080/jobs?queue=default&status=&limit=20`  
  Uses Postgres; returns jobs for the given queue/status.

### Cancel or retry

- **Cancel** (pending/scheduled): `POST http://localhost:8080/jobs/<job_id>/cancel`
- **Retry** (archived): `POST http://localhost:8080/jobs/<job_id>/retry` with body `{"queue":"default"}`

### Admin (queues)

- **List queues**: `GET http://localhost:8080/admin/queues` (Kafka-only mode returns empty list).
- **Pause / Unpause**: Not supported in Kafka-only mode (returns error).

Full curl examples: [docs/CURL_EXAMPLES.md](CURL_EXAMPLES.md).

---

## Job types and payloads

| Type    | Purpose           | Payload example |
|---------|-------------------|------------------|
| `hello` | Test / no-op      | `"world"` or any string |
| `email` | Send email        | `{"to":"...","subject":"...","body":"..."}` |
| `report`| Write CSV         | `{"headers":["A","B"],"rows":[...],"out_path":"/out/x.csv"}` |
| `invoice` | Render template | `{"template":"...", "data":{...}, "out_path":"/out/x.txt"}` |
| `image` | Resize image      | `{"source_url":"...", "width":200, "height":200, "out_path":"..."}` |

For email in Docker demo, MailHog catches all mail at http://localhost:8025.

---

## Architecture (Kafka + Postgres)

- **API**: REST only. Writes job metadata to Postgres and produces to Kafka topic `job.requests`. Status and list come from Postgres.
- **Worker**: Consumes from `job.requests`, checks Postgres for cancelled, runs handlers, updates Postgres and produces to `job.events`.
- **Postgres**: Required. Schema in `deploy/postgres/schema.sql`.
- **Kafka**: Required. Runs in **KRaft mode** (no Zookeeper; Zookeeper is deprecated). Topics: `job.requests` (queue), `job.events` (lifecycle events). Topics are auto-created if the broker allows it.

---

## Troubleshooting

- **“POSTGRES_DSN is required” / “KAFKA_BROKERS is required”**  
  API and worker need both. Set env vars (see README).

- **“Connection refused” to API**  
  Ensure the API is running and you’re using the right port (8080 local, 8083 in Docker for REST).

- **“API not reachable after 60s” (demo script)**  
  1) Run Docker Compose from the **repo root**: `docker compose -f deploy/demo/docker-compose.yml up -d`.  
  2) Check that job-api is running: `docker compose -f deploy/demo/docker-compose.yml ps` (job-api should be “Up”).  
  3) Check job-api logs: `docker compose -f deploy/demo/docker-compose.yml logs job-api`. You should see **“REST server addr=:8080”**. If you see **“gRPC server listening”** instead, the container is running an old image — **rebuild**: `docker compose -f deploy/demo/docker-compose.yml up -d --build job-api`, then run the demo script again.  
  4) From the host, test: `curl -s http://127.0.0.1:8083/jobs` (use `127.0.0.1` instead of `localhost` if on Docker Desktop for Windows).  
  5) If the API is reachable via curl but the script still fails, set `JOB_API_URL=http://127.0.0.1:8083` and run the script again.

- **Jobs stay “pending”**  
  Worker must be running and consuming from the same Kafka (`job.requests`). Check worker logs and Kafka.

- **List jobs empty**  
  Ensure Postgres schema is applied and jobs were submitted (API writes to Postgres on submit).

- **Email not visible**  
  With Docker, use MailHog at http://localhost:8025. Set `SMTP_ADDR` (e.g. `mailhog:1025`) for the worker.

More detail: [Readme.md](../Readme.md) and [docs/CURL_EXAMPLES.md](CURL_EXAMPLES.md).
