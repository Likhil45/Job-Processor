# How to Use This Repo

This repo is a **distributed job processing system**: you submit jobs (email, report, invoice, etc.) via an API, and worker processes run them in the background using Redis as the queue.

---

## Prerequisites

- **Go 1.24+** (for local run)
- **Docker** (for full demo with Postgres, MailHog, dashboard)
- **Redis** (required — run via Docker or Minikube)

---

## Option A: Fastest local run (no Docker)

Good for trying the API and worker with minimal setup.

1. **Start Redis** (pick one):

   ```bash
   # Using Docker
   docker run -d -p 6379:6379 --name redis redis:7-alpine
   ```

   Or use Minikube and port-forward (see README).

2. **Start the API and worker** (two terminals or background):

   ```bash
   export REDIS_ADDR=localhost:6379
   go run ./cmd/api &
   go run ./cmd/worker &
   ```

3. **Submit a job**:

   ```bash
   # REST (returns job_id)
   curl -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'

   # Or CLI
   go run ./cmd/enqueue hello "world"
   ```

4. **Check status** (use the `job_id` from step 3):

   ```bash
   curl http://localhost:8080/jobs/<job_id>
   ```

**Ports:** API gRPC `:50051`, REST `:8080`, metrics `:9090`. Worker health `:9090`.

---

## Option B: Full stack with Docker (recommended for demo)

Runs Redis, Postgres, MailHog, Job API, worker, dashboard, user-service, and billing-service.

1. **Start all services**:

   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```

2. **Apply Postgres schema once** (for job list and metadata):

   ```bash
   docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
   ```

3. **Run the demo script** (submits hello, email, report jobs):

   ```bash
   ./scripts/demo.sh
   ```

   On Windows use Git Bash for the script, or submit jobs via the dashboard or curl (see below).

4. **Use the system**:

   | What              | URL / Action |
   |-------------------|--------------|
   | Web dashboard     | http://localhost:8080 |
   | REST API          | http://localhost:8083 |
   | View emails       | http://localhost:8025 (MailHog) |
   | User service      | http://localhost:8081 |
   | Billing service   | http://localhost:8082 |
   | Report output     | `./out/demo-report.csv` (after report job runs) |

5. **Stop**:

   ```bash
   docker compose -f deploy/demo/docker-compose.yml down
   ```

---

## Day-to-day usage

### Submit jobs

- **REST** (recommended): `POST http://localhost:8080/jobs` (or `:8083` in Docker) with JSON body:
  - `type`: `hello` | `email` | `report` | `invoice` | `image`
  - `payload`: type-specific JSON (see [docs/CURL_EXAMPLES.md](CURL_EXAMPLES.md))
  - Optional `options`: `queue`, `max_retry`, `run_at_unix_sec`

- **gRPC**: use `grpcurl` to `job.v1.JobService/SubmitJob` on port `50051`.

- **Upstream services**: call user-service (register, password-reset) or billing-service (invoice, report, invoice-ready); they submit jobs to the Job API for you.

### Check job status

- **REST**: `GET http://localhost:8080/jobs/<job_id>` (or `:8083` in Docker).
- **Dashboard**: open http://localhost:8080, paste job ID, click “Get status”.

### List jobs

- **REST**: `GET http://localhost:8080/jobs?queue=default&limit=20`  
  Requires Postgres; without it the list is empty.

### Cancel or retry

- **Cancel** (pending/scheduled): `POST http://localhost:8080/jobs/<job_id>/cancel`
- **Retry** (archived/DLQ): `POST http://localhost:8080/jobs/<job_id>/retry` with body `{"queue":"default"}`

### Admin (queues)

- **List queues**: `GET http://localhost:8080/admin/queues`
- **Pause**: `POST http://localhost:8080/admin/queues/default/pause`
- **Unpause**: `POST http://localhost:8080/admin/queues/default/unpause`

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

## Optional: Postgres and Kafka

- **Postgres**: Set `POSTGRES_DSN` for the API and worker to persist job metadata and enable `GET /jobs` list. Schema: `deploy/postgres/schema.sql`.
- **Kafka**: Set `KAFKA_BROKERS` to publish job lifecycle events to topic `job.events`. Set `KAFKA_CONSUMER_TOPIC` on the API to consume job requests from Kafka and enqueue them to Redis.

---

## Troubleshooting

- **“Connection refused” to API**  
  Ensure the API is running and you’re using the right port (8080 local, 8083 in Docker for REST).

- **Jobs stay “pending”**  
  Worker must be running and connected to the same Redis as the API. Check worker logs.

- **List jobs empty**  
  List uses Postgres. Set `POSTGRES_DSN` and apply the schema.

- **Email not visible**  
  With Docker, use MailHog at http://localhost:8025. Set `SMTP_ADDR` (e.g. `mailhog:1025`) for the worker.

More detail: [Readme.md](../Readme.md) and [docs/CURL_EXAMPLES.md](CURL_EXAMPLES.md).
