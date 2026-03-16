# How to Use This Repo — Workflow

One-page workflow: choose how you run the system, then follow the steps.

---

## 1. Choose how you run

| Goal | Path | Section |
|------|------|--------|
| Try it fast (API + worker on host, Postgres/Kafka in Docker) | **Local** | [§ 2A](#2a-local-run-api--worker-on-host) |
| Full demo (dashboard, MailHog, scheduler, Loki, Grafana) | **Docker Compose** | [§ 2B](#2b-docker-compose-full-demo) |
| Deploy to a cluster (Kafka/Postgres in-cluster or external) | **Kubernetes** | [§ 2C](#2c-kubernetes) |

---

## 2A. Local run (API + worker on host)

**You need:** Go, Docker (for Postgres + Kafka only).

1. **Start Postgres and Kafka**
   ```bash
   docker run -d --name postgres -e POSTGRES_USER=jobs -e POSTGRES_PASSWORD=jobs -e POSTGRES_DB=jobs -p 5432:5432 postgres:15-alpine
   docker run -d --name kafka -p 9092:9092 -e KAFKA_CFG_NODE_ID=0 -e KAFKA_CFG_PROCESS_ROLES=controller,broker -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 bitnami/kafka:latest
   ```

2. **Apply schema (once)**
   ```bash
   psql -U jobs -h localhost jobs -f deploy/postgres/schema.sql
   ```

3. **Set env and start API + worker**
   ```bash
   # Windows (PowerShell)
   $env:POSTGRES_DSN="postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable"
   $env:KAFKA_BROKERS="localhost:9092"
   go run ./cmd/api
   # In another terminal:
   go run ./cmd/worker
   ```
   ```bash
   # Linux/macOS
   export POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
   export KAFKA_BROKERS=localhost:9092
   go run ./cmd/api &
   go run ./cmd/worker &
   ```

4. **Submit a job**
   ```bash
   curl -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d "{\"type\":\"hello\",\"payload\":\"world\"}"
   ```
   Use the returned `job_id` in the next step.

5. **Check status**
   ```bash
   curl http://localhost:8080/jobs/<job_id>
   ```

**Ports:** API REST `:8080`, API metrics `:9090`, Worker health `:9090`.

---

## 2B. Docker Compose (full demo)

**You need:** Docker. Runs API, worker, scheduler, dashboard, MailHog, Loki, Grafana, user-service, billing-service.

1. **Start stack**
   ```bash
   docker compose -f deploy/demo/docker-compose.yml up -d
   ```

2. **Apply schema (once)**
   ```bash
   docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql
   ```

3. **Run demo script** (optional; submits hello, email, report)
   ```bash
   export JOB_API_URL=http://localhost:8083
   ./scripts/demo.sh
   ```
   On Windows use Git Bash or submit jobs from the dashboard.

4. **Use the system**
   - **Dashboard:** http://localhost:8080 — submit jobs, demo scenarios A–E
   - **REST API:** http://localhost:8083
   - **Jaeger (traces):** http://localhost:16686 — submit a job then search by service (job-api / job-worker)
   - **MailHog (emails):** http://localhost:8025
   - **Grafana (logs):** http://localhost:3000 (admin/admin), datasource Loki

5. **Stop**
   ```bash
   docker compose -f deploy/demo/docker-compose.yml down
   ```

---

## 2C. Kubernetes

**You need:** Cluster, Kafka and Postgres (in-cluster or external). See [deploy/kubernetes/README.md](../deploy/kubernetes/README.md).

1. **Edit Secret** with your Postgres DSN: `deploy/kubernetes/secret.yaml`
2. **Apply ConfigMap and Secret**
   ```bash
   kubectl apply -f deploy/kubernetes/configmap.yaml
   kubectl apply -f deploy/kubernetes/secret.yaml
   ```
3. **Deploy apps**
   ```bash
   kubectl apply -f deploy/kubernetes/job-api.yaml
   kubectl apply -f deploy/kubernetes/job-worker.yaml
   kubectl apply -f deploy/kubernetes/job-scheduler.yaml
   ```
4. **Apply Postgres schema** to your DB (e.g. `psql "$POSTGRES_DSN" -f deploy/postgres/schema.sql`).

For **external Kafka/Postgres** (Confluent Cloud, RDS, etc.), set `KAFKA_BROKERS` and `POSTGRES_DSN` in ConfigMap/Secret — see [deploy/kubernetes/README.md](../deploy/kubernetes/README.md).

---

## 3. Day-to-day usage (any run mode)

Use the **REST API** (local: `http://localhost:8080`, Docker: `http://localhost:8083`, K8s: your API Service URL).

| Action | Method | Endpoint / Example |
|--------|--------|--------------------|
| **Submit job** | POST | `/jobs` — body: `{"type":"hello","payload":"world"}` or `email` / `report` / `invoice` / `image` (see [CURL_EXAMPLES.md](CURL_EXAMPLES.md)) |
| **Get status** | GET | `/jobs/<job_id>` |
| **List jobs** | GET | `/jobs?queue=default&status=&limit=20` |
| **Cancel** | POST | `/jobs/<job_id>/cancel` |
| **Retry (DLQ)** | POST | `/jobs/<job_id>/retry` — body: `{"queue":"default"}` |
| **List archived** | GET | `/jobs/archived?queue=&type=&limit=20` |
| **List queues** | GET | `/admin/queues` |
| **Pause queue** | POST | `/admin/queues/<name>/pause` |
| **Unpause queue** | POST | `/admin/queues/<name>/unpause` |
| **Schedules** | GET/POST/DELETE | `/schedules` — create/list/delete recurring (cron) jobs |

**CLI enqueue:** `JOB_API_URL=<api_url> go run ./cmd/enqueue <type> "<payload>"`

---

## 4. Job types (payloads)

| Type | Payload example |
|------|------------------|
| `hello` | `"world"` (any string) |
| `email` | `{"to":"u@x.com","subject":"Hi","body":"Hello"}` |
| `report` | `{"headers":["A","B"],"rows":[[1,2]],"out_path":"/out/x.csv"}` |
| `invoice` | `{"template":"...", "data":{}, "out_path":"/out/x.txt"}` |
| `image` | `{"source_url":"...", "width":200, "height":200, "out_path":"..."}` |

---

## 5. Where to read more

- **Full how-to (options, troubleshooting):** [HOW_TO_USE.md](HOW_TO_USE.md)
- **All REST examples (curl):** [CURL_EXAMPLES.md](CURL_EXAMPLES.md)
- **Tracing (OpenTelemetry, Jaeger, OTLP):** [TRACING.md](TRACING.md)
- **Upstream services (user-service, billing-service):** [UPSTREAM_SERVICES.md](UPSTREAM_SERVICES.md)
- **Scheduler (fake jobs):** [SCHEDULER.md](SCHEDULER.md)
- **Kubernetes (external Kafka/Postgres):** [deploy/kubernetes/README.md](../deploy/kubernetes/README.md)
