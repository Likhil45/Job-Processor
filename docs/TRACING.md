# Tracing (OpenTelemetry)

The job API and worker use **OpenTelemetry** for distributed tracing. Traces connect **job submit** (API) → Kafka → **job process** (worker) so you can see latency and flow in one place.

---

## How it works

- **API** creates a span `job.submit` with attributes `job.type`, `job.queue`, `job.id`, and injects trace context into the Kafka message.
- **Worker** extracts that context from the message and starts a child span `job.process`, so one trace spans both services.

---

## Configuration

Tracing is controlled by **`OTEL_EXPORTER_OTLP_ENDPOINT`**:

| Setting | Behavior |
|--------|----------|
| **Unset (default)** | Spans are exported to **stdout** (pretty-printed JSON). Use for local dev without a backend. |
| **Set** | Spans are sent via **OTLP HTTP** to the given endpoint (e.g. Jaeger, Grafana Tempo, Otel Collector). |

---

## Docker Compose demo

The demo stack includes **Jaeger** and sets `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318` for job-api and job-worker.

1. Start the stack: `docker compose -f deploy/demo/docker-compose.yml up -d`
2. Apply schema and **submit at least one job** (dashboard at http://localhost:8080 or `curl -X POST http://localhost:8083/jobs -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'`). Services appear in Jaeger only after traces are received.
3. Open **http://localhost:16686** (Jaeger UI) → **Search** → choose service `job-api` or `job-worker` → Find Traces. You should see traces containing `job.submit` and `job.process`.

---

## Local run (no Docker)

**Stdout (no backend):**

```bash
export POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
export KAFKA_BROKERS=localhost:9092
go run ./cmd/api
# In another terminal (same env, no OTEL var):
go run ./cmd/worker
```

Log line: `tracing: using stdout exporter`. Submit a job; span JSON appears in the API and worker terminals.

**OTLP (e.g. Jaeger on host):**

```bash
# Start Jaeger: docker run -d --name jaeger -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:1.76.0
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export POSTGRES_DSN=postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable
export KAFKA_BROKERS=localhost:9092
go run ./cmd/api
# Same env in another terminal:
go run ./cmd/worker
```

Then open http://localhost:16686 and search for traces.

---

## Troubleshooting: no services in Jaeger

### Only one service in the dropdown (e.g. “jaeger-all-in-one-service”)

That means **job-api** and **job-worker** are not sending traces. Usually they failed to connect to Jaeger at startup (e.g. Jaeger wasn’t ready yet) and are running without tracing.

**Fix:**

1. **Restart API and worker** so they connect to Jaeger now that it’s up:
   ```bash
   docker compose -f deploy/demo/docker-compose.yml restart job-api job-worker
   ```
2. **Confirm tracing is enabled** in their logs. You should see:
   - `tracing: using OTLP exporter` and `endpoint=http://jaeger:4318`
   If you see **`continuing without tracing`**, the OTLP connection failed at startup; the restart in step 1 usually fixes it.
   ```bash
   docker compose -f deploy/demo/docker-compose.yml logs job-api 2>&1 | findstr /i tracing
   docker compose -f deploy/demo/docker-compose.yml logs job-worker 2>&1 | findstr /i tracing
   ```
   (On Linux/macOS use `grep -i tracing` instead of `findstr /i tracing`.)
3. **Submit at least one job** so spans are created (dashboard http://localhost:8080 or curl below).
4. **Wait 5–10 seconds** (batch export), then in Jaeger click **Find Traces**. The **Service** dropdown should now list **job-api** and **job-worker**.

### Other checks

- **Submit a job first.** The service list is populated only after at least one trace is received:
  `curl -X POST http://localhost:8083/jobs -H "Content-Type: application/json" -d '{"type":"hello","payload":"world"}'`
- **Restart the stack with a clean order:** `docker compose -f deploy/demo/docker-compose.yml down` then `up -d` so Jaeger (and its healthcheck) start first; then submit a job and open Jaeger.

---

## Kubernetes

Set `OTEL_EXPORTER_OTLP_ENDPOINT` in the ConfigMap or in the Deployment env to your OTLP HTTP endpoint (e.g. `http://jaeger-collector:4318` or a managed tracing service). Ensure the endpoint is reachable from the API and worker pods.
