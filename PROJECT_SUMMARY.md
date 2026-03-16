# Distributed Job Processing System — Project Summary

One-page overview of the **current** stack (Kafka + Postgres). For workflows and demos see [README.md](README.md) and [docs/WORKFLOW.md](docs/WORKFLOW.md).

---

## Overview

**Purpose:** Accept job submissions via a **REST API**, enqueue them to **Kafka** (`job.requests`), and process them with **workers** that update **PostgreSQL**. Supports multiple job types (hello, email, image, invoice, report), configurable retries, delayed/scheduled jobs, and observability (Prometheus, slog, health probes, optional OTLP tracing).

**Stack:** Kafka (KRaft, no Zookeeper) + PostgreSQL. No Redis in the current implementation.

---

## Architecture (data flow)

1. **Submit:** Client → REST API → API writes job to Postgres and produces to Kafka `job.requests`.
2. **Process:** Worker consumes from Kafka → runs handler by type → updates Postgres (status) and optionally produces to `job.events`.
3. **Status / list / retry:** Client → REST API → reads from Postgres (and admin/queues from store).
4. **DLQ:** After max retries, worker sets status to `archived` in Postgres; list/retry via REST or gRPC.

---

## Components

| Component | Location | Role |
|-----------|----------|------|
| **Job API** | `cmd/api` | REST server (submit, status, list, cancel, retry, admin); produces to Kafka; reads/writes Postgres. |
| **Worker** | `cmd/worker` | Consumes `job.requests`; runs `internal/jobs` handlers; updates Postgres; produces `job.events`. |
| **Scheduler** | `cmd/scheduler` | Periodic fake jobs; produces to Kafka + Postgres. |
| **Kafka queue** | `internal/kafkaqueue` | Producer (job.requests) and consumer for workers. |
| **Store** | `internal/store` | Postgres job metadata (create, status, list, archived). |
| **Jobs** | `internal/jobs` | Handlers: hello, email, image, invoice, report, sleep. |
| **Events** | `internal/events` | Optional Kafka lifecycle events (`job.events`). |
| **Deploy demo** | `deploy/demo/` | Docker Compose: Kafka, Postgres, API, worker, dashboard, MailHog, Jaeger, Loki, Grafana. |
| **Deploy K8s** | `deploy/kubernetes/` | Manifests for job-api, job-worker, job-scheduler (Kafka/Postgres in-cluster or external). |

---

## Deploy layout

- **deploy/demo/** — Docker Compose; primary way to run the full stack locally.
- **deploy/kubernetes/** — Kafka+Postgres deployments for a cluster.
- **deploy/postgres/** — Schema only.
- **deploy/minikube/** — Legacy Redis-based manifests; see [deploy/minikube/README.md](deploy/minikube/README.md).
