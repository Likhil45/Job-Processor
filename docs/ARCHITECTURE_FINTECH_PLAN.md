# Architecture Diagram — Fintech Production Features Plan

This diagram reflects the target architecture described in the [Fintech Production Features](c:\Users\likhi\.cursor\plans\fintech_production_features_42b4ad89.plan.md) plan (Phases 1–5).

---

## High-level architecture

```mermaid
flowchart TB
    subgraph Clients["Clients"]
        REST["REST clients"]
        Dashboard["DLQ Dashboard"]
    end

    subgraph API["Phase 2.1: Rate limit"]
        REST_API["Job API (REST)"]
    end

    subgraph DataCorrectness["Phase 1: Data correctness"]
        Idem["Idempotency check\n(store.GetByIdempotencyKey)"]
        Backend["Backend\n(Submit, ListQueues, Pause)"]
    end

    subgraph Persistence["PostgreSQL"]
        Jobs[("jobs\n+ idempotency_key")]
        Outbox[("job_outbox\npending → sent/failed")]
        Queues[("queues\nname, paused")]
        Schedules[("schedules\ncron, next_run_at")]
    end

    subgraph Messaging["Kafka"]
        Topic["job.requests\n(priority in msg)"]
    end

    subgraph WorkerLayer["Worker layer"]
        Consumer["Kafka Consumer"]
        Semaphore["Phase 2.2: Semaphore\nper-type concurrency"]
        PauseCheck["Phase 2.3: Queue paused?\n(skip or re-enqueue)"]
        Handler["Job handlers\n(email, payment, …)"]
        OutboxWriter["Write to job_outbox\n(Phase 1.2)"]
    end

    subgraph OutboxProcessor["Outbox processor"]
        OutboxReader["Read pending"]
        SideEffect["Side-effect\n(email, payment gateway)"]
    end

    subgraph Scheduler["Phase 4.2: Scheduler"]
        CronLoop["Every minute:\nnext_run_at ≤ now"]
        EnqueueSched["Create job + Enqueue"]
    end

    subgraph Observability["Phase 3: Observability"]
        OTel["OpenTelemetry\n(API → Kafka → Worker)"]
        Metrics["Metrics\nlatency, DLQ, retries, SLA"]
        DLQ_UI["DLQ UI\n(List, Replay)"]
    end

    REST --> REST_API
    Dashboard --> REST_API
    REST_API --> Idem
    Idem --> Backend
    Backend --> Jobs
    Backend --> Queues
    Backend --> Topic
    Consumer --> Topic
    Consumer --> PauseCheck
    PauseCheck --> Semaphore
    Semaphore --> Handler
    Handler --> OutboxWriter
    OutboxWriter --> Outbox
    Outbox --> OutboxReader
    OutboxReader --> SideEffect
    CronLoop --> Schedules
    CronLoop --> EnqueueSched
    EnqueueSched --> Jobs
    EnqueueSched --> Topic
    REST_API -.-> OTel
    Consumer -.-> OTel
    Handler -.-> Metrics
    Dashboard -.-> DLQ_UI
    Backend -.-> DLQ_UI
```

---

## Component view (by phase)

```mermaid
flowchart LR
    subgraph Phase1["Phase 1: Data correctness"]
        P1A["Idempotency\n(key → return existing ID)"]
        P1B["Outbox table\n+ processor"]
    end

    subgraph Phase2["Phase 2: Reliability"]
        P2A["Rate limit\n(API 429)"]
        P2B["Worker semaphore\n(per-type)"]
        P2C["Queues table\n+ Pause/Unpause"]
    end

    subgraph Phase3["Phase 3: Observability"]
        P3A["Tracing\n(OTel)"]
        P3B["Metrics\n(latency, DLQ, SLA)"]
        P3C["DLQ UI\n(List, Replay)"]
    end

    subgraph Phase4["Phase 4: Product"]
        P4A["Priority\n(worker ordering)"]
        P4B["Recurring\n(schedules + cron)"]
        P4C["SLA breach\n(metric + event)"]
    end

    subgraph Phase5["Phase 5: Ops"]
        P5A["K8s manifests"]
        P5B["Drain\n(graceful stop)"]
    end

    Phase1 --> Phase2 --> Phase3 --> Phase4 --> Phase5
```

---

## Data flow (submit → process → outbox)

```mermaid
sequenceDiagram
    participant C as Client
    participant API as REST API
    participant Store as Postgres (store)
    participant K as Kafka
    participant W as Worker
    participant O as Outbox processor

    C->>API: POST /jobs (idempotency_key?)
    API->>Store: GetByIdempotencyKey
    alt job exists
        Store-->>API: job_id
        API-->>C: 201 job_id (no new enqueue)
    else new
        API->>Store: Create(job + idempotency_key)
        API->>K: Produce job request
        API-->>C: 201 job_id
    end

    K->>W: Consume message
    W->>Store: GetQueue(paused?)
    alt paused
        W->>W: skip / re-enqueue
    else not paused
        W->>W: semaphore.Acquire
        W->>W: Handler.Run
        W->>Store: Update job status + Write outbox row
        W->>W: semaphore.Release
    end

    O->>Store: Select pending outbox
    O->>O: Side-effect (e.g. send email)
    O->>Store: Update outbox sent/failed
```

---

## Deployment (Phase 5.1 — Kubernetes)

```mermaid
flowchart TB
    subgraph K8s["Kubernetes cluster"]
        subgraph API_Deploy["Deployment: job-api"]
            API_Pods["Replicas (HPA optional)"]
        end
        subgraph Worker_Deploy["Deployment: job-worker"]
            Worker_Pods["Replicas (per partition)"]
        end
        subgraph Scheduler_Deploy["Deployment: job-scheduler"]
            Scheduler_Pod["Single or leader-elected"]
        end
    end

    subgraph External["External (or in-cluster)"]
        Postgres["PostgreSQL\n(RDS or StatefulSet)"]
        Kafka["Kafka\n(Confluent or Strimzi)"]
    end

    API_Deploy --> Postgres
    API_Deploy --> Kafka
    Worker_Deploy --> Kafka
    Worker_Deploy --> Postgres
    Scheduler_Deploy --> Postgres
    Scheduler_Deploy --> Kafka
```

---

## Legend

| Symbol / area | Meaning |
|---------------|---------|
| **Phase 1** | Idempotency (store + API); Outbox table + processor for exactly-once side-effects |
| **Phase 2** | Rate limit (API); Worker semaphore; Queues table + Pause/Unpause + worker check |
| **Phase 3** | OpenTelemetry tracing; Richer metrics; DLQ UI (list/retry from dashboard) |
| **Phase 4** | Job priority (Kafka/worker); Recurring jobs (schedules + scheduler); SLA breach metric/event |
| **Phase 5** | Kubernetes manifests for API, worker, scheduler; Worker drain for safe rollouts |

To render the Mermaid diagrams, use a Markdown viewer with Mermaid support (e.g. GitHub, GitLab, VS Code with Mermaid extension) or [Mermaid Live Editor](https://mermaid.live).
