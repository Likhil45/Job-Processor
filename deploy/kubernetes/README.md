# Kubernetes deployment (Kafka + Postgres stack)

Manifests for running the **job-api**, **job-worker**, and **job-scheduler** on Kubernetes with the Kafka+Postgres backend. Health and readiness use existing HTTP endpoints (`/health`, `/ready`, `/metrics`).

## Prerequisites

- Kubernetes cluster (e.g. minikube, kind, EKS, GKE)
- Kafka and Postgres available (in-cluster or external)
- Container images built and pushed (or use local images with `imagePullPolicy: IfNotPresent`)

## Apply order

1. Create **ConfigMap** and **Secret** (edit `secret.yaml` with your real Postgres DSN first):
   ```bash
   kubectl apply -f configmap.yaml
   kubectl apply -f secret.yaml
   ```
2. Deploy applications (API, worker, scheduler):
   ```bash
   kubectl apply -f job-api.yaml
   kubectl apply -f job-worker.yaml
   kubectl apply -f job-scheduler.yaml
   ```

## External Kafka and Postgres

Kafka and Postgres **do not need to run inside the cluster**. You can use managed services and point the jobs stack at them:

- **Kafka:** Confluent Cloud, AWS MSK, Azure Event Hubs (Kafka API), or any Kafka reachable from the cluster. Set `KAFKA_BROKERS` in the ConfigMap (or override per deployment) to your broker list (e.g. `pkc-xxx.us-east-1.aws.confluent.cloud:9092`). Use TLS/SASL if required (configure via env or a custom image).
- **Postgres:** Amazon RDS, Google Cloud SQL, Azure Database for PostgreSQL, or any Postgres reachable from the cluster. Set `POSTGRES_DSN` in the Secret to your connection string (e.g. `postgres://user:pass@rds-host:5432/jobs?sslmode=require`). Rotate secrets via your normal process (e.g. external secrets operator).

For external services, either:

- Replace the default `job-app-config` ConfigMap and `job-app-secret` Secret with values pointing to your Kafka and Postgres, or  
- Create a separate ConfigMap/Secret (e.g. `job-app-config-external`) and use `envFrom` to reference it in the Deployments instead of (or in addition to) the default ones.

## Postgres schema

Apply the schema to your Postgres instance before or right after deploying (e.g. from the repo root):

```bash
psql "$POSTGRES_DSN" -f deploy/postgres/schema.sql
```

## Optional: Kafka and Postgres in-cluster

If you run Kafka and Postgres inside the same cluster, create their Deployments/Services first and ensure the ConfigMap uses the correct service names (e.g. `kafka:9092`, `postgres:5432`). The default `configmap.yaml` and `secret.yaml` are set up for in-cluster services named `kafka` and `postgres`.

## Optional: HPA

For high availability you can add HorizontalPodAutoscaler for the API and worker based on CPU or custom metrics (e.g. queue depth):

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: job-api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: job-api
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```
