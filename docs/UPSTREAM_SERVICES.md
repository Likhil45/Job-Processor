# How User-Service and Billing-Service Are Used

These are **demo upstream services**: they don’t run jobs themselves. They expose HTTP APIs that your app (or you via curl) call; they then **submit jobs to the Job API**, and the **workers** do the actual work (send email, generate report, etc.).

```
You / Your app  →  user-service or billing-service  →  Job API (REST)  →  Redis  →  Workers
```

---

## User-Service (port 8081)

**Purpose:** Simulates a “user” or “auth” service that triggers **email jobs** when users register or request a password reset.

| Endpoint | Method | Body | What it does |
|----------|--------|------|----------------|
| `/register` | POST | `{"email":"user@example.com"}` | Submits an **email** job: “Welcome” email to that address. |
| `/password-reset` | POST | `{"email":"user@example.com"}` | Submits an **email** job: “Password reset” email to that address. |

**Flow:**  
1. You call user-service (e.g. after a user signs up).  
2. User-service calls the Job API: `POST /jobs` with `type: "email"` and the right payload.  
3. Job API enqueues the task in Redis.  
4. A worker picks it up and sends the email (e.g. via MailHog in dev).  
5. User-service returns the `job_id` so you can track status.

**Run locally:**
```bash
# Job API must be running (e.g. on :8080)
export JOB_API_URL=http://localhost:8080
go run ./cmd/user-service
```

**Example calls:**
```bash
# Welcome email after registration
curl -X POST http://localhost:8081/register \
  -H "Content-Type: application/json" \
  -d '{"email":"newuser@example.com"}'

# Password reset email
curl -X POST http://localhost:8081/password-reset \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com"}'
```

Response: `{"job_id":"...", "message":"welcome email queued"}` (or `"password reset email queued"`).

---

## Billing-Service (port 8082)

**Purpose:** Simulates a “billing” service that triggers **invoice**, **report**, and **email** jobs.

| Endpoint | Method | Body | Job submitted |
|----------|--------|------|----------------|
| `/invoice` | POST | `{"id":"INV-001","total":"99.00"}` (optional) | **invoice** — render template, write file (e.g. `/out/invoice-INV-001.txt`). |
| `/report` | POST | (none or empty) | **report** — generate CSV (e.g. `/out/billing-report.csv`). |
| `/invoice-ready` | POST | `{"email":"billing@example.com"}` (optional) | **email** — “Your invoice is ready” to that address. |

**Flow:** Same as user-service: you call billing-service → it calls Job API → workers run the job. You get back a `job_id`.

**Run locally:**
```bash
export JOB_API_URL=http://localhost:8080
go run ./cmd/billing-service
```

**Example calls:**
```bash
# Generate an invoice (worker writes a file)
curl -X POST http://localhost:8082/invoice \
  -H "Content-Type: application/json" \
  -d '{"id":"INV-002","total":"150.00"}'

# Generate a report (worker writes CSV)
curl -X POST http://localhost:8082/report

# Send “invoice ready” email
curl -X POST http://localhost:8082/invoice-ready \
  -H "Content-Type: application/json" \
  -d '{"email":"customer@example.com"}'
```

---

## With Docker Compose

When you run `docker compose -f deploy/demo/docker-compose.yml up -d`:

- **User-service** is at `http://localhost:8081`; it talks to the Job API at `http://job-api:8080` inside the network.
- **Billing-service** is at `http://localhost:8082`; same Job API URL.

You still call **user-service** and **billing-service** from your machine (or from another service) on 8081 and 8082. They are the entry points; the Job API and workers run in the background.

---

## Why use them?

- **Decoupling:** Your app calls “register” or “create invoice”; it doesn’t need to know about job types, queues, or the Job API’s payload format.
- **Demo:** They show how real services would enqueue work (email, invoice, report) without calling the Job API directly.
- **Testing:** Easy to drive flows (welcome email, invoice generation, report) with a few curl commands.

To **check that a job ran**, use the returned `job_id` with the Job API or dashboard:

```bash
curl http://localhost:8083/jobs/<job_id>
```

(Use port 8083 when the Job API is in Docker; 8080 when the API runs on the host.)

### If you get "job api error"

- **From Docker:** User-service and billing-service call the Job API at `http://job-api:8080` (REST). Ensure the Job API container is up and the REST server is listening on port 8080 inside the stack. After the fix, the response body from the Job API is returned (e.g. `{"error":"..."}`) so you can see the real reason.
- **From host:** If you run user-service or billing-service on the host, set `JOB_API_URL=http://localhost:8080` (or `http://localhost:8083` if the API runs in Docker and you’re calling from the host).
- **Connection refused:** Start the Job API first, then the upstream services.
