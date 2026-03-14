#!/usr/bin/env bash
# Demo script: submits hello, email, and report jobs to the Job API.
# Prerequisites: stack running (e.g. docker compose -f deploy/demo/docker-compose.yml up -d)
#               grpcurl installed, API on localhost:50051

set -e
API="${JOB_API_ADDR:-localhost:50051}"
# Ensure out dir exists for report job output (when using docker-compose bind mount)
mkdir -p out 2>/dev/null || true

echo "=== Job Queue Demo ==="
echo "API: $API"
echo ""

# Wait for API to be reachable
for i in 1 2 3 4 5 6 7 8 9 10; do
  if grpcurl -plaintext -connect-timeout 1 "$API" list >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "Error: API at $API not reachable. Start the stack first (e.g. docker compose -f deploy/demo/docker-compose.yml up -d)"
    exit 1
  fi
  echo "Waiting for API..."
  sleep 2
done

echo "Submitting jobs..."
echo ""

# Extract job_id from grpcurl JSON (handles "jobId" or "job_id", and multiline)
job_id_from_resp() { echo "$1" | tr -d '\n\r' | grep -oE '"(jobId|job_id)"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | grep -oE '"[^"]*"$' | tr -d '"'; }

# 1. Hello job (payload: "world" -> base64)
HELLO_PAYLOAD=$(echo -n "world" | base64 | tr -d '\n')
HELLO_RESP=$(grpcurl -plaintext -d "{\"type\":\"hello\",\"payload\":\"$HELLO_PAYLOAD\"}" "$API" job.v1.JobService/SubmitJob)
HELLO_ID=$(job_id_from_resp "$HELLO_RESP")
echo "1. Hello job   -> job_id: $HELLO_ID"

# 2. Email job (payload JSON -> base64)
EMAIL_JSON='{"to":"demo@localhost","subject":"Demo Email","body":"Hello from the job queue! This was sent by the worker via MailHog."}'
EMAIL_PAYLOAD=$(echo -n "$EMAIL_JSON" | base64 | tr -d '\n')
EMAIL_RESP=$(grpcurl -plaintext -d "{\"type\":\"email\",\"payload\":\"$EMAIL_PAYLOAD\"}" "$API" job.v1.JobService/SubmitJob)
EMAIL_ID=$(job_id_from_resp "$EMAIL_RESP")
echo "2. Email job   -> job_id: $EMAIL_ID"

# 3. Report job (CSV written to /out in container -> bind mount to ./out on host)
REPORT_JSON='{"headers":["Name","Count"],"rows":[["Hello","1"],["World","2"],["Job Queue","3"]],"out_path":"/out/demo-report.csv"}'
REPORT_PAYLOAD=$(echo -n "$REPORT_JSON" | base64 | tr -d '\n')
REPORT_RESP=$(grpcurl -plaintext -d "{\"type\":\"report\",\"payload\":\"$REPORT_PAYLOAD\"}" "$API" job.v1.JobService/SubmitJob)
REPORT_ID=$(job_id_from_resp "$REPORT_RESP")
echo "3. Report job  -> job_id: $REPORT_ID"

echo ""
echo "=== Where to see results ==="
echo "  Emails:    http://localhost:8025  (MailHog)"
echo "  Report:    ./out/demo-report.csv (after worker runs)"
echo "  Dashboard: http://localhost:8080"
echo ""
echo "Check status of a job:"
echo "  grpcurl -plaintext -d '{\"jobId\":\"$HELLO_ID\"}' $API job.v1.JobService/GetJobStatus"
echo ""
