#!/usr/bin/env sh
# Diagnose why Grafana/Loki shows no logs. Writes NDJSON to debug-bc5d7b.log for evidence.
# Run from repo root: ./scripts/diagnose-loki.sh
# Requires: docker compose -f deploy/demo/docker-compose.yml running.

COMPOSE_FILE="deploy/demo/docker-compose.yml"
LOG_FILE="debug-bc5d7b.log"
ts() { date +%s000 2>/dev/null || date +%s; }

escape() { echo "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\n/ /g'; }

append() { printf '%s\n' "$1" >> "$LOG_FILE"; }

: > "$LOG_FILE"

# 1. Is Loki plugin installed and enabled?
plugin_status=$(docker plugin ls --format '{{.Name}} {{.Enabled}}' 2>&1 | grep -i loki || echo "no_loki_plugin")
append "{\"sessionId\":\"bc5d7b\",\"hypothesisId\":\"plugin_installed\",\"location\":\"diagnose-loki.sh\",\"message\":\"docker plugin ls (loki)\",\"data\":{\"output\":\"$(escape "$plugin_status")\"},\"timestamp\":$(ts)}"

# 2. What log driver is job-api actually using?
driver=$(docker compose -f "$COMPOSE_FILE" ps -q job-api 2>/dev/null | head -1 | xargs -I{} docker inspect {} --format '{{.HostConfig.LogConfig.Type}}' 2>/dev/null || echo "no_container")
append "{\"sessionId\":\"bc5d7b\",\"hypothesisId\":\"container_driver\",\"location\":\"diagnose-loki.sh\",\"message\":\"job-api log driver\",\"data\":{\"driver\":\"$driver\"},\"timestamp\":$(ts)}"

# 3. Does Loki respond and have any labels?
loki_labels=$(curl -s -m 5 "http://127.0.0.1:3100/loki/api/v1/labels" 2>/dev/null || echo "curl_failed")
append "{\"sessionId\":\"bc5d7b\",\"hypothesisId\":\"loki_has_data\",\"location\":\"diagnose-loki.sh\",\"message\":\"Loki /loki/api/v1/labels\",\"data\":{\"response\":\"$(escape "$loki_labels")\"},\"timestamp\":$(ts)}"

# 4. Query Loki for any stream
loki_query=$(curl -s -m 5 -G "http://127.0.0.1:3100/loki/api/v1/query_range" --data-urlencode 'query={container=~".+"}' --data-urlencode "limit=1" 2>/dev/null | head -c 300 || echo "curl_failed")
append "{\"sessionId\":\"bc5d7b\",\"hypothesisId\":\"loki_query\",\"location\":\"diagnose-loki.sh\",\"message\":\"Loki query\",\"data\":{\"snippet\":\"$(escape "$loki_query")\"},\"timestamp\":$(ts)}"

echo "Diagnostics written to $LOG_FILE"
