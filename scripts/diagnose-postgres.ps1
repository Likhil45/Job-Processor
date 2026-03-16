# Diagnose why Postgres container is unhealthy. Writes NDJSON to debug-bc5d7b.log.
# Run from repo root: .\scripts\diagnose-postgres.ps1
# Requires: docker compose -f deploy/demo/docker-compose.yml was run (may have failed).

$ErrorActionPreference = "SilentlyContinue"
$ComposeFile = "deploy/demo/docker-compose.yml"
$LogFile = "debug-bc5d7b.log"
$ts = [long](Get-Date -UFormat %s) * 1000

function Write-DebugLog($hypothesisId, $message, $data) {
    $payload = @{
        sessionId = "bc5d7b"
        hypothesisId = $hypothesisId
        location = "diagnose-postgres.ps1"
        message = $message
        data = $data
        timestamp = $ts
    } | ConvertTo-Json -Compress -Depth 4
    $payload | Out-File -FilePath $LogFile -Append -Encoding utf8
}

# A: Health status and last healthcheck log (start_period / timing)
$cid = docker compose -f $ComposeFile ps -q postgres 2>$null
if ($cid) {
    $health = docker inspect $cid --format '{{json .State.Health}}' 2>$null
    $status = docker inspect $cid --format '{{.State.Status}}' 2>$null
    Write-DebugLog "A_start_period" "postgres container health" @{ containerId = $cid.Trim(); status = $status; healthJson = $health }
} else {
    Write-DebugLog "A_start_period" "postgres container not found" @{ note = "compose may have stopped after failure" }
}

# B: Port 5432 in use on host (conflict)
$port5432 = Get-NetTCPConnection -LocalPort 5432 -ErrorAction SilentlyContinue | Select-Object -First 3
$portUsed = ($port5432 | Measure-Object).Count -gt 0
Write-DebugLog "B_port_conflict" "port 5432 usage" @{ inUse = $portUsed; count = ($port5432 | Measure-Object).Count }

# C/D/E: Postgres container logs (init timing, crash, errors)
$logs = docker compose -f $ComposeFile logs postgres --tail 100 2>&1 | Out-String
$snippet = if ($logs.Length -gt 2500) { $logs.Substring($logs.Length - 2500) } else { $logs }
Write-DebugLog "C_init_timing" "postgres logs tail" @{ logSnippet = $snippet }
Write-DebugLog "D_crash" "postgres logs for errors" @{ hasFatal = ($logs -match "FATAL|panic|killed"); hasError = ($logs -match "ERROR") }
Write-DebugLog "E_resource" "postgres logs for OOM/resource" @{ hasOOM = ($logs -match "out of memory|OOM|Cannot allocate"); logLen = $logs.Length }

# Healthcheck config (start_period present?)
$hc = docker compose -f $ComposeFile config 2>&1 | Out-String
$hasStartPeriod = $hc -match "start_period"
Write-DebugLog "A_start_period" "compose healthcheck config" @{ hasStartPeriod = $hasStartPeriod }
