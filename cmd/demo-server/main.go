// Demo server: serves a simple dashboard and proxies job submit/status to the Job API REST.
// Usage: JOB_API_URL=http://localhost:8080 go run ./cmd/demo-server
package main

import (
	_ "embed"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed static/index.html
var indexHTML []byte

var jobAPIBaseURL string

func main() {
	jobAPIBaseURL = strings.TrimSuffix(getEnv("JOB_API_URL", "http://localhost:8080"), "/")

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/submit", handleSubmit)
	http.HandleFunc("/api/status/", handleStatus)
	http.HandleFunc("/api/scenario/", handleScenario)
	http.HandleFunc("/api/archived", handleArchived)
	http.HandleFunc("/api/retry", handleRetry)
	http.HandleFunc("/api/links", handleLinks)

	addr := getEnv("LISTEN_ADDR", ":8080")
	slog.Info("demo server listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("http serve", "err", err)
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func handleLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	links := map[string]string{
		"mailhog":    "http://localhost:8025",
		"report":     "See ./out/demo-report.csv after running the demo",
		"job_api":    jobAPIBaseURL,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(links)
}

func handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid json", http.StatusBadRequest)
		return
	}
	req.Type = strings.TrimSpace(req.Type)
	if req.Type == "" {
		writeJSONError(w, "type required", http.StatusBadRequest)
		return
	}

	payload := getDefaultPayload(req.Type)
	body := map[string]interface{}{
		"type":    req.Type,
		"payload": json.RawMessage(payload),
	}
	raw, _ := json.Marshal(body)
	hr, err := http.NewRequest(http.MethodPost, jobAPIBaseURL+"/jobs", bytes.NewReader(raw))
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		slog.Warn("submit job", "type", req.Type, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		slog.Warn("submit job", "type", req.Type, "status", resp.Status, "body", string(b))
		w.WriteHeader(resp.StatusCode)
		w.Write(b)
		return
	}
	var out struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"job_id": out.JobID})
}

func getDefaultPayload(jobType string) []byte {
	switch jobType {
	case "hello":
		return []byte("world")
	case "email":
		return []byte(`{"to":"demo@localhost","subject":"From Dashboard","body":"Submitted via the demo dashboard."}`)
	case "report":
		return []byte(`{"headers":["Name","Count"],"rows":[["Dashboard","1"],["Demo","2"]],"out_path":"/out/demo-report.csv"}`)
	case "invoice":
		return []byte(`{"template":"Invoice #{{.ID}}\nTotal: ${{.Total}}","data":{"ID":"DEMO-001","Total":"99.00"},"out_path":""}`)
	case "image":
		return []byte(`{"source_url":"https://via.placeholder.com/100","width":50,"height":50,"out_path":""}`)
	case "sleep":
		return []byte(`{"seconds":30}`)
	default:
		return []byte("{}")
	}
}

func handleScenario(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	scenarioID := strings.TrimPrefix(r.URL.Path, "/api/scenario/")
	scenarioID = strings.TrimSpace(strings.ToUpper(scenarioID))
	if scenarioID == "" {
		writeJSONError(w, "scenario required (A, B, C, D, E)", http.StatusBadRequest)
		return
	}

	var body struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
		Options *struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		} `json:"options"`
	}
	switch scenarioID {
	case "A":
		body.Type = "email"
		body.Payload = getDefaultPayload("email")
		body.Options = &struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		}{MaxRetry: 0}
	case "B":
		body.Type = "invoice"
		body.Payload = []byte(`{"template":"Invoice #{{.ID}}\nTotal: ${{.Total}}","data":{"ID":"RETRY-DEMO","Total":"42.00","_demo_fail_until_attempt":1},"out_path":""}`)
		body.Options = &struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		}{MaxRetry: 2}
	case "C":
		body.Type = "report"
		body.Payload = []byte(`{"headers":["Name","Count"],"rows":[["Delayed","1"]],"out_path":"/out/delayed-report.csv"}`)
		runAt := time.Now().Unix() + 60
		body.Options = &struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		}{RunAtUnixSec: runAt}
	case "D":
		body.Type = "image"
		body.Payload = []byte(`{}`)
		body.Options = &struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		}{MaxRetry: 2}
	case "E":
		body.Type = "sleep"
		body.Payload = []byte(`{"seconds":30}`)
		body.Options = &struct {
			MaxRetry     int32 `json:"max_retry"`
			RunAtUnixSec int64 `json:"run_at_unix_sec"`
		}{MaxRetry: 0}
	default:
		writeJSONError(w, "unknown scenario (use A, B, C, D, E)", http.StatusBadRequest)
		return
	}

	raw, _ := json.Marshal(body)
	hr, err := http.NewRequest(http.MethodPost, jobAPIBaseURL+"/jobs", bytes.NewReader(raw))
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		slog.Warn("scenario submit", "scenario", scenarioID, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		slog.Warn("scenario submit", "scenario", scenarioID, "status", resp.Status, "body", string(b))
		w.WriteHeader(resp.StatusCode)
		w.Write(b)
		return
	}
	var out struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	steps := scenarioSteps(scenarioID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"scenario": scenarioID,
		"job_id":   out.JobID,
		"steps":    steps,
		"message":  "Check status below or wait for worker to process.",
	})
}

func scenarioSteps(id string) []string {
	switch id {
	case "A":
		return []string{"Job submitted", "Worker picks from queue", "Status → completed", "Execution history in DB"}
	case "B":
		return []string{"Invoice job submitted (will fail attempt 1)", "Worker retries with backoff", "Attempt 2 succeeds", "Status → completed"}
	case "C":
		return []string{"Report job scheduled 60s in future", "Status stays scheduled", "Scheduler promotes when due", "Worker runs → completed"}
	case "D":
		return []string{"Image job with invalid payload submitted", "Worker fails repeatedly", "Max retries exceeded → archived (DLQ)", "Use “Replay (valid image)” below to submit a fixed job"}
	case "E":
		return []string{"Long-running sleep job (30s) submitted", "Kill the worker pod/process while it runs", "Restart worker; Kafka redelivers the message", "Job completes on recovery"}
	default:
		return nil
	}
}

func handleArchived(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	url := jobAPIBaseURL + "/jobs/archived?" + r.URL.RawQuery
	resp, err := http.Get(url)
	if err != nil {
		slog.Warn("archived list", "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(b)
}

func handleRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		JobID string `json:"job_id"`
		Queue string `json:"queue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid json", http.StatusBadRequest)
		return
	}
	req.JobID = strings.TrimSpace(req.JobID)
	if req.JobID == "" {
		writeJSONError(w, "job_id required", http.StatusBadRequest)
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	body, _ := json.Marshal(map[string]string{"queue": req.Queue})
	hr, err := http.NewRequest(http.MethodPost, jobAPIBaseURL+"/jobs/"+req.JobID+"/retry", bytes.NewReader(body))
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		slog.Warn("retry archived", "job_id", req.JobID, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(b)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jobID := strings.TrimPrefix(r.URL.Path, "/api/status/")
	if jobID == "" {
		writeJSONError(w, "job_id required", http.StatusBadRequest)
		return
	}
	resp, err := http.Get(jobAPIBaseURL + "/jobs/" + jobID)
	if err != nil {
		slog.Warn("get status", "job_id", jobID, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		w.Write(b)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}
