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
)

//go:embed static/index.html
var indexHTML []byte

var jobAPIBaseURL string

func main() {
	jobAPIBaseURL = strings.TrimSuffix(getEnv("JOB_API_URL", "http://localhost:8080"), "/")

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/submit", handleSubmit)
	http.HandleFunc("/api/status/", handleStatus)
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
	default:
		return []byte("{}")
	}
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
