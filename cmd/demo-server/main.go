// Demo server: serves a simple dashboard and proxies job submit/status to the gRPC Job API.
// Usage: JOB_API_ADDR=localhost:50051 go run ./cmd/demo-server
package main

import (
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"

	jobv1 "github.com/savvients/sip-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed static/index.html
var indexHTML []byte

var (
	jobClient jobv1.JobServiceClient
	conn      *grpc.ClientConn
)

func main() {
	apiAddr := getEnv("JOB_API_ADDR", "localhost:50051")
	var err error
	conn, err = grpc.NewClient(apiAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("grpc dial", "addr", apiAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	jobClient = jobv1.NewJobServiceClient(conn)

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

// writeJSONError sends a JSON body {"error": msg} so the frontend can parse it.
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
		"mailhog":   "http://localhost:8025",
		"report":    "See ./out/demo-report.csv after running the demo",
		"grpc_port": "50051",
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
	resp, err := jobClient.SubmitJob(r.Context(), &jobv1.SubmitJobRequest{
		Type:    req.Type,
		Payload: payload,
	})
	if err != nil {
		slog.Warn("submit job", "type", req.Type, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"job_id": resp.JobId})
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
	resp, err := jobClient.GetJobStatus(r.Context(), &jobv1.GetJobStatusRequest{JobId: jobID})
	if err != nil {
		slog.Warn("get status", "job_id", jobID, "err", err)
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":     resp.JobId,
		"status":     resp.Status,
		"attempt":    resp.Attempt,
		"last_error": resp.LastError,
	})
}
