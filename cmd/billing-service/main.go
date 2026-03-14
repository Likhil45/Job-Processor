// billing-service: demo upstream service that submits invoice, report, and email jobs.
// Usage: JOB_API_URL=http://localhost:8080 go run ./cmd/billing-service
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func main() {
	jobAPIURL := getEnv("JOB_API_URL", "http://localhost:8080")
	if !strings.HasSuffix(jobAPIURL, "/") {
		jobAPIURL += "/"
	}
	submitURL := jobAPIURL + "jobs"

	http.HandleFunc("/invoice", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID    string  `json:"id"`
			Total string  `json:"total"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			req.ID = "INV-001"
			req.Total = "100.00"
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"template": "Invoice #{{.ID}}\nTotal: {{.Total}}",
			"data":     map[string]string{"ID": req.ID, "Total": req.Total},
			"out_path": "/out/invoice-" + req.ID + ".txt",
		})
		body, _ := json.Marshal(map[string]interface{}{
			"type":    "invoice",
			"payload": json.RawMessage(payload),
		})
		resp, err := http.Post(submitURL, "application/json", bytes.NewReader(body))
		if err != nil {
			slog.Error("submit job", "err", err)
			http.Error(w, "job api error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			slog.Warn("job api error", "status", resp.StatusCode, "body", string(respBody))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			if len(respBody) > 0 {
				w.Write(respBody)
			} else {
				json.NewEncoder(w).Encode(map[string]string{"error": "job api error", "status": strconv.Itoa(resp.StatusCode)})
			}
			return
		}
		var result struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(respBody, &result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": result.JobID, "message": "invoice job queued"})
	})

	http.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"headers":  []string{"Item", "Amount"},
			"rows":    [][]string{{"Fee", "10"}, {"Tax", "2"}},
			"out_path": "/out/billing-report.csv",
		})
		body, _ := json.Marshal(map[string]interface{}{
			"type":    "report",
			"payload": json.RawMessage(payload),
		})
		resp, err := http.Post(submitURL, "application/json", bytes.NewReader(body))
		if err != nil {
			slog.Error("submit job", "err", err)
			http.Error(w, "job api error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			slog.Warn("job api error", "status", resp.StatusCode, "body", string(respBody))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			if len(respBody) > 0 {
				w.Write(respBody)
			} else {
				json.NewEncoder(w).Encode(map[string]string{"error": "job api error", "status": strconv.Itoa(resp.StatusCode)})
			}
			return
		}
		var result struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(respBody, &result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": result.JobID, "message": "report job queued"})
	})

	http.HandleFunc("/invoice-ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			req.Email = "billing@example.com"
		}
		email := strings.TrimSpace(req.Email)
		if email == "" {
			email = "billing@example.com"
		}
		payload, _ := json.Marshal(map[string]string{
			"to":      email,
			"subject": "Your invoice is ready",
			"body":    "Please find your invoice attached.",
		})
		body, _ := json.Marshal(map[string]interface{}{
			"type":    "email",
			"payload": json.RawMessage(payload),
		})
		resp, err := http.Post(submitURL, "application/json", bytes.NewReader(body))
		if err != nil {
			slog.Error("submit job", "err", err)
			http.Error(w, "job api error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			slog.Warn("job api error", "status", resp.StatusCode, "body", string(respBody))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			if len(respBody) > 0 {
				w.Write(respBody)
			} else {
				json.NewEncoder(w).Encode(map[string]string{"error": "job api error", "status": strconv.Itoa(resp.StatusCode)})
			}
			return
		}
		var result struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(respBody, &result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": result.JobID, "message": "invoice ready email queued"})
	})

	addr := getEnv("LISTEN_ADDR", ":8082")
	slog.Info("billing-service listening", "addr", addr, "job_api", jobAPIURL)
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
