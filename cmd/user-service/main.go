// user-service: demo upstream service that submits email jobs (welcome, password reset).
// Usage: JOB_API_URL=http://localhost:8080 go run ./cmd/user-service
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

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		email := strings.TrimSpace(req.Email)
		if email == "" {
			http.Error(w, "email required", http.StatusBadRequest)
			return
		}
		payload, _ := json.Marshal(map[string]string{
			"to":      email,
			"subject": "Welcome",
			"body":    "Thanks for registering!",
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
				json.NewEncoder(w).Encode(map[string]string{"error": "job api error (HTTP " + strconv.Itoa(resp.StatusCode) + ")", "status": strconv.Itoa(resp.StatusCode)})
			}
			return
		}
		var result struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(respBody, &result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": result.JobID, "message": "welcome email queued"})
	})

	http.HandleFunc("/password-reset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		email := strings.TrimSpace(req.Email)
		if email == "" {
			http.Error(w, "email required", http.StatusBadRequest)
			return
		}
		payload, _ := json.Marshal(map[string]string{
			"to":      email,
			"subject": "Password reset",
			"body":    "Click here to reset your password.",
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
				json.NewEncoder(w).Encode(map[string]string{"error": "job api error (HTTP " + strconv.Itoa(resp.StatusCode) + ")", "status": strconv.Itoa(resp.StatusCode)})
			}
			return
		}
		var result struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(respBody, &result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": result.JobID, "message": "password reset email queued"})
	})

	addr := getEnv("LISTEN_ADDR", ":8081")
	slog.Info("user-service listening", "addr", addr, "job_api", jobAPIURL)
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
