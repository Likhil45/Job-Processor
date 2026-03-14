// Enqueue enqueues one job via the Job API REST endpoint. Usage: go run ./cmd/enqueue [task_type] [payload]
// Example: go run ./cmd/enqueue hello "world"
// Requires JOB_API_URL (e.g. http://localhost:8080).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	taskType := "hello"
	payload := []byte("world")
	if len(os.Args) >= 2 {
		taskType = os.Args[1]
	}
	if len(os.Args) >= 3 {
		payload = []byte(os.Args[2])
	}

	baseURL := getEnv("JOB_API_URL", "http://localhost:8080")
	body := map[string]interface{}{
		"type":    taskType,
		"payload": string(payload),
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/jobs", bytes.NewReader(raw))
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		log.Fatalf("enqueue: %s %s", resp.Status, string(b))
	}
	var out struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		log.Fatalf("response: %v", err)
	}
	fmt.Printf("enqueued job id=%s type=%s payload=%s\n", out.JobID, taskType, string(payload))
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
