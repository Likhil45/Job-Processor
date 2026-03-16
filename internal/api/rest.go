package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// RESTHandler provides HTTP REST API for jobs using JobBackend.
type RESTHandler struct {
	backend       JobBackend
	submitLimiter SubmitRateLimiter
}

// NewRESTHandler returns a REST handler that uses the given JobBackend.
func NewRESTHandler(backend JobBackend) *RESTHandler {
	return &RESTHandler{backend: backend}
}

// SetSubmitRateLimiter sets an optional rate limiter for job submit. When set, 429 with Retry-After is returned when exceeded.
func (h *RESTHandler) SetSubmitRateLimiter(l SubmitRateLimiter) {
	h.submitLimiter = l
}

// ServeHTTP routes REST requests: /jobs, /jobs/archived, /schedules, /admin/queues, etc.
func (h *RESTHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) >= 1 && parts[0] == "admin" {
		h.serveAdmin(w, r, parts)
		return
	}
	if len(parts) >= 1 && parts[0] == "schedules" {
		h.serveSchedules(w, r, parts)
		return
	}
	if len(parts) < 1 || parts[0] != "jobs" {
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPost:
			h.handleSubmit(w, r)
			return
		case http.MethodGet:
			h.handleList(w, r)
			return
		}
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if parts[1] == "archived" {
		if len(parts) == 2 && r.Method == http.MethodGet {
			h.handleListArchived(w, r)
			return
		}
		if len(parts) == 3 && r.Method == http.MethodGet {
			h.handleGetArchivedJob(w, r, parts[2])
			return
		}
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	jobID := parts[1]
	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			h.handleGetStatus(w, r, jobID)
			return
		}
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(parts) == 3 {
		action := parts[2]
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch action {
		case "retry":
			h.handleRetry(w, r, jobID)
			return
		case "cancel":
			h.handleCancel(w, r, jobID)
			return
		}
	}
	writeJSONError(w, "not found", http.StatusNotFound)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v interface{}, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeBackendError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"), strings.Contains(msg, "job_id required"):
		writeJSONError(w, msg, http.StatusBadRequest)
	case strings.Contains(msg, "cannot cancel"), strings.Contains(msg, "already running"):
		writeJSONError(w, msg, http.StatusConflict)
	case strings.Contains(msg, "not supported"):
		writeJSONError(w, msg, http.StatusBadRequest)
	case strings.Contains(msg, "does not exist") || strings.Contains(msg, "42P01"):
		writeJSONError(w, "Database schema not applied. Apply the schema once (from repo root): docker exec -i $(docker compose -f deploy/demo/docker-compose.yml ps -q postgres) psql -U jobs jobs < deploy/postgres/schema.sql", http.StatusServiceUnavailable)
	default:
		writeJSONError(w, msg, http.StatusInternalServerError)
	}
}

type submitRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Options *struct {
		MaxRetry       int32  `json:"max_retry"`
		Queue          string `json:"queue"`
		RunAtUnixSec   int64  `json:"run_at_unix_sec"`
		IdempotencyKey string `json:"idempotency_key"`
		Priority       int32  `json:"priority"`
	} `json:"options"`
}

func (h *RESTHandler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tracer := otel.Tracer("job-api")
	ctx, span := tracer.Start(ctx, "job.submit")
	defer span.End()

	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Type = strings.TrimSpace(req.Type)
	if req.Type == "" {
		writeJSONError(w, "type required", http.StatusBadRequest)
		return
	}
	span.SetAttributes(attribute.String("job.type", req.Type))
	payload := []byte("{}")
	if len(req.Payload) > 0 {
		payload = req.Payload
	}
	queue := "default"
	maxRetry := int32(0)
	runAt := int64(0)
	idempotencyKey := ""
	priority := int32(0)
	if req.Options != nil {
		if req.Options.Queue != "" {
			queue = strings.TrimSpace(req.Options.Queue)
		}
		maxRetry = req.Options.MaxRetry
		runAt = req.Options.RunAtUnixSec
		idempotencyKey = strings.TrimSpace(req.Options.IdempotencyKey)
		priority = req.Options.Priority
	}
	span.SetAttributes(attribute.String("job.queue", queue))
	if h.submitLimiter != nil {
		allowed, retryAfterSec := h.submitLimiter.Allow(queue)
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
			writeJSONError(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}
	jobID, err := h.backend.Submit(ctx, req.Type, payload, queue, maxRetry, runAt, idempotencyKey, priority)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	span.SetAttributes(attribute.String("job.id", jobID))
	writeJSON(w, map[string]string{"job_id": jobID}, http.StatusCreated)
}

func (h *RESTHandler) handleList(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	statusFilter := r.URL.Query().Get("status")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	jobs, err := h.backend.ListJobs(r.Context(), queue, statusFilter, limit, offset)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	out := make([]map[string]interface{}, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, map[string]interface{}{
			"job_id":                j.JobID,
			"type":                  j.Type,
			"queue":                 j.Queue,
			"status":                j.Status,
			"attempt":               j.Attempt,
			"last_error":            j.LastError,
			"created_at_unix_sec":   j.CreatedAtUnixSec,
			"updated_at_unix_sec":   j.UpdatedAtUnixSec,
			"completed_at_unix_sec": j.CompletedAtUnixSec,
		})
	}
	writeJSON(w, map[string]interface{}{"jobs": out}, http.StatusOK)
}

func (h *RESTHandler) handleGetStatus(w http.ResponseWriter, r *http.Request, jobID string) {
	status, lastError, attempt, err := h.backend.GetStatus(r.Context(), jobID)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, map[string]interface{}{
		"job_id":     jobID,
		"status":     status,
		"attempt":    attempt,
		"last_error": lastError,
	}, http.StatusOK)
}

type retryRequest struct {
	Queue string `json:"queue"`
}

func (h *RESTHandler) handleListArchived(w http.ResponseWriter, r *http.Request) {
	queue := strings.TrimSpace(r.URL.Query().Get("queue"))
	jobType := strings.TrimSpace(r.URL.Query().Get("type"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	jobs, err := h.backend.ListArchivedJobs(r.Context(), queue, jobType, limit, offset)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	out := make([]map[string]interface{}, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, map[string]interface{}{
			"job_id":                j.JobID,
			"type":                  j.Type,
			"queue":                 j.Queue,
			"last_error":            j.LastError,
			"attempt":               j.Attempt,
			"created_at_unix_sec":   j.CreatedAtUnixSec,
		})
	}
	writeJSON(w, map[string]interface{}{"jobs": out}, http.StatusOK)
}

func (h *RESTHandler) handleGetArchivedJob(w http.ResponseWriter, r *http.Request, jobID string) {
	job, err := h.backend.GetArchivedJob(r.Context(), jobID)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if job == nil {
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"job_id":                job.JobID,
		"type":                  job.Type,
		"queue":                 job.Queue,
		"payload":               job.Payload,
		"last_error":            job.LastError,
		"attempt":               job.Attempt,
		"created_at_unix_sec":   job.CreatedAtUnixSec,
	}, http.StatusOK)
}

func (h *RESTHandler) handleRetry(w http.ResponseWriter, r *http.Request, jobID string) {
	var req retryRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	queue := strings.TrimSpace(req.Queue)
	if queue == "" {
		queue = "default"
	}
	if err := h.backend.RetryArchivedJob(r.Context(), jobID, queue); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
}

type cancelRequest struct {
	Queue string `json:"queue"`
}

func (h *RESTHandler) handleCancel(w http.ResponseWriter, r *http.Request, jobID string) {
	var req cancelRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := h.backend.CancelJob(r.Context(), jobID); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
}

func (h *RESTHandler) serveAdmin(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) >= 2 && parts[1] == "workers" {
		if len(parts) >= 3 && parts[2] == "drain" && r.Method == http.MethodPost {
			writeJSON(w, map[string]string{
				"ok":      "true",
				"message": "Drain is per-worker: POST to each worker's /admin/drain (e.g. http://worker:9090/admin/drain) to stop fetching and exit after current work.",
			}, http.StatusOK)
			return
		}
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	if len(parts) < 2 || parts[1] != "queues" {
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			queues, err := h.backend.ListQueues(r.Context())
			if err != nil {
				writeBackendError(w, err)
				return
			}
			out := make([]map[string]interface{}, 0, len(queues))
			for _, q := range queues {
				out = append(out, map[string]interface{}{
					"name":      q.Name,
					"pending":   q.Pending,
					"active":    q.Active,
					"scheduled": q.Scheduled,
					"retry":     q.Retry,
					"archived":  q.Archived,
					"paused":    q.Paused,
				})
			}
			writeJSON(w, map[string]interface{}{"queues": out}, http.StatusOK)
			return
		}
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	queueName := parts[2]
	if len(parts) == 3 {
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	action := parts[3]
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch action {
	case "pause":
		if err := h.backend.PauseQueue(r.Context(), queueName); err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
	case "unpause":
		if err := h.backend.UnpauseQueue(r.Context(), queueName); err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
	default:
		writeJSONError(w, "not found", http.StatusNotFound)
	}
}

func (h *RESTHandler) serveSchedules(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) == 1 {
		if r.Method == http.MethodGet {
			schedules, err := h.backend.ListSchedules(r.Context())
			if err != nil {
				writeBackendError(w, err)
				return
			}
			out := make([]map[string]interface{}, 0, len(schedules))
			for _, s := range schedules {
				out = append(out, map[string]interface{}{
					"id":          s.ID,
					"name":        s.Name,
					"type":        s.Type,
					"cron_expr":   s.CronExpr,
					"queue":       s.Queue,
					"max_retry":   s.MaxRetry,
					"next_run_at": s.NextRunAt.Unix(),
					"created_at":  s.CreatedAt.Unix(),
				})
			}
			writeJSON(w, map[string]interface{}{"schedules": out}, http.StatusOK)
			return
		}
		if r.Method == http.MethodPost {
			var req struct {
				Name     string          `json:"name"`
				Type     string          `json:"type"`
				Payload  json.RawMessage `json:"payload"`
				CronExpr string          `json:"cron_expr"`
				Queue    string          `json:"queue"`
				MaxRetry int32           `json:"max_retry"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSONError(w, "invalid json", http.StatusBadRequest)
				return
			}
			req.Name = strings.TrimSpace(req.Name)
			req.Type = strings.TrimSpace(req.Type)
			req.CronExpr = strings.TrimSpace(req.CronExpr)
			if req.Name == "" || req.Type == "" || req.CronExpr == "" {
				writeJSONError(w, "name, type, and cron_expr required", http.StatusBadRequest)
				return
			}
			payload := []byte("{}")
			if len(req.Payload) > 0 {
				payload = req.Payload
			}
			queue := strings.TrimSpace(req.Queue)
			if queue == "" {
				queue = "default"
			}
			id, err := h.backend.CreateSchedule(r.Context(), req.Name, req.Type, payload, req.CronExpr, queue, req.MaxRetry)
			if err != nil {
				writeBackendError(w, err)
				return
			}
			writeJSON(w, map[string]interface{}{"id": id}, http.StatusCreated)
			return
		}
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodDelete {
		id, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || id <= 0 {
			writeJSONError(w, "invalid schedule id", http.StatusBadRequest)
			return
		}
		if err := h.backend.DeleteSchedule(r.Context(), id); err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
		return
	}
	writeJSONError(w, "not found", http.StatusNotFound)
}
