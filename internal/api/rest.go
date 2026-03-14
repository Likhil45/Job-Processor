package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	jobv1 "github.com/savvients/sip-core/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RESTHandler provides HTTP REST API for jobs, delegating to JobServer.
type RESTHandler struct {
	server jobv1.JobServiceServer
}

// NewRESTHandler returns a REST handler that delegates to the gRPC JobServer.
func NewRESTHandler(server jobv1.JobServiceServer) *RESTHandler {
	return &RESTHandler{server: server}
}

// ServeHTTP routes REST requests: /jobs, /jobs/:id, /jobs/:id/retry|cancel, /admin/queues, /admin/queues/:name/pause|unpause.
func (h *RESTHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) >= 1 && parts[0] == "admin" {
		h.serveAdmin(w, r, parts)
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

type submitRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Options *struct {
		MaxRetry      int32  `json:"max_retry"`
		Queue         string `json:"queue"`
		RunAtUnixSec  int64  `json:"run_at_unix_sec"`
	} `json:"options"`
}

func (h *RESTHandler) handleSubmit(w http.ResponseWriter, r *http.Request) {
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
	payload := []byte("{}")
	if len(req.Payload) > 0 {
		payload = req.Payload
	}
	submitReq := &jobv1.SubmitJobRequest{Type: req.Type, Payload: payload}
	if req.Options != nil {
		submitReq.Options = &jobv1.JobOptions{
			MaxRetry:     req.Options.MaxRetry,
			Queue:        req.Options.Queue,
			RunAtUnixSec: req.Options.RunAtUnixSec,
		}
	}
	resp, err := h.server.SubmitJob(r.Context(), submitReq)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, map[string]string{"job_id": resp.JobId}, http.StatusCreated)
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
	resp, err := h.server.ListJobs(r.Context(), &jobv1.ListJobsRequest{
		Queue:  queue,
		Status: statusFilter,
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	jobs := make([]map[string]interface{}, 0, len(resp.Jobs))
	for _, j := range resp.Jobs {
		jobs = append(jobs, map[string]interface{}{
			"job_id":               j.JobId,
			"type":                 j.Type,
			"queue":                j.Queue,
			"status":               j.Status,
			"attempt":              j.Attempt,
			"last_error":           j.LastError,
			"created_at_unix_sec":  j.CreatedAtUnixSec,
			"updated_at_unix_sec":  j.UpdatedAtUnixSec,
			"completed_at_unix_sec": j.CompletedAtUnixSec,
		})
	}
	writeJSON(w, map[string]interface{}{"jobs": jobs}, http.StatusOK)
}

func (h *RESTHandler) handleGetStatus(w http.ResponseWriter, r *http.Request, jobID string) {
	resp, err := h.server.GetJobStatus(r.Context(), &jobv1.GetJobStatusRequest{JobId: jobID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, map[string]interface{}{
		"job_id":     resp.JobId,
		"status":     resp.Status,
		"attempt":    resp.Attempt,
		"last_error": resp.LastError,
	}, http.StatusOK)
}

type retryRequest struct {
	Queue string `json:"queue"`
}

func (h *RESTHandler) handleRetry(w http.ResponseWriter, r *http.Request, jobID string) {
	var req retryRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	queue := strings.TrimSpace(req.Queue)
	if queue == "" {
		queue = "default"
	}
	_, err := h.server.RetryArchivedJob(r.Context(), &jobv1.RetryArchivedJobRequest{JobId: jobID, Queue: queue})
	if err != nil {
		writeGRPCError(w, err)
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
	_, err := h.server.CancelJob(r.Context(), &jobv1.CancelJobRequest{JobId: jobID, Queue: req.Queue})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
}

func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound:
		writeJSONError(w, st.Message(), http.StatusBadRequest)
	case codes.FailedPrecondition:
		writeJSONError(w, st.Message(), http.StatusConflict) // 409 for "cannot cancel"
	default:
		writeJSONError(w, st.Message(), http.StatusInternalServerError)
	}
}

func (h *RESTHandler) serveAdmin(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 2 || parts[1] != "queues" {
		writeJSONError(w, "not found", http.StatusNotFound)
		return
	}
	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			resp, err := h.server.ListQueues(r.Context(), &jobv1.ListQueuesRequest{})
			if err != nil {
				writeGRPCError(w, err)
				return
			}
			queues := make([]map[string]interface{}, 0, len(resp.Queues))
			for _, q := range resp.Queues {
				queues = append(queues, map[string]interface{}{
					"name":      q.Name,
					"pending":   q.Pending,
					"active":   q.Active,
					"scheduled": q.Scheduled,
					"retry":    q.Retry,
					"archived": q.Archived,
					"paused":   q.Paused,
				})
			}
			writeJSON(w, map[string]interface{}{"queues": queues}, http.StatusOK)
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
		_, err := h.server.PauseQueue(r.Context(), &jobv1.PauseQueueRequest{Queue: queueName})
		if err != nil {
			writeGRPCError(w, err)
			return
		}
		writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
	case "unpause":
		_, err := h.server.UnpauseQueue(r.Context(), &jobv1.UnpauseQueueRequest{Queue: queueName})
		if err != nil {
			writeGRPCError(w, err)
			return
		}
		writeJSON(w, map[string]string{"ok": "true"}, http.StatusOK)
	default:
		writeJSONError(w, "not found", http.StatusNotFound)
	}
}
