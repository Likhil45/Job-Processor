package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/jobs"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/store"
)

// ServeMux routes tasks to job handlers from a registry.
type ServeMux struct {
	registry *jobs.Registry
	store    store.Store         // optional: update job status on completion/failure
	producer events.Producer     // optional: job lifecycle events to Kafka
}

// NewServeMux returns a mux that dispatches to the given registry.
// store is optional; when set, job status is updated on completion or failure.
// producer is optional; when set, job lifecycle events (started, completed, failed) are published.
func NewServeMux(registry *jobs.Registry, store store.Store, producer events.Producer) *ServeMux {
	return &ServeMux{registry: registry, store: store, producer: producer}
}

// ProcessTask implements asynq.Handler.
func (m *ServeMux) ProcessTask(ctx context.Context, t *asynq.Task) error {
	jobID, _ := asynq.GetTaskID(ctx)
	typ := t.Type()
	if m.producer != nil {
		m.producer.Emit(ctx, events.JobEvent{JobID: jobID, Type: typ, Event: events.EventStarted})
	}
	start := time.Now()
	h := m.registry.Get(typ)
	if h == nil {
		metrics.JobsProcessedTotal.WithLabelValues(typ, "failure").Inc()
		slog.Warn("unknown task type", "job_id", jobID, "type", typ)
		if m.store != nil {
			_ = m.store.UpdateStatus(ctx, jobID, "failed", "unknown task type", 0, nil)
		}
		if m.producer != nil {
			m.producer.Emit(ctx, events.JobEvent{JobID: jobID, Type: typ, Event: events.EventFailed, LastError: "unknown task type"})
		}
		return ErrUnknownTaskType
	}
	err := h.Handle(ctx, t.Payload())
	dur := time.Since(start).Seconds()
	metrics.JobProcessingDurationSeconds.WithLabelValues(typ).Observe(dur)
	if err != nil {
		metrics.JobsProcessedTotal.WithLabelValues(typ, "failure").Inc()
		slog.Error("job failed", "job_id", jobID, "type", typ, "duration_sec", dur, "error", err)
		if m.store != nil {
			_ = m.store.UpdateStatus(ctx, jobID, "failed", err.Error(), 0, nil)
		}
		if m.producer != nil {
			m.producer.Emit(ctx, events.JobEvent{JobID: jobID, Type: typ, Event: events.EventFailed, LastError: err.Error()})
		}
		return err
	}
	metrics.JobsProcessedTotal.WithLabelValues(typ, "success").Inc()
	if m.store != nil {
		now := time.Now()
		_ = m.store.UpdateStatus(ctx, jobID, "completed", "", 0, &now)
	}
	if m.producer != nil {
		m.producer.Emit(ctx, events.JobEvent{JobID: jobID, Type: typ, Event: events.EventCompleted})
	}
	slog.Info("job completed", "job_id", jobID, "type", typ, "duration_sec", dur)
	return nil
}

// ErrUnknownTaskType is returned when no handler is registered for a task type.
var ErrUnknownTaskType = &UnknownTaskTypeError{}

type UnknownTaskTypeError struct{}

func (e *UnknownTaskTypeError) Error() string { return "unknown task type" }
