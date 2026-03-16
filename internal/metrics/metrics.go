package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsEnqueuedTotal counts jobs enqueued by type.
	JobsEnqueuedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_enqueued_total",
			Help: "Total number of jobs enqueued",
		},
		[]string{"type", "queue"},
	)
	// JobsProcessedTotal counts jobs processed by type and status.
	JobsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total number of jobs processed",
		},
		[]string{"type", "status"}, // status: success, failure
	)
	// JobProcessingDurationSeconds is the time to process a job.
	JobProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Time spent processing a job",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
	// JobsScheduledTotal counts jobs created by the scheduler (fake job generator).
	JobsScheduledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_scheduled_total",
			Help: "Total number of jobs scheduled by the fake job scheduler",
		},
		[]string{"type"},
	)
	// JobsArchivedTotal counts jobs moved to DLQ (archived) by type and queue.
	JobsArchivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_archived_total",
			Help: "Total number of jobs archived (DLQ)",
		},
		[]string{"type", "queue"},
	)
	// JobsRetriedTotal counts job retries (re-enqueue after failure) by type and queue.
	JobsRetriedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Total number of job retries (re-enqueued after failure)",
		},
		[]string{"type", "queue"},
	)
	// JobsSLABreachedTotal counts jobs that exceeded their SLA (completed_at - created_at > sla_sec).
	JobsSLABreachedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_sla_breached_total",
			Help: "Total number of jobs that exceeded SLA duration",
		},
		[]string{"type", "queue"},
	)
)
