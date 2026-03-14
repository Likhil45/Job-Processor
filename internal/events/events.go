package events

// JobEventType is the lifecycle event type for a job.
type JobEventType string

const (
	EventSubmitted JobEventType = "submitted"
	EventStarted   JobEventType = "started"
	EventCompleted JobEventType = "completed"
	EventFailed    JobEventType = "failed"
	EventArchived  JobEventType = "archived"
	EventCancelled JobEventType = "cancelled"
)

// JobEvent payload for Kafka (and optional Postgres job_events).
type JobEvent struct {
	JobID     string       `json:"job_id"`
	Type      string       `json:"type"`       // job type e.g. email, report
	Event     JobEventType `json:"event"`
	Queue     string       `json:"queue,omitempty"`
	Attempt   int32        `json:"attempt,omitempty"`
	LastError string       `json:"last_error,omitempty"`
	Payload   []byte       `json:"payload,omitempty"`
}
