package jobs

import "context"

// Handler processes a job. Payload is the raw job payload bytes.
// Returning an error triggers retry (if retries left); nil marks the job complete.
type Handler interface {
	Type() string
	Handle(ctx context.Context, payload []byte) error
}
