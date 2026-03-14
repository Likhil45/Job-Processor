package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

const SleepType = "sleep"

// SleepPayload is the JSON payload for sleep (long-running demo) jobs.
type SleepPayload struct {
	Seconds int `json:"seconds"` // how long to sleep (default 30)
}

// Sleep sleeps for the given duration. Used for worker crash recovery demo (Scenario E).
type Sleep struct{}

func (Sleep) Type() string { return SleepType }

func (s Sleep) Handle(ctx context.Context, payload []byte) error {
	var p SleepPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("sleep payload: %w", err)
		}
	}
	if p.Seconds <= 0 {
		p.Seconds = 30
	}
	if p.Seconds > 300 {
		p.Seconds = 300
	}
	slog.Info("sleep job started", "seconds", p.Seconds)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(p.Seconds) * time.Second):
		slog.Info("sleep job completed", "seconds", p.Seconds)
		return nil
	}
}
