package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// SubmitFunc enqueues a job and returns its ID or an error.
type SubmitFunc func(ctx context.Context, jobType string, payload []byte, maxRetry int32) (jobID string, err error)

// Run runs the periodic scheduler until ctx is cancelled.
// On each tick (every interval), it generates one fake job per type and calls submit.
func Run(ctx context.Context, interval time.Duration, submit SubmitFunc) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("scheduler stopping", "reason", ctx.Err())
			return
		case t := <-ticker.C:
			runRound(ctx, t, submit)
		}
	}
}

func runRound(ctx context.Context, at time.Time, submit SubmitFunc) {
	for _, jobType := range JobTypes {
		payload, maxRetry, err := GenerateFake(jobType)
		if err != nil {
			slog.Warn("generate fake failed", "type", jobType, "err", err)
			continue
		}
		jobID, err := submit(ctx, jobType, payload, maxRetry)
		if err != nil {
			slog.Warn("submit failed", "type", jobType, "err", err)
			continue
		}
		slog.Info("job created", "job_id", jobID, "type", jobType, "at", at)
	}
}
