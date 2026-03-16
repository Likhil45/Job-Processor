package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/store"
)

// RunPromoter promotes due scheduled jobs to the queue until ctx is cancelled.
// It lists jobs with status=scheduled and run_at_unix_sec <= now, produces each to Kafka, and sets status to pending.
func RunPromoter(ctx context.Context, st store.Store, producer *kafkaqueue.Producer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			promoteDue(ctx, st, producer)
		}
	}
}

func promoteDue(ctx context.Context, st store.Store, producer *kafkaqueue.Producer) {
	now := time.Now().Unix()
	due, err := st.ListScheduledDue(ctx, now, 50)
	if err != nil {
		slog.Warn("list scheduled due", "err", err)
		return
	}
	for _, j := range due {
		_, err := producer.Enqueue(ctx, j.ID, j.Type, j.Payload, j.Queue, 2, 0, 0, j.Priority)
		if err != nil {
			slog.Warn("promote enqueue failed", "job_id", j.ID, "err", err)
			continue
		}
		if err := st.UpdateStatus(ctx, j.ID, "pending", "", j.Attempt, nil); err != nil {
			slog.Warn("promote update status failed", "job_id", j.ID, "err", err)
			continue
		}
		slog.Info("scheduled job promoted", "job_id", j.ID, "type", j.Type)
	}
}
