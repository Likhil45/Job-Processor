package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/savvients/sip-core/internal/kafkaqueue"
	"github.com/savvients/sip-core/internal/store"
)

// RunRecurring runs the recurring job loop: every tick, list schedules due, create job + enqueue, update next_run_at from cron.
func RunRecurring(ctx context.Context, st store.Store, producer *kafkaqueue.Producer, tickInterval time.Duration) {
	if tickInterval <= 0 {
		tickInterval = time.Minute
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runRecurringTick(ctx, st, producer)
		}
	}
}

func runRecurringTick(ctx context.Context, st store.Store, producer *kafkaqueue.Producer) {
	now := time.Now()
	due, err := st.ListSchedulesDue(ctx, now, 50)
	if err != nil {
		slog.Warn("list schedules due", "err", err)
		return
	}
	for _, sch := range due {
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		sched, err := parser.Parse(sch.CronExpr)
		if err != nil {
			slog.Warn("parse cron", "schedule_id", sch.ID, "expr", sch.CronExpr, "err", err)
			continue
		}
		jobID := uuid.New().String()
		if err := st.Create(ctx, &store.JobRecord{
			ID:          jobID,
			Type:        sch.Type,
			Payload:     sch.PayloadJSON,
			Queue:       sch.Queue,
			Status:      "pending",
			AsynqTaskID: jobID,
		}); err != nil {
			slog.Warn("create job for schedule", "schedule_id", sch.ID, "err", err)
			continue
		}
		if _, err := producer.Enqueue(ctx, jobID, sch.Type, sch.PayloadJSON, sch.Queue, sch.MaxRetry, 0, 0, 0); err != nil {
			slog.Warn("enqueue schedule job", "schedule_id", sch.ID, "err", err)
			continue
		}
		nextRun := sched.Next(sch.NextRunAt)
		if err := st.UpdateScheduleNextRun(ctx, sch.ID, nextRun); err != nil {
			slog.Warn("update schedule next_run", "schedule_id", sch.ID, "err", err)
			continue
		}
		slog.Info("recurring job created", "schedule", sch.Name, "job_id", jobID, "next_run", nextRun)
	}
}
