// Scheduler is an optional separate process for moving delayed jobs into queues.
// With Asynq, delayed/scheduled tasks are handled inside the worker server (Redis-backed);
// no separate scheduler binary is required. This stub exists for future use if you need
// a custom scheduler with leader election (e.g. coordination.k8s.io/Lease) so only one
// replica runs the scheduling loop.
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.Info("scheduler stub: Asynq workers handle delayed tasks internally; exiting after 1s")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	select {
	case <-tick.C:
		os.Exit(0)
	case <-quit:
		os.Exit(0)
	}
}
