package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	jobv1 "github.com/savvients/sip-core/api/proto"
	"github.com/savvients/sip-core/internal/events"
	"github.com/savvients/sip-core/internal/metrics"
	"github.com/savvients/sip-core/internal/queue"
	"github.com/savvients/sip-core/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JobServer implements jobv1.JobServiceServer.
type JobServer struct {
	jobv1.UnimplementedJobServiceServer
	client   *queue.Client
	insp     *asynq.Inspector
	queues   []string // queues to check for GetJobStatus (e.g. high, default, low)
	store    store.Store    // optional: job metadata persistence
	producer events.Producer // optional: job lifecycle events to Kafka
}

// NewJobServer returns a new JobServer. client and inspector must use the same Redis.
// queuesToCheck is the list of queue names to try when looking up job status (e.g. []string{"high", "default", "low"}).
// store is optional; when set, job metadata is persisted and ListJobs/GetJobStatus can use it.
// producer is optional; when set, job lifecycle events (submitted, cancelled) are published.
func NewJobServer(client *queue.Client, redisOpts queue.RedisOpts, queuesToCheck []string, store store.Store, producer events.Producer) (*JobServer, error) {
	insp := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     redisOpts.Addr,
		Password: redisOpts.Password,
		DB:       redisOpts.DB,
	})
	if len(queuesToCheck) == 0 {
		queuesToCheck = []string{"default"}
	}
	return &JobServer{
		client:   client,
		insp:     insp,
		queues:   queuesToCheck,
		store:    store,
		producer: producer,
	}, nil
}

// SubmitJob enqueues a job and returns its ID.
func (s *JobServer) SubmitJob(ctx context.Context, req *jobv1.SubmitJobRequest) (*jobv1.SubmitJobResponse, error) {
	if req == nil || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type required")
	}
	opts := []asynq.Option{}
	if req.Options != nil {
		if req.Options.MaxRetry > 0 {
			opts = append(opts, asynq.MaxRetry(int(req.Options.MaxRetry)))
		}
		if req.Options.Queue != "" {
			opts = append(opts, asynq.Queue(req.Options.Queue))
		}
		if req.Options.RunAtUnixSec > 0 {
			opts = append(opts, asynq.ProcessAt(time.Unix(req.Options.RunAtUnixSec, 0)))
		}
	}
	queueName := "default"
	if req.Options != nil && req.Options.Queue != "" {
		queueName = req.Options.Queue
	}
	id, err := s.client.Enqueue(ctx, req.Type, req.Payload, opts...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}
	metrics.JobsEnqueuedTotal.WithLabelValues(req.Type, queueName).Inc()
	if s.store != nil {
		statusStr := "pending"
		if req.Options != nil && req.Options.RunAtUnixSec > 0 {
			statusStr = "scheduled"
		}
		_ = s.store.Create(ctx, &store.JobRecord{
			ID:          id,
			Type:        req.Type,
			Payload:     req.Payload,
			Queue:       queueName,
			Status:      statusStr,
			AsynqTaskID: id,
		})
	}
	if s.producer != nil {
		s.producer.Emit(ctx, events.JobEvent{
			JobID:   id,
			Type:    req.Type,
			Event:   events.EventSubmitted,
			Queue:   queueName,
			Payload: req.Payload,
		})
	}
	return &jobv1.SubmitJobResponse{JobId: id}, nil
}

// GetJobStatus returns status for a job by ID. Tries each configured queue in order.
func (s *JobServer) GetJobStatus(ctx context.Context, req *jobv1.GetJobStatusRequest) (*jobv1.GetJobStatusResponse, error) {
	if req == nil || req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}
	var info *asynq.TaskInfo
	var err error
	for _, q := range s.queues {
		info, err = s.insp.GetTaskInfo(q, req.JobId)
		if err == nil {
			break
		}
		// Queue may not exist yet if no task was ever enqueued to it; treat like task not found.
		if !errors.Is(err, asynq.ErrTaskNotFound) && !errors.Is(err, asynq.ErrQueueNotFound) {
			return nil, status.Errorf(codes.Internal, "inspector: %v", err)
		}
	}
	if info == nil && s.store != nil {
		rec, err := s.store.GetByID(ctx, req.JobId)
		if err == nil && rec != nil {
			return &jobv1.GetJobStatusResponse{
				JobId:     req.JobId,
				Status:    rec.Status,
				Attempt:   rec.Attempt,
				LastError: rec.LastError,
			}, nil
		}
	}
	if info == nil {
		return &jobv1.GetJobStatusResponse{
			JobId:   req.JobId,
			Status:  "unknown",
			Attempt: 0,
		}, nil
	}
	st := taskStateToStatus(info.State)
	resp := &jobv1.GetJobStatusResponse{
		JobId:   req.JobId,
		Status:  st,
		Attempt: int32(info.Retried),
	}
	if info.LastErr != "" {
		resp.LastError = info.LastErr
	}
	return resp, nil
}

// ListArchivedJobs returns archived (dead-letter) tasks from the given queue or all queues.
func (s *JobServer) ListArchivedJobs(ctx context.Context, req *jobv1.ListArchivedJobsRequest) (*jobv1.ListArchivedJobsResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	queues := s.queues
	if req.GetQueue() != "" {
		queues = []string{req.GetQueue()}
	}
	var out []*jobv1.ArchivedJobInfo
	for _, q := range queues {
		tasks, err := s.insp.ListArchivedTasks(q, asynq.PageSize(limit))
		if err != nil {
			continue
		}
		for _, t := range tasks {
			out = append(out, &jobv1.ArchivedJobInfo{
				JobId:     t.ID,
				Queue:     t.Queue,
				Type:      t.Type,
				Payload:   t.Payload,
				Attempt:   int32(t.Retried),
				LastError: t.LastErr,
			})
			if len(out) >= limit {
				break
			}
		}
		if len(out) >= limit {
			break
		}
	}
	return &jobv1.ListArchivedJobsResponse{Jobs: out}, nil
}

// RetryArchivedJob re-queues an archived task for processing.
func (s *JobServer) RetryArchivedJob(ctx context.Context, req *jobv1.RetryArchivedJobRequest) (*jobv1.RetryArchivedJobResponse, error) {
	if req == nil || req.JobId == "" || req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and queue required")
	}
	if err := s.insp.RunTask(req.Queue, req.JobId); err != nil {
		return nil, status.Errorf(codes.Internal, "retry: %v", err)
	}
	return &jobv1.RetryArchivedJobResponse{}, nil
}

// ListJobs returns jobs from the store (optional). When store is nil, returns empty list.
func (s *JobServer) ListJobs(ctx context.Context, req *jobv1.ListJobsRequest) (*jobv1.ListJobsResponse, error) {
	if s.store == nil {
		return &jobv1.ListJobsResponse{Jobs: nil}, nil
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}
	recs, err := s.store.List(ctx, req.GetQueue(), req.GetStatus(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list jobs: %v", err)
	}
	out := make([]*jobv1.JobInfo, 0, len(recs))
	for _, r := range recs {
		var completedAt int64
		if r.CompletedAt != nil {
			completedAt = r.CompletedAt.Unix()
		}
		out = append(out, &jobv1.JobInfo{
			JobId:             r.ID,
			Type:              r.Type,
			Queue:             r.Queue,
			Status:            r.Status,
			Attempt:           r.Attempt,
			LastError:         r.LastError,
			CreatedAtUnixSec:  r.CreatedAt.Unix(),
			UpdatedAtUnixSec:  r.UpdatedAt.Unix(),
			CompletedAtUnixSec: completedAt,
		})
	}
	return &jobv1.ListJobsResponse{Jobs: out}, nil
}

// CancelJob deletes a pending/scheduled/retry task from the queue. Returns error if task is active or not found.
func (s *JobServer) CancelJob(ctx context.Context, req *jobv1.CancelJobRequest) (*jobv1.CancelJobResponse, error) {
	if req == nil || req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}
	queues := s.queues
	if req.Queue != "" {
		queues = []string{req.Queue}
	}
	var lastErr error
	for _, q := range queues {
		err := s.insp.DeleteTask(q, req.JobId)
		if err == nil {
			if s.store != nil {
				_ = s.store.UpdateStatus(ctx, req.JobId, "cancelled", "", 0, nil)
			}
			if s.producer != nil {
				s.producer.Emit(ctx, events.JobEvent{JobID: req.JobId, Event: events.EventCancelled})
			}
			return &jobv1.CancelJobResponse{}, nil
		}
		if !errors.Is(err, asynq.ErrTaskNotFound) && !errors.Is(err, asynq.ErrQueueNotFound) {
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, status.Errorf(codes.Internal, "cancel: %v", lastErr)
	}
	return nil, status.Errorf(codes.FailedPrecondition, "task not found or already running/completed (cannot cancel)")
}

// ListQueues returns queue info (pending, active, scheduled, retry, archived, paused) for configured queues.
func (s *JobServer) ListQueues(ctx context.Context, req *jobv1.ListQueuesRequest) (*jobv1.ListQueuesResponse, error) {
	var out []*jobv1.QueueInfo
	for _, q := range s.queues {
		info, err := s.insp.GetQueueInfo(q)
		if err != nil {
			continue
		}
		out = append(out, &jobv1.QueueInfo{
			Name:     info.Queue,
			Pending:  int64(info.Pending),
			Active:   int64(info.Active),
			Scheduled: int64(info.Scheduled),
			Retry:    int64(info.Retry),
			Archived: int64(info.Archived),
			Paused:   info.Paused,
		})
	}
	return &jobv1.ListQueuesResponse{Queues: out}, nil
}

// PauseQueue pauses a queue so workers will not process tasks from it.
func (s *JobServer) PauseQueue(ctx context.Context, req *jobv1.PauseQueueRequest) (*jobv1.PauseQueueResponse, error) {
	if req == nil || req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue required")
	}
	if err := s.insp.PauseQueue(req.Queue); err != nil {
		return nil, status.Errorf(codes.Internal, "pause queue: %v", err)
	}
	return &jobv1.PauseQueueResponse{}, nil
}

// UnpauseQueue resumes a paused queue.
func (s *JobServer) UnpauseQueue(ctx context.Context, req *jobv1.UnpauseQueueRequest) (*jobv1.UnpauseQueueResponse, error) {
	if req == nil || req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue required")
	}
	if err := s.insp.UnpauseQueue(req.Queue); err != nil {
		return nil, status.Errorf(codes.Internal, "unpause queue: %v", err)
	}
	return &jobv1.UnpauseQueueResponse{}, nil
}

// Close closes the inspector. Call from shutdown.
func (s *JobServer) Close() error {
	return s.insp.Close()
}

func taskStateToStatus(s asynq.TaskState) string {
	switch s {
	case asynq.TaskStatePending:
		return "pending"
	case asynq.TaskStateScheduled:
		return "scheduled"
	case asynq.TaskStateActive:
		return "processing"
	case asynq.TaskStateCompleted:
		return "completed"
	case asynq.TaskStateArchived:
		return "archived"
	case asynq.TaskStateRetry:
		return "failed"
	default:
		return fmt.Sprintf("unknown(%s)", s)
	}
}
