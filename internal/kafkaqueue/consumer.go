package kafkaqueue

import (
	"container/heap"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func priorityOf(req *JobRequest) int32 {
	if req.Options != nil {
		return req.Options.Priority
	}
	return 0
}

// priorityQueue orders by priority (higher first). Thread-safe.
type priorityQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []*pqItem
	drain bool
}

type pqItem struct {
	msg kafka.Message
	req JobRequest
}

type pqHeap []*pqItem

func (h pqHeap) Len() int           { return len(h) }
func (h pqHeap) Less(i, j int) bool { return priorityOf(&h[i].req) > priorityOf(&h[j].req) }
func (h pqHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pqHeap) Push(x interface{}) { *h = append(*h, x.(*pqItem)) }
func (h *pqHeap) Pop() interface{} {
	old := *h
	n := len(old)
	*h = old[:n-1]
	return old[n-1]
}

func newPriorityQueue() *priorityQueue {
	pq := &priorityQueue{}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

func (pq *priorityQueue) Push(msg kafka.Message, req JobRequest) {
	pq.mu.Lock()
	heap.Push((*pqHeap)(&pq.items), &pqItem{msg: msg, req: req})
	pq.cond.Signal()
	pq.mu.Unlock()
}

func (pq *priorityQueue) SetDrain() {
	pq.mu.Lock()
	pq.drain = true
	pq.cond.Signal()
	pq.mu.Unlock()
}

func (pq *priorityQueue) Pop(ctx context.Context) (kafka.Message, *JobRequest, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for len(pq.items) == 0 {
		if pq.drain {
			return kafka.Message{}, nil, false
		}
		if ctx.Err() != nil {
			return kafka.Message{}, nil, false
		}
		pq.cond.Wait()
	}
	it := heap.Pop((*pqHeap)(&pq.items)).(*pqItem)
	return it.msg, &it.req, true
}

// ProcessFunc processes a job request. Return nil on success; non-nil to indicate failure (caller may retry or DLQ).
type ProcessFunc func(ctx context.Context, req *JobRequest) error

// RunConsumer reads from the job.requests topic and calls process for each message.
// Higher-priority jobs (Options.Priority) are processed before lower-priority ones.
// If drainCheck is non-nil and returns true, the consumer stops fetching and exits after draining the in-memory queue.
// Commits offset after process returns. On process error the message is not retried here (caller may re-produce inside process).
func RunConsumer(ctx context.Context, brokers []string, topic, groupID string, process ProcessFunc, drainCheck func() bool) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	if topic == "" {
		topic = defaultJobsTopic
	}
	if groupID == "" {
		groupID = "job-worker"
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0,
	})
	defer r.Close()
	slog.Info("kafka consumer started", "topic", topic, "group", groupID)
	pq := newPriorityQueue()
	go func() {
		for {
			if drainCheck != nil && drainCheck() {
				pq.SetDrain()
				return
			}
			select {
			case <-ctx.Done():
				pq.SetDrain()
				return
			default:
			}
			m, err := r.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					pq.SetDrain()
					return
				}
				slog.Warn("fetch message", "err", err)
				continue
			}
			var req JobRequest
			if err := json.Unmarshal(m.Value, &req); err != nil {
				slog.Warn("invalid message", "err", err, "offset", m.Offset)
				_ = r.CommitMessages(ctx, m)
				continue
			}
			pq.Push(m, req)
		}
	}()
	for {
		msg, req, ok := pq.Pop(ctx)
		if !ok {
			return
		}
		go func(msg kafka.Message, req JobRequest) {
			procCtx := ctx
			if prop := otel.GetTextMapPropagator(); prop != nil {
				carrier := make(propagation.MapCarrier)
				for _, h := range msg.Headers {
					carrier[string(h.Key)] = string(h.Value)
				}
				procCtx = prop.Extract(ctx, carrier)
			}
			var span trace.Span
			tracer := otel.Tracer("job-worker")
			procCtx, span = tracer.Start(procCtx, "job.process")
			defer span.End()
			if err := process(procCtx, &req); err != nil {
				slog.Warn("process job failed", "job_id", req.JobID, "type", req.Type, "err", err)
			}
			if err := r.CommitMessages(ctx, msg); err != nil {
				slog.Warn("commit failed", "err", err)
			}
		}(msg, *req)
	}
}
