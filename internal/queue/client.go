package queue

import (
	"context"

	"github.com/hibiken/asynq"
)

// Client wraps asynq.Client for enqueueing jobs.
type Client struct {
	asynq *asynq.Client
}

// RedisOpts holds Redis connection options.
type RedisOpts struct {
	Addr     string
	Password string
	DB       int
}

// NewClient creates a queue client. Call Close when done.
func NewClient(opts RedisOpts) (*Client, error) {
	asynqClient := asynq.NewClient(asynq.RedisClientOpt{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})
	return &Client{asynq: asynqClient}, nil
}

// Enqueue enqueues a job with the given type and payload. Pass asynq options for queue, delay, retry, etc.
func (c *Client) Enqueue(ctx context.Context, taskType string, payload []byte, opts ...asynq.Option) (id string, err error) {
	task := asynq.NewTask(taskType, payload)
	info, err := c.asynq.EnqueueContext(ctx, task, opts...)
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// Close closes the client.
func (c *Client) Close() error {
	return c.asynq.Close()
}
