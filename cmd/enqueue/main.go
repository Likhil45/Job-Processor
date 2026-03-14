// Enqueue enqueues one job for testing (Phase 1). Usage: go run ./cmd/enqueue [task_type] [payload]
// Example: go run ./cmd/enqueue hello "world"
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/savvients/sip-core/internal/queue"
)

func main() {
	taskType := "hello"
	payload := []byte("world")
	if len(os.Args) >= 2 {
		taskType = os.Args[1]
	}
	if len(os.Args) >= 3 {
		payload = []byte(os.Args[2])
	}

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))

	client, err := queue.NewClient(queue.RedisOpts{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err != nil {
		log.Fatalf("queue client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	id, err := client.Enqueue(ctx, taskType, payload)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	fmt.Printf("enqueued job id=%s type=%s payload=%s\n", id, taskType, string(payload))
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
