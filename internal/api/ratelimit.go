package api

import (
	"strconv"
	"sync"

	"golang.org/x/time/rate"
)

// SubmitRateLimiter limits job submit requests per queue and/or globally. Return 429 when exceeded.
type SubmitRateLimiter interface {
	Allow(queue string) (allowed bool, retryAfterSec int)
}

// TokenBucketLimiter implements SubmitRateLimiter with per-queue and optional global limits.
type TokenBucketLimiter struct {
	perQueue  map[string]*rate.Limiter
	global    *rate.Limiter
	perMinute int
	mu        sync.Mutex
}

// NewTokenBucketLimiter creates a limiter. perQueuePerMin and globalPerMin are requests per minute (0 = no limit).
func NewTokenBucketLimiter(perQueuePerMin, globalPerMin int) *TokenBucketLimiter {
	var global *rate.Limiter
	if globalPerMin > 0 {
		global = rate.NewLimiter(rate.Limit(globalPerMin)/60, burst(globalPerMin))
	}
	return &TokenBucketLimiter{
		perQueue:  make(map[string]*rate.Limiter),
		global:    global,
		perMinute: perQueuePerMin,
	}
}

func burst(perMin int) int {
	if perMin < 10 {
		return perMin
	}
	if perMin < 60 {
		return 10
	}
	return perMin / 6
}

func (t *TokenBucketLimiter) getQueueLimiter(queue string) *rate.Limiter {
	t.mu.Lock()
	defer t.mu.Unlock()
	if l, ok := t.perQueue[queue]; ok {
		return l
	}
	if t.perMinute <= 0 {
		return nil
	}
	l := rate.NewLimiter(rate.Limit(t.perMinute)/60, burst(t.perMinute))
	t.perQueue[queue] = l
	return l
}

// Allow returns whether the request is allowed and, if not, suggested Retry-After in seconds.
func (t *TokenBucketLimiter) Allow(queue string) (allowed bool, retryAfterSec int) {
	if queue == "" {
		queue = "default"
	}
	if t.global != nil && !t.global.Allow() {
		return false, 60
	}
	ql := t.getQueueLimiter(queue)
	if ql != nil && !ql.Allow() {
		return false, 60
	}
	return true, 0
}

// ParseRateLimitConfig reads RATE_LIMIT_PER_QUEUE and RATE_LIMIT_GLOBAL from env (via getEnv).
// getEnv(key, default) returns the value; per-queue and global are requests per minute; 0 means no limit.
func ParseRateLimitConfig(getEnv func(string, string) string) (perQueue, global int) {
	p := getEnv("RATE_LIMIT_PER_QUEUE", "0")
	g := getEnv("RATE_LIMIT_GLOBAL", "0")
	perQueue, _ = strconv.Atoi(p)
	global, _ = strconv.Atoi(g)
	return perQueue, global
}
