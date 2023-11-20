package xrl

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type TokenBucketRateLimiter struct {
	client *redis.Client
	script *redis.Script
	keys   []string
	args   []interface{}
}

// Capacity: This represents the maximum number of tokens the bucket can hold. It should be set to at least the maximum burst rate you want to allow.
// Rate: This is the rate at which tokens are added to the bucket, representing the number of tokens added per second.
//
// For 1333 requests per second without burst
// Capacity: 1333
// Rate: 1333
//
// For 1333 requests per second with maximum burst of 1500 requests per second
// Capacity: 1500
// Rate: 1333
func NewTokenBucketRateLimiter(client *redis.Client, key string, capacity int, rate float64) *TokenBucketRateLimiter {
	keys := []string{key}
	args := []interface{}{capacity, rate}

	script := redis.NewScript(`
		local key = KEYS[1]
		local capacity = tonumber(ARGV[1])
		local rate = tonumber(ARGV[2])

		local values = redis.call("mget", key, key .. ":timestamp")
		local tokens = tonumber(values[1] or 0)
		local timestamp = tonumber(values[2] or 0)

		local now = redis.call("time")[1]
		local elapsed = now - timestamp

		tokens = math.min(capacity, tokens + elapsed * rate)

		redis.call("mset", key, tokens, key .. ":timestamp", now)

		if tokens >= 1 then
			redis.call("incrbyfloat", key, -1)
			return 0
		else
			return 1
		end
	`)

	return &TokenBucketRateLimiter{
		client,
		script,
		keys,
		args,
	}
}

func (r *TokenBucketRateLimiter) Wait(ctx context.Context) error {
	for {
		l, err := r.script.Run(ctx, r.client, r.keys, r.args...).Int()
		if err != nil {
			return err
		}

		if l == 0 {
			break
		}

		time.Sleep(time.Second)
	}

	return nil
}
