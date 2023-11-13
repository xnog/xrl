package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimiter struct {
	client *redis.Client
	script *redis.Script
	keys   []string
	args   []interface{}
}

func NewRateLimiter(client *redis.Client, key string, bucket int, window time.Duration) *RateLimiter {
	keys := []string{key}
	args := []interface{}{window.Seconds(), bucket}

	script := redis.NewScript(`
		local key = KEYS[1]
		local window_s = tonumber(ARGV[1])
		local bucket_size = tonumber(ARGV[2])
		
		local time = redis.call('TIME')
		redis.call('ZREMRANGEBYSCORE', key, '-inf', tonumber(time[1]) - window_s)

		local size = redis.call('ZCARD', key)
		if size < bucket_size then
			redis.call('ZADD', key, time[1], time[1] .. time[2])
			redis.call('EXPIRE', key, window_s)
			return 0
		end

		return 1
	`)

	return &RateLimiter{
		client,
		script,
		keys,
		args,
	}
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		l, err := r.script.Run(ctx, r.client, r.keys, r.args...).Int()
		if err != nil {
			return err
		}

		if l == 0 {
			break
		}

		if l == 1 {
			time.Sleep(time.Second)
		}
	}

	return nil
}
