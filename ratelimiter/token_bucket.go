package ratelimiter

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const luaScript = `
	local key = KEYS[1]
	local capacity = tonumber(ARGV[1])
	local refill_rate = tonumber(ARGV[2])
	local refill_interval = tonumber(ARGV[3])
	local now = tonumber(ARGV[4])
	
	-- Get current state or initialize
	local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
	local tokens = tonumber(bucket[1])
	local last_refill = tonumber(bucket[2])
	
	-- Initialize if this is the first request
	if tokens == nil then
		tokens = capacity
		last_refill = now
	end
	
	-- Calculate token refill
	local time_passed = now - last_refill
	local refills = math.floor(time_passed / refill_interval)
	
	if refills > 0 then
		tokens = math.min(capacity, tokens + (refills * refill_rate))
		last_refill = last_refill + (refills * refill_interval)
	end
	
	-- Try to consume a token
	local allowed = 0
	if tokens >= 1 then
		tokens = tokens - 1
		allowed = 1
	end
	
	-- Update state
	redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
	
	-- Return result: allowed (1 or 0) and remaining tokens
	return {allowed, tokens}
`

type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}

type TokenBucket struct {
	client         *redis.Client
	script         *redis.Script
	capacity       int
	refillRate     int
	refillInterval time.Duration
}

type TokenBucketConfig struct {
	Client         *redis.Client
	Capacity       int
	RefillRate     int
	RefillInterval time.Duration
}

func NewTokenBucket(config TokenBucketConfig) *TokenBucket {
	return &TokenBucket{
		client:         config.Client,
		script:         redis.NewScript(luaScript),
		capacity:       config.Capacity,
		refillRate:     config.RefillRate,
		refillInterval: config.RefillInterval,
	}
}

func (t *TokenBucket) Allow(ctx context.Context, key string) (bool, error) {
	result, err := t.script.Run(ctx, t.client,
		[]string{key},                          // KEYS
		t.capacity,                             // ARGV[1]
		t.refillRate,                           // ARGV[2]
		t.refillInterval.Seconds(),             // ARGV[3]
		float64(time.Now().UnixMilli())/1000.0, // ARGV[4]
	).Int64Slice()
	if err != nil {
		return false, err
	}
	return result[0] == 1, nil
}
