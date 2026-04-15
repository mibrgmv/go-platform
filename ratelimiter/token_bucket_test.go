package ratelimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/mibrgmv/go-platform/ratelimiter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	redistest "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupRedis(t *testing.T) (*redis.Client, func()) {
	ctx := context.Background()

	redisContainer, err := redistest.Run(ctx,
		"redis:8-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := redisContainer.ConnectionString(ctx)
	require.NoError(t, err)

	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	client := redis.NewClient(opts)

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Logf("Failed to close Redis client: %v", err)
		}
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return client, cleanup
}

func TestTokenBucket_AllowWithinCapacity(t *testing.T) {
	t.Parallel()

	client, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	limiter := ratelimiter.NewTokenBucket(ratelimiter.TokenBucketConfig{
		Client:         client,
		Capacity:       5,
		RefillRate:     1,
		RefillInterval: time.Second,
	})

	for i := 0; i < 5; i++ {
		allowed, err := limiter.Allow(ctx, "test:within")
		require.NoError(t, err)
		assert.True(t, allowed, "request %d should be allowed", i)
	}
}

func TestTokenBucket_DenyOverCapacity(t *testing.T) {
	t.Parallel()

	client, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	limiter := ratelimiter.NewTokenBucket(ratelimiter.TokenBucketConfig{
		Client:         client,
		Capacity:       3,
		RefillRate:     1,
		RefillInterval: time.Minute,
	})

	for i := 0; i < 3; i++ {
		allowed, err := limiter.Allow(ctx, "test:over")
		require.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err := limiter.Allow(ctx, "test:over")
	require.NoError(t, err)
	assert.False(t, allowed, "4th request should be denied")
}

func TestTokenBucket_RefillAfterTime(t *testing.T) {
	t.Parallel()

	client, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	limiter := ratelimiter.NewTokenBucket(ratelimiter.TokenBucketConfig{
		Client:         client,
		Capacity:       2,
		RefillRate:     2,
		RefillInterval: time.Second,
	})

	// exhaust tokens
	for i := 0; i < 2; i++ {
		allowed, err := limiter.Allow(ctx, "test:refill")
		require.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err := limiter.Allow(ctx, "test:refill")
	require.NoError(t, err)
	assert.False(t, allowed, "should be denied after exhaustion")

	// wait for refill
	time.Sleep(1100 * time.Millisecond)

	allowed, err = limiter.Allow(ctx, "test:refill")
	require.NoError(t, err)
	assert.True(t, allowed, "should be allowed after refill")
}

func TestTokenBucket_SeparateKeys(t *testing.T) {
	t.Parallel()

	client, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	limiter := ratelimiter.NewTokenBucket(ratelimiter.TokenBucketConfig{
		Client:         client,
		Capacity:       1,
		RefillRate:     1,
		RefillInterval: time.Minute,
	})

	allowed, err := limiter.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, allowed)

	// user:1 exhausted, but user:2 should still work
	allowed, err = limiter.Allow(ctx, "user:2")
	require.NoError(t, err)
	assert.True(t, allowed, "different key should have its own bucket")

	allowed, err = limiter.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, allowed, "user:1 should be denied")
}
