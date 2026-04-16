package workerpool_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mibrgmv/go-platform/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_ExecutesTasks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := workerpool.New(ctx, 3)

	var count atomic.Int32
	results := make([]<-chan error, 10)

	for i := 0; i < 10; i++ {
		results[i] = pool.Submit(func() error {
			count.Add(1)
			return nil
		})
	}

	for _, ch := range results {
		require.NotNil(t, ch)
		err := <-ch
		assert.NoError(t, err)
	}

	pool.Shutdown()
	assert.Equal(t, int32(10), count.Load())
}

func TestPool_LimitsConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := workerpool.New(ctx, 2)

	var running atomic.Int32
	var maxRunning atomic.Int32

	results := make([]<-chan error, 10)
	for i := 0; i < 10; i++ {
		results[i] = pool.Submit(func() error {
			cur := running.Add(1)
			for {
				old := maxRunning.Load()
				if cur <= old || maxRunning.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			running.Add(-1)
			return nil
		})
	}

	for _, ch := range results {
		<-ch
	}
	pool.Shutdown()

	assert.LessOrEqual(t, maxRunning.Load(), int32(2))
}

func TestPool_RejectsOnCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	pool := workerpool.New(ctx, 1)

	pool.Submit(func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	cancel()
	time.Sleep(10 * time.Millisecond)

	ch := pool.Submit(func() error {
		return nil
	})
	assert.Nil(t, ch, "should return nil on cancelled context")

	pool.Shutdown()
}

func TestPool_ReturnsErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := workerpool.New(ctx, 2)

	ch := pool.Submit(func() error {
		return assert.AnError
	})

	err := <-ch
	assert.ErrorIs(t, err, assert.AnError)

	pool.Shutdown()
}
