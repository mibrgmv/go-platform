package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCalculateBackoff_ZeroAttempt(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Minute, 0)

	backoff := c.calculateBackoff(0)

	assert.Equal(t, time.Duration(0), backoff)
}

func TestCalculateBackoff_ExponentialGrowth(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Hour, 0)

	assert.Equal(t, time.Second, c.calculateBackoff(1))
	assert.Equal(t, 2*time.Second, c.calculateBackoff(2))
	assert.Equal(t, 4*time.Second, c.calculateBackoff(3))
	assert.Equal(t, 8*time.Second, c.calculateBackoff(4))
}

func TestCalculateBackoff_CappedAtMax(t *testing.T) {
	c := newRetryCalculator(time.Second, 5*time.Second, 0)

	backoff := c.calculateBackoff(10)

	assert.Equal(t, 5*time.Second, backoff)
}

func TestCalculateNextRetry_NegativeAttemptNormalized(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Minute, 0)

	before := time.Now()
	next := c.calculateNextRetry(-5)

	// Negative attempt is normalized to 0, backoff(0) = 0, so next ≈ now
	assert.WithinDuration(t, before, next, 100*time.Millisecond)
}

func TestCalculateBackoff_WithJitter(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Minute, 0.5)

	for i := 0; i < 100; i++ {
		backoff := c.calculateBackoff(3)

		assert.GreaterOrEqual(t, backoff, time.Second, "backoff should not go below baseDelay")
		assert.LessOrEqual(t, backoff, time.Minute, "backoff should not exceed maxDelay")
	}
}

func TestCalculateBackoff_InvalidJitterDefaultsTo02(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Minute, 1.5)

	assert.Equal(t, 0.2, c.jitterFactor)
}

func TestCalculateNextRetry_ReturnsTimeInFuture(t *testing.T) {
	c := newRetryCalculator(time.Second, time.Minute, 0)

	before := time.Now()
	next := c.calculateNextRetry(1)

	assert.True(t, next.After(before))
	assert.True(t, next.Before(before.Add(2*time.Second)))
}
