package events

import (
	"math"
	"math/rand"
	"time"
)

type retryCalculator struct {
	baseDelay    time.Duration
	maxDelay     time.Duration
	jitterFactor float64
}

func newRetryCalculator(baseDelay, maxDelay time.Duration, jitterFactor float64) *retryCalculator {
	if jitterFactor < 0 || jitterFactor > 1 {
		jitterFactor = 0.2
	}
	return &retryCalculator{
		baseDelay:    baseDelay,
		maxDelay:     maxDelay,
		jitterFactor: jitterFactor,
	}
}

func (c *retryCalculator) calculateNextRetry(attempt int) time.Time {
	if attempt < 0 {
		attempt = 0
	}
	return time.Now().Add(c.calculateBackoff(attempt))
}

func (c *retryCalculator) calculateBackoff(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}

	backoff := float64(c.baseDelay) * math.Pow(2, float64(attempt-1))

	if maxDelay := float64(c.maxDelay); backoff > maxDelay {
		backoff = maxDelay
	}

	if c.jitterFactor > 0 {
		jitterRange := backoff * c.jitterFactor
		jitter := (rand.Float64() - 0.5) * 2 * jitterRange
		backoff += jitter

		if backoff < float64(c.baseDelay) {
			backoff = float64(c.baseDelay)
		}
	}

	return time.Duration(backoff)
}
