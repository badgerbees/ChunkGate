package backend

import (
	"context"
	"time"
)

const defaultRetryBaseDelay = 100 * time.Millisecond

type RetryOptions struct {
	Timeout    time.Duration
	MaxRetries int
	BaseDelay  time.Duration
}

func DoWithRetry(ctx context.Context, options RetryOptions, retryable func(error) bool, operation func(context.Context) error) error {
	attempts := options.MaxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	baseDelay := options.BaseDelay
	if baseDelay <= 0 {
		baseDelay = defaultRetryBaseDelay
	}

	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx := ctx
		var cancel context.CancelFunc
		if options.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, options.Timeout)
		}
		last = operation(attemptCtx)
		if cancel != nil {
			cancel()
		}
		if last == nil || retryable == nil || !retryable(last) || attempt == attempts-1 {
			return last
		}
		if err := waitRetryDelay(ctx, baseDelay, attempt); err != nil {
			return err
		}
	}
	return last
}

func waitRetryDelay(ctx context.Context, baseDelay time.Duration, attempt int) error {
	delay := baseDelay * time.Duration(1<<attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
