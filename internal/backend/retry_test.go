package backend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoWithRetryRetriesRetryableErrors(t *testing.T) {
	var attempts int
	errTransient := errors.New("transient")
	err := DoWithRetry(context.Background(), RetryOptions{
		MaxRetries: 2,
		BaseDelay:  time.Nanosecond,
	}, func(err error) bool {
		return errors.Is(err, errTransient)
	}, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errTransient
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDoWithRetryStopsOnNonRetryableError(t *testing.T) {
	var attempts int
	errPermanent := errors.New("permanent")
	err := DoWithRetry(context.Background(), RetryOptions{
		MaxRetries: 3,
		BaseDelay:  time.Nanosecond,
	}, func(error) bool {
		return false
	}, func(context.Context) error {
		attempts++
		return errPermanent
	})
	if !errors.Is(err, errPermanent) {
		t.Fatalf("err = %v, want permanent", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestDoWithRetryHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var attempts int
	err := DoWithRetry(ctx, RetryOptions{MaxRetries: 3}, func(error) bool {
		return true
	}, func(context.Context) error {
		attempts++
		return errors.New("should not run")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context canceled", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0", attempts)
	}
}
