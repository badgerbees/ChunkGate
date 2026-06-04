package ops

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDrainRejectsNewWorkAndWaitsForActiveWork(t *testing.T) {
	var drain Drain
	done, err := drain.Begin()
	if err != nil {
		t.Fatalf("begin active work failed: %v", err)
	}
	drain.Start()
	if _, err := drain.Begin(); !errors.Is(err, ErrDraining) {
		t.Fatalf("begin while draining err = %v, want ErrDraining", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := drain.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait before done err = %v, want deadline exceeded", err)
	}

	done()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := drain.Wait(ctx); err != nil {
		t.Fatalf("wait after done failed: %v", err)
	}
}
