package limits

import (
	"context"
	"errors"
	"testing"
)

func TestDiskReservationsUseAtomicCapacity(t *testing.T) {
	reservations := NewDiskReservations(10)
	if err := reservations.TryReserve(7); err != nil {
		t.Fatalf("reserve 7 failed: %v", err)
	}
	if err := reservations.TryReserve(4); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("reserve 4 error = %v, want capacity exceeded", err)
	}
	reservations.Release(3)
	if err := reservations.TryReserve(4); err != nil {
		t.Fatalf("reserve after release failed: %v", err)
	}
	if reservations.Used() != 8 {
		t.Fatalf("used = %d, want 8", reservations.Used())
	}
}

func TestCPUSemaphoreHonorsCanceledContext(t *testing.T) {
	sem := NewCPUSemaphore(1)
	release, err := sem.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sem.Acquire(ctx); err == nil {
		t.Fatal("expected canceled acquire to fail")
	}
}
