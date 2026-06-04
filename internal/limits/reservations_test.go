package limits

import (
	"context"
	"errors"
	"math"
	"runtime"
	"testing"
	"time"
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

func TestAdaptiveCPUSemaphoreLeavesHeadroomAndReportsQueueing(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)

	sem := NewAdaptiveCPUSemaphore(8, 1)
	if got := sem.Limit(); got != 1 {
		t.Fatalf("limit = %d, want 1", got)
	}
	release, err := sem.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	acquired := make(chan func(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		release, err := sem.Acquire(ctx)
		if err != nil {
			t.Errorf("queued acquire failed: %v", err)
			close(acquired)
			return
		}
		acquired <- release
	}()

	deadline := time.After(500 * time.Millisecond)
	for sem.Snapshot().Waiting == 0 {
		select {
		case <-deadline:
			t.Fatal("queued acquire did not report waiting")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	release()

	select {
	case releaseQueued := <-acquired:
		if releaseQueued == nil {
			t.Fatal("queued acquire did not return a release function")
		}
		releaseQueued()
	case <-time.After(time.Second):
		t.Fatal("queued acquire did not complete after release")
	}
	if snapshot := sem.Snapshot(); snapshot.QueuedTotal != 1 || snapshot.QueueWaitNanos <= 0 {
		t.Fatalf("snapshot = %#v, want one queued acquire with wait time", snapshot)
	}
}

func TestDiskGuardCombinesFreeSpaceCheckWithAtomicReservation(t *testing.T) {
	reservations := NewDiskReservations(0)
	guard := NewDiskGuard(t.TempDir(), reservations, 0)
	if err := guard.TryReserve(context.Background(), 10); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if reservations.Used() != 10 {
		t.Fatalf("used = %d, want 10", reservations.Used())
	}
	guard.Release(4)
	if reservations.Used() != 6 {
		t.Fatalf("used after release = %d, want 6", reservations.Used())
	}
}

func TestDiskGuardRollsBackReservationWhenFreeSpaceIsTooLow(t *testing.T) {
	reservations := NewDiskReservations(100)
	guard := NewDiskGuard(t.TempDir(), reservations, math.MaxInt64)
	if err := guard.TryReserve(context.Background(), 10); !errors.Is(err, ErrInsufficientDisk) {
		t.Fatalf("reserve error = %v, want insufficient disk", err)
	}
	if reservations.Used() != 0 {
		t.Fatalf("used = %d, want rolled back to 0", reservations.Used())
	}
}
