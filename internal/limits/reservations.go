package limits

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrCapacityExceeded = errors.New("local capacity reservation exceeded")

type DiskReservations struct {
	capacity int64
	used     atomic.Int64
}

func NewDiskReservations(capacity int64) *DiskReservations {
	return &DiskReservations{capacity: capacity}
}

func (r *DiskReservations) TryReserve(bytes int64) error {
	if bytes < 0 {
		return ErrCapacityExceeded
	}
	if bytes == 0 || r.capacity == 0 {
		return nil
	}
	for {
		current := r.used.Load()
		next := current + bytes
		if next < current || next > r.capacity {
			return ErrCapacityExceeded
		}
		if r.used.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (r *DiskReservations) Release(bytes int64) {
	if bytes <= 0 || r.capacity == 0 {
		return
	}
	for {
		current := r.used.Load()
		next := current - bytes
		if next < 0 {
			next = 0
		}
		if r.used.CompareAndSwap(current, next) {
			return
		}
	}
}

func (r *DiskReservations) Used() int64 {
	return r.used.Load()
}

type CPUSemaphore struct {
	slots chan struct{}
}

func NewCPUSemaphore(limit int) *CPUSemaphore {
	if limit <= 0 {
		limit = 1
	}
	return &CPUSemaphore{slots: make(chan struct{}, limit)}
}

func (s *CPUSemaphore) Acquire(ctx context.Context) (func(), error) {
	select {
	case s.slots <- struct{}{}:
		var released atomic.Bool
		return func() {
			if released.CompareAndSwap(false, true) {
				<-s.slots
			}
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
