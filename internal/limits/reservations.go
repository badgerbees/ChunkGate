package limits

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	ErrCapacityExceeded = errors.New("local capacity reservation exceeded")
	ErrInsufficientDisk = errors.New("insufficient scratch disk space")
)

type ConcurrencyLimiter interface {
	Acquire(ctx context.Context) (func(), error)
}

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
	if bytes == 0 {
		return nil
	}
	for {
		current := r.used.Load()
		next := current + bytes
		if next < current || r.capacity > 0 && next > r.capacity {
			return ErrCapacityExceeded
		}
		if r.used.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (r *DiskReservations) Release(bytes int64) {
	if bytes <= 0 {
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

type AdaptiveCPUSemaphore struct {
	maxLimit       int
	headroom       int
	active         atomic.Int64
	waiting        atomic.Int64
	acquireTotal   atomic.Int64
	queueTotal     atomic.Int64
	queueNanos     atomic.Int64
	limitFloor     int
	pollingBackoff time.Duration
}

type AdaptiveSnapshot struct {
	Limit           int64
	Active          int64
	Waiting         int64
	AcquiresTotal   int64
	QueuedTotal     int64
	QueueWaitNanos  int64
	QueueWaitMillis float64
}

func NewAdaptiveCPUSemaphore(maxLimit int, headroom int) *AdaptiveCPUSemaphore {
	if maxLimit <= 0 {
		maxLimit = runtime.NumCPU()
	}
	if headroom < 0 {
		headroom = 0
	}
	return &AdaptiveCPUSemaphore{
		maxLimit:       maxLimit,
		headroom:       headroom,
		limitFloor:     1,
		pollingBackoff: 10 * time.Millisecond,
	}
}

func (s *AdaptiveCPUSemaphore) Acquire(ctx context.Context) (func(), error) {
	start := time.Now()
	queued := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		active := s.active.Load()
		if active < int64(s.Limit()) && s.active.CompareAndSwap(active, active+1) {
			wait := time.Since(start)
			s.acquireTotal.Add(1)
			if queued {
				s.queueTotal.Add(1)
				s.queueNanos.Add(wait.Nanoseconds())
			}
			var released atomic.Bool
			return func() {
				if released.CompareAndSwap(false, true) {
					s.active.Add(-1)
				}
			}, nil
		}
		if !queued {
			s.waiting.Add(1)
			queued = true
			defer s.waiting.Add(-1)
		}
		timer := time.NewTimer(s.pollingBackoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

func (s *AdaptiveCPUSemaphore) Limit() int {
	if s == nil {
		return 1
	}
	limit := runtime.GOMAXPROCS(0) - s.headroom
	if limit < s.limitFloor {
		limit = s.limitFloor
	}
	if s.maxLimit > 0 && limit > s.maxLimit {
		limit = s.maxLimit
	}
	return limit
}

func (s *AdaptiveCPUSemaphore) Snapshot() AdaptiveSnapshot {
	if s == nil {
		return AdaptiveSnapshot{}
	}
	queueNanos := s.queueNanos.Load()
	return AdaptiveSnapshot{
		Limit:           int64(s.Limit()),
		Active:          s.active.Load(),
		Waiting:         s.waiting.Load(),
		AcquiresTotal:   s.acquireTotal.Load(),
		QueuedTotal:     s.queueTotal.Load(),
		QueueWaitNanos:  queueNanos,
		QueueWaitMillis: float64(queueNanos) / float64(time.Millisecond),
	}
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
