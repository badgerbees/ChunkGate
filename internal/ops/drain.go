package ops

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrDraining = errors.New("server is draining active uploads")

type Drain struct {
	draining atomic.Bool
	wg       sync.WaitGroup
}

func (d *Drain) Begin() (func(), error) {
	if d == nil {
		return func() {}, nil
	}
	if d.draining.Load() {
		return nil, ErrDraining
	}
	d.wg.Add(1)
	if d.draining.Load() {
		d.wg.Done()
		return nil, ErrDraining
	}
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			d.wg.Done()
		}
	}, nil
}

func (d *Drain) Start() {
	if d != nil {
		d.draining.Store(true)
	}
}

func (d *Drain) IsDraining() bool {
	return d != nil && d.draining.Load()
}

func (d *Drain) Wait(ctx context.Context) error {
	if d == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
