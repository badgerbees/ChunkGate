package limits

import (
	"context"
	"fmt"
)

type DiskGuard struct {
	Path         string
	MinFreeBytes int64
	Reservations *DiskReservations
}

func NewDiskGuard(path string, reservations *DiskReservations, minFreeBytes int64) *DiskGuard {
	if minFreeBytes < 0 {
		minFreeBytes = 0
	}
	return &DiskGuard{Path: path, Reservations: reservations, MinFreeBytes: minFreeBytes}
}

func (g *DiskGuard) TryReserve(ctx context.Context, bytes int64) error {
	if g == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.Reservations != nil {
		if err := g.Reservations.TryReserve(bytes); err != nil {
			return err
		}
	}
	if err := g.Check(ctx, 0); err != nil {
		if g.Reservations != nil {
			g.Reservations.Release(bytes)
		}
		return err
	}
	return nil
}

func (g *DiskGuard) Check(ctx context.Context, additionalBytes int64) error {
	if g == nil {
		return nil
	}
	if additionalBytes < 0 {
		return ErrCapacityExceeded
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	available, err := AvailableBytes(g.Path)
	if err != nil {
		return fmt.Errorf("check scratch free space: %w", err)
	}
	reserved := int64(0)
	if g.Reservations != nil {
		reserved = g.Reservations.Used()
	}
	if available-reserved-additionalBytes < g.MinFreeBytes {
		return ErrInsufficientDisk
	}
	return nil
}

func (g *DiskGuard) Release(bytes int64) {
	if g != nil && g.Reservations != nil {
		g.Reservations.Release(bytes)
	}
}
