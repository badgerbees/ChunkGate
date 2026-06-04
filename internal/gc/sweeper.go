package gc

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

const (
	DefaultInterval     = time.Hour
	DefaultMinOrphanAge = 24 * time.Hour
	DefaultBatchSize    = 1000
	DefaultMaxRetries   = 3
)

type Sweeper struct {
	Store        metadata.Store
	Backend      backend.BlockStore
	Limit        int
	BatchSize    int
	MinOrphanAge time.Duration
	MaxRetries   int
	Metrics      *Metrics
	Now          func() time.Time
}

type Result struct {
	ScannedTenants  int
	CandidateBlocks int
	DeletedBlocks   int
	Failures        int
}

type Metrics struct {
	runs            atomic.Int64
	scannedTenants  atomic.Int64
	candidateBlocks atomic.Int64
	deletedBlocks   atomic.Int64
	failures        atomic.Int64
	lastRunUnix     atomic.Int64
}

type MetricsSnapshot struct {
	Runs            int64
	ScannedTenants  int64
	CandidateBlocks int64
	DeletedBlocks   int64
	Failures        int64
	LastRunUnix     int64
}

type Worker struct {
	Sweeper  *Sweeper
	Interval time.Duration
	Logger   *log.Logger
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) Observe(result Result) {
	if m == nil {
		return
	}
	m.runs.Add(1)
	m.scannedTenants.Add(int64(result.ScannedTenants))
	m.candidateBlocks.Add(int64(result.CandidateBlocks))
	m.deletedBlocks.Add(int64(result.DeletedBlocks))
	m.failures.Add(int64(result.Failures))
	m.lastRunUnix.Store(time.Now().UTC().Unix())
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		Runs:            m.runs.Load(),
		ScannedTenants:  m.scannedTenants.Load(),
		CandidateBlocks: m.candidateBlocks.Load(),
		DeletedBlocks:   m.deletedBlocks.Load(),
		Failures:        m.failures.Load(),
		LastRunUnix:     m.lastRunUnix.Load(),
	}
}

func (s Sweeper) Sweep(ctx context.Context) (Result, error) {
	var result Result
	if s.Store == nil || s.Backend == nil {
		result.Failures++
		s.metrics().Observe(result)
		return result, fmt.Errorf("gc sweeper requires store and backend")
	}

	tenants, err := s.Store.ListTenants(ctx)
	if err != nil {
		result.Failures++
		s.metrics().Observe(result)
		return result, err
	}
	result.ScannedTenants = len(tenants)

	cutoff := s.now().Add(-s.minOrphanAge())
	batchSize := s.batchSize()
	for _, tenant := range tenants {
		for {
			if err := ctx.Err(); err != nil {
				result.Failures++
				s.metrics().Observe(result)
				return result, err
			}
			blocks, err := s.Store.ListUnreferencedBlocksOlderThan(ctx, tenant, cutoff, batchSize)
			if err != nil {
				result.Failures++
				s.metrics().Observe(result)
				return result, err
			}
			result.CandidateBlocks += len(blocks)
			if len(blocks) == 0 {
				break
			}

			hashes := make([]string, 0, len(blocks))
			for _, block := range blocks {
				hashes = append(hashes, block.Hash)
			}
			if err := s.deleteWithRetry(ctx, tenant, hashes); err != nil {
				result.Failures++
				s.metrics().Observe(result)
				return result, err
			}
			if err := s.Store.ForgetBlocks(ctx, tenant, hashes); err != nil {
				result.Failures++
				s.metrics().Observe(result)
				return result, err
			}
			result.DeletedBlocks += len(hashes)
			if len(blocks) < batchSize {
				break
			}
		}
	}

	s.metrics().Observe(result)
	return result, nil
}

func (s Sweeper) SweepOnce(ctx context.Context) (int, error) {
	result, err := s.Sweep(ctx)
	return result.DeletedBlocks, err
}

func (s Sweeper) deleteWithRetry(ctx context.Context, tenant string, hashes []string) error {
	attempts := s.maxRetries() + 1
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = s.Backend.DeleteBlocks(ctx, tenant, hashes)
		if last == nil {
			return nil
		}
		if attempt == attempts-1 {
			break
		}
		delay := time.Duration(100*(1<<attempt)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return last
}

func (s Sweeper) metrics() *Metrics {
	if s.Metrics != nil {
		return s.Metrics
	}
	return nil
}

func (s Sweeper) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s Sweeper) minOrphanAge() time.Duration {
	if s.MinOrphanAge < 0 {
		return DefaultMinOrphanAge
	}
	return s.MinOrphanAge
}

func (s Sweeper) batchSize() int {
	batchSize := s.BatchSize
	if batchSize <= 0 {
		batchSize = s.Limit
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if batchSize > DefaultBatchSize {
		return DefaultBatchSize
	}
	return batchSize
}

func (s Sweeper) maxRetries() int {
	if s.MaxRetries < 0 {
		return DefaultMaxRetries
	}
	return s.MaxRetries
}

func (w Worker) Run(ctx context.Context) {
	if w.Sweeper == nil {
		return
	}
	interval := w.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	w.runOnce(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.runOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w Worker) runOnce(ctx context.Context) {
	result, err := w.Sweeper.Sweep(ctx)
	if err == nil {
		if w.Logger != nil && (result.CandidateBlocks > 0 || result.DeletedBlocks > 0) {
			w.Logger.Printf("gc sweep scanned_tenants=%d candidate_blocks=%d deleted_blocks=%d failures=%d",
				result.ScannedTenants, result.CandidateBlocks, result.DeletedBlocks, result.Failures)
		}
		return
	}
	if w.Logger != nil && ctx.Err() == nil {
		w.Logger.Printf("gc sweep failed: %v", err)
	}
}
