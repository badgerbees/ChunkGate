package gc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

const testBlockHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestSweeperDeletesUnreferencedBlocks(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	blocks := backend.NewFileStore(t.TempDir())

	createUnreferencedBlocks(t, ctx, store, "tenant-a", []string{testBlockHash})
	if err := blocks.PutBlock(ctx, "tenant-a", testBlockHash, []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}

	deleted, err := (Sweeper{Store: store, Backend: blocks}).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if refs, err := store.ListUnreferencedBlocks(ctx, "tenant-a", 10); err != nil || len(refs) != 0 {
		t.Fatalf("unreferenced after sweep = %v, %v", refs, err)
	}
}

func TestSweeperHonorsMinimumOrphanAge(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	blocks := backend.NewFileStore(t.TempDir())

	createUnreferencedBlocks(t, ctx, store, "tenant-a", []string{testBlockHash})
	if err := blocks.PutBlock(ctx, "tenant-a", testBlockHash, []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}

	result, err := (Sweeper{
		Store:        store,
		Backend:      blocks,
		MinOrphanAge: 24 * time.Hour,
	}).Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.CandidateBlocks != 0 || result.DeletedBlocks != 0 {
		t.Fatalf("result = %#v, want no candidates or deletes", result)
	}
	reader, err := blocks.GetBlock(ctx, "tenant-a", testBlockHash)
	if err != nil {
		t.Fatalf("block should still exist: %v", err)
	}
	reader.Close()
}

func TestSweeperDoesNotDeleteReferencedBlocks(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	blocks := &recordingBackend{}

	commitObject(t, ctx, store, "tenant-a", "bucket", "key", []string{testBlockHash})
	result, err := (Sweeper{Store: store, Backend: blocks}).Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.CandidateBlocks != 0 || result.DeletedBlocks != 0 {
		t.Fatalf("result = %#v, want no referenced block candidates", result)
	}
	if len(blocks.calls) != 0 {
		t.Fatalf("delete calls = %#v, want none", blocks.calls)
	}
}

func TestSweeperBatchesRetriesAndRecordsMetrics(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	hashes := numberedHashes(1001)
	createUnreferencedBlocks(t, ctx, store, "tenant-a", hashes)

	metrics := NewMetrics()
	blocks := &recordingBackend{failures: 1}
	result, err := (Sweeper{
		Store:      store,
		Backend:    blocks,
		BatchSize:  2000,
		MaxRetries: 1,
		Metrics:    metrics,
	}).Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.DeletedBlocks != 1001 || result.CandidateBlocks != 1001 || result.ScannedTenants != 1 {
		t.Fatalf("result = %#v", result)
	}
	if got := blocks.callSizes(); len(got) != 3 || got[0] != 1000 || got[1] != 1000 || got[2] != 1 {
		t.Fatalf("delete call sizes = %#v, want [1000 1000 1]", got)
	}
	snapshot := metrics.Snapshot()
	if snapshot.Runs != 1 || snapshot.CandidateBlocks != 1001 || snapshot.DeletedBlocks != 1001 || snapshot.Failures != 0 {
		t.Fatalf("metrics = %#v", snapshot)
	}
}

func TestSweeperForgetsMetadataOnlyAfterBackendDeleteSucceeds(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	createUnreferencedBlocks(t, ctx, store, "tenant-a", []string{testBlockHash})

	_, err := (Sweeper{
		Store:      store,
		Backend:    &recordingBackend{failures: 1},
		MaxRetries: 0,
	}).Sweep(ctx)
	if err == nil {
		t.Fatal("expected sweep to fail")
	}
	refs, listErr := store.ListUnreferencedBlocks(ctx, "tenant-a", 10)
	if listErr != nil {
		t.Fatalf("list unreferenced failed: %v", listErr)
	}
	if len(refs) != 1 || refs[0].Hash != testBlockHash {
		t.Fatalf("refs = %#v, want test block still present", refs)
	}
}

func TestWorkerRunsSweeperInBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := metadata.NewMemoryStore()
	createUnreferencedBlocks(t, ctx, store, "tenant-a", []string{testBlockHash})
	metrics := NewMetrics()
	go (Worker{
		Sweeper: &Sweeper{
			Store:   store,
			Backend: &recordingBackend{},
			Metrics: metrics,
		},
		Interval: time.Hour,
	}).Run(ctx)

	deadline := time.After(2 * time.Second)
	for {
		if metrics.Snapshot().Runs > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("worker did not run sweep")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

type recordingBackend struct {
	mu       sync.Mutex
	calls    [][]string
	failures int
}

func (b *recordingBackend) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	return ctx.Err()
}

func (b *recordingBackend) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (b *recordingBackend) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, append([]string(nil), hashes...))
	if b.failures > 0 {
		b.failures--
		return backend.ErrBackendUnavailable
	}
	return nil
}

func (b *recordingBackend) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (b *recordingBackend) HealthCheck(ctx context.Context) error {
	return ctx.Err()
}

func (b *recordingBackend) callSizes() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	sizes := make([]int, 0, len(b.calls))
	for _, call := range b.calls {
		sizes = append(sizes, len(call))
	}
	return sizes
}

func createUnreferencedBlocks(t *testing.T, ctx context.Context, store metadata.Store, tenant string, hashes []string) {
	t.Helper()
	commitObject(t, ctx, store, tenant, "bucket", "key-"+hashes[0], hashes)
	if _, err := store.DeleteObject(ctx, tenant, "bucket", "key-"+hashes[0]); err != nil {
		t.Fatalf("delete object failed: %v", err)
	}
}

func commitObject(t *testing.T, ctx context.Context, store metadata.Store, tenant string, bucket string, key string, hashes []string) {
	t.Helper()
	chunks := make([]metadata.ChunkRef, 0, len(hashes))
	var offset int64
	for _, hash := range hashes {
		chunks = append(chunks, metadata.ChunkRef{Hash: hash, Offset: offset, Size: 1})
		offset++
	}
	pending, err := store.CreatePendingObject(ctx, metadata.ObjectManifest{Tenant: tenant, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	if err := store.CommitObject(ctx, pending, metadata.ObjectManifest{
		Tenant: tenant,
		Bucket: bucket,
		Key:    key,
		Size:   int64(len(chunks)),
		ETag:   `"etag"`,
		Chunks: chunks,
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func numberedHashes(count int) []string {
	hashes := make([]string, 0, count)
	for i := 0; i < count; i++ {
		hashes = append(hashes, fmt.Sprintf("%064x", i))
	}
	return hashes
}
