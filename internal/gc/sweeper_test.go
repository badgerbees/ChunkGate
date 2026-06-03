package gc

import (
	"context"
	"testing"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

func TestSweeperDeletesUnreferencedBlocks(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMemoryStore()
	blocks := backend.NewFileStore(t.TempDir())

	pending, err := store.CreatePendingObject(ctx, metadata.ObjectManifest{Tenant: "tenant-a", Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	if err := blocks.PutBlock(ctx, "tenant-a", "hash1234", []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}
	if err := store.CommitObject(ctx, pending, metadata.ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "bucket",
		Key:    "key",
		Chunks: []metadata.ChunkRef{{Hash: "hash1234", Size: 7}},
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if _, err := store.DeleteObject(ctx, "tenant-a", "bucket", "key"); err != nil {
		t.Fatalf("delete failed: %v", err)
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
