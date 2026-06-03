package metadata

import (
	"context"
	"testing"
)

func TestSQLiteStorePersistsTenantShard(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("open sqlite store failed: %v", err)
	}
	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: "tenant-a", Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	if err := store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "bucket",
		Key:    "key",
		Size:   4,
		ETag:   `"etag"`,
		Chunks: []ChunkRef{{Hash: "hash-a", Size: 4}},
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("reopen sqlite store failed: %v", err)
	}
	defer reopened.Close()
	manifest, err := reopened.GetObject(ctx, "tenant-a", "bucket", "key")
	if err != nil {
		t.Fatalf("get object after reopen failed: %v", err)
	}
	if manifest.ETag != `"etag"` || len(manifest.Chunks) != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}
