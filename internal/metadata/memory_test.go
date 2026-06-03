package metadata

import (
	"context"
	"testing"
)

func TestMemoryStoreCommitDeleteAndUnreferencedBlocks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: "tenant-a", Bucket: "b", Key: "k"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	chunks := []ChunkRef{
		{Hash: "same", Offset: 0, Size: 10},
		{Hash: "same", Offset: 10, Size: 10},
	}
	if err := store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "b",
		Key:    "k",
		Size:   20,
		ETag:   `"etag"`,
		Chunks: chunks,
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if blocks, err := store.ListUnreferencedBlocks(ctx, "tenant-a", 10); err != nil || len(blocks) != 0 {
		t.Fatalf("unexpected unreferenced blocks after commit: %v %v", blocks, err)
	}
	if _, err := store.DeleteObject(ctx, "tenant-a", "b", "k"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	blocks, err := store.ListUnreferencedBlocks(ctx, "tenant-a", 10)
	if err != nil {
		t.Fatalf("list unreferenced failed: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Hash != "same" {
		t.Fatalf("blocks = %#v, want single unreferenced block", blocks)
	}
}

func TestMemoryStoreIsolatesTenants(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	for _, tenant := range []string{"a", "b"} {
		pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: tenant, Bucket: "bucket", Key: "key"})
		if err != nil {
			t.Fatalf("create pending for %s failed: %v", tenant, err)
		}
		if err := store.CommitObject(ctx, pending, ObjectManifest{
			Tenant: tenant,
			Bucket: "bucket",
			Key:    "key",
			ETag:   `"etag"`,
			Chunks: []ChunkRef{{Hash: tenant + "-hash", Size: 1}},
		}); err != nil {
			t.Fatalf("commit for %s failed: %v", tenant, err)
		}
	}
	if _, err := store.GetObject(ctx, "a", "bucket", "key"); err != nil {
		t.Fatalf("tenant a object missing: %v", err)
	}
	if _, err := store.GetObject(ctx, "b", "bucket", "key"); err != nil {
		t.Fatalf("tenant b object missing: %v", err)
	}
}
