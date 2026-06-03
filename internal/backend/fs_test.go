package backend

import (
	"context"
	"io"
	"testing"
)

func TestFileStorePutGetDeleteBlock(t *testing.T) {
	store := NewFileStore(t.TempDir())
	ctx := context.Background()
	hash := "0123456789abcdef"

	if err := store.PutBlock(ctx, "tenant-a", hash, []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}
	reader, err := store.GetBlock(ctx, "tenant-a", hash)
	if err != nil {
		t.Fatalf("get block failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read block failed: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("data = %q, want payload", data)
	}
	if err := store.DeleteBlocks(ctx, "tenant-a", []string{hash}); err != nil {
		t.Fatalf("delete block failed: %v", err)
	}
	if _, err := store.GetBlock(ctx, "tenant-a", hash); err == nil {
		t.Fatal("expected block to be deleted")
	}
}
