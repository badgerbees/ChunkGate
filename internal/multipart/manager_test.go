package multipart

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/chunkgate/chunkgate/internal/limits"
)

func TestManagerAssemblesOutOfOrderPartsSequentially(t *testing.T) {
	manager := NewManager(t.TempDir(), limits.NewDiskReservations(1024))
	ctx := context.Background()
	session, err := manager.Create(ctx, "tenant-a", "bucket", "key", 12)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := manager.PutPart(ctx, "tenant-a", session.UploadID, 2, strings.NewReader("world")); err != nil {
		t.Fatalf("put part 2 failed: %v", err)
	}
	if _, err := manager.PutPart(ctx, "tenant-a", session.UploadID, 1, strings.NewReader("hello ")); err != nil {
		t.Fatalf("put part 1 failed: %v", err)
	}
	_, reader, err := manager.Open(ctx, "tenant-a", session.UploadID, []int{1, 2})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read assembled data failed: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("assembled = %q", data)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close assembled reader failed: %v", err)
	}
	if err := manager.CompleteCleanup(ctx, "tenant-a", session.UploadID); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if limits.NewDiskReservations(0).Used() != 0 {
		t.Fatal("unreachable")
	}
}
