package object

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

func TestServicePutOpenDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: backend.NewFileStore(t.TempDir()),
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(1),
	})

	manifest, err := service.Put(ctx, "tenant-a", "bucket", "key", strings.NewReader("hello hello hello"))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if manifest.ETag != `"667a6d14b2dc154feac362ddad4d4ca8"` {
		t.Fatalf("etag = %s", manifest.ETag)
	}
	_, reader, err := service.Open(ctx, "tenant-a", "bucket", "key")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != "hello hello hello" {
		t.Fatalf("data = %q", data)
	}
	if _, err := service.Delete(ctx, "tenant-a", "bucket", "key"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, _, err := service.Open(ctx, "tenant-a", "bucket", "key"); err == nil {
		t.Fatal("expected deleted object to be missing")
	}
}
