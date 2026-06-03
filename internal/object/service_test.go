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

func TestServiceOpenRangeFetchesOnlyIntersectingChunks(t *testing.T) {
	ctx := context.Background()
	blocks := &countingBlockStore{inner: backend.NewFileStore(t.TempDir())}
	service := NewService(Config{
		Chunker: chunker.New(chunker.Options{MinSize: 8, AvgSize: 16, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: blocks,
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(1),
	})
	payload := strings.Repeat("0123456789abcdef", 4)

	manifest, err := service.Put(ctx, "tenant-a", "bucket", "key", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if len(manifest.Chunks) < 3 {
		t.Fatalf("expected at least three chunks, got %d", len(manifest.Chunks))
	}

	target := manifest.Chunks[1]
	byteRange := ByteRange{Start: target.Offset + 2, End: target.Offset + 5}
	_, reader, err := service.OpenRange(ctx, "tenant-a", "bucket", "key", byteRange)
	if err != nil {
		t.Fatalf("open range failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read range failed: %v", err)
	}
	want := payload[byteRange.Start : byteRange.End+1]
	if string(data) != want {
		t.Fatalf("range data = %q, want %q", data, want)
	}
	if len(blocks.opened) != 1 || blocks.opened[0] != target.Hash {
		t.Fatalf("opened blocks = %#v, want only %s", blocks.opened, target.Hash)
	}
}

type countingBlockStore struct {
	inner  backend.BlockStore
	opened []string
}

func (s *countingBlockStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	return s.inner.PutBlock(ctx, tenant, hash, data)
}

func (s *countingBlockStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	s.opened = append(s.opened, hash)
	return s.inner.GetBlock(ctx, tenant, hash)
}

func (s *countingBlockStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	return s.inner.DeleteBlocks(ctx, tenant, hashes)
}
