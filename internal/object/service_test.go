package object

import (
	"bytes"
	"context"
	"errors"
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

func TestServicePutStreamsAndSkipsDuplicateChunkWrites(t *testing.T) {
	ctx := context.Background()
	blocks := newDedupeBlockStore()
	service := NewService(Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 4, MaxSize: 4, SmallFileThreshold: 0}),
		Backend: blocks,
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(1),
	})

	manifest, err := service.Put(ctx, "tenant-a", "bucket", "key", strings.NewReader("abcdabcdabcdabcd"))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if len(manifest.Chunks) != 4 {
		t.Fatalf("chunk count = %d, want 4", len(manifest.Chunks))
	}
	if blocks.puts != 1 {
		t.Fatalf("put block calls = %d, want 1", blocks.puts)
	}
}

func TestServicePutStopsWhenUploadContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	blocks := newDedupeBlockStore()
	service := NewService(Config{
		Chunker: chunker.New(chunker.Options{MinSize: 64 * 1024, AvgSize: 128 * 1024, MaxSize: 256 * 1024, SmallFileThreshold: 0}),
		Backend: blocks,
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(1),
	})
	reader := &cancelAfterFirstRead{remaining: 2 * 1024 * 1024, cancel: cancel}

	_, err := service.Put(ctx, "tenant-a", "bucket", "key", reader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context canceled", err)
	}
	if blocks.puts != 0 {
		t.Fatalf("put block calls = %d, want 0", blocks.puts)
	}
	if _, _, err := service.Open(context.Background(), "tenant-a", "bucket", "key"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("open after canceled put err = %v, want not found", err)
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

type dedupeBlockStore struct {
	blocks map[string][]byte
	puts   int
}

func newDedupeBlockStore() *dedupeBlockStore {
	return &dedupeBlockStore{blocks: map[string][]byte{}}
}

func (s *dedupeBlockStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.puts++
	copied := make([]byte, len(data))
	copy(copied, data)
	s.blocks[tenant+"\x00"+hash] = copied
	return nil
}

func (s *dedupeBlockStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.blocks[tenant+"\x00"+hash])), nil
}

func (s *dedupeBlockStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	for _, hash := range hashes {
		delete(s.blocks, tenant+"\x00"+hash)
	}
	return nil
}

func (s *dedupeBlockStore) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, ok := s.blocks[tenant+"\x00"+hash]
	return ok, nil
}

type cancelAfterFirstRead struct {
	remaining int
	cancel    context.CancelFunc
}

func (r *cancelAfterFirstRead) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	r.cancel()
	return n, nil
}
