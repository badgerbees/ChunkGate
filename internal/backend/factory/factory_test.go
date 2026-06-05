package factory

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/config"
)

const factoryTestHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestNewBuildsFilesystemBackend(t *testing.T) {
	cfg := filesystemConfig(t.TempDir())
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("create filesystem backend failed: %v", err)
	}
	assertBackendRoundTrip(t, store)
}

func TestNewBuildsEncryptedFilesystemBackend(t *testing.T) {
	cfg := filesystemConfig(t.TempDir())
	cfg.LocalBlockEncryptionKey = "0123456789abcdef0123456789abcdef"
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("create encrypted filesystem backend failed: %v", err)
	}
	assertBackendRoundTrip(t, store)
}

func TestNewValidatesBackendConfiguration(t *testing.T) {
	cfg := filesystemConfig(t.TempDir())
	cfg.BackendProvider = "s3"
	cfg.S3Endpoint = ""
	cfg.S3Bucket = ""
	if _, err := New(cfg); err == nil {
		t.Fatal("expected invalid s3 backend config to fail")
	}
}

func TestNewBuildsAzureBackend(t *testing.T) {
	cfg := config.Config{
		BackendProvider: "azure",
		AzureEndpoint:   "https://chunkgatestorage.blob.core.windows.net",
		AzureContainer:  "chunkgate-blocks",
		AzureAuth:       "default",
		AzureTimeout:    1,
		AzureMaxRetries: 1,
	}
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("create azure backend failed: %v", err)
	}
	if _, ok := store.(*backend.AzureBlockStore); !ok {
		t.Fatalf("store type = %T, want *backend.AzureBlockStore", store)
	}
}

func TestNewBuildsGCSBackend(t *testing.T) {
	cfg := config.Config{
		BackendProvider: "gcs",
		GCSBucket:       "chunkgate-blocks",
		GCSEndpoint:     "http://127.0.0.1:4443/storage/v1/",
		GCSAuth:         "emulator",
		GCSTimeout:      1,
		GCSMaxRetries:   1,
	}
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("create gcs backend failed: %v", err)
	}
	if _, ok := store.(*backend.GCSBlockStore); !ok {
		t.Fatalf("store type = %T, want *backend.GCSBlockStore", store)
	}
}

func filesystemConfig(root string) config.Config {
	return config.Config{
		BackendProvider: "filesystem",
		BackendDir:      root,
	}
}

func assertBackendRoundTrip(t *testing.T, store backend.BlockStore) {
	t.Helper()
	ctx := context.Background()
	if err := store.HealthCheck(ctx); err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if err := store.PutBlock(ctx, "tenant-a", factoryTestHash, []byte("factory-payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}
	ok, err := store.HasBlock(ctx, "tenant-a", factoryTestHash)
	if err != nil {
		t.Fatalf("has block failed: %v", err)
	}
	if !ok {
		t.Fatal("expected block to exist")
	}
	reader, err := store.GetBlock(ctx, "tenant-a", factoryTestHash)
	if err != nil {
		t.Fatalf("get block failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read block failed: %v", err)
	}
	if string(data) != "factory-payload" {
		t.Fatalf("payload = %q", data)
	}
	if err := store.DeleteBlocks(ctx, "tenant-a", []string{factoryTestHash}); err != nil {
		t.Fatalf("delete block failed: %v", err)
	}
	if _, err := store.GetBlock(ctx, "tenant-a", factoryTestHash); !errors.Is(err, backend.ErrBlockNotFound) {
		t.Fatalf("get deleted block err = %v, want backend block not found", err)
	}
}
