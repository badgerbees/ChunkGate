package backend

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testBlockHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestFileStorePutGetDeleteBlock(t *testing.T) {
	store := NewFileStore(t.TempDir())
	ctx := context.Background()
	hash := testBlockHash

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

func TestFileStoreRejectsInvalidBlockHash(t *testing.T) {
	store := NewFileStore(t.TempDir())
	for _, hash := range []string{
		"short",
		strings.Repeat("g", 64),
		"../" + strings.Repeat("a", 61),
	} {
		if err := store.PutBlock(context.Background(), "tenant-a", hash, []byte("payload")); err == nil {
			t.Fatalf("expected hash %q to be rejected", hash)
		}
	}
}

func TestFileStoreKeepsTenantPathsInsideRoot(t *testing.T) {
	root := t.TempDir()
	store := NewFileStore(root)

	path, err := store.blockPath("../escape", testBlockHash)
	if err != nil {
		t.Fatalf("block path failed: %v", err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("relative path failed: %v", err)
	}
	if rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		t.Fatalf("block path escaped root: %s", path)
	}
	if strings.Contains(path, "..") {
		t.Fatalf("block path contains traversal segment: %s", path)
	}
}

func TestEncryptedFileStoreDoesNotWritePlaintext(t *testing.T) {
	root := t.TempDir()
	store, err := NewEncryptedFileStore(root, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("create encrypted store failed: %v", err)
	}
	ctx := context.Background()
	if err := store.PutBlock(ctx, "tenant-a", testBlockHash, []byte("secret-payload")); err != nil {
		t.Fatalf("put encrypted block failed: %v", err)
	}
	rawPath := filepath.Join(root, "tenants", "tenant-a", "blocks", testBlockHash[:2], testBlockHash)
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		t.Fatalf("read raw encrypted block failed: %v", err)
	}
	if strings.Contains(string(raw), "secret-payload") {
		t.Fatalf("encrypted block contains plaintext: %q", raw)
	}

	reader, err := store.GetBlock(ctx, "tenant-a", testBlockHash)
	if err != nil {
		t.Fatalf("get encrypted block failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read decrypted block failed: %v", err)
	}
	if string(data) != "secret-payload" {
		t.Fatalf("decrypted data = %q", data)
	}
}
