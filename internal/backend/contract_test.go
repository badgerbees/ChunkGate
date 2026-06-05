package backend

import (
	"context"
	"errors"
	"io"
	"testing"
)

const (
	contractHashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	contractHashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	contractHashC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func RunBlockStoreContract(t *testing.T, newStore func(*testing.T) BlockStore) {
	t.Helper()
	t.Run("health check", func(t *testing.T) {
		store := newStore(t)
		if err := store.HealthCheck(context.Background()); err != nil {
			t.Fatalf("health check failed: %v", err)
		}
	})

	t.Run("missing block behavior", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ok, err := store.HasBlock(ctx, "tenant-a", contractHashA)
		if err != nil {
			t.Fatalf("has missing block failed: %v", err)
		}
		if ok {
			t.Fatal("missing block reported as present")
		}
		reader, err := store.GetBlock(ctx, "tenant-a", contractHashA)
		if reader != nil {
			_ = reader.Close()
		}
		if !errors.Is(err, ErrBlockNotFound) {
			t.Fatalf("missing block err = %v, want ErrBlockNotFound", err)
		}
	})

	t.Run("put get exists and delete", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		if err := store.PutBlock(ctx, "tenant-a", contractHashA, []byte("payload-a")); err != nil {
			t.Fatalf("put block failed: %v", err)
		}
		ok, err := store.HasBlock(ctx, "tenant-a", contractHashA)
		if err != nil {
			t.Fatalf("has block failed: %v", err)
		}
		if !ok {
			t.Fatal("stored block reported missing")
		}
		assertBlockPayload(t, store, "tenant-a", contractHashA, "payload-a")
		if err := store.DeleteBlocks(ctx, "tenant-a", []string{contractHashA}); err != nil {
			t.Fatalf("delete block failed: %v", err)
		}
		ok, err = store.HasBlock(ctx, "tenant-a", contractHashA)
		if err != nil {
			t.Fatalf("has deleted block failed: %v", err)
		}
		if ok {
			t.Fatal("deleted block reported present")
		}
	})

	t.Run("bulk delete", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		if err := store.PutBlock(ctx, "tenant-a", contractHashA, []byte("payload-a")); err != nil {
			t.Fatalf("put block a failed: %v", err)
		}
		if err := store.PutBlock(ctx, "tenant-a", contractHashB, []byte("payload-b")); err != nil {
			t.Fatalf("put block b failed: %v", err)
		}
		if err := store.DeleteBlocks(ctx, "tenant-a", []string{contractHashA, contractHashB}); err != nil {
			t.Fatalf("bulk delete failed: %v", err)
		}
		for _, hash := range []string{contractHashA, contractHashB} {
			ok, err := store.HasBlock(ctx, "tenant-a", hash)
			if err != nil {
				t.Fatalf("has deleted block %s failed: %v", hash, err)
			}
			if ok {
				t.Fatalf("deleted block %s reported present", hash)
			}
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		if err := store.PutBlock(ctx, "tenant-a", contractHashC, []byte("tenant-a-payload")); err != nil {
			t.Fatalf("put tenant-a block failed: %v", err)
		}
		if err := store.PutBlock(ctx, "tenant-b", contractHashC, []byte("tenant-b-payload")); err != nil {
			t.Fatalf("put tenant-b block failed: %v", err)
		}
		assertBlockPayload(t, store, "tenant-a", contractHashC, "tenant-a-payload")
		assertBlockPayload(t, store, "tenant-b", contractHashC, "tenant-b-payload")
		if err := store.DeleteBlocks(ctx, "tenant-a", []string{contractHashC}); err != nil {
			t.Fatalf("delete tenant-a block failed: %v", err)
		}
		ok, err := store.HasBlock(ctx, "tenant-b", contractHashC)
		if err != nil {
			t.Fatalf("has tenant-b block failed: %v", err)
		}
		if !ok {
			t.Fatal("tenant-b block disappeared after deleting tenant-a block")
		}
		assertBlockPayload(t, store, "tenant-b", contractHashC, "tenant-b-payload")
		if err := store.DeleteBlocks(ctx, "tenant-b", []string{contractHashC}); err != nil {
			t.Fatalf("delete tenant-b block failed: %v", err)
		}
	})

	t.Run("invalid hashes", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		for _, hash := range []string{"short", "../" + contractHashA[:61], "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"} {
			if err := store.PutBlock(ctx, "tenant-a", hash, []byte("payload")); err == nil {
				t.Fatalf("put accepted invalid hash %q", hash)
			}
			if _, err := store.HasBlock(ctx, "tenant-a", hash); err == nil {
				t.Fatalf("has accepted invalid hash %q", hash)
			}
			reader, err := store.GetBlock(ctx, "tenant-a", hash)
			if reader != nil {
				_ = reader.Close()
			}
			if err == nil {
				t.Fatalf("get accepted invalid hash %q", hash)
			}
			if err := store.DeleteBlocks(ctx, "tenant-a", []string{hash}); err == nil {
				t.Fatalf("delete accepted invalid hash %q", hash)
			}
		}
	})
}

func assertBlockPayload(t *testing.T, store BlockStore, tenant string, hash string, want string) {
	t.Helper()
	reader, err := store.GetBlock(context.Background(), tenant, hash)
	if err != nil {
		t.Fatalf("get block failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		t.Fatalf("read block failed: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close block failed: %v", closeErr)
	}
	if string(data) != want {
		t.Fatalf("payload = %q, want %q", data, want)
	}
}
