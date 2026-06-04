package multipart

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
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

func TestManagerReloadsDurableMultipartState(t *testing.T) {
	ctx := context.Background()
	scratchRoot := t.TempDir()
	metadataRoot := t.TempDir()
	store, err := metadata.NewSQLiteStore(metadataRoot)
	if err != nil {
		t.Fatalf("open metadata store failed: %v", err)
	}

	firstReservations := limits.NewDiskReservations(1024)
	first := NewManager(
		scratchRoot,
		firstReservations,
		WithMetadataStore(store),
		WithMaxPartSize(1024),
		WithMaxUploadSize(1024),
	)
	session, err := first.Create(ctx, "tenant-a", "bucket", "key", 0, map[string]string{"Content-Type": "text/plain"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := first.PutPart(ctx, "tenant-a", session.UploadID, 2, strings.NewReader("world")); err != nil {
		t.Fatalf("put part 2 failed: %v", err)
	}
	if _, err := first.PutPart(ctx, "tenant-a", session.UploadID, 1, strings.NewReader("hello ")); err != nil {
		t.Fatalf("put part 1 failed: %v", err)
	}
	if firstReservations.Used() != 11 {
		t.Fatalf("first reservations = %d, want 11", firstReservations.Used())
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close metadata store failed: %v", err)
	}

	reopened, err := metadata.NewSQLiteStore(metadataRoot)
	if err != nil {
		t.Fatalf("reopen metadata store failed: %v", err)
	}
	defer reopened.Close()
	secondReservations := limits.NewDiskReservations(1024)
	second := NewManager(
		scratchRoot,
		secondReservations,
		WithMetadataStore(reopened),
		WithMaxPartSize(1024),
		WithMaxUploadSize(1024),
	)
	if err := second.LoadActive(ctx); err != nil {
		t.Fatalf("load active failed: %v", err)
	}
	if secondReservations.Used() != 11 {
		t.Fatalf("second reservations = %d, want 11", secondReservations.Used())
	}
	loaded, reader, err := second.Open(ctx, "tenant-a", session.UploadID, []int{1, 2})
	if err != nil {
		t.Fatalf("open reloaded upload failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read reloaded data failed: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reloaded reader failed: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("reloaded data = %q", data)
	}
	if loaded.Headers["Content-Type"] != "text/plain" {
		t.Fatalf("headers = %#v", loaded.Headers)
	}
	if err := second.CompleteCleanup(ctx, "tenant-a", session.UploadID); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if secondReservations.Used() != 0 {
		t.Fatalf("reservations after cleanup = %d, want 0", secondReservations.Used())
	}
	if _, err := reopened.GetMultipartSession(ctx, "tenant-a", session.UploadID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("get cleaned session err = %v, want not found", err)
	}
	if _, err := os.Stat(filepath.Join(scratchRoot, "tenant-a", session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch directory err = %v, want not exist", err)
	}
}

func TestManagerRejectsOversizedPartBeforePersisting(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(t.TempDir(), limits.NewDiskReservations(1024), WithMaxPartSize(4))
	session, err := manager.Create(ctx, "tenant-a", "bucket", "key", 0)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := manager.PutPart(ctx, "tenant-a", session.UploadID, 1, strings.NewReader("12345")); !errors.Is(err, ErrPartTooLarge) {
		t.Fatalf("put oversized part err = %v, want ErrPartTooLarge", err)
	}
	loaded, err := manager.Get(ctx, "tenant-a", session.UploadID)
	if err != nil {
		t.Fatalf("get session failed: %v", err)
	}
	if len(loaded.Parts) != 0 {
		t.Fatalf("parts = %#v, want none", loaded.Parts)
	}
	entries, err := os.ReadDir(session.Directory)
	if err != nil {
		t.Fatalf("read scratch dir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("scratch entries = %d, want 0", len(entries))
	}
}

func TestManagerRejectsUploadOverConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	reservations := limits.NewDiskReservations(1024)
	manager := NewManager(
		t.TempDir(),
		reservations,
		WithMaxPartSize(1024),
		WithMaxUploadSize(8),
	)
	session, err := manager.Create(ctx, "tenant-a", "bucket", "key", 0)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := manager.PutPart(ctx, "tenant-a", session.UploadID, 1, strings.NewReader("12345")); err != nil {
		t.Fatalf("put first part failed: %v", err)
	}
	if _, err := manager.PutPart(ctx, "tenant-a", session.UploadID, 2, strings.NewReader("1234")); !errors.Is(err, ErrUploadTooLarge) {
		t.Fatalf("put part over upload limit err = %v, want ErrUploadTooLarge", err)
	}
	if reservations.Used() != 5 {
		t.Fatalf("reservations = %d, want 5", reservations.Used())
	}
	loaded, err := manager.Get(ctx, "tenant-a", session.UploadID)
	if err != nil {
		t.Fatalf("get session failed: %v", err)
	}
	if len(loaded.Parts) != 1 {
		t.Fatalf("parts = %#v, want only first part", loaded.Parts)
	}
}

func TestManagerCleanupStaleSessionsRemovesMetadataScratchAndReservations(t *testing.T) {
	ctx := context.Background()
	scratchRoot := t.TempDir()
	store := metadata.NewMemoryStore()
	created := time.Now().UTC().Add(-2 * time.Hour)
	uploadID := "stale-upload"
	sessionDir := filepath.Join(scratchRoot, "tenant-a", uploadID)
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("create stale scratch dir failed: %v", err)
	}
	partPath := filepath.Join(sessionDir, "part-00000001")
	if err := os.WriteFile(partPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale part failed: %v", err)
	}
	if err := store.CreateMultipartSession(ctx, metadata.MultipartSession{
		UploadID:      uploadID,
		Tenant:        "tenant-a",
		Bucket:        "bucket",
		Key:           "key",
		ReservedBytes: 5,
		CreatedAt:     created,
		UpdatedAt:     created,
	}); err != nil {
		t.Fatalf("create multipart metadata failed: %v", err)
	}
	if err := store.SaveMultipartPart(ctx, "tenant-a", uploadID, metadata.MultipartPart{
		Number:    1,
		Size:      5,
		ETag:      `"etag"`,
		Path:      partPath,
		CreatedAt: created,
	}, 5); err != nil {
		t.Fatalf("save multipart part failed: %v", err)
	}

	reservations := limits.NewDiskReservations(1024)
	manager := NewManager(scratchRoot, reservations, WithMetadataStore(store))
	cleaned, err := manager.CleanupStale(ctx, time.Hour)
	if err != nil {
		t.Fatalf("cleanup stale failed: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	if reservations.Used() != 0 {
		t.Fatalf("reservations = %d, want 0", reservations.Used())
	}
	if _, err := store.GetMultipartSession(ctx, "tenant-a", uploadID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("get stale session err = %v, want not found", err)
	}
	if _, err := os.Stat(sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale scratch dir err = %v, want not exist", err)
	}
}
