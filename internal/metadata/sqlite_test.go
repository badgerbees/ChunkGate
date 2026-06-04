package metadata

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestSQLiteStorePersistsTenantShard(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("open sqlite store failed: %v", err)
	}
	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: "tenant-a", Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	if err := store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "bucket",
		Key:    "key",
		Size:   4,
		ETag:   `"etag"`,
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
		Chunks: []ChunkRef{{Hash: "hash-a", Offset: 0, Size: 4, BackendKey: "blocks/custom/hash-a"}},
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("reopen sqlite store failed: %v", err)
	}
	defer reopened.Close()
	manifest, err := reopened.GetObject(ctx, "tenant-a", "bucket", "key")
	if err != nil {
		t.Fatalf("get object after reopen failed: %v", err)
	}
	if manifest.ETag != `"etag"` || len(manifest.Chunks) != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.Headers["Content-Type"] != "text/plain" {
		t.Fatalf("headers = %#v", manifest.Headers)
	}
	if manifest.Chunks[0].BackendKey != "blocks/custom/hash-a" || manifest.Chunks[0].Offset != 0 || manifest.Chunks[0].Size != 4 {
		t.Fatalf("chunk = %#v", manifest.Chunks[0])
	}
}

func TestSQLiteStoreCreatesStructuredSchemaAndMigrationRecord(t *testing.T) {
	ctx := context.Background()
	store, db := openSQLiteTestStore(t, ctx)
	defer store.Close()

	for _, table := range []string{"objects", "object_chunks", "blocks", "multipart_sessions", "multipart_parts", "schema_migrations"} {
		if !sqliteTableExists(t, ctx, db, table) {
			t.Fatalf("table %s was not created", table)
		}
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version = 1`).Scan(&version); err != nil {
		t.Fatalf("migration version missing: %v", err)
	}
}

func TestSQLiteStoreStoresManifestInStructuredRows(t *testing.T) {
	ctx := context.Background()
	store, db := openSQLiteTestStore(t, ctx)
	defer store.Close()

	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: "tenant-a", Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	chunks := []ChunkRef{
		{Hash: "hash-a", Offset: 0, Size: 10},
		{Hash: "hash-b", Offset: 10, Size: 15, BackendKey: "blocks/custom/hash-b"},
	}
	if err := store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "bucket",
		Key:    "key",
		Size:   25,
		ETag:   `"etag"`,
		Chunks: chunks,
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	var objectRows, chunkRows, blockRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE state = 'committed'`).Scan(&objectRows); err != nil {
		t.Fatalf("count objects failed: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_chunks`).Scan(&chunkRows); err != nil {
		t.Fatalf("count object chunks failed: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks`).Scan(&blockRows); err != nil {
		t.Fatalf("count blocks failed: %v", err)
	}
	if objectRows != 1 || chunkRows != 2 || blockRows != 2 {
		t.Fatalf("rows objects=%d chunks=%d blocks=%d", objectRows, chunkRows, blockRows)
	}

	var backendKey string
	if err := db.QueryRowContext(ctx, `SELECT backend_key FROM object_chunks WHERE sequence_order = 1`).Scan(&backendKey); err != nil {
		t.Fatalf("load backend key failed: %v", err)
	}
	if backendKey != "blocks/custom/hash-b" {
		t.Fatalf("backend key = %q", backendKey)
	}
}

func TestSQLiteStoreOverwriteUpdatesReferenceCounts(t *testing.T) {
	ctx := context.Background()
	store, db := openSQLiteTestStore(t, ctx)
	defer store.Close()

	commitSQLiteObject(t, ctx, store, "tenant-a", "bucket", "key", []ChunkRef{
		{Hash: "shared", Offset: 0, Size: 10},
		{Hash: "old", Offset: 10, Size: 10},
	})
	commitSQLiteObject(t, ctx, store, "tenant-a", "bucket", "key", []ChunkRef{
		{Hash: "shared", Offset: 0, Size: 10},
		{Hash: "new", Offset: 10, Size: 10},
	})

	wantRefs := map[string]int{"shared": 1, "old": 0, "new": 1}
	for hash, want := range wantRefs {
		var got int
		if err := db.QueryRowContext(ctx, `SELECT ref_count FROM blocks WHERE hash = ?`, hash).Scan(&got); err != nil {
			t.Fatalf("load ref_count for %s failed: %v", hash, err)
		}
		if got != want {
			t.Fatalf("ref_count[%s] = %d, want %d", hash, got, want)
		}
	}
	var committed int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE bucket = 'bucket' AND key = 'key' AND state = 'committed'`).Scan(&committed); err != nil {
		t.Fatalf("count committed failed: %v", err)
	}
	if committed != 1 {
		t.Fatalf("committed objects = %d, want 1", committed)
	}
}

func TestSQLiteStoreDeleteMakesBlocksGCEligibleAndForgettable(t *testing.T) {
	ctx := context.Background()
	store, db := openSQLiteTestStore(t, ctx)
	defer store.Close()

	commitSQLiteObject(t, ctx, store, "tenant-a", "bucket", "key", []ChunkRef{{Hash: "hash-a", Offset: 0, Size: 10}})
	if _, err := store.DeleteObject(ctx, "tenant-a", "bucket", "key"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	blocks, err := store.ListUnreferencedBlocks(ctx, "tenant-a", 10)
	if err != nil {
		t.Fatalf("list unreferenced failed: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Hash != "hash-a" {
		t.Fatalf("blocks = %#v, want hash-a", blocks)
	}
	if err := store.ForgetBlocks(ctx, "tenant-a", []string{"hash-a"}); err != nil {
		t.Fatalf("forget block failed: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE hash = 'hash-a'`).Scan(&count); err != nil {
		t.Fatalf("count forgotten block failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("forgotten block rows = %d, want 0", count)
	}
}

func TestSQLiteStoreCommitRollbackLeavesNoStructuredRows(t *testing.T) {
	ctx := context.Background()
	store, db := openSQLiteTestStore(t, ctx)
	defer store.Close()

	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: "tenant-a", Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	err = store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: "tenant-a",
		Bucket: "bucket",
		Key:    "key",
		Size:   10,
		ETag:   `"etag"`,
		Chunks: []ChunkRef{
			{Hash: "ok", Offset: 0, Size: 5},
			{Hash: "bad", Offset: 5, Size: -1},
		},
	})
	if err == nil {
		t.Fatal("expected commit to fail")
	}
	if _, err := store.GetObject(ctx, "tenant-a", "bucket", "key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after failed commit err = %v, want not found", err)
	}
	var chunks, blocks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_chunks`).Scan(&chunks); err != nil {
		t.Fatalf("count chunks failed: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks`).Scan(&blocks); err != nil {
		t.Fatalf("count blocks failed: %v", err)
	}
	if chunks != 0 || blocks != 0 {
		t.Fatalf("rollback left chunks=%d blocks=%d", chunks, blocks)
	}
}

func TestSQLiteStorePersistsMultipartState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	created := time.Now().UTC().Add(-2 * time.Hour)
	store, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("open sqlite store failed: %v", err)
	}
	if err := store.CreateMultipartSession(ctx, MultipartSession{
		UploadID:      "upload-1",
		Tenant:        "tenant-a",
		Bucket:        "bucket",
		Key:           "large.bin",
		Headers:       map[string]string{"Content-Type": "application/octet-stream"},
		ReservedBytes: 16,
		CreatedAt:     created,
		UpdatedAt:     created,
	}); err != nil {
		t.Fatalf("create multipart session failed: %v", err)
	}
	if err := store.SaveMultipartPart(ctx, "tenant-a", "upload-1", MultipartPart{
		Number:    2,
		Size:      16,
		ETag:      `"etag-2"`,
		Path:      "scratch/part-00000002",
		CreatedAt: created.Add(time.Second),
	}, 16); err != nil {
		t.Fatalf("save multipart part failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close sqlite store failed: %v", err)
	}

	reopened, err := NewSQLiteStore(root)
	if err != nil {
		t.Fatalf("reopen sqlite store failed: %v", err)
	}
	defer reopened.Close()
	session, err := reopened.GetMultipartSession(ctx, "tenant-a", "upload-1")
	if err != nil {
		t.Fatalf("get multipart session failed: %v", err)
	}
	if session.Bucket != "bucket" || session.Key != "large.bin" || session.ReservedBytes != 16 {
		t.Fatalf("session = %#v", session)
	}
	if session.Headers["Content-Type"] != "application/octet-stream" {
		t.Fatalf("headers = %#v", session.Headers)
	}
	if part := session.Parts[2]; part.Size != 16 || part.ETag != `"etag-2"` || part.Path != "scratch/part-00000002" {
		t.Fatalf("part = %#v", part)
	}
	sessions, err := reopened.ListMultipartSessions(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list multipart sessions failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	stale, err := reopened.ListStaleMultipartSessions(ctx, "tenant-a", time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatalf("list stale multipart sessions failed: %v", err)
	}
	if len(stale) != 1 || stale[0].UploadID != "upload-1" {
		t.Fatalf("stale = %#v", stale)
	}
	if err := reopened.DeleteMultipartSession(ctx, "tenant-a", "upload-1"); err != nil {
		t.Fatalf("delete multipart session failed: %v", err)
	}
	if _, err := reopened.GetMultipartSession(ctx, "tenant-a", "upload-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted multipart session err = %v, want not found", err)
	}
}

func openSQLiteTestStore(t *testing.T, ctx context.Context) (*SQLiteStore, *sql.DB) {
	t.Helper()
	store, err := NewSQLiteStore(t.TempDir())
	if err != nil {
		t.Fatalf("open sqlite store failed: %v", err)
	}
	db, err := store.dbFor(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("open tenant db failed: %v", err)
	}
	return store, db
}

func sqliteTableExists(t *testing.T, ctx context.Context, db *sql.DB, table string) bool {
	t.Helper()
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master failed: %v", err)
	}
	return name == table
}

func commitSQLiteObject(t *testing.T, ctx context.Context, store *SQLiteStore, tenant string, bucket string, key string, chunks []ChunkRef) {
	t.Helper()
	var size int64
	for _, chunk := range chunks {
		size += chunk.Size
	}
	pending, err := store.CreatePendingObject(ctx, ObjectManifest{Tenant: tenant, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("create pending failed: %v", err)
	}
	if err := store.CommitObject(ctx, pending, ObjectManifest{
		Tenant: tenant,
		Bucket: bucket,
		Key:    key,
		Size:   size,
		ETag:   `"etag"`,
		Chunks: chunks,
	}); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}
