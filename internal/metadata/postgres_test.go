package metadata

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestPostgresStoreRunsMigrations(t *testing.T) {
	ctx := context.Background()
	store := openPostgresTestStore(t, ctx)
	defer store.Close()

	for _, table := range []string{"objects", "object_chunks", "blocks", "multipart_sessions", "multipart_parts", "schema_migrations"} {
		if !postgresTableExists(t, ctx, store, table) {
			t.Fatalf("table %s was not created", table)
		}
	}
	var version int
	if err := store.db.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version = 1`).Scan(&version); err != nil {
		t.Fatalf("migration version missing: %v", err)
	}
}

func TestPostgresStoreSharedDatabaseLifecycle(t *testing.T) {
	ctx := context.Background()
	storeA := openPostgresTestStore(t, ctx)
	defer storeA.Close()
	storeB := openPostgresTestStore(t, ctx)
	defer storeB.Close()

	tenantA := "pg-" + randomID()
	tenantB := tenantA + "-other"
	bucket := "bucket"
	key := "artifact.bin"

	commitPostgresObject(t, ctx, storeA, tenantA, bucket, key, []ChunkRef{
		{Hash: "shared", Offset: 0, Size: 10},
		{Hash: "old", Offset: 10, Size: 10},
	})
	manifest, err := storeB.GetObject(ctx, tenantA, bucket, key)
	if err != nil {
		t.Fatalf("second store could not load committed object: %v", err)
	}
	if manifest.Tenant != tenantA || len(manifest.Chunks) != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}

	commitPostgresObject(t, ctx, storeB, tenantA, bucket, key, []ChunkRef{
		{Hash: "shared", Offset: 0, Size: 10},
		{Hash: "new", Offset: 10, Size: 10},
	})
	blocks, err := storeA.ListUnreferencedBlocks(ctx, tenantA, 10)
	if err != nil {
		t.Fatalf("list unreferenced blocks failed: %v", err)
	}
	if !hasBlock(blocks, "old") || hasBlock(blocks, "shared") {
		t.Fatalf("blocks after overwrite = %#v, want old only from replaced manifest", blocks)
	}

	commitPostgresObject(t, ctx, storeA, tenantB, bucket, key, []ChunkRef{{Hash: "tenant-b-only", Offset: 0, Size: 10}})
	if other, err := storeB.GetObject(ctx, tenantB, bucket, key); err != nil || other.Chunks[0].Hash != "tenant-b-only" {
		t.Fatalf("tenant-b manifest = %#v err=%v", other, err)
	}
	if current, err := storeB.GetObject(ctx, tenantA, bucket, key); err != nil || current.Chunks[1].Hash != "new" {
		t.Fatalf("tenant-a manifest = %#v err=%v", current, err)
	}

	deleted, err := storeA.DeleteObject(ctx, tenantA, bucket, key)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if deleted.State != StateDeleted {
		t.Fatalf("deleted state = %s", deleted.State)
	}
	blocks, err = storeB.ListUnreferencedBlocks(ctx, tenantA, 10)
	if err != nil {
		t.Fatalf("list post-delete unreferenced blocks failed: %v", err)
	}
	if !hasBlock(blocks, "old") || !hasBlock(blocks, "shared") || !hasBlock(blocks, "new") {
		t.Fatalf("blocks after delete = %#v, want old/shared/new eligible", blocks)
	}
	if err := storeA.ForgetBlocks(ctx, tenantA, []string{"old", "shared", "new"}); err != nil {
		t.Fatalf("forget blocks failed: %v", err)
	}
	blocks, err = storeB.ListUnreferencedBlocks(ctx, tenantA, 10)
	if err != nil {
		t.Fatalf("list after forget failed: %v", err)
	}
	if hasBlock(blocks, "old") || hasBlock(blocks, "shared") || hasBlock(blocks, "new") {
		t.Fatalf("forgotten blocks still visible: %#v", blocks)
	}

	tenants, err := storeA.ListTenants(ctx)
	if err != nil {
		t.Fatalf("list tenants failed: %v", err)
	}
	if !hasTenant(tenants, tenantA) || !hasTenant(tenants, tenantB) {
		t.Fatalf("tenants = %#v, want %s and %s", tenants, tenantA, tenantB)
	}
}

func TestPostgresStoreMultipartStateAcrossInstances(t *testing.T) {
	ctx := context.Background()
	storeA := openPostgresTestStore(t, ctx)
	defer storeA.Close()
	storeB := openPostgresTestStore(t, ctx)
	defer storeB.Close()

	tenant := "pg-" + randomID()
	created := time.Now().UTC().Add(-2 * time.Hour)
	if err := storeA.CreateMultipartSession(ctx, MultipartSession{
		UploadID:      "upload-" + randomID(),
		Tenant:        tenant,
		Bucket:        "bucket",
		Key:           "large.bin",
		Headers:       map[string]string{"Content-Type": "application/octet-stream"},
		ReservedBytes: 32,
		CreatedAt:     created,
		UpdatedAt:     created,
	}); err != nil {
		t.Fatalf("create multipart session failed: %v", err)
	}
	sessions, err := storeA.ListMultipartSessions(ctx, tenant)
	if err != nil {
		t.Fatalf("list multipart sessions failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("sessions = %#v, want one session", sessions)
	}
	uploadID := sessions[0].UploadID

	if err := storeB.SaveMultipartPart(ctx, tenant, uploadID, MultipartPart{
		Number:    2,
		Size:      32,
		ETag:      `"etag-2"`,
		Path:      "scratch/upload/part-00000002",
		CreatedAt: created.Add(time.Second),
	}, 32); err != nil {
		t.Fatalf("save multipart part failed: %v", err)
	}
	session, err := storeA.GetMultipartSession(ctx, tenant, uploadID)
	if err != nil {
		t.Fatalf("get multipart session failed: %v", err)
	}
	if session.Headers["Content-Type"] != "application/octet-stream" || session.Parts[2].ETag != `"etag-2"` {
		t.Fatalf("session = %#v", session)
	}
	stale, err := storeB.ListStaleMultipartSessions(ctx, tenant, time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatalf("list stale sessions failed: %v", err)
	}
	if len(stale) != 1 || stale[0].UploadID != uploadID {
		t.Fatalf("stale = %#v, want upload %s", stale, uploadID)
	}
	if err := storeB.DeleteMultipartSession(ctx, tenant, uploadID); err != nil {
		t.Fatalf("delete multipart session failed: %v", err)
	}
	if _, err := storeA.GetMultipartSession(ctx, tenant, uploadID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted session err = %v, want not found", err)
	}
}

func openPostgresTestStore(t *testing.T, ctx context.Context) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("CHUNKGATE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set CHUNKGATE_POSTGRES_TEST_DSN to run postgres metadata integration tests")
	}
	store, err := NewPostgresStore(ctx, PostgresOptions{
		DSN:          dsn,
		MaxOpenConns: 8,
		MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("open postgres store failed: %v", err)
	}
	return store
}

func postgresTableExists(t *testing.T, ctx context.Context, store *PostgresStore, table string) bool {
	t.Helper()
	var exists bool
	if err := store.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		t.Fatalf("query to_regclass failed: %v", err)
	}
	return exists
}

func commitPostgresObject(t *testing.T, ctx context.Context, store *PostgresStore, tenant string, bucket string, key string, chunks []ChunkRef) {
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

func hasBlock(blocks []BlockRef, hash string) bool {
	for _, block := range blocks {
		if block.Hash == hash {
			return true
		}
	}
	return false
}

func hasTenant(tenants []string, tenant string) bool {
	for _, candidate := range tenants {
		if candidate == tenant {
			return true
		}
	}
	return false
}
