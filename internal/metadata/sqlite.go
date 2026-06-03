package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	resolver ShardResolver
	mu       sync.Mutex
	dbs      map[string]*sql.DB
}

func NewSQLiteStore(root string) (*SQLiteStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}
	return &SQLiteStore{
		resolver: ShardResolver{Root: root},
		dbs:      map[string]*sql.DB{},
	}, nil
}

func (s *SQLiteStore) CreatePendingObject(ctx context.Context, manifest ObjectManifest) (string, error) {
	db, err := s.dbFor(ctx, manifest.Tenant)
	if err != nil {
		return "", err
	}
	if manifest.ID == "" {
		manifest.ID = randomID()
	}
	now := time.Now().UTC()
	manifest.State = StatePending
	manifest.CreatedAt = now
	manifest.UpdatedAt = now
	payload, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal pending manifest: %w", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, key, state, size, etag, manifest_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		manifest.ID, manifest.Bucket, manifest.Key, manifest.State, manifest.Size, manifest.ETag, string(payload), formatTime(now), formatTime(now),
	)
	if err != nil {
		return "", fmt.Errorf("insert pending object: %w", err)
	}
	return manifest.ID, nil
}

func (s *SQLiteStore) CommitObject(ctx context.Context, pendingID string, manifest ObjectManifest) error {
	db, err := s.dbFor(ctx, manifest.Tenant)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata commit: %w", err)
	}
	defer tx.Rollback()

	var existingState string
	var createdAtRaw string
	if err := tx.QueryRowContext(ctx, `SELECT state, created_at FROM objects WHERE id = ?`, pendingID).Scan(&existingState, &createdAtRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("load pending object: %w", err)
	}
	if ObjectState(existingState) != StatePending {
		return ErrNotFound
	}

	now := time.Now().UTC()
	createdAt, _ := time.Parse(time.RFC3339Nano, createdAtRaw)
	if createdAt.IsZero() {
		createdAt = now
	}

	if previous, ok, err := loadCommittedForUpdate(ctx, tx, manifest.Bucket, manifest.Key); err != nil {
		return err
	} else if ok {
		if err := decrementRefs(ctx, tx, previous.Chunks); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ?, updated_at = ? WHERE id = ?`, StateDeleted, formatTime(now), previous.ID); err != nil {
			return fmt.Errorf("mark previous object deleted: %w", err)
		}
	}

	if err := incrementRefs(ctx, tx, manifest.Chunks); err != nil {
		return err
	}

	manifest.ID = pendingID
	manifest.State = StateCommitted
	manifest.CreatedAt = createdAt
	manifest.UpdatedAt = now
	payload, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal committed manifest: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE objects
		SET state = ?, size = ?, etag = ?, manifest_json = ?, updated_at = ?
		WHERE id = ?`,
		StateCommitted, manifest.Size, manifest.ETag, string(payload), formatTime(now), pendingID,
	); err != nil {
		return fmt.Errorf("commit object row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata transaction: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return ObjectManifest{}, err
	}
	var payload string
	err = db.QueryRowContext(ctx, `
		SELECT manifest_json FROM objects
		WHERE bucket = ? AND key = ? AND state = ?`,
		bucket, key, StateCommitted,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectManifest{}, ErrNotFound
	}
	if err != nil {
		return ObjectManifest{}, fmt.Errorf("load object: %w", err)
	}
	var manifest ObjectManifest
	if err := json.Unmarshal([]byte(payload), &manifest); err != nil {
		return ObjectManifest{}, fmt.Errorf("decode object manifest: %w", err)
	}
	manifest.Tenant = tenant
	return manifest, nil
}

func (s *SQLiteStore) DeleteObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return ObjectManifest{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ObjectManifest{}, fmt.Errorf("begin delete: %w", err)
	}
	defer tx.Rollback()

	manifest, ok, err := loadCommittedForUpdate(ctx, tx, bucket, key)
	if err != nil {
		return ObjectManifest{}, err
	}
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	if err := decrementRefs(ctx, tx, manifest.Chunks); err != nil {
		return ObjectManifest{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ?, updated_at = ? WHERE id = ?`, StateDeleted, formatTime(now), manifest.ID); err != nil {
		return ObjectManifest{}, fmt.Errorf("mark object deleted: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ObjectManifest{}, fmt.Errorf("commit delete: %w", err)
	}
	manifest.State = StateDeleted
	manifest.UpdatedAt = now
	return manifest, nil
}

func (s *SQLiteStore) ListUnreferencedBlocks(ctx context.Context, tenant string, limit int) ([]BlockRef, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT hash, size FROM blocks WHERE ref_count = 0 ORDER BY updated_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list unreferenced blocks: %w", err)
	}
	defer rows.Close()

	var blocks []BlockRef
	for rows.Next() {
		var block BlockRef
		if err := rows.Scan(&block.Hash, &block.Size); err != nil {
			return nil, fmt.Errorf("scan unreferenced block: %w", err)
		}
		blocks = append(blocks, block)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unreferenced blocks: %w", err)
	}
	return blocks, nil
}

func (s *SQLiteStore) ForgetBlocks(ctx context.Context, tenant string, hashes []string) error {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin forget blocks: %w", err)
	}
	defer tx.Rollback()
	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx, `DELETE FROM blocks WHERE hash = ? AND ref_count = 0`, hash); err != nil {
			return fmt.Errorf("forget block %s: %w", hash, err)
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) ListTenants(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	tenants := make([]string, 0, len(s.dbs))
	for tenant := range s.dbs {
		tenants = append(tenants, tenant)
	}
	s.mu.Unlock()

	paths, err := filepath.Glob(filepath.Join(s.resolver.Root, "tenant_*.db"))
	if err != nil {
		return nil, fmt.Errorf("glob tenant shards: %w", err)
	}
	seen := map[string]bool{}
	for _, tenant := range tenants {
		seen[tenant] = true
	}
	for _, path := range paths {
		name := filepath.Base(path)
		tenant := name[len("tenant_") : len(name)-len(".db")]
		if !seen[tenant] {
			tenants = append(tenants, tenant)
			seen[tenant] = true
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tenants, nil
}

func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var closeErr error
	for tenant, db := range s.dbs {
		if err := db.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close tenant %s: %w", tenant, err)
		}
	}
	return closeErr
}

func (s *SQLiteStore) dbFor(ctx context.Context, tenant string) (*sql.DB, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if db := s.dbs[safeTenant]; db != nil {
		s.mu.Unlock()
		return db, nil
	}
	s.mu.Unlock()

	path, err := s.resolver.Path(safeTenant)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create shard directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite shard: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.dbs[safeTenant]; existing != nil {
		db.Close()
		return existing, nil
	}
	s.dbs[safeTenant] = db
	return db, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE IF NOT EXISTS objects (
			id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'deleted')),
			size INTEGER NOT NULL DEFAULT 0,
			etag TEXT NOT NULL DEFAULT '',
			manifest_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS objects_committed_key
			ON objects(bucket, key) WHERE state = 'committed'`,
		`CREATE INDEX IF NOT EXISTS objects_state_updated
			ON objects(state, updated_at)`,
		`CREATE TABLE IF NOT EXISTS blocks (
			hash TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			ref_count INTEGER NOT NULL CHECK (ref_count >= 0),
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS blocks_ref_count
			ON blocks(ref_count, updated_at)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite store: %w", err)
		}
	}
	return nil
}

func loadCommittedForUpdate(ctx context.Context, tx *sql.Tx, bucket string, key string) (ObjectManifest, bool, error) {
	var payload string
	err := tx.QueryRowContext(ctx, `
		SELECT manifest_json FROM objects
		WHERE bucket = ? AND key = ? AND state = ?`,
		bucket, key, StateCommitted,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectManifest{}, false, nil
	}
	if err != nil {
		return ObjectManifest{}, false, fmt.Errorf("load committed object: %w", err)
	}
	var manifest ObjectManifest
	if err := json.Unmarshal([]byte(payload), &manifest); err != nil {
		return ObjectManifest{}, false, fmt.Errorf("decode committed object: %w", err)
	}
	return manifest, true, nil
}

func incrementRefs(ctx context.Context, tx *sql.Tx, chunks []ChunkRef) error {
	now := formatTime(time.Now().UTC())
	for _, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO blocks (hash, size, ref_count, updated_at)
			VALUES (?, ?, 1, ?)
			ON CONFLICT(hash) DO UPDATE SET
				ref_count = ref_count + 1,
				size = excluded.size,
				updated_at = excluded.updated_at`,
			chunk.Hash, chunk.Size, now,
		); err != nil {
			return fmt.Errorf("increment block ref %s: %w", chunk.Hash, err)
		}
	}
	return nil
}

func decrementRefs(ctx context.Context, tx *sql.Tx, chunks []ChunkRef) error {
	now := formatTime(time.Now().UTC())
	for _, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET ref_count = CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END,
				updated_at = ?
			WHERE hash = ?`,
			now, chunk.Hash,
		); err != nil {
			return fmt.Errorf("decrement block ref %s: %w", chunk.Hash, err)
		}
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
