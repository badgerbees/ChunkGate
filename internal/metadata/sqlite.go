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
	headers, err := encodeHeaders(manifest.Headers)
	if err != nil {
		return "", err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, key, state, size, etag, headers_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		manifest.ID, manifest.Bucket, manifest.Key, manifest.State, manifest.Size, manifest.ETag, headers, formatTime(now), formatTime(now),
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

	var pending objectRow
	if err := tx.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects WHERE id = ?`, pendingID).Scan(
		&pending.ID, &pending.Bucket, &pending.Key, &pending.State, &pending.Size, &pending.ETag, &pending.HeadersJSON, &pending.CreatedAt, &pending.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("load pending object: %w", err)
	}
	if ObjectState(pending.State) != StatePending {
		return ErrNotFound
	}

	now := time.Now().UTC()
	if previous, ok, err := loadCommittedForUpdate(ctx, tx, manifest.Tenant, manifest.Bucket, manifest.Key); err != nil {
		return err
	} else if ok {
		if err := detachObjectChunks(ctx, tx, previous.ID, previous.Chunks); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ?, updated_at = ? WHERE id = ?`, StateDeleted, formatTime(now), previous.ID); err != nil {
			return fmt.Errorf("mark previous object deleted: %w", err)
		}
	}

	if err := attachObjectChunks(ctx, tx, pendingID, manifest.Chunks); err != nil {
		return err
	}

	headers, err := encodeHeaders(manifest.Headers)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE objects
		SET state = ?, size = ?, etag = ?, headers_json = ?, updated_at = ?
		WHERE id = ?`,
		StateCommitted, manifest.Size, manifest.ETag, headers, formatTime(now), pendingID,
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
	manifest, ok, err := loadCommittedObject(ctx, db, tenant, bucket, key)
	if err != nil {
		return ObjectManifest{}, err
	}
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
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

	manifest, ok, err := loadCommittedForUpdate(ctx, tx, tenant, bucket, key)
	if err != nil {
		return ObjectManifest{}, err
	}
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	if err := detachObjectChunks(ctx, tx, manifest.ID, manifest.Chunks); err != nil {
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
	return s.ListUnreferencedBlocksOlderThan(ctx, tenant, time.Now().UTC(), limit)
}

func (s *SQLiteStore) ListUnreferencedBlocksOlderThan(ctx context.Context, tenant string, before time.Time, limit int) ([]BlockRef, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT hash, size, updated_at FROM blocks WHERE ref_count = 0 AND updated_at <= ? ORDER BY updated_at ASC LIMIT ?`, formatTime(before), limit)
	if err != nil {
		return nil, fmt.Errorf("list unreferenced blocks: %w", err)
	}
	defer rows.Close()

	var blocks []BlockRef
	for rows.Next() {
		var block BlockRef
		var updatedAt string
		if err := rows.Scan(&block.Hash, &block.Size, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan unreferenced block: %w", err)
		}
		block.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
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

func (s *SQLiteStore) CreateMultipartSession(ctx context.Context, session MultipartSession) error {
	db, err := s.dbFor(ctx, session.Tenant)
	if err != nil {
		return err
	}
	if session.UploadID == "" {
		return fmt.Errorf("multipart upload id must not be empty")
	}
	if session.ReservedBytes < 0 {
		return fmt.Errorf("multipart reserved bytes must be >= 0")
	}
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	if session.UpdatedAt.IsZero() {
		session.UpdatedAt = session.CreatedAt
	}
	headers, err := encodeHeaders(session.Headers)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO multipart_sessions (upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		session.UploadID, session.Bucket, session.Key, headers, session.ReservedBytes, formatTime(session.CreatedAt), formatTime(session.UpdatedAt),
	); err != nil {
		return fmt.Errorf("insert multipart session: %w", err)
	}
	return nil
}

func (s *SQLiteStore) SaveMultipartPart(ctx context.Context, tenant string, uploadID string, part MultipartPart, reservedBytes int64) error {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return err
	}
	if part.Number <= 0 {
		return fmt.Errorf("multipart part number must be positive")
	}
	if part.Size < 0 || reservedBytes < 0 {
		return fmt.Errorf("multipart part size and reserved bytes must be >= 0")
	}
	if part.CreatedAt.IsZero() {
		part.CreatedAt = time.Now().UTC()
	}
	now := time.Now().UTC()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin multipart part save: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		UPDATE multipart_sessions
		SET reserved_bytes = ?, updated_at = ?
		WHERE upload_id = ?`,
		reservedBytes, formatTime(now), uploadID,
	)
	if err != nil {
		return fmt.Errorf("update multipart session: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO multipart_parts (upload_id, part_number, local_scratch_path, size_bytes, etag, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(upload_id, part_number) DO UPDATE SET
			local_scratch_path = excluded.local_scratch_path,
			size_bytes = excluded.size_bytes,
			etag = excluded.etag,
			created_at = excluded.created_at`,
		uploadID, part.Number, part.Path, part.Size, part.ETag, formatTime(part.CreatedAt),
	); err != nil {
		return fmt.Errorf("upsert multipart part: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit multipart part save: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetMultipartSession(ctx context.Context, tenant string, uploadID string) (MultipartSession, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return MultipartSession{}, err
	}
	session, ok, err := loadMultipartSession(ctx, db, tenant, uploadID)
	if err != nil {
		return MultipartSession{}, err
	}
	if !ok {
		return MultipartSession{}, ErrNotFound
	}
	return session, nil
}

func (s *SQLiteStore) ListMultipartSessions(ctx context.Context, tenant string) ([]MultipartSession, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return nil, err
	}
	return listMultipartSessions(ctx, db, tenant, "", nil)
}

func (s *SQLiteStore) ListStaleMultipartSessions(ctx context.Context, tenant string, before time.Time) ([]MultipartSession, error) {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return nil, err
	}
	return listMultipartSessions(ctx, db, tenant, `WHERE created_at < ?`, []any{formatTime(before)})
}

func (s *SQLiteStore) DeleteMultipartSession(ctx context.Context, tenant string, uploadID string) error {
	db, err := s.dbFor(ctx, tenant)
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `DELETE FROM multipart_sessions WHERE upload_id = ?`, uploadID)
	if err != nil {
		return fmt.Errorf("delete multipart session: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	return nil
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

func (s *SQLiteStore) HealthCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.resolver.Root, 0o755); err != nil {
		return fmt.Errorf("check sqlite metadata root: %w", err)
	}
	s.mu.Lock()
	dbs := make([]*sql.DB, 0, len(s.dbs))
	for _, db := range s.dbs {
		dbs = append(dbs, db)
	}
	s.mu.Unlock()
	for _, db := range dbs {
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("ping sqlite metadata shard: %w", err)
		}
	}
	return nil
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
	if err := runMigrations(ctx, db); err != nil {
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

type objectRow struct {
	ID          string
	Bucket      string
	Key         string
	State       string
	Size        int64
	ETag        string
	HeadersJSON string
	CreatedAt   string
	UpdatedAt   string
}

type multipartSessionRow struct {
	UploadID      string
	Bucket        string
	Key           string
	HeadersJSON   string
	ReservedBytes int64
	CreatedAt     string
	UpdatedAt     string
}

func loadCommittedObject(ctx context.Context, db *sql.DB, tenant string, bucket string, key string) (ObjectManifest, bool, error) {
	var row objectRow
	err := db.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects
		WHERE bucket = ? AND key = ? AND state = ?`,
		bucket, key, StateCommitted,
	).Scan(&row.ID, &row.Bucket, &row.Key, &row.State, &row.Size, &row.ETag, &row.HeadersJSON, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectManifest{}, false, nil
	}
	if err != nil {
		return ObjectManifest{}, false, fmt.Errorf("load object: %w", err)
	}
	manifest, err := row.toManifest(tenant)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	chunks, err := loadObjectChunks(ctx, db, row.ID)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	manifest.Chunks = chunks
	return manifest, true, nil
}

func loadCommittedForUpdate(ctx context.Context, tx *sql.Tx, tenant string, bucket string, key string) (ObjectManifest, bool, error) {
	var row objectRow
	err := tx.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects
		WHERE bucket = ? AND key = ? AND state = ?`,
		bucket, key, StateCommitted,
	).Scan(&row.ID, &row.Bucket, &row.Key, &row.State, &row.Size, &row.ETag, &row.HeadersJSON, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectManifest{}, false, nil
	}
	if err != nil {
		return ObjectManifest{}, false, fmt.Errorf("load committed object: %w", err)
	}
	manifest, err := row.toManifest(tenant)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	chunks, err := loadObjectChunksTx(ctx, tx, row.ID)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	manifest.Chunks = chunks
	return manifest, true, nil
}

func (r objectRow) toManifest(tenant string) (ObjectManifest, error) {
	headers, err := decodeHeaders(r.HeadersJSON)
	if err != nil {
		return ObjectManifest{}, err
	}
	createdAt, _ := time.Parse(time.RFC3339Nano, r.CreatedAt)
	updatedAt, _ := time.Parse(time.RFC3339Nano, r.UpdatedAt)
	return ObjectManifest{
		ID:        r.ID,
		Tenant:    tenant,
		Bucket:    r.Bucket,
		Key:       r.Key,
		State:     ObjectState(r.State),
		Size:      r.Size,
		ETag:      r.ETag,
		Headers:   headers,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func loadMultipartSession(ctx context.Context, db rowQuerier, tenant string, uploadID string) (MultipartSession, bool, error) {
	var row multipartSessionRow
	rows, err := db.QueryContext(ctx, `
		SELECT upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at
		FROM multipart_sessions
		WHERE upload_id = ?`, uploadID)
	if err != nil {
		return MultipartSession{}, false, fmt.Errorf("query multipart session: %w", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			rows.Close()
			return MultipartSession{}, false, fmt.Errorf("iterate multipart session: %w", err)
		}
		rows.Close()
		return MultipartSession{}, false, nil
	}
	if err := rows.Scan(&row.UploadID, &row.Bucket, &row.Key, &row.HeadersJSON, &row.ReservedBytes, &row.CreatedAt, &row.UpdatedAt); err != nil {
		rows.Close()
		return MultipartSession{}, false, fmt.Errorf("scan multipart session: %w", err)
	}
	if rows.Next() {
		rows.Close()
		return MultipartSession{}, false, fmt.Errorf("duplicate multipart session %s", uploadID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return MultipartSession{}, false, fmt.Errorf("iterate multipart session: %w", err)
	}
	if err := rows.Close(); err != nil {
		return MultipartSession{}, false, fmt.Errorf("close multipart session: %w", err)
	}
	session, err := row.toMultipartSession(tenant)
	if err != nil {
		return MultipartSession{}, false, err
	}
	parts, err := loadMultipartParts(ctx, db, uploadID)
	if err != nil {
		return MultipartSession{}, false, err
	}
	session.Parts = parts
	return session, true, nil
}

func listMultipartSessions(ctx context.Context, db rowQuerier, tenant string, where string, args []any) ([]MultipartSession, error) {
	query := `
		SELECT upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at
		FROM multipart_sessions ` + where + `
		ORDER BY created_at ASC`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list multipart sessions: %w", err)
	}

	var sessionRows []multipartSessionRow
	for rows.Next() {
		var row multipartSessionRow
		if err := rows.Scan(&row.UploadID, &row.Bucket, &row.Key, &row.HeadersJSON, &row.ReservedBytes, &row.CreatedAt, &row.UpdatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan multipart session: %w", err)
		}
		sessionRows = append(sessionRows, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate multipart sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close multipart sessions: %w", err)
	}

	sessions := make([]MultipartSession, 0, len(sessionRows))
	for _, row := range sessionRows {
		session, err := row.toMultipartSession(tenant)
		if err != nil {
			return nil, err
		}
		parts, err := loadMultipartParts(ctx, db, row.UploadID)
		if err != nil {
			return nil, err
		}
		session.Parts = parts
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (r multipartSessionRow) toMultipartSession(tenant string) (MultipartSession, error) {
	headers, err := decodeHeaders(r.HeadersJSON)
	if err != nil {
		return MultipartSession{}, err
	}
	createdAt, _ := time.Parse(time.RFC3339Nano, r.CreatedAt)
	updatedAt, _ := time.Parse(time.RFC3339Nano, r.UpdatedAt)
	return MultipartSession{
		UploadID:      r.UploadID,
		Tenant:        tenant,
		Bucket:        r.Bucket,
		Key:           r.Key,
		Headers:       headers,
		ReservedBytes: r.ReservedBytes,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
		Parts:         map[int]MultipartPart{},
	}, nil
}

func loadMultipartParts(ctx context.Context, db rowQuerier, uploadID string) (map[int]MultipartPart, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT part_number, local_scratch_path, size_bytes, etag, created_at
		FROM multipart_parts
		WHERE upload_id = ?
		ORDER BY part_number ASC`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("load multipart parts: %w", err)
	}
	defer rows.Close()

	parts := map[int]MultipartPart{}
	for rows.Next() {
		var part MultipartPart
		var createdAt string
		if err := rows.Scan(&part.Number, &part.Path, &part.Size, &part.ETag, &createdAt); err != nil {
			return nil, fmt.Errorf("scan multipart part: %w", err)
		}
		part.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		parts[part.Number] = part
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate multipart parts: %w", err)
	}
	return parts, nil
}

type rowQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadObjectChunks(ctx context.Context, db rowQuerier, objectID string) ([]ChunkRef, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT chunk_hash, chunk_offset, chunk_size, backend_key
		FROM object_chunks
		WHERE object_id = ?
		ORDER BY sequence_order ASC`, objectID)
	if err != nil {
		return nil, fmt.Errorf("load object chunks: %w", err)
	}
	defer rows.Close()

	var chunks []ChunkRef
	for rows.Next() {
		var chunk ChunkRef
		if err := rows.Scan(&chunk.Hash, &chunk.Offset, &chunk.Size, &chunk.BackendKey); err != nil {
			return nil, fmt.Errorf("scan object chunk: %w", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate object chunks: %w", err)
	}
	return chunks, nil
}

func loadObjectChunksTx(ctx context.Context, tx *sql.Tx, objectID string) ([]ChunkRef, error) {
	return loadObjectChunks(ctx, tx, objectID)
}

func attachObjectChunks(ctx context.Context, tx *sql.Tx, objectID string, chunks []ChunkRef) error {
	now := formatTime(time.Now().UTC())
	for sequence, chunk := range chunks {
		if chunk.Hash == "" {
			return fmt.Errorf("chunk %d has empty hash", sequence)
		}
		if chunk.Size < 0 || chunk.Offset < 0 {
			return fmt.Errorf("chunk %d has invalid offset or size", sequence)
		}
		backendKey := chunk.BackendKey
		if backendKey == "" {
			backendKey = defaultBackendKey(chunk.Hash)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO blocks (hash, backend_key, size, ref_count, created_at, updated_at)
			VALUES (?, ?, ?, 0, ?, ?)
			ON CONFLICT(hash) DO UPDATE SET
				backend_key = excluded.backend_key,
				size = excluded.size,
				updated_at = excluded.updated_at`,
			chunk.Hash, backendKey, chunk.Size, now, now,
		); err != nil {
			return fmt.Errorf("upsert block %s: %w", chunk.Hash, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO object_chunks (object_id, sequence_order, chunk_hash, chunk_offset, chunk_size, backend_key)
			VALUES (?, ?, ?, ?, ?, ?)`,
			objectID, sequence, chunk.Hash, chunk.Offset, chunk.Size, backendKey,
		); err != nil {
			return fmt.Errorf("insert object chunk %d: %w", sequence, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET ref_count = ref_count + 1,
				updated_at = ?
			WHERE hash = ?`,
			now, chunk.Hash,
		); err != nil {
			return fmt.Errorf("increment block ref %s: %w", chunk.Hash, err)
		}
	}
	return nil
}

func detachObjectChunks(ctx context.Context, tx *sql.Tx, objectID string, chunks []ChunkRef) error {
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE object_id = ?`, objectID); err != nil {
		return fmt.Errorf("delete object chunk rows: %w", err)
	}
	return nil
}

func encodeHeaders(headers map[string]string) (string, error) {
	if headers == nil {
		return "{}", nil
	}
	payload, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("marshal object headers: %w", err)
	}
	return string(payload), nil
}

func decodeHeaders(payload string) (map[string]string, error) {
	if payload == "" || payload == "{}" {
		return nil, nil
	}
	headers := map[string]string{}
	if err := json.Unmarshal([]byte(payload), &headers); err != nil {
		return nil, fmt.Errorf("decode object headers: %w", err)
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func defaultBackendKey(hash string) string {
	if len(hash) >= 2 {
		return "blocks/" + hash[:2] + "/" + hash
	}
	return "blocks/" + hash
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
