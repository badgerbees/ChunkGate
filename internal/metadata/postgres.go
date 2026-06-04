package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultPostgresMaxOpenConns = 16

type PostgresOptions struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(ctx context.Context, options PostgresOptions) (*PostgresStore, error) {
	if options.DSN == "" {
		return nil, fmt.Errorf("postgres dsn must not be empty")
	}
	db, err := sql.Open("pgx", options.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres metadata store: %w", err)
	}
	maxOpen := options.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defaultPostgresMaxOpenConns
	}
	db.SetMaxOpenConns(maxOpen)
	if options.MaxIdleConns > 0 {
		db.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(options.ConnMaxLifetime)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect postgres metadata store: %w", err)
	}
	if err := runPostgresMigrations(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) CreatePendingObject(ctx context.Context, manifest ObjectManifest) (string, error) {
	tenant, err := SafeTenantID(manifest.Tenant)
	if err != nil {
		return "", err
	}
	if manifest.ID == "" {
		manifest.ID = randomID()
	}
	now := time.Now().UTC()
	manifest.State = StatePending
	headers, err := encodeHeaders(manifest.Headers)
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO objects (tenant_id, id, bucket, key, state, size, etag, headers_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		tenant, manifest.ID, manifest.Bucket, manifest.Key, manifest.State, manifest.Size, manifest.ETag, headers, now, now,
	)
	if err != nil {
		return "", fmt.Errorf("insert pending object: %w", err)
	}
	return manifest.ID, nil
}

func (s *PostgresStore) CommitObject(ctx context.Context, pendingID string, manifest ObjectManifest) error {
	tenant, err := SafeTenantID(manifest.Tenant)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata commit: %w", err)
	}
	defer tx.Rollback()

	var pending postgresObjectRow
	err = tx.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects
		WHERE tenant_id = $1 AND id = $2
		FOR UPDATE`,
		tenant, pendingID,
	).Scan(&pending.ID, &pending.Bucket, &pending.Key, &pending.State, &pending.Size, &pending.ETag, &pending.HeadersJSON, &pending.CreatedAt, &pending.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("load pending object: %w", err)
	}
	if ObjectState(pending.State) != StatePending {
		return ErrNotFound
	}

	now := time.Now().UTC()
	if previous, ok, err := postgresLoadCommittedForUpdate(ctx, tx, tenant, manifest.Bucket, manifest.Key); err != nil {
		return err
	} else if ok {
		if err := postgresDetachObjectChunks(ctx, tx, tenant, previous.ID, previous.Chunks); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = $1, updated_at = $2 WHERE tenant_id = $3 AND id = $4`, StateDeleted, now, tenant, previous.ID); err != nil {
			return fmt.Errorf("mark previous object deleted: %w", err)
		}
	}

	if err := postgresAttachObjectChunks(ctx, tx, tenant, pendingID, manifest.Chunks); err != nil {
		return err
	}

	headers, err := encodeHeaders(manifest.Headers)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE objects
		SET bucket = $1, key = $2, state = $3, size = $4, etag = $5, headers_json = $6, updated_at = $7
		WHERE tenant_id = $8 AND id = $9`,
		manifest.Bucket, manifest.Key, StateCommitted, manifest.Size, manifest.ETag, headers, now, tenant, pendingID,
	); err != nil {
		return fmt.Errorf("commit object row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata transaction: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return ObjectManifest{}, err
	}
	manifest, ok, err := postgresLoadCommittedObject(ctx, s.db, safeTenant, bucket, key)
	if err != nil {
		return ObjectManifest{}, err
	}
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	return manifest, nil
}

func (s *PostgresStore) DeleteObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return ObjectManifest{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ObjectManifest{}, fmt.Errorf("begin delete: %w", err)
	}
	defer tx.Rollback()

	manifest, ok, err := postgresLoadCommittedForUpdate(ctx, tx, safeTenant, bucket, key)
	if err != nil {
		return ObjectManifest{}, err
	}
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	if err := postgresDetachObjectChunks(ctx, tx, safeTenant, manifest.ID, manifest.Chunks); err != nil {
		return ObjectManifest{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = $1, updated_at = $2 WHERE tenant_id = $3 AND id = $4`, StateDeleted, now, safeTenant, manifest.ID); err != nil {
		return ObjectManifest{}, fmt.Errorf("mark object deleted: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ObjectManifest{}, fmt.Errorf("commit delete: %w", err)
	}
	manifest.State = StateDeleted
	manifest.UpdatedAt = now
	return manifest, nil
}

func (s *PostgresStore) ListUnreferencedBlocks(ctx context.Context, tenant string, limit int) ([]BlockRef, error) {
	return s.ListUnreferencedBlocksOlderThan(ctx, tenant, time.Now().UTC(), limit)
}

func (s *PostgresStore) ListUnreferencedBlocksOlderThan(ctx context.Context, tenant string, before time.Time, limit int) ([]BlockRef, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT hash, size, updated_at
		FROM blocks
		WHERE tenant_id = $1 AND ref_count = 0 AND updated_at <= $2
		ORDER BY updated_at ASC
		LIMIT $3`,
		safeTenant, before.UTC(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list unreferenced blocks: %w", err)
	}
	defer rows.Close()

	var blocks []BlockRef
	for rows.Next() {
		var block BlockRef
		if err := rows.Scan(&block.Hash, &block.Size, &block.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan unreferenced block: %w", err)
		}
		blocks = append(blocks, block)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unreferenced blocks: %w", err)
	}
	return blocks, nil
}

func (s *PostgresStore) ForgetBlocks(ctx context.Context, tenant string, hashes []string) error {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin forget blocks: %w", err)
	}
	defer tx.Rollback()
	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx, `DELETE FROM blocks WHERE tenant_id = $1 AND hash = $2 AND ref_count = 0`, safeTenant, hash); err != nil {
			return fmt.Errorf("forget block %s: %w", hash, err)
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) ListTenants(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT tenant_id FROM objects
		UNION
		SELECT tenant_id FROM blocks
		UNION
		SELECT tenant_id FROM multipart_sessions
		ORDER BY tenant_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	var tenants []string
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}
	return tenants, nil
}

func (s *PostgresStore) CreateMultipartSession(ctx context.Context, session MultipartSession) error {
	tenant, err := SafeTenantID(session.Tenant)
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
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO multipart_sessions (tenant_id, upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tenant, session.UploadID, session.Bucket, session.Key, headers, session.ReservedBytes, session.CreatedAt.UTC(), session.UpdatedAt.UTC(),
	); err != nil {
		return fmt.Errorf("insert multipart session: %w", err)
	}
	return nil
}

func (s *PostgresStore) SaveMultipartPart(ctx context.Context, tenant string, uploadID string, part MultipartPart, reservedBytes int64) error {
	safeTenant, err := SafeTenantID(tenant)
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin multipart part save: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		UPDATE multipart_sessions
		SET reserved_bytes = $1, updated_at = $2
		WHERE tenant_id = $3 AND upload_id = $4`,
		reservedBytes, now, safeTenant, uploadID,
	)
	if err != nil {
		return fmt.Errorf("update multipart session: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO multipart_parts (tenant_id, upload_id, part_number, local_scratch_path, size_bytes, etag, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, upload_id, part_number) DO UPDATE SET
			local_scratch_path = EXCLUDED.local_scratch_path,
			size_bytes = EXCLUDED.size_bytes,
			etag = EXCLUDED.etag,
			created_at = EXCLUDED.created_at`,
		safeTenant, uploadID, part.Number, part.Path, part.Size, part.ETag, part.CreatedAt.UTC(),
	); err != nil {
		return fmt.Errorf("upsert multipart part: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit multipart part save: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetMultipartSession(ctx context.Context, tenant string, uploadID string) (MultipartSession, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return MultipartSession{}, err
	}
	session, ok, err := postgresLoadMultipartSession(ctx, s.db, safeTenant, uploadID)
	if err != nil {
		return MultipartSession{}, err
	}
	if !ok {
		return MultipartSession{}, ErrNotFound
	}
	return session, nil
}

func (s *PostgresStore) ListMultipartSessions(ctx context.Context, tenant string) ([]MultipartSession, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return nil, err
	}
	return postgresListMultipartSessions(ctx, s.db, safeTenant, "", nil)
}

func (s *PostgresStore) ListStaleMultipartSessions(ctx context.Context, tenant string, before time.Time) ([]MultipartSession, error) {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return nil, err
	}
	return postgresListMultipartSessions(ctx, s.db, safeTenant, `AND created_at < $2`, []any{before.UTC()})
}

func (s *PostgresStore) DeleteMultipartSession(ctx context.Context, tenant string, uploadID string) error {
	safeTenant, err := SafeTenantID(tenant)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM multipart_sessions WHERE tenant_id = $1 AND upload_id = $2`, safeTenant, uploadID)
	if err != nil {
		return fmt.Errorf("delete multipart session: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) HealthCheck(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres metadata store: %w", err)
	}
	return nil
}

type postgresObjectRow struct {
	ID          string
	Bucket      string
	Key         string
	State       string
	Size        int64
	ETag        string
	HeadersJSON string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type postgresMultipartSessionRow struct {
	UploadID      string
	Bucket        string
	Key           string
	HeadersJSON   string
	ReservedBytes int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func postgresLoadCommittedObject(ctx context.Context, db rowQuerier, tenant string, bucket string, key string) (ObjectManifest, bool, error) {
	var row postgresObjectRow
	err := db.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects
		WHERE tenant_id = $1 AND bucket = $2 AND key = $3 AND state = $4`,
		tenant, bucket, key, StateCommitted,
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
	chunks, err := postgresLoadObjectChunks(ctx, db, tenant, row.ID)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	manifest.Chunks = chunks
	return manifest, true, nil
}

func postgresLoadCommittedForUpdate(ctx context.Context, tx *sql.Tx, tenant string, bucket string, key string) (ObjectManifest, bool, error) {
	var row postgresObjectRow
	err := tx.QueryRowContext(ctx, `
		SELECT id, bucket, key, state, size, etag, headers_json, created_at, updated_at
		FROM objects
		WHERE tenant_id = $1 AND bucket = $2 AND key = $3 AND state = $4
		FOR UPDATE`,
		tenant, bucket, key, StateCommitted,
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
	chunks, err := postgresLoadObjectChunks(ctx, tx, tenant, row.ID)
	if err != nil {
		return ObjectManifest{}, false, err
	}
	manifest.Chunks = chunks
	return manifest, true, nil
}

func (r postgresObjectRow) toManifest(tenant string) (ObjectManifest, error) {
	headers, err := decodeHeaders(r.HeadersJSON)
	if err != nil {
		return ObjectManifest{}, err
	}
	return ObjectManifest{
		ID:        r.ID,
		Tenant:    tenant,
		Bucket:    r.Bucket,
		Key:       r.Key,
		State:     ObjectState(r.State),
		Size:      r.Size,
		ETag:      r.ETag,
		Headers:   headers,
		CreatedAt: r.CreatedAt.UTC(),
		UpdatedAt: r.UpdatedAt.UTC(),
	}, nil
}

func postgresLoadMultipartSession(ctx context.Context, db rowQuerier, tenant string, uploadID string) (MultipartSession, bool, error) {
	var row postgresMultipartSessionRow
	err := db.QueryRowContext(ctx, `
		SELECT upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at
		FROM multipart_sessions
		WHERE tenant_id = $1 AND upload_id = $2`,
		tenant, uploadID,
	).Scan(&row.UploadID, &row.Bucket, &row.Key, &row.HeadersJSON, &row.ReservedBytes, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return MultipartSession{}, false, nil
	}
	if err != nil {
		return MultipartSession{}, false, fmt.Errorf("load multipart session: %w", err)
	}
	session, err := row.toMultipartSession(tenant)
	if err != nil {
		return MultipartSession{}, false, err
	}
	parts, err := postgresLoadMultipartParts(ctx, db, tenant, uploadID)
	if err != nil {
		return MultipartSession{}, false, err
	}
	session.Parts = parts
	return session, true, nil
}

func postgresListMultipartSessions(ctx context.Context, db rowQuerier, tenant string, extraWhere string, extraArgs []any) ([]MultipartSession, error) {
	args := append([]any{tenant}, extraArgs...)
	rows, err := db.QueryContext(ctx, `
		SELECT upload_id, bucket, key, headers_json, reserved_bytes, created_at, updated_at
		FROM multipart_sessions
		WHERE tenant_id = $1 `+extraWhere+`
		ORDER BY created_at ASC`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("list multipart sessions: %w", err)
	}

	var sessionRows []postgresMultipartSessionRow
	for rows.Next() {
		var row postgresMultipartSessionRow
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
		parts, err := postgresLoadMultipartParts(ctx, db, tenant, row.UploadID)
		if err != nil {
			return nil, err
		}
		session.Parts = parts
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (r postgresMultipartSessionRow) toMultipartSession(tenant string) (MultipartSession, error) {
	headers, err := decodeHeaders(r.HeadersJSON)
	if err != nil {
		return MultipartSession{}, err
	}
	return MultipartSession{
		UploadID:      r.UploadID,
		Tenant:        tenant,
		Bucket:        r.Bucket,
		Key:           r.Key,
		Headers:       headers,
		ReservedBytes: r.ReservedBytes,
		CreatedAt:     r.CreatedAt.UTC(),
		UpdatedAt:     r.UpdatedAt.UTC(),
		Parts:         map[int]MultipartPart{},
	}, nil
}

func postgresLoadMultipartParts(ctx context.Context, db rowQuerier, tenant string, uploadID string) (map[int]MultipartPart, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT part_number, local_scratch_path, size_bytes, etag, created_at
		FROM multipart_parts
		WHERE tenant_id = $1 AND upload_id = $2
		ORDER BY part_number ASC`,
		tenant, uploadID,
	)
	if err != nil {
		return nil, fmt.Errorf("load multipart parts: %w", err)
	}
	defer rows.Close()

	parts := map[int]MultipartPart{}
	for rows.Next() {
		var part MultipartPart
		if err := rows.Scan(&part.Number, &part.Path, &part.Size, &part.ETag, &part.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan multipart part: %w", err)
		}
		part.CreatedAt = part.CreatedAt.UTC()
		parts[part.Number] = part
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate multipart parts: %w", err)
	}
	return parts, nil
}

func postgresLoadObjectChunks(ctx context.Context, db rowQuerier, tenant string, objectID string) ([]ChunkRef, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT chunk_hash, chunk_offset, chunk_size, backend_key
		FROM object_chunks
		WHERE tenant_id = $1 AND object_id = $2
		ORDER BY sequence_order ASC`,
		tenant, objectID,
	)
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

func postgresAttachObjectChunks(ctx context.Context, tx *sql.Tx, tenant string, objectID string, chunks []ChunkRef) error {
	now := time.Now().UTC()
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
			INSERT INTO blocks (tenant_id, hash, backend_key, size, ref_count, created_at, updated_at)
			VALUES ($1, $2, $3, $4, 0, $5, $6)
			ON CONFLICT (tenant_id, hash) DO UPDATE SET
				backend_key = EXCLUDED.backend_key,
				size = EXCLUDED.size,
				updated_at = EXCLUDED.updated_at`,
			tenant, chunk.Hash, backendKey, chunk.Size, now, now,
		); err != nil {
			return fmt.Errorf("upsert block %s: %w", chunk.Hash, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO object_chunks (tenant_id, object_id, sequence_order, chunk_hash, chunk_offset, chunk_size, backend_key)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tenant, objectID, sequence, chunk.Hash, chunk.Offset, chunk.Size, backendKey,
		); err != nil {
			return fmt.Errorf("insert object chunk %d: %w", sequence, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET ref_count = ref_count + 1,
				updated_at = $1
			WHERE tenant_id = $2 AND hash = $3`,
			now, tenant, chunk.Hash,
		); err != nil {
			return fmt.Errorf("increment block ref %s: %w", chunk.Hash, err)
		}
	}
	return nil
}

func postgresDetachObjectChunks(ctx context.Context, tx *sql.Tx, tenant string, objectID string, chunks []ChunkRef) error {
	now := time.Now().UTC()
	for _, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET ref_count = CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END,
				updated_at = $1
			WHERE tenant_id = $2 AND hash = $3`,
			now, tenant, chunk.Hash,
		); err != nil {
			return fmt.Errorf("decrement block ref %s: %w", chunk.Hash, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE tenant_id = $1 AND object_id = $2`, tenant, objectID); err != nil {
		return fmt.Errorf("delete object chunk rows: %w", err)
	}
	return nil
}
