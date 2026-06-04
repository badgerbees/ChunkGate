package metadata

import (
	"context"
	"database/sql"
	"fmt"
)

type Migration struct {
	Version    int
	Name       string
	Statements []string
}

var sqliteMigrations = []Migration{
	{
		Version: 1,
		Name:    "structured metadata schema",
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS objects (
				id TEXT PRIMARY KEY,
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'deleted', 'failed')),
				size INTEGER NOT NULL DEFAULT 0 CHECK (size >= 0),
				etag TEXT NOT NULL DEFAULT '',
				headers_json TEXT NOT NULL DEFAULT '{}',
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS objects_committed_key
				ON objects(bucket, key) WHERE state = 'committed'`,
			`CREATE INDEX IF NOT EXISTS objects_state_updated
				ON objects(state, updated_at)`,
			`CREATE INDEX IF NOT EXISTS objects_bucket_key_state
				ON objects(bucket, key, state)`,
			`CREATE TABLE IF NOT EXISTS blocks (
				hash TEXT PRIMARY KEY,
				backend_key TEXT NOT NULL,
				size INTEGER NOT NULL CHECK (size >= 0),
				ref_count INTEGER NOT NULL CHECK (ref_count >= 0),
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS blocks_ref_count_updated
				ON blocks(ref_count, updated_at)`,
			`CREATE TABLE IF NOT EXISTS object_chunks (
				object_id TEXT NOT NULL,
				sequence_order INTEGER NOT NULL CHECK (sequence_order >= 0),
				chunk_hash TEXT NOT NULL,
				chunk_offset INTEGER NOT NULL CHECK (chunk_offset >= 0),
				chunk_size INTEGER NOT NULL CHECK (chunk_size >= 0),
				backend_key TEXT NOT NULL,
				PRIMARY KEY (object_id, sequence_order),
				FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
				FOREIGN KEY (chunk_hash) REFERENCES blocks(hash)
			)`,
			`CREATE INDEX IF NOT EXISTS object_chunks_hash
				ON object_chunks(chunk_hash)`,
			`CREATE INDEX IF NOT EXISTS object_chunks_object_offset
				ON object_chunks(object_id, chunk_offset)`,
			`CREATE TABLE IF NOT EXISTS multipart_sessions (
				upload_id TEXT PRIMARY KEY,
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				headers_json TEXT NOT NULL DEFAULT '{}',
				reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK (reserved_bytes >= 0),
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS multipart_sessions_created
				ON multipart_sessions(created_at)`,
			`CREATE TABLE IF NOT EXISTS multipart_parts (
				upload_id TEXT NOT NULL,
				part_number INTEGER NOT NULL CHECK (part_number > 0),
				local_scratch_path TEXT NOT NULL,
				size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
				etag TEXT NOT NULL,
				created_at TEXT NOT NULL,
				PRIMARY KEY (upload_id, part_number),
				FOREIGN KEY (upload_id) REFERENCES multipart_sessions(upload_id) ON DELETE CASCADE
			)`,
		},
	},
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure sqlite: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		return fmt.Errorf("create schema migrations table: %w", err)
	}

	applied, err := appliedMigrations(ctx, db)
	if err != nil {
		return err
	}
	for _, migration := range sqliteMigrations {
		if applied[migration.Version] {
			continue
		}
		if err := applyMigration(ctx, db, migration); err != nil {
			return err
		}
	}
	return nil
}

func appliedMigrations(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()

	applied := map[int]bool{}
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func applyMigration(ctx context.Context, db *sql.DB, migration Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", migration.Version, err)
	}
	defer tx.Rollback()

	for _, statement := range migration.Statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply migration %d %s: %w", migration.Version, migration.Name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name) VALUES (?, ?)`, migration.Version, migration.Name); err != nil {
		return fmt.Errorf("record migration %d: %w", migration.Version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %d: %w", migration.Version, err)
	}
	return nil
}
