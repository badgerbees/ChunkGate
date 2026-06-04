package metadata

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("metadata not found")
	ErrInvalidTenant = errors.New("invalid tenant")
)

type ObjectState string

const (
	StatePending   ObjectState = "pending"
	StateCommitted ObjectState = "committed"
	StateDeleted   ObjectState = "deleted"
	StateFailed    ObjectState = "failed"
)

type ChunkRef struct {
	Hash       string `json:"hash"`
	Offset     int64  `json:"offset"`
	Size       int64  `json:"size"`
	BackendKey string `json:"backend_key,omitempty"`
}

type ObjectManifest struct {
	ID        string            `json:"id"`
	Tenant    string            `json:"tenant"`
	Bucket    string            `json:"bucket"`
	Key       string            `json:"key"`
	State     ObjectState       `json:"state"`
	Size      int64             `json:"size"`
	ETag      string            `json:"etag"`
	Headers   map[string]string `json:"headers,omitempty"`
	Chunks    []ChunkRef        `json:"chunks"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type BlockRef struct {
	Hash      string
	Size      int64
	UpdatedAt time.Time
}

type MultipartSession struct {
	UploadID      string
	Tenant        string
	Bucket        string
	Key           string
	Headers       map[string]string
	ReservedBytes int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Parts         map[int]MultipartPart
}

type MultipartPart struct {
	Number    int
	Size      int64
	ETag      string
	Path      string
	CreatedAt time.Time
}

type Store interface {
	CreatePendingObject(ctx context.Context, manifest ObjectManifest) (string, error)
	CommitObject(ctx context.Context, pendingID string, manifest ObjectManifest) error
	GetObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error)
	DeleteObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error)
	ListUnreferencedBlocks(ctx context.Context, tenant string, limit int) ([]BlockRef, error)
	ListUnreferencedBlocksOlderThan(ctx context.Context, tenant string, before time.Time, limit int) ([]BlockRef, error)
	ForgetBlocks(ctx context.Context, tenant string, hashes []string) error
	ListTenants(ctx context.Context) ([]string, error)
	CreateMultipartSession(ctx context.Context, session MultipartSession) error
	SaveMultipartPart(ctx context.Context, tenant string, uploadID string, part MultipartPart, reservedBytes int64) error
	GetMultipartSession(ctx context.Context, tenant string, uploadID string) (MultipartSession, error)
	ListMultipartSessions(ctx context.Context, tenant string) ([]MultipartSession, error)
	ListStaleMultipartSessions(ctx context.Context, tenant string, before time.Time) ([]MultipartSession, error)
	DeleteMultipartSession(ctx context.Context, tenant string, uploadID string) error
	Close() error
}
