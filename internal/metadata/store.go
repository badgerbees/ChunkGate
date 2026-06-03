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
)

type ChunkRef struct {
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
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
	Hash string
	Size int64
}

type Store interface {
	CreatePendingObject(ctx context.Context, manifest ObjectManifest) (string, error)
	CommitObject(ctx context.Context, pendingID string, manifest ObjectManifest) error
	GetObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error)
	DeleteObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error)
	ListUnreferencedBlocks(ctx context.Context, tenant string, limit int) ([]BlockRef, error)
	ForgetBlocks(ctx context.Context, tenant string, hashes []string) error
	ListTenants(ctx context.Context) ([]string, error)
	Close() error
}
