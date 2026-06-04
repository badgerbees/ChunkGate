package backend

import (
	"context"
	"errors"
	"io"
)

var (
	ErrBlockNotFound      = errors.New("backend block not found")
	ErrBackendUnavailable = errors.New("backend unavailable")
)

type BlockStore interface {
	PutBlock(ctx context.Context, tenant string, hash string, data []byte) error
	GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error)
	DeleteBlocks(ctx context.Context, tenant string, hashes []string) error
}
