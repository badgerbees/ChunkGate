package backend

import (
	"context"
	"io"
)

type BlockStore interface {
	PutBlock(ctx context.Context, tenant string, hash string, data []byte) error
	GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error)
	DeleteBlocks(ctx context.Context, tenant string, hashes []string) error
}
