package gc

import (
	"context"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

type Sweeper struct {
	Store   metadata.Store
	Backend backend.BlockStore
	Limit   int
}

func (s Sweeper) SweepOnce(ctx context.Context) (int, error) {
	tenants, err := s.Store.ListTenants(ctx)
	if err != nil {
		return 0, err
	}
	limit := s.Limit
	if limit <= 0 {
		limit = 1000
	}

	var deleted int
	for _, tenant := range tenants {
		blocks, err := s.Store.ListUnreferencedBlocks(ctx, tenant, limit)
		if err != nil {
			return deleted, err
		}
		hashes := make([]string, 0, len(blocks))
		for _, block := range blocks {
			hashes = append(hashes, block.Hash)
		}
		if len(hashes) == 0 {
			continue
		}
		if err := s.Backend.DeleteBlocks(ctx, tenant, hashes); err != nil {
			return deleted, err
		}
		if err := s.Store.ForgetBlocks(ctx, tenant, hashes); err != nil {
			return deleted, err
		}
		deleted += len(hashes)
	}
	return deleted, nil
}
