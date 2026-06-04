package object

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/ops"
)

type Config struct {
	Chunker *chunker.Splitter
	Backend backend.BlockStore
	Store   metadata.Store
	CPU     limits.ConcurrencyLimiter
	Metrics *ops.Metrics
}

type Service struct {
	chunker *chunker.Splitter
	backend backend.BlockStore
	store   metadata.Store
	cpu     limits.ConcurrencyLimiter
	metrics *ops.Metrics
}

type ByteRange struct {
	Start int64
	End   int64
}

type PutOptions struct {
	Headers map[string]string
}

type PutOption func(*PutOptions)

func WithHeaders(headers map[string]string) PutOption {
	return func(options *PutOptions) {
		options.Headers = cloneStringMap(headers)
	}
}

func NewService(config Config) *Service {
	return &Service{
		chunker: config.Chunker,
		backend: config.Backend,
		store:   config.Store,
		cpu:     config.CPU,
		metrics: config.Metrics,
	}
}

func (s *Service) Put(ctx context.Context, tenant string, bucket string, key string, body io.Reader, options ...PutOption) (metadata.ObjectManifest, error) {
	putOptions := collectPutOptions(options)
	release := func() {}
	if s.cpu != nil {
		var err error
		release, err = s.cpu.Acquire(ctx)
		if err != nil {
			return metadata.ObjectManifest{}, err
		}
	}
	defer release()

	pendingID, err := s.store.CreatePendingObject(ctx, metadata.ObjectManifest{
		Tenant: tenant,
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return metadata.ObjectManifest{}, err
	}

	fullMD5 := md5.New()
	refs := make([]metadata.ChunkRef, 0)
	var size int64
	var chunks int64
	seenBlocks := map[string]bool{}
	err = s.chunker.Stream(ctx, io.TeeReader(body, fullMD5), func(chunk chunker.Chunk) error {
		hash := sha256.Sum256(chunk.Data)
		blockID := hex.EncodeToString(hash[:])
		if !seenBlocks[blockID] {
			exists, err := s.hasBlock(ctx, tenant, blockID)
			if err != nil {
				return err
			}
			if !exists {
				if err := s.backend.PutBlock(ctx, tenant, blockID, chunk.Data); err != nil {
					return err
				}
			}
			seenBlocks[blockID] = true
		}
		ref := metadata.ChunkRef{
			Hash:   blockID,
			Offset: chunk.Offset,
			Size:   int64(len(chunk.Data)),
		}
		size += ref.Size
		chunks++
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return metadata.ObjectManifest{}, err
	}

	manifest := metadata.ObjectManifest{
		Tenant:  tenant,
		Bucket:  bucket,
		Key:     key,
		Size:    size,
		ETag:    `"` + hex.EncodeToString(fullMD5.Sum(nil)) + `"`,
		Headers: putOptions.Headers,
		Chunks:  refs,
	}
	if err := s.store.CommitObject(ctx, pendingID, manifest); err != nil {
		return metadata.ObjectManifest{}, err
	}
	if s.metrics != nil {
		s.metrics.ObserveUpload(size, chunks)
	}
	return s.store.GetObject(ctx, tenant, bucket, key)
}

type blockExistenceChecker interface {
	HasBlock(ctx context.Context, tenant string, hash string) (bool, error)
}

func (s *Service) hasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	checker, ok := s.backend.(blockExistenceChecker)
	if !ok {
		return false, nil
	}
	return checker.HasBlock(ctx, tenant, hash)
}

func (s *Service) Open(ctx context.Context, tenant string, bucket string, key string) (metadata.ObjectManifest, io.ReadCloser, error) {
	manifest, err := s.store.GetObject(ctx, tenant, bucket, key)
	if err != nil {
		return metadata.ObjectManifest{}, nil, err
	}
	reader, err := s.openChunks(ctx, tenant, manifest, nil)
	if err != nil {
		return metadata.ObjectManifest{}, nil, err
	}
	return manifest, reader, nil
}

func (s *Service) OpenRange(ctx context.Context, tenant string, bucket string, key string, byteRange ByteRange) (metadata.ObjectManifest, io.ReadCloser, error) {
	manifest, err := s.store.GetObject(ctx, tenant, bucket, key)
	if err != nil {
		return metadata.ObjectManifest{}, nil, err
	}
	reader, err := s.openChunks(ctx, tenant, manifest, &byteRange)
	if err != nil {
		return metadata.ObjectManifest{}, nil, err
	}
	return manifest, reader, nil
}

func (s *Service) Stat(ctx context.Context, tenant string, bucket string, key string) (metadata.ObjectManifest, error) {
	return s.store.GetObject(ctx, tenant, bucket, key)
}

func (s *Service) openChunks(ctx context.Context, tenant string, manifest metadata.ObjectManifest, byteRange *ByteRange) (io.ReadCloser, error) {
	readers := make([]io.Reader, 0, len(manifest.Chunks))
	closers := make([]io.Closer, 0, len(manifest.Chunks))
	for _, chunk := range manifest.Chunks {
		readStart := int64(0)
		readEnd := chunk.Size - 1
		if byteRange != nil {
			chunkStart := chunk.Offset
			chunkEnd := chunk.Offset + chunk.Size - 1
			if chunk.Size == 0 || chunkEnd < byteRange.Start || chunkStart > byteRange.End {
				continue
			}
			if byteRange.Start > chunkStart {
				readStart = byteRange.Start - chunkStart
			}
			if byteRange.End < chunkEnd {
				readEnd = byteRange.End - chunkStart
			}
		}
		reader, err := s.backend.GetBlock(ctx, tenant, chunk.Hash)
		if err != nil {
			closeAll(closers)
			return nil, err
		}
		if readStart != 0 || readEnd != chunk.Size-1 {
			data, err := io.ReadAll(reader)
			closeErr := reader.Close()
			if err != nil {
				closeAll(closers)
				return nil, fmt.Errorf("read range chunk: %w", err)
			}
			if closeErr != nil {
				closeAll(closers)
				return nil, closeErr
			}
			if readStart > int64(len(data)) || readEnd >= int64(len(data)) || readStart > readEnd {
				closeAll(closers)
				return nil, fmt.Errorf("invalid chunk range for %s", chunk.Hash)
			}
			readers = append(readers, bytes.NewReader(data[readStart:readEnd+1]))
			continue
		}
		readers = append(readers, reader)
		closers = append(closers, reader)
	}
	return multiReadCloser{Reader: io.MultiReader(readers...), closers: closers}, nil
}

func (s *Service) Delete(ctx context.Context, tenant string, bucket string, key string) (metadata.ObjectManifest, error) {
	return s.store.DeleteObject(ctx, tenant, bucket, key)
}

func (s *Service) Store() metadata.Store {
	return s.store
}

func (s *Service) Backend() backend.BlockStore {
	return s.backend
}

type multiReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (m multiReadCloser) Close() error {
	return closeAll(m.closers)
}

func closeAll(closers []io.Closer) error {
	var first error
	for _, closer := range closers {
		if err := closer.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func ReadAll(ctx context.Context, reader io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return nil, fmt.Errorf("read object: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func collectPutOptions(options []PutOption) PutOptions {
	var collected PutOptions
	for _, option := range options {
		if option != nil {
			option(&collected)
		}
	}
	if collected.Headers != nil {
		collected.Headers = cloneStringMap(collected.Headers)
	}
	return collected
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
