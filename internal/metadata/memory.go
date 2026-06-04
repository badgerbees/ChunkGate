package metadata

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type MemoryStore struct {
	mu        sync.Mutex
	objects   map[string]ObjectManifest
	index     map[string]string
	blocks    map[string]map[string]BlockRefCount
	multipart map[string]map[string]MultipartSession
}

type BlockRefCount struct {
	Size      int64
	RefCount  int
	UpdatedAt time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects:   map[string]ObjectManifest{},
		index:     map[string]string{},
		blocks:    map[string]map[string]BlockRefCount{},
		multipart: map[string]map[string]MultipartSession{},
	}
}

func (s *MemoryStore) CreatePendingObject(ctx context.Context, manifest ObjectManifest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if manifest.ID == "" {
		manifest.ID = randomID()
	}
	now := time.Now().UTC()
	manifest.State = StatePending
	manifest.CreatedAt = now
	manifest.UpdatedAt = now
	s.objects[manifest.ID] = manifest
	s.ensureTenant(manifest.Tenant)
	return manifest.ID, nil
}

func (s *MemoryStore) CommitObject(ctx context.Context, pendingID string, manifest ObjectManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, ok := s.objects[pendingID]
	if !ok || pending.State != StatePending {
		return ErrNotFound
	}
	manifest.ID = pendingID
	manifest.State = StateCommitted
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = pending.CreatedAt
	}
	manifest.UpdatedAt = time.Now().UTC()

	key := objectKey(manifest.Tenant, manifest.Bucket, manifest.Key)
	if previousID, ok := s.index[key]; ok {
		previous := s.objects[previousID]
		s.decrementLocked(previous.Tenant, previous.Chunks)
		previous.State = StateDeleted
		previous.UpdatedAt = manifest.UpdatedAt
		s.objects[previousID] = previous
	}

	s.ensureTenant(manifest.Tenant)
	s.incrementLocked(manifest.Tenant, manifest.Chunks)
	s.objects[pendingID] = manifest
	s.index[key] = pendingID
	return nil
}

func (s *MemoryStore) GetObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	if err := ctx.Err(); err != nil {
		return ObjectManifest{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	id, ok := s.index[objectKey(tenant, bucket, key)]
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	manifest := s.objects[id]
	if manifest.State != StateCommitted {
		return ObjectManifest{}, ErrNotFound
	}
	return cloneManifest(manifest), nil
}

func (s *MemoryStore) DeleteObject(ctx context.Context, tenant string, bucket string, key string) (ObjectManifest, error) {
	if err := ctx.Err(); err != nil {
		return ObjectManifest{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	indexKey := objectKey(tenant, bucket, key)
	id, ok := s.index[indexKey]
	if !ok {
		return ObjectManifest{}, ErrNotFound
	}
	manifest := s.objects[id]
	if manifest.State != StateCommitted {
		return ObjectManifest{}, ErrNotFound
	}
	s.decrementLocked(tenant, manifest.Chunks)
	manifest.State = StateDeleted
	manifest.UpdatedAt = time.Now().UTC()
	s.objects[id] = manifest
	delete(s.index, indexKey)
	return cloneManifest(manifest), nil
}

func (s *MemoryStore) ListUnreferencedBlocks(ctx context.Context, tenant string, limit int) ([]BlockRef, error) {
	return s.ListUnreferencedBlocksOlderThan(ctx, tenant, time.Now().UTC(), limit)
}

func (s *MemoryStore) ListUnreferencedBlocksOlderThan(ctx context.Context, tenant string, before time.Time, limit int) ([]BlockRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 1000
	}
	blocks := s.blocks[tenant]
	refs := make([]BlockRef, 0)
	for hash, ref := range blocks {
		if ref.RefCount == 0 && !ref.UpdatedAt.After(before) {
			refs = append(refs, BlockRef{Hash: hash, Size: ref.Size, UpdatedAt: ref.UpdatedAt})
			if len(refs) == limit {
				break
			}
		}
	}
	return refs, nil
}

func (s *MemoryStore) ForgetBlocks(ctx context.Context, tenant string, hashes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		if ref, ok := s.blocks[tenant][hash]; ok && ref.RefCount == 0 {
			delete(s.blocks[tenant], hash)
		}
	}
	return nil
}

func (s *MemoryStore) ListTenants(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := map[string]bool{}
	tenants := make([]string, 0, len(s.blocks)+len(s.multipart))
	for tenant := range s.blocks {
		seen[tenant] = true
		tenants = append(tenants, tenant)
	}
	for tenant := range s.multipart {
		if !seen[tenant] {
			tenants = append(tenants, tenant)
		}
	}
	return tenants, nil
}

func (s *MemoryStore) CreateMultipartSession(ctx context.Context, session MultipartSession) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if session.CreatedAt.IsZero() {
		session.CreatedAt = time.Now().UTC()
	}
	if session.UpdatedAt.IsZero() {
		session.UpdatedAt = session.CreatedAt
	}
	if session.Parts == nil {
		session.Parts = map[int]MultipartPart{}
	}
	s.ensureTenant(session.Tenant)
	if s.multipart[session.Tenant] == nil {
		s.multipart[session.Tenant] = map[string]MultipartSession{}
	}
	s.multipart[session.Tenant][session.UploadID] = cloneMultipartSession(session)
	return nil
}

func (s *MemoryStore) SaveMultipartPart(ctx context.Context, tenant string, uploadID string, part MultipartPart, reservedBytes int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.multipart[tenant][uploadID]
	if !ok {
		return ErrNotFound
	}
	if part.CreatedAt.IsZero() {
		part.CreatedAt = time.Now().UTC()
	}
	if session.Parts == nil {
		session.Parts = map[int]MultipartPart{}
	}
	session.Parts[part.Number] = part
	session.ReservedBytes = reservedBytes
	session.UpdatedAt = time.Now().UTC()
	s.multipart[tenant][uploadID] = cloneMultipartSession(session)
	return nil
}

func (s *MemoryStore) GetMultipartSession(ctx context.Context, tenant string, uploadID string) (MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return MultipartSession{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.multipart[tenant][uploadID]
	if !ok {
		return MultipartSession{}, ErrNotFound
	}
	return cloneMultipartSession(session), nil
}

func (s *MemoryStore) ListMultipartSessions(ctx context.Context, tenant string) ([]MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	sessions := make([]MultipartSession, 0, len(s.multipart[tenant]))
	for _, session := range s.multipart[tenant] {
		sessions = append(sessions, cloneMultipartSession(session))
	}
	return sessions, nil
}

func (s *MemoryStore) ListStaleMultipartSessions(ctx context.Context, tenant string, before time.Time) ([]MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var sessions []MultipartSession
	for _, session := range s.multipart[tenant] {
		if session.CreatedAt.Before(before) {
			sessions = append(sessions, cloneMultipartSession(session))
		}
	}
	return sessions, nil
}

func (s *MemoryStore) DeleteMultipartSession(ctx context.Context, tenant string, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.multipart[tenant][uploadID]; !ok {
		return ErrNotFound
	}
	delete(s.multipart[tenant], uploadID)
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}

func (s *MemoryStore) HealthCheck(ctx context.Context) error {
	return ctx.Err()
}

func (s *MemoryStore) ensureTenant(tenant string) {
	if s.blocks[tenant] == nil {
		s.blocks[tenant] = map[string]BlockRefCount{}
	}
}

func (s *MemoryStore) incrementLocked(tenant string, chunks []ChunkRef) {
	s.ensureTenant(tenant)
	now := time.Now().UTC()
	for _, chunk := range chunks {
		ref := s.blocks[tenant][chunk.Hash]
		ref.Size = chunk.Size
		ref.RefCount++
		ref.UpdatedAt = now
		s.blocks[tenant][chunk.Hash] = ref
	}
}

func (s *MemoryStore) decrementLocked(tenant string, chunks []ChunkRef) {
	s.ensureTenant(tenant)
	now := time.Now().UTC()
	for _, chunk := range chunks {
		ref := s.blocks[tenant][chunk.Hash]
		if ref.RefCount > 0 {
			ref.RefCount--
		}
		ref.Size = chunk.Size
		ref.UpdatedAt = now
		s.blocks[tenant][chunk.Hash] = ref
	}
}

func objectKey(tenant string, bucket string, key string) string {
	return tenant + "\x00" + bucket + "\x00" + key
}

func cloneManifest(manifest ObjectManifest) ObjectManifest {
	manifest.Chunks = append([]ChunkRef(nil), manifest.Chunks...)
	if manifest.Headers != nil {
		manifest.Headers = cloneStringMap(manifest.Headers)
	}
	return manifest
}

func cloneMultipartSession(session MultipartSession) MultipartSession {
	session.Headers = cloneStringMap(session.Headers)
	session.Parts = cloneMultipartParts(session.Parts)
	return session
}

func cloneMultipartParts(parts map[int]MultipartPart) map[int]MultipartPart {
	if parts == nil {
		return nil
	}
	clone := make(map[int]MultipartPart, len(parts))
	for number, part := range parts {
		clone[number] = part
	}
	return clone
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

func randomID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(bytes[:])
}
