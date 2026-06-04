package multipart

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
)

var (
	ErrUploadNotFound = errors.New("multipart upload not found")
	ErrPartTooLarge   = errors.New("multipart part exceeds configured limit")
	ErrUploadTooLarge = errors.New("multipart upload exceeds configured limit")
)

const defaultMaxPartSize = int64(5 * 1024 * 1024 * 1024)

type Manager struct {
	root          string
	reservations  *limits.DiskReservations
	disk          *limits.DiskGuard
	store         metadata.Store
	maxPartSize   int64
	maxUploadSize int64
	mu            sync.Mutex
	sessions      map[string]*Session
}

type Session struct {
	UploadID  string
	Tenant    string
	Bucket    string
	Key       string
	Reserved  int64
	CreatedAt time.Time
	Directory string
	Headers   map[string]string
	Parts     map[int]PartInfo
}

type PartInfo struct {
	Number int
	Size   int64
	ETag   string
	Path   string
}

type partReservation struct {
	delta            int64
	reserved         int64
	hadPrevious      bool
	previous         PartInfo
	previousReserved int64
}

type Option func(*Manager)

func WithMetadataStore(store metadata.Store) Option {
	return func(manager *Manager) {
		manager.store = store
	}
}

func WithMaxPartSize(bytes int64) Option {
	return func(manager *Manager) {
		manager.maxPartSize = bytes
	}
}

func WithMaxUploadSize(bytes int64) Option {
	return func(manager *Manager) {
		manager.maxUploadSize = bytes
	}
}

func WithDiskGuard(guard *limits.DiskGuard) Option {
	return func(manager *Manager) {
		manager.disk = guard
	}
}

func NewManager(root string, reservations *limits.DiskReservations, options ...Option) *Manager {
	manager := &Manager{
		root:         root,
		reservations: reservations,
		maxPartSize:  defaultMaxPartSize,
		sessions:     map[string]*Session{},
	}
	for _, option := range options {
		if option != nil {
			option(manager)
		}
	}
	return manager
}

func (m *Manager) Create(ctx context.Context, tenant string, bucket string, key string, expectedSize int64, headers ...map[string]string) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	if expectedSize < 0 {
		expectedSize = 0
	}
	if m.maxUploadSize > 0 && expectedSize > m.maxUploadSize {
		return Session{}, ErrUploadTooLarge
	}
	if err := os.MkdirAll(m.root, 0o755); err != nil {
		return Session{}, fmt.Errorf("create multipart scratch root: %w", err)
	}
	if err := m.checkDisk(ctx, 0); err != nil {
		return Session{}, err
	}
	if expectedSize > 0 {
		if err := m.reserve(ctx, expectedSize); err != nil {
			return Session{}, err
		}
	}
	uploadID := randomUploadID()
	dir := m.sessionDirectory(tenant, uploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		m.release(expectedSize)
		return Session{}, fmt.Errorf("create multipart scratch directory: %w", err)
	}
	session := &Session{
		UploadID:  uploadID,
		Tenant:    tenant,
		Bucket:    bucket,
		Key:       key,
		Reserved:  expectedSize,
		CreatedAt: time.Now().UTC(),
		Directory: dir,
		Headers:   firstHeaderMap(headers),
		Parts:     map[int]PartInfo{},
	}
	if m.store != nil {
		if err := m.store.CreateMultipartSession(ctx, metadata.MultipartSession{
			UploadID:      session.UploadID,
			Tenant:        session.Tenant,
			Bucket:        session.Bucket,
			Key:           session.Key,
			Headers:       session.Headers,
			ReservedBytes: session.Reserved,
			CreatedAt:     session.CreatedAt,
			UpdatedAt:     session.CreatedAt,
			Parts:         map[int]metadata.MultipartPart{},
		}); err != nil {
			m.release(expectedSize)
			_ = os.RemoveAll(dir)
			return Session{}, err
		}
	}

	m.mu.Lock()
	m.sessions[sessionKey(tenant, uploadID)] = session
	m.mu.Unlock()
	return cloneSession(session), nil
}

func (m *Manager) CheckPart(ctx context.Context, tenant string, uploadID string, number int, expectedSize int64) error {
	if expectedSize <= 0 {
		return nil
	}
	if number <= 0 {
		return fmt.Errorf("part number must be positive")
	}
	if m.maxPartSize > 0 && expectedSize > m.maxPartSize {
		return ErrPartTooLarge
	}
	session, err := m.session(ctx, tenant, uploadID)
	if err != nil {
		return err
	}
	total := expectedSize
	for partNumber, part := range session.Parts {
		if partNumber != number {
			total += part.Size
		}
	}
	if m.maxUploadSize > 0 && total > m.maxUploadSize {
		return ErrUploadTooLarge
	}
	delta := total - session.Reserved
	if delta <= 0 {
		return nil
	}
	return m.checkDisk(ctx, delta)
}

func (m *Manager) PutPart(ctx context.Context, tenant string, uploadID string, number int, reader io.Reader) (PartInfo, error) {
	if number <= 0 {
		return PartInfo{}, fmt.Errorf("part number must be positive")
	}
	session, err := m.session(ctx, tenant, uploadID)
	if err != nil {
		return PartInfo{}, err
	}
	path := filepath.Join(session.Directory, fmt.Sprintf("part-%08d", number))
	tmp := path + ".tmp"

	if err := ctx.Err(); err != nil {
		return PartInfo{}, err
	}
	if err := m.checkDisk(ctx, 0); err != nil {
		return PartInfo{}, err
	}
	file, err := os.Create(tmp)
	if err != nil {
		return PartInfo{}, fmt.Errorf("create part spool file: %w", err)
	}
	hash := md5.New()
	copyReader := reader
	if m.maxPartSize > 0 {
		copyReader = &io.LimitedReader{R: reader, N: m.maxPartSize + 1}
	}
	written, copyErr := io.Copy(io.MultiWriter(file, hash), copyReader)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, fmt.Errorf("write part: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, fmt.Errorf("close part: %w", closeErr)
	}
	if m.maxPartSize > 0 && written > m.maxPartSize {
		_ = os.Remove(tmp)
		return PartInfo{}, ErrPartTooLarge
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, err
	}

	info := PartInfo{
		Number: number,
		Size:   written,
		ETag:   `"` + hex.EncodeToString(hash.Sum(nil)) + `"`,
		Path:   path,
	}
	reservation, err := m.reserveForPart(ctx, tenant, uploadID, number, info)
	if err != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		m.rollbackReservation(tenant, uploadID, number, reservation)
		return PartInfo{}, fmt.Errorf("commit part spool file: %w", err)
	}
	if m.store != nil {
		if err := m.store.SaveMultipartPart(ctx, tenant, uploadID, metadata.MultipartPart{
			Number:    info.Number,
			Size:      info.Size,
			ETag:      info.ETag,
			Path:      info.Path,
			CreatedAt: time.Now().UTC(),
		}, reservation.reserved); err != nil {
			_ = os.Remove(path)
			m.rollbackReservation(tenant, uploadID, number, reservation)
			return PartInfo{}, err
		}
	}
	m.finishReservation(reservation)
	return info, nil
}

func (m *Manager) Open(ctx context.Context, tenant string, uploadID string, orderedParts []int) (Session, io.ReadCloser, error) {
	session, err := m.session(ctx, tenant, uploadID)
	if err != nil {
		return Session{}, nil, err
	}
	if len(orderedParts) == 0 {
		orderedParts = sortedPartNumbers(session.Parts)
	}
	files := make([]io.Reader, 0, len(orderedParts))
	closers := make([]io.Closer, 0, len(orderedParts))
	for _, number := range orderedParts {
		part, ok := session.Parts[number]
		if !ok {
			closeAll(closers)
			return Session{}, nil, fmt.Errorf("missing multipart part %d", number)
		}
		if err := ctx.Err(); err != nil {
			closeAll(closers)
			return Session{}, nil, err
		}
		file, err := os.Open(part.Path)
		if err != nil {
			closeAll(closers)
			return Session{}, nil, fmt.Errorf("open part %d: %w", number, err)
		}
		files = append(files, file)
		closers = append(closers, file)
	}
	return session, multiReadCloser{Reader: io.MultiReader(files...), closers: closers}, nil
}

func (m *Manager) Get(ctx context.Context, tenant string, uploadID string) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	return m.session(ctx, tenant, uploadID)
}

func (m *Manager) Abort(ctx context.Context, tenant string, uploadID string) error {
	session, err := m.session(ctx, tenant, uploadID)
	if err != nil {
		return err
	}
	return m.deleteSession(ctx, session)
}

func (m *Manager) CompleteCleanup(ctx context.Context, tenant string, uploadID string) error {
	return m.Abort(ctx, tenant, uploadID)
}

func (m *Manager) LoadActive(ctx context.Context) error {
	if m.store == nil {
		return nil
	}
	tenants, err := m.store.ListTenants(ctx)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		sessions, err := m.store.ListMultipartSessions(ctx, tenant)
		if err != nil {
			return err
		}
		for _, stored := range sessions {
			session := m.sessionFromMetadata(stored)
			if err := os.MkdirAll(session.Directory, 0o755); err != nil {
				return fmt.Errorf("create multipart scratch directory: %w", err)
			}
			if err := m.reserve(ctx, session.Reserved); err != nil {
				return fmt.Errorf("reserve multipart upload %s: %w", session.UploadID, err)
			}
			m.mu.Lock()
			key := sessionKey(session.Tenant, session.UploadID)
			if existing := m.sessions[key]; existing != nil {
				m.mu.Unlock()
				m.release(session.Reserved)
				continue
			}
			copy := session
			m.sessions[key] = &copy
			m.mu.Unlock()
		}
	}
	return nil
}

func (m *Manager) CleanupStale(ctx context.Context, ttl time.Duration) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().Add(-ttl)
	var stale []Session

	if m.store != nil {
		tenants, err := m.store.ListTenants(ctx)
		if err != nil {
			return 0, err
		}
		for _, tenant := range tenants {
			sessions, err := m.store.ListStaleMultipartSessions(ctx, tenant, cutoff)
			if err != nil {
				return 0, err
			}
			for _, stored := range sessions {
				stale = append(stale, m.sessionFromMetadata(stored))
			}
		}
	} else {
		m.mu.Lock()
		for _, session := range m.sessions {
			if session.CreatedAt.Before(cutoff) {
				stale = append(stale, cloneSession(session))
			}
		}
		m.mu.Unlock()
	}

	cleaned := 0
	for _, session := range stale {
		if err := m.deleteSession(ctx, session); err != nil && !errors.Is(err, ErrUploadNotFound) {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

func (m *Manager) session(ctx context.Context, tenant string, uploadID string) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	m.mu.Lock()
	if session := m.sessions[sessionKey(tenant, uploadID)]; session != nil {
		clone := cloneSession(session)
		m.mu.Unlock()
		return clone, nil
	}
	m.mu.Unlock()

	if m.store == nil {
		return Session{}, ErrUploadNotFound
	}

	stored, err := m.store.GetMultipartSession(ctx, tenant, uploadID)
	if errors.Is(err, metadata.ErrNotFound) {
		return Session{}, ErrUploadNotFound
	}
	if err != nil {
		return Session{}, err
	}
	session := m.sessionFromMetadata(stored)
	if err := m.reserve(ctx, session.Reserved); err != nil {
		return Session{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	key := sessionKey(tenant, uploadID)
	if existing := m.sessions[key]; existing != nil {
		m.release(session.Reserved)
		return cloneSession(existing), nil
	}
	copy := session
	m.sessions[key] = &copy
	return cloneSession(&copy), nil
}

func (m *Manager) deleteSession(ctx context.Context, session Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.store != nil {
		if err := m.store.DeleteMultipartSession(ctx, session.Tenant, session.UploadID); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
	}

	key := sessionKey(session.Tenant, session.UploadID)
	m.mu.Lock()
	loaded := m.sessions[key]
	delete(m.sessions, key)
	m.mu.Unlock()

	if loaded != nil {
		m.release(loaded.Reserved)
	}
	return os.RemoveAll(session.Directory)
}

func (m *Manager) reserveForPart(ctx context.Context, tenant string, uploadID string, number int, info PartInfo) (partReservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return partReservation{}, err
	}
	session := m.sessions[sessionKey(tenant, uploadID)]
	if session == nil {
		return partReservation{}, ErrUploadNotFound
	}
	total := info.Size
	previous, hadPrevious := session.Parts[number]
	for partNumber, part := range session.Parts {
		if partNumber != number {
			total += part.Size
		}
	}
	if m.maxUploadSize > 0 && total > m.maxUploadSize {
		return partReservation{}, ErrUploadTooLarge
	}
	delta := total - session.Reserved
	if delta > 0 {
		if err := m.reserve(ctx, delta); err != nil {
			return partReservation{}, err
		}
	}
	reservation := partReservation{
		delta:            delta,
		reserved:         total,
		hadPrevious:      hadPrevious,
		previous:         previous,
		previousReserved: session.Reserved,
	}
	session.Parts[number] = info
	session.Reserved = total
	return reservation, nil
}

func (m *Manager) rollbackReservation(tenant string, uploadID string, number int, reservation partReservation) {
	m.mu.Lock()
	if session := m.sessions[sessionKey(tenant, uploadID)]; session != nil {
		if reservation.hadPrevious {
			session.Parts[number] = reservation.previous
		} else {
			delete(session.Parts, number)
		}
		session.Reserved = reservation.previousReserved
	}
	m.mu.Unlock()
	if reservation.delta > 0 {
		m.release(reservation.delta)
	}
}

func (m *Manager) finishReservation(reservation partReservation) {
	if reservation.delta < 0 {
		m.release(-reservation.delta)
	}
}

func (m *Manager) reserve(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	if m.disk != nil {
		return m.disk.TryReserve(ctx, bytes)
	}
	if m.reservations != nil {
		return m.reservations.TryReserve(bytes)
	}
	return nil
}

func (m *Manager) checkDisk(ctx context.Context, bytes int64) error {
	if bytes <= 0 || m.disk == nil {
		return nil
	}
	return m.disk.Check(ctx, bytes)
}

func (m *Manager) release(bytes int64) {
	if m.disk != nil {
		m.disk.Release(bytes)
	} else if m.reservations != nil {
		m.reservations.Release(bytes)
	}
}

func (m *Manager) HealthCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(m.root, 0o755); err != nil {
		return fmt.Errorf("check multipart scratch root: %w", err)
	}
	return m.checkDisk(ctx, 0)
}

func (m *Manager) sessionFromMetadata(session metadata.MultipartSession) Session {
	parts := make(map[int]PartInfo, len(session.Parts))
	for number, part := range session.Parts {
		parts[number] = PartInfo{
			Number: part.Number,
			Size:   part.Size,
			ETag:   part.ETag,
			Path:   part.Path,
		}
	}
	return Session{
		UploadID:  session.UploadID,
		Tenant:    session.Tenant,
		Bucket:    session.Bucket,
		Key:       session.Key,
		Reserved:  session.ReservedBytes,
		CreatedAt: session.CreatedAt,
		Directory: m.sessionDirectory(session.Tenant, session.UploadID),
		Headers:   cloneStringMap(session.Headers),
		Parts:     parts,
	}
}

func cloneSession(session *Session) Session {
	clone := *session
	clone.Headers = cloneStringMap(session.Headers)
	clone.Parts = make(map[int]PartInfo, len(session.Parts))
	for number, part := range session.Parts {
		clone.Parts[number] = part
	}
	return clone
}

func firstHeaderMap(headers []map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	return cloneStringMap(headers[0])
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

func sortedPartNumbers(parts map[int]PartInfo) []int {
	numbers := make([]int, 0, len(parts))
	for number := range parts {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	return numbers
}

func (m *Manager) sessionDirectory(tenant string, uploadID string) string {
	return filepath.Join(m.root, safePathPart(tenant), uploadID)
}

func sessionKey(tenant string, uploadID string) string {
	return tenant + "\x00" + uploadID
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

func randomUploadID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes[:])
}

func safePathPart(value string) string {
	if value == "" {
		return "default"
	}
	var out []rune
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r)
		case r >= '0' && r <= '9':
			out = append(out, r)
		case r == '-' || r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
