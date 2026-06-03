package multipart

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/chunkgate/chunkgate/internal/limits"
)

type Manager struct {
	root         string
	reservations *limits.DiskReservations
	mu           sync.Mutex
	sessions     map[string]*Session
}

type Session struct {
	UploadID  string
	Tenant    string
	Bucket    string
	Key       string
	Reserved  int64
	CreatedAt time.Time
	Directory string
	Parts     map[int]PartInfo
}

type PartInfo struct {
	Number int
	Size   int64
	ETag   string
	Path   string
}

func NewManager(root string, reservations *limits.DiskReservations) *Manager {
	return &Manager{
		root:         root,
		reservations: reservations,
		sessions:     map[string]*Session{},
	}
}

func (m *Manager) Create(ctx context.Context, tenant string, bucket string, key string, expectedSize int64) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	if m.reservations != nil {
		if err := m.reservations.TryReserve(expectedSize); err != nil {
			return Session{}, err
		}
	}
	uploadID := randomUploadID()
	dir := filepath.Join(m.root, safePathPart(tenant), uploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		if m.reservations != nil {
			m.reservations.Release(expectedSize)
		}
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
		Parts:     map[int]PartInfo{},
	}

	m.mu.Lock()
	m.sessions[uploadID] = session
	m.mu.Unlock()
	return cloneSession(session), nil
}

func (m *Manager) PutPart(ctx context.Context, tenant string, uploadID string, number int, reader io.Reader) (PartInfo, error) {
	if number <= 0 {
		return PartInfo{}, fmt.Errorf("part number must be positive")
	}
	session, err := m.session(tenant, uploadID)
	if err != nil {
		return PartInfo{}, err
	}
	path := filepath.Join(session.Directory, fmt.Sprintf("part-%08d", number))
	tmp := path + ".tmp"

	if err := ctx.Err(); err != nil {
		return PartInfo{}, err
	}
	file, err := os.Create(tmp)
	if err != nil {
		return PartInfo{}, fmt.Errorf("create part spool file: %w", err)
	}
	hash := md5.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), reader)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, fmt.Errorf("write part: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, fmt.Errorf("close part: %w", closeErr)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return PartInfo{}, fmt.Errorf("commit part spool file: %w", err)
	}
	info := PartInfo{
		Number: number,
		Size:   written,
		ETag:   `"` + hex.EncodeToString(hash.Sum(nil)) + `"`,
		Path:   path,
	}

	m.mu.Lock()
	if latest := m.sessions[uploadID]; latest != nil && latest.Tenant == tenant {
		latest.Parts[number] = info
	}
	m.mu.Unlock()
	return info, nil
}

func (m *Manager) Open(ctx context.Context, tenant string, uploadID string, orderedParts []int) (Session, io.ReadCloser, error) {
	session, err := m.session(tenant, uploadID)
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

func (m *Manager) Abort(ctx context.Context, tenant string, uploadID string) error {
	session, err := m.session(tenant, uploadID)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.sessions, uploadID)
	m.mu.Unlock()
	if m.reservations != nil {
		m.reservations.Release(session.Reserved)
	}
	return os.RemoveAll(session.Directory)
}

func (m *Manager) CompleteCleanup(ctx context.Context, tenant string, uploadID string) error {
	return m.Abort(ctx, tenant, uploadID)
}

func (m *Manager) session(tenant string, uploadID string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session := m.sessions[uploadID]
	if session == nil || session.Tenant != tenant {
		return Session{}, fmt.Errorf("multipart upload not found")
	}
	return cloneSession(session), nil
}

func cloneSession(session *Session) Session {
	clone := *session
	clone.Parts = make(map[int]PartInfo, len(session.Parts))
	for number, part := range session.Parts {
		clone.Parts[number] = part
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
