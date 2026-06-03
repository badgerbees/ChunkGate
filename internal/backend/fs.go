package backend

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FileStore struct {
	root string
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func (s *FileStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.blockPath(tenant, hash)
	if err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat block: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create block directory: %w", err)
	}
	tmp := path + "." + randomSuffix() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		_ = os.Remove(tmp)
		return nil
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit block: %w", err)
	}
	return nil
}

func (s *FileStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.blockPath(tenant, hash)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open block: %w", err)
	}
	return file, nil
}

func (s *FileStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		path, err := s.blockPath(tenant, hash)
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete block %s: %w", hash, err)
		}
	}
	return nil
}

func (s *FileStore) blockPath(tenant string, hash string) (string, error) {
	safeTenant := sanitizePathPart(tenant)
	if len(hash) < 2 || strings.Contains(hash, string(filepath.Separator)) || strings.Contains(hash, "..") {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	return filepath.Join(s.root, "tenants", safeTenant, "blocks", hash[:2], hash), nil
}

func sanitizePathPart(value string) string {
	if value == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func randomSuffix() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "fallback"
	}
	return hex.EncodeToString(bytes[:])
}
