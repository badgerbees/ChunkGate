package backend

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const encryptedBlockMagic = "CGFSENC1"

type FileStore struct {
	root string
	aead cipher.AEAD
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func NewEncryptedFileStore(root string, key []byte) (*FileStore, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("configure filesystem block encryption: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("configure filesystem block encryption: %w", err)
	}
	return &FileStore{root: root, aead: aead}, nil
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
	payload, err := s.encodeBlock(data)
	if err != nil {
		return err
	}
	tmp := path + "." + randomSuffix() + ".tmp"
	if err := os.WriteFile(tmp, payload, blockFileMode(s.aead != nil)); err != nil {
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
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrBlockNotFound, hash)
		}
		return nil, fmt.Errorf("open block: %w", err)
	}
	if s.aead == nil {
		return file, nil
	}
	data, err := io.ReadAll(file)
	closeErr := file.Close()
	if err != nil {
		return nil, fmt.Errorf("read encrypted block: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close encrypted block: %w", closeErr)
	}
	plaintext, err := s.decodeBlock(data)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(plaintext)), nil
}

func (s *FileStore) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := s.blockPath(tenant, hash)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, fmt.Errorf("stat block: %w", err)
	}
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

func (s *FileStore) HealthCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return fmt.Errorf("check filesystem backend root: %w", err)
	}
	return nil
}

func (s *FileStore) blockPath(tenant string, hash string) (string, error) {
	safeTenant := sanitizePathPart(tenant)
	if !validBlockHash(hash) {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	path := filepath.Join(s.root, "tenants", safeTenant, "blocks", hash[:2], hash)
	root, err := filepath.Abs(s.root)
	if err != nil {
		return "", fmt.Errorf("resolve backend root: %w", err)
	}
	target, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve block path: %w", err)
	}
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", fmt.Errorf("verify block path: %w", err)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("invalid block path")
	}
	return target, nil
}

func (s *FileStore) encodeBlock(data []byte) ([]byte, error) {
	if s.aead == nil {
		return data, nil
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("create block encryption nonce: %w", err)
	}
	payload := make([]byte, 0, len(encryptedBlockMagic)+len(nonce)+len(data)+s.aead.Overhead())
	payload = append(payload, encryptedBlockMagic...)
	payload = append(payload, nonce...)
	payload = s.aead.Seal(payload, nonce, data, nil)
	return payload, nil
}

func (s *FileStore) decodeBlock(data []byte) ([]byte, error) {
	if s.aead == nil {
		return data, nil
	}
	prefixLen := len(encryptedBlockMagic) + s.aead.NonceSize()
	if len(data) < prefixLen || !bytes.Equal(data[:len(encryptedBlockMagic)], []byte(encryptedBlockMagic)) {
		return nil, fmt.Errorf("encrypted block is missing ChunkGate encryption header")
	}
	nonce := data[len(encryptedBlockMagic):prefixLen]
	ciphertext := data[prefixLen:]
	plaintext, err := s.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt block: %w", err)
	}
	return plaintext, nil
}

func validBlockHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	for _, r := range hash {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

func blockFileMode(encrypted bool) os.FileMode {
	if encrypted {
		return 0o600
	}
	return 0o644
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
