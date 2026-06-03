package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

type ShardResolver struct {
	Root string
}

func (r ShardResolver) Path(tenant string) (string, error) {
	safe, err := SafeTenantID(tenant)
	if err != nil {
		return "", err
	}
	return filepath.Join(r.Root, "tenant_"+safe+".db"), nil
}

func SafeTenantID(tenant string) (string, error) {
	if tenant == "" {
		return "default", nil
	}
	var b strings.Builder
	for _, r := range tenant {
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
			sum := sha256.Sum256([]byte(tenant))
			return "sha256_" + hex.EncodeToString(sum[:8]), nil
		}
	}
	safe := b.String()
	if safe == "." || safe == ".." || strings.Contains(safe, "/") || strings.Contains(safe, "\\") {
		return "", fmt.Errorf("%w: %q", ErrInvalidTenant, tenant)
	}
	return safe, nil
}
