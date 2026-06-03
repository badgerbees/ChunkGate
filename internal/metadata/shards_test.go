package metadata

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestShardResolverKeepsTenantInsideRoot(t *testing.T) {
	root := t.TempDir()
	path, err := (ShardResolver{Root: root}).Path("../escape")
	if err != nil {
		t.Fatalf("path failed: %v", err)
	}
	if !strings.HasPrefix(path, root+string(filepath.Separator)) {
		t.Fatalf("path escaped root: %s", path)
	}
	if strings.Contains(filepath.Base(path), "..") {
		t.Fatalf("unsafe tenant path: %s", path)
	}
}
