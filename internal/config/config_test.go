package config

import (
	"reflect"
	"testing"
)

func TestValidateAcceptsPostgresMetadata(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetadataProvider = "postgres"
	cfg.PostgresDSN = "postgres://chunkgate:chunkgate@localhost:5432/chunkgate?sslmode=disable"
	cfg.MetadataDir = ""

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestValidateRequiresPostgresDSN(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetadataProvider = "postgres"
	cfg.PostgresDSN = ""

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected postgres metadata without DSN to fail validation")
	}
}

func TestValidateRejectsUnknownMetadataProvider(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetadataProvider = "mysql"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown metadata provider to fail validation")
	}
}

func TestValidateRequiresCredentialsUnlessAnonymousModeIsExplicit(t *testing.T) {
	cfg := validTestConfig()
	cfg.AuthAllowAnonymous = false
	cfg.AuthCredentials = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected credentials to be required when anonymous mode is disabled")
	}

	cfg.AuthAllowAnonymous = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("anonymous local config failed validation: %v", err)
	}
}

func TestValidateChecksLocalBlockEncryptionKey(t *testing.T) {
	cfg := validTestConfig()
	cfg.LocalBlockEncryptionKey = "short"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid encryption key to fail")
	}

	cfg.LocalBlockEncryptionKey = "0123456789abcdef0123456789abcdef"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid encryption key failed validation: %v", err)
	}
}

func TestValidateRejectsInvalidOperationalGuardrails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "cpu headroom", mutate: func(cfg *Config) { cfg.CPUHeadroomCores = -1 }},
		{name: "scratch free", mutate: func(cfg *Config) { cfg.ScratchMinFreeBytes = -1 }},
		{name: "max object", mutate: func(cfg *Config) { cfg.MaxObjectBytes = -1 }},
		{name: "complete xml", mutate: func(cfg *Config) { cfg.CompleteXMLMaxBytes = -1 }},
		{name: "readiness", mutate: func(cfg *Config) { cfg.ReadinessTimeout = -1 }},
		{name: "shutdown", mutate: func(cfg *Config) { cfg.ShutdownTimeout = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation to fail")
			}
		})
	}
}

func TestFromEnvParsesVirtualHostsAndCORS(t *testing.T) {
	t.Setenv("CHUNKGATE_ALLOW_ANONYMOUS", "true")
	t.Setenv("CHUNKGATE_VIRTUAL_HOSTS", "s3.example.com, localhost")
	t.Setenv("CHUNKGATE_CORS_ALLOWED_ORIGINS", "https://app.example.com,https://admin.example.com")
	t.Setenv("CHUNKGATE_CORS_ALLOWED_METHODS", "GET PUT")
	t.Setenv("CHUNKGATE_CORS_ALLOWED_HEADERS", "authorization,x-amz-date")
	t.Setenv("CHUNKGATE_CORS_EXPOSED_HEADERS", "ETag,x-amz-request-id")
	t.Setenv("CHUNKGATE_CORS_ALLOW_CREDENTIALS", "true")
	t.Setenv("CHUNKGATE_CORS_MAX_AGE_SECONDS", "900")

	cfg := FromEnv()
	for name, gotWant := range map[string]struct {
		got  []string
		want []string
	}{
		"virtual hosts":   {cfg.VirtualHosts, []string{"s3.example.com", "localhost"}},
		"cors origins":    {cfg.CORSAllowedOrigins, []string{"https://app.example.com", "https://admin.example.com"}},
		"cors methods":    {cfg.CORSAllowedMethods, []string{"GET", "PUT"}},
		"cors headers":    {cfg.CORSAllowedHeaders, []string{"authorization", "x-amz-date"}},
		"exposed headers": {cfg.CORSExposedHeaders, []string{"ETag", "x-amz-request-id"}},
	} {
		if !reflect.DeepEqual(gotWant.got, gotWant.want) {
			t.Fatalf("%s = %#v, want %#v", name, gotWant.got, gotWant.want)
		}
	}
	if !cfg.CORSAllowCredentials {
		t.Fatal("expected CORS credentials to be enabled")
	}
	if cfg.CORSMaxAgeSeconds != 900 {
		t.Fatalf("cors max age = %d", cfg.CORSMaxAgeSeconds)
	}
}

func TestValidateRejectsWildcardCORSWithCredentials(t *testing.T) {
	cfg := validTestConfig()
	cfg.CORSAllowedOrigins = []string{"*"}
	cfg.CORSAllowCredentials = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected wildcard CORS origin with credentials to fail validation")
	}
}

func validTestConfig() Config {
	return Config{
		ListenAddr:              ":0",
		DataDir:                 "data",
		MetadataProvider:        "sqlite",
		MetadataDir:             "data/metadata",
		BackendDir:              "data/backend",
		ScratchDir:              "data/scratch",
		BackendProvider:         "filesystem",
		CPUHeadroomCores:        1,
		ChunkMinBytes:           1,
		ChunkAvgBytes:           2,
		ChunkMaxBytes:           3,
		ChunkEngine:             "builtin",
		PostgresMaxOpenConns:    4,
		PostgresMaxIdleConns:    2,
		PostgresConnMaxLifetime: 1,
		AuthAllowAnonymous:      true,
	}
}
