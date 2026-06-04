package config

import "testing"

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
	}
}
