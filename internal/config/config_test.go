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

func TestValidateBackendCentralizesBackendRules(t *testing.T) {
	cfg := validTestConfig()
	cfg.BackendProvider = "s3"
	cfg.S3Endpoint = ""
	cfg.S3Bucket = "blocks"
	if err := cfg.ValidateBackend(); err == nil {
		t.Fatal("expected s3 backend without endpoint to fail validation")
	}

	cfg.S3Endpoint = "http://localhost:9000"
	cfg.S3Bucket = ""
	if err := cfg.ValidateBackend(); err == nil {
		t.Fatal("expected s3 backend without bucket to fail validation")
	}

	cfg.S3Bucket = "blocks"
	cfg.S3AccessKey = "access"
	cfg.S3SecretKey = ""
	if err := cfg.ValidateBackend(); err == nil {
		t.Fatal("expected partial s3 credentials to fail validation")
	}
}

func TestValidateBackendAcceptsKnownS3Providers(t *testing.T) {
	for _, provider := range []string{"", "generic", "aws", "aws-s3", "minio", "r2", "cloudflare-r2", "supabase", "b2", "backblaze-b2"} {
		t.Run(provider, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "s3"
			cfg.S3Endpoint = "http://localhost:9000"
			cfg.S3Bucket = "chunkgate-blocks"
			cfg.S3Provider = provider
			if err := cfg.ValidateBackend(); err != nil {
				t.Fatalf("validate backend failed: %v", err)
			}
		})
	}
}

func TestValidateBackendRejectsUnknownS3Provider(t *testing.T) {
	cfg := validTestConfig()
	cfg.BackendProvider = "s3"
	cfg.S3Endpoint = "http://localhost:9000"
	cfg.S3Bucket = "chunkgate-blocks"
	cfg.S3Provider = "not-a-provider"
	if err := cfg.ValidateBackend(); err == nil {
		t.Fatal("expected unknown s3 provider to fail validation")
	}
}

func TestValidateBackendAcceptsDellECSThroughS3AndSwift(t *testing.T) {
	s3Cfg := validTestConfig()
	s3Cfg.BackendProvider = "s3"
	s3Cfg.S3Provider = "generic"
	s3Cfg.S3Endpoint = "https://ecs.example.com:9021"
	s3Cfg.S3Bucket = "chunkgate-blocks"
	s3Cfg.S3AccessKey = "ecs-access"
	s3Cfg.S3SecretKey = "ecs-secret"
	if err := s3Cfg.ValidateBackend(); err != nil {
		t.Fatalf("validate ECS S3 config failed: %v", err)
	}

	swiftCfg := validTestConfig()
	swiftCfg.BackendProvider = "swift"
	swiftCfg.SwiftAuthURL = "https://ecs.example.com:4443/v3"
	swiftCfg.SwiftContainer = "chunkgate-blocks"
	swiftCfg.SwiftEndpoint = "https://ecs.example.com:9025/v1/AUTH_project_id/"
	swiftCfg.SwiftAuth = "password"
	swiftCfg.SwiftUsername = "chunkgate"
	swiftCfg.SwiftPassword = "ecs-swift-password"
	swiftCfg.SwiftProjectName = "service"
	swiftCfg.SwiftDomainName = "Default"
	if err := swiftCfg.ValidateBackend(); err != nil {
		t.Fatalf("validate ECS Swift config failed: %v", err)
	}
}

func TestValidateBackendRejectsNativeAtmosProvider(t *testing.T) {
	cfg := validTestConfig()
	cfg.BackendProvider = "atmos"
	if err := cfg.ValidateBackend(); err == nil {
		t.Fatal("expected native Atmos backend provider to be unsupported")
	}
}

func TestValidateBackendAcceptsAzureSharedKeyAndDefaultAuth(t *testing.T) {
	for _, auth := range []string{"auto", "shared-key", "default"} {
		t.Run(auth, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "azure"
			cfg.AzureEndpoint = "http://127.0.0.1:10000/devstoreaccount1"
			cfg.AzureContainer = "chunkgate-blocks"
			cfg.AzureAuth = auth
			if auth == "shared-key" {
				cfg.AzureAccountName = "devstoreaccount1"
				cfg.AzureAccountKey = "key"
			}
			if err := cfg.ValidateBackend(); err != nil {
				t.Fatalf("validate azure backend failed: %v", err)
			}
		})
	}
}

func TestValidateBackendRejectsInvalidAzureConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing container", mutate: func(cfg *Config) {
			cfg.AzureEndpoint = "https://account.blob.core.windows.net"
			cfg.AzureContainer = ""
		}},
		{name: "missing endpoint and account", mutate: func(cfg *Config) {
			cfg.AzureEndpoint = ""
			cfg.AzureAccountName = ""
			cfg.AzureContainer = "blocks"
		}},
		{name: "shared key missing key", mutate: func(cfg *Config) {
			cfg.AzureEndpoint = "https://account.blob.core.windows.net"
			cfg.AzureContainer = "blocks"
			cfg.AzureAuth = "shared-key"
			cfg.AzureAccountName = "account"
			cfg.AzureAccountKey = ""
		}},
		{name: "unknown auth", mutate: func(cfg *Config) {
			cfg.AzureEndpoint = "https://account.blob.core.windows.net"
			cfg.AzureContainer = "blocks"
			cfg.AzureAuth = "magic"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "azure"
			tc.mutate(&cfg)
			if err := cfg.ValidateBackend(); err == nil {
				t.Fatal("expected invalid azure backend config to fail")
			}
		})
	}
}

func TestValidateBackendAcceptsGCSAuthModes(t *testing.T) {
	for _, auth := range []string{"", "auto", "default", "emulator", "service-account"} {
		t.Run(auth, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "gcs"
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSAuth = auth
			if auth == "service-account" {
				cfg.GCSCredentialsJSON = `{"type":"service_account"}`
			}
			if err := cfg.ValidateBackend(); err != nil {
				t.Fatalf("validate gcs backend failed: %v", err)
			}
		})
	}
}

func TestValidateBackendRejectsInvalidGCSConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing bucket", mutate: func(cfg *Config) {
			cfg.GCSBucket = ""
		}},
		{name: "unknown auth", mutate: func(cfg *Config) {
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSAuth = "magic"
		}},
		{name: "service account missing credentials", mutate: func(cfg *Config) {
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSAuth = "service-account"
		}},
		{name: "credentials file and json", mutate: func(cfg *Config) {
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSCredentialsFile = "service-account.json"
			cfg.GCSCredentialsJSON = `{"type":"service_account"}`
		}},
		{name: "negative timeout", mutate: func(cfg *Config) {
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSTimeout = -1
		}},
		{name: "negative retries", mutate: func(cfg *Config) {
			cfg.GCSBucket = "chunkgate-blocks"
			cfg.GCSMaxRetries = -1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "gcs"
			tc.mutate(&cfg)
			if err := cfg.ValidateBackend(); err == nil {
				t.Fatal("expected invalid gcs backend config to fail")
			}
		})
	}
}

func TestValidateBackendAcceptsSwiftPasswordAndApplicationCredentialAuth(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "password", mutate: func(cfg *Config) {
			cfg.SwiftAuth = "password"
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftPassword = "secret"
			cfg.SwiftProjectName = "service"
			cfg.SwiftDomainName = "Default"
		}},
		{name: "application credential id", mutate: func(cfg *Config) {
			cfg.SwiftAuth = "application-credential"
			cfg.SwiftApplicationCredID = "app-id"
			cfg.SwiftApplicationCredSecret = "app-secret"
			cfg.SwiftProjectID = "project-id"
		}},
		{name: "application credential name", mutate: func(cfg *Config) {
			cfg.SwiftAuth = "application-credential"
			cfg.SwiftApplicationCredName = "chunkgate-app"
			cfg.SwiftApplicationCredSecret = "app-secret"
			cfg.SwiftProjectID = "project-id"
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftDomainName = "Default"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "swift"
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "chunkgate-blocks"
			tc.mutate(&cfg)
			if err := cfg.ValidateBackend(); err != nil {
				t.Fatalf("validate swift backend failed: %v", err)
			}
		})
	}
}

func TestValidateBackendRejectsInvalidSwiftConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing container", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = ""
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftPassword = "secret"
		}},
		{name: "missing auth url", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = ""
			cfg.SwiftContainer = "blocks"
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftPassword = "secret"
		}},
		{name: "password missing username", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftAuth = "password"
			cfg.SwiftPassword = "secret"
		}},
		{name: "password missing secret", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftAuth = "password"
			cfg.SwiftUsername = "chunkgate"
		}},
		{name: "application credential missing id and name", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftAuth = "application-credential"
			cfg.SwiftApplicationCredSecret = "app-secret"
		}},
		{name: "application credential missing secret", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftAuth = "application-credential"
			cfg.SwiftApplicationCredID = "app-id"
		}},
		{name: "unknown auth", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftAuth = "magic"
		}},
		{name: "negative timeout", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftPassword = "secret"
			cfg.SwiftTimeout = -1
		}},
		{name: "negative retries", mutate: func(cfg *Config) {
			cfg.SwiftAuthURL = "https://identity.example.com/v3"
			cfg.SwiftContainer = "blocks"
			cfg.SwiftUsername = "chunkgate"
			cfg.SwiftPassword = "secret"
			cfg.SwiftMaxRetries = -1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.BackendProvider = "swift"
			tc.mutate(&cfg)
			if err := cfg.ValidateBackend(); err == nil {
				t.Fatal("expected invalid swift backend config to fail")
			}
		})
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
