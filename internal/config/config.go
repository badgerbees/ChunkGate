package config

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chunkgate/chunkgate/internal/s3auth"
)

const (
	defaultDataDir      = "data"
	defaultListen       = ":8080"
	defaultLocalCap     = int64(20 * 1024 * 1024 * 1024)
	defaultSmallBypass  = 5 * 1024 * 1024
	defaultChunkMin     = 512 * 1024
	defaultChunkAvg     = 1024 * 1024
	defaultChunkMax     = 4 * 1024 * 1024
	defaultChunkWorkers = 0
	defaultCPUHeadroom  = 1
	defaultChunkEngine  = "fastcdc"
	defaultMetadata     = "sqlite"
	defaultPartMax      = int64(5 * 1024 * 1024 * 1024)
	defaultStaleTTL     = 24 * time.Hour
	defaultBackend      = "filesystem"
	defaultScratchFree  = int64(1024 * 1024 * 1024)
	defaultCompleteXML  = int64(1024 * 1024)
	defaultAuthAnon     = false
	defaultS3Region     = "us-east-1"
	defaultS3UseTLS     = true
	defaultS3PathStyle  = true
	defaultS3Timeout    = 30 * time.Second
	defaultS3Retries    = 3
	defaultGCEnabled    = true
	defaultGCInterval   = time.Hour
	defaultGCMinAge     = 24 * time.Hour
	defaultGCBatchSize  = 1000
	defaultGCRetries    = 3
	defaultPGMaxOpen    = 16
	defaultPGMaxIdle    = 4
	defaultPGLifetime   = 30 * time.Minute
	defaultReadyTimeout = 3 * time.Second
	defaultShutdown     = 15 * time.Second
)

type Config struct {
	ListenAddr              string
	DataDir                 string
	MetadataProvider        string
	MetadataDir             string
	BackendDir              string
	ScratchDir              string
	BackendProvider         string
	LocalBlockEncryptionKey string
	LocalCapacityBytes      int64
	MaxConcurrentChunkers   int
	CPUHeadroomCores        int
	SmallFileThresholdBytes int
	ChunkMinBytes           int
	ChunkAvgBytes           int
	ChunkMaxBytes           int
	ChunkEngine             string
	MultipartMaxPartBytes   int64
	MultipartMaxUploadBytes int64
	MultipartStaleTTL       time.Duration
	ScratchMinFreeBytes     int64
	MaxObjectBytes          int64
	CompleteXMLMaxBytes     int64
	S3Endpoint              string
	S3Region                string
	S3Bucket                string
	S3AccessKey             string
	S3SecretKey             string
	S3SessionToken          string
	S3Prefix                string
	S3UseTLS                bool
	S3PathStyle             bool
	S3Timeout               time.Duration
	S3MaxRetries            int
	GCEnabled               bool
	GCInterval              time.Duration
	GCMinOrphanAge          time.Duration
	GCBatchSize             int
	GCMaxRetries            int
	PostgresDSN             string
	PostgresMaxOpenConns    int
	PostgresMaxIdleConns    int
	PostgresConnMaxLifetime time.Duration
	ReadinessTimeout        time.Duration
	ShutdownTimeout         time.Duration
	DebugPprofEnabled       bool
	AuthAllowAnonymous      bool
	AuthCredentials         []s3auth.Credential
}

func FromEnv() Config {
	dataDir := envString("CHUNKGATE_DATA_DIR", defaultDataDir)
	return Config{
		ListenAddr:              envString("CHUNKGATE_LISTEN", defaultListen),
		DataDir:                 dataDir,
		MetadataProvider:        envString("CHUNKGATE_METADATA", defaultMetadata),
		MetadataDir:             envString("CHUNKGATE_METADATA_DIR", filepath.Join(dataDir, "metadata")),
		BackendDir:              envString("CHUNKGATE_BACKEND_DIR", filepath.Join(dataDir, "backend")),
		ScratchDir:              envString("CHUNKGATE_SCRATCH_DIR", filepath.Join(dataDir, "scratch")),
		BackendProvider:         envString("CHUNKGATE_BACKEND", defaultBackend),
		LocalBlockEncryptionKey: envString("CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY", ""),
		LocalCapacityBytes:      envInt64("CHUNKGATE_LOCAL_CAPACITY_BYTES", defaultLocalCap),
		MaxConcurrentChunkers:   envInt("CHUNKGATE_MAX_CONCURRENT_CHUNKERS", defaultChunkWorkers),
		CPUHeadroomCores:        envInt("CHUNKGATE_CPU_HEADROOM_CORES", defaultCPUHeadroom),
		SmallFileThresholdBytes: envInt("CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES", defaultSmallBypass),
		ChunkMinBytes:           envInt("CHUNKGATE_CHUNK_MIN_BYTES", defaultChunkMin),
		ChunkAvgBytes:           envInt("CHUNKGATE_CHUNK_AVG_BYTES", defaultChunkAvg),
		ChunkMaxBytes:           envInt("CHUNKGATE_CHUNK_MAX_BYTES", defaultChunkMax),
		ChunkEngine:             envString("CHUNKGATE_CHUNK_ENGINE", defaultChunkEngine),
		MultipartMaxPartBytes:   envInt64("CHUNKGATE_MULTIPART_MAX_PART_BYTES", defaultPartMax),
		MultipartMaxUploadBytes: envInt64("CHUNKGATE_MULTIPART_MAX_UPLOAD_BYTES", defaultLocalCap),
		MultipartStaleTTL:       envDurationSeconds("CHUNKGATE_MULTIPART_STALE_TTL_SECONDS", defaultStaleTTL),
		ScratchMinFreeBytes:     envInt64("CHUNKGATE_SCRATCH_MIN_FREE_BYTES", defaultScratchFree),
		MaxObjectBytes:          envInt64("CHUNKGATE_MAX_OBJECT_BYTES", 0),
		CompleteXMLMaxBytes:     envInt64("CHUNKGATE_COMPLETE_XML_MAX_BYTES", defaultCompleteXML),
		S3Endpoint:              envString("CHUNKGATE_S3_ENDPOINT", ""),
		S3Region:                envString("CHUNKGATE_S3_REGION", defaultS3Region),
		S3Bucket:                envString("CHUNKGATE_S3_BUCKET", ""),
		S3AccessKey:             envString("CHUNKGATE_S3_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
		S3SecretKey:             envString("CHUNKGATE_S3_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")),
		S3SessionToken:          envString("CHUNKGATE_S3_SESSION_TOKEN", os.Getenv("AWS_SESSION_TOKEN")),
		S3Prefix:                envString("CHUNKGATE_S3_PREFIX", ""),
		S3UseTLS:                envBool("CHUNKGATE_S3_USE_TLS", defaultS3UseTLS),
		S3PathStyle:             envBool("CHUNKGATE_S3_PATH_STYLE", defaultS3PathStyle),
		S3Timeout:               envDurationSeconds("CHUNKGATE_S3_TIMEOUT_SECONDS", defaultS3Timeout),
		S3MaxRetries:            envInt("CHUNKGATE_S3_MAX_RETRIES", defaultS3Retries),
		GCEnabled:               envBool("CHUNKGATE_GC_ENABLED", defaultGCEnabled),
		GCInterval:              envDurationSeconds("CHUNKGATE_GC_INTERVAL_SECONDS", defaultGCInterval),
		GCMinOrphanAge:          envDurationSeconds("CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS", defaultGCMinAge),
		GCBatchSize:             envInt("CHUNKGATE_GC_BATCH_SIZE", defaultGCBatchSize),
		GCMaxRetries:            envInt("CHUNKGATE_GC_MAX_RETRIES", defaultGCRetries),
		PostgresDSN:             envString("CHUNKGATE_POSTGRES_DSN", ""),
		PostgresMaxOpenConns:    envInt("CHUNKGATE_POSTGRES_MAX_OPEN_CONNS", defaultPGMaxOpen),
		PostgresMaxIdleConns:    envInt("CHUNKGATE_POSTGRES_MAX_IDLE_CONNS", defaultPGMaxIdle),
		PostgresConnMaxLifetime: envDurationSeconds("CHUNKGATE_POSTGRES_CONN_MAX_LIFETIME_SECONDS", defaultPGLifetime),
		ReadinessTimeout:        envDurationSeconds("CHUNKGATE_READINESS_TIMEOUT_SECONDS", defaultReadyTimeout),
		ShutdownTimeout:         envDurationSeconds("CHUNKGATE_SHUTDOWN_TIMEOUT_SECONDS", defaultShutdown),
		DebugPprofEnabled:       envBool("CHUNKGATE_DEBUG_PPROF_ENABLED", false),
		AuthAllowAnonymous:      envBool("CHUNKGATE_ALLOW_ANONYMOUS", defaultAuthAnon),
		AuthCredentials:         credentialsFromEnv(),
	}
}

func (c Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("CHUNKGATE_LISTEN must not be empty")
	}
	if c.MetadataProvider != "sqlite" && c.MetadataProvider != "postgres" {
		return fmt.Errorf("CHUNKGATE_METADATA must be sqlite or postgres")
	}
	if c.MetadataProvider == "sqlite" && c.MetadataDir == "" {
		return fmt.Errorf("CHUNKGATE_METADATA_DIR must be set when CHUNKGATE_METADATA=sqlite")
	}
	if c.MetadataProvider == "postgres" && c.PostgresDSN == "" {
		return fmt.Errorf("CHUNKGATE_POSTGRES_DSN is required when CHUNKGATE_METADATA=postgres")
	}
	if c.BackendDir == "" || c.ScratchDir == "" {
		return fmt.Errorf("backend and scratch directories must be set")
	}
	if c.BackendProvider != "filesystem" && c.BackendProvider != "s3" {
		return fmt.Errorf("CHUNKGATE_BACKEND must be filesystem or s3")
	}
	if c.LocalBlockEncryptionKey != "" && c.BackendProvider != "filesystem" {
		return fmt.Errorf("CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY is only supported when CHUNKGATE_BACKEND=filesystem")
	}
	if c.LocalBlockEncryptionKey != "" {
		if _, err := DecodeLocalBlockEncryptionKey(c.LocalBlockEncryptionKey); err != nil {
			return err
		}
	}
	if c.LocalCapacityBytes < 0 {
		return fmt.Errorf("CHUNKGATE_LOCAL_CAPACITY_BYTES must be >= 0")
	}
	if c.MaxConcurrentChunkers < 0 {
		return fmt.Errorf("CHUNKGATE_MAX_CONCURRENT_CHUNKERS must be >= 0")
	}
	if c.CPUHeadroomCores < 0 {
		return fmt.Errorf("CHUNKGATE_CPU_HEADROOM_CORES must be >= 0")
	}
	if c.MaxConcurrentChunkers > runtime.NumCPU()*8 {
		return fmt.Errorf("CHUNKGATE_MAX_CONCURRENT_CHUNKERS is unexpectedly high")
	}
	if c.ChunkMinBytes <= 0 || c.ChunkAvgBytes <= 0 || c.ChunkMaxBytes <= 0 {
		return fmt.Errorf("chunk sizes must be positive")
	}
	if c.ChunkMinBytes > c.ChunkAvgBytes || c.ChunkAvgBytes > c.ChunkMaxBytes {
		return fmt.Errorf("chunk sizes must satisfy min <= avg <= max")
	}
	if c.ChunkEngine != "fastcdc" && c.ChunkEngine != "builtin" {
		return fmt.Errorf("CHUNKGATE_CHUNK_ENGINE must be fastcdc or builtin")
	}
	if c.ChunkEngine == "fastcdc" && (c.ChunkMinBytes >= c.ChunkAvgBytes || c.ChunkAvgBytes >= c.ChunkMaxBytes) {
		return fmt.Errorf("fastcdc chunk sizes must satisfy min < avg < max")
	}
	if c.SmallFileThresholdBytes < 0 {
		return fmt.Errorf("CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES must be >= 0")
	}
	if c.MultipartMaxPartBytes < 0 {
		return fmt.Errorf("CHUNKGATE_MULTIPART_MAX_PART_BYTES must be >= 0")
	}
	if c.MultipartMaxUploadBytes < 0 {
		return fmt.Errorf("CHUNKGATE_MULTIPART_MAX_UPLOAD_BYTES must be >= 0")
	}
	if c.MultipartStaleTTL < 0 {
		return fmt.Errorf("CHUNKGATE_MULTIPART_STALE_TTL_SECONDS must be >= 0")
	}
	if c.ScratchMinFreeBytes < 0 {
		return fmt.Errorf("CHUNKGATE_SCRATCH_MIN_FREE_BYTES must be >= 0")
	}
	if c.MaxObjectBytes < 0 {
		return fmt.Errorf("CHUNKGATE_MAX_OBJECT_BYTES must be >= 0")
	}
	if c.CompleteXMLMaxBytes < 0 {
		return fmt.Errorf("CHUNKGATE_COMPLETE_XML_MAX_BYTES must be >= 0")
	}
	if c.BackendProvider == "s3" {
		if c.S3Endpoint == "" {
			return fmt.Errorf("CHUNKGATE_S3_ENDPOINT is required when CHUNKGATE_BACKEND=s3")
		}
		if c.S3Bucket == "" {
			return fmt.Errorf("CHUNKGATE_S3_BUCKET is required when CHUNKGATE_BACKEND=s3")
		}
		if (c.S3AccessKey == "") != (c.S3SecretKey == "") {
			return fmt.Errorf("CHUNKGATE_S3_ACCESS_KEY_ID and CHUNKGATE_S3_SECRET_ACCESS_KEY must be set together")
		}
	}
	if c.S3Timeout < 0 {
		return fmt.Errorf("CHUNKGATE_S3_TIMEOUT_SECONDS must be >= 0")
	}
	if c.S3MaxRetries < 0 {
		return fmt.Errorf("CHUNKGATE_S3_MAX_RETRIES must be >= 0")
	}
	if c.GCInterval < 0 {
		return fmt.Errorf("CHUNKGATE_GC_INTERVAL_SECONDS must be >= 0")
	}
	if c.GCMinOrphanAge < 0 {
		return fmt.Errorf("CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS must be >= 0")
	}
	if c.GCBatchSize < 0 {
		return fmt.Errorf("CHUNKGATE_GC_BATCH_SIZE must be >= 0")
	}
	if c.GCBatchSize > 1000 {
		return fmt.Errorf("CHUNKGATE_GC_BATCH_SIZE must be <= 1000")
	}
	if c.GCMaxRetries < 0 {
		return fmt.Errorf("CHUNKGATE_GC_MAX_RETRIES must be >= 0")
	}
	if c.PostgresMaxOpenConns < 0 {
		return fmt.Errorf("CHUNKGATE_POSTGRES_MAX_OPEN_CONNS must be >= 0")
	}
	if c.PostgresMaxIdleConns < 0 {
		return fmt.Errorf("CHUNKGATE_POSTGRES_MAX_IDLE_CONNS must be >= 0")
	}
	if c.PostgresMaxOpenConns > 0 && c.PostgresMaxIdleConns > c.PostgresMaxOpenConns {
		return fmt.Errorf("CHUNKGATE_POSTGRES_MAX_IDLE_CONNS must be <= CHUNKGATE_POSTGRES_MAX_OPEN_CONNS")
	}
	if c.PostgresConnMaxLifetime < 0 {
		return fmt.Errorf("CHUNKGATE_POSTGRES_CONN_MAX_LIFETIME_SECONDS must be >= 0")
	}
	if c.ReadinessTimeout < 0 {
		return fmt.Errorf("CHUNKGATE_READINESS_TIMEOUT_SECONDS must be >= 0")
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("CHUNKGATE_SHUTDOWN_TIMEOUT_SECONDS must be >= 0")
	}
	if !c.AuthAllowAnonymous && len(c.AuthCredentials) == 0 {
		return fmt.Errorf("configure CHUNKGATE_CREDENTIALS or set CHUNKGATE_ALLOW_ANONYMOUS=true for local development")
	}
	for _, credential := range c.AuthCredentials {
		if credential.AccessKey == "" || credential.SecretKey == "" {
			return fmt.Errorf("auth credentials must include both access key and secret key")
		}
	}
	return nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func DecodeLocalBlockEncryptionKey(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	candidates := [][]byte{[]byte(value)}
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		candidates = append(candidates, decoded)
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(value); err == nil {
		candidates = append(candidates, decoded)
	}
	if decoded, err := hex.DecodeString(value); err == nil {
		candidates = append(candidates, decoded)
	}
	for _, candidate := range candidates {
		switch len(candidate) {
		case 16, 24, 32:
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY must decode to a 16, 24, or 32 byte AES key")
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return time.Duration(parsed) * time.Second
}

func credentialsFromEnv() []s3auth.Credential {
	var credentials []s3auth.Credential
	if accessKey := os.Getenv("CHUNKGATE_ACCESS_KEY_ID"); accessKey != "" {
		credentials = append(credentials, s3auth.Credential{
			AccessKey: accessKey,
			SecretKey: os.Getenv("CHUNKGATE_SECRET_ACCESS_KEY"),
			Tenant:    envString("CHUNKGATE_TENANT_ID", accessKey),
		})
	}
	spec := os.Getenv("CHUNKGATE_CREDENTIALS")
	if spec == "" {
		return credentials
	}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		credential := s3auth.Credential{AccessKey: strings.TrimSpace(parts[0])}
		if len(parts) > 1 {
			credential.SecretKey = strings.TrimSpace(parts[1])
		}
		if len(parts) > 2 {
			credential.Tenant = strings.TrimSpace(parts[2])
		}
		if credential.Tenant == "" {
			credential.Tenant = credential.AccessKey
		}
		credentials = append(credentials, credential)
	}
	return credentials
}
