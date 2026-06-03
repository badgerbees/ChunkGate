package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

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
)

type Config struct {
	ListenAddr              string
	DataDir                 string
	MetadataDir             string
	BackendDir              string
	ScratchDir              string
	LocalCapacityBytes      int64
	MaxConcurrentChunkers   int
	SmallFileThresholdBytes int
	ChunkMinBytes           int
	ChunkAvgBytes           int
	ChunkMaxBytes           int
	AuthCredentials         []s3auth.Credential
}

func FromEnv() Config {
	dataDir := envString("CHUNKGATE_DATA_DIR", defaultDataDir)
	return Config{
		ListenAddr:              envString("CHUNKGATE_LISTEN", defaultListen),
		DataDir:                 dataDir,
		MetadataDir:             envString("CHUNKGATE_METADATA_DIR", filepath.Join(dataDir, "metadata")),
		BackendDir:              envString("CHUNKGATE_BACKEND_DIR", filepath.Join(dataDir, "backend")),
		ScratchDir:              envString("CHUNKGATE_SCRATCH_DIR", filepath.Join(dataDir, "scratch")),
		LocalCapacityBytes:      envInt64("CHUNKGATE_LOCAL_CAPACITY_BYTES", defaultLocalCap),
		MaxConcurrentChunkers:   envInt("CHUNKGATE_MAX_CONCURRENT_CHUNKERS", defaultChunkWorkers),
		SmallFileThresholdBytes: envInt("CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES", defaultSmallBypass),
		ChunkMinBytes:           envInt("CHUNKGATE_CHUNK_MIN_BYTES", defaultChunkMin),
		ChunkAvgBytes:           envInt("CHUNKGATE_CHUNK_AVG_BYTES", defaultChunkAvg),
		ChunkMaxBytes:           envInt("CHUNKGATE_CHUNK_MAX_BYTES", defaultChunkMax),
		AuthCredentials:         credentialsFromEnv(),
	}
}

func (c Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("CHUNKGATE_LISTEN must not be empty")
	}
	if c.MetadataDir == "" || c.BackendDir == "" || c.ScratchDir == "" {
		return fmt.Errorf("metadata, backend, and scratch directories must be set")
	}
	if c.LocalCapacityBytes < 0 {
		return fmt.Errorf("CHUNKGATE_LOCAL_CAPACITY_BYTES must be >= 0")
	}
	if c.MaxConcurrentChunkers < 0 {
		return fmt.Errorf("CHUNKGATE_MAX_CONCURRENT_CHUNKERS must be >= 0")
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
	if c.SmallFileThresholdBytes < 0 {
		return fmt.Errorf("CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES must be >= 0")
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
