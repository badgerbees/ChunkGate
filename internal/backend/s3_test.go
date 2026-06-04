package backend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestS3StoreBlockKeyUsesTenantIsolatedPrefix(t *testing.T) {
	store := &S3Store{prefix: cleanS3Prefix("chunkgate/root")}
	key, err := store.blockKey("tenant/a", testBlockHash)
	if err != nil {
		t.Fatalf("block key failed: %v", err)
	}
	if key != "chunkgate/root/tenants/tenant_a/blocks/01/"+testBlockHash {
		t.Fatalf("key = %q", key)
	}
}

func TestNormalizeEndpointInfersTLSFromScheme(t *testing.T) {
	endpoint, secure, err := normalizeEndpoint("http://localhost:9000", true)
	if err != nil {
		t.Fatalf("normalize endpoint failed: %v", err)
	}
	if endpoint != "localhost:9000" || secure {
		t.Fatalf("endpoint=%q secure=%v", endpoint, secure)
	}

	endpoint, secure, err = normalizeEndpoint("https://s3.amazonaws.com", false)
	if err != nil {
		t.Fatalf("normalize https endpoint failed: %v", err)
	}
	if endpoint != "s3.amazonaws.com" || !secure {
		t.Fatalf("endpoint=%q secure=%v", endpoint, secure)
	}
}

func TestWrapS3ErrorClassifiesNotFoundAndUnavailable(t *testing.T) {
	notFound := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: http.StatusNotFound}
	if err := wrapS3Error("get block", "key", notFound); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("not found err = %v", err)
	}

	unavailable := minio.ErrorResponse{Code: "SlowDown", StatusCode: http.StatusServiceUnavailable}
	if err := wrapS3Error("put block", "key", unavailable); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("unavailable err = %v", err)
	}
}

func TestS3StoreIntegrationMinIO(t *testing.T) {
	endpoint := os.Getenv("CHUNKGATE_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set CHUNKGATE_S3_TEST_ENDPOINT to run the MinIO integration test")
	}
	ctx := context.Background()
	bucket := envOr("CHUNKGATE_S3_TEST_BUCKET", "chunkgate-test")
	accessKey := envOr("CHUNKGATE_S3_TEST_ACCESS_KEY_ID", "minioadmin")
	secretKey := envOr("CHUNKGATE_S3_TEST_SECRET_ACCESS_KEY", "minioadmin")
	region := envOr("CHUNKGATE_S3_TEST_REGION", "us-east-1")
	secure := strings.EqualFold(os.Getenv("CHUNKGATE_S3_TEST_USE_TLS"), "true")

	clientEndpoint, clientSecure, err := normalizeEndpoint(endpoint, secure)
	if err != nil {
		t.Fatalf("normalize integration endpoint failed: %v", err)
	}
	client, err := minio.New(clientEndpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       clientSecure,
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("create integration client failed: %v", err)
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("check bucket failed: %v", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
			created, checkErr := client.BucketExists(ctx, bucket)
			if checkErr != nil {
				t.Fatalf("make bucket failed: %v; recheck bucket failed: %v", err, checkErr)
			}
			if !created {
				t.Fatalf("make bucket failed: %v", err)
			}
		}
	}

	store, err := NewS3Store(S3Options{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		Prefix:     "integration/" + time.Now().UTC().Format("20060102150405.000000000"),
		Secure:     secure,
		PathStyle:  true,
		Timeout:    10 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("create s3 store failed: %v", err)
	}

	hash := testBlockHash
	if ok, err := store.HasBlock(ctx, "tenant-a", hash); err != nil || ok {
		t.Fatalf("initial has block ok=%v err=%v, want false nil", ok, err)
	}
	if err := store.PutBlock(ctx, "tenant-a", hash, []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}
	if ok, err := store.HasBlock(ctx, "tenant-a", hash); err != nil || !ok {
		t.Fatalf("has block ok=%v err=%v, want true nil", ok, err)
	}
	reader, err := store.GetBlock(ctx, "tenant-a", hash)
	if err != nil {
		t.Fatalf("get block failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read block failed: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("payload = %q", data)
	}
	if err := store.DeleteBlocks(ctx, "tenant-a", []string{hash}); err != nil {
		t.Fatalf("delete block failed: %v", err)
	}
	if ok, err := store.HasBlock(ctx, "tenant-a", hash); err != nil || ok {
		t.Fatalf("has deleted block ok=%v err=%v, want false nil", ok, err)
	}
}

func envOr(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
