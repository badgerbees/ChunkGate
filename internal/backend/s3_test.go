package backend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
	endpoint, err := normalizeEndpoint("http://localhost:9000", true)
	if err != nil {
		t.Fatalf("normalize endpoint failed: %v", err)
	}
	if endpoint.Host != "localhost:9000" || endpoint.Secure {
		t.Fatalf("endpoint=%#v", endpoint)
	}

	endpoint, err = normalizeEndpoint("https://s3.amazonaws.com", false)
	if err != nil {
		t.Fatalf("normalize https endpoint failed: %v", err)
	}
	if endpoint.Host != "s3.amazonaws.com" || !endpoint.Secure {
		t.Fatalf("endpoint=%#v", endpoint)
	}
}

func TestNormalizeEndpointKeepsSupabaseBasePath(t *testing.T) {
	endpoint, err := normalizeEndpoint("https://project-ref.storage.supabase.co/storage/v1/s3", false)
	if err != nil {
		t.Fatalf("normalize supabase endpoint failed: %v", err)
	}
	if endpoint.Host != "project-ref.storage.supabase.co" || !endpoint.Secure || endpoint.BasePath != "/storage/v1/s3" {
		t.Fatalf("endpoint=%#v", endpoint)
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

func TestS3StoreSupportsPathBasedS3Endpoints(t *testing.T) {
	objects := map[string][]byte{}
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("missing Authorization header for %s %s", r.Method, r.URL.String())
			http.Error(w, "missing auth", http.StatusInternalServerError)
			return
		}
		if r.Header.Get("x-amz-content-sha256") == "" {
			t.Errorf("missing payload hash header for %s %s", r.Method, r.URL.String())
			http.Error(w, "missing payload hash", http.StatusInternalServerError)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/storage/v1/s3/chunkgate-blocks/") && r.URL.Path != "/storage/v1/s3/chunkgate-blocks" {
			t.Errorf("path = %s, want Supabase-style base path", r.URL.Path)
			http.Error(w, "bad path", http.StatusInternalServerError)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/storage/v1/s3/chunkgate-blocks/")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/storage/v1/s3/chunkgate-blocks":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			if _, ok := objects[key]; !ok {
				writeS3TestError(w, http.StatusNotFound, "NoSuchKey")
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read put body failed: %v", err)
				http.Error(w, "read put", http.StatusInternalServerError)
				return
			}
			objects[key] = data
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			data, ok := objects[key]
			if !ok {
				writeS3TestError(w, http.StatusNotFound, "NoSuchKey")
				return
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodPost && r.URL.RawQuery == "delete=":
			sawDelete = true
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read delete body failed: %v", err)
				http.Error(w, "read delete", http.StatusInternalServerError)
				return
			}
			if !strings.Contains(string(body), "<Key>"+key+"</Key>") && !strings.Contains(string(body), "<Key>tenants/tenant-a/blocks/01/"+testBlockHash+"</Key>") {
				t.Errorf("delete body = %s", body)
				http.Error(w, "bad delete body", http.StatusInternalServerError)
				return
			}
			delete(objects, "tenants/tenant-a/blocks/01/"+testBlockHash)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	store, err := NewS3Store(S3Options{
		Endpoint:   server.URL + "/storage/v1/s3",
		Region:     "local",
		Bucket:     "chunkgate-blocks",
		AccessKey:  "access",
		SecretKey:  "secret",
		PathStyle:  true,
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("create path s3 store failed: %v", err)
	}
	ctx := context.Background()
	if err := store.HealthCheck(ctx); err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if ok, err := store.HasBlock(ctx, "tenant-a", testBlockHash); err != nil || ok {
		t.Fatalf("initial has block ok=%v err=%v, want false nil", ok, err)
	}
	if err := store.PutBlock(ctx, "tenant-a", testBlockHash, []byte("payload")); err != nil {
		t.Fatalf("put block failed: %v", err)
	}
	if ok, err := store.HasBlock(ctx, "tenant-a", testBlockHash); err != nil || !ok {
		t.Fatalf("has block ok=%v err=%v, want true nil", ok, err)
	}
	reader, err := store.GetBlock(ctx, "tenant-a", testBlockHash)
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
	if err := store.DeleteBlocks(ctx, "tenant-a", []string{testBlockHash}); err != nil {
		t.Fatalf("delete block failed: %v", err)
	}
	if !sawDelete {
		t.Fatal("delete objects request was not observed")
	}
	if ok, err := store.HasBlock(ctx, "tenant-a", testBlockHash); err != nil || ok {
		t.Fatalf("has deleted block ok=%v err=%v, want false nil", ok, err)
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

	clientEndpoint, err := normalizeEndpoint(endpoint, secure)
	if err != nil {
		t.Fatalf("normalize integration endpoint failed: %v", err)
	}
	client, err := minio.New(clientEndpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       clientEndpoint.Secure,
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

func writeS3TestError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<Error><Code>` + code + `</Code><Message>` + code + `</Message></Error>`))
}
