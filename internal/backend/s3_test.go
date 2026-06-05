package backend

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
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

func TestNormalizeS3ProviderAliases(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", S3ProviderGeneric},
		{"generic", S3ProviderGeneric},
		{"aws-s3", S3ProviderAWS},
		{"AWS", S3ProviderAWS},
		{"cloudflare-r2", S3ProviderR2},
		{"r2", S3ProviderR2},
		{"backblaze-b2", S3ProviderB2},
		{"b2", S3ProviderB2},
		{"supabase", S3ProviderSupabase},
		{"minio", S3ProviderMinIO},
	} {
		if got := NormalizeS3Provider(tc.in); got != tc.want {
			t.Fatalf("NormalizeS3Provider(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNewS3StoreKeepsMinIOSDKForHostOnlyEndpoints(t *testing.T) {
	store, err := NewS3Store(S3Options{
		Endpoint:  "localhost:9000",
		Bucket:    "chunkgate-blocks",
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("create s3 store failed: %v", err)
	}
	if _, ok := store.client.(minioS3Client); !ok {
		t.Fatalf("client type = %T, want minioS3Client", store.client)
	}
}

func TestNewS3StoreUsesPathClientForBasePathEndpoints(t *testing.T) {
	store, err := NewS3Store(S3Options{
		Endpoint:  "https://project-ref.storage.supabase.co/storage/v1/s3",
		Provider:  S3ProviderSupabase,
		Bucket:    "chunkgate-blocks",
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("create s3 store failed: %v", err)
	}
	if _, ok := store.client.(*pathS3Client); !ok {
		t.Fatalf("client type = %T, want *pathS3Client", store.client)
	}
	if store.provider != S3ProviderSupabase {
		t.Fatalf("provider = %q, want supabase", store.provider)
	}
}

func TestNewS3StoreConfiguresBackblazeB2Preset(t *testing.T) {
	store, err := NewS3Store(S3Options{
		Endpoint:   "https://s3.us-west-004.backblazeb2.com",
		Provider:   "backblaze-b2",
		Region:     "us-west-004",
		Bucket:     "chunkgate-blocks",
		AccessKey:  "key-id",
		SecretKey:  "application-key",
		PathStyle:  true,
		Timeout:    time.Second,
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("create b2 s3 store failed: %v", err)
	}
	if store.provider != S3ProviderB2 {
		t.Fatalf("provider = %q, want b2", store.provider)
	}
	if store.deleteBatchSize != defaultS3DeleteObjectsLimit {
		t.Fatalf("delete batch size = %d, want %d", store.deleteBatchSize, defaultS3DeleteObjectsLimit)
	}
	if _, ok := store.client.(minioS3Client); !ok {
		t.Fatalf("client type = %T, want minioS3Client for host-only b2 endpoint", store.client)
	}
}

func TestPathS3ClientBuildsPathStyleURL(t *testing.T) {
	endpoint, err := normalizeEndpoint("https://storage.example.test/storage/v1/s3", false)
	if err != nil {
		t.Fatalf("normalize endpoint failed: %v", err)
	}
	client := newPathS3Client(pathS3Options{Endpoint: endpoint, PathStyle: true})
	target := client.objectURL("chunkgate-blocks", "tenants/tenant-a/blocks/aa/hash")
	if target.Scheme != "https" || target.Host != "storage.example.test" {
		t.Fatalf("target = %s, want https://storage.example.test", target.String())
	}
	if target.Path != "/storage/v1/s3/chunkgate-blocks/tenants/tenant-a/blocks/aa/hash" {
		t.Fatalf("path = %q", target.Path)
	}
}

func TestPathS3ClientBuildsVirtualHostStyleURL(t *testing.T) {
	endpoint, err := normalizeEndpoint("https://storage.example.test/storage/v1/s3", false)
	if err != nil {
		t.Fatalf("normalize endpoint failed: %v", err)
	}
	client := newPathS3Client(pathS3Options{Endpoint: endpoint, PathStyle: false})
	target := client.objectURL("chunkgate-blocks", "tenants/tenant-a/blocks/aa/hash")
	if target.Scheme != "https" || target.Host != "chunkgate-blocks.storage.example.test" {
		t.Fatalf("target = %s, want virtual-host style bucket host", target.String())
	}
	if target.Path != "/storage/v1/s3/tenants/tenant-a/blocks/aa/hash" {
		t.Fatalf("path = %q", target.Path)
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

func TestS3StoreDeleteBlocksBatchesDeleteObjects(t *testing.T) {
	client := &recordingS3Client{}
	store := &S3Store{
		client:          client,
		bucket:          "chunkgate-blocks",
		deleteBatchSize: defaultS3DeleteObjectsLimit,
	}
	hashes := make([]string, 0, defaultS3DeleteObjectsLimit+1)
	for i := 0; i < defaultS3DeleteObjectsLimit+1; i++ {
		hashes = append(hashes, fmt.Sprintf("%064x", i+1))
	}
	if err := store.DeleteBlocks(context.Background(), "tenant-a", hashes); err != nil {
		t.Fatalf("delete blocks failed: %v", err)
	}
	if len(client.deleteCalls) != 2 {
		t.Fatalf("delete calls = %d, want 2", len(client.deleteCalls))
	}
	if len(client.deleteCalls[0]) != defaultS3DeleteObjectsLimit || len(client.deleteCalls[1]) != 1 {
		t.Fatalf("delete call sizes = %d, %d", len(client.deleteCalls[0]), len(client.deleteCalls[1]))
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
			var request deleteRequest
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode delete body failed: %v", err)
				http.Error(w, "decode delete body", http.StatusInternalServerError)
				return
			}
			for _, object := range request.Objects {
				delete(objects, object.Key)
			}
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
	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return store
	})
	if !sawDelete {
		t.Fatal("delete objects request was not observed")
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
	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return store
	})
}

func TestS3StoreIntegrationBackblazeB2(t *testing.T) {
	endpoint := os.Getenv("CHUNKGATE_B2_TEST_ENDPOINT")
	bucket := os.Getenv("CHUNKGATE_B2_TEST_BUCKET")
	accessKey := os.Getenv("CHUNKGATE_B2_TEST_KEY_ID")
	secretKey := os.Getenv("CHUNKGATE_B2_TEST_APPLICATION_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set CHUNKGATE_B2_TEST_ENDPOINT, CHUNKGATE_B2_TEST_BUCKET, CHUNKGATE_B2_TEST_KEY_ID, and CHUNKGATE_B2_TEST_APPLICATION_KEY to run the B2 integration test")
	}

	secure := !strings.EqualFold(os.Getenv("CHUNKGATE_B2_TEST_USE_TLS"), "false")
	store, err := NewS3Store(S3Options{
		Endpoint:   endpoint,
		Provider:   S3ProviderB2,
		Region:     envOr("CHUNKGATE_B2_TEST_REGION", "us-west-004"),
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		Prefix:     envOr("CHUNKGATE_B2_TEST_PREFIX", "live-b2/"+time.Now().UTC().Format("20060102150405.000000000")),
		Secure:     secure,
		PathStyle:  true,
		Timeout:    15 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("create b2 s3 store failed: %v", err)
	}
	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return store
	})
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

type recordingS3Client struct {
	deleteCalls [][]string
}

func (c *recordingS3Client) PutObject(ctx context.Context, bucket string, key string, data []byte) error {
	return ctx.Err()
}

func (c *recordingS3Client) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (c *recordingS3Client) StatObject(ctx context.Context, bucket string, key string) error {
	return ctx.Err()
}

func (c *recordingS3Client) DeleteObjects(ctx context.Context, bucket string, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.deleteCalls = append(c.deleteCalls, append([]string(nil), keys...))
	return nil
}

func (c *recordingS3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return ctx.Err() == nil, ctx.Err()
}
