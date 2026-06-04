package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestServerWithS3BackendMinIO(t *testing.T) {
	endpoint := os.Getenv("CHUNKGATE_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set CHUNKGATE_S3_TEST_ENDPOINT to run the MinIO API integration test")
	}

	ctx := context.Background()
	bucket := integrationEnv("CHUNKGATE_S3_TEST_BUCKET", "chunkgate-test")
	accessKey := integrationEnv("CHUNKGATE_S3_TEST_ACCESS_KEY_ID", "minioadmin")
	secretKey := integrationEnv("CHUNKGATE_S3_TEST_SECRET_ACCESS_KEY", "minioadmin")
	region := integrationEnv("CHUNKGATE_S3_TEST_REGION", "us-east-1")
	secure := strings.EqualFold(os.Getenv("CHUNKGATE_S3_TEST_USE_TLS"), "true")
	ensureMinIOBucket(t, ctx, endpoint, secure, region, bucket, accessKey, secretKey)

	store := metadata.NewMemoryStore()
	blocks, err := backend.NewS3Store(backend.S3Options{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		Prefix:     "api-integration/" + time.Now().UTC().Format("20060102150405.000000000"),
		Secure:     secure,
		PathStyle:  true,
		Timeout:    10 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("create s3 backend failed: %v", err)
	}
	server := NewServer(object.NewService(object.Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: blocks,
		Store:   store,
		CPU:     limits.NewCPUSemaphore(2),
	}), multipart.NewManager(t.TempDir(), limits.NewDiskReservations(1024*1024)), WithAnonymousTenant("default"))

	payload := strings.Repeat("0123456789abcdef", 8)
	put := httptest.NewRequest(http.MethodPut, "/live/object.txt", strings.NewReader(payload))
	put.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/live/object.txt", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, get)
	if w.Code != http.StatusOK || w.Body.String() != payload {
		t.Fatalf("get status = %d body len = %d", w.Code, w.Body.Len())
	}

	ranged := httptest.NewRequest(http.MethodGet, "/live/object.txt", nil)
	ranged.Header.Set("Range", "bytes=7-31")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, ranged)
	if w.Code != http.StatusPartialContent || w.Body.String() != payload[7:32] {
		t.Fatalf("range status = %d body = %q", w.Code, w.Body.String())
	}

	initReq := httptest.NewRequest(http.MethodPost, "/live/multipart.bin?uploads", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, initReq)
	if w.Code != http.StatusOK {
		t.Fatalf("init multipart status = %d body = %s", w.Code, w.Body.String())
	}
	var initResult initiateMultipartUploadResult
	if err := xml.Unmarshal(w.Body.Bytes(), &initResult); err != nil {
		t.Fatalf("decode multipart init failed: %v", err)
	}
	partETags := map[int]string{}
	for _, part := range []struct {
		number int
		body   string
	}{
		{2, "world-through-minio"},
		{1, "hello-"},
	} {
		req := httptest.NewRequest(http.MethodPut, "/live/multipart.bin?uploadId="+initResult.UploadID+"&partNumber="+strconvItoa(part.number), strings.NewReader(part.body))
		w = httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d body = %s", part.number, w.Code, w.Body.String())
		}
		partETags[part.number] = w.Header().Get("ETag")
	}
	completeBody := bytes.NewBufferString(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + partETags[1] + `</ETag></Part><Part><PartNumber>2</PartNumber><ETag>` + partETags[2] + `</ETag></Part></CompleteMultipartUpload>`)
	complete := httptest.NewRequest(http.MethodPost, "/live/multipart.bin?uploadId="+initResult.UploadID, completeBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, complete)
	if w.Code != http.StatusOK {
		t.Fatalf("complete multipart status = %d body = %s", w.Code, w.Body.String())
	}
	getMultipart := httptest.NewRequest(http.MethodGet, "/live/multipart.bin", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, getMultipart)
	if w.Code != http.StatusOK || w.Body.String() != "hello-world-through-minio" {
		t.Fatalf("multipart get status = %d body = %q", w.Code, w.Body.String())
	}

	for _, key := range []string{"object.txt", "multipart.bin"} {
		req := httptest.NewRequest(http.MethodDelete, "/live/"+key, nil)
		w = httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusNoContent {
			t.Fatalf("delete %s status = %d body = %s", key, w.Code, w.Body.String())
		}
	}
	beforeSweep, err := store.ListUnreferencedBlocks(ctx, "default", 100)
	if err != nil {
		t.Fatalf("list unreferenced before sweep failed: %v", err)
	}
	if len(beforeSweep) == 0 {
		t.Fatal("expected unreferenced blocks before sweep")
	}
	result, err := (gc.Sweeper{
		Store:        store,
		Backend:      blocks,
		MinOrphanAge: 0,
		BatchSize:    1000,
		MaxRetries:   2,
	}).Sweep(ctx)
	if err != nil {
		t.Fatalf("gc sweep failed: %v", err)
	}
	if result.DeletedBlocks != len(beforeSweep) {
		t.Fatalf("deleted blocks = %d, want %d", result.DeletedBlocks, len(beforeSweep))
	}
	afterSweep, err := store.ListUnreferencedBlocks(ctx, "default", 100)
	if err != nil {
		t.Fatalf("list unreferenced after sweep failed: %v", err)
	}
	if len(afterSweep) != 0 {
		t.Fatalf("unreferenced after sweep = %#v, want none", afterSweep)
	}
}

func ensureMinIOBucket(t *testing.T, ctx context.Context, endpoint string, secure bool, region string, bucket string, accessKey string, secretKey string) {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       secure,
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("create minio client failed: %v", err)
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("check minio bucket failed: %v", err)
	}
	if exists {
		return
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		created, checkErr := client.BucketExists(ctx, bucket)
		if checkErr != nil {
			t.Fatalf("make minio bucket failed: %v; recheck bucket failed: %v", err, checkErr)
		}
		if created {
			return
		}
		t.Fatalf("make minio bucket failed: %v", err)
	}
}

func integrationEnv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
