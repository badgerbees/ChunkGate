package backend

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestGCSBlockKeyUsesTenantIsolatedPrefix(t *testing.T) {
	store := &GCSBlockStore{prefix: cleanS3Prefix("chunkgate/root")}
	key, err := store.blockKey("tenant/a", testBlockHash)
	if err != nil {
		t.Fatalf("block key failed: %v", err)
	}
	if key != "chunkgate/root/tenants/tenant_a/blocks/01/"+testBlockHash {
		t.Fatalf("key = %q", key)
	}
}

func TestNormalizeGCSAuth(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", gcsAuthAuto},
		{"auto", gcsAuthAuto},
		{"service-account", gcsAuthServiceAccount},
		{"default", gcsAuthDefault},
		{"emulator", gcsAuthEmulator},
	} {
		if got := normalizeGCSAuth(tc.in); got != tc.want {
			t.Fatalf("normalizeGCSAuth(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestGCSClientOptionsSelectsExpectedAuth(t *testing.T) {
	if _, err := gcsClientOptions(GCSOptions{
		Bucket:   "chunkgate-blocks",
		Endpoint: "http://127.0.0.1:4443/storage/v1/",
		Auth:     gcsAuthAuto,
	}); err != nil {
		t.Fatalf("auto emulator options failed: %v", err)
	}
	if _, err := gcsClientOptions(GCSOptions{
		Bucket:          "chunkgate-blocks",
		Auth:            gcsAuthServiceAccount,
		CredentialsJSON: `{"type":"service_account"}`,
	}); err != nil {
		t.Fatalf("service account json options failed: %v", err)
	}
	if _, err := gcsClientOptions(GCSOptions{
		Bucket:          "chunkgate-blocks",
		Auth:            gcsAuthServiceAccount,
		CredentialsFile: "service-account.json",
		CredentialsJSON: `{"type":"service_account"}`,
	}); err == nil {
		t.Fatal("expected mutually exclusive gcs credentials to fail")
	}
	if _, err := gcsClientOptions(GCSOptions{
		Bucket:   "chunkgate-blocks",
		Endpoint: "://bad",
		Auth:     gcsAuthEmulator,
	}); err == nil {
		t.Fatal("expected invalid endpoint to fail")
	}
}

func TestCanonicalGCSEndpointPreservesJSONAPIPath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
		auth     string
		want     string
	}{
		{
			name:     "base path gets trailing slash",
			endpoint: "http://127.0.0.1:4443/storage/v1",
			auth:     gcsAuthEmulator,
			want:     "http://127.0.0.1:4443/storage/v1/",
		},
		{
			name:     "host only emulator gets json path",
			endpoint: "http://127.0.0.1:4443",
			auth:     gcsAuthEmulator,
			want:     "http://127.0.0.1:4443/storage/v1/",
		},
		{
			name:     "existing slash is preserved",
			endpoint: "http://127.0.0.1:4443/storage/v1/",
			auth:     gcsAuthEmulator,
			want:     "http://127.0.0.1:4443/storage/v1/",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalGCSEndpoint(tc.endpoint, tc.auth)
			if err != nil {
				t.Fatalf("canonical endpoint failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("endpoint = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWrapGCSErrorClassifiesNotFoundAndUnavailable(t *testing.T) {
	if err := wrapGCSError("get block", "key", storage.ErrObjectNotExist); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("not found err = %v", err)
	}

	if err := wrapGCSError("check bucket", "bucket", storage.ErrBucketNotExist); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("bucket missing err = %v", err)
	}

	unavailable := &googleapi.Error{Code: http.StatusServiceUnavailable}
	if err := wrapGCSError("put block", "key", unavailable); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("unavailable err = %v", err)
	}
}

func TestGCSBlockStoreIntegrationFakeGCSServer(t *testing.T) {
	endpoint := os.Getenv("CHUNKGATE_GCS_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set CHUNKGATE_GCS_TEST_ENDPOINT to run the fake-gcs-server integration test")
	}
	bucketName := envOr("CHUNKGATE_GCS_TEST_BUCKET", "chunkgate-test")
	projectID := envOr("CHUNKGATE_GCS_TEST_PROJECT_ID", "chunkgate-test")

	createGCSTestBucket(t, endpoint, projectID, bucketName)

	store, err := NewGCSBlockStore(context.Background(), GCSOptions{
		ProjectID:  projectID,
		Bucket:     bucketName,
		Endpoint:   endpoint,
		Prefix:     "integration/" + time.Now().UTC().Format("20060102150405.000000000"),
		Auth:       gcsAuthEmulator,
		Timeout:    10 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("create gcs store failed: %v", err)
	}
	defer store.client.Close()

	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return store
	})
}

func createGCSTestBucket(t *testing.T, endpoint string, projectID string, bucketName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		storage.WithJSONReads(),
	)
	if err != nil {
		t.Fatalf("create gcs test client failed: %v", err)
	}
	defer client.Close()
	err = client.Bucket(bucketName).Create(ctx, projectID, nil)
	if err == nil {
		return
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict {
		return
	}
	t.Fatalf("create gcs test bucket failed: %v", err)
}
