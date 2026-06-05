package backend

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func TestAzureBlockKeyUsesTenantIsolatedPrefix(t *testing.T) {
	store := &AzureBlockStore{prefix: cleanS3Prefix("chunkgate/root")}
	key, err := store.blockKey("tenant/a", testBlockHash)
	if err != nil {
		t.Fatalf("block key failed: %v", err)
	}
	if key != "chunkgate/root/tenants/tenant_a/blocks/01/"+testBlockHash {
		t.Fatalf("key = %q", key)
	}
}

func TestAzureServiceURLDefaultsFromAccountName(t *testing.T) {
	got, err := azureServiceURL("", "chunkgatestorage")
	if err != nil {
		t.Fatalf("service url failed: %v", err)
	}
	if got != "https://chunkgatestorage.blob.core.windows.net" {
		t.Fatalf("url = %q", got)
	}
}

func TestAzureContainerURLPreservesAzuriteAccountPath(t *testing.T) {
	got, err := azureContainerURL("http://127.0.0.1:10000/devstoreaccount1", "chunkgate-blocks")
	if err != nil {
		t.Fatalf("container url failed: %v", err)
	}
	if got != "http://127.0.0.1:10000/devstoreaccount1/chunkgate-blocks" {
		t.Fatalf("url = %q", got)
	}
}

func TestNormalizeAzureAuth(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", azureAuthAuto},
		{"auto", azureAuthAuto},
		{"shared-key", azureAuthSharedKey},
		{"default", azureAuthDefault},
	} {
		if got := normalizeAzureAuth(tc.in); got != tc.want {
			t.Fatalf("normalizeAzureAuth(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWrapAzureErrorClassifiesNotFoundAndUnavailable(t *testing.T) {
	notFound := &azcore.ResponseError{ErrorCode: azureErrorBlobNotFound, StatusCode: http.StatusNotFound}
	if err := wrapAzureError("get block", "key", notFound); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("not found err = %v", err)
	}

	unavailable := &azcore.ResponseError{ErrorCode: "ServerBusy", StatusCode: http.StatusServiceUnavailable}
	if err := wrapAzureError("put block", "key", unavailable); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("unavailable err = %v", err)
	}
}

func TestAzureBlockStoreIntegrationAzurite(t *testing.T) {
	endpoint := os.Getenv("CHUNKGATE_AZURE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set CHUNKGATE_AZURE_TEST_ENDPOINT to run the Azurite integration test")
	}
	accountName := envOr("CHUNKGATE_AZURE_TEST_ACCOUNT_NAME", "devstoreaccount1")
	accountKey := envOr("CHUNKGATE_AZURE_TEST_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
	containerName := envOr("CHUNKGATE_AZURE_TEST_CONTAINER", "chunkgate-test")

	createAzureTestContainer(t, endpoint, accountName, accountKey, containerName)

	store, err := NewAzureBlockStore(AzureOptions{
		AccountName: accountName,
		AccountKey:  accountKey,
		Endpoint:    endpoint,
		Container:   containerName,
		Prefix:      "integration/" + time.Now().UTC().Format("20060102150405.000000000"),
		Auth:        azureAuthSharedKey,
		Timeout:     10 * time.Second,
		MaxRetries:  2,
	})
	if err != nil {
		t.Fatalf("create azure store failed: %v", err)
	}
	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return store
	})
}

func createAzureTestContainer(t *testing.T, endpoint string, accountName string, accountKey string, containerName string) {
	t.Helper()
	serviceURL, err := azureServiceURL(endpoint, accountName)
	if err != nil {
		t.Fatalf("azure service url failed: %v", err)
	}
	containerURL, err := azureContainerURL(serviceURL, containerName)
	if err != nil {
		t.Fatalf("azure container url failed: %v", err)
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Fatalf("create azure shared key failed: %v", err)
	}
	client, err := container.NewClientWithSharedKeyCredential(containerURL, cred, nil)
	if err != nil {
		t.Fatalf("create azure container client failed: %v", err)
	}
	_, err = client.Create(context.Background(), nil)
	if err == nil {
		return
	}
	var response *azcore.ResponseError
	if errors.As(err, &response) && strings.EqualFold(response.ErrorCode, "ContainerAlreadyExists") {
		return
	}
	t.Fatalf("create azure test container failed: %v", err)
}
