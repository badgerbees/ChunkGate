package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
)

func TestSwiftBlockKeyUsesTenantIsolatedPrefix(t *testing.T) {
	store := &SwiftBlockStore{prefix: cleanS3Prefix("chunkgate/root")}
	key, err := store.blockKey("tenant/a", testBlockHash)
	if err != nil {
		t.Fatalf("block key failed: %v", err)
	}
	if key != "chunkgate/root/tenants/tenant_a/blocks/01/"+testBlockHash {
		t.Fatalf("key = %q", key)
	}
}

func TestNormalizeSwiftAuth(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", swiftAuthAuto},
		{"auto", swiftAuthAuto},
		{"password", swiftAuthPassword},
		{"application-credential", swiftAuthApplicationCred},
	} {
		if got := normalizeSwiftAuth(tc.in); got != tc.want {
			t.Fatalf("normalizeSwiftAuth(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSwiftAuthOptions(t *testing.T) {
	password, err := swiftAuthOptions(SwiftOptions{
		AuthURL:     "https://identity.example.com/v3",
		Username:    "chunkgate",
		Password:    "secret",
		ProjectName: "service",
		DomainName:  "Default",
	}, swiftAuthPassword)
	if err != nil {
		t.Fatalf("password auth options failed: %v", err)
	}
	if password.Username != "chunkgate" || password.TenantName != "service" || !password.AllowReauth {
		t.Fatalf("password auth options = %#v", password)
	}

	appCred, err := swiftAuthOptions(SwiftOptions{
		AuthURL:                     "https://identity.example.com/v3",
		ApplicationCredentialID:     "app-id",
		ApplicationCredentialSecret: "app-secret",
		ProjectID:                   "project-id",
	}, swiftAuthApplicationCred)
	if err != nil {
		t.Fatalf("application credential options failed: %v", err)
	}
	if appCred.ApplicationCredentialID != "app-id" || appCred.ApplicationCredentialSecret != "app-secret" || appCred.TenantID != "project-id" {
		t.Fatalf("application credential options = %#v", appCred)
	}

	if _, err := swiftAuthOptions(SwiftOptions{AuthURL: "https://identity.example.com/v3"}, swiftAuthPassword); err == nil {
		t.Fatal("expected password auth without username to fail")
	}
	if _, err := swiftAuthOptions(SwiftOptions{
		AuthURL:                 "https://identity.example.com/v3",
		ApplicationCredentialID: "app-id",
	}, swiftAuthApplicationCred); err == nil {
		t.Fatal("expected application credential without secret to fail")
	}
}

func TestCanonicalSwiftEndpoint(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     string
	}{
		{"https://swift.example.com/v1/AUTH_project", "https://swift.example.com/v1/AUTH_project/"},
		{"http://127.0.0.1:8080/v1/AUTH_project/", "http://127.0.0.1:8080/v1/AUTH_project/"},
	} {
		got, err := canonicalSwiftEndpoint(tc.endpoint)
		if err != nil {
			t.Fatalf("canonical endpoint failed: %v", err)
		}
		if got != tc.want {
			t.Fatalf("endpoint = %q, want %q", got, tc.want)
		}
	}
	if _, err := canonicalSwiftEndpoint("ftp://swift.example.com/v1/AUTH_project"); err == nil {
		t.Fatal("expected invalid swift endpoint scheme to fail")
	}
}

func TestWrapSwiftErrorClassifiesNotFoundAndUnavailable(t *testing.T) {
	notFound := gophercloud.ErrUnexpectedResponseCode{Actual: http.StatusNotFound}
	if err := wrapSwiftError("get block", "key", notFound); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("not found err = %v", err)
	}

	unavailable := gophercloud.ErrUnexpectedResponseCode{Actual: http.StatusServiceUnavailable}
	if err := wrapSwiftError("put block", "key", unavailable); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("unavailable err = %v", err)
	}
}

func TestSwiftBlockStoreContractFakeSwift(t *testing.T) {
	RunBlockStoreContract(t, func(t *testing.T) BlockStore {
		t.Helper()
		return newFakeSwiftStore(t, true)
	})
}

func TestSwiftBulkDeleteFallsBackToSingleObjectDeletes(t *testing.T) {
	store := newFakeSwiftStore(t, false)
	ctx := context.Background()
	for _, hash := range []string{contractHashA, contractHashB} {
		if err := store.PutBlock(ctx, "tenant-a", hash, []byte("payload")); err != nil {
			t.Fatalf("put block failed: %v", err)
		}
	}
	if err := store.DeleteBlocks(ctx, "tenant-a", []string{contractHashA, contractHashB}); err != nil {
		t.Fatalf("delete blocks failed: %v", err)
	}
	for _, hash := range []string{contractHashA, contractHashB} {
		ok, err := store.HasBlock(ctx, "tenant-a", hash)
		if err != nil {
			t.Fatalf("has block failed: %v", err)
		}
		if ok {
			t.Fatalf("block %s still exists", hash)
		}
	}
}

func newFakeSwiftStore(t *testing.T, bulkDelete bool) *SwiftBlockStore {
	t.Helper()
	state := &fakeSwiftState{
		container:  "chunkgate-test",
		objects:    map[string][]byte{},
		bulkDelete: bulkDelete,
	}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)

	provider := &gophercloud.ProviderClient{HTTPClient: *server.Client()}
	provider.SetToken("test-token")
	endpoint := server.URL + "/v1/AUTH_test/"
	client := &gophercloud.ServiceClient{
		ProviderClient: provider,
		Endpoint:       endpoint,
		ResourceBase:   endpoint,
	}
	return &SwiftBlockStore{
		client:         client,
		container:      state.container,
		prefix:         "contract/" + time.Now().UTC().Format("20060102150405.000000000") + "/",
		timeout:        5 * time.Second,
		maxRetries:     0,
		bulkDeleteSize: defaultSwiftBulkDeleteSize,
		deleteJobs:     defaultSwiftFallbackDeletes,
	}
}

type fakeSwiftState struct {
	mu         sync.Mutex
	container  string
	objects    map[string][]byte
	bulkDelete bool
}

func (s *fakeSwiftState) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Auth-Token") == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/AUTH_test/" && r.URL.Query().Get("bulk-delete") == "true" {
		s.handleBulkDelete(w, r)
		return
	}
	containerPath := "/v1/AUTH_test/" + url.PathEscape(s.container)
	escapedPath := r.URL.EscapedPath()
	if escapedPath == containerPath {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
		return
	}
	objectPrefix := containerPath + "/"
	if !strings.HasPrefix(escapedPath, objectPrefix) {
		http.NotFound(w, r)
		return
	}
	objectName, err := url.PathUnescape(strings.TrimPrefix(escapedPath, objectPrefix))
	if err != nil {
		http.Error(w, "bad object name", http.StatusBadRequest)
		return
	}
	s.handleObject(w, r, objectName)
}

func (s *fakeSwiftState) handleObject(w http.ResponseWriter, r *http.Request, objectName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		s.objects[objectName] = data
		w.WriteHeader(http.StatusCreated)
	case http.MethodHead:
		if _, ok := s.objects[objectName]; !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		data, ok := s.objects[objectName]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	case http.MethodDelete:
		if _, ok := s.objects[objectName]; !ok {
			http.NotFound(w, r)
			return
		}
		delete(s.objects, objectName)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *fakeSwiftState) handleBulkDelete(w http.ResponseWriter, r *http.Request) {
	if !s.bulkDelete {
		http.Error(w, "bulk delete disabled", http.StatusMethodNotAllowed)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusInternalServerError)
		return
	}
	deleted := 0
	notFound := 0
	s.mu.Lock()
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		container, object, ok := strings.Cut(line, "/")
		if !ok || container != url.PathEscape(s.container) {
			continue
		}
		name, err := url.PathUnescape(object)
		if err != nil {
			continue
		}
		if _, ok := s.objects[name]; ok {
			delete(s.objects, name)
			deleted++
		} else {
			notFound++
		}
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"Response Status":  "200 OK",
		"Response Body":    "",
		"Errors":           [][]string{},
		"Number Deleted":   deleted,
		"Number Not Found": notFound,
	})
}
