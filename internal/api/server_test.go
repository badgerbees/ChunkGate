package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/deltaclient"
	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	"github.com/chunkgate/chunkgate/internal/ops"
	"github.com/chunkgate/chunkgate/internal/s3auth"
)

func TestServerPutGetDeleteObject(t *testing.T) {
	server := testServer(t)

	put := httptest.NewRequest(http.MethodPut, "/bucket/key.txt", strings.NewReader("payload"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d, body = %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("ETag"); got != `"321c3cf486ed509164edec1e1981fec8"` {
		t.Fatalf("etag = %s", got)
	}

	get := httptest.NewRequest(http.MethodGet, "/bucket/key.txt", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, get)
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", w.Code, w.Body.String())
	}
	if w.Body.String() != "payload" {
		t.Fatalf("body = %q", w.Body.String())
	}

	del := httptest.NewRequest(http.MethodDelete, "/bucket/key.txt", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, del)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", w.Code)
	}
}

func TestServerPreservesObjectHeaders(t *testing.T) {
	server := testServer(t)

	put := httptest.NewRequest(http.MethodPut, "/bucket/meta.txt", strings.NewReader("payload"))
	put.Header.Set("Content-Type", "text/plain")
	put.Header.Set("Cache-Control", "max-age=60")
	put.Header.Set("Content-Encoding", "gzip")
	put.Header.Set("x-amz-meta-build", "123")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	head := httptest.NewRequest(http.MethodHead, "/bucket/meta.txt", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, head)
	if w.Code != http.StatusOK {
		t.Fatalf("head status = %d body = %s", w.Code, w.Body.String())
	}
	for key, want := range map[string]string{
		"Content-Type":     "text/plain",
		"Cache-Control":    "max-age=60",
		"Content-Encoding": "gzip",
		"X-Amz-Meta-Build": "123",
	} {
		if got := w.Header().Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestServerHandlesSingleRangeRequests(t *testing.T) {
	server := testServer(t)
	payload := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	putObjectForRangeTest(t, server, payload)
	total := len(payload)

	for _, tc := range []struct {
		name         string
		method       string
		rangeHeader  string
		wantStatus   int
		wantBody     string
		wantRange    string
		wantLength   string
		expectNoBody bool
	}{
		{
			name:        "prefix",
			method:      http.MethodGet,
			rangeHeader: "bytes=0-4",
			wantStatus:  http.StatusPartialContent,
			wantBody:    payload[0:5],
			wantRange:   "bytes 0-4/" + strconvItoa(total),
			wantLength:  "5",
		},
		{
			name:        "middle",
			method:      http.MethodGet,
			rangeHeader: "bytes=10-25",
			wantStatus:  http.StatusPartialContent,
			wantBody:    payload[10:26],
			wantRange:   "bytes 10-25/" + strconvItoa(total),
			wantLength:  "16",
		},
		{
			name:        "suffix",
			method:      http.MethodGet,
			rangeHeader: "bytes=-6",
			wantStatus:  http.StatusPartialContent,
			wantBody:    payload[total-6:],
			wantRange:   "bytes 56-61/" + strconvItoa(total),
			wantLength:  "6",
		},
		{
			name:        "open ended",
			method:      http.MethodGet,
			rangeHeader: "bytes=50-",
			wantStatus:  http.StatusPartialContent,
			wantBody:    payload[50:],
			wantRange:   "bytes 50-61/" + strconvItoa(total),
			wantLength:  "12",
		},
		{
			name:        "full equivalent",
			method:      http.MethodGet,
			rangeHeader: "bytes=0-61",
			wantStatus:  http.StatusPartialContent,
			wantBody:    payload,
			wantRange:   "bytes 0-61/" + strconvItoa(total),
			wantLength:  strconvItoa(total),
		},
		{
			name:         "head",
			method:       http.MethodHead,
			rangeHeader:  "bytes=1-3",
			wantStatus:   http.StatusPartialContent,
			wantBody:     "",
			wantRange:    "bytes 1-3/" + strconvItoa(total),
			wantLength:   "3",
			expectNoBody: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/bucket/range.txt", nil)
			req.Header.Set("Range", tc.rangeHeader)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
			}
			if got := w.Header().Get("Content-Range"); got != tc.wantRange {
				t.Fatalf("content-range = %q, want %q", got, tc.wantRange)
			}
			if got := w.Header().Get("Content-Length"); got != tc.wantLength {
				t.Fatalf("content-length = %q, want %q", got, tc.wantLength)
			}
			if got := w.Body.String(); got != tc.wantBody {
				t.Fatalf("body = %q, want %q", got, tc.wantBody)
			}
			if tc.expectNoBody && w.Body.Len() != 0 {
				t.Fatalf("HEAD body length = %d, want 0", w.Body.Len())
			}
		})
	}
}

func TestServerRejectsInvalidAndUnsupportedRanges(t *testing.T) {
	server := testServer(t)
	payload := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	putObjectForRangeTest(t, server, payload)

	for _, tc := range []struct {
		name        string
		rangeHeader string
	}{
		{name: "invalid order", rangeHeader: "bytes=5-1"},
		{name: "unsatisfiable", rangeHeader: "bytes=62-"},
		{name: "multi range", rangeHeader: "bytes=0-1,3-4"},
		{name: "bad unit", rangeHeader: "items=0-1"},
		{name: "zero suffix", rangeHeader: "bytes=-0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/bucket/range.txt", nil)
			req.Header.Set("Range", tc.rangeHeader)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
			}
			if got := w.Header().Get("Content-Range"); got != "bytes */"+strconvItoa(len(payload)) {
				t.Fatalf("content-range = %q", got)
			}
			if !strings.Contains(w.Body.String(), "<Code>InvalidRange</Code>") {
				t.Fatalf("body = %s, want InvalidRange", w.Body.String())
			}
		})
	}
}

func TestServerHandlesBucketLevelRoutes(t *testing.T) {
	server := testServer(t)

	for _, request := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodPut, "/bucket", http.StatusOK},
		{http.MethodHead, "/bucket", http.StatusOK},
		{http.MethodGet, "/bucket?list-type=2", http.StatusOK},
		{http.MethodDelete, "/bucket", http.StatusNoContent},
	} {
		req := httptest.NewRequest(request.method, request.path, nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != request.status {
			t.Fatalf("%s %s status = %d, want %d, body = %s", request.method, request.path, w.Code, request.status, w.Body.String())
		}
	}
}

func TestServerHandlesVirtualHostedStyleBucketRequests(t *testing.T) {
	server := testServer(t, withVirtualHosts("chunkgate.test"))

	put := httptest.NewRequest(http.MethodPut, "http://photos.chunkgate.test/cat.jpg", strings.NewReader("image-data"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("virtual-host put status = %d body = %s", w.Code, w.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "http://photos.chunkgate.test/cat.jpg", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, get)
	if w.Code != http.StatusOK {
		t.Fatalf("virtual-host get status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "image-data" {
		t.Fatalf("virtual-host body = %q", got)
	}

	bucket := httptest.NewRequest(http.MethodHead, "http://photos.chunkgate.test/", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, bucket)
	if w.Code != http.StatusOK {
		t.Fatalf("virtual-host bucket status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestServerHandlesStaticCORSPreflightAndActualRequest(t *testing.T) {
	server := testServer(t, withCORS(CORSConfig{
		AllowedOrigins:   []string{"https://app.example.com"},
		AllowedMethods:   []string{http.MethodGet, http.MethodPut},
		AllowedHeaders:   []string{"authorization", "x-amz-date"},
		ExposedHeaders:   []string{"ETag", "x-amz-request-id"},
		AllowCredentials: true,
		MaxAgeSeconds:    600,
	}))

	preflight := httptest.NewRequest(http.MethodOptions, "/bucket/key.txt", nil)
	preflight.Header.Set("Origin", "https://app.example.com")
	preflight.Header.Set("Access-Control-Request-Method", http.MethodPut)
	preflight.Header.Set("Access-Control-Request-Headers", "authorization, x-amz-date")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, preflight)
	if w.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d body = %s", w.Code, w.Body.String())
	}
	for key, want := range map[string]string{
		"Access-Control-Allow-Origin":      "https://app.example.com",
		"Access-Control-Allow-Credentials": "true",
		"Access-Control-Allow-Methods":     "GET, PUT",
		"Access-Control-Allow-Headers":     "authorization, x-amz-date",
		"Access-Control-Max-Age":           "600",
	} {
		if got := w.Header().Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}

	put := httptest.NewRequest(http.MethodPut, "/bucket/key.txt", strings.NewReader("payload"))
	put.Header.Set("Origin", "https://app.example.com")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("cors put status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("actual allow origin = %q", got)
	}
	if got := w.Header().Get("Access-Control-Expose-Headers"); got != "ETag, x-amz-request-id" {
		t.Fatalf("expose headers = %q", got)
	}
}

func TestServerWritesUniqueRequestIDs(t *testing.T) {
	server := testServer(t)

	req := httptest.NewRequest(http.MethodGet, "/bucket/missing.txt", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	first := w.Header().Get("x-amz-request-id")
	if first == "" || !strings.Contains(w.Body.String(), "<RequestId>"+first+"</RequestId>") {
		t.Fatalf("first request id header/body mismatch: header=%q body=%s", first, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/bucket/missing.txt", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	second := w.Header().Get("x-amz-request-id")
	if second == "" || second == first {
		t.Fatalf("request ids should be non-empty and unique: first=%q second=%q", first, second)
	}
}

func TestServerCompletesMultipartInRequestedOrder(t *testing.T) {
	server := testServer(t)

	req := httptest.NewRequest(http.MethodPost, "/bucket/big.bin?uploads", nil)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-amz-meta-source", "multipart")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, body = %s", w.Code, w.Body.String())
	}
	var initResult initiateMultipartUploadResult
	if err := xml.Unmarshal(w.Body.Bytes(), &initResult); err != nil {
		t.Fatalf("decode initiate result failed: %v", err)
	}

	etags := map[int]string{}
	for _, part := range []struct {
		number int
		body   string
	}{
		{2, "world"},
		{1, "hello "},
	} {
		req = httptest.NewRequest(http.MethodPut, "/bucket/big.bin?uploadId="+initResult.UploadID+"&partNumber="+strconvItoa(part.number), strings.NewReader(part.body))
		w = httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d body = %s", part.number, w.Code, w.Body.String())
		}
		etags[part.number] = w.Header().Get("ETag")
	}

	completeBody := bytes.NewBufferString(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etags[1] + `</ETag></Part><Part><PartNumber>2</PartNumber><ETag>` + etags[2] + `</ETag></Part></CompleteMultipartUpload>`)
	req = httptest.NewRequest(http.MethodPost, "/bucket/big.bin?uploadId="+initResult.UploadID, completeBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("complete status = %d body = %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/bucket/big.bin", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	body, _ := io.ReadAll(w.Body)
	if string(body) != "hello world" {
		t.Fatalf("multipart body = %q", body)
	}
	if got := w.Header().Get("X-Amz-Meta-Source"); got != "multipart" {
		t.Fatalf("multipart metadata = %q, want multipart", got)
	}
}

func TestServerRejectsMultipartETagMismatch(t *testing.T) {
	server := testServer(t)

	req := httptest.NewRequest(http.MethodPost, "/bucket/big.bin?uploads", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	var initResult initiateMultipartUploadResult
	if err := xml.Unmarshal(w.Body.Bytes(), &initResult); err != nil {
		t.Fatalf("decode initiate result failed: %v", err)
	}

	req = httptest.NewRequest(http.MethodPut, "/bucket/big.bin?uploadId="+initResult.UploadID+"&partNumber=1", strings.NewReader("hello"))
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("upload part status = %d body = %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/bucket/big.bin?uploadId="+initResult.UploadID, strings.NewReader(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"bad"</ETag></Part></CompleteMultipartUpload>`))
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("complete status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>InvalidPart</Code>") {
		t.Fatalf("body = %s, want InvalidPart", w.Body.String())
	}
}

func TestServerUsesSigV4AccessKeyAsTenant(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	server := testServer(t, withTestAuth(t, now,
		s3auth.Credential{AccessKey: "tenant-a", SecretKey: "secret-a"},
		s3auth.Credential{AccessKey: "tenant-b", SecretKey: "secret-b"},
	))

	put := httptest.NewRequest(http.MethodPut, "/bucket/key.txt", strings.NewReader("payload"))
	put.Header.Set("X-ChunkGate-Tenant", "tenant-b")
	signRequest(t, put, "tenant-a", "secret-a", now)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("signed put status = %d body = %s", w.Code, w.Body.String())
	}

	getAsB := httptest.NewRequest(http.MethodGet, "/bucket/key.txt", nil)
	signRequest(t, getAsB, "tenant-b", "secret-b", now)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, getAsB)
	if w.Code != http.StatusNotFound {
		t.Fatalf("tenant b get status = %d body = %s", w.Code, w.Body.String())
	}

	getAsA := httptest.NewRequest(http.MethodGet, "/bucket/key.txt", nil)
	signRequest(t, getAsA, "tenant-a", "secret-a", now)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, getAsA)
	if w.Code != http.StatusOK || w.Body.String() != "payload" {
		t.Fatalf("tenant a get status = %d body = %q", w.Code, w.Body.String())
	}
}

func TestServerRejectsMissingAndBadSignatures(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	server := testServer(t, withTestAuth(t, now, s3auth.Credential{AccessKey: "tenant-a", SecretKey: "secret-a"}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "<Code>AccessDenied</Code>") {
		t.Fatalf("missing auth status = %d body = %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	signRequest(t, req, "tenant-a", "wrong-secret", now)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "<Code>SignatureDoesNotMatch</Code>") {
		t.Fatalf("bad signature status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestServerRequiresAuthenticationUnlessAnonymousTenantIsConfigured(t *testing.T) {
	server := testServer(t, withoutAnonymousTenant())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>AccessDenied</Code>") {
		t.Fatalf("body = %s, want AccessDenied", w.Body.String())
	}
}

func TestServerRejectsInvalidBucketAndObjectNames(t *testing.T) {
	server := testServer(t)

	for _, tc := range []struct {
		name     string
		method   string
		path     string
		wantCode string
	}{
		{name: "short bucket", method: http.MethodGet, path: "/aa/key", wantCode: "InvalidBucketName"},
		{name: "uppercase bucket", method: http.MethodGet, path: "/BadBucket/key", wantCode: "InvalidBucketName"},
		{name: "ip bucket", method: http.MethodGet, path: "/127.0.0.1/key", wantCode: "InvalidBucketName"},
		{name: "nul key", method: http.MethodPut, path: "/bucket/%00bad", wantCode: "InvalidObjectName"},
		{name: "control key", method: http.MethodPut, path: "/bucket/%1Fbad", wantCode: "InvalidObjectName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("payload"))
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code == http.StatusOK || w.Code == http.StatusNoContent {
				t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "<Code>"+tc.wantCode+"</Code>") {
				t.Fatalf("body = %s, want %s", w.Body.String(), tc.wantCode)
			}
		})
	}
}

func TestServerRejectsInvalidMultipartUploadIDsAndPartNumbers(t *testing.T) {
	server := testServer(t)

	for _, tc := range []struct {
		name     string
		method   string
		path     string
		body     string
		status   int
		wantCode string
	}{
		{
			name:     "bad upload id on part",
			method:   http.MethodPut,
			path:     "/bucket/key?uploadId=../escape&partNumber=1",
			status:   http.StatusNotFound,
			wantCode: "NoSuchUpload",
		},
		{
			name:     "zero part",
			method:   http.MethodPut,
			path:     "/bucket/key?uploadId=0123456789abcdef0123456789abcdef&partNumber=0",
			status:   http.StatusBadRequest,
			wantCode: "InvalidPart",
		},
		{
			name:     "part above s3 limit",
			method:   http.MethodPut,
			path:     "/bucket/key?uploadId=0123456789abcdef0123456789abcdef&partNumber=10001",
			status:   http.StatusBadRequest,
			wantCode: "InvalidPart",
		},
		{
			name:     "non canonical part",
			method:   http.MethodPut,
			path:     "/bucket/key?uploadId=0123456789abcdef0123456789abcdef&partNumber=+1",
			status:   http.StatusBadRequest,
			wantCode: "InvalidPart",
		},
		{
			name:     "bad upload id on complete",
			method:   http.MethodPost,
			path:     "/bucket/key?uploadId=UPPERCASE0123456789abcdef0123",
			body:     `<CompleteMultipartUpload></CompleteMultipartUpload>`,
			status:   http.StatusNotFound,
			wantCode: "NoSuchUpload",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "<Code>"+tc.wantCode+"</Code>") {
				t.Fatalf("body = %s, want %s", w.Body.String(), tc.wantCode)
			}
		})
	}
}

func TestDeltaManifestAndChunkEndpoints(t *testing.T) {
	server := testServer(t)
	payload := deltaPayload()
	put := httptest.NewRequest(http.MethodPut, "/bucket/delta.bin", strings.NewReader(payload))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	manifestReq := httptest.NewRequest(http.MethodGet, "/_chunkgate/v1/manifest?bucket=bucket&key=delta.bin", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, manifestReq)
	if w.Code != http.StatusOK {
		t.Fatalf("manifest status = %d body = %s", w.Code, w.Body.String())
	}
	var manifest deltaManifestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("decode manifest failed: %v", err)
	}
	if manifest.Version != 1 || manifest.Bucket != "bucket" || manifest.Key != "delta.bin" || manifest.Size != int64(len(payload)) {
		t.Fatalf("manifest = %#v", manifest)
	}
	if len(manifest.Chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %#v", manifest.Chunks)
	}
	if manifest.ObjectMD5 == "" {
		t.Fatalf("manifest object md5 is empty: %#v", manifest)
	}

	first := manifest.Chunks[0]
	chunkReqBody := bytes.NewBufferString(`{"bucket":"bucket","key":"delta.bin","hashes":["` + first.Hash + `"]}`)
	chunkReq := httptest.NewRequest(http.MethodPost, "/_chunkgate/v1/chunks", chunkReqBody)
	chunkReq.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, chunkReq)
	if w.Code != http.StatusOK {
		t.Fatalf("chunks status = %d body = %s", w.Code, w.Body.String())
	}
	var chunks deltaChunkResponse
	if err := json.Unmarshal(w.Body.Bytes(), &chunks); err != nil {
		t.Fatalf("decode chunks failed: %v", err)
	}
	if len(chunks.Chunks) != 1 || chunks.Chunks[0].Hash != first.Hash || chunks.Chunks[0].Size != first.Size {
		t.Fatalf("chunks = %#v", chunks)
	}
	data, err := base64.StdEncoding.DecodeString(chunks.Chunks[0].Data)
	if err != nil {
		t.Fatalf("decode chunk data failed: %v", err)
	}
	if got := testSHA256Hex(data); got != first.Hash {
		t.Fatalf("chunk hash = %s, want %s", got, first.Hash)
	}

	ordinary := httptest.NewRequest(http.MethodGet, "/bucket/delta.bin", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, ordinary)
	if w.Code != http.StatusOK || w.Body.String() != payload {
		t.Fatalf("ordinary get status = %d body len = %d", w.Code, w.Body.Len())
	}
}

func TestDeltaChunkEndpointRejectsChunksOutsideManifest(t *testing.T) {
	server := testServer(t)
	put := httptest.NewRequest(http.MethodPut, "/bucket/delta.bin", strings.NewReader(deltaPayload()))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/_chunkgate/v1/chunks", strings.NewReader(`{"bucket":"bucket","key":"delta.bin","hashes":["ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"]}`))
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>InvalidChunk</Code>") {
		t.Fatalf("body = %s, want InvalidChunk", w.Body.String())
	}
}

func TestDeltaEndpointsRequireAuthenticationUnlessAnonymousTenantIsConfigured(t *testing.T) {
	server := testServer(t, withoutAnonymousTenant())

	req := httptest.NewRequest(http.MethodGet, "/_chunkgate/v1/manifest?bucket=bucket&key=delta.bin", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>AccessDenied</Code>") {
		t.Fatalf("body = %s, want AccessDenied", w.Body.String())
	}
}

func TestDeltaClientDownloadsOnlyMissingChunksAndS3GetStillWorks(t *testing.T) {
	blocks := &deltaCountingBackend{inner: backend.NewFileStore(t.TempDir())}
	server := testServer(t, withBackend(blocks))
	payload := deltaPayload()

	put := httptest.NewRequest(http.MethodPut, "/bucket/delta-client.bin", strings.NewReader(payload))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client := deltaclient.Client{Endpoint: httpServer.URL}
	ctx := context.Background()
	manifest, err := client.FetchManifest(ctx, "bucket", "delta-client.bin")
	if err != nil {
		t.Fatalf("fetch manifest failed: %v", err)
	}
	uniqueHashes := uniqueManifestHashes(manifest.Chunks)
	if len(uniqueHashes) < 2 {
		t.Fatalf("expected multiple unique chunks, got %#v", manifest.Chunks)
	}

	cache := deltaclient.Cache{Dir: t.TempDir()}
	reader, err := blocks.inner.GetBlock(ctx, "default", uniqueHashes[0])
	if err != nil {
		t.Fatalf("read seed block failed: %v", err)
	}
	seed, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read seed block failed: %v", err)
	}
	if err := cache.Put(uniqueHashes[0], seed); err != nil {
		t.Fatalf("seed cache failed: %v", err)
	}

	blocks.opened = nil
	output := t.TempDir() + "/delta-client.bin"
	result, err := client.Download(ctx, "bucket", "delta-client.bin", output, cache.Dir)
	if err != nil {
		t.Fatalf("delta download failed: %v", err)
	}
	if result.MissingChunks != len(uniqueHashes)-1 {
		t.Fatalf("missing chunks = %d, want %d", result.MissingChunks, len(uniqueHashes)-1)
	}
	if len(blocks.opened) != len(uniqueHashes)-1 {
		t.Fatalf("backend opened chunks = %#v, want %d missing chunks", blocks.opened, len(uniqueHashes)-1)
	}
	downloaded, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output failed: %v", err)
	}
	if string(downloaded) != payload {
		t.Fatalf("downloaded payload = %q", string(downloaded))
	}

	resp, err := http.Get(httpServer.URL + "/bucket/delta-client.bin")
	if err != nil {
		t.Fatalf("ordinary get failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read ordinary get failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != payload {
		t.Fatalf("ordinary get status = %d body len = %d", resp.StatusCode, len(body))
	}
}

func TestDeltaClientSignsCompanionRequests(t *testing.T) {
	now := time.Now().UTC()
	server := testServer(t, withTestAuth(t, now, s3auth.Credential{AccessKey: "tenant-a", SecretKey: "secret-a"}))
	payload := deltaPayload()

	put := httptest.NewRequest(http.MethodPut, "/bucket/signed-delta.bin", strings.NewReader(payload))
	signRequest(t, put, "tenant-a", "secret-a", now)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("signed put status = %d body = %s", w.Code, w.Body.String())
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client := deltaclient.Client{
		Endpoint:  httpServer.URL,
		AccessKey: "tenant-a",
		SecretKey: "secret-a",
	}
	output := t.TempDir() + "/signed-delta.bin"
	if _, err := client.Download(context.Background(), "bucket", "signed-delta.bin", output, t.TempDir()); err != nil {
		t.Fatalf("signed delta download failed: %v", err)
	}
	downloaded, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read signed output failed: %v", err)
	}
	if string(downloaded) != payload {
		t.Fatalf("downloaded payload = %q", string(downloaded))
	}
}

func TestWriteInternalErrorMapsBackendErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "missing block", err: backend.ErrBlockNotFound, wantStatus: http.StatusNotFound, wantCode: "NoSuchKey"},
		{name: "backend unavailable", err: backend.ErrBackendUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: "ServiceUnavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeInternalError(w, tc.err)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "<Code>"+tc.wantCode+"</Code>") {
				t.Fatalf("body = %s, want %s", w.Body.String(), tc.wantCode)
			}
		})
	}
}

func TestServerExposesGCMetrics(t *testing.T) {
	metrics := gc.NewMetrics()
	metrics.Observe(gc.Result{ScannedTenants: 2, CandidateBlocks: 3, DeletedBlocks: 1})
	server := testServer(t, withGCMetrics(metrics))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body = %s", w.Code, w.Body.String())
	}
	for _, want := range []string{
		"chunkgate_gc_runs_total 1",
		"chunkgate_gc_scanned_tenants_total 2",
		"chunkgate_gc_candidate_blocks_total 3",
		"chunkgate_gc_deleted_blocks_total 1",
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("metrics body = %s, want %s", w.Body.String(), want)
		}
	}
}

func TestServerRejectsOversizeObjectBody(t *testing.T) {
	server := testServer(t, withBodyLimits(4, 0, 0))

	req := httptest.NewRequest(http.MethodPut, "/bucket/too-large.txt", strings.NewReader("12345"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>EntityTooLarge</Code>") {
		t.Fatalf("body = %s, want EntityTooLarge", w.Body.String())
	}
}

func TestServerRejectsUploadsWhileDraining(t *testing.T) {
	drain := &ops.Drain{}
	server := testServer(t, withDrain(drain))
	drain.Start()

	req := httptest.NewRequest(http.MethodPut, "/bucket/key.txt", strings.NewReader("payload"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<Code>ServiceUnavailable</Code>") {
		t.Fatalf("body = %s, want ServiceUnavailable", w.Body.String())
	}
}

func TestServerReadinessChecksDependenciesAndDrainState(t *testing.T) {
	drain := &ops.Drain{}
	server := testServer(t, withDrain(drain))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ready status = %d body = %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"status":"ready"`, `"metadata":"ok"`, `"backend":"ok"`, `"scratch":"ok"`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("ready body = %s, want %s", w.Body.String(), want)
		}
	}

	drain.Start()
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining ready status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"status":"not_ready"`) || !strings.Contains(w.Body.String(), `"drain":"draining"`) {
		t.Fatalf("draining body = %s", w.Body.String())
	}
}

func TestServerReadinessReportsBackendFailure(t *testing.T) {
	path := t.TempDir() + "/not-a-directory"
	if err := os.WriteFile(path, []byte("file"), 0o644); err != nil {
		t.Fatalf("create blocking file failed: %v", err)
	}
	server := testServer(t, withBackend(backend.NewFileStore(path)))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"backend"`) {
		t.Fatalf("body = %s, want backend failure", w.Body.String())
	}
}

func TestServerPprofEndpointIsGated(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	w := httptest.NewRecorder()
	testServer(t).ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("disabled pprof status = %d", w.Code)
	}

	w = httptest.NewRecorder()
	testServer(t, withPprof()).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("enabled pprof status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestServerExposesOperationalMetrics(t *testing.T) {
	metrics := ops.NewMetrics()
	server := testServer(t, withOpsMetrics(metrics))

	put := httptest.NewRequest(http.MethodPut, "/bucket/key.txt", strings.NewReader("payload"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, put)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body = %s", w.Code, w.Body.String())
	}
	for _, want := range []string{
		"chunkgate_http_requests_total",
		"chunkgate_uploads_total 1",
		"chunkgate_uploaded_bytes_total 7",
		"chunkgate_chunks_total",
		"chunkgate_chunk_limiter_limit",
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("metrics body = %s, want %s", w.Body.String(), want)
		}
	}
}

type deltaCountingBackend struct {
	inner  *backend.FileStore
	opened []string
}

func (b *deltaCountingBackend) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	return b.inner.PutBlock(ctx, tenant, hash, data)
}

func (b *deltaCountingBackend) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	b.opened = append(b.opened, hash)
	return b.inner.GetBlock(ctx, tenant, hash)
}

func (b *deltaCountingBackend) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	return b.inner.DeleteBlocks(ctx, tenant, hashes)
}

func (b *deltaCountingBackend) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	return b.inner.HasBlock(ctx, tenant, hash)
}

func (b *deltaCountingBackend) HealthCheck(ctx context.Context) error {
	return b.inner.HealthCheck(ctx)
}

func deltaPayload() string {
	var b strings.Builder
	for i := 0; i < 256; i++ {
		b.WriteString(strconvItoa(i))
		b.WriteString(":chunkgate-delta-protocol;")
	}
	return b.String()
}

func uniqueManifestHashes(chunks []deltaclient.ManifestChunk) []string {
	seen := map[string]bool{}
	hashes := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if seen[chunk.Hash] {
			continue
		}
		seen[chunk.Hash] = true
		hashes = append(hashes, chunk.Hash)
	}
	return hashes
}

type serverOption func(*serverTestConfig)

type serverTestConfig struct {
	auth       *s3auth.Verifier
	gcMetrics  *gc.Metrics
	opsMetrics *ops.Metrics
	drain      *ops.Drain
	backend    backend.BlockStore
	maxObject  int64
	maxPart    int64
	maxXML     int64
	pprof      bool
	anonymous  *string
	virtual    []string
	cors       CORSConfig
}

func testServer(t *testing.T, options ...serverOption) *Server {
	t.Helper()
	var cfg serverTestConfig
	for _, option := range options {
		option(&cfg)
	}
	metrics := cfg.opsMetrics
	if metrics == nil {
		metrics = ops.NewMetrics()
	}
	limiter := limits.NewAdaptiveCPUSemaphore(2, 0)
	blocks := cfg.backend
	if blocks == nil {
		blocks = backend.NewFileStore(t.TempDir())
	}
	service := object.NewService(object.Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: blocks,
		Store:   metadata.NewMemoryStore(),
		CPU:     limiter,
		Metrics: metrics,
	})
	apiOptions := []Option{
		WithMetrics(metrics),
		WithLimiter(limiter),
		WithBodyLimits(cfg.maxObject, cfg.maxPart, cfg.maxXML),
	}
	anonymousTenant := "default"
	if cfg.anonymous != nil {
		anonymousTenant = *cfg.anonymous
	}
	if anonymousTenant != "" {
		apiOptions = append(apiOptions, WithAnonymousTenant(anonymousTenant))
	}
	if cfg.auth != nil {
		apiOptions = append(apiOptions, WithAuthVerifier(cfg.auth))
	}
	if cfg.gcMetrics != nil {
		apiOptions = append(apiOptions, WithGCMetrics(cfg.gcMetrics))
	}
	if cfg.drain != nil {
		apiOptions = append(apiOptions, WithDrain(cfg.drain))
	}
	if cfg.pprof {
		apiOptions = append(apiOptions, WithPprof(true))
	}
	if len(cfg.virtual) > 0 {
		apiOptions = append(apiOptions, WithVirtualHosts(cfg.virtual...))
	}
	if len(cfg.cors.AllowedOrigins) > 0 {
		apiOptions = append(apiOptions, WithCORS(cfg.cors))
	}
	return NewServer(service, multipart.NewManager(t.TempDir(), limits.NewDiskReservations(1024*1024)), apiOptions...)
}

func putObjectForRangeTest(t *testing.T, server *Server, payload string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/bucket/range.txt", strings.NewReader(payload))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d body = %s", w.Code, w.Body.String())
	}
}

func withTestAuth(t *testing.T, now time.Time, credentials ...s3auth.Credential) serverOption {
	t.Helper()
	verifier, err := s3auth.NewVerifier(credentials)
	if err != nil {
		t.Fatalf("new verifier failed: %v", err)
	}
	verifier.Now = func() time.Time { return now }
	return func(config *serverTestConfig) {
		config.auth = verifier
	}
}

func withGCMetrics(metrics *gc.Metrics) serverOption {
	return func(config *serverTestConfig) {
		config.gcMetrics = metrics
	}
}

func withOpsMetrics(metrics *ops.Metrics) serverOption {
	return func(config *serverTestConfig) {
		config.opsMetrics = metrics
	}
}

func withDrain(drain *ops.Drain) serverOption {
	return func(config *serverTestConfig) {
		config.drain = drain
	}
}

func withBackend(blocks backend.BlockStore) serverOption {
	return func(config *serverTestConfig) {
		config.backend = blocks
	}
}

func withBodyLimits(maxObject int64, maxPart int64, maxXML int64) serverOption {
	return func(config *serverTestConfig) {
		config.maxObject = maxObject
		config.maxPart = maxPart
		config.maxXML = maxXML
	}
}

func withPprof() serverOption {
	return func(config *serverTestConfig) {
		config.pprof = true
	}
}

func withVirtualHosts(hosts ...string) serverOption {
	return func(config *serverTestConfig) {
		config.virtual = hosts
	}
}

func withCORS(cors CORSConfig) serverOption {
	return func(config *serverTestConfig) {
		config.cors = cors
	}
}

func withoutAnonymousTenant() serverOption {
	return func(config *serverTestConfig) {
		empty := ""
		config.anonymous = &empty
	}
}

func signRequest(t *testing.T, req *http.Request, accessKey string, secretKey string, now time.Time) {
	t.Helper()
	req.Header.Set("X-Amz-Date", now.UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	scope := now.UTC().Format("20060102") + "/us-east-1/s3/aws4_request"
	canonical := strings.Join([]string{
		req.Method,
		testCanonicalURI(req.URL),
		testCanonicalQueryString(req.URL),
		"host:" + req.Host + "\n" +
			"x-amz-content-sha256:" + req.Header.Get("X-Amz-Content-Sha256") + "\n" +
			"x-amz-date:" + req.Header.Get("X-Amz-Date") + "\n",
		signedHeaders,
		"UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		req.Header.Get("X-Amz-Date"),
		scope,
		testSHA256Hex([]byte(canonical)),
	}, "\n")
	signingKey := testSigningKey(secretKey, now.UTC().Format("20060102"), "us-east-1", "s3")
	signature := hex.EncodeToString(testHMAC(signingKey, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func testCanonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func testCanonicalQueryString(u *url.URL) string {
	values := u.Query()
	pairs := make([]string, 0)
	for name, vals := range values {
		for _, value := range vals {
			pairs = append(pairs, testAWSEncode(name)+"="+testAWSEncode(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func testSigningKey(secret string, date string, region string, service string) []byte {
	dateKey := testHMAC([]byte("AWS4"+secret), []byte(date))
	regionKey := testHMAC(dateKey, []byte(region))
	serviceKey := testHMAC(regionKey, []byte(service))
	return testHMAC(serviceKey, []byte("aws4_request"))
}

func testHMAC(key []byte, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return mac.Sum(nil)
}

func testSHA256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func testAWSEncode(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{c})))
	}
	return b.String()
}

func strconvItoa(value int) string {
	return strconv.Itoa(value)
}
