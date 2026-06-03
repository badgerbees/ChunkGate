package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
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

type serverOption func(*serverTestConfig)

type serverTestConfig struct {
	auth *s3auth.Verifier
}

func testServer(t *testing.T, options ...serverOption) *Server {
	t.Helper()
	var cfg serverTestConfig
	for _, option := range options {
		option(&cfg)
	}
	service := object.NewService(object.Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: backend.NewFileStore(t.TempDir()),
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(2),
	})
	apiOptions := []Option{}
	if cfg.auth != nil {
		apiOptions = append(apiOptions, WithAuthVerifier(cfg.auth))
	}
	return NewServer(service, multipart.NewManager(t.TempDir(), limits.NewDiskReservations(1024*1024)), apiOptions...)
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
	return string(rune('0' + value))
}
