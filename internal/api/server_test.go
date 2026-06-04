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
	"os"
	"sort"
	"strconv"
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
