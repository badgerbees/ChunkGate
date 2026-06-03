package api

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
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

func TestServerCompletesMultipartInRequestedOrder(t *testing.T) {
	server := testServer(t)

	req := httptest.NewRequest(http.MethodPost, "/bucket/big.bin?uploads", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, body = %s", w.Code, w.Body.String())
	}
	var initResult initiateMultipartUploadResult
	if err := xml.Unmarshal(w.Body.Bytes(), &initResult); err != nil {
		t.Fatalf("decode initiate result failed: %v", err)
	}

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
	}

	completeBody := bytes.NewBufferString(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part><Part><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`)
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
}

func testServer(t *testing.T) *Server {
	t.Helper()
	service := object.NewService(object.Config{
		Chunker: chunker.New(chunker.Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 0}),
		Backend: backend.NewFileStore(t.TempDir()),
		Store:   metadata.NewMemoryStore(),
		CPU:     limits.NewCPUSemaphore(2),
	})
	return NewServer(service, multipart.NewManager(t.TempDir(), limits.NewDiskReservations(1024*1024)))
}

func strconvItoa(value int) string {
	return string(rune('0' + value))
}
