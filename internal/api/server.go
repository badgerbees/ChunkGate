package api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
)

const tenantHeader = "X-ChunkGate-Tenant"

type Server struct {
	objects   *object.Service
	multipart *multipart.Manager
}

func NewServer(objects *object.Service, multipart *multipart.Manager) *Server {
	return &Server{objects: objects, multipart: multipart}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	bucket, key, ok := parseObjectPath(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NoSuchBucket", "object path must be /{bucket}/{key}")
		return
	}
	tenant := r.Header.Get(tenantHeader)
	if tenant == "" {
		tenant = "default"
	}

	switch {
	case r.Method == http.MethodPost && hasSubresource(r, "uploads"):
		s.initiateMultipart(w, r, tenant, bucket, key)
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "":
		s.uploadPart(w, r, tenant)
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
		s.completeMultipart(w, r, tenant)
	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") != "":
		s.abortMultipart(w, r, tenant)
	case r.Method == http.MethodPut:
		s.putObject(w, r, tenant, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.getObject(w, r, tenant, bucket, key)
	case r.Method == http.MethodDelete:
		s.deleteObject(w, r, tenant, bucket, key)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method")
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	manifest, err := s.objects.Put(r.Context(), tenant, bucket, key, r.Body)
	if err != nil {
		writeInternalError(w, err)
		return
	}
	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	manifest, reader, err := s.objects.Open(r.Context(), tenant, bucket, key)
	if errors.Is(err, metadata.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "object not found")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	defer reader.Close()

	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, err := io.Copy(w, reader); err != nil {
		return
	}
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	if _, err := s.objects.Delete(r.Context(), tenant, bucket, key); err != nil && !errors.Is(err, metadata.ErrNotFound) {
		writeInternalError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) initiateMultipart(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	expected := parseInt64Header(r, "X-ChunkGate-Expected-Size")
	session, err := s.multipart.Create(r.Context(), tenant, bucket, key, expected)
	if err != nil {
		writeInternalError(w, err)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: session.UploadID,
	})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, tenant string) {
	uploadID := r.URL.Query().Get("uploadId")
	number, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || number <= 0 {
		writeError(w, http.StatusBadRequest, "InvalidPart", "partNumber must be a positive integer")
		return
	}
	part, err := s.multipart.PutPart(r.Context(), tenant, uploadID, number, r.Body)
	if err != nil {
		writeInternalError(w, err)
		return
	}
	w.Header().Set("ETag", part.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request, tenant string) {
	uploadID := r.URL.Query().Get("uploadId")
	var request completeMultipartUpload
	if r.ContentLength != 0 {
		_ = xml.NewDecoder(r.Body).Decode(&request)
	}
	order := make([]int, 0, len(request.Parts))
	for _, part := range request.Parts {
		order = append(order, part.PartNumber)
	}

	session, reader, err := s.multipart.Open(r.Context(), tenant, uploadID, order)
	if err != nil {
		writeInternalError(w, err)
		return
	}

	manifest, err := s.objects.Put(r.Context(), tenant, session.Bucket, session.Key, reader)
	closeErr := reader.Close()
	if err != nil {
		writeInternalError(w, err)
		return
	}
	if closeErr != nil {
		writeInternalError(w, closeErr)
		return
	}
	if err := s.multipart.CompleteCleanup(r.Context(), tenant, uploadID); err != nil {
		writeInternalError(w, err)
		return
	}

	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Bucket: session.Bucket,
		Key:    session.Key,
		ETag:   manifest.ETag,
	})
}

func (s *Server) abortMultipart(w http.ResponseWriter, r *http.Request, tenant string) {
	if err := s.multipart.Abort(r.Context(), tenant, r.URL.Query().Get("uploadId")); err != nil {
		writeInternalError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseObjectPath(escapedPath string) (string, string, bool) {
	trimmed := strings.TrimPrefix(escapedPath, "/")
	bucketEscaped, keyEscaped, ok := strings.Cut(trimmed, "/")
	if !ok || bucketEscaped == "" || keyEscaped == "" {
		return "", "", false
	}
	bucket, err := url.PathUnescape(bucketEscaped)
	if err != nil {
		return "", "", false
	}
	key, err := url.PathUnescape(keyEscaped)
	if err != nil {
		return "", "", false
	}
	return bucket, key, true
}

func hasSubresource(r *http.Request, name string) bool {
	if _, ok := r.URL.Query()[name]; ok {
		return true
	}
	for _, part := range strings.Split(r.URL.RawQuery, "&") {
		if part == name {
			return true
		}
	}
	return false
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeXML(w, status, errorResponse{Code: code, Message: message})
}

func writeInternalError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
}

func parseInt64Header(r *http.Request, key string) int64 {
	value := r.Header.Get(key)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type completeMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func location(bucket string, key string) string {
	return fmt.Sprintf("/%s/%s", bucket, key)
}
