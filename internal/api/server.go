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

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	"github.com/chunkgate/chunkgate/internal/s3auth"
)

type Server struct {
	objects   *object.Service
	multipart *multipart.Manager
	auth      *s3auth.Verifier
	gcMetrics *gc.Metrics
}

type Option func(*Server)

func WithAuthVerifier(verifier *s3auth.Verifier) Option {
	return func(server *Server) {
		server.auth = verifier
	}
}

func WithGCMetrics(metrics *gc.Metrics) Option {
	return func(server *Server) {
		server.gcMetrics = metrics
	}
}

func NewServer(objects *object.Service, multipartManager *multipart.Manager, options ...Option) *Server {
	server := &Server{objects: objects, multipart: multipartManager}
	for _, option := range options {
		if option != nil {
			option(server)
		}
	}
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if r.URL.Path == "/metrics" {
		s.writeMetrics(w)
		return
	}

	addCommonHeaders(w)

	identity, ok := s.authenticate(w, r)
	if !ok {
		return
	}

	target, ok := parseS3Path(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidURI", "could not parse the specified URI")
		return
	}
	if !target.HasBucket {
		s.serviceRoute(w, r)
		return
	}
	if !target.HasKey {
		s.bucketRoute(w, r, target.Bucket)
		return
	}

	switch {
	case r.Method == http.MethodPost && hasSubresource(r, "uploads"):
		s.initiateMultipart(w, r, identity.Tenant, target.Bucket, target.Key)
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "":
		s.uploadPart(w, r, identity.Tenant)
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
		s.completeMultipart(w, r, identity.Tenant)
	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") != "":
		s.abortMultipart(w, r, identity.Tenant)
	case r.Method == http.MethodPut:
		s.putObject(w, r, identity.Tenant, target.Bucket, target.Key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.getObject(w, r, identity.Tenant, target.Bucket, target.Key)
	case r.Method == http.MethodDelete:
		s.deleteObject(w, r, identity.Tenant, target.Bucket, target.Key)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "the specified method is not allowed against this resource")
	}
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (s3auth.Identity, bool) {
	if s.auth == nil || !s.auth.Enabled() {
		return s3auth.Identity{Tenant: "default"}, true
	}
	identity, err := s.auth.Verify(r)
	if err == nil {
		return identity, true
	}
	var authErr *s3auth.Error
	if errors.As(err, &authErr) {
		writeError(w, authErr.Status, authErr.Code, authErr.Message)
		return s3auth.Identity{}, false
	}
	writeError(w, http.StatusForbidden, "AccessDenied", "access denied")
	return s3auth.Identity{}, false
}

func (s *Server) serviceRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "the specified method is not allowed against this resource")
		return
	}
	writeXML(w, http.StatusOK, listAllMyBucketsResult{
		Owner: ownerResult{
			ID:          "chunkgate",
			DisplayName: "chunkgate",
		},
		Buckets: bucketList{},
	})
}

func (s *Server) bucketRoute(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		writeXML(w, http.StatusOK, listBucketResult{
			Name:        bucket,
			Prefix:      r.URL.Query().Get("prefix"),
			KeyCount:    0,
			MaxKeys:     parseMaxKeys(r),
			IsTruncated: false,
		})
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "the specified method is not allowed against this resource")
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	manifest, err := s.objects.Put(r.Context(), tenant, bucket, key, r.Body, object.WithHeaders(extractObjectHeaders(r.Header)))
	if err != nil {
		writeInternalError(w, err)
		return
	}
	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	if rangeHeader := strings.TrimSpace(r.Header.Get("Range")); rangeHeader != "" {
		s.getObjectRange(w, r, tenant, bucket, key, rangeHeader)
		return
	}

	manifest, reader, err := s.objects.Open(r.Context(), tenant, bucket, key)
	if errors.Is(err, metadata.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	defer reader.Close()

	applyObjectHeaders(w, manifest.Headers)
	w.Header().Set("Accept-Ranges", "bytes")
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

func (s *Server) getObjectRange(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string, rangeHeader string) {
	manifest, err := s.objects.Stat(r.Context(), tenant, bucket, key)
	if errors.Is(err, metadata.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}

	byteRange, err := parseSingleRange(rangeHeader, manifest.Size)
	if err != nil {
		applyObjectHeaders(w, manifest.Headers)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", manifest.ETag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", manifest.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", err.Error())
		return
	}

	length := byteRange.End - byteRange.Start + 1
	if r.Method == http.MethodHead {
		applyRangeHeaders(w, manifest, byteRange, length)
		w.WriteHeader(http.StatusPartialContent)
		return
	}

	manifest, reader, err := s.objects.OpenRange(r.Context(), tenant, bucket, key, byteRange)
	if err != nil {
		writeInternalError(w, err)
		return
	}
	defer reader.Close()

	applyRangeHeaders(w, manifest, byteRange, length)
	w.WriteHeader(http.StatusPartialContent)
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
	session, err := s.multipart.Create(r.Context(), tenant, bucket, key, expected, extractObjectHeaders(r.Header))
	if errors.Is(err, multipart.ErrUploadTooLarge) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the proposed multipart upload exceeds the configured maximum size")
		return
	}
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
	if errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	}
	if errors.Is(err, multipart.ErrPartTooLarge) || errors.Is(err, multipart.ErrUploadTooLarge) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the uploaded part exceeds the configured multipart size limit")
		return
	}
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
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "MalformedXML", "the XML you provided was not well-formed or did not validate against our published schema")
		return
	}

	session, err := s.multipart.Get(r.Context(), tenant, uploadID)
	if errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	order, err := validateCompletedParts(session.Parts, request.Parts)
	if err != nil {
		var apiErr s3Error
		if errors.As(err, &apiErr) {
			writeError(w, apiErr.Status, apiErr.Code, apiErr.Message)
			return
		}
		writeInternalError(w, err)
		return
	}

	_, reader, err := s.multipart.Open(r.Context(), tenant, uploadID, order)
	if err != nil {
		writeInternalError(w, err)
		return
	}

	manifest, err := s.objects.Put(r.Context(), tenant, session.Bucket, session.Key, reader, object.WithHeaders(session.Headers))
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
		Location: location(session.Bucket, session.Key),
		Bucket:   session.Bucket,
		Key:      session.Key,
		ETag:     manifest.ETag,
	})
}

func (s *Server) abortMultipart(w http.ResponseWriter, r *http.Request, tenant string) {
	if err := s.multipart.Abort(r.Context(), tenant, r.URL.Query().Get("uploadId")); errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	} else if err != nil {
		writeInternalError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type s3Path struct {
	Bucket    string
	Key       string
	HasBucket bool
	HasKey    bool
}

func parseS3Path(escapedPath string) (s3Path, bool) {
	trimmed := strings.TrimPrefix(escapedPath, "/")
	if trimmed == "" {
		return s3Path{}, true
	}
	bucketEscaped, keyEscaped, hasKey := strings.Cut(trimmed, "/")
	if bucketEscaped == "" {
		return s3Path{}, false
	}
	bucket, err := url.PathUnescape(bucketEscaped)
	if err != nil || bucket == "" {
		return s3Path{}, false
	}
	if !hasKey || keyEscaped == "" {
		return s3Path{Bucket: bucket, HasBucket: true}, true
	}
	key, err := url.PathUnescape(keyEscaped)
	if err != nil || key == "" {
		return s3Path{}, false
	}
	return s3Path{Bucket: bucket, Key: key, HasBucket: true, HasKey: true}, true
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

func extractObjectHeaders(headers http.Header) map[string]string {
	allowed := []string{
		"Cache-Control",
		"Content-Disposition",
		"Content-Encoding",
		"Content-Language",
		"Content-Type",
		"Expires",
	}
	extracted := map[string]string{}
	for _, key := range allowed {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			extracted[key] = value
		}
	}
	for key, values := range headers {
		if !strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			continue
		}
		value := strings.TrimSpace(strings.Join(values, ","))
		if value != "" {
			extracted[http.CanonicalHeaderKey(strings.ToLower(key))] = value
		}
	}
	if len(extracted) == 0 {
		return nil
	}
	return extracted
}

func applyObjectHeaders(w http.ResponseWriter, headers map[string]string) {
	for key, value := range headers {
		w.Header().Set(key, value)
	}
}

func applyRangeHeaders(w http.ResponseWriter, manifest metadata.ObjectManifest, byteRange object.ByteRange, length int64) {
	applyObjectHeaders(w, manifest.Headers)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, manifest.Size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
}

func parseSingleRange(header string, size int64) (object.ByteRange, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return object.ByteRange{}, fmt.Errorf("only bytes ranges are supported")
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if spec == "" {
		return object.ByteRange{}, fmt.Errorf("range is empty")
	}
	if strings.Contains(spec, ",") {
		return object.ByteRange{}, fmt.Errorf("multiple ranges are not supported")
	}
	startRaw, endRaw, ok := strings.Cut(spec, "-")
	if !ok {
		return object.ByteRange{}, fmt.Errorf("range must include a dash")
	}
	startRaw = strings.TrimSpace(startRaw)
	endRaw = strings.TrimSpace(endRaw)
	if startRaw == "" && endRaw == "" {
		return object.ByteRange{}, fmt.Errorf("range is empty")
	}
	if size <= 0 {
		return object.ByteRange{}, fmt.Errorf("range is not satisfiable")
	}

	if startRaw == "" {
		suffixLength, err := strconv.ParseInt(endRaw, 10, 64)
		if err != nil || suffixLength <= 0 {
			return object.ByteRange{}, fmt.Errorf("suffix length is invalid")
		}
		start := size - suffixLength
		if start < 0 {
			start = 0
		}
		return object.ByteRange{Start: start, End: size - 1}, nil
	}

	start, err := strconv.ParseInt(startRaw, 10, 64)
	if err != nil || start < 0 {
		return object.ByteRange{}, fmt.Errorf("range start is invalid")
	}
	if start >= size {
		return object.ByteRange{}, fmt.Errorf("range is not satisfiable")
	}
	if endRaw == "" {
		return object.ByteRange{Start: start, End: size - 1}, nil
	}

	end, err := strconv.ParseInt(endRaw, 10, 64)
	if err != nil || end < 0 {
		return object.ByteRange{}, fmt.Errorf("range end is invalid")
	}
	if end < start {
		return object.ByteRange{}, fmt.Errorf("range end is before range start")
	}
	if end >= size {
		end = size - 1
	}
	return object.ByteRange{Start: start, End: end}, nil
}

func validateCompletedParts(uploaded map[int]multipart.PartInfo, completed []completePart) ([]int, error) {
	if len(completed) == 0 {
		return nil, s3Error{Status: http.StatusBadRequest, Code: "MalformedXML", Message: "complete multipart upload requires at least one part"}
	}
	order := make([]int, 0, len(completed))
	seen := map[int]bool{}
	last := 0
	for _, part := range completed {
		if part.PartNumber <= 0 {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "part numbers must be positive"}
		}
		if part.PartNumber <= last {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPartOrder", Message: "the list of parts was not in ascending order"}
		}
		if seen[part.PartNumber] {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "duplicate part number in complete multipart upload"}
		}
		uploadedPart, ok := uploaded[part.PartNumber]
		if !ok {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "one or more of the specified parts could not be found"}
		}
		if normalizeETag(part.ETag) == "" || normalizeETag(part.ETag) != normalizeETag(uploadedPart.ETag) {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "one or more of the specified parts had an invalid ETag"}
		}
		seen[part.PartNumber] = true
		last = part.PartNumber
		order = append(order, part.PartNumber)
	}
	return order, nil
}

func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(etag), `"`))
}

func addCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("x-amz-request-id", "chunkgate")
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeXML(w, status, errorResponse{Code: code, Message: message, RequestID: "chunkgate"})
}

func writeInternalError(w http.ResponseWriter, err error) {
	if errors.Is(err, backend.ErrBlockNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if errors.Is(err, backend.ErrBackendUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "the storage backend is temporarily unavailable")
		return
	}
	writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
}

func (s *Server) writeMetrics(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	snapshot := gc.MetricsSnapshot{}
	if s.gcMetrics != nil {
		snapshot = s.gcMetrics.Snapshot()
	}
	_, _ = fmt.Fprintf(w, "chunkgate_gc_runs_total %d\n", snapshot.Runs)
	_, _ = fmt.Fprintf(w, "chunkgate_gc_scanned_tenants_total %d\n", snapshot.ScannedTenants)
	_, _ = fmt.Fprintf(w, "chunkgate_gc_candidate_blocks_total %d\n", snapshot.CandidateBlocks)
	_, _ = fmt.Fprintf(w, "chunkgate_gc_deleted_blocks_total %d\n", snapshot.DeletedBlocks)
	_, _ = fmt.Fprintf(w, "chunkgate_gc_failures_total %d\n", snapshot.Failures)
	_, _ = fmt.Fprintf(w, "chunkgate_gc_last_run_unix_seconds %d\n", snapshot.LastRunUnix)
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

func parseMaxKeys(r *http.Request) int {
	value := r.URL.Query().Get("max-keys")
	if value == "" {
		return 1000
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 1000
	}
	return parsed
}

type s3Error struct {
	Status  int
	Code    string
	Message string
}

func (e s3Error) Error() string {
	return e.Code + ": " + e.Message
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type completeMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Owner   ownerResult `xml:"Owner"`
	Buckets bucketList  `xml:"Buckets"`
}

type ownerResult struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketList struct {
	Buckets []bucketResult `xml:"Bucket,omitempty"`
}

type bucketResult struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	KeyCount    int      `xml:"KeyCount"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func location(bucket string, key string) string {
	return fmt.Sprintf("/%s/%s", bucket, key)
}
