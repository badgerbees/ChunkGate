package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/object"
)

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	done, ok := s.beginUpload(w)
	if !ok {
		return
	}
	success := false
	defer func() {
		done(success)
	}()

	body, ok := s.limitedBody(w, r, s.maxObjectBytes)
	if !ok {
		return
	}
	manifest, err := s.objects.Put(r.Context(), tenant, bucket, key, body, object.WithHeaders(extractObjectHeaders(r.Header)))
	if isBodyLimitError(err) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the request body exceeds the configured object size limit")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	success = true
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
