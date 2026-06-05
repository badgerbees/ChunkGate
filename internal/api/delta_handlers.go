package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/object"
)

const (
	deltaAPIPrefix             = "/_chunkgate/v1/"
	maxDeltaChunkRequestBytes  = 1024 * 1024
	maxDeltaChunkRequestHashes = 1000
)

type deltaManifestResponse struct {
	Version   int                  `json:"version"`
	Bucket    string               `json:"bucket"`
	Key       string               `json:"key"`
	Size      int64                `json:"size"`
	ETag      string               `json:"etag"`
	ObjectMD5 string               `json:"object_md5,omitempty"`
	Headers   map[string]string    `json:"headers,omitempty"`
	Chunks    []deltaManifestChunk `json:"chunks"`
}

type deltaManifestChunk struct {
	Index  int    `json:"index"`
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type deltaChunkRequest struct {
	Bucket string   `json:"bucket"`
	Key    string   `json:"key"`
	Hashes []string `json:"hashes"`
}

type deltaChunkResponse struct {
	Version int                 `json:"version"`
	Chunks  []deltaChunkPayload `json:"chunks"`
}

type deltaChunkPayload struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
	Data string `json:"data"`
}

func (s *Server) deltaRoute(w http.ResponseWriter, r *http.Request, tenant string) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == deltaAPIPrefix+"manifest":
		s.getDeltaManifest(w, r, tenant)
	case r.Method == http.MethodPost && r.URL.Path == deltaAPIPrefix+"chunks":
		s.getDeltaChunks(w, r, tenant)
	default:
		writeError(w, http.StatusNotFound, "NoSuchEndpoint", "the requested ChunkGate companion endpoint does not exist")
	}
}

func (s *Server) getDeltaManifest(w http.ResponseWriter, r *http.Request, tenant string) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")
	if !validateBucketName(bucket) {
		writeError(w, http.StatusBadRequest, "InvalidBucketName", "the specified bucket is not valid")
		return
	}
	if !validateObjectKey(key) {
		writeError(w, http.StatusBadRequest, "InvalidObjectName", "the specified object key is not valid")
		return
	}
	manifest, err := s.objects.Stat(r.Context(), tenant, bucket, key)
	if errors.Is(err, metadata.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, makeDeltaManifest(manifest))
}

func (s *Server) getDeltaChunks(w http.ResponseWriter, r *http.Request, tenant string) {
	defer r.Body.Close()
	var request deltaChunkRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxDeltaChunkRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		if isBodyLimitError(err) {
			writeError(w, http.StatusBadRequest, "EntityTooLarge", "the chunk request payload is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "MalformedJSON", "the JSON you provided was not well-formed or did not validate against the companion API schema")
		return
	}
	if !validateBucketName(request.Bucket) {
		writeError(w, http.StatusBadRequest, "InvalidBucketName", "the specified bucket is not valid")
		return
	}
	if !validateObjectKey(request.Key) {
		writeError(w, http.StatusBadRequest, "InvalidObjectName", "the specified object key is not valid")
		return
	}
	if len(request.Hashes) > maxDeltaChunkRequestHashes {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "too many chunks were requested")
		return
	}
	for _, hash := range request.Hashes {
		if !validateChunkHash(hash) {
			writeError(w, http.StatusBadRequest, "InvalidChunk", "requested chunk hashes must be lowercase SHA-256 hex strings")
			return
		}
	}
	_, chunks, err := s.objects.ReadChunks(r.Context(), tenant, request.Bucket, request.Key, request.Hashes)
	if errors.Is(err, metadata.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if errors.Is(err, object.ErrChunkNotInManifest) {
		writeError(w, http.StatusBadRequest, "InvalidChunk", "one or more requested chunks are not part of the object manifest")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}

	response := deltaChunkResponse{
		Version: 1,
		Chunks:  make([]deltaChunkPayload, 0, len(chunks)),
	}
	for _, chunk := range chunks {
		response.Chunks = append(response.Chunks, deltaChunkPayload{
			Hash: chunk.Hash,
			Size: chunk.Size,
			Data: base64.StdEncoding.EncodeToString(chunk.Data),
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func makeDeltaManifest(manifest metadata.ObjectManifest) deltaManifestResponse {
	response := deltaManifestResponse{
		Version:   1,
		Bucket:    manifest.Bucket,
		Key:       manifest.Key,
		Size:      manifest.Size,
		ETag:      manifest.ETag,
		ObjectMD5: objectMD5FromETag(manifest.ETag),
		Headers:   cloneHeaders(manifest.Headers),
		Chunks:    make([]deltaManifestChunk, 0, len(manifest.Chunks)),
	}
	for index, chunk := range manifest.Chunks {
		response.Chunks = append(response.Chunks, deltaManifestChunk{
			Index:  index,
			Hash:   chunk.Hash,
			Offset: chunk.Offset,
			Size:   chunk.Size,
		})
	}
	return response
}

func objectMD5FromETag(etag string) string {
	value := strings.ToLower(strings.Trim(strings.TrimSpace(etag), `"`))
	if len(value) != 32 {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return ""
		}
	}
	return value
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	clone := make(map[string]string, len(headers))
	for key, value := range headers {
		clone[key] = value
	}
	return clone
}
