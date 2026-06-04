package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

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

func location(bucket string, key string) string {
	return fmt.Sprintf("/%s/%s", bucket, key)
}
