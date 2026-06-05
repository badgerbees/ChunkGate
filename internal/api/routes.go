package api

import (
	"fmt"
	"net"
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

func parseS3Target(r *http.Request, virtualHosts []string) (s3Path, bool) {
	host := normalizeHost(r.Host)
	for _, virtualHost := range virtualHosts {
		if host == "" || virtualHost == "" || host == virtualHost {
			continue
		}
		suffix := "." + virtualHost
		if !strings.HasSuffix(host, suffix) {
			continue
		}
		bucket := strings.TrimSuffix(host, suffix)
		if bucket == "" {
			continue
		}
		return parseVirtualHostedS3Path(bucket, r.URL.EscapedPath())
	}
	return parseS3Path(r.URL.EscapedPath())
}

func parseVirtualHostedS3Path(bucket string, escapedPath string) (s3Path, bool) {
	keyEscaped := strings.TrimPrefix(escapedPath, "/")
	if keyEscaped == "" {
		return s3Path{Bucket: bucket, HasBucket: true}, true
	}
	key, err := url.PathUnescape(keyEscaped)
	if err != nil || key == "" {
		return s3Path{}, false
	}
	return s3Path{Bucket: bucket, Key: key, HasBucket: true, HasKey: true}, true
}

func normalizeVirtualHosts(hosts []string) []string {
	normalized := make([]string, 0, len(hosts))
	seen := map[string]bool{}
	for _, host := range hosts {
		host = normalizeHost(host)
		if host == "" || seen[host] {
			continue
		}
		seen[host] = true
		normalized = append(normalized, host)
	}
	return normalized
}

func normalizeHost(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if parsed, err := url.Parse(host); err == nil && parsed.Host != "" {
		host = parsed.Host
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	} else if strings.Count(host, ":") == 1 {
		if beforePort, _, found := strings.Cut(host, ":"); found {
			host = beforePort
		}
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	host = strings.TrimSuffix(host, ".")
	return strings.ToLower(host)
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
