package api

import (
	"net/http"
	"strings"
)

type CORSConfig struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	MaxAgeSeconds    int
}

func (c CORSConfig) normalized() CORSConfig {
	out := CORSConfig{
		AllowedOrigins:   normalizeHeaderList(c.AllowedOrigins),
		AllowedMethods:   normalizeMethodList(c.AllowedMethods),
		AllowedHeaders:   normalizeHeaderList(c.AllowedHeaders),
		ExposedHeaders:   normalizeHeaderList(c.ExposedHeaders),
		AllowCredentials: c.AllowCredentials,
		MaxAgeSeconds:    c.MaxAgeSeconds,
	}
	if len(out.AllowedMethods) == 0 {
		out.AllowedMethods = []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete}
	}
	if len(out.AllowedHeaders) == 0 {
		out.AllowedHeaders = []string{"*"}
	}
	if len(out.ExposedHeaders) == 0 {
		out.ExposedHeaders = []string{"ETag", "Content-Length", "Content-Range", "x-amz-request-id"}
	}
	return out
}

func (c CORSConfig) enabled() bool {
	return len(c.AllowedOrigins) > 0
}

func (s *Server) handleCORS(w http.ResponseWriter, r *http.Request) bool {
	if !s.cors.enabled() {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	if !s.cors.originAllowed(origin) {
		if r.Method == http.MethodOptions {
			writeError(w, http.StatusForbidden, "AccessDenied", "the CORS origin is not allowed")
			return true
		}
		return false
	}

	w.Header().Set("Access-Control-Allow-Origin", s.cors.responseOrigin(origin))
	w.Header().Set("Vary", appendVary(w.Header().Get("Vary"), "Origin"))
	if s.cors.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	if len(s.cors.ExposedHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(s.cors.ExposedHeaders, ", "))
	}

	if r.Method != http.MethodOptions {
		return false
	}

	requestMethod := r.Header.Get("Access-Control-Request-Method")
	if requestMethod == "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "CORS preflight requires Access-Control-Request-Method")
		return true
	}
	if !s.cors.methodAllowed(requestMethod) {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "the requested CORS method is not allowed")
		return true
	}
	requestHeaders := r.Header.Get("Access-Control-Request-Headers")
	if !s.cors.headersAllowed(requestHeaders) {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "one or more requested CORS headers are not allowed")
		return true
	}

	w.Header().Set("Access-Control-Allow-Methods", strings.Join(s.cors.AllowedMethods, ", "))
	w.Header().Set("Access-Control-Allow-Headers", s.cors.allowedHeadersValue(requestHeaders))
	if s.cors.MaxAgeSeconds > 0 {
		w.Header().Set("Access-Control-Max-Age", intString(s.cors.MaxAgeSeconds))
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

func (c CORSConfig) originAllowed(origin string) bool {
	for _, allowed := range c.AllowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return true
		}
	}
	return false
}

func (c CORSConfig) responseOrigin(origin string) string {
	if !c.AllowCredentials {
		for _, allowed := range c.AllowedOrigins {
			if allowed == "*" {
				return "*"
			}
		}
	}
	return origin
}

func (c CORSConfig) methodAllowed(method string) bool {
	for _, allowed := range c.AllowedMethods {
		if strings.EqualFold(allowed, method) {
			return true
		}
	}
	return false
}

func (c CORSConfig) headersAllowed(requestHeaders string) bool {
	if strings.TrimSpace(requestHeaders) == "" {
		return true
	}
	if containsWildcard(c.AllowedHeaders) {
		return true
	}
	for _, requested := range splitHeaderValues(requestHeaders) {
		found := false
		for _, allowed := range c.AllowedHeaders {
			if strings.EqualFold(allowed, requested) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (c CORSConfig) allowedHeadersValue(requestHeaders string) string {
	if containsWildcard(c.AllowedHeaders) {
		if strings.TrimSpace(requestHeaders) == "" {
			return "*"
		}
		return strings.Join(splitHeaderValues(requestHeaders), ", ")
	}
	return strings.Join(c.AllowedHeaders, ", ")
}

func normalizeHeaderList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		for _, item := range splitHeaderValues(value) {
			key := strings.ToLower(item)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, item)
		}
	}
	return out
}

func normalizeMethodList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		for _, item := range splitHeaderValues(value) {
			method := strings.ToUpper(item)
			if seen[method] {
				continue
			}
			seen[method] = true
			out = append(out, method)
		}
	}
	return out
}

func splitHeaderValues(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func containsWildcard(values []string) bool {
	for _, value := range values {
		if value == "*" {
			return true
		}
	}
	return false
}

func appendVary(existing string, value string) string {
	if existing == "" {
		return value
	}
	for _, item := range splitHeaderValues(existing) {
		if strings.EqualFold(item, value) {
			return existing
		}
	}
	return existing + ", " + value
}

func intString(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}
