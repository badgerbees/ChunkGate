package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	"github.com/chunkgate/chunkgate/internal/ops"
	"github.com/chunkgate/chunkgate/internal/s3auth"
)

type chunkLimiter interface {
	Snapshot() limits.AdaptiveSnapshot
}

type Server struct {
	objects   *object.Service
	multipart *multipart.Manager
	auth      *s3auth.Verifier
	gcMetrics *gc.Metrics
	metrics   *ops.Metrics
	limiter   chunkLimiter

	drain             *ops.Drain
	logger            *slog.Logger
	maxObjectBytes    int64
	maxPartBytes      int64
	maxCompleteXML    int64
	readinessTimeout  time.Duration
	debugPprofEnabled bool
	anonymousTenant   string
	virtualHosts      []string
	cors              CORSConfig
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

func WithMetrics(metrics *ops.Metrics) Option {
	return func(server *Server) {
		server.metrics = metrics
	}
}

func WithLimiter(limiter chunkLimiter) Option {
	return func(server *Server) {
		server.limiter = limiter
	}
}

func WithDrain(drain *ops.Drain) Option {
	return func(server *Server) {
		server.drain = drain
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(server *Server) {
		server.logger = logger
	}
}

func WithBodyLimits(maxObjectBytes int64, maxPartBytes int64, maxCompleteXML int64) Option {
	return func(server *Server) {
		server.maxObjectBytes = maxObjectBytes
		server.maxPartBytes = maxPartBytes
		server.maxCompleteXML = maxCompleteXML
	}
}

func WithReadinessTimeout(timeout time.Duration) Option {
	return func(server *Server) {
		server.readinessTimeout = timeout
	}
}

func WithPprof(enabled bool) Option {
	return func(server *Server) {
		server.debugPprofEnabled = enabled
	}
}

func WithAnonymousTenant(tenant string) Option {
	return func(server *Server) {
		server.anonymousTenant = tenant
	}
}

func WithVirtualHosts(hosts ...string) Option {
	return func(server *Server) {
		server.virtualHosts = normalizeVirtualHosts(hosts)
	}
}

func WithCORS(config CORSConfig) Option {
	return func(server *Server) {
		server.cors = config.normalized()
	}
}

func NewServer(objects *object.Service, multipartManager *multipart.Manager, options ...Option) *Server {
	server := &Server{
		objects:          objects,
		multipart:        multipartManager,
		metrics:          ops.NewMetrics(),
		drain:            &ops.Drain{},
		maxCompleteXML:   1024 * 1024,
		readinessTimeout: 3 * time.Second,
	}
	for _, option := range options {
		if option != nil {
			option(server)
		}
	}
	if server.metrics == nil {
		server.metrics = ops.NewMetrics()
	}
	if server.drain == nil {
		server.drain = &ops.Drain{}
	}
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := generateRequestID()
	r = r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, requestID))
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	finish := s.metrics.StartRequest()
	defer func() {
		duration := time.Since(started)
		finish(recorder.status, duration)
		if s.logger != nil {
			s.logger.Info("http_request",
				"method", r.Method,
				"path", r.URL.Path,
				"request_id", requestID,
				"status", recorder.status,
				"bytes", recorder.bytes,
				"duration_ms", float64(duration.Microseconds())/1000,
			)
		}
	}()
	s.serveHTTP(recorder, r)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		if !s.debugPprofEnabled {
			http.NotFound(w, r)
			return
		}
		s.servePprof(w, r)
		return
	}
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if r.URL.Path == "/readyz" {
		s.writeReadiness(w, r)
		return
	}
	if r.URL.Path == "/metrics" {
		s.writeMetrics(w)
		return
	}

	addCommonHeaders(w, requestIDFromContext(r.Context()))
	if s.handleCORS(w, r) {
		return
	}

	identity, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if strings.HasPrefix(r.URL.Path, deltaAPIPrefix) {
		s.deltaRoute(w, r, identity.Tenant)
		return
	}

	target, ok := parseS3Target(r, s.virtualHosts)
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidURI", "could not parse the specified URI")
		return
	}
	if !target.HasBucket {
		s.serviceRoute(w, r)
		return
	}
	if !validateBucketName(target.Bucket) {
		writeError(w, http.StatusBadRequest, "InvalidBucketName", "the specified bucket is not valid")
		return
	}
	if !target.HasKey {
		s.bucketRoute(w, r, target.Bucket)
		return
	}
	if !validateObjectKey(target.Key) {
		writeError(w, http.StatusBadRequest, "InvalidObjectName", "the specified object key is not valid")
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

type requestIDContextKey struct{}

func requestIDFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDContextKey{}).(string); ok && value != "" {
		return value
	}
	return "chunkgate"
}

func generateRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return "cg-" + hex.EncodeToString(raw[:])
	}
	return "cg-" + hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000")))
}
