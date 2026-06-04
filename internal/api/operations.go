package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	httppprof "net/http/pprof"
	"time"

	"github.com/chunkgate/chunkgate/internal/gc"
)

func (s *Server) beginUpload(w http.ResponseWriter) (func(bool), bool) {
	doneDrain := func() {}
	if s.drain != nil {
		var err error
		doneDrain, err = s.drain.Begin()
		if err != nil {
			s.metrics.RejectRequest()
			writeError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "server is shutting down and is not accepting new uploads")
			return nil, false
		}
	}
	finishUpload := s.metrics.StartUpload()
	return func(success bool) {
		finishUpload(success)
		doneDrain()
	}, true
}

func (s *Server) limitedBody(w http.ResponseWriter, r *http.Request, maxBytes int64) (io.Reader, bool) {
	if maxBytes <= 0 {
		return r.Body, true
	}
	if r.ContentLength > maxBytes {
		s.metrics.RejectRequest()
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the request body exceeds the configured size limit")
		return nil, false
	}
	return http.MaxBytesReader(w, r.Body, maxBytes), true
}

func (s *Server) writeMetrics(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	opsSnapshot := s.metrics.Snapshot()
	_, _ = fmt.Fprintf(w, "chunkgate_http_requests_total %d\n", opsSnapshot.RequestsTotal)
	_, _ = fmt.Fprintf(w, "chunkgate_http_request_errors_total %d\n", opsSnapshot.RequestErrorsTotal)
	_, _ = fmt.Fprintf(w, "chunkgate_http_active_requests %d\n", opsSnapshot.ActiveRequests)
	_, _ = fmt.Fprintf(w, "chunkgate_http_rejected_requests_total %d\n", opsSnapshot.RejectedRequests)
	_, _ = fmt.Fprintf(w, "chunkgate_uploads_total %d\n", opsSnapshot.UploadsTotal)
	_, _ = fmt.Fprintf(w, "chunkgate_upload_failures_total %d\n", opsSnapshot.UploadFailures)
	_, _ = fmt.Fprintf(w, "chunkgate_active_uploads %d\n", opsSnapshot.ActiveUploads)
	_, _ = fmt.Fprintf(w, "chunkgate_uploaded_bytes_total %d\n", opsSnapshot.UploadedBytes)
	_, _ = fmt.Fprintf(w, "chunkgate_chunks_total %d\n", opsSnapshot.ChunksTotal)
	_, _ = fmt.Fprintf(w, "chunkgate_chunk_bytes_total %d\n", opsSnapshot.ChunkBytes)
	if s.limiter != nil {
		limiter := s.limiter.Snapshot()
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_limit %d\n", limiter.Limit)
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_active %d\n", limiter.Active)
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_waiting %d\n", limiter.Waiting)
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_acquires_total %d\n", limiter.AcquiresTotal)
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_queued_total %d\n", limiter.QueuedTotal)
		_, _ = fmt.Fprintf(w, "chunkgate_chunk_limiter_queue_wait_seconds_total %.9f\n", float64(limiter.QueueWaitNanos)/float64(time.Second))
	}
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

func (s *Server) writeReadiness(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	status := http.StatusOK
	if s.drain != nil && s.drain.IsDraining() {
		checks["drain"] = "draining"
		status = http.StatusServiceUnavailable
	}
	timeout := s.readinessTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	if err := runHealthCheck(ctx, s.objects.Store()); err != nil {
		checks["metadata"] = err.Error()
		status = http.StatusServiceUnavailable
	} else {
		checks["metadata"] = "ok"
	}
	if err := runHealthCheck(ctx, s.objects.Backend()); err != nil {
		checks["backend"] = err.Error()
		status = http.StatusServiceUnavailable
	} else {
		checks["backend"] = "ok"
	}
	if err := runHealthCheck(ctx, s.multipart); err != nil {
		checks["scratch"] = err.Error()
		status = http.StatusServiceUnavailable
	} else {
		checks["scratch"] = "ok"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": readinessStatus(status),
		"checks": checks,
	})
}

func runHealthCheck(ctx context.Context, target any) error {
	checker, ok := target.(interface {
		HealthCheck(context.Context) error
	})
	if !ok {
		return nil
	}
	return checker.HealthCheck(ctx)
}

func readinessStatus(status int) string {
	if status == http.StatusOK {
		return "ready"
	}
	return "not_ready"
}

func (s *Server) servePprof(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/debug/pprof/cmdline":
		httppprof.Cmdline(w, r)
	case "/debug/pprof/profile":
		httppprof.Profile(w, r)
	case "/debug/pprof/symbol":
		httppprof.Symbol(w, r)
	case "/debug/pprof/trace":
		httppprof.Trace(w, r)
	default:
		httppprof.Index(w, r)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(data []byte) (int, error) {
	n, err := r.ResponseWriter.Write(data)
	r.bytes += int64(n)
	return n, err
}
