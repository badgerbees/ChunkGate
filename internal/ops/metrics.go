package ops

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	requestsTotal      atomic.Int64
	requestErrorsTotal atomic.Int64
	activeRequests     atomic.Int64
	rejectedRequests   atomic.Int64
	activeUploads      atomic.Int64
	uploadsTotal       atomic.Int64
	uploadFailures     atomic.Int64
	uploadedBytes      atomic.Int64
	chunksTotal        atomic.Int64
	chunkBytes         atomic.Int64
}

type MetricsSnapshot struct {
	RequestsTotal      int64
	RequestErrorsTotal int64
	ActiveRequests     int64
	RejectedRequests   int64
	ActiveUploads      int64
	UploadsTotal       int64
	UploadFailures     int64
	UploadedBytes      int64
	ChunksTotal        int64
	ChunkBytes         int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) StartRequest() func(status int, duration time.Duration) {
	if m == nil {
		return func(int, time.Duration) {}
	}
	m.requestsTotal.Add(1)
	m.activeRequests.Add(1)
	return func(status int, _ time.Duration) {
		m.activeRequests.Add(-1)
		if status >= 400 {
			m.requestErrorsTotal.Add(1)
		}
	}
}

func (m *Metrics) RejectRequest() {
	if m != nil {
		m.rejectedRequests.Add(1)
	}
}

func (m *Metrics) StartUpload() func(success bool) {
	if m == nil {
		return func(bool) {}
	}
	m.uploadsTotal.Add(1)
	m.activeUploads.Add(1)
	return func(success bool) {
		m.activeUploads.Add(-1)
		if !success {
			m.uploadFailures.Add(1)
		}
	}
}

func (m *Metrics) ObserveUpload(bytes int64, chunks int64) {
	if m == nil {
		return
	}
	if bytes > 0 {
		m.uploadedBytes.Add(bytes)
		m.chunkBytes.Add(bytes)
	}
	if chunks > 0 {
		m.chunksTotal.Add(chunks)
	}
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		RequestsTotal:      m.requestsTotal.Load(),
		RequestErrorsTotal: m.requestErrorsTotal.Load(),
		ActiveRequests:     m.activeRequests.Load(),
		RejectedRequests:   m.rejectedRequests.Load(),
		ActiveUploads:      m.activeUploads.Load(),
		UploadsTotal:       m.uploadsTotal.Load(),
		UploadFailures:     m.uploadFailures.Load(),
		UploadedBytes:      m.uploadedBytes.Load(),
		ChunksTotal:        m.chunksTotal.Load(),
		ChunkBytes:         m.chunkBytes.Load(),
	}
}
