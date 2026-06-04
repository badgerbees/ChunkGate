# Operations

ChunkGate exposes production guardrails by default while keeping local deployment simple.

## Backpressure

Upload chunking uses an adaptive CPU limiter. `CHUNKGATE_MAX_CONCURRENT_CHUNKERS` caps total chunking work, while `CHUNKGATE_CPU_HEADROOM_CORES` reserves CPU capacity for HTTP handling, disk spooling, metadata, and backend I/O.

Multipart scratch writes are protected by two checks:

- Atomic reservations through `CHUNKGATE_LOCAL_CAPACITY_BYTES`.
- Real free-space checks through `CHUNKGATE_SCRATCH_MIN_FREE_BYTES`.

When known request sizes exceed configured limits, ChunkGate rejects the request before reading the full body.

## Shutdown

On shutdown, ChunkGate stops accepting new upload work, lets active upload handlers drain until `CHUNKGATE_SHUTDOWN_TIMEOUT_SECONDS`, and preserves multipart state in metadata for restart recovery.

## Endpoints

- `/healthz`: process liveness.
- `/readyz`: metadata, backend, scratch, and draining readiness.
- `/metrics`: Prometheus text metrics.
- `/debug/pprof/`: gated pprof debug endpoints when `CHUNKGATE_DEBUG_PPROF_ENABLED=true`.

The metrics endpoint includes request totals, request errors, active requests, active uploads, upload failures, uploaded bytes, chunk totals, chunk bytes, chunk limiter queueing, and GC counters.

Structured request logs are written as JSON through Go `slog`.
