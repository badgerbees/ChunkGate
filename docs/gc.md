# Garbage Collection

ChunkGate deletes object manifests immediately, then removes unreferenced backend blocks asynchronously. The GC worker is enabled by default and runs in the main process.

## Configuration

```sh
CHUNKGATE_GC_ENABLED=true
CHUNKGATE_GC_INTERVAL_SECONDS=3600
CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS=86400
CHUNKGATE_GC_BATCH_SIZE=1000
CHUNKGATE_GC_MAX_RETRIES=3
```

`CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS` defaults to 24 hours. This keeps freshly orphaned blocks around long enough to avoid deleting data during nearby upload, read, overwrite, or restart activity.

`CHUNKGATE_GC_BATCH_SIZE` is capped at 1,000 so S3-compatible backends can use a single `DeleteObjects` request per batch.

## Safety

GC only selects blocks whose metadata reference count is zero and whose zero-reference timestamp is older than the configured orphan age. It deletes backend objects first, then calls metadata `ForgetBlocks` only after the backend delete succeeds.

If a backend delete fails, metadata is left intact and the same candidates can be retried on a later sweep. Within a sweep, failed backend batches are retried up to `CHUNKGATE_GC_MAX_RETRIES`.

## Metrics

The API exposes Prometheus-style counters at `/metrics`:

```text
chunkgate_gc_runs_total
chunkgate_gc_scanned_tenants_total
chunkgate_gc_candidate_blocks_total
chunkgate_gc_deleted_blocks_total
chunkgate_gc_failures_total
chunkgate_gc_last_run_unix_seconds
```
