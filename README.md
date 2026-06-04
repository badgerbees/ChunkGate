# ChunkGate

ChunkGate is a self-hosted S3-compatible deduplication proxy written in Go. It exposes a normal object endpoint to clients while storing incoming streams as content-defined chunks behind the scenes.

This repository currently contains the deployable base architecture:

- S3-like `PUT`, `GET`, `HEAD`, `DELETE`, and multipart initiate/upload/complete/abort routes.
- AWS Signature Version 4 verification when local credentials are configured.
- Tenant isolation derived from authenticated access keys.
- Bucket-level SDK compatibility routes for list, create, head, delete, and empty object listings.
- Object header preservation for common HTTP headers and `x-amz-meta-*` metadata.
- Single-range `GET` and `HEAD` support with manifest-driven chunk selection.
- Streaming FastCDC chunking powered by a production Gear-hash engine, with a built-in fallback engine.
- Tenant-sharded SQLite metadata files under `data/metadata/tenant_{id}.db`.
- Migration-managed structured metadata tables for objects, chunks, blocks, and multipart state.
- Tenant-scoped filesystem block storage under `data/backend`.
- Durable sequential multipart spooling under `data/scratch`, with restart reload and stale upload cleanup.
- Atomic local capacity reservations for multipart upload initiation.
- CPU concurrency gating around chunk processing.
- Background soft-delete GC with orphan-age protection, provider-sized bulk deletes, retries, and metrics.
- Pluggable block backends: local filesystem by default, or S3-compatible storage for AWS S3, MinIO, Cloudflare R2, and similar providers.

## Run Locally

```sh
go run ./cmd/chunkgate
```

```sh
curl -X PUT --data-binary @artifact.tar http://localhost:8080/builds/artifact.tar
curl http://localhost:8080/builds/artifact.tar -o artifact.tar
curl -I http://localhost:8080/builds/artifact.tar
curl -X DELETE http://localhost:8080/builds/artifact.tar
```

By default, local development runs without authentication and uses the `default` tenant. For S3-client compatible authentication, configure access keys and point your SDK or CLI at the ChunkGate endpoint.

## Docker Compose

```sh
docker compose up --build
```

The compose file starts ChunkGate with a local MinIO service, creates a `chunkgate` bucket, and persists state in `chunkgate-data` and `minio-data` volumes.

## Configuration

| Variable | Default |
| --- | --- |
| `CHUNKGATE_LISTEN` | `:8080` |
| `CHUNKGATE_DATA_DIR` | `data` |
| `CHUNKGATE_METADATA_DIR` | `${CHUNKGATE_DATA_DIR}/metadata` |
| `CHUNKGATE_BACKEND_DIR` | `${CHUNKGATE_DATA_DIR}/backend` |
| `CHUNKGATE_SCRATCH_DIR` | `${CHUNKGATE_DATA_DIR}/scratch` |
| `CHUNKGATE_BACKEND` | `filesystem`, or `s3` |
| `CHUNKGATE_LOCAL_CAPACITY_BYTES` | `21474836480` |
| `CHUNKGATE_MAX_CONCURRENT_CHUNKERS` | `0` meaning CPU count |
| `CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES` | `5242880` |
| `CHUNKGATE_CHUNK_MIN_BYTES` | `524288` |
| `CHUNKGATE_CHUNK_AVG_BYTES` | `1048576` |
| `CHUNKGATE_CHUNK_MAX_BYTES` | `4194304` |
| `CHUNKGATE_CHUNK_ENGINE` | `fastcdc`, or `builtin` for the local fallback engine |
| `CHUNKGATE_MULTIPART_MAX_PART_BYTES` | `5368709120` |
| `CHUNKGATE_MULTIPART_MAX_UPLOAD_BYTES` | `21474836480` |
| `CHUNKGATE_MULTIPART_STALE_TTL_SECONDS` | `86400` |
| `CHUNKGATE_S3_ENDPOINT` | required when `CHUNKGATE_BACKEND=s3` |
| `CHUNKGATE_S3_REGION` | `us-east-1` |
| `CHUNKGATE_S3_BUCKET` | required when `CHUNKGATE_BACKEND=s3` |
| `CHUNKGATE_S3_ACCESS_KEY_ID` | unset, falls back to `AWS_ACCESS_KEY_ID` |
| `CHUNKGATE_S3_SECRET_ACCESS_KEY` | unset, falls back to `AWS_SECRET_ACCESS_KEY` |
| `CHUNKGATE_S3_SESSION_TOKEN` | unset, falls back to `AWS_SESSION_TOKEN` |
| `CHUNKGATE_S3_PREFIX` | unset |
| `CHUNKGATE_S3_PATH_STYLE` | `true` |
| `CHUNKGATE_S3_USE_TLS` | `true` |
| `CHUNKGATE_S3_TIMEOUT_SECONDS` | `30` |
| `CHUNKGATE_S3_MAX_RETRIES` | `3` |
| `CHUNKGATE_GC_ENABLED` | `true` |
| `CHUNKGATE_GC_INTERVAL_SECONDS` | `3600` |
| `CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS` | `86400` |
| `CHUNKGATE_GC_BATCH_SIZE` | `1000` |
| `CHUNKGATE_GC_MAX_RETRIES` | `3` |
| `CHUNKGATE_ACCESS_KEY_ID` | unset |
| `CHUNKGATE_SECRET_ACCESS_KEY` | unset |
| `CHUNKGATE_TENANT_ID` | access key value |
| `CHUNKGATE_CREDENTIALS` | unset, comma-separated `access:secret[:tenant]` entries |

If `CHUNKGATE_ACCESS_KEY_ID`/`CHUNKGATE_SECRET_ACCESS_KEY` or `CHUNKGATE_CREDENTIALS` are set, ChunkGate requires AWS SigV4 authorization. Tenant isolation is then derived from the authenticated access key or optional tenant value.

Example with the AWS CLI:

```sh
AWS_ACCESS_KEY_ID=tenant-a AWS_SECRET_ACCESS_KEY=dev-secret \
  aws --endpoint-url http://localhost:8080 s3 cp artifact.tar s3://builds/artifact.tar
```

Example with a remote S3-compatible block backend:

```sh
CHUNKGATE_BACKEND=s3 \
CHUNKGATE_S3_ENDPOINT=s3.amazonaws.com \
CHUNKGATE_S3_REGION=us-east-1 \
CHUNKGATE_S3_BUCKET=my-chunkgate-blocks \
CHUNKGATE_S3_ACCESS_KEY_ID=... \
CHUNKGATE_S3_SECRET_ACCESS_KEY=... \
go run ./cmd/chunkgate
```

## Development

```sh
go test ./...
go vet ./...
go build -o chunkgate ./cmd/chunkgate
go test ./internal/chunker -bench . -benchmem
```

Run the S3 backend integration test against a local MinIO endpoint:

```sh
CHUNKGATE_S3_TEST_ENDPOINT=http://localhost:9000 go test ./internal/backend -run MinIO
```

GC counters are exposed at `/metrics` in Prometheus text format.

## Next Adapters

The core object service depends on `backend.BlockStore`, so adding AWS S3, MinIO, or Cloudflare R2 storage is a bounded adapter task. The metadata layer depends on `metadata.Store`, so a PostgreSQL implementation can be added without changing the API or chunking layers.
