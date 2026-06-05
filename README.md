# ChunkGate

ChunkGate is a self-hosted proxy that sits in front of S3-compatible storage and helps reduce storage usage automatically. Your apps keep using normal S3 uploads and downloads, while ChunkGate breaks files into reusable chunks, stores repeated data only once per tenant, and rebuilds the original object when it is read back.

Key features:

- S3-compatible object, bucket, multipart upload, and range-read APIs.
- SigV4 authentication with tenant isolation derived from access keys.
- Path-style and configurable virtual-hosted-style bucket addressing.
- Static CORS preflight/response headers for browser and SDK clients.
- Streaming FastCDC deduplication with tenant-scoped block storage.
- SQLite by default, with PostgreSQL support for shared metadata deployments.
- Filesystem or S3-compatible block backends, including MinIO, AWS S3, Cloudflare R2, and similar services.
- Background garbage collection, health/readiness checks, Prometheus metrics, and structured logs.
- Optional ChunkGate-aware delta downloads through manifests and the `chunkgate-delta` client.
- Docker, Compose, Kubernetes, sample config, backup, upgrade, release, SBOM, and vulnerability-scanning support.

## Run Locally

```sh
CHUNKGATE_ALLOW_ANONYMOUS=true go run ./cmd/chunkgate
```

```sh
curl -X PUT --data-binary @artifact.tar http://localhost:8080/builds/artifact.tar
curl http://localhost:8080/builds/artifact.tar -o artifact.tar
curl -I http://localhost:8080/builds/artifact.tar
curl -X DELETE http://localhost:8080/builds/artifact.tar
```

Anonymous mode is only enabled when `CHUNKGATE_ALLOW_ANONYMOUS=true`; it uses the `default` tenant and is intended for local curl testing. By default, ChunkGate requires AWS SigV4 credentials and derives tenant identity from the authenticated access key or configured tenant mapping.

## Docker Compose

```sh
docker compose up --build
```

The compose file starts ChunkGate with a local MinIO service, creates a `chunkgate` bucket, and persists state in `chunkgate-data` and `minio-data` volumes.

To start the optional PostgreSQL metadata service for local integration testing:

```sh
docker compose up -d postgres
```

The Compose PostgreSQL service is exposed on host port `15432` to avoid colliding with an existing local PostgreSQL install.

Production Compose and Kubernetes examples are documented in [docs/deployment.md](docs/deployment.md).

## Configuration

| Variable | Default |
| --- | --- |
| `CHUNKGATE_LISTEN` | `:8080` |
| `CHUNKGATE_DATA_DIR` | `data` |
| `CHUNKGATE_METADATA` | `sqlite`, or `postgres` |
| `CHUNKGATE_METADATA_DIR` | `${CHUNKGATE_DATA_DIR}/metadata` |
| `CHUNKGATE_BACKEND_DIR` | `${CHUNKGATE_DATA_DIR}/backend` |
| `CHUNKGATE_SCRATCH_DIR` | `${CHUNKGATE_DATA_DIR}/scratch` |
| `CHUNKGATE_BACKEND` | `filesystem`, or `s3` |
| `CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY` | unset, AES key for filesystem backend only |
| `CHUNKGATE_LOCAL_CAPACITY_BYTES` | `21474836480` |
| `CHUNKGATE_MAX_CONCURRENT_CHUNKERS` | `0` meaning CPU count |
| `CHUNKGATE_CPU_HEADROOM_CORES` | `1` |
| `CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES` | `5242880` |
| `CHUNKGATE_CHUNK_MIN_BYTES` | `524288` |
| `CHUNKGATE_CHUNK_AVG_BYTES` | `1048576` |
| `CHUNKGATE_CHUNK_MAX_BYTES` | `4194304` |
| `CHUNKGATE_CHUNK_ENGINE` | `fastcdc`, or `builtin` for the local fallback engine |
| `CHUNKGATE_MULTIPART_MAX_PART_BYTES` | `5368709120` |
| `CHUNKGATE_MULTIPART_MAX_UPLOAD_BYTES` | `21474836480` |
| `CHUNKGATE_MULTIPART_STALE_TTL_SECONDS` | `86400` |
| `CHUNKGATE_SCRATCH_MIN_FREE_BYTES` | `1073741824` |
| `CHUNKGATE_MAX_OBJECT_BYTES` | `0` meaning unlimited |
| `CHUNKGATE_COMPLETE_XML_MAX_BYTES` | `1048576` |
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
| `CHUNKGATE_POSTGRES_DSN` | required when `CHUNKGATE_METADATA=postgres` |
| `CHUNKGATE_POSTGRES_MAX_OPEN_CONNS` | `16` |
| `CHUNKGATE_POSTGRES_MAX_IDLE_CONNS` | `4` |
| `CHUNKGATE_POSTGRES_CONN_MAX_LIFETIME_SECONDS` | `1800` |
| `CHUNKGATE_READINESS_TIMEOUT_SECONDS` | `3` |
| `CHUNKGATE_SHUTDOWN_TIMEOUT_SECONDS` | `15` |
| `CHUNKGATE_DEBUG_PPROF_ENABLED` | `false` |
| `CHUNKGATE_VIRTUAL_HOSTS` | unset, comma-separated endpoint hostnames for `bucket.host/key` routing |
| `CHUNKGATE_CORS_ALLOWED_ORIGINS` | unset, comma-separated origins or `*` |
| `CHUNKGATE_CORS_ALLOWED_METHODS` | `GET, HEAD, PUT, POST, DELETE` when CORS is enabled |
| `CHUNKGATE_CORS_ALLOWED_HEADERS` | `*` when CORS is enabled |
| `CHUNKGATE_CORS_EXPOSED_HEADERS` | `ETag, Content-Length, Content-Range, x-amz-request-id` |
| `CHUNKGATE_CORS_ALLOW_CREDENTIALS` | `false` |
| `CHUNKGATE_CORS_MAX_AGE_SECONDS` | `3600` |
| `CHUNKGATE_ALLOW_ANONYMOUS` | `false` |
| `CHUNKGATE_ACCESS_KEY_ID` | unset |
| `CHUNKGATE_SECRET_ACCESS_KEY` | unset |
| `CHUNKGATE_TENANT_ID` | access key value |
| `CHUNKGATE_CREDENTIALS` | unset, comma-separated `access:secret[:tenant]` entries |

Unless `CHUNKGATE_ALLOW_ANONYMOUS=true` is set, `CHUNKGATE_ACCESS_KEY_ID`/`CHUNKGATE_SECRET_ACCESS_KEY` or `CHUNKGATE_CREDENTIALS` are required and every S3 request must be AWS SigV4 signed. Tenant isolation is derived from the authenticated access key or optional tenant value.

Example with the AWS CLI:

```sh
CHUNKGATE_ACCESS_KEY_ID=tenant-a CHUNKGATE_SECRET_ACCESS_KEY=dev-secret go run ./cmd/chunkgate

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
CHUNKGATE_ACCESS_KEY_ID=tenant-a \
CHUNKGATE_SECRET_ACCESS_KEY=dev-secret \
go run ./cmd/chunkgate
```

Example with shared PostgreSQL metadata:

```sh
CHUNKGATE_METADATA=postgres \
CHUNKGATE_POSTGRES_DSN='postgres://chunkgate:chunkgate@localhost:5432/chunkgate?sslmode=disable' \
CHUNKGATE_ACCESS_KEY_ID=tenant-a \
CHUNKGATE_SECRET_ACCESS_KEY=dev-secret \
go run ./cmd/chunkgate
```

## Development

```sh
go test ./...
go vet ./...
go build -o chunkgate ./cmd/chunkgate
go build -o chunkgate-delta ./cmd/chunkgate-delta
go test ./internal/chunker -bench . -benchmem
```

Run the S3 backend integration test against a local MinIO endpoint:

```sh
CHUNKGATE_S3_TEST_ENDPOINT=http://localhost:9000 go test ./internal/backend -run MinIO
```

Run the PostgreSQL metadata integration tests against a local database:

```sh
CHUNKGATE_POSTGRES_TEST_DSN='postgres://chunkgate:chunkgate@localhost:15432/chunkgate?sslmode=disable' \
  go test ./internal/metadata -run Postgres
```

Operational endpoints:

- `/healthz`: liveness check.
- `/readyz`: readiness check for metadata, backend, scratch disk, and shutdown drain state.
- `/metrics`: Prometheus text metrics for requests, uploads, chunks, limiter queueing, and GC.
- `/debug/pprof/`: pprof endpoints, only when `CHUNKGATE_DEBUG_PPROF_ENABLED=true`.

## Storage Layers

The core object service depends on `backend.BlockStore`, so filesystem and S3-compatible block storage share the same code path. The metadata layer depends on `metadata.Store`, so SQLite and PostgreSQL can be selected without changing the API, object, chunking, multipart, or GC layers.

ChunkGate-aware clients can use the manifest-first delta protocol documented in [docs/delta-protocol.md](docs/delta-protocol.md). Ordinary S3 clients continue to use full-object `GET`, `HEAD`, and range reads.

Security assumptions and remaining risks are documented in [docs/threat-model.md](docs/threat-model.md).

Operational runbooks:

- [docs/backup-restore.md](docs/backup-restore.md)
- [docs/upgrade.md](docs/upgrade.md)
- [docs/release.md](docs/release.md)
