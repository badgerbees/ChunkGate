# ChunkGate

ChunkGate is a self-hosted S3-compatible deduplication proxy written in Go. It exposes a normal object endpoint to clients while storing incoming streams as content-defined chunks behind the scenes.

This repository currently contains the deployable base architecture:

- S3-like `PUT`, `GET`, `HEAD`, `DELETE`, and multipart initiate/upload/complete/abort routes.
- AWS Signature Version 4 verification when local credentials are configured.
- Tenant isolation derived from authenticated access keys.
- Bucket-level SDK compatibility routes for list, create, head, delete, and empty object listings.
- Object header preservation for common HTTP headers and `x-amz-meta-*` metadata.
- FastCDC-style content-defined chunking with a small-file bypass.
- Tenant-sharded SQLite metadata files under `data/metadata/tenant_{id}.db`.
- Tenant-scoped filesystem block storage under `data/backend`.
- Sequential multipart spooling under `data/scratch`.
- Atomic local capacity reservations for multipart upload initiation.
- CPU concurrency gating around chunk processing.
- Soft-delete metadata behavior plus a GC sweeper package.

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

The compose file persists all ChunkGate state in the `chunkgate-data` volume.

## Configuration

| Variable | Default |
| --- | --- |
| `CHUNKGATE_LISTEN` | `:8080` |
| `CHUNKGATE_DATA_DIR` | `data` |
| `CHUNKGATE_METADATA_DIR` | `${CHUNKGATE_DATA_DIR}/metadata` |
| `CHUNKGATE_BACKEND_DIR` | `${CHUNKGATE_DATA_DIR}/backend` |
| `CHUNKGATE_SCRATCH_DIR` | `${CHUNKGATE_DATA_DIR}/scratch` |
| `CHUNKGATE_LOCAL_CAPACITY_BYTES` | `21474836480` |
| `CHUNKGATE_MAX_CONCURRENT_CHUNKERS` | `0` meaning CPU count |
| `CHUNKGATE_SMALL_FILE_THRESHOLD_BYTES` | `5242880` |
| `CHUNKGATE_CHUNK_MIN_BYTES` | `524288` |
| `CHUNKGATE_CHUNK_AVG_BYTES` | `1048576` |
| `CHUNKGATE_CHUNK_MAX_BYTES` | `4194304` |
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

## Development

```sh
go test ./...
go vet ./...
go build -o chunkgate ./cmd/chunkgate
```

## Next Adapters

The core object service depends on `backend.BlockStore`, so adding AWS S3, MinIO, or Cloudflare R2 storage is a bounded adapter task. The metadata layer depends on `metadata.Store`, so a PostgreSQL implementation can be added without changing the API or chunking layers.
