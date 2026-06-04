# Backend Providers

ChunkGate stores deduplicated blocks through the `backend.BlockStore` interface. The filesystem provider remains the default for local development. The S3 provider stores the same block payloads in any S3-compatible service.

## Filesystem

The filesystem backend is selected with:

```sh
CHUNKGATE_BACKEND=filesystem
```

Blocks are written below `CHUNKGATE_BACKEND_DIR` using tenant-isolated paths:

```text
tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

## S3-Compatible

The S3 backend is selected with:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_ENDPOINT=s3.amazonaws.com
CHUNKGATE_S3_REGION=us-east-1
CHUNKGATE_S3_BUCKET=my-chunkgate-blocks
CHUNKGATE_S3_ACCESS_KEY_ID=...
CHUNKGATE_S3_SECRET_ACCESS_KEY=...
```

The same adapter works with AWS S3, MinIO, Cloudflare R2, and other S3-compatible endpoints. If the endpoint includes `http://` or `https://`, ChunkGate infers TLS from that scheme. Otherwise, `CHUNKGATE_S3_USE_TLS` controls whether HTTPS is used.

Blocks are stored under tenant-isolated object keys:

```text
{CHUNKGATE_S3_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

`CHUNKGATE_S3_PATH_STYLE=true` is the default because it works well with MinIO and many self-hosted endpoints. Set it to `false` for virtual-hosted bucket routing.

## Reliability

The S3 provider applies a per-operation timeout with `CHUNKGATE_S3_TIMEOUT_SECONDS` and retries transient failures with `CHUNKGATE_S3_MAX_RETRIES`. Bulk GC deletion uses the S3 multi-object delete API through `DeleteObjects`.

Backend failures are mapped before they reach S3 clients:

- Missing block reads become `NoSuchKey`.
- Timeout, throttling, and 5xx backend failures become `ServiceUnavailable`.
- Other unexpected backend errors remain `InternalError`.
