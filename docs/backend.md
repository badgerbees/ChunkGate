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

Block hashes must be lowercase SHA-256 hex strings before they are accepted by the filesystem backend. Tenant path components are sanitized before path construction and verified to stay under the configured backend root.

Local block encryption can be enabled with:

```sh
CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY=0123456789abcdef0123456789abcdef
```

The key may be raw text, base64, or hex, and must decode to a 16, 24, or 32 byte AES key. Encrypted filesystem blocks are written with AES-GCM and are not compatible with reading older plaintext blocks through an encrypted store.

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

The same adapter works with AWS S3, MinIO, Cloudflare R2, Supabase Storage S3, and other S3-compatible endpoints. If the endpoint includes `http://` or `https://`, ChunkGate infers TLS from that scheme. Otherwise, `CHUNKGATE_S3_USE_TLS` controls whether HTTPS is used.

Endpoints may include a provider base path. For example, Supabase Storage uses:

```sh
CHUNKGATE_S3_ENDPOINT=https://project_ref.storage.supabase.co/storage/v1/s3
CHUNKGATE_S3_PATH_STYLE=true
```

When a base path is present, ChunkGate signs requests against the full provider path before sending them, so no external reverse proxy is needed.

Blocks are stored under tenant-isolated object keys:

```text
{CHUNKGATE_S3_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

The S3 backend applies the same lowercase SHA-256 hash validation before constructing object keys, keeping deduplicated block namespaces tenant-scoped.

`CHUNKGATE_S3_PATH_STYLE=true` is the default because it works well with MinIO and many self-hosted endpoints. Set it to `false` for virtual-hosted bucket routing.

## Reliability

The S3 provider applies a per-operation timeout with `CHUNKGATE_S3_TIMEOUT_SECONDS` and retries transient failures with `CHUNKGATE_S3_MAX_RETRIES`. Bulk GC deletion uses the S3 multi-object delete API through `DeleteObjects`.

Backend failures are mapped before they reach S3 clients:

- Missing block reads become `NoSuchKey`.
- Timeout, throttling, and 5xx backend failures become `ServiceUnavailable`.
- Other unexpected backend errors remain `InternalError`.
