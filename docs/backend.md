# Backend Providers

ChunkGate stores deduplicated blocks through the `backend.BlockStore` interface. The filesystem provider remains the default for local development. Cloud providers store the same block payloads in S3-compatible services, Azure Blob Storage, Google Cloud Storage, or OpenStack Swift.

See `docs/provider-test-matrix.md` for the always-on, CI, and optional live tests that verify each provider path.

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
CHUNKGATE_S3_PROVIDER=generic
CHUNKGATE_S3_ENDPOINT=s3.amazonaws.com
CHUNKGATE_S3_REGION=us-east-1
CHUNKGATE_S3_BUCKET=my-chunkgate-blocks
CHUNKGATE_S3_ACCESS_KEY_ID=...
CHUNKGATE_S3_SECRET_ACCESS_KEY=...
```

The same adapter works with AWS S3, MinIO, Cloudflare R2, Supabase Storage S3, and other S3-compatible endpoints. If the endpoint includes `http://` or `https://`, ChunkGate infers TLS from that scheme. Otherwise, `CHUNKGATE_S3_USE_TLS` controls whether HTTPS is used.

`CHUNKGATE_S3_PROVIDER` is a preset label for known S3-compatible services. It does not replace endpoint credentials; it keeps provider intent explicit and gives ChunkGate a place for provider-specific block-operation quirks. Supported values are `generic`, `aws`, `minio`, `r2`, `supabase`, and `b2`.

### Presets

AWS S3:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=aws
CHUNKGATE_S3_ENDPOINT=https://s3.amazonaws.com
CHUNKGATE_S3_REGION=us-east-1
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=false
CHUNKGATE_S3_USE_TLS=true
```

MinIO:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=minio
CHUNKGATE_S3_ENDPOINT=http://minio:9000
CHUNKGATE_S3_REGION=us-east-1
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=true
CHUNKGATE_S3_USE_TLS=false
```

Cloudflare R2:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=r2
CHUNKGATE_S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
CHUNKGATE_S3_REGION=auto
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=true
CHUNKGATE_S3_USE_TLS=true
```

Supabase Storage S3:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=supabase
CHUNKGATE_S3_ENDPOINT=https://project_ref.storage.supabase.co/storage/v1/s3
CHUNKGATE_S3_REGION=us-east-1
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=true
CHUNKGATE_S3_USE_TLS=true
```

Backblaze B2 through the S3-compatible API:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=b2
CHUNKGATE_S3_ENDPOINT=https://s3.us-west-004.backblazeb2.com
CHUNKGATE_S3_REGION=us-west-004
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=true
CHUNKGATE_S3_USE_TLS=true
```

Backblaze B2 is officially supported through B2's S3-compatible API. Use `CHUNKGATE_BACKEND=s3` and `CHUNKGATE_S3_PROVIDER=b2`; there is also a ready-to-edit profile at `config/examples/production-b2.env`.

ChunkGate does not currently ship a native B2 provider. For its block-store workload, ChunkGate needs bounded object upload, object download, object metadata/existence checks, tenant-scoped object keys, and bulk deletion. The existing S3-compatible B2 path already uses the shared S3 provider logic for those operations, including S3 `DeleteObjects` batching for GC cleanup.

A native B2 provider would add extra operational state without a clear benefit today: account authorization, upload URL acquisition and refresh, B2 file IDs/version handling, native download URLs, native file-info calls, and native delete-version behavior. Native B2 should be reconsidered only if the S3-compatible API proves insufficient for ChunkGate's block operations, such as a missing required delete behavior, a measurable throughput/retry advantage, or a B2-only feature that materially improves deduplicated block storage.

Endpoints may include a provider base path. For example, Supabase Storage uses:

```sh
CHUNKGATE_S3_ENDPOINT=https://project_ref.storage.supabase.co/storage/v1/s3
CHUNKGATE_S3_PATH_STYLE=true
```

When a base path is present, ChunkGate signs requests against the full provider path before sending them, so no external reverse proxy is needed. Host-only endpoints continue to use the MinIO Go SDK path for mature S3-compatible behavior.

Blocks are stored under tenant-isolated object keys:

```text
{CHUNKGATE_S3_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

The S3 backend applies the same lowercase SHA-256 hash validation before constructing object keys, keeping deduplicated block namespaces tenant-scoped.

`CHUNKGATE_S3_PATH_STYLE=true` is the default because it works well with MinIO, R2, B2, Supabase, and many self-hosted endpoints. Set it to `false` for virtual-hosted bucket routing, especially with AWS S3 buckets that are addressed as `https://bucket.s3.amazonaws.com/key`.

## Reliability

The S3 provider applies a per-operation timeout with `CHUNKGATE_S3_TIMEOUT_SECONDS` and retries transient failures with `CHUNKGATE_S3_MAX_RETRIES`. Bulk GC deletion uses the S3 multi-object delete API through `DeleteObjects` and batches requests at the S3-compatible provider limit of 1,000 keys per call.

Backend failures are mapped before they reach S3 clients:

- Missing block reads become `NoSuchKey`.
- Timeout, throttling, and 5xx backend failures become `ServiceUnavailable`.
- Other unexpected backend errors remain `InternalError`.

## Azure Blob Storage

The Azure backend stores ChunkGate blocks as native Azure Blob objects in one container:

```sh
CHUNKGATE_BACKEND=azure
CHUNKGATE_AZURE_ACCOUNT_NAME=chunkgatestorage
CHUNKGATE_AZURE_ACCOUNT_KEY=...
CHUNKGATE_AZURE_CONTAINER=chunkgate-blocks
CHUNKGATE_AZURE_AUTH=shared-key
```

If `CHUNKGATE_AZURE_ENDPOINT` is not set, ChunkGate builds the service endpoint from the account name:

```text
https://{CHUNKGATE_AZURE_ACCOUNT_NAME}.blob.core.windows.net
```

Use `CHUNKGATE_AZURE_ENDPOINT` for Azurite, private endpoints, sovereign clouds, or custom endpoints:

```sh
CHUNKGATE_AZURE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1
```

Authentication modes:

- `CHUNKGATE_AZURE_AUTH=auto` uses shared-key auth when `CHUNKGATE_AZURE_ACCOUNT_KEY` is set, otherwise it uses Azure DefaultAzureCredential.
- `CHUNKGATE_AZURE_AUTH=shared-key` requires `CHUNKGATE_AZURE_ACCOUNT_NAME` and `CHUNKGATE_AZURE_ACCOUNT_KEY`.
- `CHUNKGATE_AZURE_AUTH=default` uses managed identity or the normal Azure SDK default credential chain.

Blocks are stored under tenant-isolated blob names:

```text
{CHUNKGATE_AZURE_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

The Azure backend uses container property checks for readiness, uploads each bounded chunk as a single block blob, and batches deletes with Azure Blob Batch. Azure batch requests support up to 256 blob deletes per request; if a target endpoint does not support batch operations, ChunkGate falls back to bounded concurrent single-blob deletes.

## Google Cloud Storage

The GCS backend stores ChunkGate blocks as native objects in one GCS bucket:

```sh
CHUNKGATE_BACKEND=gcs
CHUNKGATE_GCS_PROJECT_ID=my-gcp-project
CHUNKGATE_GCS_BUCKET=chunkgate-blocks
CHUNKGATE_GCS_AUTH=service-account
CHUNKGATE_GCS_CREDENTIALS_FILE=/run/secrets/chunkgate-gcs-service-account.json
```

Authentication modes:

- `CHUNKGATE_GCS_AUTH=auto` uses service-account auth when a credentials file or JSON value is configured, emulator auth when an endpoint is configured, otherwise Application Default Credentials.
- `CHUNKGATE_GCS_AUTH=service-account` requires `CHUNKGATE_GCS_CREDENTIALS_FILE` or `CHUNKGATE_GCS_CREDENTIALS_JSON`.
- `CHUNKGATE_GCS_AUTH=default` uses Application Default Credentials, including workload identity, attached service accounts, or local `gcloud` credentials.
- `CHUNKGATE_GCS_AUTH=emulator` disables authentication for fake-gcs-server or another local emulator.

Use `CHUNKGATE_GCS_ENDPOINT` for fake-gcs-server, private test endpoints, or compatible emulators:

```sh
CHUNKGATE_GCS_ENDPOINT=http://127.0.0.1:4443/storage/v1/
CHUNKGATE_GCS_AUTH=emulator
```

Blocks are stored under tenant-isolated object names:

```text
{CHUNKGATE_GCS_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

The GCS backend validates lowercase SHA-256 block hashes before object-name construction, uses bucket metadata checks for readiness, uploads each bounded chunk as a single object, and deletes objects through bounded concurrent delete calls.

## OpenStack Swift

The Swift backend stores ChunkGate blocks as native Swift objects in one container, without requiring an S3 compatibility layer:

```sh
CHUNKGATE_BACKEND=swift
CHUNKGATE_SWIFT_AUTH_URL=https://openstack.example.com:5000/v3
CHUNKGATE_SWIFT_USERNAME=chunkgate
CHUNKGATE_SWIFT_PASSWORD=...
CHUNKGATE_SWIFT_PROJECT_NAME=service
CHUNKGATE_SWIFT_DOMAIN_NAME=Default
CHUNKGATE_SWIFT_REGION=RegionOne
CHUNKGATE_SWIFT_CONTAINER=chunkgate-blocks
CHUNKGATE_SWIFT_AUTH=password
```

Most OpenStack values map directly from a normal `openrc` file:

- `OS_AUTH_URL` -> `CHUNKGATE_SWIFT_AUTH_URL`
- `OS_USERNAME` or `OS_USER_ID` -> `CHUNKGATE_SWIFT_USERNAME` or `CHUNKGATE_SWIFT_USER_ID`
- `OS_PASSWORD` -> `CHUNKGATE_SWIFT_PASSWORD`
- `OS_PROJECT_ID` or `OS_PROJECT_NAME` -> `CHUNKGATE_SWIFT_PROJECT_ID` or `CHUNKGATE_SWIFT_PROJECT_NAME`
- `OS_USER_DOMAIN_ID` or `OS_USER_DOMAIN_NAME` -> `CHUNKGATE_SWIFT_DOMAIN_ID` or `CHUNKGATE_SWIFT_DOMAIN_NAME`
- `OS_REGION_NAME` -> `CHUNKGATE_SWIFT_REGION`

Authentication modes:

- `CHUNKGATE_SWIFT_AUTH=auto` uses application credentials when application credential fields are present, otherwise password auth.
- `CHUNKGATE_SWIFT_AUTH=password` requires a username or user ID plus `CHUNKGATE_SWIFT_PASSWORD`.
- `CHUNKGATE_SWIFT_AUTH=application-credential` requires `CHUNKGATE_SWIFT_APPLICATION_CREDENTIAL_SECRET` and either `CHUNKGATE_SWIFT_APPLICATION_CREDENTIAL_ID` or `CHUNKGATE_SWIFT_APPLICATION_CREDENTIAL_NAME`.

Set `CHUNKGATE_SWIFT_ENDPOINT` only when the service catalog does not return the desired object-store endpoint, or when you need to force an internal/private endpoint:

```sh
CHUNKGATE_SWIFT_ENDPOINT=https://swift.example.com/v1/AUTH_project_id/
```

Blocks are stored under tenant-isolated object names:

```text
{CHUNKGATE_SWIFT_PREFIX}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```

The Swift backend uses container metadata checks for readiness, uploads each bounded chunk as one object, attempts Swift bulk delete first, and falls back to bounded concurrent single-object deletes when a Swift-compatible deployment does not enable bulk-delete middleware. `CHUNKGATE_SWIFT_INSECURE_SKIP_VERIFY=true` is available for private lab clusters with self-signed certificates, but should remain disabled in production.

## Dell ECS and EMC Atmos

Dell ECS environments should use ChunkGate's S3-compatible backend first, or the Swift backend when Swift is the available object protocol. ECS exposes multiple protocol heads over the same storage platform, including S3, Swift, and Atmos, and ChunkGate's bounded block-store operations map cleanly to S3 and Swift.

Recommended ECS S3 profile:

```sh
CHUNKGATE_BACKEND=s3
CHUNKGATE_S3_PROVIDER=generic
CHUNKGATE_S3_ENDPOINT=https://ecs.example.com:9021
CHUNKGATE_S3_BUCKET=chunkgate-blocks
CHUNKGATE_S3_PATH_STYLE=true
CHUNKGATE_S3_USE_TLS=true
```

See `config/examples/production-ecs-s3.env` for a full profile.

Recommended ECS Swift profile:

```sh
CHUNKGATE_BACKEND=swift
CHUNKGATE_SWIFT_AUTH_URL=https://ecs.example.com:4443/v3
CHUNKGATE_SWIFT_ENDPOINT=https://ecs.example.com:9025/v1/AUTH_project_id/
CHUNKGATE_SWIFT_CONTAINER=chunkgate-blocks
CHUNKGATE_SWIFT_AUTH=password
```

See `config/examples/production-ecs-swift.env` for a full profile.

ChunkGate does not currently ship a native `atmos` backend. Native Atmos support would require a dedicated REST/signing adapter for namespace object `PUT`, `GET`, `HEAD`, and `DELETE`, including Atmos `x-emc-*` authentication headers. That adapter would be narrower than the S3 and Swift providers and cannot be validated well without an Atmos-compatible appliance or emulator in CI.

For legacy Atmos-only deployments, put an S3 or Swift gateway/protocol head in front of the storage when possible, or request native Atmos support with access to a test appliance. If native Atmos is added later, it should stay limited to ChunkGate block operations and preserve the same object name layout:

```text
{prefix}/tenants/{tenant}/blocks/{hash-prefix}/{hash}
```
