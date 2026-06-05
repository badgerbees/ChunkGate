# Delta Client Protocol

ChunkGate exposes a small companion API for clients that understand deduplicated manifests. Ordinary S3 clients should continue using normal `GET`, `HEAD`, and range requests; they receive full objects exactly as before.

All companion endpoints are versioned under `/_chunkgate/v1/` and use the same authentication and tenant identity as S3 requests. In production, requests must be AWS Signature Version 4 signed with a configured ChunkGate access key. Local anonymous access works only when `CHUNKGATE_ALLOW_ANONYMOUS=true`.

## Manifest

Retrieve a manifest:

```http
GET /_chunkgate/v1/manifest?bucket=builds&key=artifact.tar
```

Successful responses are JSON:

```json
{
  "version": 1,
  "bucket": "builds",
  "key": "artifact.tar",
  "size": 1048576,
  "etag": "\"4d5d1cba9eb18884a5410f4b83bc6951\"",
  "object_md5": "4d5d1cba9eb18884a5410f4b83bc6951",
  "headers": {
    "Content-Type": "application/x-tar"
  },
  "chunks": [
    {
      "index": 0,
      "hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "offset": 0,
      "size": 1048576
    }
  ]
}
```

The `chunks` array is ordered by object byte position. Each chunk hash is a lowercase SHA-256 hex digest of the chunk bytes. `object_md5` is derived from the S3 ETag when the ETag is a full-object MD5.

## Selected Chunks

Request only chunks that are missing from the local cache:

```http
POST /_chunkgate/v1/chunks
Content-Type: application/json

{
  "bucket": "builds",
  "key": "artifact.tar",
  "hashes": [
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  ]
}
```

The response returns base64-encoded chunk bytes:

```json
{
  "version": 1,
  "chunks": [
    {
      "hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "size": 1048576,
      "data": "..."
    }
  ]
}
```

Chunk requests are constrained to hashes that appear in the requested object manifest. This prevents clients from using the endpoint to read arbitrary tenant blocks.

## Client Integrity Rules

ChunkGate-aware clients should:

- Fetch the manifest first.
- Check the local block cache for each unique chunk hash.
- Request only missing hashes through `/_chunkgate/v1/chunks`.
- Verify every downloaded chunk by SHA-256 before caching it.
- Reconstruct the object in manifest order.
- Verify the reconstructed object size and `object_md5` when present.

The proof-of-concept CLI implements this flow:

```sh
chunkgate-delta get \
  -endpoint http://localhost:8080 \
  -bucket builds \
  -key artifact.tar \
  -output artifact.tar \
  -cache-dir .chunkgate-cache
```

For signed deployments, pass `-access-key` and `-secret-key` or set `CHUNKGATE_ACCESS_KEY_ID` and `CHUNKGATE_SECRET_ACCESS_KEY`.

## Ordinary S3 Fallback

Clients that do not know this protocol should ignore it entirely. Standard S3 `GET` and `HEAD` return complete objects, and standard range reads continue to use `Range: bytes=start-end`. If a companion client cannot use the manifest endpoint, it can fall back to ordinary S3 `GET` without changing object correctness; it simply loses delta-download savings for that request.
