# Threat Model

ChunkGate is designed to sit on a trusted host or private network between S3 clients and a storage backend. This document records the security assumptions that the code is built around today.

## Assets

- Tenant object metadata, manifests, multipart state, and block reference counts.
- Deduplicated block payloads stored in the filesystem or S3-compatible backend.
- Scratch files created while multipart uploads are in progress.
- ChunkGate client credentials used for AWS Signature Version 4 authentication.
- Backend provider credentials used to write blocks to S3-compatible storage.

## Tenant Identity

Production mode requires SigV4 authentication. Anonymous tenant selection is disabled by default and must be explicitly enabled with `CHUNKGATE_ALLOW_ANONYMOUS=true`, which is intended for local development only.

Tenant identity is derived from the authenticated access key, or from the optional tenant value configured with `CHUNKGATE_TENANT_ID` or a `CHUNKGATE_CREDENTIALS` entry. Requests cannot select a production tenant with `X-ChunkGate-Tenant`.

Unknown access keys, missing signatures, and bad signatures return S3-shaped XML errors. Credential lookup avoids direct map probes by comparing access keys through fixed-length SHA-256 digests.

## Isolation Boundaries

Deduplication is tenant-scoped. Object metadata, block reference counts, filesystem block paths, S3 backend keys, and multipart scratch directories all include a tenant namespace. Identical content uploaded by two tenants is stored as two tenant-scoped block entries by default.

SQLite metadata uses one shard file per sanitized tenant identifier. Tenant names that are not safe as path components are mapped to a short SHA-256-derived identifier before the database path is created. PostgreSQL stores tenant IDs as scoped keys in shared tables.

Missing objects and missing multipart upload IDs return generic S3-style `NoSuchKey` or `NoSuchUpload` responses, including when the missing data belongs to a different tenant.

## Input Validation

ChunkGate validates S3-facing bucket names, object keys, upload IDs, and part numbers before object, multipart, metadata, or backend work begins. Multipart upload IDs accepted by the HTTP API must be 32 lowercase hex characters, and part numbers must be in the S3 range `1..10000`.

Filesystem block paths are constructed only from sanitized tenant IDs and lowercase SHA-256 block hashes. The final absolute block path is checked against the configured backend root before it is used.

## Local Encryption

Filesystem block storage can be encrypted with `CHUNKGATE_LOCAL_BLOCK_ENCRYPTION_KEY`. The key must decode to a valid AES key length, and blocks are encrypted with AES-GCM before being written to disk.

Key storage, rotation, backup, and recovery are operator responsibilities. ChunkGate does not yet implement transparent key rotation or envelope encryption. Encrypted local block storage protects block contents at rest, but it does not encrypt SQLite metadata or multipart scratch files.

## Backend And Network Assumptions

ChunkGate should be served behind TLS for production traffic, either directly through a TLS-aware reverse proxy or platform load balancer. Backend S3-compatible credentials should be scoped to the configured bucket and prefix where possible.

Operational endpoints are unauthenticated today. `/healthz`, `/readyz`, and `/metrics` should be exposed only on trusted networks. `/debug/pprof/` is disabled by default and must be enabled explicitly.

## Remaining Risks

- Timing behavior is reduced where practical for credential lookup, but ChunkGate does not claim formal side-channel resistance.
- Local filesystem encryption currently covers committed block files only, not metadata or multipart scratch.
- Anonymous mode is a convenience path and should not be enabled on untrusted networks.
- Backend administrators with direct access to the block bucket or filesystem can still inspect, alter, or delete stored data unless external controls prevent it.
