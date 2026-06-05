# Provider Test Matrix

ChunkGate backends all run through the same `backend.BlockStore` contract tests. That shared contract verifies health checks, missing-block behavior, put/get/existence checks, bulk delete, tenant isolation, invalid hash rejection, and cleanup.

## Always-On Tests

These run with `go test ./...` and do not require external credentials:

| Provider path | Coverage | Test location |
| --- | --- | --- |
| Filesystem | Full block-store contract using a temporary local directory. | `internal/backend/fs_test.go` |
| Supabase/base-path S3 | Local HTTP S3-compatible server that verifies SigV4 headers, path signing, object operations, and `DeleteObjects`. | `internal/backend/s3_test.go` |
| OpenStack Swift mock | Local fake Swift object API with health, object operations, bulk delete, and fallback delete coverage. | `internal/backend/swift_test.go` |
| Backblaze B2 preset | S3-compatible B2 provider normalization and MinIO SDK routing. | `internal/backend/s3_test.go` |
| Dell ECS/Atmos decision path | Config accepts ECS through S3 or Swift and rejects native `atmos` until a native adapter exists. | `internal/config/config_test.go` |

## CI Integration Tests

The default GitHub Actions workflow starts local provider emulators and runs the same backend contract where practical:

| Provider path | CI target | Required environment |
| --- | --- | --- |
| S3-compatible | MinIO container | `CHUNKGATE_S3_TEST_ENDPOINT`, `CHUNKGATE_S3_TEST_BUCKET`, `CHUNKGATE_S3_TEST_ACCESS_KEY_ID`, `CHUNKGATE_S3_TEST_SECRET_ACCESS_KEY` |
| Azure Blob | Azurite container | `CHUNKGATE_AZURE_TEST_ENDPOINT`, `CHUNKGATE_AZURE_TEST_ACCOUNT_NAME`, `CHUNKGATE_AZURE_TEST_ACCOUNT_KEY`, `CHUNKGATE_AZURE_TEST_CONTAINER` |
| Google Cloud Storage | fake-gcs-server container | `CHUNKGATE_GCS_TEST_ENDPOINT`, `CHUNKGATE_GCS_TEST_BUCKET`, `CHUNKGATE_GCS_TEST_PROJECT_ID` |
| PostgreSQL metadata | Postgres service container | `CHUNKGATE_POSTGRES_TEST_DSN` |

Swift does not currently run against a containerized CI target because the always-on fake Swift contract is enough for ChunkGate's bounded block operations and avoids depending on an unstable local Swift image. Add a container target later if a reliable Swift test appliance becomes available.

## Optional Live Tests

These tests are skipped unless the listed environment variables are present:

| Provider path | Test | Environment |
| --- | --- | --- |
| Backblaze B2 through S3 | Runs the full S3 block-store contract against a real B2 bucket using the S3-compatible API. | `CHUNKGATE_B2_TEST_ENDPOINT`, `CHUNKGATE_B2_TEST_BUCKET`, `CHUNKGATE_B2_TEST_KEY_ID`, `CHUNKGATE_B2_TEST_APPLICATION_KEY`, optional `CHUNKGATE_B2_TEST_REGION`, `CHUNKGATE_B2_TEST_PREFIX`, `CHUNKGATE_B2_TEST_USE_TLS` |

Atmos live coverage is intentionally not listed yet. ChunkGate does not currently ship a native Atmos adapter; ECS/Atmos deployments should use the S3-compatible or Swift provider paths. If native Atmos is added later, its first test target should be a mock adapter that runs the shared block-store contract, followed by optional live tests gated by appliance-specific credentials.

## Running The Matrix

Run the always-on matrix locally:

```sh
go test ./...
```

Run the CI-style provider matrix by starting MinIO, Azurite, fake-gcs-server, and Postgres, then setting the same environment variables used in `.github/workflows/ci.yml`.

Run optional B2 live coverage only against a disposable bucket or isolated prefix, because the shared contract creates and deletes test blocks.
