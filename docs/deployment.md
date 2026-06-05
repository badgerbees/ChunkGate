# Deployment

ChunkGate can run as a local binary, a Docker Compose stack, or a Kubernetes workload. Production deployments should keep anonymous mode disabled and provide SigV4 client credentials through environment variables or platform secrets.

## Local Binary

For local curl testing:

```sh
set -a
. config/examples/local-anonymous.env
set +a
go run ./cmd/chunkgate
```

For signed local testing:

```sh
CHUNKGATE_ACCESS_KEY_ID=tenant-a \
CHUNKGATE_SECRET_ACCESS_KEY=dev-secret \
go run ./cmd/chunkgate
```

Then point an S3 client at `http://localhost:8080`.

If your client uses virtual-hosted-style buckets, set `CHUNKGATE_VIRTUAL_HOSTS` to the public endpoint hostname. For example, `CHUNKGATE_VIRTUAL_HOSTS=s3.example.com` lets `photos.s3.example.com/cat.jpg` resolve to bucket `photos` and key `cat.jpg`. Browser clients can use the static `CHUNKGATE_CORS_*` settings for preflight and exposed response headers.

## Docker

Build the image locally:

```sh
docker build -t chunkgate:local .
docker run --rm -p 8080:8080 \
  -v chunkgate-data:/data \
  -e CHUNKGATE_ACCESS_KEY_ID=tenant-a \
  -e CHUNKGATE_SECRET_ACCESS_KEY=dev-secret \
  chunkgate:local
```

The image runs as UID/GID `10001`, exposes `/healthz` as its container healthcheck, and writes state below `/data`.

## Production Compose With MinIO

Copy the example environment file and replace all placeholder values:

```sh
cp deploy/compose/minio.env.example deploy/compose/minio.env
```

Start the stack:

```sh
docker compose --env-file deploy/compose/minio.env \
  -f deploy/compose/compose.minio.yml up -d
```

This stack starts MinIO, creates the backend block bucket, and starts ChunkGate with SQLite metadata on a persistent `chunkgate-data` volume. Expose MinIO and ChunkGate through TLS-aware infrastructure before using it outside a trusted network.

## Kubernetes

Apply the base manifest:

```sh
kubectl apply -f deploy/kubernetes/chunkgate.yaml
```

Before starting production traffic, replace the placeholder secret:

```sh
kubectl -n chunkgate create secret generic chunkgate-secrets \
  --from-literal=CHUNKGATE_ACCESS_KEY_ID=tenant-a \
  --from-literal=CHUNKGATE_SECRET_ACCESS_KEY=dev-secret \
  --from-literal=CHUNKGATE_TENANT_ID=default \
  --from-literal=CHUNKGATE_S3_ACCESS_KEY_ID=minio-or-s3-access-key \
  --from-literal=CHUNKGATE_S3_SECRET_ACCESS_KEY=minio-or-s3-secret-key \
  --dry-run=client -o yaml | kubectl apply -f -
```

Patch `chunkgate-config` for your S3-compatible backend endpoint, bucket, TLS mode, and storage limits. The default manifest uses one replica with SQLite metadata. Use PostgreSQL metadata before scaling replicas above one.

Check rollout and readiness:

```sh
kubectl -n chunkgate rollout status deployment/chunkgate
kubectl -n chunkgate get pods,svc,pvc
kubectl -n chunkgate port-forward svc/chunkgate 8080:8080
```

## Configuration Samples

- `config/examples/local-anonymous.env`: local curl-only profile.
- `config/examples/production-s3.env`: production S3-compatible block backend profile.
- `config/examples/postgres.env`: shared PostgreSQL metadata profile.
- `deploy/compose/minio.env.example`: environment file for the production Compose stack.
