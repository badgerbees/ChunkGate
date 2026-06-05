# Upgrade And Migration

ChunkGate runs metadata migrations on startup. SQLite migrations are applied per tenant shard as each shard is opened. PostgreSQL migrations are applied when the shared metadata store is opened.

## Before Upgrading

1. Read the release notes for breaking configuration or schema notes.
2. Back up metadata using [backup-restore.md](backup-restore.md).
3. Confirm the block backend credentials still point to the intended bucket or filesystem path.
4. Run the new version in a staging environment when possible.

## Docker Compose Upgrade

```sh
docker compose --env-file deploy/compose/minio.env \
  -f deploy/compose/compose.minio.yml pull chunkgate
docker compose --env-file deploy/compose/minio.env \
  -f deploy/compose/compose.minio.yml up -d chunkgate
docker compose --env-file deploy/compose/minio.env \
  -f deploy/compose/compose.minio.yml logs -f chunkgate
```

Watch `/readyz` and verify object upload/download before resuming heavier traffic.

## Kubernetes Upgrade

Patch the image tag and wait for rollout:

```sh
kubectl -n chunkgate set image deployment/chunkgate \
  chunkgate=ghcr.io/badgerbees/chunkgate:vX.Y.Z
kubectl -n chunkgate rollout status deployment/chunkgate
```

If readiness fails, inspect logs:

```sh
kubectl -n chunkgate logs deployment/chunkgate
```

Rollback to the previous image only after checking whether the failed version applied a forward-only metadata migration. If a migration changed the schema, restore metadata from backup before running an older binary.

## SQLite Notes

SQLite remains the zero-config default and is best suited to one ChunkGate process with local durable storage. Because shards are opened lazily, a tenant's migration may happen the first time that tenant receives traffic after an upgrade.

For high tenant counts, consider warming known tenants with a light `HEAD` or manifest request after upgrade so migrations happen before peak traffic.

## PostgreSQL Notes

Use PostgreSQL before horizontally scaling ChunkGate replicas. Before upgrading multiple replicas:

1. Scale replicas down to one.
2. Start the new version and let migrations complete.
3. Confirm readiness and basic S3 operations.
4. Scale replicas back up.

Use `pg_dump` or your managed PostgreSQL snapshot facility before changing versions.

## Configuration Migration

New releases may add environment variables with safe defaults. Prefer committing environment files or Kubernetes ConfigMaps separately from secrets so changes are reviewable. Never enable `CHUNKGATE_ALLOW_ANONYMOUS=true` in production as a temporary workaround for credential issues.
