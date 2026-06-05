# Backup And Restore

SQLite is the default metadata backend. ChunkGate stores one SQLite shard per tenant under `CHUNKGATE_METADATA_DIR`, which defaults to `${CHUNKGATE_DATA_DIR}/metadata`. A complete backup must include SQLite shard files and the matching WAL/SHM files when they exist.

The block backend also matters:

- Filesystem backend: back up `${CHUNKGATE_DATA_DIR}/backend` with metadata.
- S3-compatible backend: back up metadata and verify the backend bucket is protected by provider durability, lifecycle, and access controls.
- Multipart scratch files are restart state, not durable object state. They can be backed up for restart continuity, but committed object recovery depends on metadata plus blocks.

## Docker Volume Backup

The safest SQLite backup is taken with ChunkGate stopped:

```sh
docker compose stop chunkgate
mkdir -p backups
docker run --rm \
  -v chunkgate_chunkgate-data:/data:ro \
  -v "$PWD/backups:/backup" \
  alpine:3.22 \
  tar -czf /backup/chunkgate-data-$(date +%Y%m%d%H%M%S).tgz -C /data .
docker compose start chunkgate
```

If downtime is not acceptable, use a storage-level snapshot that preserves file consistency across `*.db`, `*.db-wal`, and `*.db-shm` files, or run SQLite's online `.backup` command per tenant shard from an environment that has `sqlite3` available.

## Kubernetes PVC Backup

For a conservative filesystem-level backup:

```sh
kubectl -n chunkgate scale deployment/chunkgate --replicas=0
```

Then take a CSI snapshot of the `chunkgate-data` PVC or mount the PVC into a one-shot backup pod and archive `/data`. Scale ChunkGate back after the snapshot completes:

```sh
kubectl -n chunkgate scale deployment/chunkgate --replicas=1
kubectl -n chunkgate rollout status deployment/chunkgate
```

## PostgreSQL Metadata Backup

When `CHUNKGATE_METADATA=postgres`, use your normal PostgreSQL backup process. A logical backup example:

```sh
pg_dump --format=custom --file=chunkgate-metadata.dump "$CHUNKGATE_POSTGRES_DSN"
```

Restore into a compatible PostgreSQL server, update `CHUNKGATE_POSTGRES_DSN`, and start ChunkGate. Blocks remain in the configured backend bucket or filesystem store.

## Restore

For Docker volume restores:

```sh
docker compose stop chunkgate
docker run --rm \
  -v chunkgate_chunkgate-data:/data \
  -v "$PWD/backups:/backup:ro" \
  alpine:3.22 \
  sh -c 'rm -rf /data/* && tar -xzf /backup/chunkgate-data-YYYYMMDDHHMMSS.tgz -C /data && chown -R 10001:10001 /data'
docker compose start chunkgate
```

After restore:

- Check `/readyz`.
- Upload and download a small object through an S3 client.
- Run a manual GC sweep only after verifying that restored metadata points at the intended block backend.

## Retention

Keep backups for longer than `CHUNKGATE_GC_MIN_ORPHAN_AGE_SECONDS`. This reduces the chance of restoring metadata that references blocks already swept by GC after a delete or overwrite.
