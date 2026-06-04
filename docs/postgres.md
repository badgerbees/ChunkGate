# PostgreSQL Metadata Backend

SQLite is the zero-config default and is the right choice for a single ChunkGate instance, local deployments, CI caches, and small self-hosted installs.

Choose PostgreSQL when more than one ChunkGate process must serve the same tenants, when metadata needs centralized backup and observability, or when tenant/object churn is high enough that one local SQLite shard per tenant becomes operationally awkward.

Enable PostgreSQL with:

```sh
CHUNKGATE_METADATA=postgres
CHUNKGATE_POSTGRES_DSN='postgres://chunkgate:chunkgate@postgres:5432/chunkgate?sslmode=disable'
```

The PostgreSQL schema stores tenant identity as `tenant_id` in composite keys. This keeps tenants isolated inside one shared database while allowing all ChunkGate replicas to use the same `metadata.Store` interface and the same object, multipart, and GC code paths.

Connection pool settings:

- `CHUNKGATE_POSTGRES_MAX_OPEN_CONNS`: maximum open database connections, default `16`.
- `CHUNKGATE_POSTGRES_MAX_IDLE_CONNS`: maximum idle database connections, default `4`.
- `CHUNKGATE_POSTGRES_CONN_MAX_LIFETIME_SECONDS`: connection lifetime before recycle, default `1800`.

The metadata transaction model is intentionally application-managed. Commits, overwrites, deletes, block reference count updates, and multipart part saves happen inside explicit Go transactions rather than database triggers.
