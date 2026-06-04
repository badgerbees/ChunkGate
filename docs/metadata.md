# Metadata Schema

SQLite remains the default metadata engine and is sharded per tenant. Each tenant receives an isolated database file under the configured metadata directory.

The current schema is migration-managed with `schema_migrations` and uses structured manifest rows:

- `objects`: logical S3-visible objects with lifecycle state, size, ETag, and headers.
- `object_chunks`: ordered manifest rows with sequence, byte offset, size, chunk hash, and backend key.
- `blocks`: deduplicated block registry with explicit application-managed reference counts.
- `multipart_sessions`: durable schema target for in-flight multipart upload sessions.
- `multipart_parts`: durable schema target for uploaded multipart part metadata.

Reference counts are updated explicitly by Go inside the same transaction that commits, overwrites, or deletes an object. SQLite triggers are intentionally avoided so the transaction model remains portable to a future PostgreSQL store.

When an object is deleted or overwritten, its `object_chunks` rows are removed after reference counts are decremented. Blocks whose `ref_count` reaches zero become eligible for asynchronous garbage collection.
