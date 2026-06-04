# Chunking Engine

ChunkGate uses `github.com/kalbasit/fastcdc` as its default content-defined chunking engine.

Why this engine:

- Implements FastCDC with Gear hash boundary detection.
- Supports configured minimum, target, and maximum chunk sizes.
- Provides a streaming `Next` API, which fits ChunkGate's bounded-memory upload path.
- Includes benchmarked throughput and allocation claims in the upstream package documentation.
- Is pure Go, so it works on non-SIMD and non-specialized platforms without architecture-specific assembly.

Fallback:

- Set `CHUNKGATE_CHUNK_ENGINE=builtin` to use ChunkGate's local pure-Go fallback engine.
- The fallback keeps the same min, average, max, and small-file bypass semantics.
- The fallback exists for emergency compatibility and local debugging; the production default is `fastcdc`.
