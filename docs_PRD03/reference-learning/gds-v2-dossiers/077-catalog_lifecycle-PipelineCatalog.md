# 077 catalog_lifecycle PipelineCatalog

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java |
| lane | catalog_lifecycle |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 77 |
| line_count | 142 |
| fan_in / fan_out | 39 / 5 |

## Why This File Matters

This file is the global in-memory catalog for training pipelines; it defines the lifecycle and lookup rules for pipeline artifacts in process memory.

## Public Contract

- Static, process-level store:
  - `ConcurrentHashMap<String, PipelineUserCatalog> userCatalogs`
- Pipeline operations:
  - `set(user, pipelineName, pipeline)`
  - `exists(user, pipelineName)`
  - `get(user, pipelineName)`
  - `getTyped(user, pipelineName, expectedClass)`
  - `drop(user, pipelineName)`
  - `removeAll()`
  - `getAllPipelines(user)`
- `getTyped` enforces runtime type through `Class#isInstance`.
- `PipelineUserCatalog` is nested package-private storage with:
  - `set`, `exists`, `get`, `drop`, `stream`.

## Internal Mechanics

- `set` rejects duplicate names (`IllegalStateException`).
- `get` and `drop` fail with `NoSuchElementException` when missing.
- `drop` returns removed pipeline wrapped in `Optional` inside map.
- `stream` exposes `PipelineCatalogEntry` with `pipelineName` and `pipeline` (`@ValueClass` record-like interface).
- The map is keyed only by user string, so lifecycle is user-scoped rather than per-database.

## Memory and Storage Implications

- Entire catalog is JVM memory only (no persistence).
- Concurrency model is `ConcurrentHashMap` + nested inner mutable map; no TTL or eviction defined.
- In Rust rewrite, this implies explicit lifecycle management, explicit clear/rebuild, and optional persistence strategy if behavior parity is needed.

## Snapshot And Catalog Implications

- No hard dependency on graph topology; this is purely metadata lifecycle.
- `removeAll()` becomes critical for test isolation and process-level reset.

## Verification Oracles

1. **WHEN** `set` is called twice with same `(user, name)`, **THEN** `IllegalStateException` SHALL be thrown.
2. **WHEN** caller fetches pipeline with wrong type via `getTyped`, **THEN** `IllegalArgumentException` SHALL include expected vs actual type info.
3. **WHEN** pipeline is dropped, **THEN** removed value SHALL be returned.
4. **WHEN** missing pipeline is fetched/dropped, **THEN** user-facing error SHALL mention pipeline name + user.

## Rust Rewrite Notes

- Keep catalog as a dedicated struct keyed by `user` with per-user lock-free or sync map.
- Preserve strong typing or include explicit runtime-type check on retrieval.
- Keep `removeAll()` as a first-class reset primitive for test harnesses.

## Dependencies Read Next

- `PipelineCatalogEntry` factory (`ImmutablePipelineCatalogEntry`)
- `TrainingPipeline<?>` hierarchy
- Pipeline procedure/facade layers reading/writing this catalog

## Dependents As Tests

- Unit test for duplicate write guard.
- Concurrency-safe concurrent set/get/drop tests for same user.
- Type mismatch retrieval test against subclass/superclass pipelines.

## Open Questions

- Should this in-memory structure become database-backed for multi-node correctness in the rewrite?
- What failure mode is preferred for `getTyped` mismatch: strict reject vs fallback cast behavior?

