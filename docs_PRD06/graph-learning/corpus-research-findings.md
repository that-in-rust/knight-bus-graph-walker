# Corpus Research Findings

Date: 2026-07-08. Method: 18 GitHub API topic/keyword searches (538 unique
candidates surfaced) + verification of a curated slate; every row in
`corpus-ledger.tsv` carries API-verified stars, last-push date, primary
language, archived flag, and local-clone path where one already exists.

## The verified corpus: 117 repos

| Category | Count | Top by stars |
| --- | ---: | --- |
| graph-db | 31 | surrealdb 32.7k, dgraph 21.7k, neo4j 16.9k, cayley 15.0k, arangodb 14.2k |
| storage-engine | 25 | redis 75.4k, duckdb 39.3k, leveldb 39.2k, rocksdb 31.9k, dragonfly 30.9k |
| graph-analytics | 23 | petgraph 4.0k, jgrapht 2.8k, snap 2.3k, cugraph 2.2k, igraph 2.0k |
| vector-ann | 21 | milvus 45.2k, faiss 40.5k, qdrant 33.1k, chroma 28.8k, pgvector 22.1k |
| full-text-search | 17 | elasticsearch 77.6k, meilisearch 58.5k, typesense 26.3k, sonic 21.3k, tantivy 15.5k |

23 of the 117 are already cloned locally (see `local_clone` column); ~94
would be new shallow clones.

## Findings worth knowing before Phase B

1. **Kuzu is archived.** `kuzudb/kuzu` shows `archived=true` on GitHub — the
   company shut the repo. It stays in the corpus (we already hold a local
   clone and it remains the best-documented columnar graph engine), but it
   moves to the historical-classic bucket alongside GraphChi, Ligra, Giraph
   (also archived), and Blazegraph (archived; the ancestor of Amazon
   Neptune).
2. **Dgraph moved** to `hypermodeinc/` but GitHub still canonicalizes it as
   `dgraph-io/dgraph`; several curated repos were renamed upstream
   (`apache/incubator-hugegraph`→`apache/hugegraph`,
   `bitnine-oss/agensgraph`→`skaiworldwide-oss/agensgraph`,
   `lancedb/lance`→`lance-format/lance`, `rapidsai/cuvs`→`NVIDIA/cuvs`,
   `jbellis/jvector`→`datastax/jvector`). The ledger records canonical names.
3. **Discovery surfaced strong engines the curated list missed**:
   `HelixDB/helix-db` (Rust OLTP graph+vector on object storage, 5.6k),
   `cozodb/cozo` (Rust relational-graph-vector Datalog engine, 4.1k),
   `alibaba/zvec` (in-process vector DB, 14.7k), `tursodatabase/turso`
   (SQLite rewritten in Rust, 22.7k — a live case study of exactly the
   known-endpoint rewrite thesis of PRD06), plus teaching-grade storage
   repos (`skyzh/mini-lsm`, `erikgrinaker/toydb`).
4. **The topic-search noise ratio is high**: of 538 discovered repos, most
   are RAG frameworks, note-taking apps, and awesome-lists; only ~25 passed
   the engine gate. The curated-slate + API-verification approach is the
   right method; pure topic search is not.
5. **Language spread** supports the pattern study: the same patterns appear
   in Java (lucene, neo4j), C++ (rocksdb, kuzu, faiss), Rust (tantivy,
   qdrant, sled, redb, fjall), Go (badger, bleve, pebble, weaviate), and C
   (lmdb, sqlite, redis) — cross-language recurrence is exactly the ≥2-repo
   evidence rule the spec demands.
6. **Cross-category bridges to watch** (candidates for the first pattern
   docs): HNSW is a designed graph inside every vector engine; DiskANN is
   graph-on-disk under a RAM budget (the PRD05 thesis in production);
   FalkorDB runs graphs as GraphBLAS sparse matrices; paradedb embeds
   tantivy inside Postgres the way apache-age embeds a graph; almost every
   category-1/3/4 system bottoms out on a category-5 engine (rocksdb/lmdb
   descendants), which is why storage-engine reading pays five times over.

## Deltas applied to the spec's §5 slate

- Added: helix-db, cozo, zvec, turso, duckdb, valkey, dragonfly-adjacent
  KV rows, RediSearch, FASTER, mini-lsm, toydb, jvector, parlaylib (aspen's
  maintained home — `cmuparlay/aspen` 404s), ldbc v1 impls.
- Dropped: `google-research/google-research` (ScaNN lives in a monorepo —
  too heavy for a shallow-clone corpus; revisit only if quantization
  patterns need it), `marqo-ai/marqo` (orchestration layer, thin engine).
- Renames recorded per finding 2.

## Next decision point

The ledger is the Phase A deliverable (REQ-GLC-001/002). Owner approval of
the 117-repo corpus (REQ-GLC-003) gates Phase B cloning and mapping.
