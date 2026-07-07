# Supermeta Graph Database Patterns 2: Storage Engines, WAL, Layout, Recovery

Date: 2026-07-06

Agent: 2 of 5

Assigned lens: storage engines, WAL/checkpointing, memory layout, mmap/direct I/O, columnar formats, binary formats, indexing, allocators, compaction, snapshotting, page cache, and crash recovery.

Target use: encyclopedia-style reference material for a Neo4j-style Rust rewrite. This is research-only. It does not propose production code changes to this repository.

## Evidence Protocol

This slice prioritizes direct source evidence over secondhand summaries. CodeGraphContext was attempted for high-value Rust storage engines and direct `rg`/source reads were used for all core findings.

CodeGraphContext runs attempted:

- `fjall-src`: completed. Output directory: `/tmp/codex-code-intel/codegraphcontext/fjall-src-20260706-230850`. Smoke stats reported 178 files, 597 functions, 4 traits, 74 structs, 14 enums, 114 modules. Warning noted that indexed query output mentioned `gitrefrepo/`; direct source reads were used for important claims.
- `sled-src`: completed. Output directory: `/tmp/codex-code-intel/codegraphcontext/sled-src-20260706-230850`. Smoke stats reported 61 files, 436 functions, 4 classes, 1 trait, 67 structs, 10 enums, 101 modules. Direct source reads were used for important claims.
- `redb-src`: attempted. Process exited with code 1 and produced only partial artifacts (`index.txt`, `ladybugdb.sqlite`), so this slice does not rely on CGC output for redb.
- `tantivy-src`: attempted. Process exited with code 143 and produced only partial artifacts (`index.txt`, `ladybugdb.sqlite`, WAL), so this slice does not rely on CGC output for Tantivy.

Direct source repositories inspected:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sled-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rocksdb-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tikv-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-arrow-rs-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-parquet-format-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-datafusion-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/jemalloc-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/materialize-src`
- `/Users/amuldotexe/Desktop/oss-read-only/risingwave`

Light or noisy exploration only:

- Materialize and RisingWave were scanned for compaction/checkpoint/state-store patterns. Direct snippets are included only where specific files were read. Broad `rg` output was intentionally not used as primary evidence.
- Polars was lightly searched only during spill/disk exploration; no Polars-specific pattern is asserted in this file.
- OpenDAL was not used as a primary source in this slice.

## Pattern 1: Dual Durable Root Slots With a Tiny Selector Byte

Pattern name: dual commit slots plus selector byte.

Where found:

- redb, Rust embedded database.
- Repo path: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src`
- Primary files:
  - `src/transactions.rs` around 1360-1435
  - `src/tree_store/page_store/header.rs` around 154-395
  - `src/tree_store/page_store/page_manager.rs` around 780-870

Observed evidence:

- redb documents a one-phase commit in which it updates the inactive commit slot, flips a tiny selector byte ("god byte") to make that slot primary, then fsyncs.
- It also supports a two-phase form: update inactive slot, fsync, flip selector byte, fsync.
- Transaction slots include checksums. Recovery chooses the most recent valid slot, swaps primary/secondary if the selector says one thing but slot validity says another, and explicitly handles a crash during fsync where the selector byte is updated but the previously selected slot is the newest consistent state.
- `DatabaseHeader` stores primary-slot state, recovery-required state, two-phase flag, page-size/layout data, and transaction slots. `TransactionHeader::from_bytes` verifies a checksum over slot bytes before trusting a slot.
- `PageManager::commit` writes roots into the secondary slot, writes the header, optionally flushes for two-phase, swaps the primary slot, writes the header again, flushes, then finalizes shrink/unpersisted state.

Compressed pseudocode:

```text
inactive = 1 - header.primary_slot
write_transaction_slot(inactive, tx_id, user_root, system_root, checksum)
write_database_header()
if two_phase:
    flush_file()
header.primary_slot = inactive
write_database_header()
flush_file()
```

Why it matters:

The durable database "root" is usually much smaller than the full data set, but if that root is torn or published before the pages it names are durable, the whole database can become unreachable. Dual slots let recovery choose between two small root candidates instead of trusting one mutable header.

Why it matters for rewriting Neo4j in Rust:

A graph database has multiple roots: node store root, relationship store root, property store root, label index root, schema index root, transaction id, checkpoint id, free-list state, and high-id generators. A Rust rewrite should treat the checkpoint header as a small, checksum-protected bundle of roots with at least two durable generations. Do not publish "last closed transaction id" in a place that can outpace the actual page/image durability it certifies.

When to use:

- Use for embedded/single-file stores.
- Use for page stores and copy-on-write trees where the latest durable root can be represented in a compact header.
- Use where recovery must be deterministic without scanning a large WAL.

When not to use:

- Do not rely on this alone when arbitrary data pages are updated in place without WAL redo/undo.
- Do not assume the selector byte is authoritative unless each slot is independently validated.
- Do not use a tiny two-slot history as a substitute for backup retention or long-running reader snapshots.

Transferable version:

Represent the database superblock as `N >= 2` generations. Each generation contains `epoch`, `root_pointers`, `file_layout`, `tx_id`, `checksum`, and optional feature flags. Recovery scans all generations, rejects invalid checksums/layouts, and chooses the highest epoch whose referenced pages are valid under the durability protocol.

Rust translation:

- Model roots as immutable structs with explicit `epoch`.
- Encode/decode with fixed-endian formats and a checksum field.
- Make `HeaderSlot::validate()` return `Result<ValidatedHeaderSlot, HeaderCorruption>`, not a boolean.
- Keep selector-byte writes behind a `CommitPublisher` abstraction so tests can inject crashes between `write_slot`, `fsync`, `write_selector`, and final `fsync`.

Related patterns:

- Durable-before-visible publication.
- Copy-on-write page store.
- Manifest/CURRENT update ordering.
- Crash injection at every commit step.

Risks:

- Selector write can be torn.
- Slot checksum can validate while pointed pages are absent if root publication precedes data flush.
- Two slots can both be valid but refer to incompatible file-layout assumptions after shrink/truncate.

Performance implications:

- Two-phase costs an extra flush but narrows the ambiguity window.
- A tiny header is cache-friendly and recovery-fast.
- Commit latency becomes dominated by fsync discipline, not header encoding.

Memory implications:

- Header slots are tiny.
- Recovery may need temporary root/page validation structures if slot roots must be walked.

Concurrency implications:

- Writers need exclusive commit publication.
- Readers can hold an `Arc<ValidatedRoot>` while a later writer publishes a new root.

Testing implications:

- Test every crash point in the pseudocode.
- Corrupt one slot and ensure recovery chooses the other.
- Corrupt selector byte and ensure slot checksums/root validation dominate.
- Shrink/truncate after commit must be fuzzed.

Agentic code-generation guidance:

- Generate commit code as a finite-state machine with named phases: `write_inactive_slot`, `flush_inactive_slot`, `publish_selector`, `flush_selector`, `finalize_layout`.
- Require tests for every state transition before implementation.
- Never generate a helper named vaguely like `save_header`; use a name that encodes ordering, such as `write_inactive_header_slot`.
- Emit explicit comments only for crash-ordering invariants, not ordinary assignments.

## Pattern 2: Durable Before Visible, With an Optional Non-Durable Publication Layer

Pattern name: durable-before-visible root publication.

Where found:

- redb, Rust embedded database.
- RocksDB, C++ LSM storage engine.
- Tantivy, Rust search index.
- TiKV, Rust distributed KV using RocksDB/Raft.

Repo paths and files:

- redb: `src/tree_store/page_store/page_manager.rs` around 780-870
- RocksDB: `db/db_impl/db_impl_write.cc` around 1368-1508 and 2262-2325
- Tantivy: `src/indexer/index_writer.rs` around 590-670, `src/indexer/segment_updater.rs` around 38-55 and 383-460
- TiKV: `components/raftstore/src/store/fsm/apply.rs` around 780-820 and 1088-1102

Observed evidence:

- redb has `commit` and `non_durable_commit`. `non_durable_commit` updates an in-process secondary state and write barrier but leaves durable publication to a later durable commit.
- RocksDB writes a write batch to WAL, manages sequence ordering under `wal_write_mutex_`, then inserts into memtables. The write path treats WAL sync, sequence visibility, and memtable insertion as separately ordered concerns.
- Tantivy documents that `prepare_commit()` cuts the indexing queue, waits for worker threads, flushes segment work to disk, and then `commit()` publishes through `meta.json`. Documents are visible to readers only after commit.
- TiKV comments warn that if apply state is written to a different WAL from KV data, a power failure can make metadata appear ahead of data. It stores Raft apply state in KV RocksDB in one write batch with the KV data to avoid losing data after restart.

Compressed pseudocode:

```text
durable_payload = write_data_pages_or_segments()
fsync_payload_if_required()
durable_manifest = write_root_or_meta(durable_payload)
fsync_manifest_or_directory()
publish_reader_view(durable_manifest)
```

Why it matters:

The fundamental crash-safety bug is "visible before durable" or "metadata durable before data it certifies." The source repositories repeatedly encode the same invariant: a reader-visible root, manifest, metadata file, or applied-index marker must not advance beyond the data already recoverable after a crash.

Why it matters for rewriting Neo4j in Rust:

Neo4j-style stores have high-value metadata: counts store, schema store, label scan store, relationship group store, dense-node thresholds, token ids, and last committed transaction id. A Rust rewrite must never allow these to become visible or durable in a different epoch than the pages/log records they summarize unless recovery can repair them deterministically.

When to use:

- Always use for primary storage, indexes, and catalog roots.
- Use a non-durable fast path only if the API explicitly labels it as non-durable and recovery never depends on it.
- Use where in-memory visibility can legitimately precede power-loss durability, such as batched transactions with `commit=async`.

When not to use:

- Do not offer non-durable commit behind an API named simply `commit`.
- Do not persist "applied" metadata in a different WAL domain from the data unless cross-WAL sync ordering is formally tested.

Transferable version:

For every state transition, classify it as `prepared`, `durable`, or `visible`. Only `durable` states can be used during recovery. Only `visible` states can be read by normal transactions. The transition from `durable` to `visible` must be monotonic and atomic from the reader's perspective.

Rust translation:

- Use typestates: `PreparedBatch`, `DurableBatch`, `PublishedBatch`.
- Only expose `PublishedBatch` to reader snapshots.
- Make `mark_applied` take a `DurableDataBatch` token so metadata cannot advance without the data batch.
- For async durability, return an explicit `CommitReceipt { visibility, durability }`.

Related patterns:

- RocksDB WAL-before-memtable.
- Tantivy atomic `meta.json`.
- redb dual slots.
- TiKV same-WAL apply-state.

Risks:

- Async flush APIs can obscure actual durability.
- Filesystem rename without parent directory fsync can be visible but not durable.
- Multiple WALs can independently reorder across power loss.

Performance implications:

- Strict fsync per transaction is expensive.
- Group commit and sequence-number assignment can preserve ordering while amortizing flushes.

Memory implications:

- Prepared-but-not-durable batches require memory accounting.
- Non-durable publication needs bounded unpersisted state.

Concurrency implications:

- Readers need snapshot pinning against published epochs.
- Writers need ordering locks or sequencers around WAL publication.

Testing implications:

- Crash after data write but before metadata.
- Crash after metadata write but before metadata fsync.
- Crash after in-memory publication but before durable root.
- Cross-WAL tests for apply markers and data.

Agentic code-generation guidance:

- Insist that generated commit APIs name their durability semantics.
- Ask the model to produce an ordering table before writing code.
- Reject generated code that updates `last_tx_id`, `apply_index`, or `checkpoint_id` before the corresponding data write is recoverable.

## Pattern 3: WAL Batch Bracketing With Item Counts, Checksums, and Magic End Markers

Pattern name: framed WAL batches.

Where found:

- fjall, Rust LSM-style key-value store.
- DuckDB, C++ analytical database.
- sled, Rust embedded database.
- Apache Iggy, Rust streaming log/prepare journal.

Repo paths and files:

- fjall:
  - `src/journal/entry.rs` around 1-70
  - `src/journal/batch_reader.rs` around 100-135
  - `src/journal/writer.rs` around 27-235
- DuckDB:
  - `src/include/duckdb/storage/write_ahead_log.hpp` around 1-135
  - `src/storage/write_ahead_log.cpp` around 1-170
  - `src/storage/wal_replay.cpp` around 1-150
- sled:
  - `src/metadata_store.rs` around 436-548
- Iggy:
  - `core/journal/src/prepare_journal.rs` around 96-190 and 630-682

Observed evidence:

- fjall represents a journal batch as `Start`, `Item*`, `End(checksum)`. The start includes item count and sequence number. The end terminates with a magic string and checksum. The batch reader validates that an `End` appears while in a batch, that the item counter reaches zero, and that the computed checksum matches the expected checksum.
- DuckDB writes WAL entries through `ChecksumWriter::Flush`: entry payload is serialized, optionally encrypted, checksumed, then framed with size/checksum/data. Replay reads size/checksum, bounds-checks against file size, reads the buffer, validates checksum, and rejects corrupt entries.
- sled uses a frame with a checksum over the length and a CRC over the payload. It clears checksum bits before interpreting frame length and rejects bad length CRC or payload CRC.
- Iggy prepare journal writes one `Message<PrepareHeader>` per append, fsyncs after append, and depends on a bounded `MAX_ENTRY_SIZE` invariant for safe tail truncation.

Compressed pseudocode:

```text
write(Start { item_count, seqno })
for item in items:
    update_checksum(item)
    write(Item(item))
write(End { checksum, magic })
flush_or_sync(mode)

recovery:
    read Start
    read exactly item_count Items
    read End
    verify checksum and magic
    apply batch atomically
```

Why it matters:

Recovery needs to distinguish a complete committed batch from a torn partial batch. A batch without a clear terminal record and checksum can be misinterpreted as valid operations, especially when trailing bytes happen to decode.

Why it matters for rewriting Neo4j in Rust:

Graph transactions often mutate several stores together: node records, relationship records, property chains, counts, schema indexes, free lists, and label indexes. WAL framing must preserve transaction atomicity across these stores. A `StartTx`, `Record*`, `CommitTx(checksum)` frame is easier to recover and test than a stream of unscoped record mutations.

When to use:

- Use for transaction logs, metadata logs, prepare journals, and segment append logs.
- Use item counts when the writer knows the batch size.
- Use terminal magic/version fields to reject accidental decode.

When not to use:

- Avoid excessive per-record sync if group commit is acceptable.
- Do not use only CRC without length sanity checks; a corrupted length can send recovery into the wrong byte range.
- Do not silently truncate mid-file corruption as if it were a torn tail.

Transferable version:

Define a WAL grammar:

```text
WalFile = Header Segment*
Segment = Batch*
Batch = Start(seq, count, schema_version) Record{count} Commit(checksum, magic)
```

Recovery accepts only complete `Batch` values. Partial final batch is truncated or ignored. Mid-file corruption is an error unless there is a documented salvage mode.

Rust translation:

- Encode with a small binary format: fixed header, varint payload length, payload, checksum.
- Use `crc32c` or `xxh3` for speed; consider stronger checksum for archival/object storage.
- Make `WalReader` yield `Result<CompleteBatch, WalCorruption>`, never raw records.
- Maintain a `RecoveryMode` enum: strict, tolerate_tail, point_in_time.

Related patterns:

- Checksummed binary frames.
- Torn-tail repair.
- Recovery modes.
- Group commit.

Risks:

- Checksum collision is possible, though low.
- Compression/encryption changes what bytes are checksumed; be explicit.
- Batch count can disagree with decoded items; recovery must reject.

Performance implications:

- Per-batch checksum is cheaper than per-record checksum but detects less precisely.
- Grouping records improves fsync amortization.
- Terminal magic improves corruption rejection with negligible cost.

Memory implications:

- Very large batches require streaming checksum computation.
- Recovery should avoid buffering the whole WAL unless batches are bounded.

Concurrency implications:

- Multiple writers need a WAL serializer or sharded logs with a merge protocol.
- Sequence numbers must be monotonic and assigned in log order.

Testing implications:

- Truncate after every byte in a batch.
- Flip bytes in length, item payload, checksum, and magic.
- Generate nested `Start` inside a batch and ensure recovery rejects or restarts according to spec.

Agentic code-generation guidance:

- Require the model to write the WAL grammar first.
- Generate decoders as state machines, not ad hoc loops.
- Ask for corruption tests before encoder implementation.

## Pattern 4: Fsync Discipline as an Explicit Policy Enum

Pattern name: configurable persist modes.

Where found:

- fjall, Rust LSM storage engine.
- RocksDB, C++ LSM storage engine.
- DuckDB, C++ analytical database.
- ClickHouse, C++ column store.
- Apache Iggy, Rust append/prepare journal.

Repo paths and files:

- fjall: `src/journal/writer.rs` around 27-235
- RocksDB: `db/db_impl/db_impl_write.cc` around 1368-1420
- DuckDB: `src/storage/write_ahead_log.cpp` around 1-170
- ClickHouse: `src/Common/ProfileEvents.cpp` around 59-80
- Iggy: `core/journal/src/file_storage.rs` around 55-105 and `core/journal/src/prepare_journal.rs` around 630-682

Observed evidence:

- fjall exposes `PersistMode::{Buffer, SyncData, SyncAll}`. `Buffer` flushes buffered writer data to OS buffers but is not power-loss durable. `SyncData` maps to data sync. `SyncAll` maps to full sync. Journal rotation fsyncs the directory.
- RocksDB separates WAL write, WAL sync, and memtable application. It tracks whether log files need sync and records sync ordering while handling grouped writers.
- DuckDB `Flush` writes a checksummed WAL frame and the WAL has explicit truncate and size tracking.
- ClickHouse has metrics for file and directory sync count and elapsed microseconds.
- Iggy `FileStorage::fsync` uses `sync_data`, and its prepare journal append writes one entry then fsyncs.

Compressed pseudocode:

```text
enum PersistMode {
    BufferOnly,
    DataSync,
    FullSync,
}

persist(mode):
    flush_user_space_buffers()
    match mode:
        BufferOnly => return
        DataSync => fdatasync(file)
        FullSync => fsync(file)
```

Why it matters:

Many systems accidentally hide durability behind `flush()`, which often means "flush userspace buffer to kernel" rather than "survive power loss." fjall makes the distinction explicit in the type system and documentation.

Why it matters for rewriting Neo4j in Rust:

Neo4j deployments vary: some want maximum throughput with OS-buffered commits; others require transaction durability at commit return. A Rust rewrite should expose durability policy as an explicit mode and make transaction receipts state what was guaranteed.

When to use:

- Use for WAL and checkpoint writers.
- Use for admin-configurable transaction durability.
- Use in benchmarks to compare OS-buffered, fdatasync, and fsync behavior.

When not to use:

- Do not let lower-level storage silently choose weaker durability than the public API promises.
- Do not expose too many durability modes before tests cover each mode.

Transferable version:

Define a `DurabilityPolicy` for each write path: transaction WAL, checkpoint root, segment file, index file, metadata file, directory rename. Require every write path to state whether it calls no sync, data sync, full file sync, or directory sync.

Rust translation:

- `enum DurabilityPolicy { Buffered, DataSync, FullSync }`
- `enum DirectorySyncPolicy { Required, SkippedForEphemeral, Unsupported }`
- Return `CommitDurability::PowerLossDurable` only after the required file and directory syncs have succeeded.

Related patterns:

- Manifest/CURRENT update order.
- Directory fsync after rename.
- Group commit.
- Metrics for sync latency.

Risks:

- `sync_data` may not persist metadata needed for new files or growth on all filesystems.
- Directory fsync may fail or be unsupported; decide whether to fail closed.
- Cloud/object stores have different durability semantics from POSIX.

Performance implications:

- Sync latency can dominate commit time.
- Sync batching and group commit are mandatory for throughput.
- Metrics must separate file sync and directory sync.

Memory implications:

- Buffered mode increases dirty data exposure.
- More aggressive group commit requires bounded pending batches.

Concurrency implications:

- A sync thread can serialize flush work while writers enqueue.
- Group commit needs sequence ranges and wakeups.

Testing implications:

- Assert that `Buffered` mode does not advertise power-loss durability.
- Inject sync errors and ensure commit returns failure or degraded receipt.
- Simulate crash after file write but before directory sync.

Agentic code-generation guidance:

- Tell code generators that "flush" is ambiguous and must not be used in public durability names.
- Require generated docs for every durability mode.
- Ask for metrics counters for every sync path.

## Pattern 5: Directory Fsync and Atomic Rename Are Part of the Commit Protocol

Pattern name: rename plus parent-directory fsync.

Where found:

- fjall journal rotation.
- sled metadata snapshot.
- Tantivy `meta.json` publication.
- Apache Iggy prepare-journal drain.
- RocksDB manifest/CURRENT installation.

Repo paths and files:

- fjall: `src/journal/writer.rs` around 100-235
- sled: `src/metadata_store.rs` around 753-835
- Tantivy: `src/indexer/segment_updater.rs` around 38-55
- Iggy: `core/journal/src/prepare_journal.rs` around 540-605
- RocksDB: `db/version_set.cc` around 6220-6462

Observed evidence:

- fjall creates new journal files, preallocates, syncs file data, and fsyncs the directory after rotation.
- sled writes a new snapshot to a temp path, writes all bytes, syncs it, renames it into place, then syncs the locked directory.
- Tantivy `save_metas` calls `directory.sync_directory()` and then `directory.atomic_write` for `meta.json`; the comment says `meta.json` remains untouched on failure or is written and flushed on success.
- Iggy drain writes live WAL entries to temp, `sync_all`s the temp, renames, fsyncs the parent directory, reopens storage, then advances the snapshot watermark only after the durable rewrite sequence.
- RocksDB writes manifest records, syncs manifest, then installs `CURRENT`. Failure handling distinguishes whether `CURRENT` may point to old or new manifest.

Compressed pseudocode:

```text
write(tmp, new_metadata)
fsync(tmp)
rename(tmp, final)
fsync(parent_directory)
reopen_or_publish(final)
```

Why it matters:

On POSIX filesystems, rename atomicity is not the same as power-loss durability. Without parent directory fsync, the new name can disappear after a crash. This is especially dangerous for metadata roots.

Why it matters for rewriting Neo4j in Rust:

A graph store will likely have catalog files, schema index manifests, checkpoint files, token stores, and possibly sidecar columnar/statistics files. Any "replace by temp+rename" must include parent directory fsync before it is considered durable.

When to use:

- Use for metadata files, manifests, snapshots, segment catalogs, and compacted WAL replacement.
- Use when file contents are immutable after write.
- Use when readers discover state through filenames.

When not to use:

- Do not use rename as the only durability primitive.
- Do not use temp+rename for very large hot data files when an append log would suffice.
- Do not ignore parent directory fsync errors unless the store is explicitly ephemeral.

Transferable version:

Create a `DurableFileReplace` primitive that handles temp creation, write, fsync temp, atomic rename, fsync parent, and optional reopen. Make callers unable to skip the parent sync accidentally.

Rust translation:

- Use `tempfile` or a same-directory temp path.
- Use `File::sync_all` on temp for metadata files.
- After `std::fs::rename`, open parent directory and call sync where supported.
- Return a structured error stage: `WriteTemp`, `SyncTemp`, `Rename`, `SyncParent`, `Reopen`.

Related patterns:

- Durable manifest/CURRENT ordering.
- Poison-on-uncertain-rewrite.
- Snapshot compaction.

Risks:

- Cross-device rename is not atomic.
- Some platforms make directory fsync awkward or unsupported.
- Antivirus/backup agents can interfere with rename timing.

Performance implications:

- Parent directory fsync is nontrivial; batch metadata changes.
- Avoid renaming per small transaction.

Memory implications:

- Temp replacement may require holding serialized metadata in memory.
- Streaming temp writes avoid large buffers.

Concurrency implications:

- Readers should either use atomic open-after-rename or versioned filenames.
- Writers need a metadata publication lock.

Testing implications:

- Inject failures at each replace phase.
- Verify stale temp cleanup.
- Verify that after simulated crash recovery chooses either old or new metadata, not a half-state.

Agentic code-generation guidance:

- Never let generated code call `rename` directly in storage code; route through a durable replace helper.
- Ask the model to name the filesystem guarantees for every publication operation.
- Generate tests for failed parent fsync and failed reopen.

## Pattern 6: Manifest or Catalog Update Order

Pattern name: manifest log then pointer file.

Where found:

- RocksDB manifest/CURRENT.
- DuckDB checkpoint marker plus checkpoint WAL.
- Tantivy `meta.json`.
- redb header slots.

Repo paths and files:

- RocksDB: `db/version_set.cc` around 6220-6462
- DuckDB: `src/storage/checkpoint_manager.cpp` around 193-380 and `src/storage/storage_manager.cpp` around 233-315
- Tantivy: `src/indexer/segment_updater.rs` around 38-55 and 383-460
- redb: `src/tree_store/page_store/page_manager.rs` around 780-870

Observed evidence:

- RocksDB encodes version edits, adds them to descriptor log, syncs manifest, and only then updates `CURRENT` to install a new manifest. If manifest operations fail, failure handling keeps or deletes the new manifest depending on whether `CURRENT` could point at it.
- DuckDB writes a checkpoint marker to WAL before writing checkpoint metadata. Recovery can compare the root metadata block against the WAL checkpoint entry. During checkpoint, new commits go to `.checkpoint.wal`, which is moved over the main WAL after checkpoint completion.
- Tantivy publishes searchable segments by serializing an `IndexMeta` containing segment metas, schema, opstamp, and payload into `meta.json`.
- redb publishes root slots after data/header preparation.

Compressed pseudocode:

```text
write_manifest_records(new_files, deleted_files, roots)
fsync(manifest)
install_manifest_pointer(CURRENT or header selector)
fsync(pointer_or_directory)
publish_in_memory_view()
```

Why it matters:

The manifest is the database's table of contents. If it points to files that do not exist, or omits files that are the only copy of data, recovery can leak space or lose data. Catalog update order is the difference between deterministic recovery and guesswork.

Why it matters for rewriting Neo4j in Rust:

A graph store may have multiple physical files per logical store: record pages, relationship adjacency segments, property blob segments, full-text index segments, vector index files, columnar stats, and token catalogs. The rewrite should have a single manifest epoch that describes all files belonging to a checkpoint.

When to use:

- Use any time a checkpoint introduces new immutable files and retires old files.
- Use for LSM/SST manifests, index segment lists, and catalog roots.

When not to use:

- Do not use only directory scanning as the source of truth for committed state unless every filename encodes a validated epoch.
- Do not delete old files before the manifest that removes them is durable and no snapshots reference them.

Transferable version:

Maintain an append-only manifest log of file additions/deletions and periodic manifest snapshots. Install a small pointer (`CURRENT`, header slot, or root superblock) only after the new manifest state is synced.

Rust translation:

- `ManifestEdit { add_files, delete_files, root_ids, epoch }`
- `ManifestWriter::append_and_sync(edit)`
- `CurrentPointer::install(epoch, manifest_file_id)`
- Garbage collection only reads from `PublishedManifest`.

Related patterns:

- Immutable segment files.
- Snapshot retention.
- Garbage collection after publication.

Risks:

- Manifest grows without snapshots.
- Pointer update can be torn.
- Deleting files after manifest publication can race readers.

Performance implications:

- Manifest sync can be batched.
- Large manifests need preallocation or segmented manifests. RocksDB exposes manifest preallocation size in options.

Memory implications:

- Holding all file metadata in memory can be expensive for many tiny segments.
- Use compact per-file descriptors.

Concurrency implications:

- Readers should pin manifest epochs.
- Compaction writes a new manifest edit but does not mutate the old manifest epoch in place.

Testing implications:

- Crash after new files, before manifest.
- Crash after manifest sync, before pointer.
- Crash after pointer, before old file deletion.
- Restart with both old and new manifests present.

Agentic code-generation guidance:

- Ask for a manifest state machine and file lifecycle table.
- For generated compaction code, reject any deletion path that does not consult reader-pinned epochs.
- Generate manifest recovery tests before file garbage collection.

## Pattern 7: Snapshot Plus WAL Compaction Into a Fresh Synced Snapshot

Pattern name: snapshot-and-log consolidation.

Where found:

- sled metadata store.
- DuckDB checkpoint manager.
- Iggy prepare-journal drain.
- redb integrity repair/compaction.

Repo paths and files:

- sled:
  - `src/metadata_store.rs` around 678-835
  - `tests/00_regression.rs` around 149-180 and 242-275
- DuckDB:
  - `src/storage/checkpoint_manager.cpp` around 193-380
  - `src/storage/storage_manager.cpp` around 233-315
- Iggy:
  - `core/journal/src/prepare_journal.rs` around 540-605
- redb:
  - `src/db.rs` around 572-635

Observed evidence:

- sled enumerates logs and snapshots, removes incomplete temp files, keeps the newest snapshot, removes stale logs at or before the snapshot id, reads snapshot and logs, materializes a map, writes a fresh snapshot to a temp path, syncs it, renames it, and syncs the directory.
- DuckDB creates a checkpoint under a checkpoint transaction, writes checkpoint markers to WAL, serializes catalog entries, flushes metadata writers, writes the database header, verifies blocks optionally, truncates, then finalizes WAL checkpoint handling.
- Iggy drains live prepare-journal entries by rewriting to a temp WAL and only advances the snapshot watermark after the temp write, sync, rename, parent fsync, and reopen sequence.
- redb `check_integrity` can clear cache, reload, repair roots, and commit repaired roots with two-phase durability.

Compressed pseudocode:

```text
state = read_latest_snapshot()
for log in logs_after_snapshot:
    state.apply(log)
write_temp_snapshot(state)
fsync(temp)
rename(temp, snapshot_epoch)
fsync(parent)
delete_or_ignore_logs_covered_by_snapshot_after_publication()
```

Why it matters:

Append-only logs need compaction. Without snapshots, recovery time grows forever and stale metadata becomes a reliability risk. But snapshotting can itself lose data if the snapshot watermark advances before the compacted representation is durable.

Why it matters for rewriting Neo4j in Rust:

Graph workloads can produce huge transactional logs. Recovery must not replay an unbounded history. A Rust rewrite should checkpoint graph roots plus store free-lists plus index roots, then retain only WAL records after the checkpoint epoch. The checkpoint must include enough data to rebuild counts/index consistency or mark those side structures as rebuildable.

When to use:

- Use for metadata stores, transaction logs, schema/catalog state, and streaming prepare journals.
- Use when replay time exceeds startup SLO.
- Use before compacting a fixed-size in-memory WAL index that would otherwise wrap.

When not to use:

- Do not snapshot without a precise `covered_through_tx_id`.
- Do not delete logs solely because a snapshot file exists; validate snapshot checksum and epoch.
- Do not advance watermarks before replacement is durable.

Transferable version:

Create `Checkpoint { epoch, root_set, covered_wal_lsn, checksum }`. Recovery chooses the highest valid checkpoint and replays WAL records strictly after `covered_wal_lsn`.

Rust translation:

- Store checkpoint metadata in the dual-slot superblock or manifest.
- Use a `CheckpointBuilder` with `begin_snapshot`, `write_state`, `sync_state`, `publish_checkpoint`.
- Make `covered_lsn` immutable after publication.

Related patterns:

- Durable file replacement.
- Dual root slots.
- Recovery modes.
- Poison-on-uncertain rewrite.

Risks:

- Snapshot can be internally inconsistent if captured without a read transaction.
- Long-running readers can pin old logs and prevent deletion.
- Background snapshot can race foreground writes.

Performance implications:

- Checkpointing trades periodic IO bursts for fast recovery.
- Incremental checkpoints reduce write amplification but complicate recovery.

Memory implications:

- Full materialization snapshot can be memory-heavy.
- Streaming snapshot writers need stable iterators over pinned state.

Concurrency implications:

- Online checkpoint needs a consistent read timestamp.
- New commits may need a separate WAL while checkpoint runs, as DuckDB does.

Testing implications:

- Crash during temp snapshot write.
- Crash after temp sync before rename.
- Crash after rename before parent sync.
- Crash after checkpoint marker before checkpoint root.
- Verify old logs are retained until snapshot is valid.

Agentic code-generation guidance:

- Generate checkpoint code around `covered_lsn` and `snapshot_epoch`.
- Require recovery tests that bound replay length.
- Do not let generated code delete WAL files in the same function that writes the snapshot unless publication state is explicit.

## Pattern 8: Online Checkpoint With a Checkpoint WAL for Concurrent Commits

Pattern name: split WAL during checkpoint.

Where found:

- DuckDB, C++ analytical database.

Repo path and files:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src`
- `src/storage/storage_manager.cpp` around 233-315
- `src/storage/checkpoint_manager.cpp` around 193-380
- `src/storage/wal_replay.cpp` around 1-150

Observed evidence:

- `WALStartCheckpoint` uses lock ordering: WAL lock before transaction lock.
- It starts a checkpoint transaction whose start time defines checkpoint visibility.
- It writes a checkpoint marker to the main WAL and flushes.
- It closes the main WAL and replaces the active WAL with `.checkpoint.wal` for concurrent transactions.
- `WALFinishCheckpoint` removes the main WAL if no checkpoint WAL was written, or moves checkpoint WAL over the main WAL path and reopens it.
- Recovery uses WAL checkpoint state and expected checkpoint markers to know whether to replay main WAL, checkpoint WAL, or both.

Compressed pseudocode:

```text
lock(wal)
lock(transactions)
checkpoint_tx = begin_read_only_checkpoint_tx()
write_checkpoint_marker(main_wal)
flush(main_wal)
close(main_wal)
active_wal = checkpoint_wal
unlock(transactions, wal)

write_checkpoint_from(checkpoint_tx)
publish_checkpoint_root()

lock(wal)
if checkpoint_wal_empty:
    remove(main_wal)
else:
    rename(checkpoint_wal, main_wal)
reopen(active_wal)
unlock(wal)
```

Why it matters:

Online checkpoints must not block all writes for the whole checkpoint duration. DuckDB separates the stable snapshot being checkpointed from new commits by redirecting new commits to a checkpoint WAL that belongs to the next recovery epoch.

Why it matters for rewriting Neo4j in Rust:

Neo4j-style deployments need online checkpoints under write load. A Rust rewrite can checkpoint a consistent graph snapshot while new transactions continue into a new WAL segment. Recovery then applies the correct WAL tail after the checkpoint.

When to use:

- Use when checkpoints are expensive and write downtime is unacceptable.
- Use when a read transaction can define a stable snapshot.
- Use for stores that can serialize checkpoint roots independent of newer writes.

When not to use:

- Avoid if your store cannot distinguish snapshot-visible data from newer data.
- Avoid until WAL recovery state machine is well-tested.
- Avoid if single-writer embedded use can tolerate short checkpoint pauses.

Transferable version:

At checkpoint start, seal WAL segment A and open WAL segment B. Checkpoint root covers state through segment A marker. During recovery, if root publish completed, replay B; otherwise replay A and possibly B depending on marker state.

Rust translation:

- `CheckpointPhase::{Idle, MarkerWritten, WritingRoot, RootPublished, WalSwapped}`
- Keep a `WalRouter` that atomically seals/open segments.
- Use reader snapshots for checkpoint iteration.

Related patterns:

- Snapshot plus WAL compaction.
- Durable-before-visible.
- Manifest update order.

Risks:

- Lock ordering deadlocks.
- Checkpoint WAL rename can be lost without directory sync.
- Recovery logic becomes multi-phase and must handle all partial states.

Performance implications:

- Reduces writer stalls.
- Adds WAL file churn and recovery complexity.

Memory implications:

- Checkpoint transaction pins old pages/versions.
- New writes accumulate in separate WAL and maybe dirty buffers.

Concurrency implications:

- Requires precise transaction timestamping.
- Requires checkpoint transaction isolation from writers.

Testing implications:

- Crash after marker, before checkpoint root.
- Crash after root, before WAL swap.
- Crash while checkpoint WAL has concurrent commits.
- Verify lock ordering with concurrency tests.

Agentic code-generation guidance:

- Ask the model to enumerate recovery states before writing checkpoint code.
- Generate a table mapping on-disk files to recovery action for every phase.
- Forbid hidden global locks that span the full checkpoint write unless explicitly chosen.

## Pattern 9: MVCC Snapshot Tracking as a Safe-to-GC Watermark

Pattern name: snapshot reference counting plus garbage-collection watermark.

Where found:

- fjall, Rust LSM storage engine.
- redb, Rust embedded copy-on-write B-tree database.
- RisingWave, Rust distributed streaming database, for epoch/TTL compaction hints.

Repo paths and files:

- fjall:
  - `src/snapshot_tracker.rs` around 1-145
  - `src/compaction/worker.rs` around 1-65
- redb:
  - `src/db.rs` around 572-635
  - `src/tree_store/page_store/page_manager.rs` around 780-870
- RisingWave:
  - `/Users/amuldotexe/Desktop/oss-read-only/risingwave/src/meta/src/hummock/compaction/picker/ttl_reclaim_compaction_picker.rs` around 72-118

Observed evidence:

- fjall `SnapshotTrackerInner` tracks current sequence number, a GC lock, a map of active snapshot sequence numbers to counts, a freed count, and a lowest freed instant. Opening a snapshot increments the count. Publishing a batch advances the global sequence. `get_seqno_safe_to_gc` exposes a sequence safe for compaction. Compaction uses this safe-to-GC sequence.
- redb prevents compaction when live read transactions or savepoints exist; it drains pending free pages and uses a two-phase commit during compaction.
- RisingWave TTL reclaim compaction compares SST min epochs with table retention settings and current physical time to decide whether an SST can be reclaimed.

Compressed pseudocode:

```text
open_snapshot():
    seq = current_seq
    active_snapshots[seq] += 1
    return SnapshotGuard(seq)

publish_batch(batch_seq):
    current_seq = max(current_seq, batch_seq + 1)

safe_to_gc():
    return min(active_snapshots.keys()) - 1
```

Why it matters:

Compaction wants to delete or rewrite old versions. Readers want stable snapshots. Without a safe-to-GC watermark, compaction can delete data still visible to a reader.

Why it matters for rewriting Neo4j in Rust:

Graph traversals can be long-running. A Cypher query can hold a snapshot while writes continue. Dense-node relationship chains, property records, and indexes must not discard old versions until no active transaction can observe them. Rust ownership can help model snapshot guards and prevent accidental early release.

When to use:

- Use for MVCC stores, LSM stores, copy-on-write page stores, and index segment GC.
- Use whenever readers can outlive writes.

When not to use:

- Do not use only wall-clock retention for transactional visibility.
- Do not let compaction consult an approximate reader count for correctness.
- Do not allow unbounded snapshot pinning without observability and cancellation policy.

Transferable version:

Maintain a global committed epoch and a registry of reader-pinned epochs. Compaction can reclaim versions strictly older than the minimum pinned epoch and older than any savepoint/backup epoch.

Rust translation:

- `SnapshotGuard` holds an `Arc<SnapshotTracker>` and decrements on drop.
- `GcWatermark` is a newtype, not a raw integer.
- Compaction APIs take `GcWatermark`, not `current_seq`.

Related patterns:

- LSM compaction.
- Immutable segment file GC.
- Online checkpoint.
- Copy-on-write pages.

Risks:

- Leaked snapshot guards prevent GC forever.
- Savepoints/backups need the same pinning mechanism.
- Distributed systems need consensus on watermarks.

Performance implications:

- Long snapshots increase space amplification.
- Tracking snapshots in a concurrent map adds overhead but is usually small.

Memory implications:

- Old pages/segments remain pinned.
- Metrics should expose oldest snapshot age and pinned bytes.

Concurrency implications:

- Snapshot open/close must be race-safe with publication.
- GC lock may be needed to avoid watermark movement while compaction selects files.

Testing implications:

- Open snapshot, write/delete, compact, assert snapshot still reads old data.
- Leak snapshot in test and ensure compaction refuses unsafe deletion.
- Fuzz snapshot open/close around publication.

Agentic code-generation guidance:

- Require compaction functions to take `GcWatermark`.
- Generate tests where readers intentionally outlive compaction.
- Reject generated code that compares raw sequence numbers from unrelated domains.

## Pattern 10: LSM Compaction Policy Must Bound Space, Memory, and File Count

Pattern name: compaction as resource governance.

Where found:

- fjall compaction worker.
- RocksDB and Materialize RocksDB configuration.
- RisingWave Hummock compaction pickers.
- ClickHouse MergeTree data parts.

Repo paths and files:

- fjall: `src/compaction/worker.rs` around 1-65
- Materialize: `src/rocksdb-types/src/config.rs` around 52-115
- RisingWave: `/Users/amuldotexe/Desktop/oss-read-only/risingwave/src/meta/src/hummock/compaction/picker/tier_compaction_picker.rs` around 60-125
- ClickHouse: `src/Storages/StorageMergeTree.cpp` around 2928-2945

Observed evidence:

- fjall's compaction worker checks deletion state, clones a compaction strategy, increments active compaction count, calls tree compaction with the safe-to-GC sequence, records timing/counters, and sleeps briefly.
- Materialize documents RocksDB level versus universal compaction tradeoffs. Universal is simpler and can be better for some workloads, but may use significantly more temporary space. The default is level compaction. It also discusses dynamic level bytes to bound space amplification and a memtable budget around 512MB for heavy-write workloads.
- RisingWave tier compaction limits compaction bytes by `max_compaction_bytes` and sublevel settings, and limits SST file count to avoid too much memory.
- ClickHouse checks data parts and writes `checksums.txt` if absent after verifying calculated checksums equal part checksums.

Compressed pseudocode:

```text
candidate = pick_files(levels, safe_to_gc)
if candidate.bytes > max_compaction_bytes: stop
if candidate.file_count > max_file_count: stop
if candidate.overlaps_pending_compaction: skip
rewrite(candidate)
publish_manifest_edit(add=new_files, delete=old_files)
```

Why it matters:

Compaction is not just cleanup. It determines read amplification, write amplification, space amplification, memory pressure, and tail latency.

Why it matters for rewriting Neo4j in Rust:

Graph stores have adjacency-heavy access patterns and high update locality around hot nodes. A Rust rewrite may use append-only adjacency/property segments plus compaction. Compaction policies must avoid rewriting massive hot adjacency lists too often while still reclaiming tombstoned relationships/properties.

When to use:

- Use LSM/segment compaction for append-heavy stores and secondary indexes.
- Use bounded compaction candidates to avoid memory spikes.
- Use level compaction when read amplification and space amplification matter.

When not to use:

- Do not use generic LSM compaction blindly for fixed-width primary records where a page store is simpler.
- Do not compact files still visible to snapshots.
- Do not run unbounded compaction inside foreground transaction paths.

Transferable version:

Separate primary graph record storage from side-index compaction. Use append-only segments for variable-sized relationships/properties and compact them with safe-to-GC watermarks, byte budgets, and file-count budgets.

Rust translation:

- `CompactionCandidate { files, estimated_bytes, estimated_memory, min_epoch, max_epoch }`
- `CompactionBudget { max_bytes, max_files, max_memory, max_duration }`
- `CompactionPlanner::pick(candidate, watermark, budget)`

Related patterns:

- MVCC safe-to-GC.
- Immutable segments.
- Manifest update order.
- Checksums per data part.

Risks:

- Write amplification can dominate.
- Universal compaction can temporarily require large extra space.
- Compaction can starve foreground IO.

Performance implications:

- Tune for graph traversal read patterns, not only point lookups.
- Track read amplification per store: adjacency, properties, labels, indexes.

Memory implications:

- Merging many files requires buffers and bloom/index structures.
- Cap file count and compaction bytes.

Concurrency implications:

- Use background workers with cancellation and admission control.
- Prevent overlapping compactions on same files.

Testing implications:

- Validate old and new files during manifest swap.
- Fuzz compaction under active snapshots.
- Test compaction interruption and restart.

Agentic code-generation guidance:

- Ask code generators to write compaction selection as pure functions with deterministic tests.
- Require budgets in every compaction API.
- Do not let generated compaction delete files directly; deletion is a separate GC phase.

## Pattern 11: Copy-on-Write B-Tree/Page Store for Stable Readers

Pattern name: COW page root publication.

Where found:

- redb, Rust embedded database.

Repo paths and files:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src`
- `src/tree_store/page_store/header.rs` around 154-395
- `src/tree_store/page_store/page_manager.rs` around 780-870
- `src/db.rs` around 572-635

Observed evidence:

- redb uses page headers/root slots and copy-on-write style transaction publication.
- Commit writes roots into the inactive transaction slot before making the slot primary.
- Compaction is blocked when live reads/savepoints exist and uses two-phase commit.
- Integrity checking can reload state, repair roots, and commit repaired roots.

Compressed pseudocode:

```text
write_tx:
    new_pages = copy_and_mutate(old_pages)
    new_root = root_pointing_to(new_pages)
    publish_root_slot(new_root)

read_tx:
    root = pin_current_root()
    read_pages_reachable_from(root)
```

Why it matters:

COW page stores are simple to reason about: readers use old roots, writers build new pages, commit publishes a new root. They avoid in-place update undo logging at the cost of write amplification and free-list management.

Why it matters for rewriting Neo4j in Rust:

Neo4j traditionally uses record stores and transaction logs. A Rust rewrite could use COW pages for metadata, schema catalogs, token stores, and some index roots even if primary graph records remain append/log structured. COW is especially appealing for small critical roots.

When to use:

- Use for catalog, schema, token metadata, free-list state, and maybe counts store.
- Use when snapshots are important and write working sets are moderate.

When not to use:

- Avoid for very large hot adjacency lists if every small update rewrites large pages.
- Avoid without a robust free-page reclamation and snapshot pinning strategy.

Transferable version:

Use COW for metadata/root structures and append/segment stores for large variable-sized graph payloads. Let the COW root reference immutable segment manifests.

Rust translation:

- `PageId`, `PageEpoch`, `RootSet` newtypes.
- Immutable `PageRef` for readers.
- `WritablePage` only exists inside a write transaction.
- Free pages returned only after `GcWatermark` permits.

Related patterns:

- Dual root slots.
- MVCC snapshot tracking.
- Integrity repair.

Risks:

- Free-list corruption is catastrophic.
- Page cache must distinguish page id plus generation if pages are reused.
- Large write transactions can allocate many pages.

Performance implications:

- Great for point lookups and stable snapshots.
- Write amplification depends on tree height and page split rate.

Memory implications:

- Dirty page sets need accounting.
- Page cache should pin roots and hot internal nodes.

Concurrency implications:

- Single writer is simplest.
- Multiple readers can be lock-free against immutable roots.

Testing implications:

- Crash after page writes before root publication.
- Crash after root publication before free-list publication.
- Reuse freed pages while old snapshot reads.

Agentic code-generation guidance:

- Force generated code to distinguish `PageId` from `PageVersion`.
- Generate model tests comparing a COW tree to a BTreeMap.
- Ask for crash tests around root/free-list ordering.

## Pattern 12: Buffer Pool, Page Cache, Spill, and Pinning

Pattern name: bounded memory with eviction and spill.

Where found:

- DuckDB buffer pool.
- DataFusion runtime environment and disk manager.
- ClickHouse userspace page/cache metrics.
- Tantivy block cache in store reader.

Repo paths and files:

- DuckDB:
  - `src/include/duckdb/storage/buffer/buffer_pool.hpp` around 80-140
  - `src/storage/buffer/buffer_pool.cpp` around 47-112
  - `src/storage/buffer/block_handle.cpp` around 106-155
- DataFusion:
  - `datafusion/execution/src/runtime_env.rs` around 46-115
  - `datafusion/execution/src/disk_manager.rs` around 18-115 and 160-240
- ClickHouse:
  - `src/Common/CurrentMetrics.cpp` around 356-372
  - `src/Common/ProfileEvents.cpp` around 59-100
- Tantivy:
  - `src/store/reader.rs` around 188-225

Observed evidence:

- DuckDB buffer pool evicts blocks until used memory plus extra memory fits; eviction result can include reusable buffers and reservations. `BlockMemory::CanUnload` refuses eviction if already unloaded, if readers are present, or if a temp block needs a temp directory that does not exist. Temporary blocks can be persisted on eviction and memory charge zeroed.
- DataFusion `RuntimeEnv` includes `MemoryPool`, `DiskManager`, `CacheManager`, and object-store registry. `DiskManager` manages spill files for datasets larger than memory, tracks max temp directory size with an atomic, used disk space, and active temp file count.
- ClickHouse exports current metrics for mark cache bytes/files, primary index cache bytes/files, userspace page cache bytes/cells, mmap cache cells, and profile events for page cache hits/misses, mmap cache hits/misses, directory sync, file sync, and checksum mismatches.
- Tantivy store reader gets a compressed block by byte range, decompresses it, caches decompressed blocks keyed by byte-range start, and exposes cache stats.

Compressed pseudocode:

```text
reserve(bytes):
    while used + bytes > limit:
        victim = eviction_queue.pop()
        if victim.pinned_or_readers: continue
        if victim.dirty_or_temp: spill_or_write(victim)
        unload(victim)
    return MemoryReservation(bytes)
```

Why it matters:

Graph databases are memory-sensitive. Page cache behavior often determines traversal latency more than raw storage format. Spill paths prevent a single query or compaction from OOMing the process.

Why it matters for rewriting Neo4j in Rust:

A graph rewrite needs memory governance across page cache, adjacency cache, property cache, query operators, schema indexes, full-text/vector indexes, WAL buffers, and compaction buffers. Rust safety does not prevent unbounded memory growth. Memory reservations and eviction policies must be first-class.

When to use:

- Use for page stores, compressed block stores, query execution, sorting/hash joins, and compaction.
- Use spill for query operators and background compaction.
- Use cache metrics from the beginning.

When not to use:

- Do not build many independent caches without a shared memory budget.
- Do not evict pinned pages.
- Do not spill critical WAL/commit data to temp storage without durability semantics.

Transferable version:

Create a global `StorageMemoryManager` with sub-budgets for page cache, decompressed blocks, query execution, and compaction. Every subsystem gets a reservation before allocating large buffers.

Rust translation:

- `MemoryReservation` RAII object releases on drop.
- `PinnedPage` increments a reader count.
- `SpillManager` creates temp files through a `DiskManager` with max directory size.
- Metrics: cache hit/miss, evicted bytes, spill bytes, active spill files, pinned pages.

Related patterns:

- mmap/direct I/O decision matrix.
- Arrow buffer layout.
- Tantivy compressed block cache.
- ClickHouse cache telemetry.

Risks:

- Multiple caches can double-store the same data.
- Spill can exhaust disk and fail queries.
- Eviction under locks can cause latency spikes.

Performance implications:

- Cache hit rate drives traversal latency.
- Decompression cache helps repeated property/document reads.
- Spill trades memory safety for IO cost.

Memory implications:

- Use exact accounting for large buffers and approximate accounting for small objects.
- Track allocator-resident memory separately from logical cache size.

Concurrency implications:

- Pin/unpin must be cheap and thread-safe.
- Eviction queues need sequence numbers or generation checks to avoid unloading stale handles, as DuckDB does.

Testing implications:

- Force tiny memory limit and assert spill/eviction correctness.
- Test pinned pages cannot be unloaded.
- Test temp-dir-disabled mode returns clean resource errors.

Agentic code-generation guidance:

- Require generated operators and storage readers to request memory reservations.
- Reject unbounded `Vec::new` growth in storage hot paths.
- Ask for metrics at the same time as cache implementation.

## Pattern 13: mmap Versus Direct I/O Is a Workload Policy, Not a Universal Optimization

Pattern name: explicit read method selection.

Where found:

- RocksDB options.
- ClickHouse settings and metrics.
- sled direct-I/O regression notes.

Repo paths and files:

- RocksDB:
  - `db/db_impl/db_impl_open.cc` around 246-265
  - `include/rocksdb/options.h` around 1110-1152
- ClickHouse:
  - `src/Core/Settings.cpp` around 1640-1665
  - `src/Common/CurrentMetrics.cpp` around 356-372
  - `src/Common/ProfileEvents.cpp` around 59-100
- sled:
  - `tests/00_regression.rs` around 1528-1540

Observed evidence:

- RocksDB rejects `allow_mmap_reads && use_direct_reads`, and rejects `allow_mmap_writes && use_direct_io_for_flush_and_compaction`. Its options document that mmap reads bypass block cache and check checksums on every read if checksum verification is enabled. It also notes `DB::SyncWAL()` only works if mmap writes are false.
- ClickHouse uses `min_bytes_to_use_direct_io` to switch to O_DIRECT when total read volume exceeds a threshold. `min_bytes_to_use_mmap_io` is experimental, recommended around 64MB because mmap/munmap is slow, and helps only if data resides in the page cache. `checksum_on_read` is enabled by default and should stay enabled in production.
- sled regression notes mention O_DIRECT slab allocation assumptions: the slab allocator writes to the end of the slab to remain compatible with O_DIRECT.

Compressed decision table:

```text
small random cached reads        -> buffered IO + block/page cache
large sequential scans           -> direct IO if cache pollution matters
large page-cache-resident files   -> mmap only if mapping churn is controlled
WAL writes needing SyncWAL        -> avoid mmap writes unless semantics proven
```

Why it matters:

mmap, buffered IO, and direct IO have different caching, alignment, checksum, and durability implications. Treating mmap as "always faster" is a common storage-engine mistake.

Why it matters for rewriting Neo4j in Rust:

Graph workloads mix tiny random traversals with large analytical scans and index rebuilds. A Rust rewrite likely needs different IO policies for WAL, page store, adjacency segments, property blob scans, full-text segments, and backup/export paths.

When to use:

- Use buffered IO for random graph traversal with a managed page cache.
- Use direct IO for large scans/rebuilds to avoid polluting OS cache.
- Use mmap for stable immutable files when mapping lifetime is long and address-space pressure is acceptable.

When not to use:

- Do not mix mmap writes with WAL sync semantics unless deeply verified.
- Do not use direct IO without alignment-aware allocation.
- Do not mmap many small files with high churn.

Transferable version:

Expose `ReadMethod::{Buffered, Direct, Mmap}` per file class and workload. File classes include WAL, page-store data, immutable adjacency segment, search segment, columnar sidecar, backup scan.

Rust translation:

- Hide OS-specific flags behind `StorageFileOptions`.
- Validate illegal combinations at open time.
- Use aligned buffers for direct IO.
- Track metrics by read method.

Related patterns:

- Page cache.
- Allocator/alignment.
- Checksums on read.
- Immutable segment files.

Risks:

- Direct IO bypasses OS cache and can hurt repeated reads.
- mmap can SIGBUS on truncated files if not protected.
- Checksums may need to be performed on every mmap read if no block cache boundary exists.

Performance implications:

- Read-method thresholds should be benchmark-driven.
- Direct IO can improve scans and harm point reads.
- mmap avoids copy but can pay TLB/page-fault costs.

Memory implications:

- mmap consumes virtual address space and can inflate RSS accounting.
- Direct IO requires aligned buffers and often larger request sizes.

Concurrency implications:

- mmap lifetime must not outlive file validity.
- Direct IO and buffered IO on same file can have cache coherence pitfalls.

Testing implications:

- Run with each read method.
- Test direct IO buffer alignment.
- Test truncation/replacement behavior with mmap readers.

Agentic code-generation guidance:

- Tell models not to "optimize" by switching to mmap/direct IO without a decision table.
- Generate config validation tests for illegal combinations.
- Require benchmark hooks for read method choices.

## Pattern 14: Checksums Everywhere, But With Scope Clarity

Pattern name: scoped checksums.

Where found:

- redb transaction headers.
- fjall WAL batches.
- sled frames.
- DuckDB WAL frames.
- ClickHouse MergeTree parts and compressed blocks.
- Tantivy/search and Parquet metadata indirectly through format checks.

Repo paths and files:

- redb: `src/tree_store/page_store/header.rs` around 225-395
- fjall: `src/journal/batch_reader.rs` around 100-135
- sled: `src/metadata_store.rs` around 436-548
- DuckDB: `src/storage/write_ahead_log.cpp` around 1-170 and `src/storage/wal_replay.cpp` around 1-150
- ClickHouse: `src/Storages/StorageMergeTree.cpp` around 2928-2945 and `src/Common/ProfileEvents.cpp` around 59-80

Observed evidence:

- redb validates transaction header checksums before accepting slots.
- fjall validates WAL batch checksum at `End`.
- sled validates both frame length checksum and payload CRC.
- DuckDB WAL replay validates frame checksum and rejects beyond-file-size frames.
- ClickHouse recomputes/checks part checksums and exposes checksum mismatch metrics for compressed blocks. It also states checksum-on-read is enabled by default and should be enabled in production.

Compressed pseudocode:

```text
Frame {
    len
    len_checksum
    payload
    payload_checksum
}

read_frame:
    verify(len_checksum)
    read(payload)
    verify(payload_checksum)
```

Why it matters:

Checksums turn silent corruption into explicit recovery decisions. The scope of the checksum matters: length, header, payload, page, batch, segment, and manifest checksums catch different fault classes.

Why it matters for rewriting Neo4j in Rust:

Graph corruption can manifest as wrong traversals, not just crashes. Checksums on WAL frames, page headers, adjacency segments, property blobs, and manifests should make corruption fail fast. Online consistency checks can then localize damage.

When to use:

- Use on every durable binary frame.
- Use separate header/length checksums for variable-length records.
- Use part/segment checksums for immutable files.

When not to use:

- Do not rely on checksums as authorization or tamper-proofing.
- Do not skip checksums on production reads unless the storage layer has an equivalent guarantee and you have measured the tradeoff.

Transferable version:

Define checksum scope per file type:

- WAL: length checksum plus batch checksum.
- Page: page header checksum plus optional page body checksum.
- Segment: block checksums plus manifest-level checksum.
- Metadata: whole-file checksum and generation id.

Rust translation:

- `ChecksumScope` enum in format docs.
- `CheckedBytes<T>` wrapper after validation.
- Decode APIs return checked domain structs, not unchecked raw bytes.

Related patterns:

- WAL framing.
- Recovery modes.
- Immutable data parts.

Risks:

- CPU overhead on hot path.
- Checksums can validate stale but internally inconsistent metadata if scope is too narrow.
- Compression/encryption ordering mistakes can make checksums useless.

Performance implications:

- Hardware CRC32C is fast.
- Page-level checksums can be verified on cache miss rather than every logical access.

Memory implications:

- Checksums avoid retaining duplicate validation state.
- Whole-file checksums may require streaming.

Concurrency implications:

- Validate before publishing to shared cache.
- Cache should store only validated blocks.

Testing implications:

- Corrupt each checksum scope independently.
- Test checksum mismatch metrics.
- Test recovery behavior for final-frame corruption versus mid-file corruption.

Agentic code-generation guidance:

- Ask the model to state "what bytes are covered by this checksum" in docs/tests.
- Generate corrupt-byte tests automatically for every binary format.
- Reject generated decoders that parse fields before validating length sanity.

## Pattern 15: Immutable Segment Files With a Small Atomic Meta File

Pattern name: segment component set plus meta root.

Where found:

- Tantivy search index.
- ClickHouse MergeTree data parts.
- Iggy segmented log.
- RocksDB SST plus manifest pattern.

Repo paths and files:

- Tantivy:
  - `src/index/segment_component.rs` around 1-80
  - `src/index/index_meta.rs` around 266-310
  - `src/indexer/index_writer.rs` around 590-670 and 730-820
  - `src/indexer/segment_updater.rs` around 38-55 and 383-460
- ClickHouse:
  - `src/Storages/StorageMergeTree.cpp` around 2928-2945
- Iggy:
  - `core/server/src/streaming/partitions/log.rs` around 1-220

Observed evidence:

- Tantivy stores each segment component in its own file named `segment_uuid.component_extension`, with deletes using `segment_uuid.delete_opstamp.component_extension`. Components include postings, positions, fast fields, field norms, terms dictionary, compressed row-oriented store, and live/deleted bitset.
- Tantivy `IndexMeta` is serialized on disk in `meta.json` and contains searchable segment metas, schema, opstamp, settings, and optional commit payload. `save_metas` is atomic and flushed.
- Tantivy `prepare_commit` cuts the queue, joins workers, flushes segment work, then `commit` schedules metadata publication. Changes are visible only after commit.
- ClickHouse data parts have `checksums.txt` and can recalculate/check/write checksums if missing.
- Iggy `SegmentedLog` keeps vectors of `Segment`, `Storage`, optional indexes, and an in-flight journal. It lazily ensures active indexes with capacity derived from 16MB and `INDEX_SIZE`.

Compressed pseudocode:

```text
segment/
  <uuid>.postings
  <uuid>.positions
  <uuid>.terms
  <uuid>.fast
  <uuid>.store
  <uuid>.<delete_opstamp>.delete

meta.json = { schema, opstamp, segments: [uuid, max_doc, deletes...] }
```

Why it matters:

Immutable segment files let writers build new state off to the side and publish it atomically by changing a small metadata root. Readers can pin old segment lists while compaction/merge builds new ones.

Why it matters for rewriting Neo4j in Rust:

This is directly useful for graph side indexes: label indexes, property value indexes, full-text indexes, vector indexes, and perhaps append-only relationship/property segments. Primary graph roots can point at immutable segment generations.

When to use:

- Use for secondary indexes and append-heavy side stores.
- Use when merges can rewrite immutable files in the background.
- Use for compressed blocks and search/document stores.

When not to use:

- Avoid over-segmenting tiny updates; file-count overhead matters.
- Do not use immutable segment replacement for tiny fixed-width records where page updates are cheaper.

Transferable version:

Represent every side index as `IndexMeta { generation, segments, schema_version, opstamp }`. Segment files are immutable after publication. Deletes are separate delete bitsets/tombstone segments until merge.

Rust translation:

- `SegmentId` UUID/newtype.
- `SegmentComponent` enum.
- `IndexMeta` serde/format with checksum.
- `SegmentSet` pinned by readers and swapped atomically on commit.

Related patterns:

- Manifest/CURRENT update.
- Durable file replacement.
- Search index block checkpoints.
- MVCC snapshot pinning.

Risks:

- Too many files degrade open/list performance.
- Delete bitsets can accumulate until merge.
- Metadata file becomes a single point of publication.

Performance implications:

- Excellent for read-mostly search/index workloads.
- Merge policy determines write amplification and query fanout.

Memory implications:

- Segment readers cache terms/postings/store blocks.
- Metadata is small and can be cloned per reader.

Concurrency implications:

- Readers use old segment lists while writer publishes new `meta.json`.
- Merge threads need task inventories to avoid deleting live files.

Testing implications:

- Crash before and after meta publication.
- Missing component file should fail segment validation.
- Delete bitset opstamp ordering must be tested.

Agentic code-generation guidance:

- Ask generators to separate building segment files from publishing segment metadata.
- Generate file-lifecycle diagrams: temp, staged, published, obsolete, deleted.
- Use precise names like `publish_index_meta_atomically`.

## Pattern 16: Search Store Block Checkpoints and Compressed Block Cache

Pattern name: block checkpoint index.

Where found:

- Tantivy, Rust search engine.

Repo path and files:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src`
- `src/store/index/block.rs` around 1-130
- `src/store/reader.rs` around 188-225

Observed evidence:

- Tantivy doc store index checkpoints are organized into blocks for readability and compression.
- A `CheckpointBlock` stores checkpoints with doc ranges and byte ranges.
- Serialization writes count, first doc, first byte offset, then varint-encoded deltas for doc counts and block byte sizes.
- Store reader seeks a checkpoint by doc id, slices compressed block bytes by byte range, decompresses, and caches decompressed blocks keyed by byte range start.

Compressed pseudocode:

```text
checkpoint_block:
    vint(num_checkpoints)
    vint(first_doc)
    vint(first_byte_offset)
    for checkpoint:
        vint(doc_delta)
        vint(byte_len_delta)

read_doc(doc):
    cp = skip_index.seek(doc)
    block = cache.get(cp.byte_start) or decompress(file[cp.byte_range])
    return block.doc(doc)
```

Why it matters:

Compressed block storage needs a compact index from logical ids to byte ranges. Delta/vint checkpoints make the index small while preserving random access.

Why it matters for rewriting Neo4j in Rust:

Graph property values, large dynamic records, relationship chains, and historical transaction payloads can be compressed in blocks. A checkpoint index can map node id/relationship id/range to compressed blocks without storing per-record file offsets.

When to use:

- Use for compressed property blobs, adjacency lists, document/property side stores, and index payloads.
- Use when records are naturally read in ranges or blocks.

When not to use:

- Do not use if every access needs single fixed-width records and decompression overhead dominates.
- Do not use without a cache for repeated hot-block access.

Transferable version:

For each immutable segment, maintain a sparse checkpoint index: logical range start/end plus byte range. Encode deltas. Cache decompressed blocks by byte range.

Rust translation:

- `BlockCheckpoint { logical_range: Range<u64>, byte_range: Range<u64> }`
- `CheckpointBlock::serialize_varint_delta`.
- `CompressedBlockCache` keyed by `(segment_id, byte_start)`.

Related patterns:

- Immutable segment files.
- Page cache.
- Arrow/columnar layout for analytical sidecars.

Risks:

- Corrupt checkpoint index can point outside file.
- Large blocks increase read amplification.
- Small blocks reduce compression ratio.

Performance implications:

- Tune block size around traversal/property access patterns.
- Cache decompressed hot blocks.

Memory implications:

- Decompressed blocks can double memory use if raw compressed bytes are also cached.
- Keep cache under global memory manager.

Concurrency implications:

- Immutable blocks make sharing easy.
- Cache insertion should validate checksum before publishing.

Testing implications:

- Roundtrip checkpoint encode/decode.
- Fuzz doc id seeks.
- Corrupt byte ranges and ensure reader rejects.

Agentic code-generation guidance:

- Ask for block-size benchmarks before hardcoding constants.
- Generate seek tests around block boundaries.
- Require bounds checks before slicing file bytes.

## Pattern 17: Arrow Array Layout as the Standard In-Memory Columnar Contract

Pattern name: offset/length plus shared buffers and null bitmap.

Where found:

- Apache Arrow Rust implementation.

Repo path and file:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-arrow-rs-src`
- `arrow-data/src/data.rs` around 150-270

Observed evidence:

- `ArrayData` is a generic representation of Arrow array data.
- It stores data type, element length, element offset, buffers, child data, and optional null buffer.
- The comments emphasize that multiple `ArrayData` values can refer to the same underlying buffer with different offset/len, child arrays can have their own buffers/children, and the null bitmap represents exactly `len` elements.

Compressed layout:

```text
ArrayData {
    data_type
    len
    offset      // in logical items, not bytes
    buffers     // values, offsets, etc.
    child_data  // nested arrays
    nulls       // optional validity bitmap
}
```

Why it matters:

Arrow is a lingua franca for vectorized execution and analytical interchange. Its layout makes slicing cheap by adjusting offsets and sharing buffers.

Why it matters for rewriting Neo4j in Rust:

A graph database rewrite may keep primary storage graph-native but expose analytical scans, vectorized Cypher operators, or export/import through Arrow. Columnar sidecars for labels, property columns, counts, and relationship projections should use Arrow-compatible layout where possible.

When to use:

- Use for query execution batches.
- Use for analytical sidecars and export.
- Use for property columns with homogeneous types.

When not to use:

- Do not force deeply pointer-heavy graph adjacency into Arrow if traversal needs record-local mutation.
- Do not use Arrow buffers as your only durable format unless you define metadata/version/checksum around them.

Transferable version:

Use Arrow as the execution/interchange layer, not necessarily the storage source of truth. Convert graph records into `RecordBatch` chunks for scans, analytics, and vectorized operators.

Rust translation:

- Use `arrow-array`, `arrow-buffer`, `arrow-data` for in-memory batches.
- Store graph ids as primitive arrays.
- Store optional properties with null buffers.
- Use dictionary arrays for labels/types/tokens.

Related patterns:

- Parquet statistics and pushdown.
- DataFusion runtime/spill.
- Columnar sidecar files.

Risks:

- Offset semantics are subtle: item offsets are not byte offsets.
- Child-array offsets can be cumulative.
- Null semantics must match graph property absence semantics.

Performance implications:

- Vectorized scans and SIMD-friendly buffers.
- Cheap slicing and projection.
- Conversion cost from graph-native layout can be nontrivial.

Memory implications:

- Shared buffers reduce copies.
- Null bitmaps and offsets add overhead for nested/optional data.

Concurrency implications:

- Immutable `ArrayData` behind `Arc` is reader-friendly.
- Builders require mutable ownership and memory accounting.

Testing implications:

- Test slicing with nonzero offsets.
- Test null versus missing-property semantics.
- Test nested list arrays for multi-valued properties.

Agentic code-generation guidance:

- Ask the model to state whether a property is absent, null, empty list, or default.
- Generate Arrow conversion tests with sliced arrays.
- Do not let generated code assume buffer offset is bytes unless the Arrow API says so.

## Pattern 18: Parquet Statistics and Page Index Pushdown

Pattern name: min/max/null/page-index pruning.

Where found:

- Apache Parquet format.
- Apache DataFusion Parquet reader.

Repo paths and files:

- Parquet format:
  - `apache-parquet-format-src/src/main/thrift/parquet.thrift` around 264-315 and 1210-1275
- DataFusion:
  - `datafusion/datasource-parquet/src/page_filter.rs` around 40-170, 224-315, and 412-470
  - `datafusion/datasource-parquet/src/row_group_filter.rs` around 1-160

Observed evidence:

- Parquet `Statistics` contains optional min/max, null count, distinct count, newer `min_value`/`max_value` determined by column order, exactness flags, and NaN handling notes.
- Parquet `OffsetIndex` stores ordered page locations and first row index; `ColumnIndex` stores page-level null flags and min/max arrays. If `ColumnIndex` is present, `OffsetIndex` must also be present.
- DataFusion page filter evaluates `ParquetColumnIndex` and `ParquetOffsetIndex`, converts them into row selections, extracts single-column pruning predicates, skips page pruning for row groups already fully matched by row-group statistics, evaluates pruning predicates over page stats, and converts page booleans into `RowSelector::select`/`skip` ranges.
- DataFusion row group filter progressively narrows row groups and can classify row groups as not matching, partially matching, or fully matching by evaluating predicates and inverted predicates against metadata.

Compressed pseudocode:

```text
for row_group in access_plan:
    if row_group_stats_prove_no_match:
        skip(row_group)
    if row_group_stats_prove_full_match:
        mark_fully_matched(row_group)
    else if page_index_available:
        for page in column_pages:
            may_match = predicate(min[page], max[page], nulls[page])
        convert may_match[] to RowSelection
```

Why it matters:

Metadata pruning avoids reading data. Page-level pruning can skip row ranges inside a row group, not just whole files.

Why it matters for rewriting Neo4j in Rust:

Graph databases increasingly serve hybrid transactional/analytical workloads. If the Rust rewrite builds columnar sidecars for labels/properties/relationship projections, Parquet-style statistics can let scans skip chunks. Even inside graph-native segments, storing min/max/token ranges and first-row offsets enables pruning.

When to use:

- Use for analytical scans over property values, labels, timestamps, relationship types, and vector metadata.
- Use for immutable sidecar segments and backups/export.

When not to use:

- Do not rely on min/max for highly unclustered graph properties; false positives can be high.
- Do not treat missing statistics as proof of no match.
- Do not use truncated stats for correctness; they are pruning hints only.

Transferable version:

For every immutable graph side segment, store chunk-level and page-level stats: min/max property value, null/missing count, first logical id, row count, byte offset, and optional bloom/filter. Query planner turns predicates into chunk/page selections.

Rust translation:

- `SegmentStatistics { row_count, min, max, null_count, exactness }`
- `PageIndex { page_locations, column_bounds }`
- `PruningPredicate` over typed scalar stats.
- Use DataFusion/Arrow for sidecar scans where feasible.

Related patterns:

- Arrow columnar layout.
- Immutable segment files.
- Search index segments.
- Buffer/spill manager.

Risks:

- Stats can be absent, truncated, or semantically unordered.
- NaN and collation rules are tricky.
- Multi-column predicates often cannot be page-pruned independently.

Performance implications:

- Huge win for selective scans on clustered data.
- Metadata reads add overhead for unselective scans.

Memory implications:

- Page indexes consume memory; cache selectively.
- Min/max values for variable-width data can be truncated.

Concurrency implications:

- Immutable segment stats are easy to share.
- New segments publish stats atomically with data.

Testing implications:

- Test missing stats means "unknown", not "empty".
- Test exactness/truncation behavior.
- Test page selection around boundaries.

Agentic code-generation guidance:

- Tell models that pruning is an optimization, never a correctness filter unless rows are later evaluated.
- Generate tests where stats are absent and truncated.
- Require explicit sort/collation semantics for min/max.

## Pattern 19: Allocator Telemetry and Cache Arenas Are Storage Features

Pattern name: allocator-aware memory observability.

Where found:

- jemalloc documentation.
- DuckDB jemalloc allocator integration.
- ClickHouse allocator and metrics.

Repo paths and files:

- jemalloc:
  - `doc/jemalloc.xml.in` around 384-435 and 2728-2812
- DuckDB:
  - `src/common/allocator/allocator_jemalloc.cpp` around 1-180
- ClickHouse:
  - `src/Common/Allocator.h` around 20-36
  - `src/Common/CurrentMetrics.cpp` around 356-372

Observed evidence:

- jemalloc `mallctl` provides a tree-structured introspection/control namespace. The docs describe reading/writing values, translating names to MIBs for repeated queries, and stats such as `stats.allocated`, `stats.active`, `stats.metadata`, and `stats.resident`.
- DuckDB wraps `duckdb_je_mallctl`, supports thread cache flush, thread idle, all-arena purge, peak reset, and background thread control.
- ClickHouse removed manual `mmap/mremap/munmap` for large allocations and relies on jemalloc because performance tests were faster without manual mmap/mremap. ClickHouse metrics mention a dedicated cache jemalloc arena for primary index cache and clarify it does not overlap with part memory.

Compressed pseudocode:

```text
allocator_stats():
    mallctl("epoch", update=true)
    allocated = mallctl("stats.allocated")
    active = mallctl("stats.active")
    resident = mallctl("stats.resident")
    metadata = mallctl("stats.metadata")
```

Why it matters:

Storage engines often lose memory to allocator fragmentation, thread caches, metadata, and independent caches. Logical cache bytes can be much smaller than resident memory.

Why it matters for rewriting Neo4j in Rust:

A Rust graph engine will allocate many small objects during query planning/execution and large buffers for pages, WAL, compaction, and Arrow batches. Without allocator telemetry, operators may see "mysterious" RSS growth. Allocator-aware metrics are part of production storage design.

When to use:

- Use jemalloc or mimalloc telemetry in production builds where possible.
- Use dedicated arenas for caches if allocator supports it and the team can operate it.
- Use thread-cache flushing at worker lifecycle boundaries if measured.

When not to use:

- Do not prematurely hand-roll mmap allocators for large buffers.
- Do not expose allocator tuning knobs without metrics.
- Do not assume Rust's logical ownership equals returned memory to OS.

Transferable version:

Expose memory metrics in layers: logical cache bytes, reserved query bytes, allocator allocated/active/resident bytes, spill bytes, pinned page bytes, and per-cache bytes.

Rust translation:

- Use `tikv-jemallocator`/`jemalloc-ctl` if project policy allows.
- Wrap large storage allocations in accounting types.
- Provide allocator stats through admin endpoint and logs.

Related patterns:

- Buffer pool and spill.
- mmap/direct IO.
- ClickHouse cache metrics.

Risks:

- Allocator-specific APIs reduce portability.
- Incorrect arena use can fragment memory.
- Stats can require enabling allocator stats and updating epoch.

Performance implications:

- Allocator choice affects query and compaction workloads.
- Thread caches improve speed but retain memory.

Memory implications:

- Track resident, active, allocated, metadata, and retained/mapped separately.
- Cache budgets should consider allocator overhead.

Concurrency implications:

- Per-thread caches and arenas interact with worker pools.
- Background allocator threads can purge memory asynchronously.

Testing implications:

- Memory limit tests should check logical and resident trends.
- Stress tests should include worker churn and cache eviction.

Agentic code-generation guidance:

- Ask generated code to include memory accounting at allocation boundaries.
- Do not let generated code introduce custom global allocators casually.
- Generate observability names along with cache structures.

## Pattern 20: Poison-on-Uncertain-Rewrite

Pattern name: fail closed after ambiguous durable state.

Where found:

- Apache Iggy prepare journal.
- RocksDB manifest failure handling provides a related fail-safe style.

Repo paths and files:

- Iggy:
  - `core/journal/src/prepare_journal.rs` around 96-190, 540-605, and 630-682
- RocksDB:
  - `db/version_set.cc` around 6432-6462

Observed evidence:

- Iggy `PrepareJournal` has a `poisoned` state populated after `drain()` progresses past atomic rename and a subsequent step fails. All IO entry points must short-circuit with `JournalError::Poisoned` to prevent future appends from writing into an orphaned old file descriptor or serving stale offsets.
- After rename, Iggy fsyncs parent directory. If opening/syncing parent or reopening the file fails, the journal is poisoned.
- Iggy advances snapshot watermark only after rewrite is durable. Advancing earlier could allow future append to evict a live entry from the in-memory index while the entry remains on disk but unreachable.
- RocksDB failure handling around manifest/CURRENT similarly treats uncertain pointer state carefully and avoids deleting a manifest that `CURRENT` may point to.

Compressed pseudocode:

```text
rename(tmp, wal)
if fsync(parent) fails:
    poison("parent fsync failed after rename")
    return error
if reopen(wal) fails:
    poison("reopen failed after rename")
    return error
advance_watermark()
```

Why it matters:

Some failures occur after the external world may have changed. Continuing as if nothing happened can corrupt future state. Poisoning turns an ambiguous state into a loud operational failure.

Why it matters for rewriting Neo4j in Rust:

Graph databases cannot silently lose relationship/property records. If a checkpoint, manifest rewrite, or WAL compaction reaches an uncertain durable point, the store should stop writes and require recovery/repair rather than keep serving writes on stale assumptions.

When to use:

- Use after rename, partial manifest update, failed parent fsync, failed file reopen, or detected invariant mismatch.
- Use when continuing can make recovery worse.

When not to use:

- Do not poison for ordinary transient read errors that can be retried safely.
- Do not poison without surfacing clear diagnostics and recovery guidance.

Transferable version:

Introduce `StoragePoison` with stage, source error, and affected file/epoch. All write APIs check poison state. Read APIs may continue only if they do not depend on ambiguous state.

Rust translation:

- `OnceLock<PoisonState>` or atomic error flag for one-way poison.
- `PoisonState { stage: &'static str, path: PathBuf, epoch: u64, source }`
- Admin `repair` or restart recovery path clears poison only after validation.

Related patterns:

- Durable file replacement.
- Snapshot watermark ordering.
- Recovery modes.

Risks:

- Poisoning can reduce availability.
- Operators need tooling to inspect/repair.
- Over-broad poisoning can turn minor issues into outages.

Performance implications:

- Negligible steady-state overhead.
- Prevents compounding corruption.

Memory implications:

- Poison state is tiny.

Concurrency implications:

- Must be visible to all writer threads immediately.
- In-flight operations need clear semantics: fail, drain, or finish.

Testing implications:

- Inject failure after rename, after parent fsync open, after parent sync, after reopen.
- Assert future append fails.
- Assert poison diagnostics name the stage.

Agentic code-generation guidance:

- Ask models to classify failures as retryable, fatal, or poison.
- Generate one-way poison checks at public write entry points.
- Do not let generated code swallow errors after commit-point side effects.

## Pattern 21: Recovery Modes and Corruption Taxonomy

Pattern name: strict versus salvage recovery.

Where found:

- RocksDB WAL recovery.
- DuckDB WAL replay.
- redb header slot selection and integrity repair.
- sled and Iggy torn-tail tests.

Repo paths and files:

- RocksDB:
  - `db/db_impl/db_impl_open.cc` around 1168-1388
  - `src/config/mod.rs` in TiKV for RocksDB recovery mode defaults and tests
- DuckDB:
  - `src/storage/wal_replay.cpp` around 1-150
- redb:
  - `src/tree_store/page_store/header.rs` around 154-395
  - `src/db.rs` around 572-635
- sled:
  - `tests/00_regression.rs` around 149-180 and 242-275
- Iggy:
  - `core/journal/src/prepare_journal.rs` around 780-925

Observed evidence:

- RocksDB `RecoverLogFiles` processes log files and reads records with a recovery mode and checksum setting, checking that sequence numbers do not move backward.
- TiKV config references RocksDB `DBRecoveryMode::PointInTime` by default and tests other modes such as skipping corrupted records.
- DuckDB WAL replay rejects corrupt checksums and entries beyond file size, with special handling for older no-checksum WAL and encrypted WAL.
- redb recovery chooses valid header slots and can run slow `check_integrity` to repair roots after crashes/power loss.
- sled regression tests document recovery bugs caused by file-tip/accounting mistakes and missing checksums.
- Iggy tests explicitly distinguish torn tail repair from mid-file corruption.

Compressed taxonomy:

```text
CorruptionKind:
    TornTail          -> truncate/ignore final partial record
    BadChecksum       -> reject record or stop at point-in-time boundary
    MidFileDamage     -> hard error, no silent truncation
    MissingFile       -> manifest/segment validation error
    FutureSequence    -> reject or stop, depending recovery mode
    StaleRoot         -> choose older valid root
```

Why it matters:

Recovery is policy, not just parsing. A final torn record after a crash may be safely ignored; mid-file corruption may indicate lost committed data and must not be silently discarded.

Why it matters for rewriting Neo4j in Rust:

Graph recovery must distinguish uncommitted tail from committed corruption. A missing relationship record inside the log is not equivalent to a torn final transaction. Operator-facing recovery modes should be explicit and tested.

When to use:

- Always define recovery modes before writing WAL readers.
- Use strict mode by default for production.
- Use salvage/point-in-time modes for operator-driven repair.

When not to use:

- Do not silently skip corrupted records in normal startup.
- Do not let recovery continue after sequence-number regression unless explicitly in salvage mode.

Transferable version:

Implement `RecoveryMode::{Strict, TolerateFinalTornTail, PointInTime, SalvageSkipCorrupt}`. Every corruption path returns `RecoveryDecision::{Apply, StopAtLsn, TruncateTail, Fatal}` based on mode and location.

Rust translation:

- `WalCorruption { kind, offset, lsn, context }`
- `RecoveryPolicy::decide(corruption, position)`
- Persist recovery report for operators.

Related patterns:

- Checksummed WAL batches.
- Torn-tail repair.
- Integrity check.

Risks:

- Salvage can hide data loss.
- Strict mode can block startup even if data could be mostly recovered.
- Recovery reports need enough detail for operators.

Performance implications:

- Checksumming and validation cost startup time.
- Recovery snapshots reduce log scan length.

Memory implications:

- Recovery should stream records.
- Integrity repair may need page graph traversal.

Concurrency implications:

- Recovery should usually run before normal writers start.
- Online repair requires extra isolation.

Testing implications:

- Generate corrupted WALs at every byte offset.
- Test each recovery mode's decision.
- Test sequence regression and missing file cases.

Agentic code-generation guidance:

- Demand a corruption taxonomy before implementation.
- Generate recovery reports as data structures, not just logs.
- Reject code that treats all `io::ErrorKind::InvalidData` the same.

## Pattern 22: Crash-Safety Testing With Failpoints and Reference Models

Pattern name: crash model testing.

Where found:

- sled failpoint tests and regression postmortems.
- Tantivy failpoints around `save_metas`.
- Iggy prepare-journal corruption tests.
- redb integrity checks.

Repo paths and files:

- sled:
  - `tests/test_tree_failpoints.rs` around 168-225 and 353-490
  - `tests/00_regression.rs` around 149-180, 242-275, and 1528-1540
- Tantivy:
  - `src/indexer/segment_updater.rs` around 38-55
- Iggy:
  - `core/journal/src/prepare_journal.rs` around 780-925
- redb:
  - `src/db.rs` around 572-635

Observed evidence:

- sled has a property-style crash harness using a global mutex, failpoints, temporary configs, segment sizes, a reference model, and `catch_unwind`. The macro catches failpoint errors, tears down failpoints, restarts, and continues. Operations update a reference model; after `Flush`, operations since the last crash become durable in the reference.
- sled regression notes document real recovery bugs: segment accountant failed to set file tip; rewritten buffers lacked checksum and garbage could be read; snapshot loading and uninitialized segment tip bugs; O_DIRECT slab allocator assumptions.
- Tantivy injects a failpoint in `save_metas` before atomic write.
- Iggy tests reopen/rebuild index, corrupt command byte, torn tail repair, and mid-file corruption rejection.

Compressed pseudocode:

```text
model = BTreeMap::new()
for op in generated_ops:
    maybe_enable_failpoint()
    result = run_engine_op(op)
    if crash:
        engine = reopen()
        model = model.only_durable_ops()
    else:
        model.apply(op)
    assert(engine.snapshot() == model)
```

Why it matters:

Storage correctness cannot be proven by happy-path unit tests. The most important bugs occur between writes, syncs, renames, and in-memory publication.

Why it matters for rewriting Neo4j in Rust:

A graph store has rich invariants: relationship chains must be reciprocal, degrees/counts must match records, index entries must match properties, high ids must not regress, and recovered transaction id must match data. Crash testing should compare against a simple model and graph invariant checker.

When to use:

- Use from the first WAL/checkpoint implementation.
- Use for every commit, checkpoint, compaction, manifest, and recovery path.
- Use model testing for record store invariants.

When not to use:

- Do not wait until the storage engine is "done" to add crash tests.
- Do not rely only on process-kill tests; use deterministic failpoints too.

Transferable version:

Define a tiny graph model: nodes, relationships, properties, labels. Generate operations, commit/flush markers, injected crash points, reopen, and compare durable graph state plus invariants.

Rust translation:

- Use `proptest` for operation sequences.
- Use feature-gated failpoints.
- Use temp directories and crash/reopen harness.
- Track durable prefix in model based on commit policy.

Related patterns:

- WAL framing.
- Durable-before-visible.
- Manifest update order.
- Recovery modes.

Risks:

- Model can be wrong or too weak.
- Failpoints can miss OS/filesystem reorderings.
- Tests can become slow; shard them into quick and exhaustive suites.

Performance implications:

- Crash tests are not hot-path code but should run in CI with bounded cases.
- Longer randomized crash suites can run nightly.

Memory implications:

- Reference model should be small and bounded.

Concurrency implications:

- Add concurrent reader/writer crash tests after single-writer model passes.
- Use deterministic schedulers where feasible.

Agentic code-generation guidance:

- Require failing crash tests before storage implementation.
- Ask models to list crash points in comments or test data.
- Generate reference-model comparisons, not just "does not panic" tests.

## Neo4j-Style Rust Rewrite Synthesis

The strongest cross-repo conclusion is that a Rust rewrite should split storage design into several explicit durability domains rather than one monolithic "graph file."

Recommended storage domains:

1. Primary graph records: fixed-width or page-based node/relationship headers with WAL redo and/or COW roots.
2. Variable payload segments: append-only immutable or semi-immutable property/adjacency blobs with block checkpoints and checksums.
3. Metadata roots: COW or dual-slot superblock for graph root set, high ids, store versions, manifest epoch, checkpoint LSN.
4. Secondary indexes: immutable segment sets with small atomic metadata roots, Tantivy-style.
5. Analytical sidecars: Arrow/Parquet-compatible batches for property scans, label scans, and relationship projections.
6. WAL/checkpoint subsystem: framed transaction WAL, online checkpoint with sealed WAL segments, recovery modes, and crash reports.
7. Memory subsystem: global memory manager, page cache, decompressed block cache, query memory reservations, spill manager, allocator telemetry.

Core invariants to encode:

- A transaction id is never durable unless every store mutation it certifies is recoverable.
- A root pointer is never visible to readers until the pages/segments it names are durable or intentionally non-durable.
- A manifest never deletes files still visible to any pinned snapshot.
- A checkpoint watermark never advances before the checkpoint image is durable.
- A WAL tail can be truncated only when it is demonstrably a final torn append, not mid-file damage.
- Every binary frame declares length, version, checksum scope, and recovery behavior.
- Every large allocation or cache insertion is accounted against a budget.

Suggested module boundaries:

- `wal`: framed batches, recovery policy, group commit, WAL segments.
- `checkpoint`: online checkpoint, superblock slots, root bundle.
- `manifest`: immutable file lifecycle, durable file replacement, GC.
- `page_store`: COW or WAL-backed fixed pages, page cache, checksums.
- `segment_store`: immutable compressed blocks, checkpoint indexes, direct/mmap/buffered IO policy.
- `index_store`: search/property index segment metadata and merge policy.
- `columnar`: Arrow/Parquet sidecar encoding and pruning metadata.
- `memory`: reservations, buffer pool, spill, allocator stats.
- `crash_tests`: failpoints, model, corruption fixtures.

Agentic code-generation rules for the rewrite:

- Start every storage feature with executable crash-state specs.
- Generate data-format grammars before encoders.
- Generate recovery before compaction.
- Generate corruption tests before accepting decoders.
- Require type names that encode durability and visibility: `Prepared`, `Durable`, `Published`, `Pinned`, `Validated`.
- Reject code that calls `rename`, `set_len`, `flush`, `sync_all`, or `sync_data` in ad hoc ways outside storage primitives.
- Require every file format to have version, magic, length, and checksum tests.
- Require every background compaction/checkpoint task to have memory and disk budgets.

## Direct Source Files Cited

redb:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src/src/transactions.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src/src/tree_store/page_store/header.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redb-src/src/db.rs`

fjall:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/journal/writer.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/journal/entry.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/journal/batch_reader.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/journal/recovery.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/db.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/compaction/worker.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/fjall-src/src/snapshot_tracker.rs`

sled:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sled-src/src/metadata_store.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sled-src/tests/test_tree_failpoints.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sled-src/tests/00_regression.rs`

RocksDB and TiKV:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rocksdb-src/db/db_impl/db_impl_write.cc`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rocksdb-src/db/version_set.cc`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rocksdb-src/db/db_impl/db_impl_open.cc`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rocksdb-src/include/rocksdb/options.h`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tikv-src/components/raftstore/src/store/fsm/apply.rs`

DuckDB:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/include/duckdb/storage/write_ahead_log.hpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/write_ahead_log.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/wal_replay.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/checkpoint_manager.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/storage_manager.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/include/duckdb/storage/buffer/buffer_pool.hpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/buffer/buffer_pool.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/storage/buffer/block_handle.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/duckdb-src/src/common/allocator/allocator_jemalloc.cpp`

ClickHouse:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src/src/Storages/StorageMergeTree.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src/src/Core/Settings.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src/src/Common/Allocator.h`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src/src/Common/CurrentMetrics.cpp`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/clickhouse-src/src/Common/ProfileEvents.cpp`

Tantivy:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/index/segment_component.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/index/index_meta.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/indexer/index_writer.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/indexer/segment_updater.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/store/index/block.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tantivy-src/src/store/reader.rs`

Arrow, Parquet, and DataFusion:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-arrow-rs-src/arrow-data/src/data.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/mod.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-parquet-format-src/src/main/thrift/parquet.thrift`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/page_filter.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/row_group_filter.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-datafusion-src/datafusion/execution/src/runtime_env.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-datafusion-src/datafusion/execution/src/disk_manager.rs`

jemalloc:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/jemalloc-src/doc/jemalloc.xml.in`

Apache Iggy:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/journal/src/file_storage.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/journal/src/prepare_journal.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/journal/src/lib.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/server/src/streaming/partitions/journal.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/server/src/streaming/partitions/log.rs`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/apache-iggy-src/core/server/src/streaming/partitions/segments.rs`

Materialize and RisingWave:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/materialize-src/src/rocksdb-types/src/config.rs`
- `/Users/amuldotexe/Desktop/oss-read-only/risingwave/src/meta/src/hummock/compaction/picker/tier_compaction_picker.rs`
- `/Users/amuldotexe/Desktop/oss-read-only/risingwave/src/meta/src/hummock/compaction/picker/ttl_reclaim_compaction_picker.rs`

## Gaps and Caveats

- CGC completed only for fjall and sled. redb and Tantivy CGC attempts failed/terminated, so their findings are direct-source only.
- RocksDB and DuckDB are C++ and require translation judgment for Rust; their ordering patterns are transferable, but their exact APIs are not.
- Materialize and RisingWave were only lightly sampled. This slice uses them for compaction-policy contrast, not as primary sources for WAL/checkpointing.
- OpenDAL, Pinot, Feldera, Arroyo, OpenObserve, pandas, and most Polars internals were not deeply inspected in this slice.
- Line ranges are approximate positions from the local checkout inspected on 2026-07-06. Source repositories may drift.
- This file intentionally favors high recall and pattern transfer over a single prescriptive architecture.
