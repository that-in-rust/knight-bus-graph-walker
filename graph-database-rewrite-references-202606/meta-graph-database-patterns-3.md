# Meta Graph Database Patterns 3: Storage And Rust Systems Patterns

This canonical file preserves the storage-heavy Worker 3 encyclopedia slice for
rewriting a Neo4j-like graph store in Rust with lower RAM usage. It contains
source-backed material on page/cache layout, mmap and zero-copy reads,
WAL/checkpoint recovery, indexes, allocators, unsafe encapsulation, concurrency,
and verification.

Important constraint: graph tools were used only as navigational evidence. Every substantive pattern below is backed by direct source reads from the referenced repository paths.

## Repositories Inspected

### Deep or Directly Source-Read

- `gitrefrepo/redb-src`
- `gitrefrepo/rocksdb-src`
- `gitrefrepo/rust-rocksdb-src`
- `gitrefrepo/qdrant-src`
- `gitrefrepo/tantivy-src`
- `gitrefrepo/sled-src`
- `gitrefrepo/fjall-src`
- `gitrefrepo/jemalloc-src`
- `gitrefrepo/tikv-src`
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit`
- `/Users/amuldotexe/Desktop/oss-read-only/iggy`
- `/Users/amuldotexe/Desktop/oss-read-only/opendal`
- `/Users/amuldotexe/Desktop/reference-repos-yard/kani`
- `/Users/amuldotexe/Desktop/reference-repos-yard/miri`
- `/Users/amuldotexe/Desktop/reference-repos-yard/lockbud`
- `/Users/amuldotexe/Desktop/reference-repos-yard/Rudra`
- `/Users/amuldotexe/Desktop/personal-repos-lane/nostd-union-find`

### Present but Only Shallow-Inspected

- `gitrefrepo/tikv-src`: direct source read only for `components/tracker/src/slab.rs`.
- `/Users/amuldotexe/Desktop/oss-read-only/opendal`: direct source read only for range read options, retry layer, and concurrency-limit layer.
- `/Users/amuldotexe/Desktop/oss-read-only/iggy`: direct source read only for prepare journal and partition/log search results.
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit`: direct source read only for bundled split directories, hot cache, and mmap split cache.

### Explicitly Uninspected or Deferred

- `gitrefrepo/tikv-src` Raft log, MVCC engine, Titan/blob storage, RocksDB engine wrappers.
- `gitrefrepo/qdrant-src` HNSW vector index and collection-level snapshot machinery.
- `gitrefrepo/fjall-src` lower-level `lsm-tree` crate internals beyond journal/recovery/config docs.
- `/Users/amuldotexe/Desktop/reference-repos-yard/{polonius,prusti-dev,creusot,aeneas,charon,flux,hax,RAPx,cargo-scan,dylint}` beyond presence checks or prior `rg` discovery.
- `/Users/amuldotexe/Desktop/personal-repos-lane/nostd-fmt-fixed-capacity` and `nostd-toposort-kahns-algorithm`.

## Evidence Commands and Tools Used

- Read required local skills:
  - `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
  - `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`
- `codebase-memory-mcp`:
  - Indexed `gitrefrepo/redb-src` in a temporary project.
  - Search results highlighted `page_store/page_manager.rs`, `buddy_allocator.rs`, `cached_file.rs`, and page reuse tests.
  - Claims were verified with direct reads from those files.
- CodeGraphContext:
  - Attempted `redb-src` and `rust-rocksdb-src` with `--database ladybugdb`; both stalled after initialization and were interrupted.
  - Successful fallback indexing on `/Users/amuldotexe/Desktop/personal-repos-lane/nostd-union-find`; stats reported 11 files, 39 functions, 1 struct, 1 enum, 5 modules.
  - Used as required navigational evidence, not as proof of storage-engine behavior.
- Direct source evidence commands used repeatedly:
  - `rg -n "pattern" <repo paths>`
  - `nl -ba <file> | sed -n '<start>,<end>p'`
  - `find <repo> -maxdepth ...`
  - `git status --short`

Representative direct reads include:

```bash
nl -ba gitrefrepo/redb-src/docs/design.md | sed -n '68,99p'
nl -ba gitrefrepo/redb-src/src/tree_store/page_store/cached_file.rs | sed -n '200,239p'
nl -ba gitrefrepo/qdrant-src/lib/wal/src/segment.rs | sed -n '47,89p'
nl -ba gitrefrepo/rocksdb-src/include/rocksdb/table.h | sed -n '229,328p'
nl -ba gitrefrepo/sled-src/fuzz/fuzz_targets/fuzz_model.rs | sed -n '15,145p'
nl -ba /Users/amuldotexe/Desktop/reference-repos-yard/kani/docs/src/reference/attributes.md | sed -n '110,151p'
```

## Page and Cache Design

### Pattern: Single-Writer MVCC with Copy-On-Write Pages

Source path:

- `gitrefrepo/redb-src/docs/design.md:5-25`

Source evidence:

- redb is an ACID embedded key-value store with a single writer and multiple concurrent readers.
- It uses MVCC and serializable isolation, with writes applied sequentially.
- Its logical file contains metadata, pending free tree, table tree, and per-table data trees.
- Everything except metadata is copy-on-write.

Pseudocode:

```rust
struct GraphStore {
    header: DatabaseHeader,
    pending_free_tree: BTreeMap<TransactionId, PageList>,
    schema_tree: BTreeMap<TableName, TableDefinition>,
    node_tree: CowBtree<NodeId, NodeRecord>,
    edge_tree: CowBtree<EdgeKey, EdgeRecord>,
}

fn commit_graph_write_slot(
    old_root: RootSet,
    dirty_pages: DirtyPageSet,
) -> Result<RootSet, StoreError> {
    let new_root = write_copy_on_write_pages(dirty_pages)?;
    publish_commit_root_atomically(old_root, new_root)?;
    Ok(new_root)
}
```

Rust translation:

- Make read transactions hold an immutable root generation.
- Make write transactions allocate new pages and publish a new root only at commit.
- Store freed pages under the freeing transaction id, not immediately in the global free pool.

Memory and performance implications:

- Readers do not copy the entire graph; they pin root pages and old page generations.
- Writer memory is bounded by dirty pages plus metadata deltas.
- Pending free trees are extra metadata, but they prevent use-after-free under concurrent readers.

Concurrency implications:

- This pattern gives snapshot readers without reader-writer contention on graph pages.
- It still requires a single write commit path or a transaction serialization layer.

Testing implications:

- Add a racing-reader test where a reader starts during commit fsync, then a later writer reuses pages, and the old reader still sees the original value.
- redb has such a test in `tests/integration_tests.rs:542-620`.

Agentic coding guidance:

- Start with read root handles and page generations before optimizing.
- Do not let a read-only tree accept the full mutable page manager. redb uses `PageResolver` as a read-only view so B-tree readers cannot bypass allocation tracking (`page_manager.rs:84-91`).
- Name APIs by the lifetime they protect, for example `pin_snapshot_root_guard`, `allocate_cow_graph_page`, and `retire_reader_pages_after`.

### Pattern: Dynamic Read/Write Cache Partitioning

Source path:

- `gitrefrepo/redb-src/src/tree_store/page_store/cached_file.rs:200-239`

Source evidence:

- `PagedCachedFile` tracks read-cache bytes and write-buffer bytes separately with atomic counters.
- The write buffer never exceeds 50 percent of maximum cache size.
- Write buffering evicts read cache only when write is under 50 percent and read is over 50 percent.
- Read plus write cache never exceeds the configured maximum.
- Read cache can use 100 percent when no writes are in progress, while write-heavy workloads cannot starve readers below 50 percent.

Pseudocode:

```rust
struct PageCacheBudget {
    read_bytes: AtomicUsize,
    write_bytes: AtomicUsize,
    max_bytes: usize,
}

fn admit_dirty_page_cache(
    budget: &PageCacheBudget,
    page_len: usize,
) -> CacheAdmission {
    let half = budget.max_bytes / 2;
    if budget.write_bytes.load(Relaxed) + page_len > half {
        return CacheAdmission::FlushNow;
    }
    if budget.read_bytes.load(Relaxed) > half {
        evict_read_cache_page();
    }
    CacheAdmission::BufferDirty
}
```

Rust translation:

- Keep a read cache of immutable `Arc<[u8]>` pages.
- Keep a separate dirty write buffer guarded by a mutex or transaction-local owner.
- Use soft atomic accounting, not a global total counter on every cache change.

Memory and performance implications:

- Prevents write bursts from consuming all memory.
- Allows high read cache utilization during read-heavy graph traversals.
- Avoids one global contended counter per insert/remove.

Concurrency implications:

- Stripe the read cache with `RwLock`s.
- Keep lock ordering explicit. redb documents write-buffer before read-cache locks elsewhere in the same file.

Testing implications:

- Stress with mixed read/write workloads and assert budget invariants with fuzzed page sizes.
- Add a histogram for evictions by cause: dirty pressure, read pressure, explicit invalidation.

Agentic coding guidance:

- Treat cache budget rules as executable invariants, not comments.
- Build tests first:
  - WHEN dirty pages exceed half the cache, THEN the system SHALL flush instead of buffering.
  - WHEN no writes are active, THEN read cache SHALL be allowed to fill the full budget.

### Pattern: Lowest-Page Allocation for Shrinkable Files

Source paths:

- `gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs:70-81`
- `gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs:1185-1213`

Source evidence:

- redb has `AllocationPolicy::Default` and `AllocationPolicy::Lowest`.
- `Lowest` is more expensive but keeps trailing pages free so `try_shrink()` can reclaim grown space.
- `try_shrink()` checks trailing free pages in the last region and reduces the last region when enough trailing space exists.

Pseudocode:

```rust
enum PageAllocationPolicy {
    FastFit,
    LowestPage,
}

fn allocate_graph_page_lowest(
    regions: &mut RegionMap,
    order: PageOrder,
) -> Option<PageNumber> {
    regions
        .iter_mut_by_lowest_page()
        .find_map(|region| region.allocate_lowest_order(order))
}

fn shrink_trailing_region_pages(state: &mut FileLayout) -> bool {
    let trailing = state.last_region().trailing_free_pages();
    if trailing < state.last_region().len() / 2 {
        return false;
    }
    state.reduce_last_region(trailing / 2);
    true
}
```

Rust translation:

- Add an allocation policy parameter to compaction, bulk delete, and graph import cleanup paths.
- Use fast allocation for normal writes; use lowest-page allocation after file growth or before compaction.

Memory and performance implications:

- Lowest-page allocation costs more CPU but can return disk space and reduce mmap footprint.
- This matters for graph stores where import/delete workflows can create large transient adjacency pages.

Concurrency implications:

- Requires a synchronized allocator state.
- Do not shrink while readers can hold pages above the new end-of-file.

Testing implications:

- Insert enough data to grow the file, delete most of it, perform lowest-page allocation, then assert file length shrinks.
- Keep a reader alive during the shrink attempt and assert shrink is deferred.

Agentic coding guidance:

- Do not implement shrink as a separate cleanup script first. Make allocation policy a first-class storage option so tests can force it.
- Keep policy names behavioral: `allocate_lowest_page_first`, `shrink_trailing_region_pages`.

### Pattern: Packed Buddy Bitmaps for Free Space

Source paths:

- `gitrefrepo/redb-src/docs/design.md:149-164`
- `gitrefrepo/redb-src/src/tree_store/page_store/buddy_allocator.rs:23-67`

Source evidence:

- redb stores free page orders per region as `BtreeBitmap`s.
- A `BtreeBitmap` is a 64-way tree packed into `u64` values.
- The buddy allocator supports dynamically sized pages up to `page_size * 2^max_order`.
- A page is marked free at only one order, and it must be the largest order.

Pseudocode:

```rust
struct BuddyRegion {
    free_by_order: Vec<PackedBitmap>,
    pages: u32,
    max_order: u8,
}

fn initialize_free_orders_only_largest(
    pages: u32,
    max_order: u8,
) -> BuddyRegion {
    let mut region = BuddyRegion::new(pages, max_order);
    let mut accounted = 0;
    for order in (0..=max_order).rev() {
        let span = 1u32 << order;
        while accounted + span <= pages {
            region.mark_free(order, accounted / span);
            accounted += span;
        }
    }
    region
}
```

Rust translation:

- Use packed bitsets for page-free metadata instead of `HashSet<PageId>` or `Vec<PageState>`.
- Store one free marker per maximal buddy range.
- Keep debug-only expensive consistency checks behind `debug_assertions`.

Memory and performance implications:

- Big RAM win for large files: allocator metadata becomes bit-packed and hierarchical.
- Largest-order representation minimizes duplicated metadata.

Concurrency implications:

- Allocator mutation still needs a lock or transactional owner.
- Readers should never mutate allocator state; use a read-only resolver.

Testing implications:

- Fuzz allocate/free sequences and assert total free + allocated pages equals region capacity.
- Add Kani harnesses for small region sizes and bounded orders.

Agentic coding guidance:

- Do not translate this as a generic slab allocator. It is page allocator metadata.
- Preserve the invariant in code names and tests: `mark_largest_free_order_only`.

## mmap and Zero-Copy IO

### Pattern: Pinned Engine-Owned Slices with Rust Lifetimes

Source paths:

- `gitrefrepo/rust-rocksdb-src/src/db_pinnable_slice.rs:21-73`
- `gitrefrepo/rust-rocksdb-src/src/db.rs:1026-1097` was also inspected for `get_pinned_opt` call flow.

Source evidence:

- `DBPinnableSlice<'a>` wraps a RocksDB raw `PinnableSlice` pointer.
- `PhantomData<&'a DB>` ties the slice lifetime to the database.
- `Deref` exposes `&[u8]` from raw pointer and length.
- `Drop` destroys the RocksDB pinnable slice.
- `from_c` is unsafe and requires the pointer to come from `rocksdb_get_pinned`.

Pseudocode:

```rust
pub struct PinnedGraphValue<'db> {
    ptr: NonNull<EnginePinnedValue>,
    _db: PhantomData<&'db GraphDb>,
}

impl Deref for PinnedGraphValue<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { engine_pinned_value_as_slice(self.ptr) }
    }
}

impl Drop for PinnedGraphValue<'_> {
    fn drop(&mut self) {
        unsafe { engine_pinned_value_destroy(self.ptr) }
    }
}
```

Rust translation:

- Return `PinnedNodeRecord<'txn>` or `PinnedAdjacencySlice<'snapshot>` for hot reads.
- Offer `to_owned()` or `copy_graph_value_to_vec` for long-lived values.
- Never expose raw mmap pointers outside the wrapper.

Memory and performance implications:

- Avoids copying node records and adjacency lists for short-lived traversals.
- Long-lived pinned values can keep cache pages alive, so callers need visible ownership tradeoffs.

Concurrency implications:

- `unsafe impl Send/Sync` must be justified by the backing engine's guarantees.
- Prefer not to make pinned writable slices `Sync`.

Testing implications:

- Compile-fail tests should prove a pinned value cannot outlive its database, snapshot, transaction, or cache guard.
- Run Miri on all pure-Rust unsafe wrappers that do not depend on C++ FFI.

Agentic coding guidance:

- Always write a `# Safety` comment for `from_raw_*` constructors.
- Use lifetimes to encode ownership instead of documentation only.
- Make copy-vs-pin visible in method names: `get_node_value_owned` versus `get_node_value_pinned`.

### Pattern: Conditional Zero-Copy with Explicit Buffer Provenance

Source paths:

- `gitrefrepo/rocksdb-src/file/random_access_file_reader.h:156-177`
- `gitrefrepo/rocksdb-src/table/block_fetcher.cc:152-176`
- `gitrefrepo/rocksdb-src/table/block_fetcher.cc:225-290`

Source evidence:

- RocksDB documents different read result ownership for mmap, non-mmap scratch buffers, and direct I/O aligned buffers.
- Small blocks may use stack buffers to avoid heap allocation, but final returned content may need a heap copy if the lifetime would be wrong.
- `BlockFetcher::GetBlockContents()` branches based on whether the returned slice points into mmap/prefetch/stack/heap/compressed/direct I/O buffers.

Pseudocode:

```rust
enum ReadBufferOrigin<'a> {
    MmapBorrow(&'a [u8]),
    StackScratch(Vec<u8>),
    HeapOwned(Box<[u8]>),
    DirectIoAligned(AlignedBuffer),
    PrefetchBorrow(&'a [u8]),
}

fn finalize_block_contents(
    origin: ReadBufferOrigin<'_>,
    compression: CompressionState,
) -> BlockBytes {
    match origin {
        ReadBufferOrigin::MmapBorrow(bytes) if compression.is_none() => {
            BlockBytes::Borrowed(bytes)
        }
        ReadBufferOrigin::StackScratch(bytes) => BlockBytes::Owned(bytes.into_boxed_slice()),
        ReadBufferOrigin::DirectIoAligned(buf) if compression.is_none() => {
            BlockBytes::Owned(buf.copy_to_box())
        }
        other => decompress_or_copy_block(other),
    }
}
```

Rust translation:

- Model buffer provenance as an enum.
- Only return borrowed bytes when the backing storage lifetime is tied to a snapshot or file handle.
- Use owned bytes for stack scratch, direct I/O alignment buffers, or decompressed blocks.

Memory and performance implications:

- Avoids accidental copies on mmap paths.
- Prevents returning borrowed stack/direct-I/O scratch.
- Lets hot block paths choose stack allocation for small temporary reads.

Concurrency implications:

- Borrowed mmap bytes are safe only if the underlying file is immutable or protected by snapshot generation.
- Writable mmap plus shared immutable slice is a soundness hazard.

Testing implications:

- Unit-test all provenance transitions.
- Miri-test pure Rust wrappers.
- Fuzz small block sizes around stack-buffer thresholds and compression boundaries.

Agentic coding guidance:

- Do not call every borrowed byte path "zero-copy." Add an origin enum first.
- Any function returning `&[u8]` from storage must name the owner in its type or lifetime.

### Pattern: Mmap View Slicing with Unsafe Encapsulation

Source paths:

- `gitrefrepo/qdrant-src/lib/wal/src/mmap_view_sync.rs:15-75`
- `gitrefrepo/qdrant-src/lib/wal/src/mmap_view_sync.rs:75-190`

Source evidence:

- `MmapViewSync` stores `Arc<UnsafeCell<MmapMut>>`, offset, and length.
- `split_at` and `restrict` create bounded subviews.
- `inner()` and `inner_mut()` are private and document unsafety around concurrent modification and range access.
- `as_slice`, `as_mut_slice`, and `clone` are unsafe and require caller guarantees.
- `Send` and `Sync` impls are only compiled for tests in the inspected version.

Pseudocode:

```rust
pub struct MmapGraphView {
    mapping: Arc<UnsafeCell<MmapMut>>,
    offset: usize,
    len: usize,
}

impl MmapGraphView {
    pub fn restrict_graph_page_view(
        &mut self,
        offset: usize,
        len: usize,
    ) -> Result<(), StoreError> {
        if offset + len > self.len {
            return Err(StoreError::OutOfBoundsView);
        }
        self.offset += offset;
        self.len = len;
        Ok(())
    }

    pub unsafe fn as_graph_bytes_slice(&self) -> &[u8] {
        &(*self.mapping.get())[self.offset..self.offset + self.len]
    }
}
```

Rust translation:

- Hide `UnsafeCell<MmapMut>` behind a small view type.
- Make range restriction safe and byte access unsafe only when aliasing cannot be guaranteed by type state.
- Prefer immutable mmap for committed segments and mutable mmap only for writer-owned WAL segments.

Memory and performance implications:

- Slicing mmap views avoids allocating `Vec<u8>` for WAL entries.
- `close()`/`DontNeed` style hints can release old WAL segment pages from RSS.

Concurrency implications:

- Do not implement `Send`/`Sync` for mutable mmap views until aliasing rules are proven.
- Split immutable and mutable views into different types if possible.

Testing implications:

- Miri cannot fully validate OS mmap behavior, but it can test the pure range arithmetic and unsafe wrappers over anonymous memory where applicable.
- Add stress tests with concurrent readers only after immutable view types exist.

Agentic coding guidance:

- Encapsulate all mmap unsafety in one module.
- Use typestates: `MutableWalMap`, `FrozenSegmentMap`, `ReadOnlyGraphMap`.

### Pattern: Single-Page Borrow, Multi-Page Assemble

Source paths:

- `gitrefrepo/qdrant-src/lib/gridstore/src/pages.rs:54-101`
- `gitrefrepo/qdrant-src/lib/gridstore/src/pages.rs:210-326`

Source evidence:

- Qdrant page files are opened with `Populate::No` and random access advice.
- Page ranges for a value usually contain one entry, or two entries if the value spans pages.
- `read_from_pages` avoids allocation for single-page values.
- Multi-page values allocate `Vec<MaybeUninit<u8>>` because chunks must be assembled.
- Batch reads use a pending map only for multi-page values; single-page reads call the callback directly.

Pseudocode:

```rust
fn read_adjacency_value_pages<'a>(
    pointer: ValuePointer,
    pages: &'a PageFiles,
) -> Result<Cow<'a, [u8]>, StoreError> {
    let ranges = compute_pointer_page_ranges(pointer);
    if ranges.len() == 1 {
        return pages.read_borrowed_range(ranges[0]);
    }

    let mut out = Vec::<MaybeUninit<u8>>::with_capacity(pointer.length as usize);
    assemble_ranges_into_buffer(&mut out, ranges)?;
    Ok(Cow::Owned(unsafe { assume_init_vec(out) }))
}
```

Rust translation:

- Store graph payloads so the common case fits in one page.
- Only allocate for rare cross-page adjacency values.
- Use `Cow<[u8]>` at internal APIs so callers can be agnostic to borrowed versus owned.

Memory and performance implications:

- Lowers RAM and allocation rate for typical node/edge property reads.
- Cross-page values still pay an allocation, but only proportional to value size.

Concurrency implications:

- Borrowed single-page data must be protected by a snapshot/page guard.
- Multi-page assembly must handle interleaved async I/O completions.

Testing implications:

- Boundary tests for value length 0, exactly page end, page end plus 1, and multi-page.
- Batch-read tests where completions arrive out of order.

Agentic coding guidance:

- First design `ValuePointer` and page-range math.
- Do not optimize multi-page reads until the single-page borrowed path is correct.

## WAL, Recovery, and Checkpointing

### Pattern: Double Commit Slots and a Single Atomic Selector Byte

Source paths:

- `gitrefrepo/redb-src/docs/design.md:68-99`
- `gitrefrepo/redb-src/src/transactions.rs:1368-1420`

Source evidence:

- redb uses a 512-byte super-header with two commit slots.
- A single "god byte" chooses the primary commit slot and stores recovery/two-phase flags.
- redb relies on that byte for atomic commits.
- Normal commit writes inactive slot, flips the byte, then fsyncs.
- Two-phase commit writes inactive slot, fsyncs, flips the byte, then fsyncs again.
- Quick repair saves allocator state in each commit and enables two-phase commit.

Pseudocode:

```rust
struct SuperHeader {
    god_byte: AtomicCommitByte,
    slots: [CommitSlot; 2],
}

fn commit_header_slot_atomic(
    header: &mut SuperHeader,
    new_roots: RootSet,
    mode: CommitMode,
) -> Result<(), StoreError> {
    let next = header.inactive_slot();
    header.slots[next].write_with_checksum(new_roots)?;
    if mode.requires_pre_flip_sync() {
        fsync_database_file()?;
    }
    header.god_byte.flip_primary_bit(next);
    fsync_database_file()?;
    Ok(())
}
```

Rust translation:

- Use two durable root slots for graph metadata and root pointers.
- Store checksum, transaction id, roots, and allocator metadata in the slot.
- Treat the selector byte as the only atomic publish point.

Memory and performance implications:

- Very low metadata RAM.
- Two-phase commit costs extra fsync but reduces recovery ambiguity.
- Quick repair increases commit work but avoids full allocator reconstruction after crash.

Concurrency implications:

- Single writer simplifies commit-slot updates.
- Readers should see either old or new root, never half-updated roots.

Testing implications:

- Fault-inject crash after inactive slot write, after first fsync, after byte flip, and during final fsync.
- Verify reopening selects latest valid slot or repairs.

Agentic coding guidance:

- Make crash points explicit enum values in tests.
- Avoid a "current root file" rewrite scheme until the atomic publish primitive is designed.

### Pattern: Allocator-State Quick Repair and Full Repair Fallback

Source paths:

- `gitrefrepo/redb-src/docs/design.md:149-153`
- `gitrefrepo/redb-src/src/db.rs:569-590`
- `gitrefrepo/redb-src/src/db.rs:822-912`
- `gitrefrepo/redb-src/src/transactions.rs:1408-1420`

Source evidence:

- redb can store region tracker state at clean shutdown or in an allocator state table for quick repair.
- `check_integrity()` notes normal crash recovery is automatic and slow repair should be exceptional.
- `do_repair()` verifies primary checksums, rolls back corrupted primary if possible, scans data/system trees, marks allocated pages, visits freed trees, and ends repair.
- Quick repair stores allocator state per commit and enables two-phase commit, making crash recovery nearly instant at the cost of slower commits.

Pseudocode:

```rust
fn recover_allocator_state_fast(
    header: &DatabaseHeader,
    system_tree: &SystemTree,
) -> Result<AllocatorState, StoreError> {
    if let Some(state) = system_tree.load_allocator_state(header.tx_id)? {
        return Ok(state);
    }
    rebuild_allocator_by_scanning_roots(header.roots())
}

fn rebuild_allocator_by_scanning_roots(
    roots: RootSet,
) -> Result<AllocatorState, StoreError> {
    let mut allocator = AllocatorState::empty();
    visit_all_graph_pages(roots.node_root, |page| allocator.mark_allocated(page))?;
    visit_all_graph_pages(roots.edge_root, |page| allocator.mark_allocated(page))?;
    visit_freed_page_trees(|page| allocator.mark_allocated(page))?;
    Ok(allocator)
}
```

Rust translation:

- Keep a compact allocator-state table in the system metadata.
- Fall back to full page graph scan if allocator state is absent or corrupted.
- Expose a repair session callback for progress and abort.

Memory and performance implications:

- Quick repair trades commit bytes and fsync work for fast startup after crash.
- Full repair can be bounded by page count, not graph record count, if page roots are explicit.

Concurrency implications:

- Full repair must require exclusive access: no active read/write transactions.
- redb returns `TransactionInProgress` if integrity repair is requested with live transactions.

Testing implications:

- Crash with and without quick repair.
- Corrupt allocator-state table and verify full scan fallback.
- Abort repair callback and verify open fails cleanly.

Agentic coding guidance:

- Build the slow full repair before trusting quick repair.
- Add a `repair_allocator_from_roots` test oracle before optimizing.

### Pattern: WAL Entries with Seeded CRC and Tail Truncation

Source path:

- `gitrefrepo/qdrant-src/lib/wal/src/segment.rs:47-89`
- `gitrefrepo/qdrant-src/lib/wal/src/segment.rs:246-289`
- `gitrefrepo/qdrant-src/lib/wal/src/segment.rs:387-447`

Source evidence:

- Qdrant WAL segment format includes magic bytes, version, random CRC seed, entry length, data, padding, and CRC32-C.
- The random seed ensures old entries in reused segments are ignored.
- Opening a segment parses entries until CRC mismatch; the remaining bytes are considered empty.
- Truncation drains index entries, recalculates CRC, and zeroes the deleted region so crash does not resurrect old entries.
- Flush only flushes the range since the previous flush offset.

Pseudocode:

```rust
struct WalSegmentHeader {
    magic: [u8; 3],
    version: u8,
    crc_seed: u32,
}

fn parse_wal_segment_index(
    bytes: &[u8],
    seed: u32,
) -> Vec<EntryOffset> {
    let mut crc = Crc32c::new(seed);
    let mut out = Vec::new();
    while let Some(entry) = read_next_entry(bytes) {
        if !entry.crc_matches(&mut crc) {
            break;
        }
        out.push(entry.offset_len());
    }
    out
}

fn truncate_wal_suffix_zeroed(
    segment: &mut WalSegment,
    keep_entries: usize,
) -> Result<(), StoreError> {
    let suffix = segment.index.drain(keep_entries..);
    segment.recompute_crc_prefix();
    segment.zero_entry_region(suffix.byte_range());
    segment.flush_range_from_last_flush()?;
    Ok(())
}
```

Rust translation:

- Use a random generation/CRC seed when recycling WAL files.
- Treat CRC mismatch as logical end-of-log, not necessarily fatal.
- Zero truncated suffixes before exposing the segment for reuse.

Memory and performance implications:

- WAL index can be a `Vec<(offset, len)>`, not full entry data.
- Range flush lowers fsync pressure.

Concurrency implications:

- Appends need a single segment writer.
- Readers can clone restricted mmap views only after entry index is stable.

Testing implications:

- Corrupt entry length, entry payload, CRC, and padding.
- Reuse an old segment with a different seed and verify old entries are ignored.
- Crash after truncate metadata update but before zeroing, and after zeroing but before flush.

Agentic coding guidance:

- Build WAL parsing as a pure function over bytes before wiring file I/O.
- Store an explicit `flush_offset` and test it.

### Pattern: Fixed Ring Index over Append-Only Prepare Journal

Source paths:

- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/journal/src/prepare_journal.rs:35-130`
- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/journal/src/prepare_journal.rs:122-255`
- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/journal/src/prepare_journal.rs:375-421`

Source evidence:

- Iggy prepare journal uses `MAX_ENTRY_SIZE` to prevent bit-flipped size fields from causing huge allocations.
- `SLOT_COUNT` is a fixed-size journal ring buffer indexed by `op % SLOT_COUNT`.
- The slot count must exceed entries between snapshots; otherwise old unsnapshotted entries become unreachable.
- `open()` scans the WAL forward, rebuilds the index, and truncates corrupt or truncated tail entries.
- `snapshot_op` prevents appending entries that would evict unsnapshotted entries.
- Compaction writes a temp WAL, syncs it, renames atomically, fsyncs parent directory, reopens storage, and rebuilds offsets.

Pseudocode:

```rust
const PREPARE_SLOT_COUNT: usize = 1024;
const MAX_PREPARE_ENTRY_BYTES: u64 = 64 * 1024 * 1024;

struct PrepareWalIndex {
    headers: Vec<Option<PrepareHeader>>,
    offsets: Vec<Option<u64>>,
    snapshot_op: u64,
}

fn rebuild_prepare_index_scan(
    wal: &mut WalFile,
    snapshot_op: u64,
) -> Result<PrepareWalIndex, StoreError> {
    let mut index = PrepareWalIndex::empty(snapshot_op);
    let mut pos = 0;
    while let Some(header) = wal.read_header(pos)? {
        if header.size < HEADER_SIZE || header.size > MAX_PREPARE_ENTRY_BYTES {
            wal.truncate(pos)?;
            break;
        }
        if pos + header.size > wal.len() {
            wal.truncate(pos)?;
            break;
        }
        index.insert(header.op % PREPARE_SLOT_COUNT, header, pos);
        pos += header.size;
    }
    Ok(index)
}
```

Rust translation:

- Use this pattern for graph transaction prepare records or Raft-like commit metadata.
- Keep a snapshot watermark to make ring overwrites explicit.
- Bound every size field before allocation.

Memory and performance implications:

- In-memory journal index is fixed size.
- Startup scan is sequential and tail-truncating.
- Large WAL files still need compaction or snapshots.

Concurrency implications:

- `RefCell`/`Cell` in Iggy imply single-threaded or externally synchronized use. A graph store should prefer `Mutex`/writer-owned state for multi-thread use.

Testing implications:

- Generate WALs with truncated headers, oversized sizes, duplicate ops, and ring collisions.
- Verify appending panics/errors before overwriting unsnapshotted slots.

Agentic coding guidance:

- Never trust length fields from disk.
- Add `MAX_ENTRY_SIZE` before writing the parser.

### Pattern: Checksum-Terminated Journal Batches and Persist Modes

Source paths:

- `gitrefrepo/fjall-src/README.md:116-148`
- `gitrefrepo/fjall-src/src/journal/writer.rs:33-45`
- `gitrefrepo/fjall-src/src/journal/writer.rs:240-325`
- `gitrefrepo/fjall-src/src/journal/manager.rs:94-155`
- `gitrefrepo/fjall-src/src/db.rs:332-365`
- `gitrefrepo/fjall-src/src/db.rs:579-690`

Source evidence:

- Fjall docs distinguish flushing to OS buffers from syncing to disk.
- `PersistMode::Buffer` is not guaranteed against power loss or OS crash; sync modes use `fdatasync` or stronger sync.
- Journal writes emit a start marker, item payload, and end marker containing an xxh3 checksum.
- Journal manager deletes old journals only after every keyspace's flushed table sequence number is high enough; recovery must flush journals oldest to newest.
- `Database::persist()` says persistence affects durability, not consistency; without flushing, data is crash-safe.
- Database open recovers active and sealed journals, recovers keyspaces, then ensures active memtables are empty after recovery.

Pseudocode:

```rust
enum PersistMode {
    BufferOnly,
    DataSync,
    SyncAll,
}

fn write_journal_batch_checked(
    journal: &mut JournalWriter,
    seqno: SeqNo,
    items: &[JournalItem],
) -> Result<(), StoreError> {
    journal.write_start(items.len(), seqno)?;
    let mut checksum = Xxh3::default();
    for item in items {
        let bytes = encode_item(item)?;
        journal.write_all(&bytes)?;
        checksum.update(&bytes);
    }
    journal.write_end(checksum.finish())?;
    Ok(())
}

fn evict_flushed_journals_fifo(
    journals: &mut JournalManager,
    keyspaces: &[Keyspace],
) -> Result<(), StoreError> {
    while journals.oldest_is_flushed_by_all_keyspaces(keyspaces) {
        journals.remove_oldest()?;
    }
    Ok(())
}
```

Rust translation:

- Separate consistency from durability in the API.
- Use checksum-terminated batches so torn writes are detected at batch boundaries.
- Evict WALs only when all dependent flushed structures have durable sequence numbers.

Memory and performance implications:

- Buffer-only persistence is faster but not power-loss durable.
- Per-keyspace watermarks prevent keeping all old journals forever.

Concurrency implications:

- Flush queues must preserve FIFO if journal eviction relies on sequence comparisons.
- Poison the database after fatal flush failure, as Fjall does through `is_poisoned`.

Testing implications:

- Truncate after start marker, after payload, after checksum.
- Simulate keyspace lag and verify old journal is retained.
- Recover sealed journals oldest-to-newest.

Agentic coding guidance:

- In API docs, avoid saying "persist" means durable unless the mode actually calls sync.
- Name durability modes explicitly: `persist_buffer_only`, `persist_datasync`, `persist_sync_all`.

## LSM, B-Tree, and Index Design

### Pattern: Metadata Cache Is a First-Class Memory Budget

Source paths:

- `gitrefrepo/rocksdb-src/include/rocksdb/table.h:176-205`
- `gitrefrepo/rocksdb-src/include/rocksdb/table.h:229-328`

Source evidence:

- RocksDB can cache index and filter blocks with high priority.
- L0 filter/index blocks can be pinned by table readers.
- Metadata pinning can reduce contention, but excessive pinning can overflow block cache.
- Two-level indexes keep second-level partitions in the block cache.
- `kBinarySearchWithFirstKey` can reduce read amplification for short range scans, but makes the index 2x or more larger.
- Data block hash indexes trade memory for faster in-block lookup.
- Block cache can be disabled or externally supplied, and dynamic changes should go through the cache object.

Pseudocode:

```rust
struct GraphIndexCachePolicy {
    cache_filters: bool,
    pin_hot_level_filters: bool,
    index_shape: IndexShape,
    data_block_lookup: DataBlockLookup,
    cache_capacity: usize,
}

enum IndexShape {
    BinarySearch,
    PrefixHash,
    TwoLevelPartitioned,
    BinaryWithFirstKey,
}
```

Rust translation:

- Treat node/edge secondary index metadata as cacheable pages, not always-resident heap structures.
- Use partitioned/two-level indexes for huge label/property indexes.
- Pin only small/hot metadata such as current memtable filters or top-level partition indexes.

Memory and performance implications:

- Lower RAM by making metadata eviction explicit.
- Avoid over-pinning per-label/per-relationship-type indexes.
- First-key indexes may help short range scans but can double index memory.

Concurrency implications:

- Pinned metadata held by iterators must count against cache usage.
- Avoid global index locks by caching immutable partition blocks.

Testing implications:

- Measure cache usage split among data, index, and filters.
- Add tests for long scans and short range scans across label/property indexes.

Agentic coding guidance:

- Require every new index to declare cache policy and expected resident metadata.
- Do not hardcode "pin all filters" because it passes benchmarks on small graphs.

### Pattern: Bloom/Ribbon Filter Choice by Level and Comparator Semantics

Source path:

- `gitrefrepo/rocksdb-src/include/rocksdb/filter_policy.h:159-181`

Source evidence:

- If a comparator ignores parts of keys, a filter policy must ignore the same parts.
- Ribbon filters save about 30 percent space compared with Bloom filters at similar query time, but cost more CPU and temporary construction memory.
- RocksDB suggests Ribbon for lower/larger/longer-lived LSM levels and Bloom for highest levels.

Pseudocode:

```rust
enum GraphFilterPolicy {
    Bloom { bits_per_key: f64 },
    Ribbon { bloom_equivalent_bits: f64 },
    PrefixAwareBloom { key_projection: KeyProjection },
}

fn choose_filter_policy_level(
    level: usize,
    key_semantics: GraphKeySemantics,
) -> GraphFilterPolicy {
    if key_semantics.ignores_suffix() {
        return GraphFilterPolicy::PrefixAwareBloom {
            key_projection: key_semantics.filter_projection(),
        };
    }
    if level <= 1 {
        GraphFilterPolicy::Bloom { bits_per_key: 10.0 }
    } else {
        GraphFilterPolicy::Ribbon { bloom_equivalent_bits: 10.0 }
    }
}
```

Rust translation:

- For graph keys like `(label, property, node_id)` or `(src, type, dst)`, the filter must match comparator semantics.
- Use Bloom filters for hot mutable levels and Ribbon-like compact filters for cold immutable levels if construction memory is acceptable.

Memory and performance implications:

- Compact filters can reduce RAM/disk for large cold graph indexes.
- Construction-time memory must be budgeted during compaction.

Concurrency implications:

- Compaction builds filters; readers should see immutable completed filters only.

Testing implications:

- Comparator/filter consistency tests are mandatory.
- Generate keys with ignored suffixes and assert no false negatives.

Agentic coding guidance:

- Do not add a generic Bloom filter to custom key comparators.
- Write `project_filter_key_from_graph_key` first and test it.

### Pattern: Immutable FileSlice plus Small Decompressed Block Cache

Source paths:

- `gitrefrepo/tantivy-src/src/store/reader.rs:63-98`
- `gitrefrepo/tantivy-src/src/store/reader.rs:152-228`

Source evidence:

- Tantivy `StoreReader` stores a data `FileSlice`, an `Arc<SkipIndex>`, space usage, and `BlockCache`.
- `BlockCache` is an optional mutex-guarded LRU of decompressed `OwnedBytes`, keyed by block byte-range start.
- Opening the reader extracts a footer, splits data from offset index, reads the index, and opens a skip index.
- `read_block` checks the cache, reads a compressed block from `FileSlice`, decompresses to `OwnedBytes`, caches a clone, and returns it.

Pseudocode:

```rust
struct ImmutableGraphSegmentReader {
    data: FileSlice,
    offset_index: Arc<SkipIndex>,
    decompressed_blocks: Option<Mutex<LruCache<u64, OwnedBytes>>>,
}

fn read_compressed_graph_block(
    reader: &ImmutableGraphSegmentReader,
    block: Checkpoint,
) -> io::Result<OwnedBytes> {
    let key = block.byte_range.start;
    if let Some(bytes) = reader.cache_get(key) {
        return Ok(bytes);
    }
    let compressed = reader.data.slice(block.byte_range).read_bytes()?;
    let decoded = OwnedBytes::new(decompress_graph_block(compressed.as_ref())?);
    reader.cache_put(key, decoded.clone());
    Ok(decoded)
}
```

Rust translation:

- Store cold graph snapshots as immutable segment files.
- Keep offset/skip index resident or hot-cached.
- Cache decompressed adjacency/property blocks in bounded LRU.

Memory and performance implications:

- Cold segment bytes are file-backed.
- RAM is spent on skip indexes and recently decompressed blocks only.
- `OwnedBytes` clone is cheap if it shares backing storage.

Concurrency implications:

- Immutable readers are shareable across threads.
- LRU mutex can be sharded if contention appears.

Testing implications:

- Verify cache hit/miss counters.
- Corrupt footer or offset index and assert data corruption errors, not panics.

Agentic coding guidance:

- Favor immutable segments for historical graph snapshots and analytical indexes.
- Do not keep all decompressed adjacency blocks resident.

### Pattern: FST Index over OwnedBytes

Source path:

- `gitrefrepo/tantivy-src/sstable/src/index/v3.rs:14-60`

Source evidence:

- `SSTableIndexV3` stores an `Arc<Map<OwnedBytes>>` FST index and a block address store.
- Loading splits one `OwnedBytes` buffer into FST bytes and block-address-store bytes.
- `locate_with_key()` streams the FST range from `ge(key)` to locate the candidate block.

Pseudocode:

```rust
struct GraphSstableIndex {
    fst: Arc<FstMap<OwnedBytes>>,
    blocks: BlockAddressStore,
}

fn locate_graph_key_block(
    index: &GraphSstableIndex,
    key: &[u8],
) -> Option<BlockAddress> {
    let block_id = index.fst.range().ge(key).into_stream().next()?.1;
    index.blocks.get(block_id)
}
```

Rust translation:

- Use FST or compact trie-style indexes for immutable string-heavy label/property keys.
- Keep the FST backed by an `OwnedBytes` slice instead of expanding into `HashMap<String, ...>`.

Memory and performance implications:

- Excellent for many repeated prefixes, labels, relationship types, and property names.
- Avoids heap allocation per key.

Concurrency implications:

- Immutable FST can be shared behind `Arc`.

Testing implications:

- Golden tests for lexicographic boundary keys.
- Fuzz split lengths and corrupted index bytes.

Agentic coding guidance:

- Translate graph dictionary indexes to immutable byte-backed structures before reaching for hash maps.

### Pattern: Bundled Split Footer and Hot Range Cache

Source paths:

- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit/quickwit/quickwit-directories/src/bundle_directory.rs:46-132`
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit/quickwit/quickwit-directories/src/hot_directory.rs:174-276`
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit/quickwit/quickwit-directories/src/hot_directory.rs:377-392`
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit/quickwit/quickwit-indexing/src/split_store/indexing_split_store.rs:183-200`

Source evidence:

- Quickwit split footer stores bundle metadata length and hotcache length at the end of the split.
- `read_split_footer()` reads only tail byte ranges to locate the footer and hotcache.
- `StaticDirectoryCache` opens from `OwnedBytes`, slices subranges, and maps file paths to static slice caches.
- `FileSliceWithCache::read_bytes()` checks the static cache first, then falls back to the underlying `FileSlice`.
- Indexing split store uses `MmapDirectory::open_with_madvice(..., Advice::Sequential)` for cached local splits.

Pseudocode:

```rust
struct GraphSegmentFooter {
    bundle_metadata_len: u64,
    hot_cache_len: u64,
}

fn read_graph_segment_footer(
    storage: &dyn ObjectStorage,
    path: &Path,
) -> Result<(OwnedBytes, OwnedBytes), StoreError> {
    let file_len = storage.len(path)?;
    let hot_len = read_u64_tail(storage, path, file_len - 8)?;
    let footer_len_pos = file_len - 8 - hot_len - 8;
    let footer_len = read_u64_at(storage, path, footer_len_pos)?;
    let footer = storage.get_slice(path, footer_len_pos - footer_len..file_len)?;
    let bundle_only = footer.slice(0..footer_len as usize + 8);
    Ok((footer, bundle_only))
}

fn read_hot_cached_range(
    cache: &StaticSliceCache,
    file: &FileSlice,
    range: Range<usize>,
) -> io::Result<OwnedBytes> {
    cache.try_read_bytes(range.clone())
        .map(Ok)
        .unwrap_or_else(|| file.read_bytes_slice(range))
}
```

Rust translation:

- Append hot graph metadata to immutable segment files: label dictionaries, offset tables, top-level index blocks.
- Fetch tail ranges first for remote/cold storage.
- Use local mmap for cached splits with sequential advice during scans.

Memory and performance implications:

- Hot metadata is small and byte-backed.
- Cold payload stays remote or file-backed until accessed.

Concurrency implications:

- Immutable split cache is shareable.
- Cache invalidation is per-segment generation, not per-record.

Testing implications:

- Test footer parsing with truncated tail, invalid lengths, and absent hotcache.
- Verify hot-cache hit path and fallback path return identical bytes.

Agentic coding guidance:

- For graph snapshots, put small metadata at the tail so startup can read a few ranges instead of the whole segment.

## Segment Files

### Pattern: Stable Pointer Layout for On-Disk Values

Source path:

- `gitrefrepo/qdrant-src/lib/gridstore/src/tracker.rs:33-105`

Source evidence:

- Qdrant defines `OptionalPointer` as `#[repr(C)]`, `Pod`, `Zeroable`, with an explicit `u32` discriminant to avoid relying on `Option<ValuePointer>` layout.
- `ValuePointer` is `#[repr(C)]`, `Pod`, `Zeroable`, and contains page id, block offset, and byte length.
- `None` is all zeroes; `Some` stores discriminant 1 and the pointer.

Pseudocode:

```rust
#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct OptionalGraphPointer {
    tag: u32,
    value: GraphValuePointer,
}

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct GraphValuePointer {
    page_id: u32,
    block_offset: u32,
    length: u32,
}
```

Rust translation:

- Do not persist Rust `Option<T>` unless layout is explicitly specified.
- Use explicit tags and `repr(C)` for file-compatible pointer tables.
- Keep all fields fixed-width integers.

Memory and performance implications:

- Pointer tables can be mmap-backed and decoded without heap allocation.
- No varint overhead for hot pointer lookup tables.

Concurrency implications:

- Pointer tables should be immutable per committed generation or updated via WAL.

Testing implications:

- Compile-time size/alignment assertions.
- Round-trip old and new file versions.
- Miri tests for safe wrapper APIs, not raw mmap system calls.

Agentic coding guidance:

- Every on-disk struct needs a layout test next to it.
- Use `OptionalGraphPointer`, not `Option<GraphValuePointer>`, in file pages.

### Pattern: Sparse Segment Indexes with Optional In-Memory Cache

Source paths:

- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/partitions/src/log.rs` search evidence around lines 105-260
- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/partitions/src/iggy_index.rs` search evidence around offset/timestamp lower-bound tests
- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/partitions/src/iggy_index_writer.rs` search evidence around append-at-position and optional fsync

Source evidence:

- Iggy partition log stores `segments: Vec<Segment>`, `indexes: Vec<Option<IggyIndexCache>>`, and `index_writers`.
- `ensure_indexes()` creates an active index cache lazily.
- `IggyIndexCache` supports lower-bound lookup by offset and timestamp in tests.
- Index writer appends sparse index bytes and can fsync conditionally.

Pseudocode:

```rust
struct GraphSegmentLog {
    segments: Vec<GraphSegment>,
    sparse_indexes: Vec<Option<GraphSparseIndex>>,
    index_writers: Vec<Option<IndexWriter>>,
}

fn ensure_active_sparse_index(
    log: &mut GraphSegmentLog,
    capacity: usize,
) -> &mut GraphSparseIndex {
    if log.sparse_indexes.last().unwrap().is_none() {
        *log.sparse_indexes.last_mut().unwrap() = Some(GraphSparseIndex::with_capacity(capacity));
    }
    log.sparse_indexes.last_mut().unwrap().as_mut().unwrap()
}
```

Rust translation:

- Keep sparse offset indexes for append-only adjacency segments.
- Load inactive segment indexes lazily.
- Persist index append separately from data append if data can be replayed or rebuilt.

Memory and performance implications:

- Reduces memory by avoiding active cache for every segment.
- Lower-bound sparse index makes offset/time seeks efficient.

Concurrency implications:

- Segment vector mutation needs writer ownership.
- Immutable closed segments can share index caches.

Testing implications:

- Rebuild missing sparse indexes from segment data.
- Corrupt index file and verify rebuild path.

Agentic coding guidance:

- Do not eagerly load every segment index on graph open.
- Design `open_segment_index_lazy` before adding segment retention.

## Memory Ownership, Arenas, and Slabs

### Pattern: Arena Allocation with Inline Block and Opposite-End Alignment

Source paths:

- `gitrefrepo/rocksdb-src/memory/arena.h:25-133`
- `gitrefrepo/rocksdb-src/memtable/inlineskiplist.h:853-892`

Source evidence:

- RocksDB `Arena` has an inline block, minimum/maximum block sizes, optional huge page allocation, and memory usage accounting.
- Aligned and unaligned chunks are allocated from opposite ends of the active block to reduce alignment waste.
- Inline skip list allocates node pointers, node struct, and key bytes in one aligned arena allocation.
- Splice state is also allocated as one contiguous arena block.

Pseudocode:

```rust
struct GraphArena {
    inline: [MaybeUninit<u8>; INLINE_BYTES],
    blocks: Vec<Box<[MaybeUninit<u8>]>>,
    unaligned_top: usize,
    aligned_bottom: usize,
    allocated_bytes: usize,
}

fn allocate_inline_skip_node(
    arena: &mut GraphArena,
    height: usize,
    key: &[u8],
) -> NonNull<SkipNode> {
    let prefix = size_of::<AtomicPtr<SkipNode>>() * (height - 1);
    let total = prefix + size_of::<SkipNode>() + key.len();
    let raw = arena.allocate_aligned(total);
    unsafe { layout_skip_node(raw, prefix, key, height) }
}
```

Rust translation:

- Use arena allocation for memtable records and temporary import indexes.
- Keep graph memtable keys inline with nodes to avoid pointer chasing.
- Drop the entire arena when memtable flush completes.

Memory and performance implications:

- Reduces per-record allocation overhead.
- Improves locality for skip list or trie memtables.
- Arena can over-retain memory until flush; expose memory accounting.

Concurrency implications:

- A write-thread-owned arena is simpler than a shared arena.
- If shared, use per-thread arenas or synchronized bump allocation.

Testing implications:

- Allocation accounting tests.
- Miri tests for pure Rust arena pointer math if unsafe is used.
- Fuzz variable key sizes and heights.

Agentic coding guidance:

- Prefer existing crates for bump arenas unless the storage layout requires custom pointer placement.
- If implementing custom arena, first write a safe API and isolate unsafe layout code.

### Pattern: Sharded Slab with Generational Tokens

Source path:

- `gitrefrepo/tikv-src/components/tracker/src/slab.rs:1-230`

Source evidence:

- TiKV tracker slab uses 64 shards, each `CachePadded<Mutex<TrackerSlab>>`.
- `next_shard_id()` is thread-local and round-robin.
- Each slab entry stores a sequence number.
- `TrackerToken` packs shard id, sequence number, and slab key into `u64`.
- Lookup checks that the entry sequence matches the token sequence.
- Inserts fail with `INVALID_TRACKER_TOKEN` and increment a metric when the shard is full.

Pseudocode:

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
struct GraphHandleToken(u64);

struct ShardedGraphSlab<T> {
    shards: [CachePadded<Mutex<GenerationalSlab<T>>>; 64],
}

fn insert_graph_handle_token<T>(
    slab: &ShardedGraphSlab<T>,
    value: T,
) -> GraphHandleToken {
    let shard = next_thread_local_shard();
    slab.shards[shard].lock().insert(value)
}

fn get_graph_handle_mut<T>(
    slab: &ShardedGraphSlab<T>,
    token: GraphHandleToken,
) -> Option<&mut T> {
    let shard = token.shard_id();
    let seq = token.sequence();
    let key = token.key();
    slab.shards[shard].lock().get_if_sequence_matches(key, seq)
}
```

Rust translation:

- Use generational tokens for session-local handles: open cursors, active traversals, temporary import jobs.
- Shard by thread to reduce lock contention.
- Pack token fields explicitly and expose debug formatting.

Memory and performance implications:

- Slab avoids per-handle hash map overhead.
- Capacity caps protect RAM.
- Generational sequence prevents stale token reuse.

Concurrency implications:

- Cache padding lowers false sharing.
- Mutex scope should be tiny.

Testing implications:

- Concurrent insert/remove tests.
- Stale-token lookup test after remove and reinsert.
- Full-slab metrics test.

Agentic coding guidance:

- Use slabs for short-lived engine handles, not persistent graph ids.
- Persistent ids need stable allocation and file mapping, not generational slab keys.

### Pattern: Allocator Knobs Are Part of the Memory Budget

Source paths:

- `gitrefrepo/jemalloc-src/doc/jemalloc.xml.in:300-326`
- `gitrefrepo/jemalloc-src/doc/jemalloc.xml.in:785-803`
- `gitrefrepo/jemalloc-src/doc/jemalloc.xml.in:1153-1205`
- `gitrefrepo/jemalloc-src/doc/jemalloc.xml.in:1370-1398`
- `gitrefrepo/jemalloc-src/doc/jemalloc.xml.in:1727-1855`

Source evidence:

- jemalloc supports choosing explicit tcaches and arenas via allocation flags.
- Background threads can purge asynchronously.
- Dirty and muzzy decay settings control how quickly unused pages are purged or reused.
- Thread-specific caches satisfy many allocations without synchronization, but increase memory use.
- `thread.arena`, thread allocation/deallocation counters, peak reads, tcache enable/flush are exposed via mallctl.

Pseudocode:

```rust
struct AllocatorBudgetConfig {
    tcache_max_bytes: usize,
    dirty_decay_ms: isize,
    muzzy_decay_ms: isize,
    background_purge: bool,
}

fn configure_storage_allocator_budget(
    config: AllocatorBudgetConfig,
) -> Result<(), AllocError> {
    set_mallctl("background_thread", config.background_purge)?;
    set_mallctl("dirty_decay_ms", config.dirty_decay_ms)?;
    set_mallctl("muzzy_decay_ms", config.muzzy_decay_ms)?;
    Ok(())
}
```

Rust translation:

- If using jemalloc, expose allocator metrics in graph-store memory reports.
- Consider lower tcache ceilings for memory-constrained deployments.
- Use decay settings for import workloads that allocate large temporary structures.

Memory and performance implications:

- Thread caches improve throughput but can inflate RSS.
- Aggressive purging lowers RSS but can increase CPU/page-fault cost.

Concurrency implications:

- Per-thread caches reduce synchronization but hide per-thread retained memory.

Testing implications:

- Memory benchmarks should record allocator stats, not only graph cache stats.
- Include long-running import/delete workloads to reveal retained RSS.

Agentic coding guidance:

- Do not claim lower RAM from data-structure changes without allocator-level RSS measurements.

### Pattern: Fixed-Capacity `no_std` Core for Hot Algorithms

Source path:

- `/Users/amuldotexe/Desktop/personal-repos-lane/nostd-union-find/src/lib.rs:1-120`

Source evidence:

- `nostd-union-find` is `no_std` outside tests.
- It uses zero heap allocation with const-generic capacity.
- It stores parent and rank arrays as `[usize; N]`.
- It documents invariants: valid parents, rank upper bound, and path compression.

Pseudocode:

```rust
pub struct FixedGraphUnionFind<const N: usize> {
    parent: [u32; N],
    rank: [u8; N],
}

fn union_sets_by_rank<const N: usize>(
    uf: &mut FixedGraphUnionFind<N>,
    a: u32,
    b: u32,
) -> Result<(), GraphAlgoError> {
    let root_a = find_set_with_compression(uf, a)?;
    let root_b = find_set_with_compression(uf, b)?;
    attach_lower_rank_root(uf, root_a, root_b);
    Ok(())
}
```

Rust translation:

- Keep hot graph algorithms in allocation-free cores when capacity is known per batch/shard.
- Use `u32` ids when graph shard cardinality permits.

Memory and performance implications:

- Predictable stack or preallocated memory.
- No allocator noise in algorithm benchmarks.

Concurrency implications:

- Usually worker-local; merge results through explicit data structures.

Testing implications:

- Kani can verify small fixed `N` invariants.
- Fuzz can compare against `petgraph` or `BTreeMap` model for small graphs.

Agentic coding guidance:

- Split graph algorithm kernels from storage adapters.
- Make fixed-capacity variants first for verification, then layer dynamic allocation outside.

## Unsafe Encapsulation, Send/Sync, and Interior Mutability

### Pattern: Unsafe Boundary with Compile-Fail Lifetime Test

Source paths:

- `gitrefrepo/rust-rocksdb-src/src/perf.rs:217-309`
- `gitrefrepo/rust-rocksdb-src/tests/fail/memory_usage_builder_outlive_db.stderr:1-11`

Source evidence:

- `MemoryUsageBuilder<'a>` wraps raw RocksDB memory consumer pointers.
- It stores `PhantomData<&'a ()>` and methods take `&'a DB`/`&'a Cache`.
- The compile-fail test proves the builder cannot outlive a DB borrowed into it.

Pseudocode:

```rust
pub struct MemoryReportBuilder<'a> {
    raw: NonNull<RawMemoryConsumers>,
    _borrowed: PhantomData<&'a ()>,
}

impl<'a> MemoryReportBuilder<'a> {
    pub fn add_graph_cache(&mut self, cache: &'a GraphCache) {
        unsafe { raw_add_cache(self.raw, cache.raw()) }
    }
}
```

Rust translation:

- Any raw pointer aggregator should carry lifetimes for every borrowed source.
- Add trybuild tests that intentionally drop DB/cache too early.

Memory and performance implications:

- Safe memory reporting avoids keeping extra `Arc` clones or deep copies just to collect stats.

Concurrency implications:

- Builder can be single-threaded and short-lived; do not make it `Send` unless needed.

Testing implications:

- `trybuild` compile-fail tests are part of correctness, not docs.

Agentic coding guidance:

- For every unsafe wrapper, ask: "What compile-fail test would prove the lifetime boundary?"

### Pattern: Interior Mutability Only at Ownership Boundary

Source paths:

- `gitrefrepo/qdrant-src/lib/wal/src/mmap_view_sync.rs:15-190`
- `/Users/amuldotexe/Desktop/oss-read-only/iggy/core/journal/src/prepare_journal.rs:81-104`

Source evidence:

- Qdrant wraps `MmapMut` in `Arc<UnsafeCell<_>>` but exposes private unsafe accessors and range views.
- Iggy prepare journal uses `RefCell<Vec<Option<PrepareHeader>>>`, `RefCell<Vec<Option<u64>>>`, and `Cell` for `last_op` and `snapshot_op`.

Pseudocode:

```rust
struct WriterOwnedWalIndex {
    headers: RefCell<Vec<Option<Header>>>,
    offsets: RefCell<Vec<Option<u64>>>,
    last_op: Cell<Option<u64>>,
}

struct ThreadSafeWalIndex {
    inner: Mutex<WalIndexInner>,
}
```

Rust translation:

- `Cell`/`RefCell` are appropriate for single-threaded writer-owned components.
- Use `Mutex`/`RwLock` or actor ownership when the component crosses threads.
- Avoid mixing `Arc<UnsafeCell<_>>` with public `Send/Sync` unless there is a stronger type-state proof.

Memory and performance implications:

- Interior mutability avoids broad mutable borrows in parser/index code.
- Overusing it can hide aliasing and concurrency bugs.

Concurrency implications:

- `RefCell` panics on borrow violations and is not thread-safe.
- `UnsafeCell` requires explicit aliasing discipline.

Testing implications:

- Miri tests can catch some aliasing violations in pure Rust paths.
- Use static analysis for unsafe dataflow and Send/Sync variance.

Agentic coding guidance:

- When translating single-threaded code to graph-store server code, replace `RefCell` with an explicit owner model before adding `Arc`.

### Pattern: Static Analysis for Unsafe and Concurrency Hotspots

Source paths:

- `/Users/amuldotexe/Desktop/reference-repos-yard/lockbud/src/callbacks.rs:122-171`
- `/Users/amuldotexe/Desktop/reference-repos-yard/Rudra/src/lib.rs:117-133`

Source evidence:

- Lockbud has detectors for deadlocks, atomicity violations, invalid free, and use-after-free.
- Rudra runs unsafe destructor, Send/Sync variance, and unsafe dataflow analyses based on configuration.

Pseudocode:

```bash
cargo miri test -p graph-storage-core
cargo kani -p graph-page-allocator
# plus static detectors where toolchains are available:
# lockbud: deadlock, atomicity, memory
# Rudra: SendSyncVariance, UnsafeDataflow, UnsafeDestructor
```

Rust translation:

- Use static analysis as a gate for modules containing:
  - manual `Send`/`Sync`
  - `UnsafeCell`
  - mmap view code
  - custom allocators
  - raw FFI pointers

Memory and performance implications:

- Analysis does not lower RAM directly, but it protects optimizations that avoid copying and use unsafe layout.

Concurrency implications:

- Deadlock and atomicity detectors are especially relevant to page cache and WAL locks.

Testing implications:

- Pair static tools with runtime stress tests; neither replaces the other.

Agentic coding guidance:

- Create an `unsafe-audit.md` list and update it whenever a new `unsafe impl Send/Sync` lands.

## Typestates, Newtypes, and Error Handling

### Pattern: Explicit Durable State Typestates

Sources:

- redb commit-slot/two-phase evidence above.
- Fjall `PersistMode` evidence above.
- Iggy snapshot watermark evidence above.

Pseudocode:

```rust
struct Unflushed;
struct OsBuffered;
struct DiskSynced;

struct CommitBatch<State> {
    id: TxId,
    bytes: Vec<u8>,
    _state: PhantomData<State>,
}

fn flush_batch_to_os(
    batch: CommitBatch<Unflushed>,
) -> Result<CommitBatch<OsBuffered>, StoreError> {
    write_all_batch_bytes(&batch)?;
    Ok(batch.restate())
}

fn sync_batch_to_disk(
    batch: CommitBatch<OsBuffered>,
) -> Result<CommitBatch<DiskSynced>, StoreError> {
    fdatasync_batch_file()?;
    Ok(batch.restate())
}
```

Rust translation:

- Distinguish `buffered`, `datasynced`, `sync_all`, `published`, and `checkpointed` in types or enums.
- Use newtypes for `PageId`, `BlockOffset`, `TransactionId`, `SnapshotOp`, `SegmentGeneration`.

Memory and performance implications:

- Typestates prevent extra defensive copies caused by unclear state transitions.

Concurrency implications:

- State types make it harder to publish roots before WAL bytes are in the required durability state.

Testing implications:

- Compile-fail tests should reject publishing an unflushed batch.

Agentic coding guidance:

- Encode crash/recovery state in types before implementing recovery.
- Avoid `bool durable` and `bool committed`; use enums or typestates.

### Pattern: Error Policy by Recovery Boundary

Source paths:

- `gitrefrepo/fjall-src/README.md:140-145`
- `gitrefrepo/fjall-src/src/db.rs:353-365`
- `/Users/amuldotexe/Desktop/oss-read-only/opendal/core/layers/retry/src/lib.rs:31-47`
- `/Users/amuldotexe/Desktop/oss-read-only/opendal/core/layers/retry/src/lib.rs:393-435`

Source evidence:

- Fjall docs say error enum variants are mostly for debugging/tracing and recommend crash/restart as safest recovery for transient I/O errors.
- `Database::persist()` poisons the database after fatal journal persist failure.
- OpenDAL retry layer retries temporary errors, then marks them persistent; it warns that dropped futures can leave retry state invalid.
- Retry reader advances byte range after successful reads.

Pseudocode:

```rust
enum StoreErrorPolicy {
    RetryTemporary,
    PoisonDatabase,
    RequireRestart,
}

fn handle_persist_error_policy(
    db: &GraphDb,
    err: io::Error,
) -> StoreError {
    db.poison.store(true, Release);
    StoreError::FatalPersistence { source: err }
}

fn retry_range_read_progress(
    reader: &mut RetryRangeReader,
) -> Result<Bytes, StoreError> {
    let bytes = reader.current.read()?;
    reader.range.advance(bytes.len() as u64);
    Ok(bytes)
}
```

Rust translation:

- Poison the store on ambiguous persistence failures.
- Let higher-level process supervision restart and recover from WAL/checkpoint.
- For remote/cold storage reads, advance retry range only after successful bytes.

Memory and performance implications:

- Poisoning avoids accumulating inconsistent in-memory state after I/O ambiguity.
- Retry wrappers hold state; dropped futures can leak or invalidate read progress if not designed carefully.

Concurrency implications:

- Poison flag should be atomic and checked at transaction entry points.

Testing implications:

- Inject fsync failures and assert store rejects future writes.
- Retry tests should include partial reads and dropped futures if async APIs are used.

Agentic coding guidance:

- Do not "handle" persistence errors by logging and continuing.
- Treat poison/restart as a valid storage-engine design.

### Pattern: Range Reads with Concurrency Permits

Source paths:

- `/Users/amuldotexe/Desktop/oss-read-only/opendal/core/core/src/types/options.rs:61-130`
- `/Users/amuldotexe/Desktop/oss-read-only/opendal/core/layers/concurrent-limit/src/lib.rs:243-323`

Source evidence:

- OpenDAL read options include explicit byte ranges, version/conditional fields, concurrent chunk reads, chunk size, and range merge gap.
- The concurrent-limit layer acquires a permit before `read`, `write`, `list`, and returns wrappers that hold permits until the reader/writer/lister is dropped.

Pseudocode:

```rust
struct GraphRangeReadOptions {
    range: Range<u64>,
    concurrent_chunks: usize,
    chunk_size: Option<usize>,
    merge_gap: usize,
}

struct PermitReader<R, P> {
    inner: R,
    _permit: P,
}
```

Rust translation:

- Make graph cold-storage reads range-first.
- Hold concurrency permits for the lifetime of streaming readers, not just for the open call.
- Merge nearby ranges for remote storage, but keep precise ranges for local mmap.

Memory and performance implications:

- Prevents unbounded concurrent remote reads from inflating buffers.
- Range merging trades bandwidth for fewer requests.

Concurrency implications:

- Permit-held readers bound active memory and file descriptors.

Testing implications:

- Assert permits are released on drop.
- Simulate many concurrent scans and check maximum in-flight readers.

Agentic coding guidance:

- Any async graph segment API should accept explicit ranges and an I/O budget handle.

## Verification, Fuzzing, and Model Checking

### Pattern: Model-Based Fuzzing Against BTreeMap

Source path:

- `gitrefrepo/sled-src/fuzz/fuzz_targets/fuzz_model.rs:15-145`

Source evidence:

- sled fuzzes operations `Get`, `Insert`, `Reboot`, `Remove`, `Cas`, and `Range`.
- It opens a temp DB and a `BTreeMap` model.
- Each operation is applied to both.
- `Reboot` drops and reopens the DB.
- After each op it checks all model keys and full DB iteration match.

Pseudocode:

```rust
enum GraphOp {
    AddNode(NodeId),
    AddEdge(NodeId, NodeId, EdgeType),
    RemoveNode(NodeId),
    RemoveEdge(NodeId, NodeId, EdgeType),
    Reboot,
    RangeAdjacency(NodeId),
}

fn fuzz_graph_store_model(ops: Vec<GraphOp>) {
    let mut store = open_temp_graph_store();
    let mut model = BTreeMapGraph::default();
    for op in ops {
        apply_to_store_and_model(&mut store, &mut model, op);
        assert_graph_equal(&store, &model);
    }
}
```

Rust translation:

- Use a simple in-memory graph model as the oracle.
- Include reboot as an ordinary fuzz operation.
- Compare range traversal and full iteration after every step.

Memory and performance implications:

- Fuzzing small graphs catches allocator/recovery bugs before large benchmarks.

Concurrency implications:

- Start single-threaded; add concurrency model only after sequential invariants are stable.

Testing implications:

- This should be the first fuzz target for any graph-store rewrite.

Agentic coding guidance:

- Do not write storage code without a model oracle.
- Keep operation names close to graph semantics, not storage internals.

### Pattern: Failpoint Crash Loop with Durable Reference Versions

Source paths:

- `gitrefrepo/sled-src/tests/test_tree_failpoints.rs:171-235`
- `gitrefrepo/sled-src/tests/test_tree_failpoints.rs:353-368`
- `gitrefrepo/sled-src/tests/test_tree_failpoints.rs:474-523`

Source evidence:

- sled serializes QuickCheck failpoint tests with a global mutex.
- It catches panics, tears down failpoints, and restarts.
- Test config uses tiny cache and segment size.
- `fp_crash!` handles failpoint errors by teardown, crash counter increment, restart, and continue.
- After a successful flush, the reference model collapses recent versions as durable.
- QuickCheck runs generated operation sequences.

Pseudocode:

```rust
fn run_graph_crashes_nicely(
    ops: Vec<GraphCrashOp>,
) -> bool {
    let mut store = open_tiny_graph_store();
    let mut reference = VersionedGraphModel::default();
    let mut crash_epoch = 0;

    for op in ops {
        match apply_with_failpoints(&mut store, &mut reference, op) {
            Ok(FlushComplete) => reference.collapse_durable_versions(crash_epoch),
            Err(FailPointCrash) => {
                clear_failpoints();
                crash_epoch += 1;
                store = reopen_graph_store();
            }
            Err(_) => return false,
            _ => {}
        }
    }
    true
}
```

Rust translation:

- Add failpoints at WAL append, page write, commit-slot write, fsync, checkpoint rename, and cache invalidation.
- Reference model must track both latest intended value and last durable value.

Memory and performance implications:

- Tiny cache/segment settings force edge cases quickly.

Concurrency implications:

- Serialize global failpoint tests unless the failpoint framework is thread-isolated.

Testing implications:

- Run in CI with small case count and nightly/deep mode with larger count.

Agentic coding guidance:

- Build failpoints as named crash sites before optimizing recovery.

### Pattern: Bounded Proof Harnesses for Allocator Invariants

Source paths:

- `/Users/amuldotexe/Desktop/reference-repos-yard/kani/docs/src/reference/attributes.md:14-32`
- `/Users/amuldotexe/Desktop/reference-repos-yard/kani/docs/src/reference/attributes.md:110-151`
- `/Users/amuldotexe/Desktop/reference-repos-yard/kani/docs/src/reference/arbitrary.md:25-51`

Source evidence:

- `#[kani::proof]` marks proof harnesses.
- Harnesses can use `kani::any()`.
- `#[kani::unwind(n)]` bounds loops; the required bound is maximum iterations plus one.
- `any_where` or `kani::assume` constrains arbitrary values.

Pseudocode:

```rust
#[kani::proof]
#[kani::unwind(9)]
fn verify_buddy_allocator_small() {
    let order: u8 = kani::any_where(|o| *o < 4);
    let page: u32 = kani::any_where(|p| *p < 16);
    let mut alloc = BuddyAllocator::<16>::new();
    let allocated = alloc.allocate(order);
    if let Some(page) = allocated {
        alloc.free(page);
        assert!(alloc.invariants_hold());
    }
}
```

Rust translation:

- Prove small bounded variants of page allocator, pointer range math, and commit-slot selector.
- Keep proof harnesses in storage-core crates without OS I/O.

Memory and performance implications:

- Proof targets should be tiny and pure; do not run them on the whole database.

Concurrency implications:

- Kani can prove local state machines, not full OS crash ordering.

Testing implications:

- Use Kani for bit-packed allocator invariants and page-range arithmetic.

Agentic coding guidance:

- Extract pure functions until they are proofable.
- If a function cannot be tested without a file, split parsing/math from I/O.

### Pattern: Miri for Unsafe Execution, Not Proof of Soundness

Source paths:

- `/Users/amuldotexe/Desktop/reference-repos-yard/miri/README.md:77-119`
- `/Users/amuldotexe/Desktop/reference-repos-yard/miri/README.md:415-500`

Source evidence:

- Miri can detect UB in a particular execution, but absence of UB in Miri is not a proof of soundness.
- `cargo miri test` runs tests under Miri.
- Flags disabling data-race detector, Stacked Borrows, or validation are unsound and can miss bugs.
- Tree Borrows is experimental and may be looser than final Rust aliasing rules.

Pseudocode:

```bash
cargo +nightly miri test -p graph-storage-core page_range
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-many-seeds=32" cargo +nightly miri test -p graph-storage-core
```

Rust translation:

- Use Miri on unsafe layout code, pointer wrappers, custom arenas, and byte-slice casts.
- Avoid depending on native mmap/FFI in Miri tests; use in-memory buffers.

Memory and performance implications:

- Miri is slow; keep tests focused.

Concurrency implications:

- Use Miri seeds/preemption to explore schedule-sensitive unsafe code, but do not treat passing as proof.

Testing implications:

- Miri should be a gate for modules that contain `unsafe`.

Agentic coding guidance:

- Never use Miri flags that disable the bug class you are trying to catch.

## Concrete Graph-Store Design Guidance

### Recommended Storage Shape

```rust
struct GraphStorageEngine {
    header: DoubleBufferedHeader,
    wal: ActiveWalSegment,
    allocator: TransactionalPageAllocator,
    page_cache: PageCacheBudget,
    cold_segments: SegmentCatalog,
    indexes: GraphIndexCatalog,
}

struct GraphSnapshot<'db> {
    roots: RootSet,
    generation: SnapshotGeneration,
    _db: PhantomData<&'db GraphStorageEngine>,
}

struct GraphWriteTxn<'db> {
    snapshot: GraphSnapshot<'db>,
    dirty_pages: DirtyPageSet,
    wal_batch: JournalBatch<Unflushed>,
}
```

Design decisions to carry forward:

- Single writer at first; multi-writer can wait.
- MVCC readers over root generations.
- Copy-on-write page trees for node/edge/property records.
- Immutable segment files for cold snapshots and analytical indexes.
- WAL with bounded entry sizes, seeded checksums, and tail truncation.
- Double commit slots with checksum and an atomic selector byte.
- Allocator quick-repair table plus full page scan fallback.
- Cache budget split between read pages, dirty pages, metadata, and decompressed blocks.
- Byte-backed dictionaries/FSTs for labels, relationship types, and property keys.
- Stable `repr(C)` pointer tables for page/block/length references.

### RAM Reduction Checklist

- Use `u32` ids within shards where possible.
- Store adjacency payloads in page files and return borrowed single-page slices.
- Cache decompressed blocks, not entire segments.
- Make index/filter blocks cacheable and evictable.
- Use compact filters on cold LSM levels or immutable segments.
- Avoid Rust `HashMap` for persistent dictionaries; use FST/trie/byte-backed maps.
- Keep allocator metadata bit-packed.
- Use arenas for write memtables and drop them wholesale after flush.
- Load segment sparse indexes lazily.
- Track allocator RSS and cache RSS separately.

### Correctness Checklist

- Every on-disk struct has size/alignment/version tests.
- Every raw pointer wrapper has a safety comment and a compile-fail lifetime test where possible.
- Every commit step has a crash test.
- Every WAL parser bounds length before allocation.
- Every comparator has matching filter key projection tests.
- Every page free path checks active readers before reuse in debug builds.
- Every repair path can rebuild allocator state from roots.
- Every fatal fsync/persist error poisons the database.

## Gaps and Follow-Up Work

High-value gaps:

- TiKV Raft log and RocksDB engine wrappers need a separate pass. This slice only used TiKV's sharded slab tracker pattern.
- Quickwit metastore checkpoints and split lifecycle need a separate pass. This slice only used split footer/hotcache and mmap split-cache paths.
- Qdrant collection snapshots and vector index persistence were not inspected.
- OpenDAL was inspected only as an I/O boundary. Its full service backends and object-store consistency behavior were not inspected.
- Iggy partition segment implementation deserves a deeper pass; this slice only included prepare journal and sparse-index/log search evidence.
- Formal verification repos beyond Kani/Miri/Lockbud/Rudra were not substantively read.
- `nostd-toposort-kahns-algorithm` and `nostd-fmt-fixed-capacity` remain uninspected.

Recommended next evidence targets:

```bash
rg -n "Snapshot|checkpoint|raft log|apply|truncate|compact" gitrefrepo/tikv-src/components gitrefrepo/tikv-src/src
rg -n "SplitMetadata|checkpoint|metastore|publish_splits" /Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit/quickwit
rg -n "snapshot|recovery|segment|checksum" gitrefrepo/qdrant-src/lib gitrefrepo/qdrant-src/src
rg -n "prusti|creusot|flux|hax|charon" /Users/amuldotexe/Desktop/reference-repos-yard
```
