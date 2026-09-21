# Exact Similarity Through Runwise Range Selection

Date: 2026-09-20. A05 research alternative; this file is the entire write scope. No production implementation, shared-file change, or established novelty claim.

**Successor note:** the [bounded-seed and ordered-endpoint-merge follow-on](Similarity-Bounded-Seed-Selection.md) removes the R term from retained selector state and implements a no-event-sort option when the present-feature streams fit their admitted cursor budget. This document and its Python fence preserve the original retain-all-head schedule and its evidence. R-dependent heap refusals and the 96-byte-times-(R+k) illustration below describe that older schedule, not a lower bound on exact runwise similarity. Posting fragmentation, mandatory head RMQs and lifecycle costs still matter. The successor includes a public-data comparison that wins some queries and loses others to an ordinary scan.

## Premise Check

**Working thesis:** represent the query's exact intersection count as a piecewise-constant function of a frozen target rank. Query-independent range-minimum indexes then expose the best target in each constant-count interval without enumerating its members. This eliminates both the changed-target expansion of [Similarity-Exception-Merge.md](Similarity-Exception-Merge.md) and its per-anchor target tables. It does not eliminate the global target index, all queried feature intervals, endpoint sorting, output, or refresh.

The lead supplied this derivation. This document tests and prices it. The selected implementation is an external endpoint sort plus two argmin fields in one shared disk segment tree. An in-memory linear-time RMQ builder, an n-entry overlap array, and a free external heap are not assumed.

## Expert Lenses And Alternatives

The retrieval lens checks full score/ID ordering and zero tails. The external-memory lens checks construction as well as query state. The adversarial lens separates interval complexity from population size. The prior-art lens gives an ordinary range-query executor the same overlap runs and indexes.

Three relevant alternatives are: the previous complete per-anchor bucket index; compressed postings with document-at-a-time scoring/pruning; and the selected run field with shared range selection. The selected mechanism wins structurally when the queried postings have few intervals, even if their union contains every target. It can lose when rank order fragments postings or nearly every target has its own overlap run. An external priority queue could extend its admitted domain; this miniature instead explicitly rejects an oversized resident selector.

## Exact Contract

The snapshot contains n eligible targets at immutable positions `j in [0,n)`, with unique original IDs `id_j`, binary feature sets T_j and exact cardinalities b_j. Physical position is **not** the original-ID tie order. Each feature f has a sorted, disjoint, maximal half-open interval list whose union is exactly `{j:f in T_j}`. Targets need not have equal sets or equal b values. The rank permutation, projection, memberships, cardinalities, both RMQ fields and inverse ID map share one immutable version.

The normalized query S is a binary set, a=|S|. Return exactly `k'=min(k,n-1[self ID exists])` distinct eligible nonself IDs, Jaccard descending then original ID ascending. No boundary-tie expansion. Score is `c/(a+b-c)` for c>0, and zero for c=0, including empty-empty. Self exclusion is by original ID resolved through a paid inverse index. k=0 and n=0 publish empty output. Oversized k returns every nonself target. An empty source creates the all-zero run when n>0; it does not need a posting scan.

Only exact binary Jaccard is claimed/tested. Fractions are compared exactly. Under the packed model `n,a,b < 2^32`, denominators need at most 33 bits and cross-products at most 65 bits; checked 128-bit comparisons suffice. Signed endpoint accumulation uses checked wide integers. The prototype uses Python integers/Fraction and 64-bit wire fields instead of claiming a packed production implementation.

## Mechanism And Proof

### Exact Overlap Field

For every interval `[l,r)` in a feature posting with f in S, emit `(l,+1)` and `(r,-1)`. Let L be the number of source posting intervals and E=2L. Sort E events externally by position. **Sum all deltas at a position before advancing the sweep.** The prefix value on positions j is

```text
c(j) = sum_(f in S) sum_([l,r) in posting(f)) 1[l <= j < r]
     = |S intersect T_j|.
```

Disjointness inside each feature prevents double-counting. Cover all of `[0,n)`, including gaps with value zero. Ignore zero net endpoint changes and coalesce adjacent equal values. Write the resulting R nonempty maximal runs to disk; no target is visited merely to construct this field. `R <= min(n,2L+1)` for n>0; R=0 for n=0. The inequality is a bound, not a promise that R is small. Correct prepared postings imply `0<=c(j)<=min(a,b_j)`.

All L intervals are read even when most events cancel. R alone is therefore not a bound on query input work. For example, S can contain arbitrarily many features each present in every target: R=1 but L=a and E=2a.

### Two Shared Argmin Fields

Prepare two position-returning RMQ orders once per target snapshot:

```text
positive_key(j) = (b_j, id_j)
zero_key(j)     = id_j.
```

For fixed c>0 and a>0, `c/(a+b-c)` strictly decreases with b. Hence the positive argmin is the exact score/ID winner of **any** subinterval of a c-run, independent of a. For c=0 all scores tie and the zero argmin is required. These are different orders; a single cardinality RMQ cannot answer zero runs correctly. Separate logical fields can share one segment-tree layout and metadata file.

Resolve self to position s and split its containing run into `[l,s)` and `[s+1,r)` before asking for candidates. Let R' be the number of remaining initial nonempty intervals. Removing one position gives `R'<=R+1` and partitions the eligible domain exactly.

### Best-First Selection

```text
pin coherent interval, rank, metadata, inverse-ID and RMQ versions
resolve self and k'; if k'=0 publish empty output
preflight and externally sort all E source endpoint events
sweep/coalesce full-domain runs to a disk file; count R
admit at most min(n-n_self, R+k') heap entries and the complete output
for each run, splitting around self first:
    RMQ the appropriate positive/zero order
    insert (exact score/ID key, l,r,c,winner_position) into the heap
repeat k' times:
    pop the best interval winner and emit its exact result record
    unless this was the final requested result:
        RMQ each nonempty remainder [l,winner), [winner+1,r)
        insert their winners, keeping the same c
publish only a complete staged output
```

**Theorem 1: exact full ordered output.** Endpoint summation gives the exact c at every position. Initially the active intervals partition all nonself targets. Each interval head is its exact best remaining target by the argmin lemma. Removing its winner and replacing the interval with its two remainders preserves the partition and head invariant. The globally best remaining target is therefore always a heap head, and popping the least score/ID key emits it. Induction yields the first k' in total order with neither duplicates nor omissions; zero runs supply complete tails. No quality certificate, guessed threshold or candidate verification scan is used.

**Theorem 2: non-expanding selection state/work.** For k'>0, after t nonfinal outputs the heap has at most R'+t entries. At most k'-1 outputs create children, so peak entries are at most

```text
min(n-n_self, R'+k'-1) <= min(n-n_self, R+k').
RMQ calls <= R' + 2*(k'-1) <= R + 2*k' - 1.
```

Every active interval is nonempty and disjoint, giving the population cap. The literal prototype uses the conservative R+k' admission bound, counts every RMQ call and checks both inequalities. Individual RMQs take O(log n) disk-record probes in the chosen implementation. Heap maintenance takes `O((R+k') log(R+k'+1))` CPU; incremental insertion, not a claimed linear heap build, is used in the probe.

**Separation from target expansion, not from all competitors.** Let one feature be present in all n targets, let S consist solely of that feature, and compare to an empty anchor only to define q. Then q=n, L=1 and R=1, while target-specific private features can make all sets distinct and vary b. After paid shared preparation, the query needs O(log F) feature-directory service, two events, O(k') RMQs and output: `O(log F + (k'+1)log n + k'log(k'+1))` CPU/probes with binary directories. There is no n/q query expansion or per-anchor table. The source feature lookup cannot be omitted from this bound. A same-run RMQ executor has the identical bound.

### Optional Incremental Form

For a retained anchor field c_P(j), source edits give `c_S(j)=c_P(j)+d(j)`, where d is the endpoint sweep of added postings minus removed postings. Merge the two piecewise-constant fields and coalesce in time linear in their encoded lengths after event sorting. Example: remove `[0,n)` and add `[0,h)` and `[h,n)`. Six signed endpoint events reduce to the identically-zero edit field, without expanding any target. The old/new source sizes can still differ, so scores must use the new a.

This is ordinary interval algebra, not a new update identity. It requires paid old-run retention, exact version compatibility and its own admission. The probe checks the six-event cancellation but does **not** implement a general incremental-anchor cache/merge. The primary query architecture needs no anchor.

## Disk RMQ Without An n-RAM Builder

Let N be the next power of two at least max(1,n). Leaves N through 2N-1 represent target positions plus padding. Each disk node stores two argmin positions, with an explicit invalid sentinel for padded leaves. The metadata file stores `(original_ID,b)` by position. An iterative interval RMQ decomposes `[l,r)` into at most `2*(log2(N)+1)` canonical nodes, reading the chosen argmin and its metadata for each. It retains just the current winner and O(1) traversal counters. Argmin positions do not make metadata fetches free.

Construct the tree bottom-up in sequential levels. The temporary level record carries both winning keys and positions, so each parent is computed from two consecutive child records without random metadata reads. Stream the parent keys to the next temporary level and the two positions to the permanent tree's contiguous level region. Delete the child level once the parent level is complete. Peak temporary key-level payload is at most `1.5*N*w_key`; the permanent tree has `2*N*w_node` bytes. There is no resident leaf/minimum array, Cartesian-tree stack of length n, or array of all level offsets. A handful of level counters and file handles suffice. The prototype executes this builder.

The tree phase writes `(2N-1)*w_key` temporary key bytes, reads `(2N-2)*w_key`, writes `2N*w_node` permanent bytes and reads n metadata records. Its payload traffic is therefore `n*w_meta + (4N-3)*w_key + 2N*w_node`, before filesystem service. Membership sorting, inverse-map sorting, input normalization and all their writes are separate preparation costs. This derivation uses only the geometric sizes of streamed levels, not a claimed in-memory construction theorem.

The feature directory and inverse-ID map are also on disk. Membership pairs `(feature,position)` are externally sorted, duplicate pairs removed and adjacent positions coalesced to prepare interval postings. Building from an unsorted raw graph requires paid ID assignment, binary deduplication and cardinality derivation. The miniature's **fixture writer** supplies already-normalized target metadata from small sets; its external membership sort, interval construction, inverse-ID sort and RMQ builder are real. It is not a scalable raw-graph importer.

## Resources And Lifecycle

Let H=R+k', w_e/w_r/w_o be event/run/output widths, M the admitted sort-buffer bytes, f its fan-in, and B physical block size. For a coherent admitted query:

```text
RAM <= fixed sort, merge, tree/metadata cache, I/O, output and runtime buffers
       + w_heap*min(n-n_self,H) + O(log n) bookkeeping.
event sort traffic <= 2*w_e*E*(1+ceil(log_f(max(1,ceil(w_e*E/M)))))
query scratch/output reservation <= 2*w_e*E + w_r*min(n,E+1) + w_o*k'
query traffic = source scans + directory probes + interval payload
                + event sort + event sweep + run spool/write/read
                + RMQ node/metadata probes + full output/publication.
```

The scratch reservation is conservative: sort generations, run spool and staged output do not all peak together. Allocation units, file headers, directories and recovery copies are extra. The probe checks live payload on every arena write and preflights the conservative bound. It materializes R runs only on disk; after R is known it admits the heap or rejects before output. This is a resource rejection, not a similarity-quality fallback. A claimed external heap would need its own implementation and I/O proof; none is silently used.

Binary search for F feature-directory entries costs O(log F) record probes per source feature, and self lookup costs O(log n). The probe validates/counts S and preflights L in one scan, then rereads S to emit events, with two directory lookups per feature. Each source posting interval produces two events, regardless of its length. Worst-case RMQ traffic is O((R+k')log n) random node **and** metadata probes; charging each probe one full B-byte miss is a pessimistic physical-I/O estimate, not the measured payload. No O(n) norm or intersection array exists in the query.

All source-feature intervals are paid even for a small k. With singleton/alternating postings, L and R can be Theta(n), making selector admission fail and event sorting expensive. A different target permutation changes interval complexity; obtaining a useful permutation is a paid preparation/rebuild problem, not a given universal compression ratio.

### Explicit Storage Scenario

Illustrative decimal-byte model, not measured data: n=100M, m=1B distinct memberships, F=10M, total interval count L_all=50M, and 100k queried sources with k=50. Stable positions and cardinalities fit u32; IDs use u64. N=134,217,728.

| Retained component | Bytes |
| --- | ---: |
| Raw membership/provenance copy, selected format | 8,000,000,000 |
| Forward memberships, u32 feature ranks | 4,000,000,000 |
| Forward row offsets | 800,000,008 |
| Interval postings, two u32 endpoints | 400,000,000 |
| Feature `(rank,offset,count)` directory, packed 20 bytes | 200,000,000 |
| Global `(ID,b)` by position, 12 bytes/target | 1,200,000,000 |
| Global `(ID,position)` inverse map, 12 bytes/target | 1,200,000,000 |
| Shared tree, two u32 argmins/node, 2N nodes | 2,147,483,648 |
| Explicit format/manifest/recovery reservation | 1,000,000,000 |
| Two retained outputs, 24 bytes/result | 240,000,000 |

Total is **19,187,483,656 bytes** plus any other selected family blocks and source mapping not covered by the reservation. One complete replacement of every shared block alongside the old one, retaining three outputs, is **38,254,967,312 bytes**. This fits 50 GB only if all omitted selected blocks and metadata fit the remaining approximately 11.745 GB. No per-anchor multiplier appears. If L_all=m, interval payload alone becomes 8 GB and the corresponding fully duplicated refresh exceeds 50 GB. Actual compressibility matters.

Packed temporary key records can use 28 bytes: positive `(b:u32,ID:u64,position:u32)` plus zero `(ID:u64,position:u32)`. Their peak level scratch is about 5.637 GB, separate from retained state. External sorting of 1B membership pairs in a chosen 8-byte format can need 16 GB of payload scratch. Raw input coexistence, ID-map construction and sort traffic remain paid. Thus a 50 GB retained limit is not permission to assume a 50 GB physical disk suffices. The prototype uses 40-byte key levels, 16-byte tree nodes and wider metadata; its measured bytes must not be reported as the packed table.

A provisional worker reservation is 256 MB sort, 32 MB merge, 64 MB shared tree/metadata cache, 32 MB other file I/O, 16 MB output and 200 MB runtime/allocator allowance: 600 MB plus an explicit 96-byte packed heap slot times admitted min(n-n_self,R+k). R=100k,k=50 adds about 9.6 MB; R=30M adds about 2.88 GB and breaches a provisional 3 GB worker allowance. Neither number proves fit on a physical 4 GB machine. Concurrency, kernel cache residency and page-cache pressure remain measured obligations.

Full all-source output at 100M sources, k=50 and the selected 24-byte record format is **120 GB**, before headers. The smaller exported source set above is a different admitted job. A bounded blocking sink may pause the producer but cannot erase total output volume or guarantee a completion deadline. The probe writes one record at a time to a pre-admitted staging file, renames only complete output, and cleans up after an injected sink failure. No unbounded client queue is used; fsync/crash durability is not implemented.

### Refresh Boundary

Initial refresh means a coherent rebuild of memberships/intervals, cardinalities, ID mappings and both tree fields, with old readers pinned. A changed target can change feature intervals as well as its positive-order key. A fixed-rank point update could update O(log n) tree nodes, but this does not supply a bounded interval-directory update protocol or crash-consistent publication. Reordering targets invalidates all positional artifacts. Deletions/tombstones would add eligibility handling to RMQ and are outside this fixed-eligible-domain contract. Shared source-as-target changes require the new snapshot; an external frozen source may query either pinned snapshot explicitly.

## Primary Comparison And Novelty Boundary

These are inspected primary sources, not search-title evidence. Access limits are stated where relevant.

| Source | Closest mechanism and consequence |
| --- | --- |
| [Arroyuelo et al., Hybrid compression of inverted lists for reordered document collections](https://www.sciencedirect.com/science/article/abs/pii/S0306457317307057), publisher abstract and [repository PDF's indexed opening](https://repositorio.uchile.cl/bitstream/handle/2250/169432/Hybrid_compression.pdf?sequence=1) | Reordering, hybrid gap/run encoding, implicit interval decompression and WAND processing are already studied. Direct repository opening returned a landing page; no uninspected detailed algorithm is attributed. Avoiding expansion of long posting runs is not itself new. |
| [Lemire, Ssi-Yan-Kai, Kaser, Consistently faster and smaller compressed bitmaps with Roaring](https://arxiv.org/pdf/1603.06549), run containers and section on container operations, pp. 5-10 | Run/array/bitmap hybrids and run-to-run operations are established. A run representation need not enumerate every integer, but not every operation or representation avoids enumeration. Our overlap-count field is an integer sum, not just Boolean union. |
| [Ding and Suel, Faster Top-k Document Retrieval Using Block-Max Indexes](https://research.engineering.nyu.edu/~suel/papers/bmw.pdf), sections 2, 4 and 5 | Safe upper bounds, compressed-list skipping, shallow block access and document-at-a-time evaluation are established. Section 4 also discusses earlier aligned-boundary interval processing. Here interval summaries are exact overlap counts and argmins produce winners directly; this does not prove superiority over an adapted block/run executor. |
| [Akram and Saxena, Sorted Range Reporting and Range Minima Queries](https://arxiv.org/pdf/2104.02461), section 2, Algorithm 1 | RMQ winner extraction, left/right splitting and a binary heap are explicitly the prior algorithm. Its RAM-model RMQ bounds are not our physical-disk bounds. Our multi-run executor is this known procedure initialized with several disjoint intervals, with the query's exact c selecting an order/key. |
| [Gawrychowski et al., Compressed Range Minimum Queries](https://arxiv.org/pdf/1902.04427), introduction and Theorems 1.1-1.4 | Grammar and Cartesian-tree compression support RMQ in compressed space; the representations can differ substantially in size. This does not grant a compressed index for arbitrary `(b,ID)` or ID order, or a free small-memory builder. We deliberately price an uncompressed external tree; a succinct/grammar replacement needs separate construction and fetch accounting. |
| [Mann et al., SWOOP: top-k similarity joins over set streams](https://pmc.ncbi.nlm.nih.gov/articles/PMC11666680/), indexed full-text sections on query semantics, candidate generation, stock and experiments; [publisher PDF](https://link.springer.com/content/pdf/10.1007/s00778-024-00880-x.pdf) | The task is continuously maintaining globally best pairs in a sliding window, with candidates and a stock for expiration. This differs from per-source top-k against one frozen target snapshot. Avoiding all pairs and supporting multiple set metrics are already established goals. Its stream guarantees and experiments do not transfer to this fixed-rank disk plan. |

The direct PMC open was challenged by the site; its indexed primary full text and publisher PDF were available. No result about the internal details of an unavailable section is needed here. No quoted performance number from these papers is used to predict this implementation.

**Strong matched competitor:** supply an ordinary executor with the same L interval postings, exact event sweep, shared two-order RMQ and complete tie/self semantics. It obtains exactly the same R+k state and RMQ bound. No asymptotic separation from that competitor is established. A measured expanded-overlap baseline below isolates eliminated per-target work; it is not the strongest compressed-retrieval baseline and cannot establish novelty.

The candidate contribution is a **representation-sensitive composition**: exact query overlap runs plus a query-independent two-order sufficient index for Jaccard, with whole-workflow admission. It supports nonidentical targets and removes per-anchor linear state. The run, sweep, heap and RMQ primitives are known. Whether this precise composition/contract is unpublished remains unresolved; the inspected run-aware retrieval literature is close enough that a new-algorithm claim would be premature. A publishable result would need a stronger comparison/theorem or empirical lifecycle separation against a same-representation engine.

## Revised After Counterexample

| Initial tempting shortcut | Failure | Revision |
| --- | --- | --- |
| Physical leftmost target wins a zero run | Rank 0 has ID 9, rank 1 has ID 1; all scores zero. | Separate original-ID RMQ, independent of physical rank. |
| Minimum ID wins a positive run | ID 1 has {0,1,2}, ID 9 has {0}; S={0}. | Positive `(b,ID)` argmin; ID 9 wins 1 versus 1/3. |
| Exclude self after selecting k | Self is the sole score-1 target and k=1; deleting it leaves no replacement. | Remove its position before initial RMQs/selection. |
| Only emit positive runs | Missing-feature or empty S has no positive overlap but still requires a complete zero tail. | Sweep full [0,n), including both boundary gaps. |
| R=1 means constant query input work | Many source features each cover [0,n). | Charge all L intervals and 2L events. |
| Build RMQ by loading all leaves | A global index can still violate RAM during preparation. | Sequential two-key level builder and paid level scratch. |
| Mixed tree and posting generations are usable when source is unchanged | A target cardinality or rank change makes an old argmin/order incorrect. | Reject mismatched manifests; pin or rebuild every dependent block. |

## Executable Finite-File Probe

One Python 3.11 fence; temporary artifacts only. The fixture writer and independent oracle use tiny collections. The actual index constructor accepts only prepared file metadata, not the fixture targets. It and the query kernel keep only four-record sort buffers, two merge cursors, constant sweep/build state, the explicitly admitted heap and fixed counters. Four decoded Python records are not a claim of four wire-record widths of RSS. No query reads a target feature row, constructs an n/q overlap array, or caches all tree nodes. Binary reads count payload and operations, not physical device misses. The expanded oracle baseline is intentionally a separate, charged miniature comparator.

```python
from collections import Counter
from fractions import Fraction
from heapq import heappush, heappop, merge
from itertools import groupby, product
from pathlib import Path
from random import Random
from struct import Struct
from tempfile import TemporaryDirectory
from time import perf_counter

U = Struct('<Q')
META, INVERSE = Struct('<qQ'), Struct('<qQ')
PAIR, INTERVAL = Struct('<QQ'), Struct('<QQ')
DIRECTORY, EVENT, RUN = Struct('<QQQ'), Struct('<Qq'), Struct('<QQq')
NODE, KEYS, OUTPUT = Struct('<qq'), Struct('<Qqqqq'), Struct('<qQQQ')
stats = Counter()

def read_fixed_file_records(path, layout, tag, offset=0, count=None):
    with open(path, 'rb', buffering=0) as stream:
        stream.seek(offset)
        while count is None or count:
            raw = stream.read(layout.size)
            stats[tag+'_bytes'] += len(raw)
            if not raw:
                assert count is None
                return
            assert len(raw) == layout.size
            stats[tag+'_records'] += 1
            if count is not None:
                count -= 1
            yield layout.unpack(raw)

def write_fixed_file_record(stream, layout, record, tag):
    raw = layout.pack(*record)
    assert stream.write(raw) == len(raw)
    stats[tag+'_bytes'] += len(raw)

def lookup_ordered_file_record(path, layout, size, value, tag):
    lo, hi = 0, size
    while lo < hi:
        mid = (lo+hi)//2
        row = next(read_fixed_file_records(path, layout, tag, mid*layout.size, 1))
        if row[0] < value: lo = mid+1
        elif row[0] > value: hi = mid
        else: return row
    return None

class Arena:
    def __init__(self, root, cap):
        self.root, self.cap, self.live = root, cap, 0

    def write(self, path, layout, records):
        count = 0
        with open(path, 'wb', buffering=0) as stream:
            for row in records:
                assert self.live+layout.size <= self.cap, 'disk payload admission'
                write_fixed_file_record(stream, layout, row, 'arena_write')
                if layout is OUTPUT: stats['output_write_bytes'] += layout.size
                count += 1; self.live += layout.size
                stats['arena_peak'] = max(stats['arena_peak'], self.live)
        return count

    def remove(self, path):
        self.live -= path.stat().st_size; path.unlink()
        assert self.live >= 0

def sort_finite_record_stream(records, layout, count, arena, capacity=4, fanin=2):
    assert 2*count*layout.size <= arena.cap
    buffer, runs, seen = [], 0, 0
    def filename(generation, number):
        return arena.root/('sort-%d-%d' % (generation, number))
    for record in records:
        buffer.append(record); seen += 1
        stats['sort_buffer_peak'] = max(stats['sort_buffer_peak'], len(buffer))
        if len(buffer) == capacity:
            buffer.sort(); arena.write(filename(0,runs), layout, buffer)
            buffer.clear(); runs += 1
    assert seen == count
    if buffer or not runs:
        buffer.sort(); arena.write(filename(0,runs), layout, buffer)
        buffer.clear(); runs += 1
    generation = 0
    while runs > 1:
        outputs = 0
        for start in range(0,runs,fanin):
            end = min(start+fanin,runs)
            cursors = [read_fixed_file_records(filename(generation,i),layout,'sort_read')
                       for i in range(start,end)]
            stats['fanin_peak'] = max(stats['fanin_peak'], len(cursors))
            arena.write(filename(generation+1,outputs),layout,merge(*cursors))
            for i in range(start,end): arena.remove(filename(generation,i))
            outputs += 1
        runs = outputs; generation += 1
    stats['sort_levels'] += generation
    return filename(generation,0)

def combine_two_key_records(left, right):
    if left[2] < 0: return right
    if right[2] < 0: return left
    positive = min((left,right), key=lambda row:(row[0],row[1]))
    zero = min((left,right), key=lambda row:row[3])
    return positive[:3]+zero[3:]

def write_normalized_target_fixture(root, targets, epoch=0):
    began = perf_counter(); root.mkdir(); memberships = 0
    with open(root/'meta','wb',buffering=0) as meta, open(root/'pairs','wb',buffering=0) as pairs:
        for position,(node,raw) in enumerate(targets):
            values = sorted(set(raw))
            write_fixed_file_record(meta,META,(node,len(values)),'fixture_write')
            for feature in values:
                write_fixed_file_record(pairs,PAIR,(feature,position),'fixture_write')
                memberships += 1
    stats['fixture_seconds'] += perf_counter()-began
    return root,len(targets),memberships,epoch

class Snapshot:
    def __init__(self, prepared_files):
        began = perf_counter()
        root,self.n,self.memberships,epoch = prepared_files
        self.root = root
        self.token = (epoch,'binary','fixed-rank','original-ID-ties')
        self.post_token = self.tree_token = self.meta_token = self.inverse_token = self.token
        self.meta, self.inverse = root/'meta', root/'inverse'
        self.pairs, self.posts, self.directory = root/'pairs', root/'intervals', root/'directory'
        self.tree = root/'tree'; self.N = 1 << max(0,(self.n-1).bit_length()) if self.n else 1
        with TemporaryDirectory(dir=root) as temporary:
            arena = Arena(Path(temporary),2*INVERSE.size*self.n)
            records = ((node,pos) for pos,(node,_) in enumerate(read_fixed_file_records(self.meta,META,'build_meta')))
            ordered = sort_finite_record_stream(records,INVERSE,self.n,arena)
            previous = None
            for node,_ in read_fixed_file_records(ordered,INVERSE,'build_inverse'):
                assert previous is None or node > previous
                previous = node
            arena.live -= ordered.stat().st_size; ordered.replace(self.inverse)
            assert arena.live == 0
        self.F, self.L_all = 0,0
        with TemporaryDirectory(dir=root) as temporary:
            arena = Arena(Path(temporary),2*PAIR.size*self.memberships)
            ordered = sort_finite_record_stream(read_fixed_file_records(self.pairs,PAIR,'build_pairs'),
                                                PAIR,self.memberships,arena)
            with open(self.posts,'wb',buffering=0) as posts, open(self.directory,'wb',buffering=0) as directory:
                offset = 0
                for feature,rows in groupby(read_fixed_file_records(ordered,PAIR,'build_pairs_sorted'),key=lambda r:r[0]):
                    start, end, count = None,None,0
                    for _,position in rows:
                        if start is None: start,end = position,position+1
                        elif position == end-1: continue
                        elif position == end: end += 1
                        else:
                            assert position > end
                            write_fixed_file_record(posts,INTERVAL,(start,end),'index_write'); count += 1
                            start,end = position,position+1
                    write_fixed_file_record(posts,INTERVAL,(start,end),'index_write'); count += 1
                    write_fixed_file_record(directory,DIRECTORY,(feature,offset,count),'index_write')
                    offset += count*INTERVAL.size; self.F += 1; self.L_all += count
            arena.remove(ordered)
        with TemporaryDirectory(dir=root) as temporary, open(self.tree,'wb',buffering=0) as tree:
            arena = Arena(Path(temporary),2*KEYS.size*self.N)
            write_fixed_file_record(tree,NODE,(-1,-1),'tree_write')
            def leaves():
                rows = iter(read_fixed_file_records(self.meta,META,'build_meta'))
                for position in range(self.N):
                    if position < self.n:
                        node,b = next(rows); row = (b,node,position,node,position)
                    else: row = (0,0,-1,0,-1)
                    tree.seek((self.N+position)*NODE.size)
                    write_fixed_file_record(tree,NODE,(row[2],row[4]),'tree_write')
                    yield row
            old = arena.root/'level-0'; arena.write(old,KEYS,leaves())
            width,generation = self.N,0
            while width > 1:
                new = arena.root/('level-%d' % (generation+1))
                def parents():
                    rows = iter(read_fixed_file_records(old,KEYS,'level_read'))
                    for index in range(width//2):
                        parent = combine_two_key_records(next(rows),next(rows))
                        tree.seek((width//2+index)*NODE.size)
                        write_fixed_file_record(tree,NODE,(parent[2],parent[4]),'tree_write')
                        yield parent
                arena.write(new,KEYS,parents()); arena.remove(old)
                old,width,generation = new,width//2,generation+1
            arena.remove(old)
            assert arena.live == 0
        assert self.tree.stat().st_size == 2*self.N*NODE.size
        stats['prepare_seconds'] += perf_counter()-began

    def feature(self, value):
        return lookup_ordered_file_record(self.directory,DIRECTORY,self.F,value,'feature_lookup')

    def self_position(self, node):
        row = lookup_ordered_file_record(self.inverse,INVERSE,self.n,node,'id_lookup') if node is not None else None
        return row[1] if row else None

    def metadata(self, position):
        return next(read_fixed_file_records(self.meta,META,'rmq_meta',position*META.size,1))

    def rmq(self, lo, hi, positive, local):
        assert 0 <= lo < hi <= self.n
        local['rmq_calls'] += 1
        left,right,winner = lo+self.N,hi+self.N,None
        def consider(index):
            nonlocal winner
            pair = next(read_fixed_file_records(self.tree,NODE,'rmq_node',index*NODE.size,1))
            position = pair[0 if positive else 1]
            assert position >= 0
            node,b = self.metadata(position)
            key = (b,node) if positive else (node,)
            if winner is None or key < winner[0]: winner = key,position,node,b
            local['rmq_probes'] += 1
        while left < right:
            if left&1: consider(left); left += 1
            if right&1: right -= 1; consider(right)
            left //= 2; right //= 2
        assert winner is not None and lo <= winner[1] < hi
        return winner[1:]

def write_normalized_source_file(path, values):
    with open(path,'wb',buffering=0) as stream:
        for value in sorted(set(values)):
            write_fixed_file_record(stream,U,(value,),'source_write')

def sweep_coalesced_overlap_runs(events, n, maximum=None):
    begin,value,last = 0,0,-1
    for position,rows in groupby(events,key=lambda row:row[0]):
        assert last < position <= n; last = position
        delta = sum(delta for _,delta in rows)
        if delta:
            if position > begin: yield begin,position,value
            begin = position; value += delta
            if maximum is not None: assert 0 <= value <= maximum
    if begin < n: yield begin,n,value
    assert value == 0, 'unbalanced endpoints'

def construct_source_overlap_runs(snapshot, source, arena, local):
    a,L,previous = 0,0,None
    for (feature,) in read_fixed_file_records(source,U,'source_read'):
        assert previous is None or previous < feature
        previous = feature; a += 1
        entry = snapshot.feature(feature); L += entry[2] if entry else 0
    E = 2*L
    assert 2*EVENT.size*E+RUN.size*min(snapshot.n,E+1)+local['output_bytes'] <= arena.cap
    def events():
        for (feature,) in read_fixed_file_records(source,U,'source_read'):
            entry = snapshot.feature(feature)
            if entry:
                end_previous = -1
                for lo,hi in read_fixed_file_records(snapshot.posts,INTERVAL,'query_intervals',entry[1],entry[2]):
                    assert 0 <= lo < hi <= snapshot.n and lo > end_previous
                    end_previous = hi
                    yield lo,1; yield hi,-1
    ordered = sort_finite_record_stream(events(),EVENT,E,arena)
    path = arena.root/'field'
    R = arena.write(path,RUN,sweep_coalesced_overlap_runs(read_fixed_file_records(ordered,EVENT,'event_read'),snapshot.n,a))
    arena.remove(ordered)
    local.update(a=a,L=L,E=E,R=R)
    assert R <= min(snapshot.n,E+1)
    return path

def exact_jaccard_score_key(a,b,c):
    assert 0 <= c <= min(a,b)
    return Fraction(c,a+b-c) if c else Fraction(0)

def select_runwise_exact_answers(snapshot, source, sid, k, heap_cap=100000, disk_cap=10**8,
                                 output_cap=10**6, fault=None, source_epoch=None, fail_after=None):
    began = perf_counter()
    assert snapshot.token == snapshot.post_token == snapshot.tree_token == snapshot.meta_token == snapshot.inverse_token
    assert source_epoch is None or source_epoch == snapshot.token[0]
    assert k >= 0
    selfpos = snapshot.self_position(sid)
    count = min(k,snapshot.n-int(selfpos is not None))
    local = Counter(k=count,output_bytes=count*OUTPUT.size)
    assert local['output_bytes'] <= output_cap and local['output_bytes'] <= disk_cap
    before_expansion = stats['build_pairs_records']
    with TemporaryDirectory(dir=snapshot.root,prefix='query-') as temporary:
        arena = Arena(Path(temporary),disk_cap)
        field,heap = None,[]
        if count:
            field = construct_source_overlap_runs(snapshot,source,arena,local)
            admitted = min(snapshot.n-int(selfpos is not None),local['R']+count)
            assert admitted <= heap_cap, 'heap admission'
            def insert(lo,hi,c):
                if lo >= hi: return
                if c == 0 and fault == 'position-zero':
                    pos = lo; node,b = snapshot.metadata(pos)
                else:
                    pos,node,b = snapshot.rmq(lo,hi,c>0 and fault!='id-positive',local)
                score = exact_jaccard_score_key(local['a'],b,c)
                heappush(heap,(-score,node,pos,b,c,lo,hi))
                local['heap_peak'] = max(local['heap_peak'],len(heap))
                assert len(heap) <= heap_cap
            for lo,hi,c in read_fixed_file_records(field,RUN,'field_read'):
                if fault == 'omit-zero' and c == 0: continue
                if selfpos is not None and lo <= selfpos < hi and fault != 'keep-self':
                    insert(lo,selfpos,c); insert(selfpos+1,hi,c)
                else: insert(lo,hi,c)
        def answers():
            for rank in range(count):
                assert heap, 'incomplete result'
                _,node,pos,b,c,lo,hi = heappop(heap)
                if fail_after is not None and rank == fail_after: raise OSError('injected blocked/failed sink')
                yield node,local['a'],b,c
                if rank+1 < count:
                    insert(lo,pos,c); insert(pos+1,hi,c)
        stage,final = arena.root/'staged',arena.root/'published'
        written = arena.write(stage,OUTPUT,answers())
        assert written == count
        stage.replace(final); local['published'] = 1
        if count:
            assert local['heap_peak'] <= local['R']+count
            assert local['rmq_calls'] <= local['R']+2*count-1
            assert local['rmq_probes'] <= local['rmq_calls']*2*snapshot.N.bit_length()
        assert stats['build_pairs_records'] == before_expansion
        # Only the independent test sink materializes already-published output.
        result = list(read_fixed_file_records(final,OUTPUT,'output_check'))
        arena.remove(final)
        if field is not None: arena.remove(field)
        assert arena.live == 0
    stats['query_seconds'] += perf_counter()-began
    return result,local

def compute_independent_set_oracle(targets, values, sid, k):
    source = set(values)
    rows = [(node,len(source),len(set(raw)),len(source.intersection(raw)))
            for node,raw in targets if node != sid]
    return sorted(rows,key=lambda r:(-exact_jaccard_score_key(*r[1:]),r[0]))[:k]

def compare_expanded_overlap_baseline(snapshot, values, sid, k):
    # Deliberately separate non-production comparator: charged n-entry count array.
    overlaps = [0]*snapshot.n; visits = 0
    for feature in sorted(set(values)):
        entry = snapshot.feature(feature)
        if entry:
            for lo,hi in read_fixed_file_records(snapshot.posts,INTERVAL,'baseline_intervals',entry[1],entry[2]):
                for position in range(lo,hi): overlaps[position] += 1; visits += 1
    rows = []
    for position,(node,b) in enumerate(read_fixed_file_records(snapshot.meta,META,'baseline_meta')):
        if node != sid: rows.append((node,len(set(values)),b,overlaps[position]))
    return sorted(rows,key=lambda r:(-exact_jaccard_score_key(*r[1:]),r[0]))[:k],visits

checks,rng = Counter(),Random(20260923)
started = perf_counter()
with TemporaryDirectory(prefix='runwise-selection-') as temporary:
    root = Path(temporary)
    for case in range(64):
        ids = [7*j-90 for j in range(case%19)]; rng.shuffle(ids)
        targets = [(node,[f for f in range(9) if rng.randrange(3)==0]) for node in ids]
        if targets and case%3==0: targets[0][1].extend(targets[0][1][:])
        snapshot = Snapshot(write_normalized_target_fixture(root/('random%d'%case),targets))
        source = snapshot.root/'source'
        for step in range(10):
            values = set() if step==0 else {f for f in range(12) if rng.randrange(3)==0}
            sid = ids[step%len(ids)] if ids and step%2 else None
            k = (0,1,3,30,2)[step%5]
            write_normalized_source_file(source,values)
            answer,info = select_runwise_exact_answers(snapshot,source,sid,k)
            assert answer == compute_independent_set_oracle(targets,values,sid,k)
            checks['random_answers'] += 1
            checks['max_R'] = max(checks['max_R'],info['R'])
            if case%8==0:
                other,_ = compare_expanded_overlap_baseline(snapshot,values,sid,k)
                assert other == answer; checks['matched_expanded'] += 1
        if case < 20:
            for lo in range(snapshot.n):
                for hi in range(lo+1,snapshot.n+1):
                    for positive in (False,True):
                        pos,node,b = snapshot.rmq(lo,hi,positive,Counter())
                        expected = min(range(lo,hi),key=lambda j:(len(set(targets[j][1])),targets[j][0]) if positive else (targets[j][0],))
                        assert pos == expected
                        checks['rmq_ranges'] += 1
    subsets = [set(),{0},{1},{0,1}]
    for number,assignment in enumerate(product(subsets,repeat=3)):
        targets = list(zip((9,-3,2),assignment))
        snapshot = Snapshot(write_normalized_target_fixture(root/('exhaust%d'%number),targets)); source = snapshot.root/'source'
        for values,sid,k in product(subsets,(None,-3),(0,1,2,5)):
            write_normalized_source_file(source,values)
            answer,_ = select_runwise_exact_answers(snapshot,source,sid,k)
            assert answer == compute_independent_set_oracle(targets,values,sid,k)
            checks['exhaustive_answers'] += 1
    for n in (128,512,2048):
        ids = list(range(n)); rng.shuffle(ids)
        targets = [(ids[j],{0,10000+j}|set(range(100000,100000+j%5))) for j in range(n)]
        snapshot = Snapshot(write_normalized_target_fixture(root/('broad%d'%n),targets)); source = snapshot.root/'source'
        write_normalized_source_file(source,{0})
        prior = stats['query_intervals_records']
        answer,info = select_runwise_exact_answers(snapshot,source,None,10)
        other,visits = compare_expanded_overlap_baseline(snapshot,{0},None,10)
        assert answer == other == compute_independent_set_oracle(targets,{0},None,10)
        assert info['L']==info['R']==1 and info['E']==2 and visits==n
        assert stats['query_intervals_records']-prior == 1
        print('nonexpansion',n,dict(info),'expanded_visits',visits)
        checks['broad_nonidentical'] += 1
    variants = [
        ('position-zero',[(9,{2}),(1,set())],{7},None,1),
        ('id-positive',[(1,{0,1,2}),(9,{0})],{0},None,1),
        ('keep-self',[(0,{0}),(9,{0,1})],{0},0,1),
    ]
    for fault,targets,values,sid,k in variants:
        snapshot = Snapshot(write_normalized_target_fixture(root/fault,targets)); source = snapshot.root/'source'
        write_normalized_source_file(source,values)
        expected = compute_independent_set_oracle(targets,values,sid,k)
        answer,_ = select_runwise_exact_answers(snapshot,source,sid,k)
        wrong,_ = select_runwise_exact_answers(snapshot,source,sid,k,fault=fault)
        assert answer == expected and wrong != expected
        print('falsified',fault,'wrong',wrong,'correct',expected)
        checks['falsified_variants'] += 1
    # Exact cross-run tie: 1/(2+1-1) equals 2/(2+4-2), so ID, not c or b, decides.
    targets = [(9,{0}),(1,{0,1,2,3})]
    tied = Snapshot(write_normalized_target_fixture(root/'cross-run-tie',targets)); source = tied.root/'source'
    write_normalized_source_file(source,{0,1})
    answer,info = select_runwise_exact_answers(tied,source,None,2)
    assert answer == compute_independent_set_oracle(targets,{0,1},None,2)
    assert info['R']==2 and [row[0] for row in answer]==[1,9]
    checks['cross_run_tie'] = 1
    # Boundary cancellation: two disjoint features tile the full target domain.
    targets = [(100-j,{0,1 if j<16 else 2,1000+j}) for j in range(32)]
    snapshot = Snapshot(write_normalized_target_fixture(root/'cancellation',targets)); source = snapshot.root/'source'
    write_normalized_source_file(source,{1,2})
    answer,info = select_runwise_exact_answers(snapshot,source,None,7)
    assert answer == compute_independent_set_oracle(targets,{1,2},None,7)
    assert info['L']==2 and info['E']==4 and info['R']==1
    with TemporaryDirectory(dir=root) as temporary:
        arena = Arena(Path(temporary),1024)
        events = [(0,-1),(32,1),(0,1),(16,-1),(16,1),(32,-1)]
        ordered = sort_finite_record_stream(iter(events),EVENT,len(events),arena)
        field = list(sweep_coalesced_overlap_runs(read_fixed_file_records(ordered,EVENT,'edit_read'),32))
        assert field == [(0,32,0)]; arena.remove(ordered)
    checks['cancellation_cases'] = 2
    # R=1 does not imply small L: all four query features cover every target.
    dense = Snapshot(write_normalized_target_fixture(root/'many-broad',[(30-j,{0,1,2,3}) for j in range(20)]))
    source = dense.root/'source'; write_normalized_source_file(source,{0,1,2,3})
    _,info = select_runwise_exact_answers(dense,source,None,3)
    assert info['L']==4 and info['R']==1
    # The opposite regime: alternating singleton postings create one run per target.
    targets = [(200-j,({0} if j%2==0 else set())|{2000+j}) for j in range(64)]
    fragmented = Snapshot(write_normalized_target_fixture(root/'fragmented',targets)); source = fragmented.root/'source'
    write_normalized_source_file(source,{0})
    answer,info = select_runwise_exact_answers(fragmented,source,None,1)
    assert answer == compute_independent_set_oracle(targets,{0},None,1)
    assert info['R']==64 and info['L']==32
    print('fragmented',dict(info))
    try: select_runwise_exact_answers(fragmented,source,None,1,heap_cap=32)
    except AssertionError: checks['fragmentation_rejected'] += 1
    else: raise AssertionError('fragmented field escaped admission')
    assert not list(fragmented.root.glob('query-*'))
    # A full rebuild changes cardinality ordering; old readers remain coherent.
    old = Snapshot(write_normalized_target_fixture(root/'old',[(10,{0,1}),(20,{0,1,2})],epoch=0))
    source = old.root/'source'; write_normalized_source_file(source,{0})
    old_answer,_ = select_runwise_exact_answers(old,source,None,1)
    refresh_start = perf_counter()
    new = Snapshot(write_normalized_target_fixture(root/'new',[(10,{0,1}),(20,{0})],epoch=1))
    new_answer,_ = select_runwise_exact_answers(new,source,None,1)
    stats['refresh_seconds'] += perf_counter()-refresh_start
    assert old_answer[0][0]==10 and new_answer[0][0]==20
    assert select_runwise_exact_answers(old,source,None,1)[0] == old_answer
    for kind in ('tree','post','meta','inverse','source','heap','disk','output','sink','zero-tail'):
        kwargs = {}
        if kind in ('tree','post','meta','inverse'): setattr(new,kind+'_token',old.token)
        if kind=='source': kwargs=dict(source_epoch=0)
        if kind=='heap': kwargs=dict(heap_cap=0)
        if kind=='disk': kwargs=dict(disk_cap=2*OUTPUT.size)
        if kind=='output': kwargs=dict(output_cap=0)
        if kind=='sink': kwargs=dict(fail_after=1)
        if kind=='zero-tail': write_normalized_source_file(source,{99}); kwargs=dict(fault='omit-zero')
        try:
            select_runwise_exact_answers(new,source,None,2,**kwargs)
        except (AssertionError,OSError): checks['rejected_'+kind] += 1
        else: raise AssertionError('undetected failure '+kind)
        new.tree_token = new.post_token = new.meta_token = new.inverse_token = new.token
        assert not list(new.root.glob('query-*'))
    assert stats['sort_buffer_peak']<=4 and stats['fanin_peak']<=2
print('checks',dict(sorted(checks.items())))
print('logical', {k:v for k,v in sorted(stats.items()) if not k.endswith('seconds')})
print('seconds',{k:round(v,6) for k,v in stats.items() if k.endswith('seconds')})
print('whole_seconds',round(perf_counter()-started,6))
```

## Verification Receipt And Open Gap

The retained source was executed successfully on 2026-09-20, including actual binary files for preparation, interval generation, endpoint sort, run spool, tree construction, random RMQs, complete output, refresh and failure cleanup. Reproduce from the repository root:

```sh
awk '/^```python/ {active=1; next} /^```/ {active=0} active {print}' research_algorithms_20260920/Similarity-Runwise-Selection.md | /usr/bin/time -l python3.11
```

SHA-256 of the extracted Python source including its final newline: `dd17dadda76e7eb480e362a7c3bea0921ec290e81132a31aece11352d0cf6842`.

| Check | Observed result |
| --- | --- |
| Randomized oracle comparison | 640/640 exact answers over 64 fixtures, seed 20260923; shuffled/negative original IDs, raw duplicate memberships, unknown features, empty domains/sources and varied k. |
| Exhaustive small domain | 2,048/2,048: 64 ordered three-target assignments over two features x 4 source sets x 2 self modes x 4 k values. Exhaustive only over that stated domain. |
| Disk RMQ direct comparison | 2,280 interval/order queries matched independent argmins, including non-power-of-two target counts and both tie orders. |
| Expanded-overlap comparator | 80/80 randomized matched outputs, plus all three separating-family queries. Its n-entry overlap array and target visits are charged comparator work, never query-kernel state. |
| Nonidentical broad-feature family | At n=128/512/2,048: q=n relative to an empty anchor, L=R=1, E=2 and one posting interval read. Top-10 took 18/19/19 RMQ calls, 66/98/133 canonical-node probes and heap peaks 9/10/10. Expanded visits were 128/512/2,048. Each node probe also fetches winner metadata. |
| Counterexample variants | Three incorrect implementations produced the wrong answers below; correct code matched the oracle. Omitting zero runs was separately rejected for incomplete output. |
| Cancellation and ties | Two source half-intervals coalesced to one full-domain c=1 run. Six signed edit events coalesced to one zero-delta run. A cross-run 1/2 versus 2/4 score tie was resolved by original ID. |
| Fragmentation | L=32, E=64, R=64 on 64 targets; top-1 initialized 64 heap entries/RMQs. A 32-entry selector cap rejected the query and removed its temporary files. |
| Version/resource/output failures | Stale tree, posting, metadata, inverse-ID and shared-source epochs rejected. Heap, event/output-disk and output-only reservations rejected. Failure after one staged result left no published/temporary query artifact. |
| Refresh | Full rebuild changed winner ID 10 to ID 20; the pinned old index still returned 10. |
| Bounded execution | Run-sort buffer peak 4 records, fan-in peak 2, maximum live arena payload 262,048 bytes across measured builder/query phases. Every completed query passed the heap and RMQ-call/probe bounds. |

Falsification tuples have `(original_ID,a,b,c)` format:

```text
physical position for zero: wrong (9,1,1,0), correct (1,1,0,0)
ID minimum for positive:    wrong (1,1,3,1), correct (9,1,1,1)
unremoved self position:    wrong (0,1,1,1), correct (9,1,2,1)
```

Run-wide payload receipts, including all direct-RMQ tests, baselines, failed and deliberately broken cases: 262,240 fixture-write bytes; 181,688 interval/directory-write bytes; 122,240 permanent tree-write bytes; 2,560,656 external-sort read bytes; 3,556,432 arena-write bytes including sort generations, builder levels, query fields and output; 74,848 source-posting interval-read bytes; 149,696 event-read bytes; 136,872 run-field read bytes; 13,434 tree-node reads / 214,944 bytes; 214,960 RMQ metadata-read bytes. Output writes were 145,312 bytes; published-output check reads were 145,280 bytes. The 32-byte difference is the one deliberately failed staged result. Renaming a sorted inverse map or staged output does not create a second payload write. These counters are not physical block I/O, filesystem allocation, fsync durability or compressed-format sizes.

The recorded run took 4.128690 s inside the probe and 4.26 s real under `/usr/bin/time -l`. Tiny fixture normalization/writing took 0.048785 s, file-only preparation 1.602400 s and completed query/output/check calls 2.099342 s. The tiny refresh fixture/build/new-query interval took 0.001712 s and overlaps those categories. Whole time additionally includes oracle/baseline work, failed cases, source-file preparation and cleanup; the named timers are not a disjoint partition. Timing is illustrative, not an end-to-end speed comparison with a tuned competitor.

Lead replay on 2026-09-20 exited 0 with the same correctness and logical-work receipts, including 640 random answers, 2,048 exhaustive answers and 2,280 direct RMQs. The 2,048-target broad-feature query again used one interval, 19 RMQs, 133 canonical-node probes and ten peak heap entries. The replay reported 3.713192 seconds for the whole test program, including its oracles; this variation is not a performance finding. No old A05 suite was rerun and no physical cap was enforced.

Maximum reported RSS was 21,774,336 bytes with zero swaps on an arm64 host having 25,769,803,776 physical bytes. There was **no enforced 4 GB machine limit**, no cold-cache control and no 100M-target experiment. The RSS includes tiny fixture/oracle collections and the deliberately expanded baseline. Production allocator accounting, full raw-graph ingestion, crash durability, concurrent refresh, live slow-sink liveness and a general external heap were not implemented/tested.

**Surviving result:** q can equal n while the exact query consumes one interval and O(k) RMQs, for distinct unequal-cardinality targets, using one shared O(n) index and no anchor index or changed-target list. R+k heap state, full zero/tie/self semantics and a file-only bottom-up builder survive the tests. This is a materially different repository architecture from per-anchor exception expansion, not evidence that RMQ range selection is new.

The remaining question is whether useful data/orderings yield small enough L and R, over a long enough snapshot lifetime, to amortize the shared index and its refresh against a strong run-aware competitor. The illustrative 50M-interval storage model fits both generations at about 38.255 GB; the singleton-interval model does not. A same-run executor matches the proved query bound. Global novelty and physical 4 GB / 50 GB whole-workflow validation remain unresolved.
