# Similarity Through A Paid Exception Merge Index

Date: 2026-09-20. A05 alternative architecture. Ownership: this file only. Research procedure and executable miniature, not a production implementation or established novelty claim.

## Premise Check

**Thesis:** pay for a complete per-anchor target ordering, then answer exact Jaccard top-k by merging unchanged bucket streams with exactly corrected exceptions. There is no witness set U, cardinality acceptance envelope, survivor gate, or data-dependent fallback scan. Every coherent, resource-admitted query has a complete answer. Resource/deadline admission can still fail; this is not a universal completion claim on a 4 GB machine.

Read the portfolio README and the lead's [Similarity-Envelope-Certificate.md](Similarity-Envelope-Certificate.md), including its source and reported 128-fixture / 6,400-query evidence. That work compresses outsiders to an envelope and may reject a query. This alternative keeps **every target** in the bucket index and spends Theta(n) baseline records per anchor. Its evidence is independent below. No envelope, synthesis, A06, review, or shared journal file is changed.

The inherited [signed-reducer and exact-score contract](Certified-Similarity.md) supplies the source-change identity, not a novel component. Exact equality classes, source batching and multiresolution bounds are not the mechanism here.

## Expert Lenses And Alternatives

The retrieval lens checks whether all omitted candidates are dominated; the external-memory lens counts random exception lookups; the lifecycle lens prices every anchor and replacement generation; the skeptical lens gives competitors exactly the same buckets.

1. **Compact envelope:** smaller retained state, but no guarantee that its certificate admits a query. Keep it as a different storage/query tradeoff, not a weaker implementation of this index.
2. **Complete overlap buckets, selected:** frozen per-bucket order, exact exception corrections, disk membership, bounded head merge. Work need not include all unchanged targets, but storage includes them all.
3. **Same buckets with corrected top-k materialization:** compute changed top-k and unchanged top-k independently, then merge. Correct and bounded, but it can read unnecessary unchanged rows when changed winners dominate. An ordinary fused executor removes that difference; this is not a new heap-merge invention.

## Contract And Representation

Freeze binary target sets T_j, their cardinalities b_j, a total original-ID order, target eligibility, feature projection, immutable anchor P, and all posting/index generations. Let n be the target count and p=|P|. Duplicate memberships are deduplicated during preparation; distinct target IDs remain distinct even when their feature sets coincide.

For a query set S, write a=|S| and

```text
c_j     = |P intersect T_j|
Delta_j = |S intersect T_j| - c_j
C       = {j : Delta_j != 0}, q=|C|
score_j = (c_j+Delta_j)/(a+b_j-c_j-Delta_j).
```

Empty-empty and every zero intersection score zero. Return exactly `k'=min(k, n-1[self_id in targets])` distinct IDs, score descending then original ID ascending, with zero tails and no boundary-tie expansion. Self is excluded before any top-k truncation. Empty sources directly stream the first k' eligible IDs from the ID index, with `(a,b,c_actual)=(0,b,0)`; k'=0 publishes an empty result. These are semantic special cases, not fallback gates.

The initial scope is **binary-set Jaccard**. A positive-c bucket's `(b,ID)` order also works for binary cosine, but this file does not claim tested cosine support. Intersection-only ranking needs ID order within every fixed-c bucket, so the Jaccard bucket layout cannot silently serve that metric. Weighted variants require their own range/order proof.

For the packed resource example, n, p, a and b fit unsigned 32-bit values, and stable dense target ranks preserve the original-ID order. A query score is compared with exact cross-products, never rounded floats. Denominators fit 33 bits and products fit 65 bits, so checked 128-bit comparisons suffice. Signed delta sums are checked before narrowing. The miniature uses wider fixed records and Python integers, not an audited packed comparator.

### Paid Index

For each anchor retain:

- One record per target in exactly one bucket `B_c`, where c is its anchor overlap. Positive-c buckets are sorted by `(b,ID)`; bucket zero is sorted by **ID alone**, regardless of b.
- A directory of nonempty buckets `(c, offset, count)`, with `g<=min(n,p+1)`.
- An ID-addressable baseline index giving `(c,b)` for every target. This is a second ordering/access path, not free data obtained from bucket offsets. A dense-rank array is possible only with a separately paid stable ID map; otherwise use an external ordered ID index.
- Anchor content, snapshot/projection/metric/ID-order metadata and index publication state.

There is one occurrence per target in the **bucket partition**, not one physical access per target across the entire job. Changed targets also require baseline-index reads; postings can mention them many times; membership searches can revisit the same C pages.

## Algorithm

### Frozen Order And Exact Exceptions

For fixed positive c and a>0, `c/(a+b-c)` is strictly decreasing in b. Equal b values tie and are ordered by ID. For c=0 every score is zero, so only ID matters. Deleting arbitrary C/self records from a bucket preserves this total order. Thus the same bucket order works for **every positive query cardinality**, after exceptions are removed. No cardinality domain has to be certified.

Obtain exact exceptions through signed postings:

```text
D+ = S minus P; D- = P minus S
Delta_j = sum_(f in D+) 1[f in T_j] - sum_(f in D-) 1[f in T_j].
```

Externally sort/reduce all signed contributions by target ID. Keep only nonzero net deltas in the final sorted C file. A target touched by both signs can be unchanged. The procedure pays for all posting records even if q=0. The executable listing actually generates these records from prepared postings; no oracle supplies C.

Scan C, recover `(c,b)` from the paid ID index, compute the exact corrected score, exclude self, and retain only its first k' in a bounded selector H_C. Sorting H_C in place produces the changed stream. A changed candidate outside that top-k' can never enter the global top-k': k' other eligible changed candidates precede it.

### Cursor Merge

```text
query_paid_exception_merge(S, self_id, k, index):
    pin and validate source/target, postings, anchor, metric and index versions
    compute a, k'; handle empty source and k'=0
    admit directory/head/selector, reducer scratch and complete output
    C = bounded signed-posting reduction, sorted by ID, with zero sums removed
    H_C = exact top-k' of changed nonself targets using indexed (c,b)+delta

    H = empty head heap
    for each nonempty c bucket:
        advance its cursor past self and IDs found in on-disk C
        if a record survives, insert it with exact key (-c/(a+b-c), ID)

    repeat until k' rows have been emitted:
        choose the smaller total-order key of H.minimum and H_C.next
        if changed wins: emit it and advance only the changed stream
        otherwise:
            pop and emit the winning bucket head
            unless that was the final output:
                advance only that bucket, skipping C/self, and reinsert its head
    publish only the complete staged output
```

The C membership test is a fresh binary search or a bounded-cache indexed lookup. A single forward C cursor is **invalid**: bucket traversal is not globally ordered by ID. The reference schedule uses one shared bucket file and explicit offsets, not one open file or one page-sized buffer for every bucket. The heap head stores the next offset and end for its bucket. Directory metadata and bounded caches are separately charged.

## Correctness And Head Bound

**Theorem 1, full ordered answer.** The procedure returns exactly the declared top-k', including zeros and ties.

**Proof.** Signed reduction computes every actual intersection change, so the eligible universe partitions into exactly scored changed targets and unchanged targets. The latter partition into disjoint filtered bucket streams, each sorted by the actual score/ID key by the frozen-order lemma. At every step each nonempty filtered bucket exposes its best remaining record; a heap merge therefore exposes the best unchanged record. Truncating the changed stream at k' cannot remove a global winner. Merging that stream with the unchanged stream consequently emits the global first k' in total order, with no duplicates because their ID domains are disjoint. Self was removed before truncation and cursor insertion. Special empty-source and zero-k cases follow directly from their declared scores. QED.

**Theorem 2, bucket-record bound.** For a positive-source query, let u be the number of unchanged results emitted and r the number of skipped records examined. Let v count bucket records decoded, including prefetched heads that are not returned. Then

```text
r <= |C union {self_id if eligible}| <= q+1
v <= g + u + r <= g+q+k'+1
v <= n, hence v <= min(n, g+q+k'+1).
```

**Proof.** Every bucket cursor moves strictly forward and buckets are disjoint, so every skipped target is charged at most once. Initialization creates at most g eligible heads. Each emitted unchanged head causes at most one successor head to be created; changed outputs cause no bucket advancement. All decoded records are either skipped records or created eligible heads. Summing these charges gives the bound. QED.

The listing stops before refilling after the final result. For k'>0 this yields the slightly stronger `v<=min(n,g+q+k')`: if the final result is unchanged it causes no refill, while if it is changed then u<=k'-1. The `+1` in the conservative bound accounts for self, not an arbitrary number of filtered targets. Additional query-time eligibility filters would add their skipped population and are outside this contract.

This is **not** an I/O bound of g+q+k' disk operations. With an uncached binary C search, up to `v*ceil(log2(q+1))` C-record probes occur, plus up to q baseline lookups and posting-directory lookups. The same ID can be read from several different indexes. Nor is this an instance-optimality theorem.

## Revised After Counterexample

| Tempting simplification | Counterexample | Required correction |
| --- | --- | --- |
| Sort zero bucket by b | Zero-overlap ID 1 has b=5; ID 9 has b=0. Both scores are zero, but the shorter-first order hides ID 1. | Bucket zero is ID-sorted regardless of b. |
| Sort every bucket by ID | P=S={0}; target ID 1 is {0,1,2}, ID 9 is {0}. The head must be ID 9 with score 1, not ID 1 with 1/3. | Positive buckets use b before ID. |
| Use one forward pointer into sorted C | P={0}, S={7}; targets 0:empty, 9:{0}, 1:{0,2,3}. C has IDs 1,9; the c=1 bucket visits 9 before 1. After matching 9, a forward-only C cursor misses changed ID 1 and emits its false positive counterfactual score. | Independent C membership search; no monotone-ID assumption across buckets. |
| Trim changed top-k then remove self | P=empty; S={1,2} with self ID 0; targets 0:{1,2}, 1:{1}, 2:{9}; k=1. Keeping only changed self first loses true winner 1. | Exclude self before changed selection. |
| Leave corrected targets in their old bucket | Actual scores can move either up or down; a hidden deep changed row can become the winner or be emitted twice. | Remove every nonzero net-delta ID from unchanged streams, including those not retained in H_C. |
| An empty delta stream validates an old target index | P=S={0}; replace target 20:{0,1,2} by {0}, while target 10:{0,1} stays. Source deltas remain empty and the old size/order is wrong. | Reject mixed/stale generations; rebuild the paid index. No quality fallback is hidden behind version mismatch. |

All four ordering/skip/selection mistakes in the first four rows are deliberately injected into the file-backed miniature and must produce an incorrect answer. The main procedure is then checked separately. A wrong candidate is not legitimized merely because a runtime validity assertion could catch some instances.

## RAM, Work And I/O

Let `P_D=sum_(edited f) posting_length(f)`, w_D be a signed contribution width, w_B a bucket width, w_I a baseline-index width, M a run buffer in records, f a merge fan-in, and B a physical I/O block size. Keep g and k' explicit, not fixed constants hidden in O(1).

```text
Resident query state <= fixed sort/merge/cursor/cache/output/runtime buffers
                        + w_directory*g + w_head*g + w_selector*k'.
Scratch reducer peak <= 2*w_D*P_D + file/header/allocation allowances.
Query live disk     <= reducer reservation + output_width*k'
                       + directory/file/allocation/recovery allowances.
Retained per anchor  = n*(w_B+w_I) + w_directory*g + anchor/manifest bytes.
```

The simple sorter writes initial runs, performs at most `ceil(log_f(max(1,ceil(P_D/M))))` merge levels, then reduces the final stream. The final raw run and reduced C coexist briefly and fit the same two-payload reservation. The listing checks live scratch payload on every write and retains only bounded run buffers, fan-in cursors and two integer generation/run counters. There is no resident C set, source-by-target overlap plane, per-bucket page cache, or candidate array of size q/n. Sorting the k' selector requires a charged O(k') reference/compare workspace.

Define `Sort_f(X)` as the actual traffic of the admitted run plan, conservatively `2X*(1+ceil(log_f(max(1,ceil(X/M_bytes)))))`. Query traffic includes

```text
source/anchor merge scans and posting-directory probes
+ exact posting payload for P_D
+ Sort_f(w_D*P_D) + final reduction/C scans
+ q*Lookup_ID(n) + v*Lookup_C(q)
+ bucket pages for v records + g directory records
+ complete output, publication and any recovery rereads.
```

`Lookup_ID` is a charged dense-rank slot/page access or ordered-index search; the miniature uses the latter. `Lookup_C` is not free just because C is sorted. Worst-case fixed-record binary search costs O(log q) probes; a bounded page-aware index/cache can reduce physical traffic but adds its measured state. The bucket's sequential order does not imply globally sequential disk service when a heap interleaves many buckets.

For a positive-source, positive-k query the listing scans S once to count a and twice to generate source edits, and P twice. Its source/anchor payload is therefore `8*(3a+2p)` bytes, plus two posting-directory searches per edited feature and `8*P_D` bytes of posting IDs. The two edit passes preflight the exact scratch reservation before writing any signed records. File-open, seek, directory service and sort traffic are additional, not included in those payload counts.

CPU after reduction is `O(q*(lookup_ID+log(k'+1)) + v*lookup_C + (g+k')*log(g+1) + k'*log(k'+1))` with a straightforward heap builder. Score arithmetic and page service are additional measured costs. There is no scan fallback term, but a frequent or common-feature edit can itself make P_D or q linear in the population. First output waits for complete exception discovery and changed top-k selection.

### Preparation And Refresh

Compute each target's c exactly, write its ID-ordered baseline record, externally sort another record stream into buckets, and scan that stream to build the directory. A bounded merge intersection can compute c without an n-array. A small admitted anchor array can avoid repeatedly reading P; otherwise charge `n*bytes(P)` replay. The miniature intentionally uses bounded file cursors. A different posting-based overlap builder must charge its aggregation runs instead of treating all c values as already present.

For A anchors, the straightforward preparation performs A overlap discoveries and A full bucket builds. Even with a resident p-entry anchor, all memberships/target metadata and `Theta(A*n)` index output are paid. The bucket construction needs `Sort(n)` traffic per anchor, and the baseline lookup order is a distinct retained representation. Construction is not O(g).

The initial refresh protocol rebuilds every selected per-anchor index against a new coherent target snapshot, then publishes their manifests. A target edit can alter b and c under many anchors, move its bucket/position, and change shared postings. Old readers retain old blocks. A single source-as-target change is not a source-only edit. Small overlays could reduce rewrites but would change cursor uniqueness, order and skip accounting; no unproved overlay scheme is assumed here.

On source-only changes, reuse index and postings after pinning the same target snapshot. On crash, discard or explicitly resume versioned runs/staged output. Full transactional recovery, slow-sink handling and packed index maintenance are implementation obligations not supplied by the miniature. No partial result or resource rejection is called a successful top-k.

### Practical Decimal-Byte Scenario

An illustrative projection has n=100M, 1B memberships, 10M features, eight anchors with p=200, g<=201, and k'=50. These are model inputs, not a measured dataset. Use frozen dense target ranks preserving ID order:

| Retained component | Bytes |
| --- | --- |
| Two u32 membership orientations | 8,000,000,000 |
| u64 node/feature offsets, rounded | 880,000,016 |
| Shared target cardinalities | 400,000,000 |
| Original u64 ID map | 800,000,000 |
| Separately retained raw membership/provenance copy | 8,000,000,000 |
| Explicit metadata/recovery/format reservation | 1,000,000,000 |
| Per anchor: `(rank,b)` bucket records and `(c,b)` dense lookup slots | 1,600,000,000 |
| Per anchor: 24-byte bucket directory, maximum g=201 | 4,824 |
| Two outputs for 100k sources, k'=50, 24 bytes/result | 240,000,000 |

The selected eight-anchor portfolio totals about **32.12004 GB**, before any other algorithm family's selected blocks and separately stated anchor/manifest overhead. Twenty anchors already exceed 50 GB (about 51.32 GB). No compression or sharing between distinct anchor tables is assumed. The 24-byte export assumption can encode a u32 source rank, u64 original target ID and three u32 values `(a,b,actual_intersection)`; the source rank map is also required and must fit the metadata reservation. In the actual 64-bit-ID miniature, bucket and lookup records are 24 bytes each and output rows 32 bytes, so its index format costs **48n per anchor**, not the packed example's 16n.

A complete new eight-anchor generation alongside the old one requires about **64.12 GB** including one new output, so full-replacement refresh is inadmissible under 50 GB retention. This explicitly assumes a replacement of the shared projection/raw/metadata blocks as well as the anchor indexes, without cross-generation block sharing. Three-anchor old/new generations plus three outputs are about **48.12 GB**, with only about 1.88 GB left for omitted selected blocks/overheads. Inputs and scratch are additional: 50 GB retained is not permission to assume a 50 GB physical SSD suffices. External-sort generations for a 100M-record, 24-byte temporary build alone can need 4.8 GB of payload scratch, plus headers and other admitted builder phases.

One provisional worker allocation is 256 MB sort, 32 MB merge, 64 MB shared ID/C page cache, 32 MB cursor/decode/I/O, 16 MB output, and 200 MB runtime/allocator/residency allowance: 600 MB total plus `88*g+64*k'` bytes for directory/heads/selector, and explicitly admitted anchor/sort workspace. At g=201,k'=50 the latter payload is 20,888 bytes. No page buffer is multiplied by g. These are reservations, not Python RSS or a measured fit within the provisional 3 GB worker / 1 GB OS-other split of the **4 GB physical machine**.

For q=20,000, k'=50, g=201, the conservative bound is v<=20,252. An uncached 15-probe C binary search permits 303,780 C-record probes, roughly 1.24 GB of 4 KiB page service if every probe misses, although this C file itself is small enough to cache in this example. In addition, up to 20,000 random baseline accesses, posting reduction and output remain. This is why record-count bounds cannot be sold as disk latency.

Complete all-source output at n=100M,k'=50 and 24 bytes/row is **120 GB**. The index cannot make that output retainable under 50 GB. The smaller 100k-source export above is a different, explicitly selected job.

## Primary Comparison And Novelty Boundary

Sources were inspected through their primary PDFs in this pass; inspection locations and limitations are explicit:

| Primary work | Inspected mechanism and consequence |
| --- | --- |
| [Fagin, Lotem, Naor, Optimal Aggregation Algorithms for Middleware](https://www.wisdom.weizmann.ac.il/~naor/PAPERS/middle_agg.pdf), sections 1, 4, 6.1 and 8 | Sorted versus random access, bounded top-k retention, threshold stopping, and cost-sensitive alternatives are established. Their model combines multiple attribute lists of the same objects; ours merges disjoint target partitions with externally corrected exceptions. Its instance-optimality results do not transfer to g growing with p, paid index construction, arbitrary disk probes or our fixed ID ties. |
| [Amagata, Hara, Xiao, Dynamic Set kNN Self-Join](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf), sections IV-A through IV-G | Exact difference indexes, postings, reverse-neighbor updates and scan/indexed reverification are existing mechanisms. Their local-index space is data-dependent and may be quadratic; our all-target tables are deliberately Theta(A*n), and target refresh is initially a full rebuild. This is a different paid-state choice, not invention of signed set updates. |

**Competitor given the same buckets.** Provide an ordinary database executor with `(1)` these ordered partitions, `(2)` exact C and corrected changed scores, and `(3)` the same random membership primitive. A generic anti-join/filter plus top-k union/merge reproduces this algorithm and the same head bound. No graph-specific advantage over that competitor is established. The miniature also executes a less fused, but still exact, same-bucket baseline: select up to k' unchanged rows before unioning them with changed top-k. Any advantage against that baseline is fusion/early consumption, and vanishes when it adopts the same merge.

The domain-specific observation is the **combination** of arbitrary source edits factoring into a complete signed-posting exception set and an overlap-class order that remains valid for every positive source size. This applies to arbitrary binary sets, not uniquely to graph neighborhoods. It differentiates the repository's compact-certificate and identical-set plans, but each ingredient and the generic merge are established. A targeted search has not established whether this complete index/query combination is unpublished. Treat it as a useful exact paid-index architecture, with research novelty **unresolved and currently weak without a stronger theorem or lifecycle result**.

The decisive comparison gives every method the same operator, zero/tie/self rules, source/target churn, output requirements, random-I/O prices, index build cost and 50 GB retained union. Prefer this plan only when saved unchanged-target work amortizes its additional build/refresh/storage cost before the snapshot expires. No automatic superiority over the compact envelope, LI-DSN-Join or a generic executor with the same indexes is claimed.

## Executable File-Backed Probe

The following standalone Python 3.11 fence is research evidence, not production scaffolding. It creates temporary files and deletes them on exit. The shared snapshot fixture builder and independent expected answers use small dictionaries. The paid-index builder and query receive only immutable file metadata/cursors; they do not read the oracle dictionaries. Source edits, signed posting reduction, baseline lookups, sorted-C binary searches, bucket advancement and output staging are actually executed. Four-record sort buffers and two-way merge force external run boundaries. Record reads/writes and logical occupancy are counted, not physical RSS or OS cache residency. Reducer scratch and staged output have separate checked byte reservations; their sum must fit the admitted query-disk allowance.

The version tokens model trusted immutable manifests, not content verification or concurrency control. An external source can request arbitrary self-ID exclusion. A source taken from the shared target graph must instead supply its source epoch; the miniature tests rejection when that epoch differs. Production code must obtain that epoch from the pinned source reader, not trust a caller's unsupported claim.

```python
from collections import Counter
from fractions import Fraction as Q
from heapq import heappush, heappop, heapreplace, heapify, merge
from itertools import groupby, product
from pathlib import Path
from random import Random
from struct import Struct
from tempfile import TemporaryDirectory
from time import perf_counter

FEATURE, IDENT = Struct('<Q'), Struct('<q')
ROW, BASE, DIRECTORY = Struct('<qQQ'), Struct('<qQQ'), Struct('<QQQ')
DELTA, OUTPUT = Struct('<qq'), Struct('<qQQQ')
stats = Counter()

def iterate_fixed_file_records(path, layout, tag, offset=0, count=None):
    with open(path, 'rb', buffering=0) as stream:
        stream.seek(offset)
        while count is None or count > 0:
            value = stream.read(layout.size)
            stats[tag+'_read_bytes'] += len(value)
            if not value:
                assert count is None
                return
            assert len(value) == layout.size
            if count is not None:
                count -= 1
            yield layout.unpack(value)

def write_fixed_stream_record(stream, layout, record, tag):
    value = layout.pack(*record)
    assert stream.write(value) == len(value)
    stats[tag+'_write_bytes'] += len(value)

def lookup_sorted_file_record(path, layout, count, key, tag):
    lo, hi = 0, count
    while lo < hi:
        mid = (lo+hi)//2
        record = next(iterate_fixed_file_records(path, layout, tag, mid*layout.size, 1))
        stats[tag+'_probes'] += 1
        if record[0] < key:
            lo = mid+1
        elif record[0] > key:
            hi = mid
        else:
            return record
    return None

def write_sorted_source_features(path, values):
    with open(path, 'wb', buffering=0) as stream:
        for value in sorted(set(values)):
            write_fixed_stream_record(stream, FEATURE, (value,), 'source')

def merge_signed_source_edits(source, anchor):
    additions = ((f, 1) for (f,) in iterate_fixed_file_records(source, FEATURE, 'source'))
    removals = ((f, -1) for (f,) in iterate_fixed_file_records(anchor, FEATURE, 'anchor'))
    for feature, records in groupby(merge(additions, removals), key=lambda x: x[0]):
        change = sum(sign for _, sign in records)
        if change:
            yield feature, change

def intersect_streamed_feature_rows(left, right):
    left, right = iter(left), iter(right)
    x, y = next(left, None), next(right, None)
    result = 0
    while x is not None and y is not None:
        if x == y:
            result += 1; x, y = next(left, None), next(right, None)
        elif x < y:
            x = next(left, None)
        else:
            y = next(right, None)
    return result

class Snapshot:
    def __init__(self, root, targets, epoch=0):
        root.mkdir(); self.root, self.n = root, len(targets)
        self.token = (epoch, 'binary', 'ascending-original-ID')
        self.post_token = self.token
        self.rows, self.rowdir = root/'rows', root/'rowdir'
        self.posts, self.postdir = root/'posts', root/'postdir'
        with open(self.rows, 'wb', buffering=0) as rows, open(self.rowdir, 'wb', buffering=0) as directory:
            offset = 0
            for node, values in sorted(targets.items()):
                values = sorted(set(values))              # fixture builder only
                write_fixed_stream_record(directory, ROW, (node, offset, len(values)), 'prepare')
                for value in values:
                    write_fixed_stream_record(rows, FEATURE, (value,), 'prepare')
                offset += FEATURE.size*len(values)
        features = sorted({f for values in targets.values() for f in values})
        self.F = len(features)
        with open(self.posts, 'wb', buffering=0) as posts, open(self.postdir, 'wb', buffering=0) as directory:
            offset = 0
            for feature in features:
                count = sum(feature in values for values in targets.values())
                write_fixed_stream_record(directory, DIRECTORY, (feature, offset, count), 'prepare')
                for node, values in sorted(targets.items()):
                    if feature in values:
                        write_fixed_stream_record(posts, IDENT, (node,), 'prepare')
                offset += IDENT.size*count

    def posting(self, feature):
        return lookup_sorted_file_record(self.postdir, DIRECTORY, self.F, feature, 'postdir')

class Scratch:
    def __init__(self, root, cap):
        self.root, self.cap, self.live = root, cap, 0

    def write(self, path, layout, records):
        count = 0
        with open(path, 'wb', buffering=0) as stream:
            for record in records:
                assert self.live+layout.size <= self.cap, 'scratch admission violated'
                write_fixed_stream_record(stream, layout, record, 'scratch')
                self.live += layout.size; count += 1
                stats['scratch_peak'] = max(stats['scratch_peak'], self.live)
        return count

    def remove(self, path):
        self.live -= path.stat().st_size; path.unlink()
        assert self.live >= 0

def sort_bounded_record_stream(records, layout, key, arena, count, capacity=4, fanin=2):
    assert capacity > 0 and fanin >= 2 and 2*layout.size*count <= arena.cap
    buffer, runs, seen = [], 0, 0
    def path(g, r):
        return arena.root/('run-%d-%d' % (g, r))
    for record in records:
        buffer.append(record); seen += 1
        stats['sort_peak'] = max(stats['sort_peak'], len(buffer))
        assert len(buffer) <= capacity
        if len(buffer) == capacity:
            buffer.sort(key=key); arena.write(path(0, runs), layout, buffer)
            runs += 1; buffer.clear()
    assert seen == count
    if buffer or not runs:
        buffer.sort(key=key); arena.write(path(0, runs), layout, buffer)
        runs += 1; buffer.clear()
    generation = 0
    while runs > 1:
        outputs = 0
        for start in range(0, runs, fanin):
            end = min(runs, start+fanin)
            cursors = [iterate_fixed_file_records(path(generation, r), layout, 'scratch')
                       for r in range(start, end)]
            stats['fanin_peak'] = max(stats['fanin_peak'], len(cursors))
            arena.write(path(generation+1, outputs), layout, merge(*cursors, key=key))
            for r in range(start, end):
                arena.remove(path(generation, r))
            outputs += 1
        generation += 1; runs = outputs
    stats['merge_levels'] += generation
    return path(generation, 0)

class Index:
    def __init__(self, snapshot, anchor, name='index', fault=None):
        self.snap, self.token = snapshot, snapshot.token
        self.root = snapshot.root/name; self.root.mkdir()
        self.anchor, self.ids = self.root/'anchor', self.root/'by-id'
        self.buckets, self.directory = self.root/'buckets', self.root/'directory'
        write_sorted_source_features(self.anchor, anchor)
        self.p = sum(1 for _ in iterate_fixed_file_records(self.anchor, FEATURE, 'anchor'))
        def key(record):
            node, b, c = record
            if fault == 'positive-id':
                return c, 0, node
            return c, (b if c or fault == 'zero-size' else 0), node
        with open(self.ids, 'wb', buffering=0) as ids:
            def baseline():
                for node, offset, b in iterate_fixed_file_records(snapshot.rowdir, ROW, 'prepare'):
                    cp = intersect_streamed_feature_rows(
                        iterate_fixed_file_records(self.anchor, FEATURE, 'anchor'),
                        iterate_fixed_file_records(snapshot.rows, FEATURE, 'prepare', offset, b))
                    record = (node, b, cp)
                    write_fixed_stream_record(ids, BASE, record, 'index')
                    yield record
            with TemporaryDirectory(dir=self.root) as temporary:
                arena = Scratch(Path(temporary), 2*BASE.size*snapshot.n)
                result = sort_bounded_record_stream(baseline(), BASE, key, arena, snapshot.n)
                arena.live -= result.stat().st_size
                result.replace(self.buckets)
                assert arena.live == 0
        self.g = 0
        with open(self.directory, 'wb', buffering=0) as directory:
            offset = 0
            for c, records in groupby(iterate_fixed_file_records(self.buckets, BASE, 'index'), key=lambda x: x[2]):
                count = sum(1 for _ in records)
                write_fixed_stream_record(directory, DIRECTORY, (c, offset, count), 'index')
                offset += BASE.size*count; self.g += 1
        assert self.g <= min(snapshot.n, self.p+1)

    def baseline(self, node):
        return lookup_sorted_file_record(self.ids, BASE, self.snap.n, node, 'baseline')

def reduce_exact_changed_postings(index, source, arena, local):
    snap, volume = index.snap, 0
    for feature, _ in merge_signed_source_edits(source, index.anchor):
        entry = snap.posting(feature)
        volume += entry[2] if entry else 0
    assert 2*DELTA.size*volume <= arena.cap, 'scratch admission'
    def signed():
        for feature, sign in merge_signed_source_edits(source, index.anchor):
            entry = snap.posting(feature)
            if entry:
                for (node,) in iterate_fixed_file_records(snap.posts, IDENT, 'posting', entry[1], entry[2]):
                    yield node, sign
    raw = sort_bounded_record_stream(signed(), DELTA, lambda x: x[0], arena, volume)
    def reduced():
        for node, records in groupby(iterate_fixed_file_records(raw, DELTA, 'scratch'), key=lambda x: x[0]):
            value = sum(value for _, value in records)
            if value:
                yield node, value
    changed = arena.root/'changed'
    q = arena.write(changed, DELTA, reduced())
    arena.remove(raw)
    local['postings'], local['q'] = volume, q
    return changed, q

def calculate_exact_score_key(a, b, c):
    return Q(c, a+b-c) if c else Q(0)

def offer_bounded_changed_record(heap, k, record):
    node, a, b, c = record
    entry = (calculate_exact_score_key(a, b, c), -node, node, a, b, c)
    if len(heap) < k:
        heappush(heap, entry)
    elif entry > heap[0]:
        heapreplace(heap, entry)
    assert len(heap) <= k

def iterate_unchanged_bucket_rows(index, changed, q, a, sid, local, fault=None):
    heap = []
    forward = iter(iterate_fixed_file_records(changed, DELTA, 'C-forward'))
    current = next(forward, None) if fault == 'forward-C' else None
    def contains(node):
        nonlocal current
        local['membership_calls'] += 1
        if fault == 'forward-C':
            while current is not None and current[0] < node:
                current = next(forward, None)
            return current is not None and current[0] == node
        return lookup_sorted_file_record(changed, DELTA, q, node, 'C-random') is not None
    def advance(c, offset, end):
        while offset < end:
            node, b, stored_c = next(iterate_fixed_file_records(index.buckets, BASE, 'bucket', offset, 1))
            offset += BASE.size; local['examined'] += 1
            assert stored_c == c
            if node == sid or contains(node):
                local['skipped'] += 1
                continue
            assert 0 <= c <= min(a, b)
            local['created_heads'] += 1
            heappush(heap, (-calculate_exact_score_key(a, b, c), node, b, c, offset, end))
            local['heads_peak'] = max(local['heads_peak'], len(heap))
            assert len(heap) <= index.g
            return
    for c, offset, count in iterate_fixed_file_records(index.directory, DIRECTORY, 'directory'):
        advance(c, offset, offset+count*BASE.size)
    while heap:
        _, node, b, c, offset, end = heappop(heap)
        yield node, a, b, c
        advance(c, offset, end)

def execute_paid_exception_query(index, source, sid, k, strategy='fused', fault=None,
                                 scratch_cap=10**8, head_cap=1000, selector_cap=1000,
                                 output_cap=10**6, query_disk_cap=101*10**6,
                                 snapshot=None, source_epoch=None):
    snap = index.snap if snapshot is None else snapshot
    assert index.token == index.snap.token == snap.token == snap.post_token == index.snap.post_token, 'snapshot mismatch'
    assert source_epoch is None or source_epoch == snap.token[0], 'shared source mismatch'
    assert k >= 0
    a = sum(1 for _ in iterate_fixed_file_records(source, FEATURE, 'source'))
    kp = min(k, snap.n-int(sid is not None and index.baseline(sid) is not None))
    assert index.g <= head_cap and kp <= selector_cap, 'selector admission'
    assert OUTPUT.size*kp <= output_cap, 'output admission'
    assert scratch_cap+output_cap <= query_disk_cap, 'query disk admission'
    local = Counter(g=index.g, k=kp)
    initial_membership_probes = stats['C-random_probes']
    with TemporaryDirectory(dir=index.root, prefix='query-') as temporary:
        arena = Scratch(Path(temporary), scratch_cap)
        selected = []
        if kp and a:
            C, q = reduce_exact_changed_postings(index, source, arena, local)
            for node, delta in iterate_fixed_file_records(C, DELTA, 'C-sequential'):
                if node == sid and fault != 'late-self':
                    continue
                _, b, cp = index.baseline(node)
                actual = cp+delta
                assert 0 <= actual <= min(a, b)
                offer_bounded_changed_record(selected, kp, (node, a, b, actual))
            if fault == 'late-self':
                selected[:] = [entry for entry in selected if entry[2] != sid]
            local['changed_peak'] = len(selected)
            unchanged = iterate_unchanged_bucket_rows(index, C, q, a, sid, local, fault)
        def answers():
            if not kp:
                return
            if not a:
                emitted = 0
                for node, b, _ in iterate_fixed_file_records(index.ids, BASE, 'baseline'):
                    if node != sid:
                        yield node, 0, b, 0
                        emitted += 1
                        if emitted == kp:
                            return
                return
            if strategy == 'materialize':
                heapify(selected)
                for _ in range(kp):
                    record = next(unchanged, None)
                    if record is None:
                        break
                    offer_bounded_changed_record(selected, kp, record)
                selected.sort(reverse=True)
                for entry in selected:
                    yield entry[2:]
                return
            selected.sort(reverse=True)
            position = 0
            head = next(unchanged, None)
            for rank in range(kp):
                entry = selected[position] if position < len(selected) else None
                changed_key = (-entry[0], entry[2]) if entry is not None else None
                head_key = (-calculate_exact_score_key(*head[1:]), head[0]) if head is not None else None
                if entry is not None and (head is None or changed_key < head_key):
                    yield entry[2:]; position += 1
                else:
                    assert head is not None, 'underfilled output'
                    yield head; local['unchanged_output'] += 1
                    if rank+1 < kp:
                        head = next(unchanged, None)
        stage, final = arena.root/'stage', arena.root/'final'
        count = 0
        with open(stage, 'wb', buffering=0) as stream:
            for record in answers():
                assert OUTPUT.size*(count+1) <= output_cap
                write_fixed_stream_record(stream, OUTPUT, record, 'output')
                count += 1
        assert count == kp
        stage.replace(final)
        local['published'] = 1
        local['membership_probes'] = stats['C-random_probes']-initial_membership_probes
        assert local['membership_probes'] <= local['examined']*local['q'].bit_length()
        assert local['skipped'] <= local['q']+1
        assert local['examined'] <= min(snap.n, index.g+local['q']+kp+1)
        if kp and strategy == 'fused':
            assert local['examined'] <= min(snap.n, index.g+local['q']+kp)
        # Only the independent tiny test sink materializes already-published output.
        result = list(iterate_fixed_file_records(final, OUTPUT, 'output-check'))
        if kp and a:
            unchanged.close()
            arena.remove(C)
        assert arena.live == 0
        return result, local

def compute_independent_oracle_answer(targets, values, sid, k):
    source = set(values); records = []
    for node, raw in targets.items():
        if node == sid:
            continue
        target = set(raw)
        records.append((node, len(source), len(target), len(source & target)))
    return sorted(records, key=lambda r: (-calculate_exact_score_key(*r[1:]), r[0]))[:k]

rng, checks = Random(20260922), Counter()
started = perf_counter()
with TemporaryDirectory(prefix='exception-merge-') as temporary:
    root = Path(temporary)
    for case in range(80):
        targets = {j*5-40: [f for f in range(8) if rng.randrange(3) == 0]
                   for j in range(case%21)}
        if targets and case%3 == 0:
            first = min(targets)
            targets[first] += targets[first]          # raw duplicates, prepared as sets
        anchor = {f for f in range(8) if rng.randrange(2)}
        prepare_start = perf_counter()
        snap = Snapshot(root/str(case), targets)
        index = Index(snap, anchor)
        stats['prepare_seconds'] += perf_counter()-prepare_start
        source = snap.root/'source'
        for step in range(10):
            values = (anchor if step == 0 else set() if step == 1 else
                      {f for f in range(12) if rng.randrange(3) == 0})
            write_sorted_source_features(source, values)
            sid = step*5-40 if step%2 else 999
            k = (0, 1, 3, 30, 2)[step%5]
            query_start = perf_counter()
            answer, info = execute_paid_exception_query(index, source, sid, k)
            stats['query_output_seconds'] += perf_counter()-query_start
            assert answer == compute_independent_oracle_answer(targets, values, sid, k)
            checks['answers'] += 1
            checks['positive_queries'] += bool(values and info['k'])
            checks['zero_sources'] += not bool(values)
            checks['examined'] += info['examined']; checks['q_total'] += info['q']
            checks['max_g'] = max(checks['max_g'], index.g)
            if case%4 == 0:
                other, baseline_info = execute_paid_exception_query(index, source, sid, k, 'materialize')
                assert other == answer
                assert info['examined'] <= baseline_info['examined']
                checks['matched_baseline'] += 1
                checks['strict_fusion_savings'] += info['examined'] < baseline_info['examined']
        checks['fixtures'] += 1
    # Exhaust all three-target assignments, anchors, sources, self modes and k values
    # over a two-feature universe. Oracle sets never enter the file-backed kernel.
    exhaustive_start = perf_counter()
    subsets = [set(), {0}, {1}, {0, 1}]
    for case, assignment in enumerate(product(subsets, repeat=3)):
        targets = dict(zip((-3, 9, 2), assignment))
        snap = Snapshot(root/('exhaust%d' % case), targets)
        source = snap.root/'source'
        for anchor_number, anchor in enumerate(subsets):
            index = Index(snap, anchor, name='anchor%d' % anchor_number)
            for values in subsets:
                write_sorted_source_features(source, values)
                for sid, k in product((None, 2), (0, 1, 2, 5)):
                    answer, info = execute_paid_exception_query(index, source, sid, k)
                    assert answer == compute_independent_oracle_answer(targets, values, sid, k)
                    checks['exhaustive_answers'] += 1
    stats['exhaustive_lifecycle_seconds'] = perf_counter()-exhaustive_start
    # Same-bucket separating example: changed winners are deep in the old zero bucket.
    targets = {j: {3} for j in range(1000)} | {j: {1} for j in range(1000, 1040)}
    snap = Snapshot(root/'fusion', targets); index = Index(snap, {0})
    source = snap.root/'source'; write_sorted_source_features(source, {1})
    answer, info = execute_paid_exception_query(index, source, None, 10)
    other, baseline_info = execute_paid_exception_query(index, source, None, 10, 'materialize')
    assert answer == other == compute_independent_oracle_answer(targets, {1}, None, 10)
    assert info['examined'] == 1 and baseline_info['examined'] == 10 and info['q'] == 40
    print('same_bucket_fusion', dict(info), 'materialized_examined', baseline_info['examined'])
    # Popular cancellation: the two signed posting lists cannot be replaced by a touched set.
    targets = {j: {0, 1, j+2} for j in range(100)}
    snap = Snapshot(root/'cancel', targets); index = Index(snap, {0})
    source = snap.root/'source'; write_sorted_source_features(source, {1})
    answer, info = execute_paid_exception_query(index, source, None, 5)
    assert answer == compute_independent_oracle_answer(targets, {1}, None, 5)
    assert info['postings'] == 200 and info['q'] == 0 and info['examined'] == 5
    print('cancellation', dict(info))
    # Deliberately falsify four tempting variants using the same physical kernel.
    cases = [
        ('zero-size', {1: set(range(2, 7)), 9: set()}, {0}, {7}, None, 1),
        ('positive-id', {1: {0, 1, 2}, 9: {0}}, {0}, {0}, None, 1),
        ('forward-C', {0: set(), 9: {0}, 1: {0, 2, 3}}, {0}, {7}, None, 1),
        ('late-self', {0: {1, 2}, 1: {1}, 2: {9}}, set(), {1, 2}, 0, 1),
    ]
    for fault, targets, anchor, values, sid, k in cases:
        snap = Snapshot(root/fault, targets)
        good = Index(snap, anchor)
        bad = Index(snap, anchor, name='bad-index', fault=fault)
        source = snap.root/'source'; write_sorted_source_features(source, values)
        expected = compute_independent_oracle_answer(targets, values, sid, k)
        answer, _ = execute_paid_exception_query(good, source, sid, k)
        wrong, _ = execute_paid_exception_query(bad, source, sid, k, fault=fault)
        assert answer == expected and wrong != expected
        print('falsified', fault, 'wrong', wrong, 'correct', expected)
        checks['falsified_variants'] += 1
    # ID-sensitive cross-bucket tie; all targets changed; and complete self-excluded zero tail.
    extra = [
        ({1: {0, 1, 2, 3}, 9: {0}}, {0, 1}, {0, 1}, None, 1),
        ({j: {1} for j in range(30)}, set(), {1}, 0, 10),
        ({j: set() for j in range(20)}, {0}, {7}, 0, 100),
    ]
    for number, (targets, anchor, values, sid, k) in enumerate(extra):
        snap = Snapshot(root/('extra%d' % number), targets); index = Index(snap, anchor)
        source = snap.root/'source'; write_sorted_source_features(source, values)
        answer, info = execute_paid_exception_query(index, source, sid, k)
        assert answer == compute_independent_oracle_answer(targets, values, sid, k)
        checks['adversarial_answers'] += 1
    # Full rebuild and old-reader coexistence; no stale-index scan fallback.
    old = Snapshot(root/'old', {10: {0, 1}, 20: {0, 1, 2}}, epoch=0)
    index = Index(old, {0})
    source = old.root/'source'; write_sorted_source_features(source, {0})
    old_answer, _ = execute_paid_exception_query(index, source, None, 1)
    refresh_start = perf_counter()
    new = Snapshot(root/'new', {10: {0, 1}, 20: {0}}, epoch=1)
    refreshed = Index(new, {0})
    new_answer, _ = execute_paid_exception_query(refreshed, source, None, 1)
    stats['refresh_seconds'] += perf_counter()-refresh_start
    assert old_answer == [(10, 1, 2, 1)] and new_answer == [(20, 1, 1, 1)]
    pinned, _ = execute_paid_exception_query(index, source, None, 1)
    assert pinned == old_answer
    for kind in ('index', 'source', 'postings', 'scratch', 'heads', 'selector', 'output', 'query_disk'):
        kwargs = {}
        active = refreshed
        if kind == 'index': active, kwargs = index, dict(snapshot=new)
        if kind == 'source': kwargs = dict(source_epoch=0)
        if kind == 'postings': new.post_token = old.token
        if kind == 'scratch':
            write_sorted_source_features(source, {1}); kwargs = dict(scratch_cap=0)
        if kind == 'heads': kwargs = dict(head_cap=0)
        if kind == 'selector': kwargs = dict(selector_cap=0)
        if kind == 'output': kwargs = dict(output_cap=0)
        if kind == 'query_disk': kwargs = dict(query_disk_cap=0)
        try:
            execute_paid_exception_query(active, source, 20, 1, **kwargs)
        except AssertionError:
            checks['rejected_'+kind] += 1
        else:
            raise AssertionError('admission/version bypass: '+kind)
        new.post_token = new.token
    assert stats['sort_peak'] <= 4 and stats['fanin_peak'] <= 2
print('checks', dict(sorted(checks.items())))
print('logical_counts', {key: value for key, value in sorted(stats.items()) if not key.endswith('seconds')})
print('stage_seconds', {key: round(value, 6) for key, value in stats.items() if key.endswith('seconds')})
print('whole_probe_seconds', round(perf_counter()-started, 6))
```

## Verification Status And Hard Gap

Executed the retained Python fence on 2026-09-20 with exit status 0. Reproduction from the repository root:

```sh
awk '/^```python/ {active=1; next} /^```/ {active=0} active {print}' research_algorithms_20260920/Similarity-Exception-Merge.md | /usr/bin/time -l python3.11
```

SHA-256 of the extracted Python source, including its final newline: `33c7d3d9995a46daca889e0a716ddb6c5f6d4040d909f241552e15c22a047a44`.

| Executed check | Result |
| --- | --- |
| Randomized file-backed answers | 800/800 equal the independent set-intersection oracle across 80 fixtures; seed 20260922. |
| Exhaustive two-feature universe | 8,192/8,192 exact answers: 64 ordered three-target assignments x 4 anchors x 4 sources x 2 self modes x 4 k values. This is exhaustive only over that domain. |
| Same-bucket materialize-then-union baseline | 200/200 answers match. Fused cursor consumption reads strictly fewer bucket records on 26 cases, never more on these cases. |
| Separating same-bucket example | n=1,040, g=1, q=40, k=10: fused reads 1 bucket record and performs 6 C probes; less-fused baseline reads 10 records. Both discover and score all 40 changed targets. This is not a 10x end-to-end speedup claim. |
| Cancellation | 200 signed posting records reduce to q=0; 5 bucket records supply the correct top-5. A touched-ID set cannot replace net reduction. |
| Deliberately incorrect variants | All four produce the explicit wrong answers shown below; repaired procedure matches the oracle. |
| Additional exact adversaries | Cross-bucket rational tie, all targets changed, and self-excluded complete zero tail all pass. |
| Bounds and finite buffers | Both bucket-record bounds and the binary-C-probe bound hold on every completed run. Run-buffer peak 4 records, merge fan-in peak 2, scratch payload peak 49,920 bytes. |
| Refresh/version/resource failures | Rebuilt target snapshot changes winner from ID 10 to ID 20; pinned old reader still returns 10. Mixed index, stale shared source, stale postings, insufficient scratch, heads, selector, output and combined query-disk reservations are each rejected. |

Wrong outputs are retained as compact falsification witnesses; each tuple is `(ID,a,b,actual_intersection)` except that a broken variant may report a false intersection:

```text
zero-size:   wrong (9,1,0,0), correct (1,1,5,0)
positive-id: wrong (1,1,3,1), correct (9,1,1,1)
forward-C:   wrong (1,1,3,1), correct (0,1,0,0)
late-self:   wrong (2,2,1,0), correct (1,2,1,1)
```

Logical traffic totals for the entire run, including baseline executions and deliberately broken variants: 22,833 random C-record probes / 365,328 bytes; 28,422 baseline-index probes, with 764,760 bytes of baseline reads including sequential empty-source access; 403,392 bucket-read bytes; 129,256 posting-read bytes; 941,472 scratch-read and 1,164,208 scratch-write bytes; 456,576 output-write bytes and the same independent output-check read bytes. Preparation writes total 129,848 bytes, separate index-tagged writes 82,104 bytes, and source/anchor writes 35,496 bytes. Bucket creation is recorded as scratch writes followed by a rename, not counted a second time as index writes. These are payload counters, not filesystem-allocation sizes or physical device traffic.

Recorded timing scope is explicit: the initial 80 snapshot/index builds took 0.136713 s; their 800 primary queries including staged output and test-sink reread took 1.282324 s; the exhaustive preparation/query/output/oracle loop took 4.770065 s; the tiny replacement snapshot/index plus new query took 0.001500 s. These named timers are not a complete stage partition: source normalization, baseline runs, adversarial fixtures and other setup are included in the whole run, not all in the named subtimers. Whole probe including fixture preparation, query, output, rebuild, checks and temporary cleanup took 6.908123 s; `/usr/bin/time -l` reported 6.94 s real, 19,775,488 bytes maximum RSS and zero swaps.

The host was arm64 with 25,769,803,776 bytes physical RAM; there was **no enforced 4 GB physical-memory limit**. Neither OS page-cache state nor cold-disk access was controlled. The small RSS observation includes the fixture/oracle dictionaries and interpreter, but is not a proof of the proposed packed implementation's memory bound. No production durability, concurrent refresh, crash recovery, allocator attribution, 100M-target run or whole-portfolio 50 GB retention test was performed.

**Surviving delta:** an exact, no-quality-gate query architecture with a complete exception partition and `O(g+k')` selection state, trading that property for two paid per-anchor target access paths. The sharper tested bucket-record bound is `min(n,g+q+k')` for positive-k queries with no final refill; random lookup, posting, output and refresh costs remain additional.

The main hard gap is not heap correctness: it is whether paying for all targets under each anchor, exact exception discovery, random C/ID lookups, complete output and refresh is better than the strongest matched alternative over a useful snapshot lifetime. A competitor given these same buckets and exceptions can run the same merge. Physical 4 GB / 50 GB lifecycle validation and a stronger originality claim remain unresolved.
