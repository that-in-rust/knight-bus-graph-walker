# Similarity And Embedding Evidence

Date: 2026-09-20. Evidence for [Certified Similarity](Certified-Similarity.md) and [Replay Embeddings](Replay-Embeddings.md). This file is within the sidecar's assigned write scope. No shared README, journal, implementation or other algorithm-family document is modified by this sidecar.

## Scope And Read Ledger

Read the portfolio README, the architecture decision map including D10-D14, the final research brief, the existing class-similarity proof, the D12 certified-join and D14 replay sections, and the hub-bounded similarity evidence. Existing artifacts are context, not independent new experiments.

The following external primary material was actually inspected through browser text extraction. Inspection was targeted, not a claim of full-paper replication or full-source audit. Equations used in the proposals are separately derived there.

| Source | Inspected material | Use and limitation |
| --- | --- | --- |
| [All-Pairs, WWW 2007](https://www.bayardo.org/ps/www2007.pdf) | Sections 2-4.6: nonnegative vectors, indexed candidate generation/completeness, binary constraints, disk-resident matching | Establishes exact indexing and bounded-index repeated scans as prior art; no timing reproduction. |
| [L2AP, ICDE 2014](https://davidanastasiu.net/pdf/papers/2014-AnastasiuK-ICDE-l2ap.pdf) | Section V, equations 3-4 and Algorithm 3; surrounding candidate-generation discussion | Prefix L2 bounds and residual filtering are not new contributions. |
| [Transformation-based KNN set search](https://www.jinwang18.net/files/tkde19-setknn.pdf) | Introduction/problem, representation discussion and section 5, Algorithm 2/Theorem 2 | Exact R-tree count-summary search is a close baseline; full appendix proof and implementation not audited. |
| [Dynamic Set kNN Self-Join](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf) | Sections II-III, IV-A local index, IV-G cosine/intersection/batch discussion | Closest inspected novelty threat to edit transfer. Difference indexes, reverse-kNN state and incremental intersections are established. |
| [Safe regions, SSTD 2009](https://www.aamircheema.com/research/SR_SSTD09.pdf) | Revision: full six-page paper, especially section 2's all-neighbor/all-outsider half-spaces and dynamic impact-region extension | Direct prior art for the dominance-domain principle, superseding the earlier metadata-only inspection. |
| [FastRP paper](https://arxiv.org/pdf/1908.11512) | Sections 3.1-3.5, equations 6-9, Algorithm 1 | Linear propagation/right degree scaling and sparse initialization; no claim of GDS equivalence. |
| [FastRP authors' implementation](https://github.com/GTmac/FastRP/blob/master/fastrp.py) | `fastrp_projection` and `fastrp_merge` bodies | Optional row normalization and final `scale` distinguish this code from our output profile. Mutable master inspected on this date; no commit-pinned parity audit. |
| [GDS FastRP reference](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/) | Introduction, raw neighborhood averaging and normalized weighted combination | Official documentation only; GDS source/RNG and actual arithmetic not tested. |
| [Very Sparse Random Projections](https://hastie.su.domains/Papers/Ping/KDD06_rp.pdf) | Sections 1 and 4.1, Lemmas 1-3, moment assumptions | Supports the distinction between deterministic operator preservation and statistical distortion. |
| [Learning with Hypergraphs](https://papers.nips.cc/paper_files/paper/2006/file/dff8e9c2ac33381546d96deea9922999-Paper.pdf) | Sections 3-6, equation 3, transition/Laplacian distinction | Incidence operators and hypergraph embedding are established; exact graph normalization differs. |
| [S4](https://arxiv.org/pdf/2111.00396) | Sections 2.4 and 3, diagonal-plus-low-rank discussion, Theorems 2-3 | Structured recurrences and generating-function evaluation are existing foundations; graph schedule novelty remains unresolved. |
| [Low-Rank Updates of Matrix Functions](https://arxiv.org/pdf/1707.03045) | Revision: section 3, Theorem 3.2 and its polynomial-exactness argument | Restricts algebraic novelty; does not supply this incidence/output schedule. |
| [LSSL](https://papers.neurips.cc/paper_files/paper/2021/file/05546b0e38ab9175cd905eebcc6ebb76-Paper.pdf) | Revision: Theorem 2 and exact-arithmetic qualification | Operation counts are distinct from bit complexity and stability. |
| [RandNE](https://zw-zhang.github.io/files/2018_ICDM_RandNE.pdf) | Revision: Algorithms 1 and 3, equations 5-7, section IV-D | Iterative projection and retained-intermediate dynamic updates predate the proposed schedule. |

The PMC SWOOP and moving-query pages became browser-check pages on attempted follow-up inspection; their methods are not relied on. The Stanford-hosted sparse-projection PDF failed once; the author's alternate host above was readable. A TAPNN PDF attempt failed and is not claimed as read. These gaps are not evidence of absent prior art.

The entire 270-line [independent review](Similarity-Embeddings-Independent-Review.md) was read, including its reproduction source and lifecycle audit. No review/shared file was edited. Its strongest claims were tested, rather than accepted as instructions or treated as novelty clearance. The dynamic set-kNN local-index, fallback and batch sections were re-inspected in the primary PDF during this revision.

## Computational Status

Only tiny ephemeral Python probes were executed. Python's exact `Fraction` arithmetic and exhaustive finite domains provide falsification evidence, not general proofs, production tests, resource measurements or timing benchmarks. The first launch with the system `python3` failed before testing because its integer type lacked `bit_count`; the same probe ran under the available `python3.11`. No package installation, generated source file, large dataset or scaffold was created.

The executable listings below preserve the probes in Markdown. They can be fed to Python 3.11 as standard input; they deliberately use full small oracle arrays and must not be mistaken for the proposed large-data memory schedule.

Revision after the independent review: the first two listings remain algebra probes. The third listing executes an actual file-backed finite-buffer similarity query, including reduced-delta scoring and fallback. Fixture preparation and independent expected results use small dictionaries; those dictionaries are not accessible to its query kernel. The fourth listing investigates grouped embedding completion. These distinctions supersede any earlier blanket statement that no schedule was executed.

## Similarity Probe

The binary domain has all 16 sets over four target features plus two duplicate targets, three fixed target universes, all 16 anchors, all 64 source sets over six features, witness sizes 2/4/8, two exclusion-ID cases and k in 0/1/3/30. Arbitrary exclusion IDs exercise the general exclusion operation as well as ordinary node self-exclusion. Cosine ranking is compared exactly via its squared positive ordering. Cases rejected by the certificate are counted, not declared fast-path successes.

```python
from fractions import Fraction as F
from collections import Counter
from itertools import product

def exact_rank_value(a, b, c, metric):
    if a == 0:
        return F(0)
    if metric == "intersection":
        return F(c)
    if metric == "jaccard":
        return F(c, a+b-c)
    return F(c*c, b or 1)

def compute_integer_safe_interval(P, targets, U, metric):
    lo, hi = 1, 10**9
    for uid in U:
        u = targets[uid]; cu = (P & u).bit_count(); bu = u.bit_count()
        for vid, v in targets.items():
            if vid in U:
                continue
            cv = (P & v).bit_count(); bv = v.bit_count()
            need = int(uid > vid)
            if metric == "jaccard":
                A, B = cu-cv, cu*bv-cv*bu
            elif metric == "intersection":
                A, B = 0, cu-cv
            else:
                A, B = 0, cu*cu*(bv or 1)-cv*cv*(bu or 1)
            if A > 0:
                lo = max(lo, -((B-need)//A))
            elif A < 0:
                hi = min(hi, (B-need)//(-A))
            elif B < need:
                return (1, 0)
    return lo, hi

stats = Counter(); sizes = Counter()
for metric in ["intersection", "jaccard", "cosine"]:
    for ids in [list(range(18)), list(range(0, 18, 2)), [0, 1, 7, 16]]:
        targets = {i: (i if i < 16 else (0 if i == 16 else 15)) for i in ids}
        for P in range(16):
            for h in [2, 4, 8]:
                U = sorted(targets, key=lambda j: (
                    -exact_rank_value(P.bit_count() or 1, targets[j].bit_count(),
                                      (P & targets[j]).bit_count(), metric), j))[:h]
                lo, hi = compute_integer_safe_interval(P, targets, U, metric)
                for a in range(1, 9):
                    def ordered_anchor_pair(u, v):
                        x = exact_rank_value(a, targets[u].bit_count(), (P & targets[u]).bit_count(), metric)
                        y = exact_rank_value(a, targets[v].bit_count(), (P & targets[v]).bit_count(), metric)
                        return x > y or (x == y and u < v)
                    expected = all(ordered_anchor_pair(u, v) for u in U for v in targets if v not in U)
                    assert expected == (lo <= a <= hi)
                    stats["interval_checks"] += 1
                for S in range(64):
                    a = S.bit_count()
                    C = {j for j, T in targets.items() if (S & T).bit_count() != (P & T).bit_count()}
                    for j, T in targets.items():
                        delta = sum((1 if S & (1 << f) else -1) for f in range(6)
                                    if ((P ^ S) & T) & (1 << f))
                        assert (P & T).bit_count()+delta == (S & T).bit_count()
                        stats["signed_delta_checks"] += 1
                    for source in [S if S < 18 else 99, 99]:
                        for k in [0, 1, 3, 30]:
                            kp = min(k, len(targets)-int(source in targets))
                            rank = lambda j: (-exact_rank_value(a, targets[j].bit_count(),
                                                               (S & targets[j]).bit_count(), metric), j)
                            oracle = sorted((j for j in targets if j != source), key=rank)[:kp]
                            if a == 0 or kp == 0:
                                actual = sorted(j for j in targets if j != source)[:kp]
                                stats["special_cases"] += 1
                            elif lo <= a <= hi and sum(j not in C and j != source for j in U) >= kp:
                                actual = sorted((set(U) | C)-{source}, key=rank)[:kp]
                                stats["accepted"] += 1
                                stats["accepted_nonidentical"] += P != S
                                sizes[metric] += 1
                            else:
                                stats["fallback"] += 1
                                continue
                            assert actual == oracle
                            stats["answer_checks"] += 1
print("binary", dict(stats))
print("accepted_by_metric", dict(sizes))
P = {0, 1}; S = {0, 1, 5, 6}; T = [{0}, {0, 1, 2, 3, 4}]
print("denominator_counterexample", [str(F(len(P&t), len(P|t))) for t in T],
      [str(F(len(S&t), len(S|t))) for t in T])

# Independent tiny integer-weighted cosine extension.
vectors = list(product(range(3), repeat=3)); weighted = Counter()
def compute_exact_dot_product(x, y):
    return sum(a*b for a, b in zip(x, y))
norms = [compute_exact_dot_product(t, t) for t in vectors]
for P in vectors:
    base = [compute_exact_dot_product(P, t) for t in vectors]
    for h in [2, 5, 9]:
        U = sorted(range(27), key=lambda j: (-F(base[j]**2, norms[j] or 1), j))[:h]
        for source, S in enumerate(vectors):
            dots = [compute_exact_dot_product(S, t) for t in vectors]
            delta = [compute_exact_dot_product(tuple(s-p for s, p in zip(S, P)), t) for t in vectors]
            assert all(base[j]+delta[j] == dots[j] for j in range(27))
            C = {j for j in range(27) if delta[j] != 0}
            for k in [1, 3, 30]:
                kp = min(k, 26)
                rank = lambda j: (-F(dots[j]**2, norms[j] or 1), j)
                oracle = sorted((j for j in range(27) if j != source), key=rank)[:kp]
                if not any(S):
                    actual = sorted(j for j in range(27) if j != source)[:kp]
                    weighted["zero_cases"] += 1
                elif sum(j not in C and j != source for j in U) >= kp:
                    actual = sorted((set(U) | C)-{source}, key=rank)[:kp]
                    weighted["accepted"] += 1
                else:
                    weighted["fallback"] += 1
                    continue
                assert actual == oracle
                weighted["answer_checks"] += 1
print("weighted", dict(weighted))
```

Observed binary results: 3,456 interval checks; 285,696 signed-delta checks; 63,460 accepted transfers (61,930 nonidentical source/anchor); 57,888 special cases; 121,348 answers checked; 99,836 required fallback. Accepted counts by mode: intersection 21,146; Jaccard 20,846; cosine 21,468. The concrete denominator counterexample changed scores from `(1/2,2/5)` to `(1/4,2/7)`.

Observed weighted extension: 1,422 checked answers, comprising 1,179 accepted transfers and 243 zero-source cases; 5,139 cases required fallback. Both listings in this file were executed directly from their Markdown code blocks under `python3.11`; both exited successfully.

## Embedding Probe

All 4,096 binary `4 x 3` incidence matrices are enumerated. The oracle explicitly constructs the loop-free weighted graph and its rational transition matrix. The candidate separately builds each feature history from its own reconstructed rows. Exact vectors, feature aggregates and squared norms are compared. The fixture is signed/dyadic and includes repeated node initialization; it is not the specified SHA-256 profile or a statistical-quality test.

```python
from fractions import Fraction as F
from collections import Counter
from math import nextafter, inf

N, G, DIM, L = 4, 3, 3, 4
R = [[F(((i+1)*(j+3)+j*j)%3-1) for j in range(DIM)] for i in range(N)]
stats = Counter()
for bits in range(1 << (N*G)):
    B = [[int(bool(bits & (1 << (i*G+g)))) for g in range(G)] for i in range(N)]
    counts = [sum(row[g] for row in B) for g in range(G)]
    weights = [F(g+1) if counts[g] >= 2 else F(0) for g in range(G)]
    degree = [sum(weights[g]*B[i][g]*(counts[g]-1) for g in range(G)) for i in range(N)]
    diag = [-sum(weights[g]*B[i][g] for g in range(G))/degree[i]
            if degree[i] else F(0) for i in range(N)]
    U = [[weights[g]*B[i][g]/degree[i] if degree[i] else F(0) for g in range(G)] for i in range(N)]
    V = [[B[i][g] if weights[g] else 0 for g in range(G)] for i in range(N)]
    P = [[sum(weights[g]*B[i][g]*B[j][g] for g in range(G))/degree[i]
          if i != j and degree[i] else F(0) for j in range(N)] for i in range(N)]
    E = R; Z = []
    def reconstruct_exact_node_layers(i, t):
        local = R[i][:]
        for ell in range(t):
            local = [diag[i]*local[j]+sum(U[i][g]*Z[ell][g][j] for g in range(G))
                     for j in range(DIM)]
        return local
    for t in range(L):
        current = [[F(0) for j in range(DIM)] for g in range(G)]
        for i in range(N):
            local = reconstruct_exact_node_layers(i, t)
            for g in range(G):
                for j in range(DIM):
                    current[g][j] += V[i][g]*local[j]
        expected = [[sum(V[i][g]*E[i][j] for i in range(N)) for j in range(DIM)] for g in range(G)]
        assert current == expected
        stats["exact_feature_cells"] += G*DIM
        Z.append(current)
        E = [[sum(P[i][v]*E[v][j] for v in range(N)) for j in range(DIM)] for i in range(N)]
        for i in range(N):
            local = reconstruct_exact_node_layers(i, t+1)
            assert local == E[i]
            assert sum(x*x for x in local) == sum(x*x for x in E[i])
            stats["exact_layer_rows"] += 1
            stats["exact_zero_rows"] += not any(local)
        stats["layer_checks"] += 1
    stats["incidence_matrices"] += 1
print(dict(stats))
print("self_subtraction_f64", (1.0+2.0**-54)-1.0, "exact_nonself", 2.0**-54)
print("outward_scalar_enclosure", (0.0, nextafter(1.0, inf)-1.0))
x, y = [F(1), F(0)], [F(-1), F(0)]
print("squared_norms_vs_sum", sum(v*v for v in x)+sum(v*v for v in y),
      sum((a+b)**2 for a, b in zip(x, y)))
```

Observed strengthened rerun: 4,096 matrices; 16,384 layers; 65,536 exact row/norm comparisons; 11,664 exact-zero rows; 147,456 independently built feature-history cells. Every assertion passed. The binary64 subtraction is zero while the exact neighbor value is `5.551115123125783e-17`; a conservative interval is `[0,2.220446049250313e-16]`, so its norm cannot be certified strictly positive. The norm-only shortcut gives 2 instead of 0.

## Evidence Limits And Surviving Deltas

- Similarity: an exact affine domain and surviving-witness proof can transfer ranking work between unequal source sets; a signed net-edit stream supplies complete corrections. The limited certificate avoids an uncharged pair-score matrix. Closest prior-art and amortization gaps remain.
- Embeddings: finite feature history plus diagonal local replay reconstructs raw vectors and full norms without node-layer/norm planes. Revision: grouped closure, exact group-zero dependence and admitted scaled-integer completion resolve the seed-8 fixture. Their large-data economic coverage remains unresolved.
- No 4 GB physical run, 50 GB production build, scalable external preparer/refresh/recovery implementation, general interval propagation library audit, GDS comparison or downstream quality evaluation was performed. The revised probes measure tiny file-backed queries, staged outputs and rebuilt refresh fixtures, not production lifecycle deadlines. Failed gates and failed certification are not successful results.
- The small tests confirm only the declared finite domains and exact identities. Large reference arrays used by these probes are not part of the production proposal. No performance percentages can be inferred from the case counts.

## Finite-Buffer Similarity Execution

Run this third Python fence independently. It uses temporary binary files, not a retained implementation. Target rows and posting directories are read with fixed-record cursors; candidate selection cannot access fixture dictionaries. Run names are generated from two integer counters, not an unbounded in-memory run catalog. Every sort/merge/output record is charged; files are opened unbuffered. Three input records and two merge cursors are deliberately small enough to force multi-generation merging and cross-run cancellation. The source and anchor are also file cursors. The stored witness list and selection heap are bounded by admitted `h` and `k`.

Fixture preparation is intentionally a tiny, dictionary-based reference builder, not the proposed external preparer. Its time and writes are counted separately. Python object/allocator overhead and OS page cache are not measured by these logical counters. The probe uses signed 64-bit records on small weights and arbitrary-precision comparison integers; it does not validate the proposed production 128/256-bit implementation.

```python
from collections import Counter
from fractions import Fraction as Q
from heapq import heappush, heapreplace, merge
from itertools import groupby
from pathlib import Path
from random import Random
from struct import Struct
from tempfile import TemporaryDirectory
from time import perf_counter

PAIR = Struct("<qq")
DIRECTORY = Struct("<qqqq")
POSTDIR = Struct("<qqq")
stats = Counter()

def read_fixed_record_stream(path, layout, offset=0, count=None):
    with open(path, "rb", buffering=0) as stream:
        stream.seek(offset)
        remaining = count
        while remaining is None or remaining > 0:
            data = stream.read(layout.size)
            stats["read_bytes"] += len(data)
            if not data:
                assert remaining in (None, 0)
                break
            assert len(data) == layout.size
            if remaining is not None:
                remaining -= 1
            yield layout.unpack(data)

def write_fixed_record_stream(path, layout, records):
    count = 0
    with open(path, "wb", buffering=0) as stream:
        for record in records:
            data = layout.pack(*record)
            assert stream.write(data) == len(data)
            stats["write_bytes"] += len(data)
            count += 1
    return count

def compute_streamed_vector_dot(left, right):
    a, b = iter(left), iter(right)
    x, y = next(a, None), next(b, None)
    total = 0
    while x is not None and y is not None:
        if x[0] == y[0]:
            total += x[1]*y[1]
            x, y = next(a, None), next(b, None)
        elif x[0] < y[0]:
            x = next(a, None)
        else:
            y = next(b, None)
    return total

def stream_signed_vector_edits(source, anchor):
    entries = merge(read_fixed_record_stream(source, PAIR),
                    ((f, -v) for f, v in read_fixed_record_stream(anchor, PAIR)))
    for feature, records in groupby(entries, key=lambda x: x[0]):
        delta = sum(v for _, v in records)
        if delta:
            yield feature, delta

class Snapshot:
    def __init__(self, root, targets, epoch=0, projection="binary", order=0):
        self.root, self.n = root, len(targets)
        self.token = (epoch, projection, order)
        self.post_token = self.token
        self.data, self.directory = root/"rows", root/"directory"
        self.posts, self.postdir = root/"posts", root/"postdir"
        root.mkdir()
        offset = 0
        directory = []                    # fixture preparation only
        for node, row in sorted(targets.items()):
            norm = sum(v*v for v in row.values())
            directory.append((node, offset, len(row), norm))
            offset += PAIR.size*len(row)
        write_fixed_record_stream(self.data, PAIR,
            ((f, v) for _, row in sorted(targets.items()) for f, v in sorted(row.items())))
        write_fixed_record_stream(self.directory, DIRECTORY, directory)
        features = sorted({f for row in targets.values() for f in row})
        self.nfeatures = len(features)
        postdir, posts = [], []            # fixture preparation only
        for f in features:
            entries = [(j, row[f]) for j, row in sorted(targets.items()) if f in row]
            postdir.append((f, len(posts)*PAIR.size, len(entries)))
            posts.extend(entries)
        write_fixed_record_stream(self.posts, PAIR, posts)
        write_fixed_record_stream(self.postdir, POSTDIR, postdir)

    def lookup(self, path, layout, count, key):
        lo, hi = 0, count
        while lo < hi:
            mid = (lo+hi)//2
            record = next(read_fixed_record_stream(path, layout, mid*layout.size, 1))
            if record[0] < key:
                lo = mid+1
            elif record[0] > key:
                hi = mid
            else:
                return record
        return None

    def row(self, record):
        return read_fixed_record_stream(self.data, PAIR, record[1], record[2])

    def target(self, node):
        return self.lookup(self.directory, DIRECTORY, self.n, node)

    def posting(self, feature):
        return self.lookup(self.postdir, POSTDIR, self.nfeatures, feature)

def compute_exact_score_key(a, b, c, metric):
    if not a:
        return Q(0)
    if metric == "intersection":
        return Q(c)
    return Q(c, a+b-c) if metric == "jaccard" else Q(c*c, b or 1)

def offer_bounded_rank_record(heap, k, node, a, b, c, metric):
    if not k:
        return
    entry = (compute_exact_score_key(a, b, c, metric), -node, node, a, b, c)
    if len(heap) < k:
        heappush(heap, entry)
    elif entry > heap[0]:
        heapreplace(heap, entry)
    stats["heap_peak"] = max(stats["heap_peak"], len(heap))

def finish_bounded_rank_records(heap):
    return [(j, a, b, c) for _, _, j, a, b, c in sorted(heap, reverse=True)]

def scan_complete_exact_targets(snap, source, source_id, a, k, metric):
    heap = []
    for record in read_fixed_record_stream(snap.directory, DIRECTORY):
        node, _, _, b = record
        if node == source_id:
            continue
        c = compute_streamed_vector_dot(read_fixed_record_stream(source, PAIR), snap.row(record))
        stats["full_scores"] += 1
        offer_bounded_rank_record(heap, k, node, a, b, c, metric)
    return finish_bounded_rank_records(heap)

def build_bounded_rank_certificate(snap, anchor, h, metric):
    a = sum(v*v for _, v in read_fixed_record_stream(anchor, PAIR))
    U = {j: (b, c) for j, _, b, c in scan_complete_exact_targets(
        snap, anchor, None, max(1, a), h, metric)}
    lo, hi = 1, 2**32-1
    for record in read_fixed_record_stream(snap.directory, DIRECTORY):
        vid, _, _, bv = record
        if vid in U:
            continue
        cv = compute_streamed_vector_dot(read_fixed_record_stream(anchor, PAIR), snap.row(record))
        for uid, (bu, cu) in U.items():
            need = int(uid > vid)
            if metric == "jaccard":
                slope, intercept = cu-cv, cu*bv-cv*bu
            elif metric == "intersection":
                slope, intercept = 0, cu-cv
            else:
                slope, intercept = 0, cu*cu*(bv or 1)-cv*cv*(bu or 1)
            if slope > 0:
                lo = max(lo, -((intercept-need)//slope))
            elif slope < 0:
                hi = min(hi, (intercept-need)//(-slope))
            else:
                assert intercept >= need
    return (snap.token, metric), U, (lo, hi)

def reduce_finite_posting_stream(records, root, count, cap, capacity=3, fanin=2):
    assert capacity > 0 and fanin >= 2 and 2*PAIR.size*count <= cap
    buffer, runs, live = [], 0, 0
    def path(g, r):
        return root/("run_%d_%d" % (g, r))
    def write_run(g, r, items):
        nonlocal live
        def tracked():
            nonlocal live
            for item in items:
                live += PAIR.size
                assert live <= cap
                stats["scratch_peak"] = max(stats["scratch_peak"], live)
                yield item
        write_fixed_record_stream(path(g, r), PAIR, tracked())
    seen = 0
    for record in records:
        buffer.append(record); seen += 1
        stats["sort_peak"] = max(stats["sort_peak"], len(buffer))
        assert len(buffer) <= capacity
        if len(buffer) == capacity:
            buffer.sort()
            write_run(0, runs, buffer)
            runs += 1; buffer.clear()
    assert seen == count
    if buffer or not runs:
        buffer.sort(); write_run(0, runs, buffer); runs += 1; buffer.clear()
    generation = 0
    while runs > 1:
        next_runs = 0
        for start in range(0, runs, fanin):
            stop = min(runs, start+fanin)
            cursors = [read_fixed_record_stream(path(generation, r), PAIR)
                       for r in range(start, stop)]
            stats["fanin_peak"] = max(stats["fanin_peak"], len(cursors))
            def reduced():
                for key, values in groupby(merge(*cursors), key=lambda x: x[0]):
                    delta = sum(value for _, value in values)
                    if delta:
                        yield key, delta
            write_run(generation+1, next_runs, reduced())
            for r in range(start, stop):
                file = path(generation, r)
                live -= file.stat().st_size; file.unlink()
            next_runs += 1
        runs, generation = next_runs, generation+1
    stats["merge_generations"] += generation
    # The last run may be an unreduced initial run. The cursor below reduces it.
    return path(generation, 0)

def stream_reduced_net_records(path):
    for node, values in groupby(read_fixed_record_stream(path, PAIR), key=lambda x: x[0]):
        delta = sum(value for _, value in values)
        if delta:
            yield node, delta

def query_finite_rank_certificate(snap, source, anchor, source_id, cert, k, metric,
                                  scratch, cap=10**8, source_epoch=None, strict=False):
    assert snap.token == snap.post_token, "mixed target/posting snapshot"
    assert source_epoch is None or source_epoch == snap.token[0], "mixed shared source"
    a = sum(v*v for _, v in read_fixed_record_stream(source, PAIR))
    kp = min(k, snap.n-int(snap.target(source_id) is not None)) if source_id is not None else min(k, snap.n)
    def fallback(reason):
        stats["fallback_"+reason] += 1
        return scan_complete_exact_targets(snap, source, source_id, a, kp, metric), reason
    if not kp:
        stats["empty_k"] += 1
        return [], "empty_k"
    if cert[0] != (snap.token, metric):
        return fallback("version")
    U, (lo, hi) = cert[1:]
    if not a:
        return fallback("zero_source")
    if not lo <= a <= hi:
        return fallback("domain")
    count = 0
    for f, _ in stream_signed_vector_edits(source, anchor):
        entry = snap.posting(f)
        count += entry[2] if entry else 0
    if 2*PAIR.size*count > cap:
        return fallback("scratch")
    def records():
        for feature, delta in stream_signed_vector_edits(source, anchor):
            entry = snap.posting(feature)
            if entry:
                for node, value in read_fixed_record_stream(snap.posts, PAIR, entry[1], entry[2]):
                    stats["posting_records"] += 1
                    yield node, value*delta
    run = reduce_finite_posting_stream(records(), scratch, count, cap)
    survivors = len(U)-int(source_id in U)
    changed = 0
    for node, delta in stream_reduced_net_records(run):
        changed += 1
        survivors -= int(node in U and node != source_id)
    stats["net_records"] += changed
    if survivors < kp or (strict and changed):
        run.unlink()
        return fallback("survival" if survivors < kp else "strict_safe")
    heap = []
    combined = merge(stream_reduced_net_records(run), ((node, 0) for node in sorted(U)))
    for node, values in groupby(combined, key=lambda x: x[0]):
        delta = sum(value for _, value in values)
        if node == source_id:
            continue
        if node in U:
            b, cp = U[node]
        else:
            record = snap.target(node)
            b = record[3]
            cp = compute_streamed_vector_dot(read_fixed_record_stream(anchor, PAIR), snap.row(record))
            stats["anchor_recoveries"] += 1
        c = cp+delta
        assert c >= 0
        stats["candidate_scores"] += 1
        offer_bounded_rank_record(heap, kp, node, a, b, c, metric)
    run.unlink(); stats["accepted"] += 1
    return finish_bounded_rank_records(heap), "accepted"

def compute_independent_expected_output(targets, source, sid, k, metric):
    a = sum(v*v for v in source.values())
    records = [(j, a, sum(v*v for v in row.values()),
                sum(value*row.get(f, 0) for f, value in source.items()))
               for j, row in targets.items() if j != sid]
    return sorted(records, key=lambda x: (-compute_exact_score_key(*x[1:], metric), x[0]))[:k]

rng = Random(20260921)
started = perf_counter()
checks = Counter()
with TemporaryDirectory(prefix="similarity-ephemeral-") as temp:
    root = Path(temp)
    for case in range(60):
        weighted = case % 2
        metric = "cosine" if weighted else ("intersection", "jaccard", "cosine")[(case//2)%3]
        targets = {j*7-35: {f: (rng.randrange(1, 4) if weighted else 1)
                           for f in range(8) if rng.randrange(3) == 0} for j in range(case%13)}
        prep_start = perf_counter()
        snap = Snapshot(root/str(case), targets, projection="weighted" if weighted else "binary")
        anchor_values = {f: (rng.randrange(1, 4) if weighted else 1)
                         for f in range(8) if rng.randrange(3) == 0}
        anchor, source = snap.root/"anchor", snap.root/"source"
        write_fixed_record_stream(anchor, PAIR, sorted(anchor_values.items()))
        cert = build_bounded_rank_certificate(snap, anchor, case%7, metric)
        stats["prepare_seconds"] += perf_counter()-prep_start
        for step in range(6):
            values = (dict(anchor_values) if step == 0 else
                      {f: (rng.randrange(1, 4) if weighted else 1)
                       for f in range(10) if rng.randrange(3) == 0})
            write_fixed_record_stream(source, PAIR, sorted(values.items()))
            sid, k = (step*7-35 if step%2 else 999), (0, 1, 3, 20, 2, 1)[step]
            query_start = perf_counter()
            result, route = query_finite_rank_certificate(snap, source, anchor, sid, cert, k,
                metric, snap.root, cap=0 if step == 4 else 10**8)
            stats["query_seconds"] += perf_counter()-query_start
            output_start = perf_counter()
            stage, final = snap.root/"output-stage", snap.root/"output-final"
            write_fixed_record_stream(stage, DIRECTORY, result)
            stage.replace(final)
            stats["output_seconds"] += perf_counter()-output_start
            assert list(read_fixed_record_stream(final, DIRECTORY)) == result
            expected = compute_independent_expected_output(targets, values, sid, k, metric)
            assert result == expected, (case, step, route, result, expected)
            # Charged-state comparator: a full per-source anchor-dot cache, then net updates.
            cached = {j: sum(v*row.get(f, 0) for f, v in anchor_values.items()) for j, row in targets.items()}
            for f in anchor_values.keys() | values.keys():
                difference = values.get(f, 0)-anchor_values.get(f, 0)
                for j, row in targets.items():
                    cached[j] += difference*row.get(f, 0)
            assert all(cached[j] == sum(v*targets[j].get(f, 0) for f, v in values.items()) for j in targets)
            cached_rows = [(j, sum(v*v for v in values.values()), sum(v*v for v in targets[j].values()), c)
                           for j, c in cached.items() if j != sid]
            cached_result = sorted(cached_rows, key=lambda x:
                (-compute_exact_score_key(*x[1:], metric), x[0]))[:k]
            assert cached_result == expected
            checks[route] += 1; checks[metric] += 1
    # Cancellation across hundreds of runs, including both signs on every target.
    targets = {j: {0: 1, 1: 1, j+2: 1} for j in range(1000)}
    snap = Snapshot(root/"cancellation", targets)
    anchor, source = snap.root/"anchor", snap.root/"source"
    write_fixed_record_stream(anchor, PAIR, [(0, 1)])
    write_fixed_record_stream(source, PAIR, [(1, 1)])
    cert = build_bounded_rank_certificate(snap, anchor, 4, "jaccard")
    before = stats.copy()
    result, route = query_finite_rank_certificate(snap, source, anchor, 9999, cert, 3, "jaccard", snap.root)
    assert route == "accepted" and result == compute_independent_expected_output(targets, {1: 1}, 9999, 3, "jaccard")
    print("cancellation", {key: stats[key]-before[key] for key in
        ("posting_records", "net_records", "candidate_scores", "merge_generations", "read_bytes", "write_bytes")})
    # A nonempty exception set: strict safe reuse rejects, survivor reuse accepts.
    targets = {j: dict.fromkeys(list(range(8))+[8+j], 1) for j in range(1024)}
    snap = Snapshot(root/"nearcore", targets)
    anchor, source = snap.root/"anchor", snap.root/"source"
    write_fixed_record_stream(anchor, PAIR, [(f, 1) for f in range(8)])
    write_fixed_record_stream(source, PAIR, sorted(targets[0].items()))
    cert = build_bounded_rank_certificate(snap, anchor, 8, "jaccard")
    before = stats.copy()
    result, route = query_finite_rank_certificate(snap, source, anchor, 0, cert, 3, "jaccard", snap.root)
    strict, strict_route = query_finite_rank_certificate(snap, source, anchor, 0, cert, 3,
        "jaccard", snap.root, strict=True)
    assert result == strict == compute_independent_expected_output(targets, targets[0], 0, 3, "jaccard")
    assert (route, strict_route) == ("accepted", "strict_safe")
    print("nearcore", {key: stats[key]-before[key] for key in
        ("posting_records", "candidate_scores", "full_scores", "anchor_recoveries")})
    # Force the real cardinality-domain fallback with unchanged intersections.
    targets = {0: {0: 1}, 1: dict.fromkeys(range(5), 1)}
    snap = Snapshot(root/"domain", targets)
    anchor, source = snap.root/"anchor", snap.root/"source"
    write_fixed_record_stream(anchor, PAIR, [(0, 1), (1, 1)])
    values = dict.fromkeys((0, 1, 5, 6), 1)
    write_fixed_record_stream(source, PAIR, sorted(values.items()))
    cert = build_bounded_rank_certificate(snap, anchor, 1, "jaccard")
    result, route = query_finite_rank_certificate(snap, source, anchor, 99, cert, 1, "jaccard", snap.root)
    assert route == "domain" and result == compute_independent_expected_output(targets, values, 99, 1, "jaccard")
    checks["forced_domain"] += 1
    # Stale target certificate, shared-source generation, and mixed postings.
    old = Snapshot(root/"old", {10: {0: 1, 1: 1}, 20: {0: 1, 1: 1, 2: 1}}, epoch=0)
    refresh_start = perf_counter()
    new = Snapshot(root/"new", {10: {0: 1, 1: 1}, 20: {0: 1}}, epoch=1)
    stats["refresh_build_seconds"] += perf_counter()-refresh_start
    anchor, source = root/"version_anchor", root/"version_source"
    write_fixed_record_stream(anchor, PAIR, [(0, 1)])
    write_fixed_record_stream(source, PAIR, [(0, 1)])
    cert = build_bounded_rank_certificate(old, anchor, 1, "jaccard")
    answer, route = query_finite_rank_certificate(new, source, anchor, 99, cert, 1, "jaccard", new.root)
    assert route == "version" and answer[0][0] == 20
    pinned, _ = query_finite_rank_certificate(old, source, anchor, 99, cert, 1, "jaccard", old.root)
    assert pinned[0][0] == 10
    old_source = root/"old_shared_source"
    write_fixed_record_stream(old_source, PAIR, [(0, 1), (1, 1), (2, 1)])
    old_shared, _ = query_finite_rank_certificate(old, old_source, anchor, 20, cert, 1,
        "jaccard", old.root, source_epoch=0)
    new_shared, _ = query_finite_rank_certificate(new, source, anchor, 20, cert, 1,
        "jaccard", new.root, source_epoch=1)
    assert old_shared == [(10, 3, 2, 2)] and new_shared == [(10, 1, 2, 1)]
    checks["shared_snapshot_answers"] += 2
    # Deliberately forge a certificate token to reproduce the unsafe stale answer.
    forged = ((new.token, "jaccard"), cert[1], cert[2])
    stale, _ = query_finite_rank_certificate(new, source, anchor, 99, forged, 1, "jaccard", new.root)
    assert stale[0][0] == 10 and stale != answer
    metric_answer, metric_route = query_finite_rank_certificate(old, source, anchor, 99, cert, 1,
        "cosine", old.root)
    assert metric_route == "version" and metric_answer == [(10, 1, 2, 1)]
    checks["metric_guard"] += 1
    for mode in ("shared", "postings"):
        if mode == "postings":
            new.post_token = old.token
        try:
            query_finite_rank_certificate(new, source, anchor, 20, cert, 1, "jaccard", new.root,
                                          source_epoch=0 if mode == "shared" else None)
        except AssertionError:
            checks["rejected_"+mode] += 1
        else:
            raise AssertionError("mixed snapshot accepted")
    assert stats["sort_peak"] <= 3 and stats["fanin_peak"] <= 2
    assert stats["anchor_recoveries"] > 0
print("random_answers", dict(sorted(checks.items())))
print("logical_io", {key: stats[key] for key in
    ("read_bytes", "write_bytes", "sort_peak", "fanin_peak", "heap_peak", "scratch_peak", "anchor_recoveries")})
print("fixture_prepare_seconds", round(stats["prepare_seconds"], 6),
      "whole_probe_seconds", round(perf_counter()-started, 6))
print("tiny_stage_seconds", {key: round(stats[key], 6) for key in
    ("query_seconds", "output_seconds", "refresh_build_seconds")})
```

## Grouped Exact Completion Execution

This fourth independent fence runs integer-scaled grouped moments with file-backed membership and row-metadata cursors, a separate dense rational operator oracle, complete normalized binary32/binary64 output certification, and a separately written uniform-diagonal baseline. Membership/feature counts are logical counts. Python integers are checked against a derived width but are not physically packed to that width. Initial vectors here are supplied through an immutable input file; the K4 input is generated by the exact declared SHA profile. No intermediate node-layer/norm file or dense feature Gram matrix is used by the candidate.

`Q` below is the integer common transition denominator; `Rat` names rational arithmetic. The admitted exact-output test uses a precomputed precision, not retry-until-lucky. Integer square-root bounds decide raw zeros and enclose every nonzero norm. The independent dense oracle compares **all raw reconstructed layers**, including zero vectors; this plus the elementary outward rational bound certifies the output. The small implementation still needs a production primitive/packing audit.

```python
from collections import Counter
from fractions import Fraction as Rat
from hashlib import sha256
from math import gcd, isqrt, isfinite, inf, nextafter, lcm
from pathlib import Path
from random import Random
from struct import Struct, pack, unpack
from tempfile import TemporaryDirectory
from time import perf_counter

WORD, ROW = Struct("<Q"), Struct("<QQQQQQ")
stats = Counter()

def compute_declared_hash_value(seed, node, dimension, r=1):
    digest = sha256(b"DFHR-v1"+seed.to_bytes(32, "little")
                   +node.to_bytes(8, "little")+dimension.to_bytes(4, "little")).digest()
    value = int.from_bytes(digest[:8], "little")
    return 0 if value & ((1 << r)-1) else (-1 if (value >> r) & 1 else 1)

class Incidence:
    def __init__(self, root, raw, weights, initial, L, bits_cap=4096, groups_cap=100, scalar_cap=10000, zero_only=False):
        root.mkdir()
        self.path, self.directory, self.init = root/"incidence", root/"rows", root/"initial"
        self.n, self.d, self.L = len(raw), len(initial[0]), L
        counts = [sum(f in row for row in raw) for f in range(len(weights))]
        active = [f for f, count in enumerate(counts) if count >= 2]
        remap = {f: i for i, f in enumerate(active)}
        self.weights = [weights[f] for f in active]
        self.F = len(active)
        rows = [[remap[f] for f in sorted(row) if f in remap] for row in raw]
        counts = [counts[f] for f in active]
        qs = [sum(self.weights[f] for f in row) for row in rows]
        ss = [sum(self.weights[f]*(counts[f]-1) for f in row) for row in rows]
        hs = [gcd(*(self.weights[f] for f in row)) for row in rows]
        diagonals = [(-Rat(q, s) if s else Rat(0)) for q, s in zip(qs, ss)]
        self.groups = sorted(set(di for di, s in zip(diagonals, ss) if s))
        assert len(self.groups) <= groups_cap, "group admission"
        assert (len(self.groups)+(0 if zero_only else L))*self.F*self.d <= scalar_cap, "scalar admission"
        self.S = max(x.denominator for row in initial for x in row)
        assert self.S & (self.S-1) == 0
        assert all(self.S % x.denominator == 0 for row in initial for x in row)
        scaled = [[int(x*self.S) for x in row] for row in initial]
        self.M = max(abs(x) for row in scaled for x in row)
        if not zero_only:
            self.Q = 1
            for s, h in zip(ss, hs):
                if s:
                    row_denominator = s//h
                    reduced = self.Q//gcd(self.Q, row_denominator)
                    assert reduced <= ((1 << bits_cap)-1)//row_denominator, "denominator admission"
                    self.Q = reduced*row_denominator
        bound = max(1, (1 if zero_only else 3)*self.n*self.M)
        maximum = (1 << (bits_cap-2))-1
        assert bound <= maximum, "integer width admission"
        for _ in range(L if self.M and not zero_only else 0):
            assert bound <= maximum//self.Q, "integer width admission"
            bound *= self.Q
        self.W = bound.bit_length()+2
        assert self.W <= bits_cap, "integer width admission"
        self.initial_width = max(1, (self.W+7)//8)
        self.m = sum(map(len, rows))
        with open(self.path, "wb", buffering=0) as data, open(self.directory, "wb", buffering=0) as directory:
            offset = 0
            for row, q, s, di, h in zip(rows, qs, ss, diagonals, hs):
                group = self.groups.index(di) if s else len(self.groups)
                directory.write(ROW.pack(offset, len(row), q, s, group, h))
                for f in row:
                    data.write(WORD.pack(f))
                offset += WORD.size*len(row)
        with open(self.init, "wb", buffering=0) as data:
            for row in scaled:
                for x in row:
                    data.write(x.to_bytes(self.initial_width, "little", signed=True))

    def records(self):
        with open(self.directory, "rb", buffering=0) as stream:
            for i in range(self.n):
                data = stream.read(ROW.size); stats["metadata_bytes"] += len(data)
                yield i, ROW.unpack(data)

    def features(self, record):
        with open(self.path, "rb", buffering=0) as stream:
            stream.seek(record[0])
            for _ in range(record[1]):
                stats["membership_visits"] += 1
                yield WORD.unpack(stream.read(WORD.size))[0]

    def initial(self, node):
        with open(self.init, "rb", buffering=0) as stream:
            stream.seek(node*self.d*self.initial_width)
            data = stream.read(self.d*self.initial_width)
            stats["initial_bytes"] += len(data)
        return [int.from_bytes(data[j*self.initial_width:(j+1)*self.initial_width],
                               "little", signed=True) for j in range(self.d)]

    def check(self, value):
        assert abs(value).bit_length()+1 <= self.W
        return value

def build_grouped_integer_history(data, zero_shortcut=True):
    if not data.L:
        return [], False
    G = [[[0]*data.d for _ in range(data.F)] for _ in data.groups]
    for i, row in data.records():
        value = data.initial(i)
        for f in data.features(row):
            for j in range(data.d):
                G[row[4]][f][j] = data.check(G[row[4]][f][j]+value[j])
                stats["feature_units"] += 1
    zero = all(x == 0 for group in G for vector in group for x in vector)
    if zero and zero_shortcut:
        return [], True
    H = []
    for t in range(data.L):
        total = [[data.check(sum(group[f][j] for group in G))
                  for j in range(data.d)] for f in range(data.F)]
        H.append(total)
        if t+1 == data.L:
            break
        for g, diagonal in enumerate(data.groups):
            factor = int(data.Q*diagonal)
            for f in range(data.F):
                for j in range(data.d):
                    G[g][f][j] = data.check(factor*G[g][f][j])
        for _, row in data.records():
            if not row[3]:
                continue
            local = [0]*data.d
            for f in data.features(row):
                factor = (data.Q//(row[3]//row[5]))*(data.weights[f]//row[5])
                for j in range(data.d):
                    local[j] = data.check(local[j]+factor*total[f][j])
                    stats["feature_units"] += 1
            for f in data.features(row):
                for j in range(data.d):
                    G[row[4]][f][j] = data.check(G[row[4]][f][j]+local[j])
                    stats["feature_units"] += 1
    return H, False

def build_uniform_integer_baseline(data):
    assert len(data.groups) <= 1
    if not data.L:
        return []
    H = [[[0]*data.d for _ in range(data.F)]]
    for i, row in data.records():
        value = data.initial(i)
        for f in data.features(row):
            for j in range(data.d):
                H[0][f][j] += value[j]
    factor = int(data.Q*data.groups[0]) if data.groups else 0
    for _ in range(data.L-1):
        previous = H[-1]
        current = [[factor*x for x in vector] for vector in previous]
        for _, row in data.records():
            local = [0]*data.d
            for f in data.features(row):
                u = (data.Q//(row[3]//row[5]))*(data.weights[f]//row[5])
                for j in range(data.d):
                    local[j] += u*previous[f][j]
            for f in data.features(row):
                for j in range(data.d):
                    current[f][j] += local[j]
        H.append(current)
    return H

def enclose_integer_normalized_row(values, precision):
    norm2 = sum(x*x for x in values)
    if not norm2:
        return [(Rat(0), Rat(0)) for _ in values]
    scaled = norm2 << (2*precision)
    lower = isqrt(scaled)
    upper = lower if lower*lower == scaled else lower+1
    result = []
    for x in values:
        a, b = Rat(x << precision, lower), Rat(x << precision, upper)
        result.append((min(a, b), max(a, b)))
    return result

def choose_nearest_format_value(value, output_bits):
    if output_bits == 64:
        middle = float(value)
        values = (nextafter(middle, -inf), middle, nextafter(middle, inf))
        encode = lambda x: unpack("<Q", pack("<d", x))[0]
    else:
        middle = unpack("<I", pack("<f", float(value)))[0]
        values = [unpack("<f", pack("<I", bits))[0]
                  for bits in (middle-1, middle, middle+1) if 0 <= bits < 2**32]
        encode = lambda x: unpack("<I", pack("<f", x))[0]
    return min((x for x in values if isfinite(x)), key=lambda x: (abs(Rat(x)-value), encode(x) & 1))

def iterate_exact_completed_rows(data, H, zero, alpha, tau, bits, oracle):
    A = sum(abs(a) for a in alpha)
    precision = 1
    while A*Rat(1, 2**precision) > tau/4:
        precision += 1
    assert precision <= 4096              # planned before output, never retried
    for i, row in data.records():
        local = [[0]*data.d for _ in range(data.L)]
        if H:
            for f in data.features(row):
                u = (data.Q//(row[3]//row[5]))*(data.weights[f]//row[5])
                for t in range(data.L):
                    for j in range(data.d):
                        local[t][j] = data.check(local[t][j]+u*H[t][f][j])
                        stats["feature_units"] += 1
        e = data.initial(i)
        factor = -(data.Q//(row[3]//row[5]))*(row[2]//row[5]) if row[3] else 0
        y = [(Rat(0), Rat(0)) for _ in range(data.d)]
        for t in range(data.L+1):
            expected = oracle[t][i]         # assertion only; not used by candidate
            assert [Rat(x, data.S*data.Q**t) for x in e] == expected
            stats["raw_row_checks"] += 1
            stats["zero_row_checks"] += int(not any(e))
            norm = enclose_integer_normalized_row(e, precision)
            for j, (lo, hi) in enumerate(norm):
                lo, hi = sorted((alpha[t]*lo, alpha[t]*hi))
                y[j] = (y[j][0]+lo, y[j][1]+hi)
            if t < data.L:
                e = [data.check(factor*x+local[t][j]) for j, x in enumerate(e)]
        result = []
        for lo, hi in y:
            lo, hi = max(lo, -A), min(hi, A)
            value = choose_nearest_format_value((lo+hi)/2, bits)
            error = max(abs(Rat(value)-lo), abs(Rat(value)-hi))
            if error > tau:
                assert lo == hi             # tests below deliberately use exact 1/3
                raise ValueError("output-format-impossible")
            result.append(value)
            stats["output_coordinates"] += 1
        yield result

def emit_exact_completed_rows(data, H, zero, alpha, tau, bits, oracle):
    stage, final = data.path.with_name("output-stage"), data.path.with_name("output-final")
    layout = Struct("<"+("f" if bits == 32 else "d")*data.d)
    try:
        with open(stage, "wb", buffering=0) as stream:
            for result in iterate_exact_completed_rows(data, H, zero, alpha, tau, bits, oracle):
                encoded = layout.pack(*result)
                assert stream.write(encoded) == len(encoded)
                stats["output_bytes"] += len(encoded)
        stage.replace(final)
        stats["published_outputs"] += 1
    except BaseException:
        stage.unlink(missing_ok=True)
        raise
    # Independent small test sink reads published output only after kernel completion.
    rows = []
    with open(final, "rb", buffering=0) as stream:
        for _ in range(data.n):
            rows.append(list(layout.unpack(stream.read(layout.size))))
        assert stream.read(1) == b""
    A, precision = sum(abs(a) for a in alpha), 1
    while A*Rat(1, 2**precision) > tau/4:
        precision += 1
    return rows, precision

def compute_dense_independent_layers(raw, weights, initial, L):
    n = len(raw)
    W = [[sum(w for f, w in enumerate(weights) if f in raw[i] and f in raw[j])
          if i != j else 0 for j in range(n)] for i in range(n)]
    P = [[Rat(w, sum(row)) if sum(row) else Rat(0) for w in row] for row in W]
    layers = [initial]
    for _ in range(L):
        old = layers[-1]
        layers.append([[sum(P[i][j]*old[j][c] for j in range(n))
                        for c in range(len(initial[0]))] for i in range(n)])
    return layers

def complete_group_zero_profile(data, alpha, tau, bits):
    # This route intentionally never accesses Q or any propagated layer/history.
    initial_groups = [[[0]*data.d for _ in range(data.F)] for _ in data.groups]
    for i, record in data.records():
        initial = data.initial(i)
        for f in data.features(record):
            for j in range(data.d):
                initial_groups[record[4]][f][j] = data.check(initial_groups[record[4]][f][j]+initial[j])
    if any(x for group in initial_groups for row in group for x in row):
        return None
    del initial_groups
    A, precision = sum(abs(a) for a in alpha), 1
    while A*Rat(1, 2**precision) > tau/4:
        precision += 1
    result = []
    for i, record in data.records():
        coefficient = sum(a*((-1)**t) for t, a in enumerate(alpha)) if record[3] else alpha[0]
        row = []
        for lo, hi in enclose_integer_normalized_row(data.initial(i), precision):
            lo, hi = sorted((coefficient*lo, coefficient*hi))
            value = choose_nearest_format_value((lo+hi)/2, bits)
            assert max(abs(Rat(value)-lo), abs(Rat(value)-hi)) <= tau
            row.append(value)
        result.append(row)
    return result

started = perf_counter()
rng = Random(620092027)
checks = Counter()
with TemporaryDirectory(prefix="embeddings-ephemeral-") as temp:
    root = Path(temp)
    for case in range(180):
        n, F, d, L = rng.randrange(1, 7), rng.randrange(6), rng.randrange(1, 5), rng.randrange(7)
        raw = [{f for f in range(F) if rng.randrange(2)} for _ in range(n)]
        weights = [rng.randrange(1, 6) for _ in range(F)]
        initial = [[Rat(rng.randrange(-3, 4), 2**rng.randrange(4)) for _ in range(d)] for _ in range(n)]
        alpha = [Rat(rng.randrange(-3, 4), 2) for _ in range(L+1)]
        data = Incidence(root/str(case), raw, weights, initial, L)
        before = stats.copy()
        H, zero = build_grouped_integer_history(data, zero_shortcut=False)
        oracle = compute_dense_independent_layers(raw, weights, initial, L)
        result, precision = emit_exact_completed_rows(data, H, zero, alpha, Rat(1, 10**5),
                                                     32 if case%2 else 64, oracle)
        if L:
            assert stats["membership_visits"]-before["membership_visits"] == 2*L*data.m
            assert stats["feature_units"]-before["feature_units"] == (3*L-1)*data.m*d
        else:
            assert stats["membership_visits"] == before["membership_visits"]
        if len(data.groups) <= 1:
            baseline_before = stats["membership_visits"]
            assert H == build_uniform_integer_baseline(data)
            assert stats["membership_visits"]-baseline_before == ((2*L-1)*data.m if L else 0)
            checks["matched_uniform"] += 1
        checks["fixtures"] += 1
        checks["max_width"] = max(checks["max_width"], data.W)
        checks["max_groups"] = max(checks["max_groups"], len(data.groups))
    # Legal SHA seed 8, exact-zero group certificate, both output formats.
    initial = [[Rat(compute_declared_hash_value(8, i, 0))] for i in range(4)]
    assert initial == [[1], [-1], [0], [0]]
    data = Incidence(root/"seed8", [{0} for _ in range(4)], [1], initial, 2)
    before = stats.copy()
    H, zero = build_grouped_integer_history(data)
    assert H == [] and zero
    oracle = compute_dense_independent_layers([{0} for _ in range(4)], [1], initial, 2)
    for bits in (32, 64):
        result, precision = emit_exact_completed_rows(data, H, zero, [Rat(0), Rat(0), Rat(1)],
                                                     Rat(1, 10**20), bits, oracle)
        assert result == [[1.0], [-1.0], [0.0], [0.0]]
    print("seed8", result, "zero_route_memberships", stats["membership_visits"]-before["membership_visits"],
          "Q", data.Q, "W", data.W)
    denominator = data.Q
    del data.Q
    assert complete_group_zero_profile(data, [Rat(0), Rat(0), Rat(1)], Rat(1, 10**20), 64) == result
    data.Q = denominator
    checks["zero_profile_without_Q"] += 1
    # Full refresh: dropping the negative initialized member invalidates the zero proof.
    refresh_start = perf_counter()
    changed_raw = [{0}, set(), {0}, {0}]
    fresh = Incidence(root/"refresh", changed_raw, [1], initial, 2)
    stats["refresh_prepare_seconds"] += perf_counter()-refresh_start
    refresh_start = perf_counter()
    changed_history, changed_zero = build_grouped_integer_history(fresh)
    assert not changed_zero
    assert complete_group_zero_profile(fresh, [Rat(0), Rat(0), Rat(1)], Rat(1, 10**5), 64) is None
    changed_oracle = compute_dense_independent_layers(changed_raw, [1], initial, 2)
    changed_output, _ = emit_exact_completed_rows(fresh, changed_history, changed_zero,
        [Rat(0), Rat(0), Rat(1)], Rat(1, 10**5), 64, changed_oracle)
    assert changed_output == [[1.0], [0.0], [1.0], [1.0]] and changed_output != result
    assert data.path.with_name("output-final").exists()
    with open(data.path.with_name("output-final"), "rb") as old_reader:
        assert unpack("<dddd", old_reader.read()) == (1.0, -1.0, 0.0, 0.0)
    stats["refresh_query_output_seconds"] += perf_counter()-refresh_start
    checks["refresh_zero_invalidated"] += 1
    # Raw singleton provenance: activate a second feature, then deactivate it again.
    singleton_initial = [[Rat(1)], [Rat(0)], [Rat(1)], [Rat(0)]]
    singleton_rows = ([{0}, {0}, {1}, set()], [{0}, {0}, {1}, {1}], [{0}, {0}, {1}, set()])
    singleton_outputs = []
    for epoch, raw_rows in enumerate(singleton_rows):
        refreshed = Incidence(root/("singleton_%d" % epoch), raw_rows, [1, 1], singleton_initial, 1)
        histories, zeros = build_grouped_integer_history(refreshed)
        oracle = compute_dense_independent_layers(raw_rows, [1, 1], singleton_initial, 1)
        output, _ = emit_exact_completed_rows(refreshed, histories, zeros, [Rat(0), Rat(1)],
                                             Rat(1, 10**5), 64, oracle)
        singleton_outputs.append(output)
    assert singleton_outputs[0] == singleton_outputs[2]
    assert singleton_outputs[0][3] == [0.0] and singleton_outputs[1][3] == [1.0]
    checks["singleton_refreshes"] += 2
    # Total Z0=0 is NOT invariant when the diagonal varies.
    raw = [{1}, {1}, {0}, {0, 1}]
    initial = [[Rat(x)] for x in (-1, 0, -1, 1)]
    data = Incidence(root/"falsezero", raw, [1, 1], initial, 2)
    H, zero = build_grouped_integer_history(data)
    assert not zero and H[0] == [[0], [0]]
    assert complete_group_zero_profile(data, [Rat(0), Rat(0), Rat(1)], Rat(1, 10**5), 64) is None
    assert [Rat(x[0], data.S*data.Q) for x in H[1]] == [Rat(1, 3), -Rat(1, 6)]
    print("total_zero_not_invariant", "groups", len(data.groups), "Z1", [str(Rat(x[0], data.Q)) for x in H[1]])
    # Nonzero supplied dyadic input at the minimum binary64 subnormal magnitude.
    initial = [[Rat(1)], [Rat(1, 2**1074)]]
    data = Incidence(root/"subnormal", [{0}, {0}], [1], initial, 1)
    H, zero = build_grouped_integer_history(data)
    oracle = compute_dense_independent_layers([{0}, {0}], [1], initial, 1)
    result, _ = emit_exact_completed_rows(data, H, zero, [Rat(0), Rat(1)], Rat(1, 10**20), 64, oracle)
    assert result == [[1.0], [1.0]]
    checks["subnormal_completion"] += 1
    # Exact output-format floor, L=0. sqrt(9) is exact, so no enclosure ambiguity.
    initial = [[Rat(1)]*9]
    data = Incidence(root/"floor", [set()], [], initial, 0)
    for bits in (32, 64):
        try:
            emit_exact_completed_rows(data, [], False, [Rat(1)], Rat(1, 10**20), bits, [initial])
        except ValueError as failure:
            assert str(failure) == "output-format-impossible"
            assert not data.path.with_name("output-stage").exists()
            assert not data.path.with_name("output-final").exists()
            error = abs(Rat(choose_nearest_format_value(Rat(1, 3), bits))-Rat(1, 3))
            print("format_floor", bits, str(error))
            checks["format_rejections"] += 1
        else:
            raise AssertionError("impossible output accepted")
    # Review regression: zero admission must not construct a general Q/history.
    primes = [2, 3, 5, 7, 11, 13]
    star_rows, star_initial, feature = [], [], 0
    for degree in primes:
        star_rows.append(set(range(feature, feature+degree)))
        star_initial.append([Rat(1)])
        for f in range(feature, feature+degree):
            star_rows.append({f}); star_initial.append([Rat(-1)])
        feature += degree
    try:
        Incidence(root/"star-general-reject", star_rows, [1]*feature, star_initial, 8, bits_cap=8)
    except AssertionError as failure:
        assert str(failure) == "denominator admission"
    else:
        raise AssertionError("general denominator should not fit")
    zero_data = Incidence(root/"star-zero-first", star_rows, [1]*feature, star_initial, 8,
                          bits_cap=8, scalar_cap=64, zero_only=True)
    assert not hasattr(zero_data, "Q") and zero_data.W == 8
    assert complete_group_zero_profile(zero_data, [Rat(0)]*8+[Rat(1)], Rat(1, 10**5), 64) == star_initial
    checks["zero_before_general_admission"] += 1
    # Review improvement: globally shared weights must cancel before LCM.
    primes += [17, 19, 23, 29, 31, 37, 41, 43, 47, 53]
    pair_rows = [{f} for f in range(16) for _ in range(2)]
    pair_initial = [[Rat(1)] for _ in pair_rows]
    pair_data = Incidence(root/"prime-weight-pairs", pair_rows, primes, pair_initial, 8, bits_cap=16)
    assert pair_data.Q == 1 and pair_data.W == 9
    assert 2+(3*len(pair_rows)*lcm(*primes)**8).bit_length() == 528
    pair_history, _ = build_grouped_integer_history(pair_data, zero_shortcut=False)
    pair_oracle = compute_dense_independent_layers(pair_rows, primes, pair_initial, 8)
    pair_output, _ = emit_exact_completed_rows(pair_data, pair_history, False,
        [Rat(0)]*8+[Rat(1)], Rat(1, 10**5), 64, pair_oracle)
    assert pair_output == [[1.0] for _ in pair_rows]
    checks["reduced_denominator_admission"] += 1
    for mode in ("groups", "width", "scalars"):
        try:
            Incidence(root/("reject_"+mode), raw, [1, 1], [[Rat(x)] for x in (-1, 0, -1, 1)], 20,
                      bits_cap=8 if mode == "width" else 4096, groups_cap=1 if mode == "groups" else 100,
                      scalar_cap=1 if mode == "scalars" else 10000)
        except AssertionError:
            checks["admission_"+mode] += 1
        else:
            raise AssertionError("resource admission bypassed")
print("grouped_checks", dict(sorted(checks.items())))
print("execution_counts", dict(sorted(stats.items())))
print("whole_probe_seconds", round(perf_counter()-started, 6))
```

## Revised Denominator And Admission Receipt

The fourth fence was revised after the independent completion review and replayed successfully. It now uses the minimal common denominator of the declared U, divide-before-multiply coefficient evaluation, the tighter norm interval width, and a separate zero-only preparer profile. Its row record adds the weight gcd; the earlier metadata byte counts below are historical.

Latest fourth-fence results: 180 random fixtures, 135 matched uniform histories, 2,685 raw-row checks, 758 zero rows, and 188 complete binary-output publications, all passing. Maximum scalar width in the randomized fixtures fell from 113 to 95 bits. The 47-node star fixture completes zero discovery with eight-bit initial cells without constructing Q or reserving general history, while its general route is correctly rejected. Sixteen prime-weighted pairs admit Q=1 and nine-bit cells instead of the old 528-bit allowance, and their full output matches the independent dense oracle.

Latest logical counters were 5,386 membership visits, 185,616 metadata bytes, 11,285 initialization bytes, 12,909 feature units and 9,588 binary output bytes. The larger totals include the added pair-output and zero-admission regressions; they are not a like-for-like timing comparison. The zero-only test helper returns a tiny fixture result list for assertions; a production implementation must stream that output as the manuscript requires. Complete packed midpoint/rounding/gcd-workspace admission is specified but not implemented by this Fraction-based probe. Do not describe the record/bit counters as measured physical memory.

## Revision Run Receipt

All four authored Python fences and the independent review's counterexample fence were executed from Markdown with Python 3.11 after the revisions; every command exited zero. The review fence reproduced its exact interval endpoints, including the non-singleton zero-row enclosure. The new route's success is therefore tested against the actual reported failure, not just a different random input. An initial text-patch attempt failed to match a heading and changed no file; it was corrected before execution. No scientific test failure is hidden by that editing failure.

| Executed check | Observation | Limit |
| --- | --- | --- |
| Original finite similarity/oracle checks | 121,348 binary and 1,422 weighted answers, 3,456 domains, 285,696 signed identities matched | Original rejected cases still do not execute fallback; use the new listing for that obligation. |
| Original exhaustive embedding checks | 4,096 incidence inputs; 65,536 raw rows; 147,456 feature cells matched | Exact algebra, not an interval or resource benchmark. |
| File-backed similarity | 360 random complete outputs matched direct enumeration and dense incremental-dot results; forced domain, scratch, survivor and version fallback paths executed | The dense comparator is not LI-DSN-Join; full external preparation is not implemented. |
| Actual delta-based scoring | 30 streamed anchor recoveries outside `U`; canceled popular edits: 2,000 postings, ten merge generations, zero final deltas, four scores | Canceled support does not imply cheap discovery. |
| Similarity bounded state | Maximum sort occupancy 3 records, merge fan-in 2, observed selector occupancy 11, scratch payload peak 44,288 B | Record counters exclude Python allocation, filesystem allocation granularity and page cache. |
| Similarity snapshot tests | Old/new shared-source score witnesses matched; inconsistent source/posting generations rejected; wrong-metric certificate uses fallback; deliberately forged stale token produces the wrong old winner | Demonstrates necessity of trusted immutable manifests, not production transaction/recovery machinery. |
| Grouped constructor/output | 180 random graphs plus adversarial/refresh fixtures; 2,397 raw rows checked, including 758 zero rows; 187 complete output files published | Fixture preparer and independent dense oracle remain small-memory test conveniences. |
| Matched uniform baseline | 135 cases have exactly equal histories; candidate membership and feature-work equations checked fixture by fixture | No claimed advantage in this already-known single-group regime. |
| Zero/format completion | Seed 8 completes in both formats; no-Q zero route passes; minimum-subnormal input normalizes correctly; two `1/3` tolerance requests rejected with no staged/final output | General interval propagation and all resource domains remain unaudited. |
| Embedding refresh | Removing the negative K4 member invalidates the old zero proof; rebuilt output is `(1,0,1,1)` while old reader retains `(1,-1,0,0)`; singleton activation/deactivation checked | Full fixture rebuild, not an efficient incremental update algorithm. |

One reference run of the third listing recorded 2,025,240 logical read bytes and 964,008 write bytes, including fixture preparation, output verification and comparisons. For its 360-query randomized subset: fixture/certificate preparation was 0.046868 s, querying 0.309445 s, and output staging/publication 0.053563 s. The tiny replacement-target builder took 0.000286 s. Whole probe, including adversarial fixtures and independent expected results, took 0.932728 s. These timings **do not add up to a production lifecycle quote**: the named subsets differ and the whole run includes oracle overhead; no cold-cache or physical 4 GB restriction was imposed.

The fourth listing recorded 4,792 membership visits, 139,400 row-metadata bytes, 12,345 initialization bytes, 12,173 candidate feature units, and 9,332 actual binary output bytes for 1,563 coordinates. The totals include matched-baseline rereads and special zero/refresh tests; the formula assertions concern each ordinary randomized candidate separately. One run took 0.308895 s overall; the four-node refresh used 0.000415 s for fixture preparation and 0.000734 s for history/oracle/output work. These are reproducibility receipts, not scale predictions. Temporary files were removed by the enclosing temporary-directory contexts.

### Surviving Deltas And Hard Gap

- **A05:** a tested finite-buffer signed reducer feeds actual candidate scoring and a real complete fallback; the rank-domain/survivor theorem remains valid. The interesting combination is bounded cross-source reuse with exceptions, not safe-region intersection or signed posting updates alone. An end-to-end advantage over strong dynamic-set indexes before target refresh is unestablished.
- **A06:** grouped moments replace quadratic history-building replay with a paid `G*F*d` plane; exact group-zero dependence and bounded integer/norm arithmetic provide explicit completion domains. Ordinary membership passes do not decrease, and the uniform baseline matches. Global denominator growth, output representation and rational workspace constrain coverage.
- **Still open:** a production external preparer, packed arithmetic/cursor/publication/recovery implementation, physical 4 GB peak, portfolio-wide 50 GB retention under overlapping generations, matched lifecycle crossover, and novelty of the complete combinations. No shared document or review was changed, and no PageRank work was duplicated.
