# Similarity Through An Outsider Score Envelope

Date: 2026-09-20. A05 research extension. Exact Jaccard, frozen target snapshot, arbitrary source-set changes with exact exception correction. This is a stronger algorithm than the original all-witness safe interval, not an established global novelty claim or a measured machine-budget result.

## Question And Design Choice

The original [A05 certificate](Certified-Similarity.md) requires every retained witness to outrank every outsider throughout an integer cardinality interval. That is sufficient but unnecessarily restrictive: an irrelevant weak witness can invalidate a correct top-1 answer. Keeping more witnesses can even narrow that interval.

First considered: retain a piecewise minimum count of witnesses beating each outsider, then subtract a count of changed witnesses. That gives a robust rank envelope, but costs up to O(nh) pair events and discards the identities of the actual surviving witnesses. A simpler and stronger construction is available: retain the **best counterfactual outsider score as a function of source cardinality**, and compare it with the actual kth surviving witness. Do not implement the weaker rank-count proposal first.

The three expert checks here are order-statistic correctness, bounded external construction, and skeptical nearest-art comparison. The relevant user pain is repeated exact similar-neighbor queries over a stable target snapshot, where source sets vary but touching common postings can be expensive. We do not assume those queries are common without workload evidence.

## Exact Contract

Keep A05's projection, binary sets, stable IDs, exact intersection, source-self exclusion, net signed posting reduction, and pinned snapshot semantics. This extension initially covers only Jaccard with nonempty query source S. Empty-source behavior follows the explicitly declared baseline, not a division by zero.

Let P be an anchor, p=|P|, U any retained witness set of size h, and O the target universe minus U. For every target j, store or recover b_j=|T_j| and c_j=|P intersect T_j|. For a positive source cardinality a define the counterfactual score

```text
f_j(a) = c_j / (a + b_j - c_j).
order  = higher score first, then smaller stable target ID.
C      = {j : |S intersect T_j| - c_j != 0}.
W      = U minus C minus the query's own target ID.
```

All denominators are positive because c_j<=b_j and a>=1. A counterfactual score can exceed one when c_j>a; that is harmless as a bound, but it is not an actual Jaccard score. For every unchanged j, counterfactual and actual scores coincide, hence c_j<=a there.

Admit source cardinalities 1<=a<=A_max. Outside that range use the existing complete baseline. For a packed reference format use A_max<=2^63-2, all cardinalities <=2^63-1, target count <=2^63-1, and checked signed 128-bit pairwise arithmetic. Actual row, run, index and runtime bytes still require admission.

## Dominance Compression Before The Envelope

For each distinct c, retain the outsider with smallest b, then smallest ID. For c>0 it has the highest f(a) among outsiders with that c at every a. For c=0 all scores equal zero, so retain the **smallest ID regardless of b**. Thus at most p+1 outsiders need represent the whole target tail.

This detail matters: choosing the shortest zero-overlap target can violate the ID tie order. If any positive-c outsider exists, all zero-c outsiders lose to it for every admitted a and may be dropped from the score envelope. If O is empty, the candidate set U already covers all targets and no envelope comparison is necessary.

Let E(a) be the best retained outsider representative under the exact order. It is also the best of **all** counterfactual outsiders. It is an upper bound for every unchanged outsider, even when the envelope's winner itself is changed or is the excluded source. That conservatism can cause rejection, never an incorrect acceptance.

## Exact Integer Hull

For positive c, maximizing c/(a+b-c) is equivalent to minimizing the line a/c+(b-c)/c. Lower envelopes and convex-hull tricks are existing techniques. We implement the equivalent integer crossing rule so rounded intersections cannot move a tie boundary.

Process representatives in increasing c. For a new representative x and an older y, c_x>c_y. The new one first outranks the older one at

```text
tau_xy = 0 if ID_x < ID_y else 1
first  = ceil((tau_xy - c_x*b_y + c_y*b_x)/(c_x-c_y)).
```

This follows by cross multiplication; the integral comparison is
`(c_x-c_y)*a + c_x*b_y-c_y*b_x >= tau_xy`.

Maintain a stack of `(first_cardinality, c, b, ID)` with increasing starts. Compare each new representative with the last stack entry. If its first winning cardinality is no later than that entry's start, pop and compare again. Otherwise append at that crossing, clipped below by 1; discard if it would first win beyond A_max. The first entry starts at 1. Each representative is pushed and popped at most once. The strict ID-aware integer threshold retains a line that wins at only a single integer cardinality when appropriate.

The stack can be a capped resident array or an external append/pop file with bounded page buffers. Querying uses predecessor search on starts. No n-entry mutable target array is required. Representative count is at most p+1; the tighter nonempty envelope bound is e<=min(n_o,max(1,p),A_max). It can be Theta(p), not generically constant-sized. The independent review gives the realizable family (c,b,ID)=(c,c^2,c), whose c-th row starts winning at a=c(c-1)+1.

## Query And Correctness

```text
query_with_outsider_envelope(S, k, certificate):
    validate snapshot, projection, anchor, ID order and source epoch
    compute a and k'; return empty immediately when k'=0
    use the declared baseline if a is outside the positive admitted domain
    use A05's bounded signed posting reducer to produce exact C and deltas
    if O is not empty:
        look up E(a)
        merge ID-sorted U against C and exclude self
        count unchanged witnesses strictly ahead of E(a), saturating at k'
        if the count is less than k': use complete baseline
    score U union C exactly, exclude self, and return its exact top k'
```

Use k'=min(k, eligible target count) throughout. The counting gate is equivalent to asking whether the kth surviving witness outranks E(a), but uses constant additional threshold state and O(h) score comparisons. Matching survival against ID-sorted C costs O(h+|C|) sequential record work. The former proposed threshold heap would cost O(h log(k'+1)) comparisons; it did not justify the earlier O(h) claim. Final candidate selection still has its own charged top-k workspace. The final U-union-C scoring and complete fallback follow the separately executed A05 procedure; the new bounded reducer/count-gate/candidate composition has not yet been integration-tested. No full source-versus-target rescore is hidden inside the proposed fast-query cost.

**Theorem.** If the gate accepts, the complete universe's exact top-k' answer is contained in U union C.

**Proof.** Every omitted eligible j lies in O minus C. Its true score/order equals f_j(a) and is no better than E(a). The accepted gate supplies k' unchanged, eligible witnesses, all strictly ahead of E(a) in the total score/ID order. They are therefore all ahead of j. No omitted j can be in the true top k'. Exact rescoring and selection over U union C returns that top k'. This remains true when some changed candidates beat all witnesses; that only adds winners to the candidate set. A failed gate says nothing about whether that candidate set would nevertheless have been sufficient.

**Dominance over the old gate.** Whenever the original all-U interval and survival gate accept, at least k' surviving witnesses outrank every outsider and hence E(a); this gate also accepts. The converse is false. For fixed surviving witnesses, the new gate exactly tests their dominance over the global counterfactual outsider maximum. It is not a maximal gate for the actual query because a changed outsider can unnecessarily raise that maximum.

## Separating Family And Failure Cases

Take P={0,1}; U contains ID 0 with target {0,1} and ID 1 with target {0}. Every one of N outsiders has {0,1,2,3,4}, with IDs greater than 1. At anchor size two these are the actual top two witnesses. Query S={0,1,100,101}, k=1. The new features occur in no target, so C is empty.

```text
At a=4:
strong witness   2/4 = 1/2
outsider         2/7
weak witness     1/4

Old all-U certificate: rejects because the weak witness loses.
New envelope gate:     accepts because the strong witness wins.
```

The outsider envelope has one record for every N. With paid preparation, repeated such queries score two candidates instead of the old gate's full fallback over N+2 targets. Posting lookups, source parsing and output still count. This separates the two specified certificate algorithms; it does **not** prove superiority over the strongest dynamic set-kNN index or a competent competitor given the same outsider envelope.

Failure cases to retain:

- Many distinct anchor overlaps can create a large envelope; bounded lookup does not make its preparation/retention free.
- A common edited feature can emit most targets before net cancellation. The new gate does not remove that reducer cost.
- If the strongest envelope outsider changes, retaining it in the bound may reject even when all unchanged outsiders are harmless.
- Target edits invalidate representatives and hull boundaries. An old certificate is invalid even when the current source equals its anchor.
- Self-exclusion and zero-score ID ties are part of the theorem, not output cleanup.
- A smaller certificate-build cost may still lose overall when each anchor is used only once.

## Resource And Lifecycle Model

Let n_o=|O|, p=|P|, z<=min(n_o,p+1) be representative count, e<=z envelope size, M the sort buffer and B the I/O block. Compute each target's c using the existing admitted anchor-intersection builder. This pass may be expensive; neither an n-by-anchor score plane nor free random anchor lookups is assumed.

For resident grouping, reserve O(p) representative slots. Otherwise stream `(c,b,ID)` records into bounded external runs, sort by c and its correct winner order, and reduce each c to one representative. For zero c, use ID as the first tie key. Build the hull with bounded stack pages. Logical additional work is O(n_o log n_o) comparison sorting in the straightforward external implementation and O(z) stack operations. Per query, the corrected count gate uses O(h) score comparisons, O(h+|C|) survival-matching records, and O(log(e+1)) conservative lookup probes. Final selection and exception discovery remain separate. The original certificate explicitly performs O(n_o*h) pairwise inequalities after the same score discovery. Neither builder expression includes source intersection discovery.

External I/O is Sort(n_o) plus O(z) conservative stack-record reads/writes and O(log e) conservative predecessor record reads per query. A page-aware index can improve lookup; charge that index. Scratch includes simultaneously live sort input/output runs and hull staging. Final retention includes U, anchor data, e hull records, metadata and any index, plus all pinned old versions. The hull and representative artifacts share blocks only when actually implemented that way.

Refresh initially rebuilds this certificate from the new target snapshot. Cancelled builds release runs and unpublished hull files; a partially written hull is never a queryable generation. No incremental hull maintenance result is asserted. Preparation, changed-source posting reads, complete candidate output and failed-gate fallback must be reported separately.

## Prior Art And Claim Boundary

Inspected primary sources, not novelty clearance:

- [Hasan et al., SSTD 2009, Section 2](https://www.aamircheema.com/research/SR_SSTD09.pdf): its safe region intersects retained-versus-outsider dominance half-spaces. That is the basis already acknowledged for the original A05, not our invention.
- [Li et al., influential-neighbor moving kNN, VLDB 2014](https://user.it.uu.se/~wangyi/pdf-files/2014/vldb14.pdf): the follow-up reviewer inspected Sections 6.1-6.2 and Algorithm 2. Comparing a current kth-result boundary with a guarding outsider is established; a result threshold is not a newly invented reuse principle.
- [Amagata et al., dynamic set-kNN, ICDE 2019](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf): previously inspected in the independent A05 review; exact signed updates and dynamic set-neighbor maintenance are direct competing territory.

The candidate contribution is the **cardinality/overlap-compressed outsider certificate combined with exact changed-intersection exceptions and a bounded construction/query schedule**. The lower envelope, dominance skyline, rank threshold and external sort are established ingredients. A separating theorem against our previous certificate is a useful improvement, not proof that this complete combination is unpublished. Compare against a competitor explicitly equipped with the same skyline/envelope before a research-priority claim.

## Independent Review And Revised Disposition

The [A05 follow-up review](Similarity-Embeddings-Independent-Review.md#follow-up-new-a05-outsider-envelope) found no accepted-answer or discrete-hull counterexample. It reports 126,900 exact hull checks and 122,880 gate cases, including count-gate equivalence, oversized k, self-exclusion, empty pools and tie-only hull entries. These are reviewer-authored finite checks, not a physical-budget result. The retained author fence below remains the reviewed historical oracle-level query check; its bounded external builder is executed, but its exception production and survivor sorting do not implement the revised full query schedule.

The review also constructs an exact strong-comparator reduction: map each positive-c outsider to (1/c,(b-c)/c), then perform an ID-aware linear support query with weight (a,1). It inspected the [Onion technique](https://sigmodrecord.org/publications/sigmodRecord/0006/pdfs/The%20Onion%20Technique_%20Indexing%20for%20Linear%20Optimization%20Queries.pdf), [robust ranked-query indexing](https://hanj.cs.illinois.edu/pdf/vldb06_indexrank.pdf), and [ranked-view reuse](https://dbucsd.github.io/paperpdfs/2004_12.pdf). Equipped with the same exceptions and gate, that comparator makes identical admission decisions, including all improvements over our older gate. This is reviewer-inspected primary evidence and a constructive equivalence, not a claim that the lead reread every source or that one paper publishes this complete combination verbatim.

Keep this as a useful compact-certificate architecture, not a demonstrated algorithmic novelty. The separate [paid exception-merge index](Similarity-Exception-Merge.md) offers complete answers without this certificate gate, at Theta(n) additional baseline records per anchor and explicitly charged refresh costs. Neither plan universally dominates the other.

## Executable Probe

The following standalone Python fence builds the representative stream and hull through binary files, bounded sort runs of four records and two-way merge. The envelope builder consumes triples, not the fixture's target dictionary. The dictionary is confined to generating fixtures and independently checking envelope/answer correctness. Query exception production is oracle-assisted here; the separately executed finite A05 reducer remains the procedure-level evidence for that component. This probe does not establish physical RAM, crash consistency or large-scale timing.

```python
from fractions import Fraction
from heapq import merge
from pathlib import Path
from random import Random
from struct import Struct
from tempfile import TemporaryDirectory

TRIPLE, HULL = Struct('<QQQ'), Struct('<QQQQ')
stats = dict(fixtures=0, envelope_checks=0, queries=0, accepts=0,
             strict_accepts=0, sort_peak=0, merge_peak=0, stack_pops=0)

def order_target_counterfactual_key(row, a):
    c,b,i = row
    return (-Fraction(c,a+b-c),i)

def order_representative_record_key(row):
    c,b,i = row
    return (c, b if c else 0, i)

def iterate_packed_triple_records(path):
    with open(path,'rb',buffering=0) as f:
        while True:
            b = f.read(TRIPLE.size)
            if not b: return
            assert len(b)==TRIPLE.size
            yield TRIPLE.unpack(b)

def sort_bounded_triple_records(rows, root, capacity=4):
    generation = count = 0
    buffer = []
    for row in rows:
        buffer.append(row)
        stats['sort_peak']=max(stats['sort_peak'],len(buffer))
        if len(buffer)==capacity:
            with open(root/f'r-{generation}-{count}','wb',buffering=0) as f:
                for x in sorted(buffer,key=order_representative_record_key): f.write(TRIPLE.pack(*x))
            count+=1; buffer.clear()
    if buffer:
        with open(root/f'r-{generation}-{count}','wb',buffering=0) as f:
            for x in sorted(buffer,key=order_representative_record_key): f.write(TRIPLE.pack(*x))
        count+=1; buffer.clear()
    if not count: return None
    while count>1:
        outputs=0
        for start in range(0,count,2):
            inputs=[root/f'r-{generation}-{j}' for j in range(start,min(count,start+2))]
            stats['merge_peak']=max(stats['merge_peak'],len(inputs))
            with open(root/f'r-{generation+1}-{outputs}','wb',buffering=0) as f:
                for row in merge(*(iterate_packed_triple_records(p) for p in inputs),
                                 key=order_representative_record_key): f.write(TRIPLE.pack(*row))
            for p in inputs: p.unlink()
            outputs+=1
        count=outputs; generation+=1
    return root/f'r-{generation}-0'

def build_bounded_outsider_envelope(rows, root, maximum):
    sorted_path=sort_bounded_triple_records(rows,root)
    path=root/'hull'; count=0; previous_c=None
    with open(path,'w+b',buffering=0) as f:
        if sorted_path is None: return path,0
        for c,b,i in iterate_packed_triple_records(sorted_path):
            if c==previous_c: continue
            previous_c=c
            start=1
            while count:
                f.seek((count-1)*HULL.size)
                oldstart,oc,ob,oi=HULL.unpack(f.read(HULL.size))
                assert c>oc
                numerator=(0 if i<oi else 1)-c*ob+oc*b
                denominator=c-oc
                start=max(1,-((-numerator)//denominator))
                if start>oldstart: break
                count-=1; f.truncate(count*HULL.size); stats['stack_pops']+=1
            if start<=maximum:
                f.seek(count*HULL.size); f.write(HULL.pack(start,c,b,i)); count+=1
    sorted_path.unlink()
    return path,count

def lookup_exact_outsider_envelope(path, count, a):
    if not count: return None
    lo,hi=0,count
    with open(path,'rb',buffering=0) as f:
        while lo<hi:
            mid=(lo+hi)//2; f.seek(mid*HULL.size)
            start,*_=HULL.unpack(f.read(HULL.size))
            if start<=a: lo=mid+1
            else: hi=mid
        assert lo
        f.seek((lo-1)*HULL.size)
        _,c,b,i=HULL.unpack(f.read(HULL.size))
    return c,b,i

rng=Random(20260920)
with TemporaryDirectory() as temporary:
    base=Path(temporary)
    for case in range(128):
        root=base/str(case); root.mkdir()
        P={x for x in range(12) if rng.randrange(2)} or {0}
        targets={i:{x for x in range(12) if rng.randrange(3)==0} for i in range(17)}
        rows={i:(len(P&t),len(t),i) for i,t in targets.items()}
        U=set(sorted(targets,key=lambda i:order_target_counterfactual_key(rows[i],len(P)))[:5])
        O=set(targets)-U
        path,count=build_bounded_outsider_envelope((rows[i] for i in O),root,40)
        assert count<=len(P)+1
        for a in range(1,41):
            best=min((rows[i] for i in O),key=lambda row:order_target_counterfactual_key(row,a))
            assert lookup_exact_outsider_envelope(path,count,a)==best
            stats['envelope_checks']+=1
        for _ in range(50):
            S={x for x in range(16) if rng.randrange(3)==0} or {15}
            a=len(S); self_id=rng.randrange(20); k=rng.randrange(1,5)
            changed={i for i,t in targets.items() if len(S&t)!=rows[i][0]}
            survivors=U-changed-{self_id}
            winner=lookup_exact_outsider_envelope(path,count,a)
            W=sorted(survivors,key=lambda i:order_target_counterfactual_key(rows[i],a))
            new=len(W)>=k and order_target_counterfactual_key(rows[W[k-1]],a)<order_target_counterfactual_key(winner,a)
            old=len(W)>=k and all(order_target_counterfactual_key(rows[u],a)<order_target_counterfactual_key(rows[o],a)
                                    for u in U for o in O)
            assert not old or new
            if new:
                key=lambda i:(-Fraction(len(S&targets[i]),len(S|targets[i])),i)
                got=sorted((U|changed)-{self_id},key=key)[:k]
                want=sorted(set(targets)-{self_id},key=key)[:k]
                assert got==want
                stats['accepts']+=1; stats['strict_accepts']+=not old
            stats['queries']+=1
        stats['fixtures']+=1
    root=base/'separation'; root.mkdir()
    path,count=build_bounded_outsider_envelope(((2,5,i) for i in range(2,1026)),root,40)
    assert count==1
    outsider=lookup_exact_outsider_envelope(path,count,4)
    strong,weak=(2,2,0),(1,1,1)
    assert order_target_counterfactual_key(strong,4)<order_target_counterfactual_key(outsider,4)<order_target_counterfactual_key(weak,4)
    root=base/'zero-tie'; root.mkdir()
    path,count=build_bounded_outsider_envelope(iter([(0,1,9),(0,100,1)]),root,40)
    assert lookup_exact_outsider_envelope(path,count,1)==(0,100,1)
    root=base/'empty'; root.mkdir()
    path,count=build_bounded_outsider_envelope(iter([]),root,40)
    assert lookup_exact_outsider_envelope(path,count,1) is None
print(stats)
print('separation: 1024 outsiders, one hull record, two candidate scores; old gate rejects')
print('zero-overlap tie and empty outsider fixtures passed')
```

## Verification Status

The retained source executed with exit status 0: 128 fixtures, 5,120 exact envelope checks and 6,400 source queries. All 1,053 accepted queries matched exhaustive top-k; 58 were accepted where the old all-U gate rejected. Every old-gate acceptance also passed the new gate. The counters are fixture results, not a customer acceptance-rate estimate.

Observed sort occupancy was at most four records and merge fan-in at most two. The external hull performed 211 pops. The 1,024-outsider separating fixture produced one hull record; the zero-overlap ID tie and empty-outsider fixtures passed. These counters validate the exercised logical schedule, not Python RSS, filesystem allocation or production recovery. The zero-class tie fix was included before the first scientific run; a later naming-only Perl command initially failed because the host locale was unavailable, then succeeded with the C locale.
