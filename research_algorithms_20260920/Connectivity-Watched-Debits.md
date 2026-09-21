# Connectivity Through Watched Certificate Debits

Date: 2026-09-20. A02 follow-on research. A monotone snapshot solver and validation schedule, not a new general dynamic-connectivity bound, proven novelty, or physical 4 GB implementation.

## Premise Check

The [current connectivity manuscript](Witness-Connectivity.md) prices certificate validity separately from its static minimum-read frontier. Its fragment-relative rule gets stronger as known connectivity grows, but recomputing every debit after every useful edge can consume the reads that certificates were supposed to save. This note specifies a demand-driven validation mechanism and an adaptive execution contract. It does not preserve the static DP's optimality claim under adaptive discoveries.

The concrete transfer is from watched cardinality constraints to missing graph witnesses. Keep only enough currently cross-component failures to prove that a certificate is still unsafe. Subscribe those witnesses to component merges; scan more missing occurrences only when a watched obstruction disappears. Merge actual current edges, never assumed future edges.

Expert lenses: cut-certificate correctness; monotone constraint propagation; external-memory state and indexes; skeptical comparison with sensitivity oracles. The customer pain is repeated validation and raw fallback on changed snapshots, not merely an n-entry union-find allocation.

## Alternatives And Choice

| Approach | State/work property | Decision |
| --- | --- | --- |
| Rescan complete missing lists after each merge | Simple and exact; can reread the same prefix quadratically | Reference validation baseline, not strongest competitor |
| Subscribe every missing occurrence to endpoint unions | Each occurrence can be updated incrementally, but resident incidence can be O(M) | Competent eager counter baseline; external indexes must be charged |
| Watch k cross-component occurrences per certificate | At most sum(k) live watches; forward-only missing-list cursors | Selected schedule; familiar watched-constraint principle, with graph-trigger implementation below |
| Rebuild/maintain a current root certificate or sensitivity oracle | May avoid these validation and scanning costs entirely | Strong graph baseline, not declared inferior |

## Snapshot And Certificate Contract

Use the unchanged vertex universe, edge identities, deletion set D and live insertions A of A02. The initial known subgraph B0 consists only of actual surviving original forest edges and actual live insertions. Its active fragment quotient has q vertices; implicit untouched components remain on disk. Later B only gains actual surviving edges read from a raw extent or certificate. The underlying graph snapshot is frozen during this solve.

For each registered certificate i, H_i is a k_i-residual-forest certificate of extent E_i. Its complete seekable missing stream is D intersect H_i, with one record per distinct missing edge identity in that certificate. The same edge may occur in several certificates, and every occurrence is separately charged. Parallel identities may share endpoints. A repeated notification of one absent identity is not a second missing edge.

Map endpoints to the initial active fragments before this guard engine. A missing original edge can have both endpoints in the same fragment; that is an internal record, not an illegal graph self-loop. Missing edges in implicit untouched original components are also already internal and can be suppressed after a paid ownership check. It is safe to omit any record proved internal in B0, since this solve never splits B0. Complete change/membership accounting is still required to justify those omissions; a partially sampled missing list is invalid.

Let find_B denote current known connectivity. Define

```text
d_i(B) = count of e=(u,v) in D intersect H_i
         for which find_B(u) != find_B(v).

d_i(B) < k_i
  => CC(B union (H_i minus D)) = CC(B union (E_i minus D)).
```

**Cut proof.** A cut between components of B union (H_i-D) cannot split a B component. If a current E_i edge crosses it, either H_i originally contained every edge of that cut (when fewer than k_i), or H_i contained at least k_i distinct crossing edges. The first case contradicts a surviving crossing E_i edge. In the second, all those missing H_i witnesses cross B components, contradicting d_i(B)<k_i. The reverse inclusion follows from H_i being a subgraph. This is the existing strong-certificate argument applied after contracting B, not a new sparse-certificate theorem.

Actual unions only coarsen B, so d_i(B) never increases during a solve. A certificate valid now stays valid later. A new snapshot may split B or change D, which destroys this monotonicity: rebuild/remap the guards from its complete occurrence indexes. A saved forward cursor is not a deletion-capable dynamic data structure.

## Guard State And Invariant

For each certificate retain its capacity, missing-stream offset/length, one forward cursor, at most k_i watches, and a ready flag. A watch is a missing occurrence whose endpoints are currently in distinct B components. There are J certificate descriptors and K=sum_i k_i available slots.

Refill a guard by scanning from its saved cursor. Skip internal occurrences forever within this solve; watch cross-component occurrences. Stop on either k_i live watches or end of stream.

```text
k_i live watches                -> unsafe; unseen suffix need not be read
end of stream and < k_i watches  -> safe; release remaining watches
```

Every scanned cross-component occurrence is either still watched or has become internal. Previously skipped/internal occurrences cannot become cross-component. Thus k_i watches prove d_i>=k_i; exhaustion with fewer proves d_i<k_i exactly. Watch selection does not approximate the guard. The guard itself remains only a sufficient test of certificate correctness.

One file handle can address a concatenation of the missing streams using descriptor offsets. Do not allocate a page buffer or file descriptor per certificate. Constructing that seekable current index is a separate paid operation, not supplied for free by the forward-cursor theorem.

## Merge-Triggered Watch Index

Use a union-find over the q initial fragments and two intrusive endpoint handles per live watch. Each root owns a list of handles incident on its component. Each handle also stores its current owner root, with a pointer to its peer handle. Update that owner whenever the handle moves. Union by the number of initial fragments, not by watch degree:

1. Select the smaller root S and larger root L.
2. Pop endpoint handles from S one at a time. Do not copy its whole list into a temporary vector.
3. Read the peer's stored owner in O(1). If it is L, remove both handles and its watch slot; mark the owning certificate for refill.
4. Otherwise move the popped handle to L and replace its stored owner with L.
5. Commit the parent/size change; only then refill affected guards from their existing cursors.

A marked-certificate bit and bounded queue prevent duplicate refill entries. Guards becoming ready are queued once. Do not publish intermediate, half-merged guard state. The miniature below uses sets/dictionaries to make the logic readable; fixed slots and intrusive handles, or explicitly bounded external indexes, are the production representation obligation.

**Amortized bound.** Let sigma be total missing-stream records actually consumed in a solve; sigma<=M, the total registered occurrence count. Each consumed cross-component occurrence creates at most one watch. Each endpoint handle moves at most floor(log2 q) times because its component at least doubles whenever that endpoint is moved. A retiring watch is deleted once. Total handle operations are O(sigma log(q+1)+K+J). Creating a watch needs two root lookups; reading its current owner thereafter does not. For T attempted actual-edge unions, union-by-size lookup without compression gives total CPU O(q+K+J+(sigma+T)log(q+1)). This counts control work, not endpoint mapping, index construction or physical page service.

The first draft looked up original endpoints again at every handle move and therefore only established O(sigma log^2(q+1)) trigger work. Explicit owner fields remove that extra factor. The owner invariant holds because every handle at the disappearing root is moved or retired before the parent change; no live handle retains that old owner. New watches and ready-release operations occur only after the union commits. The retained probe checks owner fields against independently resolved original endpoints and checks exactly two maintenance root lookups per consumed occurrence or attempted union. Test-only invariant scans are excluded from that counter explicitly.

Resident logical state is O(q+K+J), not O(M). This depends on admitting all registered descriptors and watch slots. Otherwise register fewer options, or use a separately priced external watch index. A claim of low memory cannot hide one resident object per missing occurrence, every certificate ever constructed, or every original vertex.

## Adaptive Execution And Its Limit

```text
pin a coherent graph/certificate/delta snapshot
initialize known B0 and the admitted watched guards
while uncovered extents remain:
    execute an available ready certificate, or choose a paid raw/certificate probe
    union only actual surviving edges from the chosen input
    propagate those unions through the watch index
    mark an extent covered only after a valid complete replacement was processed
    account for any overlapping previously read input again
stream canonical labels and requested original-edge witnesses
```

An invalid certificate may still be read as an edge probe: its surviving edges are real. Invalid means it cannot yet stand in for its complete extent, not that its edges are unsafe to union. Reading all surviving certificate edges first is a strong alternative schedule, and can unlock certificates that initially fail. If every H_i edge is known missing from the complete index, the surviving payload is empty, not a hidden read.

**Adaptive correctness.** At the instant an extent is discharged, the cut lemma says its surviving raw edges add no connectivity beyond current B plus its processed surviving certificate. Future actual unions preserve that relation. Induction over discharged extents proves that covering every extent and including A yields the full current partition. It also explains why two currently invalid guards cannot mutually justify hypothetical unions. No final labels are inferred merely from all watches disappearing without processing the required live edges.

For a fixed registered portfolio and fixed initial known partition, exhausting all newly ready certificate payloads to quiescence has an order-independent final partition: the enabling predicates and edge-closure operator are monotone on the finite partition lattice. This is a standard least-fixed-point argument, not invention of monotone dataflow. Additional raw or invalid-certificate probes change the base partition and may change the ready-only fixed point itself, not just how quickly it is reached. Complete covering solves still return the full current partition by the discharge lemma; quiescence alone need not constitute a covering solve. No globally optimal adaptive scheduler or deadline guarantee is proved here.

The [completed independent review](Connectivity-Paths-Independent-Review.md#follow-on-watched-certificate-debits) supplies a seven-vertex witness with two initially invalid k=1 certificates. Ready-only processing stops at five fragments; paying for four real surviving certificate-edge records enables both replacements and yields the correct three components without raw extent reads. No hypothetical mutual justification is used. Its separate 31,250-state cut audit, 4,800 guard checks and 1,786 ready-action states found no counterexample to the final owner/cut mechanism. Those are reviewer-executed receipts, not a new lead rerun.

The review also proves a safe optional upper-bound shortcut: at a committed state with w watches, cursor p and exact stream length m, `w <= d_i(B) <= w+(m-p)`. If the upper bound is below k_i, the guard may release without reading the suffix. This can skip at most k_i-1 records on a guard's release and does not change the asymptotic result. The author probe below still executes the original EOF rule; the optional shortcut is executed in the independent review's new probe, not silently claimed to be implemented here.

## Two Separating Experiments

### Validation-Only Prefix

One k=1 missing list is (A,B1),...,(A,Bm); a further isolated fragment C remains untouched. Actual known edges merge A with B1, then B2, and so on. A stateless early-stopping validator reads 1,2,...,m,m records across initialization and those merges, or m(m+3)/2 total. The watched cursor consumes m records total and needs at most one live watch. Final B still has two components, so this is not a whole-graph-connected termination shortcut.

This separates from stateless revalidation only. A generic watched-cardinality engine supplied the same union events matches it, and an eager all-occurrence incidence index also avoids repeated scans at a different state cost. Do not present this as a new dynamic-connectivity lower bound.

### A Simple Graph With A Truly Split Answer

Take vertex groups A and B of w vertices each, plus C. B0 contains an intact star within A and within B. Partition all w^2 distinct A-B edges into r disjoint extents, and add one distinct B_i-C edge to each extent. Prepare a k=2 residual-forest certificate per extent, and delete every edge in these certificates. The original global forest uses one deleted A-B certificate edge and one deleted B-C bridge, so the changed snapshot starts with exactly three known fragments. Internal stars survive.

When w/r is sufficiently large, each extent still contains an A-B edge after deleting at most 4w+1 certificate edges. All B-C edges are missing. Therefore the true answer changes from one component to exactly {A union B, C}.

Initially each relative guard fails. Read one raw extent and union an actual surviving A-B edge. Every missing A-B witness becomes internal; each remaining debit is exactly one, from that extent's missing B-C edge. All k=2 certificates become valid, with empty surviving payloads, certifying the real split without reading the other raw extents.

Charge the full first raw extent, not an optimistically free selected bridge. At fixed r and w, candidate raw reads are about w^2/r instead of w^2 for the frozen initial relative-frontier policy; both also pay their missing-index validation and output. M is O(rw), so choosing r around sqrt(w) gives O(w^(3/2)) consumed edge/occurrence records versus O(w^2) raw records in that specifically frozen policy. Index creation, original F0 preparation and full source ingestion are not included in this per-snapshot comparison and remain paid.

This is a simple-graph construction, not a million duplicated parallel edges. It does not defeat a sensitivity oracle, a current-C-cut index, or every adaptive graph engine. They can recognize the split by other means. Its purpose is to prove that adaptive certificate enabling can matter beyond an unchanged-label cache and beyond the stateless-prefix example.

**Stronger matched-family boundary from independent review.** If partial extent reads are allowed, a real A-B witness appears within at most `|D intersect E_i|+1` raw records, O(w) here. That actual union can enable the same replacements without paying the entire first extent, giving O(w+rw) logical work rather than the displayed whole-extent schedule's quadratic term. A paid current-fragment cut-count index also resolves this promised three-group family with O(d) deletion-receipt work and a paid witness prefix. General edge-failure sensitivity methods are further comparators; see the review's inspected primary sources and restrictions. The eightfold raw-read number is therefore not a separation from the strongest adaptive access policy.

## Primary Prior Art And Remaining Claim

| Inspected primary source | What it already establishes | Consequence here |
| --- | --- | --- |
| [Moskewicz et al., Chaff, Section 2](https://www.princeton.edu/~chaff/publication/DAC2001v56.pdf) | Watch a small set of literals and reconsider a clause when a watched obstruction changes, instead of updating every occurrence. | Watching itself is not new. We use graph-component merger events as the truth changes. |
| [Gent et al., Watched Literals for Constraint Propagation in Minion, Sections 2-3 and Figure 1](https://heather.cafe/publications/papers/gent2006watched.pdf) | General dynamic triggers and watched Boolean sums; the paper explicitly credits earlier weighted-sum watching. | A generic cardinality engine can implement our per-certificate threshold test. Unlike its backtrack-stable setting, our released ready guards/cursors must be reset on a component split. |
| [Eppstein, Galil, Italiano, Improved Sparsification, Section 3 definitions and Section 4](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf) | Strong certificates compose under union; maintained sparse hierarchies and update strategies predate this work. | Adaptive replacement safety rests on established cut/certificate machinery; compare against current sparse maintenance, not only raw rescans. |

The St Andrews copy of the Minion PDF initially opened but subsequent section requests failed; the author-hosted mirror above supplied the inspected text. No source claim relies on an unseen method section. The graph-specific contribution candidate is the complete union-triggered watch schedule with O(q+K+J) state, monotone adaptive replacement contract, and paid missing-index/lifecycle model. Its ingredients are known and no inspected source comparison yet establishes a publishable algorithmic gap. The two separating experiments establish narrower statements only. A serious systems paper would need useful workload coverage and end-to-end equal-budget measurements; a new theory claim needs a stronger matched-comparator result.

## Resource And Lifecycle Accounting

The missing-occurrence index may be built by an external join of complete D with retained certificate membership, or maintained from complete transition receipts. It can duplicate an edge at many hierarchy levels. A 24-byte occurrence format costs 24M bytes; old/new generations, sort runs and endpoint-to-fragment mapping all count. If each query first scans/builds all M occurrences, forward cursors do not make that query's total work sublinear in M.

An illustrative admitted shape has q=200,000 active fragments, J=1M certificates, k=2 and M=100M missing occurrences. Provisional allowances of 128q + 64J + 128K + 600MB fixed services total 945.6 MB worker memory. They are packed engineering reservations, not measured Python memory or a complete allocation proof. The occurrence payload alone is 2.4 GB; two distinct such generations are 4.8 GB. Adding them to the old 34.35 GB A02 example is 39.15 GB before other selected blocks, headers and answers. Do not assume the old index allowance already included this index, or that every family receives its own 50 GB.

If the endpoint watch lists live on disk, their moved/deleted handles can cause random I/O; O(sigma log q) handle operations is not a sequential-bandwidth guarantee. Stream reads can also seek across certificate offsets. Fixed cache, WAL, byte offsets and wide arithmetic need explicit admission. Snapshot changes, interrupted unions and guard publication require consistent checkpointing or restarting this query; no crash-safe implementation is supplied. Original-node label output and canonical minima retain all costs documented in A02.

## Executable Research Probe

The engine consumes concatenated fixed-width files and keeps only current watches, descriptors and fragment state. The fixture/oracle uses resident lists to construct test sources and count true debits; that is not an implementation of the production missing-index builder. Random guard tests do not certify arbitrary random streams as graph certificates. The simple-graph experiment separately builds actual residual forests and checks full current WCC against an independent graph traversal.

```python
from collections import Counter
from pathlib import Path
from random import Random
from struct import Struct
from tempfile import TemporaryDirectory

RECORD = Struct('<QQQ')

class WatchedDebitContractEngine:
    def __init__(self, q, capacities, streams, path):
        self.parent, self.size = list(range(q)), [1]*q
        self.incident = [set() for _ in range(q)]
        self.slots = [set() for _ in capacities]
        self.ready, self.cursor = [False]*len(capacities), [0]*len(capacities)
        self.capacity, self.offset, self.length = capacities, [], []
        self.watches, self.serial, self.stats = {}, 0, Counter()
        with path.open('wb') as out:
            for stream in streams:
                self.offset.append(out.tell())
                count = 0
                for edge, u, v in stream:
                    assert 0 <= u < q and 0 <= v < q
                    out.write(RECORD.pack(edge, u, v)); count += 1
                self.length.append(count)
        self.input = path.open('rb')
        self.stats['prepared_occurrences'] = sum(self.length)
        for c, k in enumerate(capacities):
            assert k >= 1
            self.fill_pending_certificate_slots(c)

    def find_current_fragment_root(self, u):
        self.stats['find_calls'] += 1
        while self.parent[u] != u:
            u = self.parent[u]
        return u

    def remove_existing_watch_handles(self, wid):
        c, u, v, a, b = self.watches.pop(wid)
        self.incident[a].remove(wid)
        self.incident[b].remove(wid)
        self.slots[c].remove(wid)

    def fill_pending_certificate_slots(self, c):
        while len(self.slots[c]) < self.capacity[c] and self.cursor[c] < self.length[c]:
            self.input.seek(self.offset[c]+RECORD.size*self.cursor[c])
            edge, u, v = RECORD.unpack(self.input.read(RECORD.size))
            self.cursor[c] += 1; self.stats['consumed'] += 1
            a, b = self.find_current_fragment_root(u), self.find_current_fragment_root(v)
            if a == b:
                self.stats['skipped_internal'] += 1
                continue
            wid = self.serial; self.serial += 1
            self.watches[wid] = [c, u, v, a, b]
            self.slots[c].add(wid)
            self.incident[a].add(wid); self.incident[b].add(wid)
            self.stats['created'] += 1
            self.stats['peak_watches'] = max(self.stats['peak_watches'], len(self.watches))
        if len(self.slots[c]) < self.capacity[c]:
            assert self.cursor[c] == self.length[c]
            self.ready[c] = True; self.stats['ready_events'] += 1
            while self.slots[c]:
                self.remove_existing_watch_handles(next(iter(self.slots[c])))

    def merge_known_fragment_components(self, u, v):
        self.stats['merge_attempts'] += 1
        a, b = self.find_current_fragment_root(u), self.find_current_fragment_root(v)
        if a == b:
            return False
        if self.size[a] > self.size[b]:
            a, b = b, a
        affected, marked = [], set()
        while self.incident[a]:
            wid = self.incident[a].pop()
            c, x, y, rx, ry = self.watches[wid]
            other = ry if rx == a else rx
            assert rx == a or ry == a
            if other == b:
                self.incident[b].remove(wid)
                self.slots[c].remove(wid); del self.watches[wid]
                self.stats['retired'] += 1
                if c not in marked:
                    marked.add(c); affected.append(c)
            else:
                self.incident[b].add(wid)
                self.watches[wid][3 if rx == a else 4] = b
                self.stats['moved_handles'] += 1
        self.parent[a] = b; self.size[b] += self.size[a]
        self.stats['unions'] += 1
        for c in affected:
            self.fill_pending_certificate_slots(c)
        return True

    def verify_current_guard_invariants(self, streams):
        before = self.stats['find_calls']
        find = self.find_current_fragment_root
        for c, stream in enumerate(streams):
            debit = sum(find(u) != find(v) for _, u, v in stream)
            assert self.ready[c] == (debit < self.capacity[c])
            assert len(self.slots[c]) <= self.capacity[c]
            if not self.ready[c]:
                assert len(self.slots[c]) == self.capacity[c]
        for wid, (c, u, v, owned_a, owned_b) in self.watches.items():
            a, b = find(u), find(v)
            assert (a, b) == (owned_a, owned_b)
            assert a != b and wid in self.incident[a] and wid in self.incident[b]
            assert wid in self.slots[c]
        assert sum(map(len, self.incident)) == 2*len(self.watches)
        assert self.stats['consumed'] <= self.stats['prepared_occurrences']
        assert len(self.watches) <= sum(self.capacity)
        self.stats['guard_checks'] += len(streams)
        self.stats['verification_find_calls'] += self.stats['find_calls']-before
        self.stats['find_calls'] = before
        assert before == 2*(self.stats['consumed']+self.stats['merge_attempts'])

    def close_current_input_handle(self):
        self.input.close()

def compute_oracle_component_labels(n, edges):
    rows = [[] for _ in range(n)]
    for _, u, v in edges:
        rows[u].append(v); rows[v].append(u)
    labels = [-1]*n
    for root in range(n):
        if labels[root] >= 0:
            continue
        labels[root] = root; stack = [root]
        while stack:
            u = stack.pop()
            for v in rows[u]:
                if labels[v] < 0:
                    labels[v] = root; stack.append(v)
    return labels

def create_residual_forest_certificate(n, edges, k):
    remaining, selected = list(edges), []
    for _ in range(k):
        parent = list(range(n))
        def find_current_oracle_root(u):
            while parent[u] != u:
                u = parent[u]
            return u
        rest = []
        for edge, u, v in remaining:
            a, b = find_current_oracle_root(u), find_current_oracle_root(v)
            if a == b:
                rest.append((edge, u, v))
            else:
                parent[a] = b; selected.append((edge, u, v))
        remaining = rest
    return selected

def verify_random_watched_guards(root):
    rng, totals = Random(841), Counter()
    for case in range(180):
        q = rng.randrange(3, 19)
        universe = [(u*q+v, u, v) for u in range(q) for v in range(u+1, q)]
        streams = [rng.sample(universe, rng.randrange(len(universe)+1)) for _ in range(12)]
        capacities = [rng.randrange(1, 6) for _ in streams]
        engine = WatchedDebitContractEngine(q, capacities, streams, root/'random.bin')
        engine.verify_current_guard_invariants(streams)
        pairs = [(u, v) for _, u, v in universe]; rng.shuffle(pairs)
        for u, v in pairs[:2*q]:
            engine.merge_known_fragment_components(u, v)
            engine.verify_current_guard_invariants(streams)
        assert engine.stats['moved_handles'] <= 2*engine.stats['created']*q.bit_length()
        totals.update({key: value for key, value in engine.stats.items() if key != 'peak_watches'})
        totals['max_fixture_watches'] = max(totals['max_fixture_watches'], engine.stats['peak_watches'])
        engine.close_current_input_handle()
    print('random_guards', dict(totals), 'fixtures', 180)

def verify_prefix_validation_separation(root):
    rows = []
    for m in (8, 32, 128, 512):
        stream = [(j, 0, j) for j in range(1, m+1)]
        engine = WatchedDebitContractEngine(m+2, [1], [stream], root/'prefix.bin')
        scans = 0
        for stage in range(m+1):
            if stage:
                engine.merge_known_fragment_components(0, stage)
            engine.verify_current_guard_invariants([stream])
            before = engine.stats['find_calls']
            for _, u, v in stream:
                scans += 1
                if engine.find_current_fragment_root(u) != engine.find_current_fragment_root(v):
                    break
            engine.stats['reference_find_calls'] += engine.stats['find_calls']-before
            engine.stats['find_calls'] = before
        assert scans == m*(m+3)//2 and engine.stats['consumed'] == m
        assert engine.find_current_fragment_root(0) != engine.find_current_fragment_root(m+1)
        rows.append((m, scans, engine.stats['consumed'], engine.stats['peak_watches']))
        engine.close_current_input_handle()
    print('prefix_m_stateless_watched_peak', rows)

def verify_changed_partition_family(root):
    counts = []
    for w, r in ((16, 2), (64, 4), (256, 8)):
        n, serial = 2*w+1, 0
        extents = [[] for _ in range(r)]
        for a in range(w):
            for b in range(w):
                extents[((b-a) % w) % r].append((serial, a, w+b)); serial += 1
        for i in range(r):
            extents[i].append((serial, w+i, 2*w)); serial += 1
        known = []
        for base in (0, w):
            for j in range(1, w):
                known.append((serial, base, base+j)); serial += 1
        certs = [create_residual_forest_certificate(n, edges, 2) for edges in extents]
        deleted = {e for cert in certs for e, _, _ in cert}
        crossing = next(row for row in certs[0] if row[2] < 2*w)
        bridge = extents[0][-1]
        forest = known + [crossing, bridge]
        assert len(forest) == n-1 and len(set(compute_oracle_component_labels(n, forest))) == 1
        assert [row for row in forest if row[0] not in deleted] == known
        assert len(set(compute_oracle_component_labels(n, known))) == 3
        owner = lambda u: 0 if u < w else 1 if u < 2*w else 2
        streams = [[(e, owner(u), owner(v)) for e, u, v in cert] for cert in certs]
        assert all(u != v for stream in streams for _, u, v in stream)
        engine = WatchedDebitContractEngine(3, [2]*r, streams, root/'family.bin')
        engine.verify_current_guard_invariants(streams)
        assert not any(engine.ready)
        actual = known + [row for extent in extents for row in extent if row[0] not in deleted]
        original = known + [row for extent in extents for row in extent]
        assert len(set(compute_oracle_component_labels(n, original))) == 1
        expected = compute_oracle_component_labels(n, actual)
        assert len(set(expected)) == 2
        for e, u, v in extents[0]:
            if e not in deleted:
                engine.merge_known_fragment_components(owner(u), owner(v))
        engine.verify_current_guard_invariants(streams)
        assert all(engine.ready)
        assert all(all(e in deleted for e, _, _ in cert) for cert in certs)
        candidate = known + [row for row in extents[0] if row[0] not in deleted]
        assert compute_oracle_component_labels(n, candidate) == expected
        counts.append(dict(w=w, r=r, vertices=n, raw_static=sum(map(len, extents)),
            raw_adaptive=len(extents[0]), missing_occurrences=sum(map(len, streams)),
            watched_consumed=engine.stats['consumed'], peak_watches=engine.stats['peak_watches'],
            final_components=2))
        engine.close_current_input_handle()
    print('changed_partition_family', counts)

def verify_reset_circularity_counterexamples(root):
    internal = [[(10, 0, 0), (11, 1, 1), (12, 0, 1)]]
    inner = WatchedDebitContractEngine(3, [1], internal, root/'internal.bin')
    inner.verify_current_guard_invariants(internal)
    assert inner.stats['skipped_internal'] == 2 and not inner.ready[0]
    inner.merge_known_fragment_components(0, 1)
    inner.verify_current_guard_invariants(internal)
    assert inner.ready[0]
    inner.close_current_input_handle()
    empty = WatchedDebitContractEngine(0, [1], [[]], root/'empty.bin')
    empty.verify_current_guard_invariants([[]])
    assert empty.ready == [True]
    empty.close_current_input_handle()
    stream = [[(1, 0, 1)]]
    engine = WatchedDebitContractEngine(3, [1], stream, root/'reset.bin')
    assert not engine.ready[0]
    engine.merge_known_fragment_components(0, 2)
    engine.merge_known_fragment_components(2, 1)
    assert engine.ready[0]
    reset = WatchedDebitContractEngine(3, [1], stream, root/'fresh.bin')
    reset.merge_known_fragment_components(0, 2)
    assert not reset.ready[0]
    assert engine.ready != reset.ready
    # A missing certificate edge cannot stand in for its unread parallel survivor.
    assert compute_oracle_component_labels(2, []) != compute_oracle_component_labels(2, [(2, 0, 1)])
    engine.close_current_input_handle(); reset.close_current_input_handle()
    print('counterexamples snapshot_split_reset=required hypothetical_union=invalid initial_internal=passed q_zero=passed')

with TemporaryDirectory() as directory:
    root = Path(directory)
    verify_random_watched_guards(root)
    verify_prefix_validation_separation(root)
    verify_changed_partition_family(root)
    verify_reset_circularity_counterexamples(root)
```

## Evidence Status

Executed the retained source with Python 3 on 2026-09-20. Development corrections replaced aggregated peak counters with the maximum fixture peak and explicitly constructed F0 to verify its two cuts. The direct-owner revision then added maintenance lookup accounting; its first instrumentation run failed because the stateless comparator's oracle lookups polluted that counter, not because a guard was wrong. Comparator/verifier lookups now have separate counters. The final boundary revision admits same-initial-fragment records and q=0 after paid suppression of implicit internal occurrences. The latest exit-0 receipt is authoritative:

```text
random_guards: fixtures=180, prepared_occurrences=64515, consumed=61558,
created=11347, ready_events=2085, guard_checks=47352, retired=9932,
moved_handles=7137, unions=1728, skipped_internal=50211,
max_fixture_watches=44, merge_attempts=3766, maintenance_find_calls=130648,
verification_find_calls=3911772

prefix (m, stateless records, watched records, peak watches):
(8,44,8,1), (32,560,32,1), (128,8384,128,1), (512,131840,512,1)

simple-graph family (w,r,n,static raw,adaptive raw,missing reads,peak watches):
(16,2,33,258,129,114,4)
(64,4,129,4100,1025,964,8)
(256,8,513,65544,8193,7944,16)
all three original graphs have one component; all current answers have two
snapshot_split_reset=required; hypothetical_union=invalid;
initial_internal=passed; q_zero=passed
```

The largest family's raw-read count falls eightfold, but the candidate also consumes 7,944 missing occurrences: 16,137 raw-plus-occurrence records, not merely 8,193. An early-stopping initial validator needs only two occurrences per extent to reject, so that frozen policy's comparable count is 65,560, not an artificially inflated repeated full validation. These are logical access counts. Index build/maintenance, source reads, D filtering, metadata, page service and full output remain outside this pair of counters. The raw-family driver/oracle uses resident edge lists and deletion sets; only the watched missing-stream engine is file-backed in this probe. It is not a complete bounded raw-solver implementation.

No physical-memory, crash, publication or external watch-index benchmark is claimed. The independent review has been requested separately; the new theorem and schedule are not promoted to independently validated status merely because this probe passes.

## Next Scientific Test

Compare the same certificate portfolio, complete missing-index preparation, graph snapshot and output across static frontier, eager all-occurrence updates, watched adaptive execution, certificate-first probing and a tuned forest-sensitivity engine. Seek a cost frontier in which eliminating M-sized mutable incidence matters without spending more random I/O or raw probes than the memory it saves. Failure to beat that frontier would leave a useful implementation pattern, not the requested new graph-algorithm contribution.
