# A05 Bounded Seed Selection: Independent Review

Date: 2026-09-20. Scope: theorem/prior-art sidecar, not implementation approval.

Read: [runwise theory through prior art and its counterexample table](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Similarity-Runwise-Selection.md) (lines 1-180 at review time) and [novelty evidence policy](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Novelty-Baseline-Evidence-Policy.md). No old probe/suite execution or lead-prototype imports. This review writes only this file.

## Verdict

**The proposed pruning and ordered enumeration are correct under the stated fixed-snapshot, disjoint-domain, exact-total-order contract.** There is no counterexample to that theorem here. For k'>0, the live heap-entry bound is indeed

~~~text
s = min(k', R')
peak_heap_entries <= min(N, s + k' - 1) <= 2*k' - 1,
~~~

where N is the exact eligible nonself population. The bound requires pop-before-split and no children after the final output. It is **not** an exact bound on all simultaneously live records or allocated bytes. Incoming heads, popped winners, heap-conversion temporaries, allocator capacity, and output buffers need their own accounting.

The improvement over retaining every seed is genuine: selector state loses its R' term. The generic selection ingredients are established. The exact composed disk-Jaccard contribution remains an unresolved research claim, not a seventh-family completion certificate.

## Contract And Expert Checks

The four review lenses are total-order correctness, transient/storage accounting, failure completeness, and historical attribution.

Let U contain N distinct eligible nonself targets from one immutable snapshot. Initial nonempty intervals I_1,...,I_R' partition U. The source is a normalized binary set of size a; each interval has its exact constant intersection count c. Every target's cardinality b, original ID, membership, rank and both RMQ fields agree with that snapshot. No late self filtering, tombstone filtering, boundary-tie expansion, or changing k is admitted.

Define x preceding y by

~~~text
x precedes y iff
    J(x) > J(y), or
    J(x) = J(y) and original_ID(x) < original_ID(y).

J(x) = c_x / (a + b_x - c_x) for c_x > 0;
J(x) = 0 for c_x = 0, including empty-empty.
~~~

Unique IDs make this a strict total order. Equal rational scores must be recognized by exact cross-products, not floating approximations or lexicographic numerator/denominator pairs. Across different runs, c and b alone do not order scores. Under the supplied a,b,n<2^32 model, checked 128-bit arithmetic covers the stated 65-bit cross-products; broader integers require explicit bit-cost/width accounting. Reverse the entire comparator for the worst-first heap, including the ID tie direction.

Within a positive constant-c interval, score strictly decreases with b; equal b is settled by ID. Its positive (b,ID) argmin is therefore its exact head. Within a zero interval, the ID argmin is its head. This applies to every later subinterval as well. Removal of self before the initial RMQs preserves disjointness and supplies the required nonself heads.

## Proof

### 1. Bounded Seeding

Maintain H as the best min(k',t) heads after t initial intervals have been scanned. When H is not full, insert the new head. Otherwise compare it with H's worst head: discard the newcomer unless it precedes that head; if it does, replace the worst head and restore the worst-first heap.

Induction on t proves this invariant. Use conditional replacement, not an unconditional replacement that could evict a better head. Every initial interval is examined, including those whose heads are ultimately discarded. No array of all heads or persistent list of rejected heads is needed.

### 2. Permanent Root Pruning

At completion of seeding, let h be any discarded head. A discard is possible only if R'>k', so H contains k' heads, each strictly preceding h. These are k' **distinct actual targets** because the initial intervals are disjoint and their IDs unique.

For every x in h's interval, h precedes x or h=x by the head lemma. Transitivity therefore gives k' distinct targets preceding x. Thus x has global rank greater than k', and no member of that interval belongs to the requested answer.

This proof does not assert that all retained heads themselves belong to the global top-k'. For example, ascending-key lists [1,2], [3], [4], with k'=2 retain roots 1 and 3, but the answer is 1,2. The witnesses establish a rank bound; they need not be the final answer set. Their later removal from the heap cannot invalidate that bound.

### 3. Enumeration And Full Length

Let V be the union of the retained seed intervals. The preceding lemma shows top-k'(U) is contained in V. Moreover, |V|>=k': if R'<k', all intervals survive and V=U; otherwise k' disjoint nonempty intervals survive.

Heapify the same H storage using the best-first comparator. Its active intervals partition the unreported portion of V, and every head is that interval's best remaining target. Pop the globally best head, then replace its interval with its two nonempty half-open remainders, excluding the popped position. Each remainder inherits c and is answered by the same appropriate RMQ.

Induction yields exactly the first k' targets of U, ordered and without duplicates. The heap cannot legitimately exhaust before k' outputs. The retained intervals do **not** partition all of U after pruning; a proof that keeps claiming that old invariant is incorrect. The replacement invariant is a partition of the unreported part of V.

### 4. Heap Peak, Conversion And Work

After t completed nonfinal pops and their splits,

~~~text
|H_t| = s - t + sum_(i=1..t) d_i,  where 0 <= d_i <= 2;
|H_t| <= s + t;
|H_t| <= |V| - t <= N - t.
~~~

For t<=k'-1 this implies the advertised peak. Within a transition, popping first reduces occupancy; sequential insertion of at most two children never exceeds the larger of the before/after bounds. The final pop creates no children.

The bound is attainable. Use ascending keys, initial intervals [10,1,11], [12,2,13], [3], [4], and k'=3. Retain roots 1,2,3 and prune 4. Popping 1 and 2 each exposes two children, giving peak 5=2k'-1. This is a generic forest example, independent of a Jaccard realization.

Resource distinctions:

| Phase/resource | Supported bound and condition |
|---|---|
| Seeding heap length | At most k'; an incoming head and a swap/replacement temporary add O(1) records. Push-then-pop instead permits k'+1 heap entries transiently. |
| Heap conversion | Same s slots; bottom-up iterative sift-down under the new comparator uses O(1) working records and O(s) time. Merely changing a comparator without rebuilding is incorrect. A second heap, mapped copy or sorted copy does not meet the in-place claim. |
| Enumeration heap length | At most min(N,s+k'-1), including intermediate inserts when the parent was removed first. A popped parent/output record and at most two child temporaries remain separately chargeable. |
| Allocated storage | Slot count is not capacity. Geometric growth, reallocating old/new backing stores, object wrappers and delayed reclamation require measurement or an admitted reservation. A single pre-reserved min(N,2k'-1)-slot array is sufficient, using checked size arithmetic. |
| Other state | Fixed admitted sort/I/O/cache buffers and disk run spool remain; result storage is O(k') if retained, or a bounded buffer plus paid staged output. No per-discard witness log is necessary. |

In particular, k'=1 can have one resident head plus one incoming head: two live records although 2k'-1=1. This refutes an all-record reading of the number, not the heap-length theorem or O(k') state claim.

For a successful, single-attempt execution with cached head keys:

~~~text
initial logical RMQs = R'
child logical RMQs <= 2*(k'-1)
heap CPU = O((R' + k') * log(k'+1))
conversion CPU = O(s)
retained selector records = O(k'), independent of R'
~~~

The head stream is still an R'-item scan. The selected disk segment tree still charges O(log n) node/metadata probes per RMQ, hence O((R'+k')log n) such probes in the worst case. No claim is made that all physical reads are only R'+2(k'-1), or that arbitrary retries fit that logical-call count.

All source-feature interval reads, endpoint generation/sorting, overlap sweep, run spool writes/reads, head metadata fetches, shared-index preparation and output/publication remain paid. Fragmentation R'=Theta(N) still creates linear seeding work even for tiny k'. The removed resource rejection is specifically the R'-sized resident head set, not arbitrary event/scratch/cache/output rejection. k'=N can still require O(N) selector/output state.

For k'=0, bypass seeding, heap conversion, child creation and endpoint work needed solely for selection; publish the admitted empty result according to the existing contract. Do not evaluate 2k'-1 as an unsigned capacity. Necessary request/snapshot validation and publication costs are not claimed to vanish.

## Actual Failure Examples And Completion Obligations

These are failures of weakened implementations/contracts, not counterexamples to the theorem above.

| Shortcut | Counterexample or consequence |
|---|---|
| Ignore ID when deciding whether to replace a seed | a=3; stream heads (ID=9,c=1,b=1), then (ID=1,c=2,b=5), k'=1. Both scores are exactly 1/3. A score-only strict replacement keeps ID 9; the required answer is ID 1. |
| Let an RMQ use positional tie order | One c=1 interval has IDs [9,1], both b=1; another c=2 interval has ID 5,b=5, with a=3. All scores are 1/3. Returning 9 instead of 1 for the first head lets root 5 prune the true winner's interval. |
| Count duplicate roots as separate witnesses | Initial domains {ID 1}, {ID 1}, {ID 2}, all scores zero, k'=2. Two copies of ID 1 cannot certify that ID 2 is outside the distinct top-2. |
| Emit retained heads without splitting | Lists [1,2],[3],[4], k'=2 return 1,3 instead of 1,2. |
| Stop scanning seeds once the heap is full | k'=1; an unread later interval can have the global winner. An early prefix is not a complete result. |
| Split even after final output | k'=1 with an interior winner inserts two unnecessary children, violating both the child-RMQ bound of zero and the heap bound of one. |
| Treat failed nonempty RMQ or early heap exhaustion as normal EOF | The length proof requires k' results. An empty/sentinel head from a nonempty range, truncated run file or missing metadata must fail the attempt, not silently reduce k'. |

**Success must mean all mandatory seed reads completed and exactly k' ordered records were published.** Seeding RMQ/read failure, snapshot mismatch, capacity/arithmetic failure, conversion failure, or child RMQ failure must not publish a successful partial result. Neither a rejected head nor an unread run may be treated as safely pruned because its read failed.

A sink error can occur after some winners were produced. Stage output, and publish only after every required output write/close and complete-count check succeed. Preserve the preceding published result on an aborted replacement; release bounded scratch on failure. Direct irrevocable streaming needs an explicit incomplete-result protocol and is not equivalent to an atomic complete-file result. Crash durability beyond the existing contract is not inferred.

A count check alone does not detect silently missing input runs: validated run coverage/count and coherent input framing remain prerequisites. Conversely, no obligation is introduced to read descendants of correctly pruned intervals, or to compute children after the final result. Those reads are legitimately absent.

These are review obligations for the lead's finite-file integration, **not tested file-I/O/failure-handling claims in this sidecar**.

## Primary Sources And Passage Scope

Inspected on 2026-09-20. Technical claims below rely on primary text/source code, not search snippets. Version-specific passage numbering is intentional.

**S1. [Sedgewick and Wayne, TopM.java, Algorithms 4th Edition source](https://algs4.cs.princeton.edu/24pq/TopM.java.html).** Class description and main's input loop: bounded priority-queue selection of the largest m stream items; this implementation inserts then deletes and explicitly permits m+1 entries. This directly establishes the streaming bounded-top-k primitive after reversing the order. Its stack-based output is not the candidate's in-place conversion or interval expansion. No claim of earliest historical priority is needed.

**S2. [Akram and Saxena, arXiv:2104.02461v4](https://arxiv.org/pdf/2104.02461v4).** Section 2, Algorithm 1 and Remarks 1-2, printed pp. 2-4: RMQ extraction, left/right subintervals, a binary heap, sorted reporting, and at most one net frontier entry per expansion. This directly covers enumeration of a retained interval's implicit tree. It does not describe the proposed multi-root bounded seeding, disk cost model, or Jaccard reduction. Its Algorithm 1 also computes children on its final iteration; omitting that unnecessary work is necessary for this review's exact call bound.

**S3. [Kaplan, Kozma, Zamir and Zwick, arXiv:1802.07041v1](https://arxiv.org/pdf/1802.07041v1), with [SOSA 2019 publication record](https://drops.dagstuhl.de/entities/document/10.4230/OASIcs.SOSA.2019.5).** In the arXiv version, Section 3.1/Figure 1 explicitly gives ordinary best-first heap enumeration in sorted order; Theorem 3.3 gives O(k) selection without sorted output. Section 3.3/Theorem 3.6 treats general heap-ordered trees. Section 4.1/Theorem 4.1 reduces sorted lists to a tree and gives O(m+k) selection. These are strong general heap/forest precedents. The inspected procedures do not establish this exact one-pass k-root filter plus in-place conversion. Their RAM access model is not disk RMQ I/O; their faster unordered selection does not automatically provide sorted results.

**S4. [Guava v33.4.8, TopKSelector.java](https://raw.githubusercontent.com/google/guava/v33.4.8/guava/src/com/google/common/collect/TopKSelector.java).** Class contract, constructor, offer, trim and topK: comparator-defined stream top-k using a 2k buffer and threshold rejection, with expected O(n+k log k) time, worst-case O(n log k), and O(k) memory. The source expressly uses the existence of k better elements for rejection. This is a published implementation comparator for seed selection, not a forest/Jaccard theorem. Its ties require our full comparator; its returned copy and sorting scratch must be charged. Its expected bound is not a worst-case linear-seeding guarantee.

**S5. [Python heapq documentation, heapify and heapreplace](https://docs.python.org/3/library/heapq.html#heapq.heapify).** The documented operations supply in-place linear-time heapification and fixed-length replacement. This supports ordinary realizability of the conversion/replacement primitives, not any particular runtime's exact allocation peak or this sidecar's disk theorem.

The Frederickson 1993 original [DOI](https://doi.org/10.1006/inco.1993.1030) and Purdue report could not be opened through the browsing tool. No detailed original passage is attributed to it here. S3 supplies directly inspected primary algorithm text without relying on an inaccessible historical citation.

The bounded search also checked Jaccard/RMQ and run-length/top-k combinations. No inspected source established the full exact composed disk-Jaccard contract. This is a search limitation, not positive novelty evidence or a claim that no such work exists.

## Reduction And Novelty Ledger

An initial interval defines an implicit binary Cartesian tree: its head is the root and recursively the heads of the two remainders are its children. The head lemma makes every such tree heap-ordered under the global score/ID comparator. Thus the selector is a heap-ordered forest problem, not a new kind of top-k order.

Three levels of contribution must remain separate:

1. **Established primitives:** streaming bounded selection, heap order, RMQ splitting, and best-first enumeration. S1-S5 rule out presenting O(k') top-k state or those individual operations as new.
2. **Generic deduction proved here:** apply bounded stream selection to the roots; use k' distinct root witnesses to prune entire dominated trees; heapify and enumerate the surviving forest. This constructive reduction substantially narrows the algorithmic claim. The review does not mislabel its own adaptation as a historically published identical procedure.
3. **A05-specific composition:** exact full-domain overlap runs; the positive (b,ID)/zero(ID) query-independent sufficient indexes for binary Jaccard; frozen-rank disk construction; and fully paid admission, ties, self exclusion and complete output. The new selector removes an R'-dependent resident component from that composition. Whether the remaining combination is scientifically nontrivial and unpublished is unresolved.

A generic virtual super-root above all R' interval trees also makes the reduction to S3 concrete: its degree is R', while other nodes have degree at most two. The resulting selection work is O(R'+k') in that paper's unordered RAM model; sorting adds O(k' log k'). This does **not** supply the candidate's O(k') resident implementation when R' is large: the direct construction handles R' roots, and materialization/child-oracle costs remain chargeable. [S3, general-tree theorem](https://arxiv.org/pdf/1802.07041v1).

An identical reimplementation using the candidate's pruning step is a correctness/engineering control, not independent historical proof. Conversely, importing a generic selector from an existing library does not by itself make the complete disk-Jaccard composition old. Novelty is not defined as inability to copy.

## Independent Model Receipt

The appendix was executed as a fresh JavaScript model in the tool runtime, with no filesystem/network access and no production imports. It uses direct interval scans as the head oracle, not the lead's RMQ implementation, and checks output against an independent whole-population exact sort.

- 199,051 cases: every permutation and interval partition for n=1..6, all k=0..n+1.
- 72,203 additional cases: every self position for the same enumeration restricted to n<=5.
- 50,000 cases: 10,000 deterministic small binary-set fixtures, five k values each, varying physical ID order and self presence. Local heads use positive (b,ID) or zero ID; the whole-population oracle uses exact rational comparisons.
- Total: **321,254 passing cases**, plus the explicit tight-peak witness (4 initial RMQs, 4 child RMQs, peak 5, output [1,2,3]).
- Separate small evaluations reproduced score-only tie failure (returned 9, required 1), duplicate witness IDs [1,1], and k'=1's two-live-record transient.

These validate the abstract model, not asymptotic complexity empirically, disk allocation peaks, endpoint sorting, complete-file publication, or injected file failures. The model materializes small fixture intervals and accesses a population array; it is not itself an O(k')-total-memory implementation. Deterministic generated fixtures are not a representative performance sample. The proof, not the case count, establishes general correctness.

## Meaningful Next Comparison

Use the lead's planned same-source finite-file integration; do not start another overlapping prototype.

**First comparison:** retain-all-root runwise selection versus bounded seeding, on the same prepared files, source, exact ordering, k, resource budgets and complete-output obligations. Vary R'/k' from small to heavily fragmented and vary whether winners concentrate in one interval or many. Record output identity, peak heap length **and allocated bytes through conversion**, initial/child RMQs, metadata probes, endpoint/run traffic, total completion time and admitted/rejected outcome. This isolates the claimed removal of resident R' state, without claiming historical novelty.

**Closest generic control:** plug a source-attributed bounded stream selector (S1, or S4's buffered alternative) into the same implicit-forest interface and known RMQ enumerator. Mark the root-pruning reduction and Jaccard adapter as supplied by this candidate/review, not as quoted from those sources. For S4, compare seeding comparisons/time and transient bytes under equal budgets; do not expect fewer than R' initial head reads merely from its cheaper comparison schedule. This can reveal whether the binary-heap seed scan is the practical bottleneck without requiring a new algorithm scope.

Failure checks in that integration should cover an initial-head read failure after the seed heap fills, a child-RMQ failure after one staged winner, a truncated run stream, and a sink failure before final publication. Success criteria are no successful partial file, unchanged previous publication, bounded cleanup, and exact full output on the nonfailing counterparts.

**Final conclusion:** the theorem is proved with explicit qualifications; actual counterexamples defeat weaker tie, witness, transient and completeness claims. The useful next result is a measured same-source resource/admission separation and a precisely attributed generic control. This review neither establishes a new generic selection algorithm nor closes the seven-family genuine-innovation goal.

## Appendix: Executed Independent Model

The following is a JavaScript function body (execute with new Function(body)()). Fixtures and oracle intentionally retain small populations; only the selector's logical state is being checked.

~~~javascript
const check = (condition, message) => { if (!condition) throw new Error(message); };
function compare_total_key_order(x, y) {
  const delta = y.num * x.den - x.num * y.den;
  return delta < 0n ? -1 : delta > 0n ? 1 : x.id - y.id;
}
function sift_binary_heap_down(heap, index, before) {
  for (;;) {
    let child = 2 * index + 1;
    if (child >= heap.length) return;
    if (child + 1 < heap.length && before(heap[child + 1], heap[child])) child++;
    if (!before(heap[child], heap[index])) return;
    [heap[index], heap[child]] = [heap[child], heap[index]];
    index = child;
  }
}
function push_binary_heap_entry(heap, entry, before) {
  let index = heap.length;
  heap.push(entry);
  while (index > 0) {
    const parent = (index - 1) >> 1;
    if (!before(heap[index], heap[parent])) break;
    [heap[index], heap[parent]] = [heap[parent], heap[index]];
    index = parent;
  }
}
function pop_binary_heap_entry(heap, before) {
  const result = heap[0], last = heap.pop();
  if (heap.length) { heap[0] = last; sift_binary_heap_down(heap, 0, before); }
  return result;
}
function verify_bounded_model_case(items, intervals, requested, excluded = -1, localOrder) {
  const population = items.length - Number(excluded >= 0);
  const limit = Math.min(requested, population);
  const parts = intervals.flatMap(([left, right]) =>
    excluded >= left && excluded < right
      ? [[left, excluded], [excluded + 1, right]].filter(([l, r]) => l < r)
      : [[left, right]]);
  if (!limit) return {seed: 0, child: 0, peak: 0, outputs: []};
  let seed = 0, child = 0, peak = 0;
  const heap = [], outputs = [];
  const best = (x, y) => compare_total_key_order(items[x.position], items[y.position]) < 0;
  const worst = (x, y) => best(y, x);
  const head = (left, right, initial) => {
    initial ? seed++ : child++;
    let position = left;
    for (let index = left + 1; index < right; index++)
      if ((localOrder || compare_total_key_order)(items[index], items[position]) < 0) position = index;
    return {left, right, position};
  };
  for (const [left, right] of parts) {
    const entry = head(left, right, true);
    if (heap.length < limit) push_binary_heap_entry(heap, entry, worst);
    else if (best(entry, heap[0])) { heap[0] = entry; sift_binary_heap_down(heap, 0, worst); }
    check(heap.length <= limit, "seed bound");
  }
  const survivors = heap.length, sameArray = heap;
  for (let index = (heap.length >> 1) - 1; index >= 0; index--) sift_binary_heap_down(heap, index, best);
  check(heap === sameArray && heap.length === survivors, "conversion");
  const record = () => {
    peak = Math.max(peak, heap.length);
    check(heap.length <= Math.min(population, survivors + limit - 1), "frontier bound");
  };
  record();
  while (outputs.length < limit) {
    check(heap.length > 0, "incomplete result");
    const {left, right, position} = pop_binary_heap_entry(heap, best);
    outputs.push(items[position]); record();
    if (outputs.length === limit) break;
    if (left < position) { push_binary_heap_entry(heap, head(left, position, false), best); record(); }
    if (position + 1 < right) { push_binary_heap_entry(heap, head(position + 1, right, false), best); record(); }
  }
  const expected = items.filter((_, index) => index !== excluded).sort(compare_total_key_order).slice(0, limit);
  check(outputs.map(x => x.id).join() === expected.map(x => x.id).join(), "ordered output");
  check(seed === parts.length && child <= 2 * (limit - 1), "RMQ bound");
  return {seed, child, peak, outputs: outputs.map(x => x.id)};
}
function* enumerate_key_order_permutations(values) {
  if (!values.length) { yield []; return; }
  for (let index = 0; index < values.length; index++)
    for (const rest of enumerate_key_order_permutations(values.filter((_, j) => j !== index)))
      yield [values[index], ...rest];
}
let forestCases = 0, selfCases = 0, jaccardCases = 0;
for (let n = 1; n <= 6; n++)
  for (const order of enumerate_key_order_permutations(Array.from({length:n}, (_, i) => i)))
    for (let mask = 0; mask < 2 ** (n - 1); mask++) {
      const parts = []; let left = 0;
      for (let i = 1; i <= n; i++) if (i === n || (mask & (1 << (i - 1)))) { parts.push([left, i]); left = i; }
      const items = order.map(id => ({id, num:-BigInt(id), den:1n}));
      for (let k = 0; k <= n + 1; k++) {
        verify_bounded_model_case(items, parts, k); forestCases++;
        if (n <= 5) for (let self = 0; self < n; self++) {
          verify_bounded_model_case(items, parts, k, self); selfCases++;
        }
      }
    }
let state = 20260920;
const random = bound => { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; return state % bound; };
const count = mask => { let total = 0; for (; mask; mask &= mask - 1) total++; return total; };
for (let trial = 0; trial < 10000; trial++) {
  const n = random(13), source = random(16), a = count(source);
  const ids = Array.from({length:n}, (_, i) => 17 * i + 1);
  for (let i = n - 1; i > 0; i--) { const j = random(i + 1); [ids[i], ids[j]] = [ids[j], ids[i]]; }
  const items = ids.map(id => {
    const target = random(32), b = count(target), c = count(source & target);
    return {id, b, c, num:BigInt(c), den:BigInt(c ? a + b - c : 1)};
  });
  const parts = []; let left = 0;
  for (let i = 1; i <= n; i++) if (i === n || items[i].c !== items[left].c) {
    parts.push([left, i]); left = i;
  }
  const local = (x, y) => x.c ? x.b - y.b || x.id - y.id : x.id - y.id;
  const self = random(n + 1) - 1;
  for (const k of [0, 1, 2, n, n + 3]) { verify_bounded_model_case(items, parts, k, self, local); jaccardCases++; }
}
const tightItems = [10, 1, 11, 12, 2, 13, 3, 4].map(id => ({id, num:-BigInt(id), den:1n}));
const tight = verify_bounded_model_case(tightItems, [[0,3],[3,6],[6,7],[7,8]], 3);
return {forestCases, selfCases, jaccardCases, tight};
~~~
