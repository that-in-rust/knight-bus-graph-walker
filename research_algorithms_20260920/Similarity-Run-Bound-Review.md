# A05 Run-Head Bound: Independent Theory And Prior-Art Review

Date: 2026-09-20. Scope: the proposed pre-RMQ test only. This review does not inspect or approve the lead's implementation, rerun its experiments, or close the broader research goal.

## Verdict

**Correct under the stated contract.** A full heap of k' distinct eligible exact heads supplies enough witnesses to reject an incoming interval when `c/a < score(worst head)`. The optimized scan retains exactly the same seeds as the existing bounded selector. If P initial intervals pass this strict rejection test, it saves exactly P initial logical RMQs; winner splitting and its child RMQs are unchanged. The interval scan, overlap construction, prepared indexes, and output remain paid. Worst-case work and the O(k') selector-state bound do not improve asymptotically.

**Scientific status: ordinary bound pruning, with an explicit A05 adaptation.** The generic principle has close primary precedent, including interval pruning before expensive block access and a full-heap, strict-bound test in exact Tanimoto search. The c/a envelope is elementary and information-tight, not a new similarity bound. Those precedents do not establish that the complete disk-RMQ/bounded-seed composition was previously published.

## Contract Read

Read [Similarity-Bounded-Seed-Selection.md](Similarity-Bounded-Seed-Selection.md) and [Novelty-Baseline-Evidence-Policy.md](Novelty-Baseline-Evidence-Policy.md). Review lenses: exact ranking, charged work/state, and skeptical historical attribution. No code or experiment receipts were used as evidence of this proposed step's correctness or speed.

The proof assumes:

- One coherent fixed snapshot; deduplicated binary feature sets and consistent cardinalities/postings/RMQ metadata.
- Unique original target IDs. Distinct targets may have identical feature sets. Physical position is not the tie key.
- Self removed before forming each initial nonempty interval. The R' resulting disjoint intervals partition the N eligible nonself targets; R' <= R+1 for R original overlap runs.
- `k' = min(k,N)`. Return exactly k' distinct complete records, descending exact Jaccard then ascending original ID, without expanding ties. All zero intersections, including empty-empty, score zero.
- For positive c, the exact head minimizes `(b, original ID)`; for c=0, it minimizes original ID alone. No late eligibility changes or partial publication.

Contract-file SHA-256 values at inspection:

```text
Similarity-Bounded-Seed-Selection.md
8277301f26aef90900068c63325f3f204f66c18272e75129bcb0ff4d03debc5a
Novelty-Baseline-Evidence-Policy.md
db38de5c8e93bf2ee398242a941fd9a4bca7af046a013001cec2cbd9466d7016
```

## Bound And Tightness

These are derivations for this contract, not quotations from prior art.

Let A be the source, `a=|A|`, and let a target B in an incoming interval have `b=|B|` and the interval's exact `c=|A intersect B|`. For a>0:

```text
0 <= c <= min(a,b)
a+b-c >= a
J(A,B) = c/(a+b-c) <= c/a = U(I).
```

For a=0, coherence forces c=0, and the contract makes every score zero: define U(I)=0 explicitly. Never divide by a or evaluate empty-empty as 0/0. Against an exact threshold `tau=p/q`, q>0, the test for a>0 is precisely:

```text
c*q < a*p
```

Use exact nonoverflowing products, not rounded scores or unchecked fixed-width multiplication. For a=0 the corresponding test is `0 < tau`; on a coherent query it never succeeds because every witness also scores zero. The positive-a cross-product expression is not a substitute for this definition at a=0.

**Tight with only a,c known.** For every feasible a>0 and 0<=c<=a, choose a target containing exactly c source features and no others. Then b=c and J=c/a. Give it an original ID different from self. In particular, c=a can attain one at a nonself target with identical features: unique IDs do not mean unique feature sets. For c=0 and for a=0 the zero bound is attained as well.

Therefore no uniformly smaller score upper bound depending only on a,c is valid over this contract. This is an information-relative statement, not a claim that every actual interval contains an attaining target or that RMQ access is globally optimal. Additional valid metadata can help. For example, a certified interval-wide `b >= ell` gives, for a>0,

```text
J <= c/(a + max(c,ell) - c).
```

Acquiring or storing ell is extra work/state; acquiring the exact minimum b by the RMQ one hopes to avoid would not save that RMQ. Without an ID certificate, score equality still cannot decide the tie order.

## Correctness

Write x precedes y when x has greater exact score, or equal score and smaller original ID.

**1. A strict rejection has enough actual witnesses.** When the heap is full, let h be its worst exact head and tau its score. Every retained head has score at least tau. If U(I)<tau, every target in I has score strictly below every one of these k' heads. The intervals are disjoint and IDs unique, so these are k' distinct eligible targets outside I. No target in I can belong to global top-k'. This needs neither the incoming b nor its ID.

**2. The old bounded-heap invariant survives without materializing rejected heads.** Induct over the initial intervals, comparing with the old scan that RMQs every interval. Before the heap fills, both scans obtain and retain each exact head. When the test rejects an interval, its hypothetical exact head is strictly worse than the current root, so the old scan would reject it too. Otherwise both execute the same exact RMQ and full-order heap update. Thus the retained seed set is identical after every prefix, including skipped intervals. Once full, its worst score cannot decrease; the historical rejection certificates remain valid as heads are replaced.

**3. Winner splitting remains necessary and sufficient.** At the end, every discarded interval has k' better distinct witnesses, either by the strict test or by the inherited exact-head comparison. The union V of retained seed intervals therefore contains the entire global top-k' and at least k' targets. Convert the same seed storage in place to best-first order. Pop the best head; unless this is the last required output, obtain heads for its nonempty remainders. The remaining heap partitions the unreported part of V, with an exact best representative for each piece. Consequently it emits exactly the required records in order.

The threshold here is the kth best *initial head seen*, not necessarily the kth best target seen or the final kth score. It is a sufficient lower bound, potentially a weak one when many winners occupy one interval. An interval's length does not make its single maximum into multiple high-scoring witnesses. If R'<k', the proposed schedule never fills the seed heap and never uses this pruning rule, even when a few intervals contain many winners.

## Exact Work And Heap Bound

For k'>0 define:

```text
s = min(k',R')                 final retained seed count
P = number of initial intervals rejected before RMQ
Q = R' - P                    initial RMQs actually issued; s <= Q <= R'
D = nonempty child intervals generated before the final output
u = seed replacements after the initial s insertions; u <= Q-s
N_V = number of targets in retained seed intervals
```

There are exactly R'-s logical bound decisions after the heap fills, if each incoming interval then executes the proposed gate. Some decisions may short-circuit arithmetic. There are Q-s exact incoming-head/root comparisons after filling. In a fixed traversal, the original and new selectors have the same s, u, V and child traversal.

| Charged item | Bound or identity |
| --- | --- |
| Initial interval visits | Exactly R', including the P rejected intervals |
| Initial logical RMQs | Exactly Q=R'-P, replacing the old R' |
| Child logical RMQs | Exactly D; D <= min(2(k'-1), N_V-s) |
| Total logical RMQs | Exactly Q+D; at most R'-P+2(k'-1) |
| Initial heap updates | s insertions and u replacements; rejected intervals insert nothing |
| Conversion | O(s) in-place key conversion and heapify; no second seed array |
| Enumeration | Exactly k' pops and D child insertions |
| Heap CPU | O(Q + (s+u+D+k') log(k'+1)); plus O(R') bound/scan work |
| Tree/metadata probes | O((Q+D) log(n+1)) in the existing disk-tree RMQ, n physical targets |
| Complete output | k' records, with staging/publication still charged |

The child-count refinement follows because every discovered head denotes a different target of V. Also `N_V <= N-(R'-s)`, since each discarded initial interval contains at least one target, hence `D <= N-R'` is another valid cap. These are logical counts, not physical I/O measurements.

For the same deterministic traversal, the change saves **exactly P logical initial RMQs**, plus their associated probes and exact head comparisons. It does not save child RMQs. Probe savings are the actual probes those particular skipped RMQs would have made, not necessarily P times a fixed logarithmic number. The added bound comparisons have a cost; no complete-query latency improvement follows from the logical identity alone.

After t nonfinal outputs, heap length is at most s+t (and at most N_V-t). Pop before pushing children, and skip all children after the final output. Thus:

```text
seed-phase heap peak <= s
overall heap peak <= min(N_V, s+k'-1) <= min(N, 2k'-1).
```

The inherited conservative reservation `min(N,2k'-1)` remains sufficient. A run bound does not reduce this worst-case reservation. A realizable five-slot example remains: a=6, k'=3, with three runs `(c; b values)` equal to `(5; 7,5,7)`, `(1; 1)`, `(4; 6,4,6)`. All targets have distinct nonself IDs. The first two winners are the interior targets of the first and third runs, with scores 5/6 and 2/3; the first run's children score 5/8. Heap length grows 3 -> 4 -> 5. All cardinalities/overlaps are realizable using sufficiently many outside features.

Heap length is not total resident bytes. An incoming head, popped winner, conversion temporary, exact-integer storage, list spare capacity, allocator overhead and output buffers remain chargeable. The gate itself needs only fixed extra state under the packed arithmetic model. Bit complexity must be included when integer widths are unbounded.

**No hidden preparation reduction.** If T_field denotes the existing paid overlap construction, an arithmetic-operation upper bound is:

```text
T_query = T_field
        + O(R' + (Q+k') log(k'+1) + (Q+k') log(n+1))
        + paid output/validation/publication work.
```

For endpoint merging, retain the existing `O(a log(F+1) + E log(g+1) + R)` construction charge: F is the feature-directory size, g the present query-feature streams, and L their total posting intervals. Retain O(g) admitted cursor state, all L interval reads and E=2L endpoint processing. The sort alternative retains its external-sort charges. Both retain the complete run spool and its scan; the query-independent index build, footprint and refresh remain separately paid. The existing output and scratch reservations are unchanged.

No sublinear-in-R' query bound follows: P can be zero for arbitrarily many runs. Take a=2, alternate singleton runs with c=1 and c=2, and give every target b=4. Their exact scores are 1/5 and 1/2, but their bounds are 1/2 and one. No bound is strictly below any possible seed threshold, so Q=R'. Zero runs likewise cannot be rejected while tau=0. For k'=0 the existing early empty-output path uses no selector/RMQ/overlap construction.

## Failure Cases For Weakened Rules

The following are hand-derived counterexamples, not another model suite. All set examples use source A={1,2} unless specified, with self already absent except where explicitly testing that mistake.

| Weakened condition | Concrete failure |
| --- | --- |
| Prune score equality | k'=1; visit singleton runs `ID20:{1}`, `ID30:{}`, `ID10:{2}`. Counts are 1,0,1, so these can be maximal runs. ID20 establishes tau=1/2. The last interval has U=1/2 and its correct winner is ID10. A `<=` test loses the required low-ID result. |
| Use a threshold before k' witnesses exist | k'=2; visit `{1,2}`, then `{1}`, then `{}` under distinct IDs. After only the first head, using tau=1 rejects the required second result of score 1/2. |
| Count upper bounds as exact witnesses | k'=1; first interval has c=2 and target `{1,2,3,4,5}`, whose actual score is 2/5 but U=1. Treating U as the retained score rejects the next `{1}` interval with U=1/2, losing the true winner. |
| Count self or duplicate targets as witnesses | For k'=1 a self head of score one can suppress the best nonself target and then disappear at output. For k'=2 two entries for one score-one target can falsely suppress a distinct score-1/2 target. Heap occupancy alone does not prove witness cardinality. |
| Use incomplete overlap as an upper bound | A valid witness `{1,3}` scores 1/3. For incoming `{2}`, after processing only feature 1 the partial overlap is zero. Using `0/a` rejects it, though its final exact overlap is one and score 1/2. Unprocessed contributions require a certified upper bound, not the partial sum. |
| Emit seeds without splitting | With A={1,2,3} and k'=2, let the first run have c=3 and target sizes 3,4, and the second have c=2 and target size 3. Seed scores are 1 and 1/2, but the correct second output is the first run's remaining target, scoring 3/4. |

If tau=0, zero-bound intervals must also survive equality until the ID-aware selector decides which zeros fill the tail. When tau>0 they can safely be rejected. Equality can be pruned only with an additional sound full-order certificate, for example a certified lower bound on incoming IDs that puts every possible equal-score target after the worst retained head. Physical ordering is not such a certificate under the general contract.

Likewise, an RMQ/read failure, stale cardinality or eligibility change cannot silently become a successful shortened answer. Abort without publishing incomplete output. A rigorous lower score bound for each of k' distinct eligible targets could certify rejection in a different algorithm, but optimistic scores, unverified counts and incomplete eligibility do not; the inherited exact-head enumeration still requires its own invariants.

## Inspected Primary Prior Art

The passages below are historical evidence. The mapping to this executor is stated separately afterward. Publication dates, not upload/crawl dates, identify priority. MSR and Ding/Suel passages were inspected in parsed primary PDFs; direct MSR screenshot retrieval timed out. PMC direct-open requests returned browser checks, so the chemical-search passages were inspected through search-retrieved text of the primary author manuscripts, including the named equations/procedures. This is not an exhaustive literature search.

### 1. Chakrabarti, Chaudhuri, Ganti, ICDE 2011

[MSR publication page](https://www.microsoft.com/en-us/research/publication/interval-based-pruning-for-top-k-processing-over-compressed-lists/) and [primary paper, Interval-Based Pruning for Top-k Processing over Compressed Lists](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/FinalProceedings_Top_IREngines.pdf), DOI 10.1109/ICDE.2011.5767855.

**Direct passage:** Section III-A, PDF p.3, specifies "ties broken arbitrarily". Algorithm 1, PDF p.6, gates processing with `V[j].ubscore > thresholdScore`.

**Published procedure, paraphrased:** Section IV-C constructs intervals with bounds from block summaries. Section IV-D maintains current top-k documents and tests each interval before reading/decompressing blocks and executing interval-local DAAT. PRUNESEQ follows document order; PRUNESCOREORDER follows descending bounds. The latter's Theorem 1 concerns distinct decompressed blocks under its stated access/buffer class, not arbitrary RMQ work or complete A05 cost.

This directly precedes upper-bound rejection of a whole interval before its expensive evaluation. Its arbitrary ties explain why its non-strict rejection is not A05's rule. Its intervals bound monotone combinations of per-term scores; they are not exact Jaccard overlap runs.

### 2. Ding And Suel, SIGIR 2011

[Primary paper, Faster Top-k Document Retrieval Using Block-Max Indexes](https://research.engineering.nyu.edu/~suel/papers/bmw.pdf), DOI 10.1145/2009916.2010048.

**Direct passage:** Algorithm 3, PDF p.6: `if ( maxposs > threshold ) then`.

**Published procedure, paraphrased:** Section 5 and Algorithms 1-3 use block maxima and shallow pointer movement to test candidates before deeper access/evaluation. Section 3, Figure 4 warns that simply substituting a current block maximum into global WAND pivoting is unsafe: its range of validity may end before the proposed skip. Section 4 explicitly discusses the independently developed interval-pruning paper above.

The relevant precedent is cheap summary-bound testing before expensive retrieval, with correctly delimited skip ranges. These are per-term impact bounds in a WAND schedule, not this nonadditive Jaccard formula or bounded RMQ seed selection. The printed scalar equality check does not itself establish the required original-ID semantics. Its experimental speedups are not evidence for A05.

### 3. Swamidass And Baldi, JCIM 2007

[Primary author manuscript, Bounds and Algorithms for Fast Exact Searches of Chemical Fingerprints in Linear and Sub-Linear Time](https://pmc.ncbi.nlm.nih.gov/articles/PMC2527184/), DOI 10.1021/ci600358f.

**Direct passage:** Section 6.1, Algorithm 1, line 12: `if K <= HeapSize(hits) and bound < MinSimilarity(hits) then`.

**Published procedure, paraphrased:** Equation 5 uses the binary-fingerprint size envelope `min(a,b)/max(a,b)` for nonempty sizes. Algorithm 1 visits cardinality bins in descending bound order, evaluates candidates, tracks hits in a heap and stops once the full-heap bound test succeeds.

This is especially close precedent for the full-heap/strict-score condition in exact Tanimoto search. It uses known b rather than known c. Ordered-bin termination cannot become termination of A05's physical-order run scan; only the current bounded interval is rejected here.

### 4. Nasr, Vernica, Li, Baldi, JCIM 2012

[Primary author manuscript, Speeding Up Chemical Searches Using the Inverted Index: the Convergence of Chemoinformatics and Text Search Methods](https://pmc.ncbi.nlm.nih.gov/articles/PMC3415597/), DOI 10.1021/ci200552r.

**Direct passage:** The top-K paragraph following Figure 3 says the algorithm "starts with T = 0 occurrences".

**Published procedure, paraphrased:** The section Using the Inverted Index for Similarity Search, Equation 3, transforms a Tanimoto threshold into an intersection threshold using known a,b. Cardinality bins and DivideSkip prune feature-list work; the top-K variant updates its threshold as a size-K result array fills.

This establishes direct Tanimoto/Jaccard inverted-search precedent, not merely an analogy to text ranking. Its preprocessing and occurrence search differ from constructing all exact overlap runs and RMQ-selecting their heads. The inspected procedure does not establish A05's complete zero-tail, original-ID, self-exclusion and staged-output contract.

## Adaptation And Claim Ledger

**Derived here for the proposed step:** map a prunable group to an initial nonself overlap interval; its score upper bound to c/a; its lower-threshold witnesses to k' retained exact heads; and the expensive operation to the existing RMQ. The proof above supplies the full-order and completion obligations. This is an ordinary instantiation of published group-bound pruning, not a claim that any cited pseudocode is the unchanged A05 executor.

| Claim | Supported conclusion |
| --- | --- |
| New generic top-k bound pruning | Not supported; direct published procedures precede it. |
| Correct pre-RMQ A05 gate | Supported by the explicit bound and witness proof. |
| Fewer mandatory initial RMQs | Supported conditionally: R' becomes Q=R'-P; P can be zero. |
| Lower worst-case heap or sublinear total run work | Not supported; inherited reservation and the full scan remain. |
| Entire A05 composition already published | Not established by the inspected passages. |
| End-to-end speedup or paper-worthy novelty | Not established by this review. |

A comparator implementing the new gate is an ablation/reimplementation control. It is not independent historical evidence that the exact composition appeared earlier. Conversely, an ordinary Jaccard adaptation of published pruning must be described with its additional index, tie, zero-tail, preparation and output costs, not called a verbatim published executor.

## One Next Scientific Direction

**Ask what paid, query-independent certificate can avoid constructing rejected overlap regions, not merely avoid their final head lookup.** The present gate learns exact c only after all relevant endpoints have already been processed. Its information-tightness leaves no stronger a,c-only envelope to discover.

A narrowly scoped next result would specify a summary/access model that certifies an overlap upper bound for a physical region before expanding its endpoint streams, then prove either a metadata-versus-work tradeoff or an impossibility result using indistinguishable coherent snapshots. Require a separating family where endpoint/run-generation work, not just RMQs, falls after charging certificate acquisition, index build/refresh, and exact output. Compare against published interval/block pruning with an explicitly derived Jaccard adaptation on the same available information. Merely storing block cardinality minima or renaming block maxima is not the scientific delta. No implementation or broad architecture search is proposed here.

## Bounded Fresh Algebraic Check

One fresh in-memory JavaScript BigInt check was run for this review, loading no repository code and writing no files. Scope: `a=0..8`, `b=0..12`, `c=0..min(a,b)`, and every pair of target/threshold triples sharing a. It checked the envelope, exact cross-product equivalence with the explicit a=0 branch, safety of every strict rejection, and equality attainability at b=c.

Result: **465 feasible triples; 28,437 target/threshold pairs; 9,479 strict rejections, all sound; 45 b=c attaining cases; no assertion failures.** This is a finite algebra sanity check, not a selector/RMQ/endpoint test, general proof, benchmark, overflow test for a packed implementation, or validation of the lead's changes. The proof, historical attribution, and work accounting above are the review's deliverable. The broader goal remains open.
