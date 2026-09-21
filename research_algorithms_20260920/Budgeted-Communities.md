# Budgeted Communities: Exact Serial-Move Run Certificates

Date: 2026-09-20. Research candidate, not production implementation or a claim of worldwide novelty.

Current follow-on: [two monotone frontier streams](Communities-Monotone-Frontier-Streams.md)
derive and implement a different execution representation for the exact
matching-defect family. It avoids the previous delete-only endpoint sets and
retained fragmented labels, while preserving the original serial accepted
trace. This does not remove the general manuscript's eligibility conditions
or turn its interval obstruction into a universal RAM lower bound.

## Premise Check

The contribution worth testing is **event compression of a greedy optimization trajectory**: certify that the next `b` individual vertex decisions have the same outcome, then execute their aggregate state change without performing `b` candidate searches. It is not merely compressed adjacency, external Louvain, a community histogram cache, simultaneous Jacobi moves, or permanently contracting equivalent vertices.

The mathematical result below concerns one explicitly defined Louvain-style local-moving schedule. It preserves that schedule exactly, including when equivalent vertices must split across communities. It does not find the global modularity optimum, reproduce an unspecified Neo4j GDS partition, or inherit Leiden's refinement guarantees.

Repository inputs inspected:

- [Portfolio README](README.md): A04, whole-workflow resource and evidence requirements.
- [Decision Map](../research_4gb_20260919/Architecture-Decision-Map.md): D06 already avoids derived clique expansion; D15 already proposes bounded community histograms with current-total revalidation. Neither is claimed as this contribution.
- [Decision Brief](../research_4gb_20260919/Final-Research-Decision-Brief.md): preparation, original-ID output, freshness and failed fast paths all count.
- [PRD04 storage analysis, section 6.5](../docs_PRD04/Algorithm-Storage-Decision-Analysis.md) and [innovation atlas, A6.1 and Duck 10](../docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md): streamed levels, tally spill and old/new disk overlap are inherited infrastructure.
- [Sol-02](../docs_PRD04/Sol-02.md): community detection is an engineering-priority hypothesis, not a measured usage share.

Expert lenses: discrete optimization, symmetry/compression, external-memory execution, and adversarial schedule equivalence. The main skeptical question is whether a compressed vertex represents interchangeable *decisions*, not just interchangeable adjacency rows.

## Candidate Approaches

| Candidate | Potential work elimination | Decision |
| --- | --- | --- |
| Cached tallies plus drift bounds | Avoid rescanning a neighborhood whose best move remains unchanged | Useful baseline, substantially overlaps D15 and existing active-set approaches |
| Permanently merge twins or dense communities | Replace many vertex decisions with one supervertex | Reject as an exact local-moving transformation; it removes possible splits |
| **Certified Serial Cohort Jumps (CSCJ)** | Replace consecutive identical decisions by an analytically certified run | Main candidate; exact for the declared schedule, benefit depends on actual run lengths |
| Approximate-twin robust intervals | Bound row differences and certify some common decisions without full equivalence | Follow-on conjecture, not used in the guarantees or resource estimates below |

The narrow proposed delta is an affine crossover certificate over split-capable multiplicities, compared with a **matched type-cached scalar engine**. Both engines now build the same score cache once per contiguous type block and update only two score keys per move/event. Avoided repeated histogram scans belong to that shared baseline, not to CSCJ. Generic symmetry reduction, caches and algebraic aggregation are established ideas.

## Precise Graph And Answer Contract

Input is a finite undirected weighted graph on stable original IDs. Distinct vertices have symmetric nonnegative integer weights `A_uv`; zero means absent. Parallel source relationships are summed after the specified undirected projection. Source self-loops are rejected in this first profile. Directed projections, negative weights, binary thresholding of shared-feature counts, and arbitrary floating weights require separate profiles. Nonnegative rational weights are supported only after an explicitly charged common scaling to integers.

Let:

```text
k_v = sum_u A_vu
M   = sum_v k_v = 2m
gamma = p/q > 0, with positive integers p,q
K_C = sum_{v in C} k_v
Q(P) = (1/M) sum_{u,v in same community} A_uv
       - (p/q) sum_C (K_C/M)^2
```

Both orientations occur in the first sum. Isolates remain present. For `M=0`, define `Q=0` and return the initial partition unchanged. We maximize this specified generalized modularity, not another definition of community quality.

The complete result is an original-ID membership stream plus the exact rational modularity, configuration/snapshot identity, and a termination status. Community labels are deterministic internal tokens, not semantic IDs. A completed no-move sweep means **single-vertex local optimality** under this objective. A sweep/time budget exhaustion means `budget_stopped`, with a valid monotone-improved partition but no local-optimality claim.

### Declared Reference Schedule

1. Fix a certified type order, then increasing original-ID order inside each type. This order is immutable during a local-moving run.
2. Start from a supplied partition; default is all singletons. A type-aligned warm start is an explicit different configuration, not a hidden optimization.
3. Visit each vertex once per sweep in that order. Evaluate every active destination community and a fresh singleton after conceptually removing the vertex from its source.
4. Move only for a strictly positive modularity gain. Among equally good positive moves, choose the smallest destination token. Staying wins every zero-gain tie. Fresh singleton tokens increase monotonically and are larger than existing tokens.
5. Commit immediately. No other vertex move interleaves with the current decision. Repeat until a complete no-move sweep, or the declared budget.

The all-community rule specifies semantics, not a required full scan. For `k_v>0`, a zero-affinity destination has score `-p*k_v*K_C<=0`, so a positive-volume non-neighbor loses to fresh. A zero-volume community ties fresh and its older token may win. A tie-preserving adjacency baseline evaluates neighbor communities, source, fresh, and the smallest-token active zero-volume community. For `k_v=0`, all gains are zero and the vertex stays.

**Singleton-first-sweep limitation:** no multi-vertex cohort exists anywhere in the default first sweep. Earlier moves change only visited vertices' labels; every unvisited vertex retains its own distinct initial singleton token. Even one perfect twin type cannot defeat this obstruction. Moving jumps require a different supplied start or later sweeps; any warm-start construction and quality difference must be charged identically to both engines.

### Structural Certificate

Partition vertices into `t` types. Every distinct pair from types `a,b` has weight `w_ab=w_ba`; every distinct pair inside type `a` has weight `a_a=w_aa`. All members of a type therefore have degree

```text
k_a = sum_b n_b w_ab - a_a.
```

This is weighted true/false-twin structure, not an equitable partition defined only by equal neighbor counts. Type membership **does not constrain** community membership. Maintain `x_aC`, the number of type-`a` vertices currently in community `C`; a type may occupy many communities.

For near-template input, one conservative exact route is to isolate every endpoint of a mismatching edge as its own type, retaining the untouched remainder of each type. This can destroy compression. It is not legitimate to erase the defects. If supplied types fail validation, split/refuse/replan before taking any jump.

## Affine Run Certificate

For the next vertex of type `a` currently in source `A`, define its community affinity excluding itself:

```text
h_C = sum_b w_ab x_bC - a_a * [C=A]
Kminus_C = K_C - k_a * [C=A]
sigma_C = q M h_C - p k_a Kminus_C
sigma_fresh = 0
DeltaQ(A -> B) = 2 (sigma_B - sigma_A) / (q M^2).
```

The self-exclusion in both affinity and volume is essential. These are exact integers, so no epsilon or unqualified floating comparison decides a move.

Suppose the next `b` vertices in the fixed visit order are of the same type and currently have source `A`. There is no intervening vertex visit. The first vertex chooses destination `B`. If `r` predecessors have made the same `A -> B` move, define

```text
beta = q M a_a - p k_a^2
sigma_B(r) = sigma_B(0) + r beta
sigma_A(r) = sigma_A(0) - r beta
sigma_C(r) = sigma_C(0), for C other than A,B.
```

The next vertex itself has not moved: its source still needs the same self-exclusion. Every moved twin contributes `a_a` affinity and `k_a` volume to the destination. This proves the displayed affine identities.

Let `R` be the largest unchanged destination score, with the smallest token among ties, including an available fresh singleton at score zero. If `B` was itself the fresh singleton, subsequent fresh singletons still have score zero and a larger token. The jump length is the largest `ell <= b` such that for every integer `0 <= r < ell`:

```text
sigma_B(0) - sigma_A(0) + 2 r beta > 0
sigma_B(0) - sigma_R(0) + r beta >= 0
```

The second inequality becomes strict if `R` wins an equal-score token tie. Missing unchanged communities still leave the fresh-singleton competitor. A source that empties only does so after its last member's decision; the certificate is never queried beyond the available run.

Only source, best destination and best unchanged competitor are needed for the certificate. Let `g=sigma_B-sigma_A`, `h=sigma_B-sigma_R`, and `epsilon=1` if `R` has a smaller token than `B`, otherwise zero. A valid first move has `g>=1` and `h>=epsilon`. Use this exact division procedure, not a repeated predicate search:

```text
if beta >= 0: ell = b
else: ell = min(b, 1 + floor((g-1)/(-2*beta)),
                   1 + floor((h-epsilon)/(-beta)))
```

All divided numerators are nonnegative. Staying remains strictly preferable to any zero-gain move regardless of token. This distinction explains the `g-1`, while the competitor bound uses `h-epsilon`.

If `beta >= 0`, a winning positive move cannot lose either inequality during this homogeneous run: the whole run can jump. If `beta < 0`, explicitly stop at the first crossover. Recompute at that position, which may select another destination or stay.

If the first vertex stays, all remaining vertices in this run stay: no state changes, and their scores are identical. This no-move shortcut alone is not the originality claim; the nontrivial candidate is collapsing **moving** runs while preserving sequential feedback and allowing splits.

## Type-Cached Execution

For the current contiguous type block `a`, keep one score per **active community**, including active zero-volume communities:

```text
s_C = q*M*sum_b w_ab*x_bC - p*k_a*K_C
source sigma_A = s_A - beta
other sigma_C = s_C; fresh sigma = 0
transfer ell type-a members A -> B:
    s_A -= ell*beta
    s_B += ell*beta       # use s_B=0 before creating a fresh community
```

Build this cache once when entering a type, by a community-grouped count/volume stream; discard/rebuild it at the next type. No `t*C` simultaneous cache is needed. Two score-key updates suffice per scalar move or jump because the only changed multiplicities and volumes are those of `A,B`. Remove a cache key only when its **member count** is zero, never merely when its volume or score is zero. Cached scores exclude no vertex; source self-exclusion is applied at query time. A newly created destination takes the current fresh token; only then advance the token counter, and offer its replacement at score zero.

Resident mode uses an indexed max-heap with a token-to-position map, ordered by decreasing exact score then increasing token. Heapify is linear in `C`; replacement, insertion and deletion take `O(log(C+2))`. Enumerating the top three keys with a constant-size heap frontier supplies the best two excluding source. Merge in fresh and, if it wins, the replacement fresh token to obtain `R`. The retained probe implements this heap and independently streams the entire cache, in alternating orders, retaining only two extrema to check every choice. Streaming is the validation/fallback path, not a hidden per-event scan in the cached main path.

The probe uses a JS Map for token lookup, so its lookup complexity relies on that runtime. For a deterministic worst-case implementation, index tokens in a balanced tree pointing to stable heap handles; swaps update handle positions directly. One tree search and one heap repair still cost `O(log C)`. Never allocate a position array indexed by all tokens ever issued: retired-token history can exceed `C`.

If `O(C)` score state does not fit its reservation, use token-keyed and score/token-ordered external indexes with explicitly reserved page caches. Two changed *logical* keys entail a constant number of index operations, not two physical writes. Bulk-build each type's indexes, and charge sort, page splits and WAL. A deliberately scan-only fallback may spend `O(C)` per event, but must be labeled separately from the matched indexed scalar baseline.

## Pseudocode

```text
execute_certified_cohort_sweeps(snapshot, initial_partition, p, q, budget):
    validate projection, type certificate, integer widths and resource admission
    store initial labels as intervals in fixed (type, original-ID) rank order
    build sparse (community,type,count) table and exact community volumes
    repeat:
        changed = false
        cursor = first vertex rank
        while cursor exists and logical budget permits a visit:
            (type, source, run_length) = next homogeneous current-label interval
            if entering a new type:
                stream counts/volumes; build that type's score cache and extrema index
            read source cache score minus beta; query best/runner-up excluding source
            merge fresh candidate, retaining exact token order and replacement fresh
            truncate run_length at remaining logical visit budget
            if staying is optimal:
                advance cursor by run_length
            else:
                truncate run_length at remaining logical move budget, if any
                if run_length == 0: return current partition, status=budget_stopped
                ell = direct integer-division prefix(run_length, g, h, beta, epsilon)
                reserve log/index pages, interval splits, count cells and fresh token
                prepare interval prefix rewrite, two counts and two volumes
                prepare source/destination score keys with -ell*beta and +ell*beta
                prepare empty-source removal and used-fresh token advancement
                prepare next_cursor=cursor+ell and exact objective receipt
                commit all prepared state, token counter and cursor as one event
                changed = true
            checkpoint or stop at a declared budget boundary
        if complete sweep and not changed: return partition, exact Q, status=locally_optimal
        if budget exhausted: return partition, exact Q, status=budget_stopped
```

Atomic event commit means equivalent observable partition state at event boundaries, not a claim that simultaneous stale proposals are safe. A request to expose every intermediate membership vector disables the compressed-output advantage. An event receipt can encode the run and its exact serial gains without expanding those vectors.

An interval commit changes at most the containing run plus its two neighbors: split off the visited prefix, retain a source suffix when necessary, coalesce adjacent equal `(type,token)` runs, then change two count cells and volumes. Zero count cells are deleted. The exact objective-numerator receipt is

```text
Delta(q*M^2*Q) = 2*(ell*g + beta*ell*(ell-1)).
```

A logical visit/move budget can stop at the same scalar position by truncating the prefix. A wall-clock stop promises correspondence to a scalar prefix, **not** the same final partition as a slower engine given the same elapsed time. Checkpoint elapsed-time stops at committed event boundaries. A partial sweep never certifies local optimality.

## Correctness And Scope Of Theorems

**Lemma 1: exact candidate scores.** Expanding the change in the two affected squared community volumes and the internal-edge sum gives `DeltaQ=2(sigma_B-sigma_A)/(qM^2)`. All other community contributions cancel. Therefore maximizing the scores gives the exact reference decision.

**Lemma 2: exact event compression.** The affine identities describe the actual state before each next member's visit. The first inequality enforces strict improvement; the second enforces optimality and the token tie order against every unchanged candidate. Induction on `r` proves all `ell` individual moves occur in the reference schedule. The aggregate transfer gives exactly their final multiplicities, volumes and labels. If the prefix ends before the interval ends, recomputation happens before the first uncertified decision.

**Lemma 3: cache invariant.** At type-block entry the count stream defines every `s_C` exactly. A type-`a` transfer changes only source/destination affinity by `ell*a_a` and volume by `ell*k_a`, yielding the two displayed updates. Heap order includes tokens and preserves the maximum; three extrema suffice after excluding one source. Fresh/replacement entries restore the always-available zero candidate. Induction therefore makes the cache engine and the all-community semantic reference choose the same destination. This applies equally to the scalar baseline and CSCJ.

**Theorem: schedule equivalence.** Under the semantic and structural certificates, exact arithmetic, and the stated visit order/initial partition, CSCJ has the same partition at every completed event/sweep boundary as the scalar reference. This is a derived mathematical result in this note, not a formally machine-checked proof or a novelty theorem.

Every accepted move strictly increases `Q`. On a fixed finite graph there are finitely many partitions, so uncapped execution terminates. Since `-gamma <= Q <= 1`, integer score gaps also give the crude bound

```text
accepted individual moves <= (q+p) M^2 / 2.
```

This is a loose pseudopolynomial upper bound, not a useful completion deadline. A complete no-move sweep evaluates each vertex on an unchanged final partition, proving single-vertex local optimality. Neither assertion proves a global optimum, high application quality, or connected communities.

### Louvain Levels And Leiden Boundary

The proved core is the Louvain **local-moving phase**, usable standalone. An optional conventional multilevel wrapper can contract the current communities, but it must preserve internal-edge weight as diagonal mass, retain original degrees/total `M`, and charge map composition and old/new levels. The loop-free certificate above is not automatically a certificate for these looped supervertices: run scalar exact moves at those levels unless a separate equal-diagonal derivation is supplied. That wrapper retains ordinary monotone modularity and its declared schedule, not an unchanged original-node trajectory.

For final original-node local optimality after contraction, lift and run the original-node sweeps again. Without that step, report only the guarantee for the final contracted level. No multilevel implementation was tested here.

Leiden's local queue, constrained randomized refinement and aggregation are a different algorithm. Theorems about its connectivity/subset optimality require those phases and conditions; merely adding a connected-component split is not an equivalent substitute. A future Leiden integration must certify identical refinement eligibility, RNG consumption and visit behavior, or advertise a different algorithm.

## Counterexamples That Changed The Design

### Identical Vertices Must Be Allowed To Split

Take `K_2,2` with left vertices `u,v`, right vertices `x,y`, and all four cross edges of weight one. Types are `{u,v}` and `{x,y}`. Start with communities `A={u,v}`, `B={x}`, `C={y}`, tokens `A<B<C`, and visit `u` then `v`; `gamma=1`.

```text
M=8, k_u=k_v=2, a_left=0, beta=-4.
Before u: sigma_A=-4, sigma_B=4, sigma_C=4.
u chooses B. Before v: sigma_A=0, sigma_B=0, sigma_C=4.
v must choose C, not B.
```

Exact modularities: initial `-3/8`; after `u -> B`, `-1/8`; after `v -> C`, `0`. Blindly transferring both twins to `B` gives `-1/8`, a different partition and a forbidden zero-gain second move. Permanent twin contraction cannot reproduce the split. **Correction:** keep multiplicities per community, preserve member identity/order, and stop the run at the affine crossover.

### Positive-Curvature Hand Check

In a unit-weight 60-clique, start with two consecutive 30-vertex communities, and visit the first community first. All vertices have `k=59`, `M=3540`, `beta=59`. The first source/destination scores are `29*59` and `30*59`. Every later source-to-destination gap increases by `2*59`; all 30 vertices transfer in one certified event. The starting `Q=-1/118` becomes `Q=0`. This is 30 serial moves represented by one event, not a measured 30x speedup.

The revised executable cache/interval model confirms this contrast with identical starts within each row:

| `K60` first sweep | Cached scalar | Cached CSCJ |
| --- | ---: | ---: |
| Default singleton: moves / moving events | 59 / 59 | 59 / 59 |
| Default: cache-build count reads / score-key updates | 60 / 118 | 60 / 118 |
| Warm two-by-30: moves / moving events | 30 / 30 | 30 / 1 |
| Warm: cache-build count reads / score-key updates | 2 / 60 | 2 / 2 |
| Warm: heap comparisons / interval commits | 59 / 30 | 1 / 1 |

Warm input preparation still visits 60 IDs for both engines. Across unit cliques `K_(2b)` with the same two consecutive `b`-member seed, the moving phase has `b` scalar decisions versus one CSCJ event at constant `C=2`; both retain an `Omega(n)` input/output floor. This is an event-work separation from a competent cached comparator, not an end-to-end asymptotic speedup or evidence that naturally useful starts have this shape.

### Zero-Volume Tokens Are Real Candidates

For the edge `uv` plus isolated `z`, start `{u,v}` at token 0 and `{z}` at token 1, `gamma=3`, next token 2. Visiting `u` gives source score `-1`, isolate-community score zero, and fresh score zero. It must choose token 1. Removing zero-volume keys or pruning all non-neighbors changes the trajectory. The revised probe checks this decision.

### Approximate Twins Are Not Certified Twins

Adding one incident weighted edge to one member changes its degree, null-model penalty and affinities. Sharing a feature vector, degree, or sampled neighborhood does not validate `beta`. A future approximate-twin bound must include both affinity error and degree/volume error. For now, invalidate the affected structural certificate.

## Executed Falsification Evidence

Ephemeral JavaScript probes ran in the tool runtime on 2026-09-20. Dependency-free source was subsequently retained in [community-triangles-Probe-Evidence.md](community-triangles-Probe-Evidence.md) at the lead's request and rerun from that Markdown file with Node.js v24.9.0. No standalone production/probe code or scaffolding was added. Arithmetic is integer-exact at these small magnitudes.

Protocol, sufficient to reconstruct the exhaustive domain:

```text
vertex types = [0,0,1,1,2]
enumerate all 64 symmetric Boolean 3x3 type matrices, diagonal included
expand off-diagonal vertex adjacency; exclude M=0
enumerate all 52 set partitions of five vertices via restricted-growth strings
gamma in {1/2, 1, 2}
for each case, run one scalar sweep and one affine-jump sweep
scalar oracle chooses moves by recomputing full modularity numerator:
    q*M*(directed internal weight) - p*sum_C K_C^2
compare exact final labels and accepted individual-move counts
```

| Quantity | Observed result |
| --- | ---: |
| Compared configurations | 9,672 |
| Label/move-count mismatches | 0 |
| Accepted individual moves | 16,902 |
| Moving jump events | 16,146 |
| No-move vertex visits skipped/grouped | 31,458 |
| Largest moving jump in this tiny domain | 2 |

Two template matrices are edgeless after removing diagonal self-pairs on the singleton type, giving `62*52*3=9672`. The probe recomputed full scalar objective changes independently of the compressed score expression. It checked a single sweep, not all multilevel behavior, weighted arithmetic limits, persistence, refresh or a 4 GB run. The 60-clique example is a separate exact hand check. The modest event reduction in the exhaustive tiny set is important negative evidence against a universal speedup claim.

A retained deterministic weighted test adds 2,000 nine-vertex configurations with three three-member types, weights in `{0,1,2,3}`, four initial-partition patterns and resolutions `{1/2,1,2,3/2}`. It compares every completed sweep, fresh-token state and individual-move count against full-objective recomputation until stability. The specified upper-bit LCG uses seed `20260920`; this is reproducible test generation, not representative sampling.

| Weighted multisweep quantity | Observed result |
| --- | ---: |
| Completed sweep comparisons | 3,877 |
| Mismatches | 0 |
| Accepted individual moves / moving events | 8,952 / 7,966 |
| Largest jump / largest sweep count to stability | 3 / 5 |

Those original probes retain all candidate scores and use binary search; they remain algebra/sweep tests, not evidence of the revised data structures. The new retained `execute_cached_interval_sweep` separately implements sparse counts, one indexed score heap per type, streamed extrema validation, BigInt direct division, interval prefix split/coalescing, source deletion, fresh-token allocation and exact event receipts. It compares **every individual decision inside every event and every event-boundary state** with a full-objective BigInt oracle. The matched scalar engine uses the same cache/count/interval procedure with `ell=1`.

The revision domain includes all 64 templates (including edgeless graphs), all 52 partitions, three resolutions, and non-dense token permutation `[31,7,203,55,101]`: **9,984 matched pairs, 95,636 event boundaries and 99,840 individual gain receipts**, zero mismatches. Moving score-key updates are **33,816 scalar versus 32,304 CSCJ**; the latter executes 1,556 interval-count increases due to splits and 18,338 divisions. These are operation counts, not timings.

Separate probes exercise 248,040 crossover configurations, a 501-member negative-curvature arithmetic prefix, 100-bit division, 80-bit graph weights, the older zero-volume token, and a 17-visit budget truncation. A complete `K40,40` sweep with initial community sizes `40/30/10` verifies a **21-member first jump at negative beta**, including its final competitor tie and the next changed decision. Every moving event forces interval-capacity refusal before mutation and verifies unchanged live state. Detailed source/counters are retained in the evidence file.

The interval model genuinely edits intervals/count cells, but uses a small array for interval location/replacement, not the proposed on-disk tree. The score heap is implemented; crash atomicity, external community indexes and production width admission are not. Oracle expansions and validation scans are excluded from algorithm operation counters but not from the probe's wall time.

## RAM, I/O And Worst Cases

Use decimal bytes: physical ceiling `4,000,000,000`; provisional worker limit `3,000,000,000`, with the other `1,000,000,000` covering OS, page cache outside the worker, source/client processes and other overhead. This split is an experiment assumption, not a measured platform guarantee. Run one heavy stage at a time and measure physical pressure as well as worker RSS.

Symbols: `n` vertices, `t` types, `C` active communities, `x=nnz(x_aC)<=n`, `z<=n` membership intervals, `J` moving/staying events, `U` moving events, `V` scalar visits, `b_io` bytes per I/O page, and `w` admitted arithmetic/record width. `B_types` denotes all type-block entries across sweeps, not simultaneous caches. Variable-sized IDs add their actual bytes and offsets.

### Algorithm-Shaped State

Store a type-major original-ID tape and type boundaries, weighted template rows, a sparse `(community,type,count)` table, community volumes, and split-capable membership intervals. Membership labels never need an `n`-entry resident array. Do not eagerly store every vertex's community histogram or every level graph.

```text
R_worker <= R_runtime + R_type_row_cache + R_count_page_cache
            + R_score_heap_or_index_cache + R_interval_cache
            + R_sort_or_merge + R_IO + R_output + R_WAL
            <= 3,000,000,000
```

The maximum row, community count and moved prefix cannot enlarge these reservations. With resident score heap and template row:

```text
CPU_kernel = O(sum_{b in B_types}(x_b+C_b)
               + U*log(C_max+2) + J*log(z_max+2) + output_rows)
IO_query_bytes = O(sum_{b in B_types} w*(x_b+C_b)
                   + b_io*J*log_{b_io/w}(n+2) + output_bytes)
```

The interval-tree term includes lookup and split/coalescing; two count/volume changes add the same index order. A resident heap needs `O(C)` records **including its token-position index**, not merely three extrema. The probe implements heap updates; its interval-array replacement costs `O(J*z_max)` CPU and is not evidence of the tree bound.

External-score mode adds bulk construction `sum_b Sort(w*C_b)` traffic and comparison-sort CPU, plus `O(b_io*J*log_{b_io/w}(C_max+2))` query/update bytes. Keep token and score orders explicitly; two logical-key changes touch a constant number of paths in both. The external B-tree procedure is specified, not executed. A scan-only mode costs `O(sum_events C_event)` and is not the comparator used to claim a separation.

A too-large template row adds at worst one page miss per count cell **at type entry**, `O(b_io*sum_b x_b)` bytes, not per event. Sorts, indexes, checkpoints and output ordering are whole-job costs beyond the arithmetic kernel. Numeric operation cost depends on operand width.

Matched scalar has the same initialization and structures, with `J=V`, `U=accepted_moves`; only their reduction by certified prefixes is the proposed delta. With `t=n`, rebuilding at each singleton type can still require `Theta(n^2)` count visits per sweep. With `t=1`, the singleton clique builds once in `O(n)` and makes `O(n log n)` heap work, not the old avoidable quadratic scans. A neighbor-pruned adjacency engine can win when type caching is a poor fit. Alternating labels can force `J=V` even for one type.

Integer arithmetic is not free. Comparisons involve `q*M*h`, `p*k*K` and `M^2`; bound their bit width from input weights, `n,p,q` before choosing u128. Otherwise allocate explicitly budgeted multiprecision or refuse the exact profile. The probe does not establish production overflow handling.

### Preparation And Structural Discovery

For expanded input, normalize IDs/undirected pairs, deduplicate and sum weights through bounded external sort. With `S` normalized bytes, sort memory `U`, fan-in `F`, and `passes=ceil(log_F(max(1,ceil(S/U))))`, a simple run/merge schedule moves about `2S(1+passes)` bytes, plus ID joins and the source read. Two input/output run sets may coexist; they must be reserved.

A supplied candidate type partition can be verified without keeping an adjacency row in RAM: aggregate each unordered type-pair's distinct edge count and minimum/maximum positive weight, and compare with `n_a*n_b` or `choose(n_a,2)`. A nonzero uniform block requires complete coverage and one weight; an empty block requires zero edges. Exact duplicates must already be removed. Count equality plus uniqueness and valid endpoints certifies coverage. Partial blocks fail this certificate.

This costs an additional sort of annotated edges and `O(t^2)` possible block metadata in the worst case, represented sparsely/on disk. Type discovery is not assumed free: testing supplied groups is enough for the first experiment. General row-equivalence discovery or near-template optimization is a separate paid stage. Source-native type declarations avoid reading nonexistent expanded edges only when those declarations are themselves the authoritative graph semantics.

### Retention, Output, Refresh And Recovery

For fixed-width illustrative records:

```text
S_generation ~= 8n original IDs + S_template + 24z intervals
                + S_count_indexes + S_volumes + S_boundaries + S_manifest
S_output >= 16n for original-u64-ID/community-u64-ID rows
S_retained = unique bytes of all pinned generations, outputs and shared artifacts
S_live_peak = S_local_input + S_retained + S_unpublished_replacement
              + S_scratch + S_WAL + S_other_portfolio
```

Enforce `S_retained <= 50,000,000,000`. For the proposed strict experiment, also require `S_live_peak <= 50,000,000,000`; otherwise disclose a separate larger scratch requirement and do not call that experiment a 50 GB total-disk success. Shared immutable blocks count once; pinned distinct old/new blocks count twice.

Illustrative payload, not a benchmark: `n=100M`, `t=10k`, `z=x=5M`, `C=1M`, u64 template weights in a full matrix, two 24-byte count-index orders and 32 bytes/community give `1.992 GB`, before metadata, plus `1.6 GB` full output. A type row is 80 KB. A packed heap/token-index reservation of 96 bytes/community adds 96 MB for **one** live cache, plausibly within 3 GB but requiring measurement; do not assume `C=1M` precludes caching. JavaScript object/Map overhead is not represented by this estimate.

Each type entry scans 120 MB of count payload. Ten thousand type entries can still read 1.2 TB per sweep, identically for cached scalar and CSCJ. In external-score mode reserve two 32-byte index orders (`64C` bytes before occupancy overhead), bulk-build scratch and WAL; stage the new cache before retiring old blocks. Their overlap and retained graph/output generations count in `S_live_peak`. No `t*C` cache is retained. Storage fit is not a latency result.

Refresh applies a complete source change interval to a new snapshot. Even one changed edge changes `M`, so **all old affine move certificates expire**, including those for remote vertices. Stable type certificates can survive only after exact affected-block validation; a broken group is split before reuse. The old partition may be a warm start, explicitly identified as such. Rerun the schedule with new degrees and total weight. This note provides no unchanged-community certificate.

Persist `(snapshot, gamma, visit-order hash, initial-partition hash, cursor, labels, counts, volumes, fresh-token counter)` consistently. Commit event state and cursor together, or replay the event exactly once. Bound the WAL with checkpoints and admission before interval/index splits. An old reader pins its membership generation; a full result export pins its buffers only up to consumer backpressure. If refresh requires old/new storage beyond the cap, wait, expire an authorized lease, stream output to an agreed sink, or refuse. Do not silently delete another reader's generation.

## Closest-Art Claim Matrix

Primary material inspected on 2026-09-20; section/algorithm anchors make the comparison falsifiable. Absence from these inspected sections is not evidence of absence from all literature.

| Primary source and inspected locus | Established mechanism | Boundary of this candidate's claim |
| --- | --- | --- |
| [Blondel et al., Louvain author page and original-paper description](https://perso.uclouvain.be/vincent.blondel/research/louvain.html) | Greedy local moves and graph aggregation; modularity optimization is not an exact global optimizer | Objective and multilevel foundation are prior art |
| [Lu, Halappanavar and Kalyanaraman, 2014, section V-B and Lemma 1](https://eecs.wsu.edu/~ananth/papers/Lu_MTAAP14.pdf) | Vertex following for single-neighbor vertices; simultaneous community updates can lose modularity | CSCJ is not vertex following or stale parallel moves; it certifies a particular consecutive serial trace for arbitrary certified twin degree |
| [Traag, Waltman and van Eck, Leiden, section III and Appendices A/D](https://arxiv.org/html/1810.08473v3) | Fast local moving, constrained refinement, aggregation; distinct per-iteration/stable/asymptotic guarantees | None of the stronger Leiden guarantees is borrowed for CSCJ |
| [Yu et al., VLouvain, EDBT 2026, sections 4, 5.1-5.4, Algorithms 1-3](https://openproceedings.org/2026/conf/edbt/paper-72.pdf) | Community-vector sufficient statistics, no explicit dense graph, vector aggregation; also batched no-mover checks; stated first-level cost `O(n^2 d)` and memory `O(nd)` | Particularly close art. Vectorized gains, omitted edges and batched stability checks are not new here. The proposed delta is positive-move run collapse with an exact crossover and split-capable identity trace |
| [Cordasco, Gargano and Rescigno, FUN 2021 proceedings, section 3](https://drops.dagstuhl.de/storage/00lipics/lipics-vol157-fun2021/LIPIcs.FUN.2021.21/LIPIcs.FUN.2021.21.pdf) | Neighborhood-diversity type partitions and compact type graphs | The structural equivalence itself is known; it is not a new community objective or permission to force twins into one community |
| [Karande, Chellapilla and Andersen, 2009, sections 2-3](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf) | Direct algorithm execution on virtual-node compressed graphs and compressed-size matrix-vector multiplication | Eliminating expanded edges alone cannot support an originality claim |
| [Francisco et al., 2022, Proposition 1, vertical slicing and biclique sections](https://koeppl.github.io/bin/paper/sncs22graph.pdf) | Reuse of referenced-row products, signed differences and biclique plus residual multiplication | Reused partial computation and signed residuals are established; CSCJ targets nonlinear serial decision events instead of just operator application |
| Matched type-cached scalar, derived comparator in the independent review | One cache per type block; source correction, two score-key updates, token-aware extrema; neighbor-pruned scalar is another valid baseline | The revised probe implements this comparator. CSCJ is credited only with replacing repeated positive decisions/updates by one certified event, not with cache construction or omitted edges |

VLouvain's PDF prose and algorithm transcription have sign/index details that deserve direct code verification before a parity experiment. This note uses an independently derived, explicitly signed objective and does not assert a bug in that implementation from PDF extraction alone.

## Experiments That Can Reject The Thesis

1. Scale the executed exact-objective / matched cached-scalar / cached-CSCJ comparison with identical starts/order/ties. Add the proved neighbor-pruned scalar baseline; separate first-sweep, later-sweep and warm-start work. Require event-boundary agreement.
2. Exhaustively extend to weighted four/five-vertex graphs, multiple sweeps, empty sources, newly created singleton tokens, isolates and u128-boundary arithmetic. Inject an invalid type certificate and require rejection before the first jump.
3. Sweep cohort length, type count, number of occupied type-community cells, initial interval fragmentation and positive/negative `beta`. Include random graphs, exact clones and near-clones with adversarial defects.
4. Compare community quality against pinned Louvain and Leiden separately from trace equivalence. Report `Q`, connectedness, coverage, seed sensitivity and downstream usefulness; no arbitrary partition equality to GDS.
5. On an actual 4 GB machine, measure source-to-full-output and refresh, peak physical memory, unique pinned bytes, total peak disk, bytes read/written, event count, eliminated moves and failed admissions. Include a stalled output consumer and mid-event crash.
6. Pre-register a whole-job win threshold with the lead. Kill the research-contribution claim if event compression rarely exceeds its discovery/index/scan cost, or an existing exact symmetry-aware local-moving implementation already supplies the same crossover rule. A useful integration can survive without a novelty claim.

## Final Synthesis And Remaining Gaps

The strongest candidate remains the affine **moving-run** certificate, now over an actual two-update cache and compared with the same competent scalar cache. New tests cover compressed counts, heap/streamed extrema, exact division, interval edits, budget prefixes and every event boundary. Default singleton first-sweep compression is categorically impossible; the warm clique shows a real event-work separation but retains input/output floors. Remaining gaps are useful naturally occurring run lengths, paid type discovery, external index/recovery implementation, production numeric admission and broader novelty search. No physical 4 GB, end-to-end speed or publication-readiness claim is established.

The subsequent [natural singleton crossover study](Communities-Natural-Crossover.md) proves a weighted three-type family from the ordinary singleton start: exactly three sweeps and a sweep-two partial moving prefix `ceil((75b-39)/100)` on `4b` vertices. Its author-executed comparison covers nine families, 27 full trajectories and 17,879 full-label boundary checks, including output serialization and a cached scalar comparator. At 4,096 vertices cached score updates fall from 9,724 to 8,190, but ordinary pairwise integer quadratic line search matches the batched trajectory and uses one fewer division. The largest run is not the whole-workflow speedup: total moving-event savings approach only 3/19, and singleton preparation/output remain linear. This supplies weighted multisweep evidence for one engineered family, not general weighted coverage, workload prevalence or a paper-worthy separation.

Its [matching-defect extension](Communities-Natural-Crossover.md#bounded-extension-matching-defects-force-fragmentation) then breaks the pure quadratic pattern with one exceptional neighbor per Y vertex. A pair-freezing theorem determines the remaining move count from a prefix-cut matching count; a counting argument proves some matchings force Omega(n) final ID-order runs and contiguous moving events. This rules out a uniform sublinear guarantee for the chosen interval representation, not for all possible encodings or algorithms. A same-input delete-only successor baseline uses established union-find to process the one-time pair completions in almost-linear total work. The author executed 108 new trajectories, 3,600 expanded-graph gain checks and 215 schedule checks; the lead inspected the proofs/receipt without rerunning prior suites. Defect degree one is not a promise of sparse total defects or few label runs.
