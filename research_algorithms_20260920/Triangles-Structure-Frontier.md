# Triangles Structure Frontier: Local Adjoints Of Certified Contractions

Date: 2026-09-20. Bounded follow-on research, not an A07 promotion. This task writes only this file; it makes no edits to existing A07, community notes, evidence, reviews or portfolio files.

## Premise And Decision

**Candidate tested:** a width-certified contraction frontier computes the weighted triangle polynomial; an eventwise reverse pass recovers every exact local triangle count without retaining an arithmetic-operation tape. This handles nonuniform blocks, not just twins, and is materially different from A07's fixed-template defect diagonal.

**Decision:** useful derived exact algorithm, but **no strict gain over the strongest matched baseline established**. It saves state relative to an unfused automatic-differentiation tape, an inadequate comparator. A checkpointed/fused version of the same known contraction algorithm has the same bounds. On the explicit twin-free separating-family attempt below, a certificate-aware sparse baseline is asymptotically faster. Do not call this a new replacement A07 algorithm.

The new work here is the explicit weighted invariant, unambiguous transition enumerator, two-pass local-count procedure, retained executable falsification model, and the failed-separation analysis. The next unproved mechanism is specified at the end; it is not presented as a result.

Expert lenses: exact graph counting; arithmetic-circuit differentiation; external-memory lifetimes; adversarial same-input comparison. Adaptive ordinary module/twin refinement was not pursued as the main candidate because both modular and heterogeneous structural triangle algorithms already exist.

## Exact Semantics

The graph is a fixed simple undirected snapshot `G=(V,E)` with stable original IDs. Apply declared filters, symmetrize by unordered endpoint pair, discard self-loops, deduplicate parallel/reciprocal records and retain isolates. Weights and parallel multiplicities do not become triangle weights. Feature-derived adjacency means Boolean OR, not a shared-feature Gram count; its exact projection/discovery costs remain payable.

Return for every vertex `v`: degree `d_v`, simple unordered triangle count `t_v`, and exact local clustering `t_v/choose(d_v,2)` when `d_v>=2`, otherwise zero. Also return `T=sum_v t_v/3`. No approximate answers, triangle listing, GDS partition guarantee or omitted zero rows.

Introduce formal vertex variables only for computation:

```text
P(x) = sum_{unordered triangles {i,j,k}} x_i*x_j*x_k
t_v  = (partial P / partial x_v)(1,...,1)
```

These variables do not change graph semantics. They expose where a global count depends on each original vertex. All calculations at one use exact integers; no floating AD or finite differences.

## Certificate And Frontier State

Supply a binary contraction sequence over the vertex IDs: each step replaces two active disjoint bags `U,V` by `W=U union V`. Verify that the sequence covers every vertex exactly once at its leaves and ends in one bag. Between active bags, maintain one of:

- `0`: no graph edges across the pair;
- `B`: every possible cross pair is an edge;
- `R`: neither empty nor complete, represented as an unresolved nonuniform pair.

For contraction purposes red also remains red when a red pair is merged with another pair. A bag's red neighbors form `N_R(X)`. The supplied width bound `d` must hold for **every active bag after every step**, not just the newly created bag. Bag types are certificates of current cross-pair structure; they need not be twin classes or internally uniform.

Topology is independent of `x`. For each active bag and red pair retain the following polynomials through their numeric values and dependency recipes:

```text
S_X  = sum_{i in X} x_i
M_X  = sum_{{i,j} in E, i,j in X} x_i*x_j
C_XY = sum_{{i,j} in E, i in X,j in Y} x_i*x_j   # only for red pairs
cross(X,Y) = 0 / S_X*S_Y / C_XY, for color 0 / B / R
S_W  = S_U+S_V
M_W  = M_U+M_V+cross(U,V)
C_WX = cross(U,X)+cross(V,X), for newly red W-X
```

Singletons initialize `S_{v}=x_v,M_{v}=0`. Each new auxiliary value has a constant-size sum-of-products recipe. No vector of one entry per original vertex is attached to a bag. That vector would destroy the proposed state bound.

A sequence alone is not an authoritative compressed graph: many graphs share a sequence. This profile consumes a normalized explicit graph and validates colors by exact pair updates. Avoiding the initial `Omega(m)` input read requires an independently certified authoritative symbolic graph; it is not assumed here. Failed width admission yields a refusal/replan, never an approximate contraction.

## Weighted Invariant And Ownership

Call a triangle unresolved at an active frontier if it has either:

1. three bags with three black pair relations;
2. three bags with exactly one red and two black relations;
3. two vertices in one bag and the third in a black-adjacent bag.

All other triangles are resolved. Once resolved, a triangle stays resolved under further contractions. A previously red relation never becomes black, and merging bags cannot increase the number of bags containing that triangle.

Let `Q` accumulate the polynomial of triangles at their first transition to resolved. Then

```text
P = Q
    + sum_{black bag triangles X,Y,Z} S_X*S_Y*S_Z
    + sum_{red XY, black XZ,YZ} C_XY*S_Z
    + sum_{black unordered XY} (M_X*S_Y + M_Y*S_X).
```

Each sum uses unordered configurations once. This is a **weighted extension of the known contraction invariant**, not a new global-counting principle. Initially `Q=0`; at the one-bag frontier no unresolved terms remain, so `Q=P`.

### Explicit Transition Generator

For a step `U,V -> W`, let `R=N_R(W)` after the color update, but evaluate all factors on the **old** frontier. The following rules emit monomials of `Delta Q`, exactly once. `c` runs over `U,V` and `other` denotes the other child.

| Owner configuration | Emitted monomial |
| --- | --- |
| `UV=B` | `S_U*M_V + S_V*M_U` |
| `X in R`, `cX=B` | `S_c*M_X + S_X*M_c` |
| Previous row plus `UV=B`, `other-X=R` | `S_c*C_other,X` |
| `X in R`, `Y in N_R(X)`, `WY=B`, `cX=B` | `S_c*C_XY` |
| Unordered `X<Y` in `R`, `XY=B`, `cX=cY=B` | `S_c*S_X*S_Y` |
| Unordered `X<Y` in `R`, `XY=B`, `cX=R,cY=B` | `C_cX*S_Y` |
| Unordered `X<Y` in `R`, `XY=B`, `cX=B,cY=R` | `C_cY*S_X` |
| Unordered `X<Y` in `R`, `XY=R`, `cX=cY=B` | `S_c*C_XY` |

The fourth row has exactly one of `WX,WY` red, so it is not a duplicate of the last four. In the last four rows use unordered pairs; iterating both orientations doubles counts. A white necessary pair emits nothing. This table is derived from the ownership invariant rather than copied from a printed implementation.

At a step with `r=|R|`, its test budget is

```text
w_i = O(1+r+r^2 + sum_{X in R} |N_R(X)|) = O(1+d^2).
```

Only red rows and color lookups are needed for these rules. Creating the new colored adjacency still reads/updates its black as well as red neighbors; this is separately charged.

## Eventwise Local-Count Algorithm

Retain only auxiliary-value records, constant-size recipes, and a reversible topology-change journal. Stream and discard the monomials emitted for `Delta Q`; **do not store their multiplication/addition gates**. Recreate them in the reverse pass.

```text
count_local_frontier_adjoints(graph, sequence, width_cap, resource_caps):
    normalize graph, IDs and degree stream; reserve full stage lifetimes
    validate sequence topology/width as contractions execute
    for each contraction U,V -> W:
        stream transition monomials; evaluate and add them to Q
        create S_W, M_W and C_WX from constant-size recipes
        journal removed/created colored pairs and variable IDs
        apply contraction; refuse before publishing if a cap fails
    initialize auxiliary adjoints to zero on a bounded external index
    for each contraction in reverse order:
        restore old topology using its journal
        regenerate its transition monomials and differentiate them with seed 1
        reverse the new auxiliary recipes, using their accumulated adjoints
        retire output-variable adjoints after propagating them to old variables
    join singleton-S adjoints to original IDs and the saved degree stream
    check nonnegativity, t_v<=choose(d_v,2), and sum(t_v)=3Q
    stream complete exact local answers and publish snapshot manifest
```

For a product `z1*...*zk`, `k<=3`, accumulate `seed*product_{j!=i} z_j` into each factor's adjoint. This requires at most three primal reads and three indexed additions; it does not divide by a factor, so zero weights do not break differentiation. A sum distributes its seed. Numeric value zero must not be confused with a constant-zero polynomial.

The reverse step must differentiate both the emitted monomials and the auxiliary recipes. Differentiating just bag sizes or distributing bag totals uniformly is incorrect. The adjoint of the running `Q` is always one, so its successive accumulation states require no saved numeric history.

### Theorem And Proof

**Conditional exact-local theorem.** Given a verified sequence, exact arithmetic and the above ownership rules, the algorithm returns every exact local count. It retains `O(n+sum_i r_i)` auxiliary records, not one record per emitted arithmetic operation. With a reversible colored topology implementation, its counting/differentiation kernel is `O(n+sum_i w_i)`; topology, indexes, preparation and output are additional costs below.

Proof: color cases partition the unresolved triangle configurations. The table accounts for each configuration precisely when it ceases to be unresolved; a resolved triangle never re-enters. Replace each triangle by its monomial to obtain the same invariant over integer polynomials. Constant-size auxiliary recipes preserve their displayed meanings by disjointness of bags. Thus the forward scalar is `P(x)` for all `x`, not merely at one numeric input. The reverse product/sum rules apply the chain rule to that polynomial without changing it. Every triangle containing `v` contributes exactly one to its derivative at all ones, and no other triangle contributes. Recreating an event from restored topology and immutable old auxiliary values recreates the same operation. There are at most two bag values and `r_i` red-pair values created at step `i`; transition terms need not be saved. This proves the claims. It is an ordinary mathematical argument, not a formal proof or priority claim.

The source model below genuinely regenerates event terms and reverses their recipes. It keeps its small topology/auxiliary records in JS memory and checks width by scanning all active bags; it does **not** implement the external indexed complexity.

## Separating Family Attempt And Its Failure

Let even `d>=4`. Build `H_d` with a clique of `d` anchors and `d` mutually nonadjacent spokes. Spoke `i` connects to the cyclic interval of `r=d/2` anchors starting at `i`. Supply the clique/independent partition and the same explicit graph to every comparator. Contract all spokes in cyclic order into one bag, then contract the anchors, then the remaining pair.

This family has `n=2d`, `m=choose(d,2)+d*r=Theta(d^2)`, many triangles, and **no twin pair**: spoke neighborhoods and anchor spoke-neighbor sets are all distinct; their degrees also distinguish the two sides. It is not rescued by ordinary twin aggregation. Its displayed contraction width is at most `d`.

For a linear fraction of the spoke contractions, the merged spoke bag has `Theta(d)` red anchor neighbors while the next spoke has `Theta(d)` black anchor neighbors. The unordered red-neighbor-pair loop emits `Theta(d^2)` genuine triangle monomials in each such step. Over the sequence, a fully taped scalar AD evaluation retains `Theta(d^3)` gates; auxiliary recipes and topology changes total only `O(d^2)`. The implemented probe reports the actual terms and record counts for `d=8,16,32,64`.

This is a valid separation from **full operation taping**, but not from a competent same-input baseline. Two stronger comparators defeat it:

1. Fused/checkpointed scalar contraction plus reverse differentiation can save precisely the same auxiliary records and replay the same terminal event computations. It matches the candidate. Generic differentiation is not required to retain every elementary gate.
2. A split-certificate sparse plan validates that the anchors form a clique and spokes are independent, then computes

```text
t_spoke = choose(degree(spoke),2)
t_anchor = choose(d-1,2)
           + sum_{spoke adjacent to anchor}(degree(spoke)-1).
```

One cross-edge scan accumulates all anchor answers. Its total work is `Theta(n+m)=Theta(d^2)`, state is at most `O(n+m)` (or bounded external reductions), and it delivers the identical full output. Both receive the same partition/certificate and pay its validation. For this family the candidate is **slower by a factor Theta(d)** in counting work, not stronger. The probe implements this matched baseline and compares its entire local vector, not just the total.

Therefore **there is no surviving separating family versus the strongest comparator in this note**. The requested strict-gain criterion is not met, and no replacement novelty claim is manufactured. The positive state separation above is explicitly restricted to an inferior baseline.

## Counterexamples That Changed The Design

### A Global Bag Count Cannot Be Shared Uniformly

Take a triangle on `0,1,2` and a pendant edge `0-3`; contract `0,3` first. The correct local vector is `[1,1,1,0]`. The bag `{0,3}` is nonuniform relative to vertices 1 and 2. Assigning its total incidence one uniformly gives halves, or rounds one vertex incorrectly. The global scalar can still be correct. **Correction:** keep the polynomial dependency recipes and reverse them to original leaves. This witness is executed below.

### Zero At An Evaluation Point Does Not Mean Constant Zero

For a single triangle at vertex weights `(0,1,1)`, `P=0` but its gradient is `(1,0,0)`. Pruning a zero-valued event without an identity/structural-zero certificate loses a derivative. The probe tests zero weights specifically. Product derivatives use multiplication of the other factors rather than `product/factor`.

### Good Width Does Not Certify Affordable State Or Discovery

A claimed `d=0` sequence for a five-cycle is invalid. The probe requires rejection. More generally, small width bounds red state, not dense black adjacency, graph input size, discovery effort or old/new disk overlap. All remain charged. A sequence selected using numerical `x` values would invalidate the fixed-polynomial differentiation argument; this candidate requires graph-only branching.

## RAM, I/O And Lifecycle

Use decimal ceilings: physical RAM `4,000,000,000` bytes and retained prepared storage `50,000,000,000` bytes. A provisional worker reservation is 3 GB with 1 GB for OS/other processes, not a measured guarantee. For the strict experiment impose the same 50 GB ceiling on **peak live disk**, including local input, all scratch and pinned generations; otherwise disclose a larger workspace separately.

Let `D` be the auxiliary-record count, `J` the number of topology-change records, `Qmax` the maximum nonwhite pair count, `W=sum_i w_i`, `b` the I/O page size and `F=b/record_width` an index fanout. A valid width-`d` sequence has `D=O(n(d+1))`. A merge-join adjacency implementation amortizes common black-edge removals and red changes, giving `J=O(m+nd)`; the proof charges a common black neighbor to one disappearing edge and remaining exceptional neighbors to red width. Reading empty/nonadjacent pairs by scanning all active bags instead loses that bound.

```text
CPU_kernel = O(n+W) = O(n*(1+d^2))
CPU_whole  = CPU_normalize_discover + O((J+W+D)*log(D+Qmax+2))
             + comparison-sort CPU + Theta(n_output)
IO_bytes   <= Sort(input_bytes) + required_ID/degree/output_sorts
              + O(record_width*(J+D+n))
              + O(b*(W+D+J)*log_F(D+Qmax+2))
```

The indexed expression is a deliberately pessimistic B-tree plan, not an optimal external-memory bound. It pays lookup and adjoint updates separately from arithmetic. Resident indexes may remove many misses, but cannot be assumed to fit. `W` can be cubic; bad sequences do not produce useful deadlines. Compare before admission with A07, sparse intersections and certificate-specific plans, including the split plan above.

Use a bounded merge buffer for adjacency updates, bounded index page caches for colors/values/adjoints, and fixed tiles/cursors for red-pair enumeration. If a red list exceeds its reservation, store it as a journal range and replay only that range for pair tiles; do not allocate `d^2` monomials or an `n`-vector in RAM. Enumerating one degree-three term needs constant arithmetic temporaries. The full recipe/value/journal sets are external state, not exempt from disk accounting.

```text
R_worker = runtime + adjacency_merge + topology_cache + value_adjoint_cache
           + red_tiles + sort_buffers + output_buffers + WAL <= 3 GB
S_work   = colored_pair_indexes + change_journal
           + auxiliary_values_recipes_adjoints + normalized_ID_degree_tapes
           + sort_runs + WAL + unpublished_output
S_live   = unique pinned input/prepared/answer generations + S_work
```

Illustrative **reservation widths**, not measured layouts: two directed/indexed color orders can require `64*Qmax` bytes before B-tree occupancy overhead; reserve `48*J` bytes for topology journal records and `96*D` bytes for auxiliary values, adjoints and recipes. Real fragmentation/index duplication adds a multiplier. These are state terms, not just input-byte estimates. A full output `(id:u64,degree:u64,t:u128,LCC_denominator:u128)` costs `48n` bytes: 4.8 GB at 100M vertices. At `n=100M,d=64`, even the kernel bound is about `4.1e11` width-squared units before discovery, indexes or output; no latency or capacity success follows from the theorem.

**Preparation:** canonicalize source pairs and IDs, compute degrees by external reduction, load a supplied sequence and validate every transition. Charge source parsing, sorted pair/index construction, journal creation and every refused partial build. Finding a small-width sequence is not supplied by this algorithm. Keep the graph-only sequence and the exact projection/filter identity in the manifest.

**Refresh:** an edge change can alter colors, ownership events and red width all the way along ancestor contractions. Conservatively rerun validation and both passes on a new immutable snapshot. Reusing old adjoints is not sound. Old/new graph, journal, recipes and full output can coexist; reserve their live union before starting. No `O(batch size)` refresh guarantee is claimed. A local cone-of-influence repair would be another algorithm requiring proof.

**Recovery/output:** persist forward/reverse phase, event cursor, immutable recipe generation and accumulator checkpoint consistently. Journal the accumulator changes before checkpoint advancement; replay exactly once or restart the uncommitted phase. Use backpressure on the output stream; a stalled reader pins its snapshot. If a cap cannot be met, stop/refuse rather than publish a partial local vector. Retire query-only journals/recipes after successful output publication unless explicitly retained for another run.

**Integer widths:** at all ones, primal bag edge sums are at most `choose(n,2)` and the final scalar is at most `choose(n,3)`. Admit a concrete bound for intermediate products and adjoints before choosing fixed-width arithmetic. The probe uses BigInt; it is not evidence of checked i128 production code or physical 4 GB operation.

## Narrow Primary-Art Check

Sources were opened and inspected on 2026-09-20. The comparisons below, not search-result absence, determine the claim boundary.

| Primary source / inspected locus | Collision and consequence |
| --- | --- |
| [Kratsch, Nelles, Simon, On Triangle Counting Parameterized by Twin-Width, 2022, section 3, invariant and final complexity proof](https://arxiv.org/pdf/2202.06708) | Supplies the scalar contraction frontier, bag/edge statistics and `O(d^2 n+m)` result given a sequence. These are prior art. This note weights that invariant and makes a local-adjoint schedule explicit; it does not claim a new scalar counter. |
| [Baur and Strassen, The Complexity of Partial Derivatives, 1983, Theorems 1-2](https://web.vu.lt/mif/s.jukna/tropical/Baur-Strassen.pdf) | All first derivatives of an arithmetic computation can be evaluated with constant-factor arithmetic overhead. Turning a triangle polynomial into local counts cannot itself be a novelty claim. The theorem does not waive storage/I/O. |
| [Stumm and Walther, New Algorithms for Optimal Online Checkpointing, 2010, introduction and checkpointing model](https://tu-dresden.de/mn/math/wir/ressourcen/dateien/forschung/publikationen/pdf2010/new_algorithms_for_optimal_online_checkpointing.pdf) | Reverse differentiation storage/recomputation tradeoffs are established. Event fusion/rematerialization is a comparator capability, not a new graph certificate. The original Revolve DOI was located but its publisher page did not load in this inspection; no claims depend on unseen text there. |
| [Kratsch and Nelles, heterogeneous structural graphs, 2022, section 5.1, Theorem 7 and Lemmas 7-9](https://arxiv.org/pdf/2209.14429) | Mixed structural expressions already support exact triangle algorithms combining parameter regimes. Merely choosing different known kernels in different modules is insufficient differentiation. |

The split baseline is explicitly derived above, so its correctness does not depend on a literature search proving priority. Neither this bounded search nor the absence of an exact named local-adjoint implementation establishes novelty. A manually fused strongest baseline already defeats the proposed state claim.

## Runnable Falsification Source

From the repository root, Node.js, no dependencies or new files:

```sh
awk '/^```javascript$/{active=1;next} /^```$/{if(active){active=0;exit}} active' research_algorithms_20260920/Triangles-Structure-Frontier.md | node
```

The candidate below stores only constant-size auxiliary recipes plus local topology undo records. Transition terms are generated twice, never retained between passes. The independent oracle enumerates original vertex triples. The model also constructs the split baseline, checks the twin-free family, tests non-unit/zero weights, and refuses an invalid width claim. It is an arithmetic/state-structure probe, not an external execution benchmark.

```javascript
"use strict";
const assert = require("node:assert/strict");

function create_simple_graph_matrix(n, edges) {
  const A = Array.from({ length: n }, () => Array(n).fill(0));
  for (const [u, v] of edges) {
    assert(u !== v && !A[u][v], "canonical simple pairs required");
    A[u][v] = A[v][u] = 1;
  }
  return A;
}

function enumerate_triangle_gradient_oracle(A, weights) {
  const local = Array(A.length).fill(0n); let total = 0n;
  for (let i = 0; i < A.length; i++) for (let j = i + 1; j < A.length; j++)
  for (let k = j + 1; k < A.length; k++) if (A[i][j] && A[i][k] && A[j][k]) {
    total += weights[i] * weights[j] * weights[k];
    local[i] += weights[j] * weights[k]; local[j] += weights[i] * weights[k];
    local[k] += weights[i] * weights[j];
  }
  return { total, local };
}

function create_index_contraction_sequence(n, mode) {
  const active = Array.from({ length: n }, (_, i) => i), sequence = [];
  let next = n;
  while (active.length > 1) {
    const i = mode === 1 ? active.length - 1 : 0;
    const u = active.splice(i, 1)[0];
    const j = mode === 2 ? Math.floor(active.length / 2) : 0;
    const v = active.splice(j, 1)[0]; sequence.push([u, v]);
    if (mode === 0) active.unshift(next++); else active.push(next++);
  }
  return sequence;
}

function execute_frontier_local_adjoints(A, sequence, weights, widthCap = A.length) {
  const n = A.length, adj = new Map(), S = new Map(), M = new Map();
  const values = [], recipes = [], bars = [], journal = [];
  let total = 0n, forwardTerms = 0, reverseTerms = 0, topologyRecords = 0, width = 0;
  const stats = { pairTests: 0, recipeFactors: 0 };
  const product = factors => factors.reduce((value, id) => value * values[id], 1n);
  const create = terms => {
    terms = terms.filter(term => !term.includes(-1));
    if (!terms.length) return -1;
    const id = values.length;
    values.push(terms.reduce((sum, term) => sum + product(term), 0n));
    recipes.push(terms); bars.push(0n);
    stats.recipeFactors += terms.reduce((sum, term) => sum + term.length, 0);
    return id;
  };
  for (let i = 0; i < n; i++) {
    adj.set(i, new Map()); S.set(i, values.length); M.set(i, -1);
    values.push(weights[i]); recipes.push(null); bars.push(0n);
  }
  const put = (u, v, edge) => { adj.get(u).set(v, edge); adj.get(v).set(u, edge); };
  const erase = (u, v) => { adj.get(u).delete(v); adj.get(v).delete(u); };
  const color = (u, v) => adj.get(u)?.get(v)?.color || 0;
  const redValue = (u, v) => { assert.equal(color(u, v), 2); return adj.get(u).get(v).id; };
  const redNeighbors = u => [...adj.get(u)].filter(([, edge]) => edge.color === 2).map(([v]) => v);
  for (let u = 0; u < n; u++) for (let v = u + 1; v < n; v++) if (A[u][v]) put(u, v, { color: 1, id: -1 });
  const mergedColors = (u, v) => {
    const result = new Map();
    for (const x of new Set([...adj.get(u).keys(), ...adj.get(v).keys()])) if (x !== u && x !== v) {
      const a = color(u, x), b = color(v, x); result.set(x, a === 1 && b === 1 ? 1 : 2);
    }
    return result;
  };
  const cross = (u, v) => color(u, v) === 1 ? [[S.get(u), S.get(v)]]
    : color(u, v) === 2 ? [[redValue(u, v)]] : [];
  const emit = (u, v, colors, sink) => {
    const term = ids => { if (!ids.includes(-1)) sink(ids); };
    const R = [...colors].filter(([, c]) => c === 2).map(([x]) => x).sort((a, b) => a - b);
    if (color(u, v) === 1) { term([S.get(u), M.get(v)]); term([S.get(v), M.get(u)]); }
    for (const x of R) {
      for (const [c, other] of [[u, v], [v, u]]) if (color(c, x) === 1) {
        term([S.get(c), M.get(x)]); term([S.get(x), M.get(c)]);
        if (color(u, v) === 1 && color(other, x) === 2) term([S.get(c), redValue(other, x)]);
      }
      for (const y of redNeighbors(x)) {
        stats.pairTests++;
        if (colors.get(y) !== 1) continue;
        for (const c of [u, v]) if (color(c, x) === 1) term([S.get(c), redValue(x, y)]);
      }
    }
    for (let i = 0; i < R.length; i++) for (let j = i + 1; j < R.length; j++) {
      stats.pairTests++; const x = R[i], y = R[j], xy = color(x, y);
      for (const c of [u, v]) {
        const cx = color(c, x), cy = color(c, y);
        if (xy === 1) {
          if (cx === 1 && cy === 1) term([S.get(c), S.get(x), S.get(y)]);
          else if (cx === 2 && cy === 1) term([redValue(c, x), S.get(y)]);
          else if (cx === 1 && cy === 2) term([redValue(c, y), S.get(x)]);
        } else if (xy === 2 && cx === 1 && cy === 1) term([S.get(c), redValue(x, y)]);
      }
    }
  };
  assert.equal(sequence.length, Math.max(0, n - 1));
  for (let step = 0; step < sequence.length; step++) {
    const [u, v] = sequence[step], w = n + step;
    assert(u !== v && adj.has(u) && adj.has(v));
    const colors = mergedColors(u, v), createdIds = [], oldEdges = [];
    emit(u, v, colors, ids => { forwardTerms++; total += product(ids); });
    const record = terms => { const id = create(terms); if (id !== -1) createdIds.push(id); return id; };
    S.set(w, record([[S.get(u)], [S.get(v)]]));
    M.set(w, record([[M.get(u)], [M.get(v)], ...cross(u, v)]));
    const newEdges = [...colors].map(([x, c]) => [x, { color: c, id: c === 2 ? record([...cross(u, x), ...cross(v, x)]) : -1 }]);
    for (const c of [u, v]) for (const [x, edge] of adj.get(c)) {
      if (c === v && x === u) continue;
      oldEdges.push([c, x, edge]);
    }
    for (const [a, b] of oldEdges) erase(a, b);
    adj.delete(u); adj.delete(v); adj.set(w, new Map());
    for (const [x, edge] of newEdges) put(w, x, edge);
    for (const x of adj.keys()) {
      const red = redNeighbors(x).length; width = Math.max(width, red);
      if (red > widthCap) throw Error("width certificate refused");
    }
    topologyRecords += oldEdges.length + newEdges.length;
    journal.push({ u, v, w, oldEdges, newEdges, createdIds });
  }
  const derivative = (factors, seed) => {
    for (let i = 0; i < factors.length; i++) {
      let value = seed;
      for (let j = 0; j < factors.length; j++) if (j !== i) value *= values[factors[j]];
      bars[factors[i]] += value;
    }
  };
  for (let step = journal.length - 1; step >= 0; step--) {
    const { u, v, w, oldEdges, newEdges, createdIds } = journal[step];
    for (const [x] of newEdges) erase(w, x);
    adj.delete(w); adj.set(u, new Map()); adj.set(v, new Map());
    for (const [a, b, edge] of oldEdges) put(a, b, edge);
    const colors = mergedColors(u, v);
    emit(u, v, colors, ids => { reverseTerms++; derivative(ids, 1n); });
    for (const id of [...createdIds].reverse()) {
      for (const term of recipes[id]) derivative(term, bars[id]);
      bars[id] = 0n;
    }
  }
  assert.equal(forwardTerms, reverseTerms);
  for (let u = 0; u < n; u++) for (let v = 0; v < n; v++) assert.equal(color(u, v), A[u][v], "topology undo");
  return { total, local: Array.from({ length: n }, (_, v) => bars[S.get(v)]),
    stats: { vertices: n, width, forwardTerms, reverseTerms, auxiliaryRecords: values.length,
      topologyRecords, ...stats, persistedTransitionTerms: 0 } };
}

function verify_exhaustive_frontier_cases() {
  const n = 5, pairs = [];
  for (let u = 0; u < n; u++) for (let v = u + 1; v < n; v++) pairs.push([u, v]);
  let cases = 0, localChecks = 0, maxWidth = 0;
  for (let mask = 0; mask < 1024; mask++) {
    const A = create_simple_graph_matrix(n, pairs.filter((_, i) => (mask >> i) & 1));
    for (let mode = 0; mode < 3; mode++) for (const weights of [Array(n).fill(1n), [0n, 2n, 1n, 3n, 1n]]) {
      const result = execute_frontier_local_adjoints(A, create_index_contraction_sequence(n, mode), weights);
      const oracle = enumerate_triangle_gradient_oracle(A, weights);
      assert.deepEqual([result.total, result.local], [oracle.total, oracle.local]);
      cases++; localChecks += n; maxWidth = Math.max(maxWidth, result.stats.width);
    }
  }
  const toy = create_simple_graph_matrix(4, [[0, 1], [0, 2], [1, 2], [0, 3]]);
  const toyResult = execute_frontier_local_adjoints(toy, [[0, 3], [1, 2], [4, 5]], Array(4).fill(1n));
  assert.deepEqual(toyResult.local, [1n, 1n, 1n, 0n]);
  const triangle = create_simple_graph_matrix(3, [[0, 1], [0, 2], [1, 2]]);
  const zero = execute_frontier_local_adjoints(triangle, [[0, 1], [2, 3]], [0n, 1n, 1n]);
  assert.deepEqual([zero.total, zero.local], [0n, [1n, 0n, 0n]]);
  const cycle = create_simple_graph_matrix(5, [[0, 1], [1, 2], [2, 3], [3, 4], [0, 4]]);
  assert.throws(() => execute_frontier_local_adjoints(cycle, create_index_contraction_sequence(5, 0), Array(5).fill(1n), 0), /width certificate refused/);
  return { cases, localChecks, maxWidth, counterexamples: 3, mismatches: 0 };
}

function verify_separation_family_baseline() {
  const results = [];
  for (const d of [8, 16, 32, 64]) {
    const edges = [], r = d / 2;
    for (let a = 0; a < d; a++) for (let b = a + 1; b < d; b++) edges.push([a, b]);
    for (let i = 0; i < d; i++) for (let j = 0; j < r; j++) edges.push([d + i, (i + j) % d]);
    const A = create_simple_graph_matrix(2 * d, edges), sequence = []; let next = 2 * d, bag = d;
    for (let v = d + 1; v < 2 * d; v++) { sequence.push([bag, v]); bag = next++; }
    const spokeBag = bag; bag = 0;
    for (let v = 1; v < d; v++) { sequence.push([bag, v]); bag = next++; }
    sequence.push([spokeBag, bag]);
    let twins = 0;
    for (let u = 0; u < 2 * d; u++) for (let v = u + 1; v < 2 * d; v++) {
      if (A.every((_, x) => x === u || x === v || A[u][x] === A[v][x])) twins++;
    }
    assert.equal(twins, 0);
    const candidate = execute_frontier_local_adjoints(A, sequence, Array(2 * d).fill(1n), d);
    // Matched sparse split-certificate baseline, including pair validation.
    const local = Array(2 * d).fill(0n), degrees = Array(d).fill(0); let crossVisits = 0;
    for (let a = 0; a < d; a++) for (let b = a + 1; b < d; b++) assert.equal(A[a][b], 1);
    for (let a = d; a < 2 * d; a++) for (let b = a + 1; b < 2 * d; b++) assert.equal(A[a][b], 0);
    for (const [u, v] of edges) if (u >= d) { degrees[u - d]++; crossVisits++; }
    for (let a = 0; a < d; a++) local[a] = BigInt((d - 1) * (d - 2) / 2);
    for (let i = 0; i < d; i++) local[d + i] = BigInt(degrees[i] * (degrees[i] - 1) / 2);
    for (const [u, v] of edges) if (u >= d) { local[v] += BigInt(degrees[u - d] - 1); crossVisits++; }
    assert.deepEqual(candidate.local, local);
    const oracle = enumerate_triangle_gradient_oracle(A, Array(2 * d).fill(1n));
    assert.deepEqual([candidate.total, candidate.local], [oracle.total, oracle.local]);
    results.push({ d, edges: edges.length, twins, ...candidate.stats, baselineCrossVisits: crossVisits });
  }
  return results;
}

console.log(JSON.stringify({ exhaustive: verify_exhaustive_frontier_cases(),
  family: verify_separation_family_baseline() }, null, 2));
```

## Executed Results

Executed the exact fenced source with Node.js v24.9.0 on 2026-09-20; process exit status **0**. This is a fresh probe of this candidate, not a rerun or reinterpretation of A07 evidence.

- Exhaustive over all 1,024 labeled simple graphs on five vertices, three specified contraction sequences and two weight vectors: **6,144 cases, 30,720 local derivative comparisons, zero mismatches**. It is not exhaustive over all contraction sequences or all weights.
- Each case compares both the scalar and complete local vector with original-vertex triple enumeration, and checks exact topology restoration. One weight vector includes zero and non-unit values.
- The pendant-triangle and zero-weight witnesses pass; the five-cycle with a false width-zero certificate is rejected. These are three additional targeted checks, outside the 6,144 cases.
- Four twin-free split-family instances agree with both the implemented sparse split baseline and the independent triple oracle. Forward and regenerated reverse term counts match.

| `d` | Edges | Forward terms | Auxiliary records | Topology records | Pair tests, both passes | Baseline cross-edge visits |
| --- | --- | --- | --- | --- | --- | --- |
| 8 | 60 | 46 | 90 | 216 | 410 | 64 |
| 16 | 248 | 350 | 278 | 912 | 3,030 | 256 |
| 32 | 1,008 | 2,750 | 942 | 3,744 | 23,086 | 1,024 |
| 64 | 4,064 | 21,886 | 3,422 | 15,168 | 179,806 | 4,096 |

Every measured family width equals `d` and every twin count is zero. Term regeneration is implemented, not inferred from the direct-sum oracle. No transition-term array survives between passes; `persistedTransitionTerms=0` describes that representation, not measured resident bytes. Auxiliary/topology counters omit JS overhead, temporary arrays, input/oracle matrices and index layouts. The naive full-tape bound is an analytical comparison; no naive-tape memory benchmark was run. Nor was a separate fused scalar-AD implementation timed: its matched schedule is the explicit equivalence argument above.

`baselineCrossVisits` counts two cross-edge passes only. It excludes clique/independent validation and scans over the full edge stream, which also cost `Theta(d^2)` on this family. Candidate pair tests and baseline cross visits are different operation classes, so their ratio is **not** a measured speedup or whole-CPU comparison. The asymptotic failure follows from the displayed formulas and emitted-term lower bound, not from treating these counters as equivalent instructions.

Exact machine-readable output, retained for equality checking on reproduction:

```json
{
  "exhaustive": {"cases": 6144, "localChecks": 30720, "maxWidth": 3, "counterexamples": 3, "mismatches": 0},
  "family": [
    {"d": 8, "edges": 60, "twins": 0, "vertices": 16, "width": 8, "forwardTerms": 46, "reverseTerms": 46, "auxiliaryRecords": 90, "topologyRecords": 216, "pairTests": 410, "recipeFactors": 154, "persistedTransitionTerms": 0, "baselineCrossVisits": 64},
    {"d": 16, "edges": 248, "twins": 0, "vertices": 32, "width": 16, "forwardTerms": 350, "reverseTerms": 350, "auxiliaryRecords": 278, "topologyRecords": 912, "pairTests": 3030, "recipeFactors": 506, "persistedTransitionTerms": 0, "baselineCrossVisits": 256},
    {"d": 32, "edges": 1008, "twins": 0, "vertices": 64, "width": 32, "forwardTerms": 2750, "reverseTerms": 2750, "auxiliaryRecords": 942, "topologyRecords": 3744, "pairTests": 23086, "recipeFactors": 1786, "persistedTransitionTerms": 0, "baselineCrossVisits": 1024},
    {"d": 64, "edges": 4064, "twins": 0, "vertices": 128, "width": 64, "forwardTerms": 21886, "reverseTerms": 21886, "auxiliaryRecords": 3422, "topologyRecords": 15168, "pairTests": 179806, "recipeFactors": 6650, "persistedTransitionTerms": 0, "baselineCrossVisits": 4096}
  ]
}
```

Remaining verification gaps are the external indexed implementation, certificate discovery, checked fixed-width arithmetic, complete physical-RAM/disk telemetry and crash/refresh execution. None is validated by these finite arithmetic probes.

## Next Actual Unexplored Mechanism

The remaining bottleneck is **certifying when a quadratic red-frontier interaction can be replaced by a small degree/edge-mass formula**, not reducing its AD tape. A precise next target is an exact boundary certificate for a *region of contractions* whose triangle polynomial can be expressed using only bag masses plus edge masses and a bounded number of boundary corrections. Such a region could be skipped in both passes, yielding work below `sum_i w_i`; checkpointing alone cannot do that.

For `H_d`, the clique/independent boundary certificate supplies exactly that elimination, so it is already a required baseline, not the proposed new result. The genuinely unresolved step is finding and validating broader **non-module, nonuniform boundary** certificates in time proportional to their description, without enumerating the same red pairs to verify them. A useful theorem would bound discovery, output-sensitive derivative state and refresh invalidation together, and exhibit a family not already handled by split/modular/known heterogeneous decomposition plans. No such theorem or separating family was established in this bounded task.

This document therefore closes with a tested negative research result: global-to-local adjoints and changed-value replay are not sufficient differentiation. Existing A07 and its evidence remain untouched.
