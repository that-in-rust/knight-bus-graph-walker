# Semiring Matrix Traversal — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `semiring-matrix-traversal-ascii.md` / `semiring-matrix-traversal-mermaid.md` |
| One-line job | Replace the traversal loop with sparse matrix-vector multiply over a custom (+,x) algebra — pick the semiring and one kernel becomes BFS, SSSP, reachability, or triangle counting |

## 1. The job

Every frontier step in pattern 8 has the same shape: combine each
frontier entry with the edge that leaves it, then reduce all
candidates arriving at the same vertex. That IS a matrix-vector
multiply — if you let "+" and "x" mean something other than
arithmetic:

```text
ordinary SpMV:   y[v] = SUM_u ( q[u] * A[u][v] )
generalized:     y[v] = ADD_u ( MUL(q[u], A[u][v]) )   over semiring
                                                        (ADD, MUL)
```

A semiring supplies: an ADD monoid (associative, with identity) and a
MUL operator. Swap them and the same kernel computes different graph
algorithms:

```text
(MIN, PLUS)   -> shortest path relaxation (SSSP)
(MIN, FIRST)  -> BFS levels (carry the frontier's value through)
(ANY, SECONDI)-> BFS parents (SECONDI = index of the edge's source)
(ANY, PAIR)   -> plain reachability (boolean: does any edge arrive?)
(PLUS, TIMES) -> ordinary linear algebra; on A.^2 masked by A:
                 triangle counting
```

This is the GraphBLAS thesis: graphs ARE sparse matrices (the CSR
pair already showed the layouts are identical), so traversal is
algebra, and decades of sparse-kernel engineering apply for free.

## 2. Raw data shape

```text
A: |V| x |V| sparse adjacency matrix, CSR/CSC internally
   (SuiteSparse:GraphBLAS stores both orientations as needed)
q: sparse vector = the frontier      (only active entries stored)
mask: sparse/dense vector = visited  (computed entries suppressed)

one BFS step, exactly as LAGraph writes it:
    q'{!mask} = q' * A         over the chosen semiring
i.e.  GrB_vxm(q, mask, NULL, semiring, q, A, GrB_DESC_RSC)
      (LG_BreadthFirstSearch_SSGrB_template.c:305-307 — the comment
       reads "push (saxpy-based vxm)")
```

The mask is the visited set: GraphBLAS fuses "don't revisit" INTO the
multiply instead of filtering afterwards — the descriptor `RSC`
(replace, structural complement) says "write only where mask is
absent."

## 3. Step-by-step: BFS as four vxm calls

Graph: 0->1, 0->3, 1->2, 3->2, 2->4. Source 0. Semiring (MIN, FIRST),
q carries level numbers:

```text
level 0: q = {0:0}          visited = {0}
level 1: q' = q*A masked    -> {1:1, 3:1}       visited += {1,3}
level 2: q' = q*A masked    -> {2:2}            (both 1 and 3 hit 2;
                                MIN-reduce collapses duplicates —
                                the semiring does pattern 8's
                                "claim" without atomics in user code)
level 3: q' = q*A masked    -> {4:3}
level 4: q' empty           -> done
```

For parent BFS, swap in `GxB_ANY_SECONDI`: MUL returns the *index* of
the frontier vertex the edge came from, ADD takes ANY winner —
exactly BFS's "any parent is fine" semantics
(LG_BreadthFirstSearch_SSGrB_template.c:140-143).

## 4. The witnesses

```text
SuiteSparse:GraphBLAS  the kernel engine: 1000s of (semiring x
                       storage x mask) kernel variants, JIT-compiled;
                       also does pattern 8's push/pull switch
                       INTERNALLY (vxm picks saxpy vs dot form by
                       frontier density)
LAGraph                the algorithm layer: BFS, SSSP (min_plus,
                       LAGr_SingleSourceShortestPath.c:151-226),
                       PageRank, Betweenness, TriangleCount,
                       CC (LG_CC_FastSV6.c) — each a page of algebra
                       instead of a traversal engine
FalkorDB               a PRODUCTION GRAPH DATABASE on this pattern:
                       Cypher patterns compile to algebraic
                       expressions; relationship traversal is
                       Delta_mxm over GxB_ANY_PAIR_BOOL
                       (algebraic_expression_mul.c:33,49) — the
                       delta-matrix wrapper adds pattern-1-style
                       pending updates on top
```

## 5. Worked example 1 — 2-hop friends-of-friends in algebra

"friends of friends of user u, excluding u's friends and u":

```text
f   = row u of A                    (1 vxm or direct row extract)
fof = f * A  masked by !(f + {u})   (1 vxm with mask)

At |V|=100M, |friends(u)|=500, avg deg 500:
work touched = 500 rows x 500 entries = 250k multiply-adds
             = microseconds; never materializes anything bigger
             than the 250k-entry candidate vector.
FalkorDB executes exactly this shape for MATCH (u)-[:F]->()-[:F]->(w)
— the query planner emits the mask from the WHERE clause.
```

## 6. Worked example 2 — triangle counting as masked multiply

Triangles = paths of length 2 that close. With L = strictly-lower
triangle of symmetric A:

```text
C<L> = L * L'   over (PLUS, PAIR);  triangles = reduce(C) 
the mask <L> is the trick: compute ONLY entries where an edge
already exists — never materialize the (huge) full L*L'.

numbers: m = 1B edges, avg deg 20. Unmasked L*L' would produce
~ sum(deg^2) ~ 400B candidate entries (~3 TB). Masked: exactly
m = 1B probed entries. The mask is a 400x work filter.
(LAGraph ships this as LAGr_TriangleCount.c.)
```

## 7. Where graph systems inherit this

- FalkorDB proves the pattern carries a full property-graph DB: query
  planning becomes algebraic rewriting (associativity = join
  reordering) — its `algebraic_expression` module is a tiny query
  optimizer over matrix products.
- SuiteSparse's internal push/pull choice means pattern 8 arrives
  free to every GraphBLAS user — one abstraction level down.
- RedisGraph/FalkorDB's delta matrices bolt pattern 1 (LSM-ish
  pending deltas + periodic flatten) onto matrices — mutation strikes
  again, same answer.
- The cost: constant factors. Hand-tuned gapbs BFS beats vxm BFS on
  a single machine; the algebra wins on generality, GPU/multicore
  portability, and composition (masks fuse steps that a traversal
  engine would pipeline by hand).
- This repo: a Cypher-subset executor could compile pattern matching
  to semiring products over roaring-backed boolean matrices — the
  differential oracle (stock Neo4j) doesn't care which execution
  strategy produced the rows.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/template/LG_BreadthFirstSearch_SSGrB_template.c` | BFS as masked vxm (305-307), ANY_SECONDI parents (140-143) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_SingleSourceShortestPath.c` | SSSP over MIN_PLUS semirings (151-226) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_TriangleCount.c` | masked-multiply triangle counting |
| GraphBLAS | `reference-repos-corpus/GraphBLAS-src` | SuiteSparse kernel engine: semiring x storage x mask variants, internal push/pull |
| falkordb | `reference-repos-competitors/falkordb-src/src/arithmetic/algebraic_expression/algebraic_expression_mul.c` | production traversal = Delta_mxm over ANY_PAIR_BOOL (33, 49) |
| falkordb | `reference-repos-competitors/falkordb-src/src/graph/delta_matrix/delta_mxm.c` | delta-matrix multiply (mutability layer) |

## 9. Cross-references

- Sibling patterns: `csr-adjacency-layout` (the matrix IS that
  layout); `frontier-pushpull-switching` (reappears inside vxm);
  `lsm-compaction-tradeoff` (delta matrices are the LSM move);
  `roaring-bitmap-idsets` (boolean matrices over compressed bitmaps).
- Next in category: connected-components hooking/shortcutting
  (FastSV lives in both worlds) and PageRank iteration structure.
- 202606 digest overlap: digests named GraphBLAS as FalkorDB's
  engine; this pair adds the semiring table, the mask semantics, and
  the masked-triangle work arithmetic.
