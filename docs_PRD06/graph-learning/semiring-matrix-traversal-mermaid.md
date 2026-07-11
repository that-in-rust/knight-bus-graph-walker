# Semiring Matrix Traversal — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `semiring-matrix-traversal-ascii.md` / `semiring-matrix-traversal-mermaid.md` |
| One-line job | Replace the traversal loop with sparse matrix-vector multiply over a custom (+,x) algebra — pick the semiring and one kernel becomes BFS, SSSP, reachability, or triangle counting |

## 1. The generalization

```mermaid
flowchart TD
    T["one frontier step (pattern 8):<br/>combine frontier entry with its edge,<br/>reduce candidates arriving at same vertex"]
    T --> M["that IS matrix-vector multiply<br/>y[v] = ADD_u( MUL(q[u], A[u][v]) )"]
    M --> S["semiring = (ADD monoid, MUL op) —<br/>swap them, same kernel,<br/>different graph algorithm"]
```

## 2. The semiring table

```mermaid
flowchart LR
    SR[semiring] --> A["(MIN, PLUS): SSSP relaxation"]
    SR --> B["(MIN, FIRST): BFS levels"]
    SR --> C["(ANY, SECONDI): BFS parents —<br/>SECONDI = index of edge's source"]
    SR --> D["(ANY, PAIR): boolean reachability —<br/>FalkorDB's production choice"]
    SR --> E["(PLUS, TIMES) masked on A:<br/>triangle counting"]
```

The GraphBLAS thesis: graphs ARE sparse matrices (pattern 7 showed
the layouts are identical), so traversal is algebra and decades of
sparse-kernel engineering apply free.

## 3. One BFS step, exactly as LAGraph writes it

```mermaid
flowchart TD
    Q["q: sparse frontier vector"] --> VXM["GrB_vxm(q, mask, NULL, semiring, q, A, GrB_DESC_RSC)<br/>(LG_BreadthFirstSearch_SSGrB_template.c:305-307,<br/>comment: 'push (saxpy-based vxm)')"]
    A2["A: |V|x|V| sparse adjacency"] --> VXM
    MK["mask = visited vector;<br/>DESC_RSC = replace + structural complement:<br/>'write only where mask is absent'"] --> VXM
    VXM --> Q2["q' = next frontier — the don't-revisit filter<br/>is FUSED INTO the multiply, not applied after"]
```

## 4. BFS trace: 0->1, 0->3, 1->2, 3->2, 2->4, source 0

```mermaid
sequenceDiagram
    participant Q as frontier q
    participant K as vxm (MIN, FIRST)
    participant V as visited mask
    Q->>K: level 0: q = {0:0}
    K->>V: q' = {1:1, 3:1}, visited += {1,3}
    K->>V: level 2: both 1 and 3 hit vertex 2 —<br/>MIN-reduce collapses duplicates:<br/>pattern 8's "claim" without user atomics
    K->>V: q' = {2:2}
    K->>V: level 3: q' = {4:3}
    K-->>Q: level 4: q' empty — done
```

For parents, swap in `GxB_ANY_SECONDI` (template:140-143): MUL yields
the frontier vertex's index, ADD takes ANY winner — "any parent is
fine" as algebra.

## 5. The three witnesses, three layers

```mermaid
flowchart TD
    GB["SuiteSparse:GraphBLAS — kernel engine:<br/>1000s of (semiring x storage x mask) JIT variants;<br/>does pattern 8's push/pull switch INTERNALLY<br/>(vxm picks saxpy vs dot form by frontier density)"]
    LA["LAGraph — algorithm layer:<br/>BFS, SSSP (MIN_PLUS, LAGr_SingleSourceShortestPath.c:151-226),<br/>PageRank, BC, TriangleCount, CC (LG_CC_FastSV6.c) —<br/>each a page of algebra, not a traversal engine"]
    FK["FalkorDB — production graph DB on the pattern:<br/>Cypher patterns compile to algebraic expressions;<br/>traversal = Delta_mxm over GxB_ANY_PAIR_BOOL<br/>(algebraic_expression_mul.c:33,49)"]
    GB --> LA --> FK
```

## 6. Worked example — 2-hop friends-of-friends

```mermaid
flowchart TD
    F["f = row u of A (u's friends)"] --> FOF["fof = f * A, masked by !(f + {u})"]
    FOF --> NUM["|V|=100M, deg 500:<br/>work = 500 rows x 500 entries = 250k mul-adds<br/>= microseconds; nothing bigger than the<br/>250k-entry candidate vector materializes"]
    NUM --> CY["FalkorDB emits exactly this for<br/>MATCH (u)-[:F]->()-[:F]->(w) —<br/>the WHERE clause becomes the mask"]
```

## 7. Worked example — masked triangle counting

```mermaid
flowchart TD
    L["L = strictly-lower triangle of symmetric A"] --> C["C&lt;L&gt; = L * L' over (PLUS, PAIR);<br/>triangles = reduce(C)"]
    C --> WHY["the mask &lt;L&gt; computes ONLY where an<br/>edge already exists"]
    WHY --> N["m=1B, deg 20: unmasked L*L' ~ sum(deg²)<br/>~ 400B candidates (~3 TB);<br/>masked: exactly m = 1B probes.<br/>The mask is a 400x work filter.<br/>(LAGr_TriangleCount.c)"]
```

## 8. Inheritance and the honest cost

```mermaid
flowchart LR
    P[semiring traversal] --> F1["FalkorDB: query planning = algebraic<br/>rewriting; associativity = join reordering"]
    P --> F2["delta matrices: LSM-ish pending updates +<br/>periodic flatten (pattern 1's move) —<br/>mutation strikes again, same answer"]
    P --> F3["cost: constant factors — hand-tuned gapbs<br/>BFS beats vxm BFS single-machine; algebra<br/>wins on generality, GPU portability, fusion"]
    P --> F4["this repo: Cypher-subset matching could<br/>compile to semiring products over<br/>roaring-backed boolean matrices — the<br/>differential oracle doesn't care which<br/>strategy produced the rows"]
```

## 8b. Multi-hop paths as matrix products

The pattern's deepest payoff: a whole path pattern is ONE algebraic
expression, and the engine gets to choose evaluation order.

```mermaid
flowchart TD
    CY["MATCH (a:User)-[:FOLLOWS]->(b)-[:LIKES]->(c:Post)"] --> AE["algebraic expression:<br/>result = D_user x F x L x D_post<br/>(diagonal label matrices as filters)"]
    AE --> ORD["associativity = the engine may evaluate<br/>(D_user x F) x (L x D_post) or any other<br/>parenthesization — join ordering as algebra"]
    ORD --> CH["cheapest order depends on intermediate<br/>sparsity: exactly the cardinality-estimation<br/>problem query optimizers solve, but the<br/>rewrite rules are now PROVABLE identities"]
    CH --> FK2["FalkorDB's algebraic_expression module is<br/>this optimizer: it reorders and fuses the<br/>product chain before calling Delta_mxm"]
```

```mermaid
sequenceDiagram
    participant P as planner
    participant AE as algebraic expr
    participant GB as GraphBLAS
    P->>AE: parse Cypher pattern into product chain
    AE->>AE: reorder by estimated sparsity (associativity)
    AE->>GB: Delta_mxm per product, masks from WHERE
    GB->>GB: internal push/pull choice per multiply
    GB-->>P: candidate matrix -> rows
    Note over GB: the same result rows regardless of<br/>parenthesization — an algebraic invariant<br/>a differential harness can exploit
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/template/LG_BreadthFirstSearch_SSGrB_template.c` | BFS as masked vxm (305-307), ANY_SECONDI parents (140-143) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_SingleSourceShortestPath.c` | SSSP over MIN_PLUS (151-226) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_TriangleCount.c` | masked-multiply triangles |
| GraphBLAS | `reference-repos-corpus/GraphBLAS-src` | SuiteSparse kernel engine, internal push/pull |
| falkordb | `reference-repos-competitors/falkordb-src/src/arithmetic/algebraic_expression/algebraic_expression_mul.c` | production Delta_mxm over ANY_PAIR_BOOL (33, 49) |
| falkordb | `reference-repos-competitors/falkordb-src/src/graph/delta_matrix/delta_mxm.c` | delta-matrix multiply (mutability layer) |

## 10. Cross-references

- Sibling patterns: `csr-adjacency-layout` (the matrix IS that
  layout); `frontier-pushpull-switching` (reappears inside vxm);
  `lsm-compaction-tradeoff` (delta matrices); `roaring-bitmap-idsets`
  (boolean matrices over compressed bitmaps).
- Next in category: connected-components hooking/shortcutting
  (FastSV) and PageRank iteration structure.
- 202606 digest overlap: digests named GraphBLAS as FalkorDB's
  engine; this pair adds the semiring table, mask semantics, and the
  masked-triangle arithmetic.
