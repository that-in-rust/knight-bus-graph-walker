# gtm-POC-01 — The One-Algorithm Swap: Replacing a Single GDS Procedure with a Rust Engine over FFI

**The question this document answers:** if we could change how just ONE
algorithm is processed — leaving the rest of Neo4j untouched, and rerouting
that one procedure call into our Rust engine — which algorithm should it be,
and what is the concrete architecture for doing it?

**Method:** Timeline Traverser — three-to-four plausible futures per
candidate, compared, then a decision filter. Evidence base: the actual GDS
source in `reference-repos-neo4j-family/graph-data-science-src/` (read for
this document, file paths cited throughout), plus the market evidence in
`simulation01.md` and the per-algorithm storage plans in `Arch06.md`.

---

## Decision Frame

- **Fork in the road:** which single GDS procedure do we re-implement in
  Rust and swap in first — WCC, PageRank, Louvain, or a shortest-path proc?
- **Desired outcome:** a demo a Neo4j user can run in their own instance:
  same Cypher call shape, same results, dramatically lower RAM, and a
  printed receipt — proving the whole thesis on one algorithm.
- **Hard constraints:**
  - We do not fork or rebuild Neo4j itself (GPL entanglement, maintenance
    hell, and users will not install a forked database).
  - The demo must run against a stock Neo4j + stock data; setup friction
    kills GTM demos.
  - Result parity must be *checkable* (bit-identical or provably equivalent
    output vs. `gds.wcc.stream` etc.).
- **Time horizon:** week 1 (spike), month 1 (working POC), quarter 1
  (demoable to strangers).
- **What would count as failure:** a POC that requires patching GDS's jar,
  or one whose results can't be verified against GDS, or one where the
  RAM win is invisible because the graph fits in memory anyway.

### The seam in the GDS codebase (what the code audit found)

The critical discovery from reading the GDS source: **the procedure layer is
a paper-thin facade, and the algorithm is invoked three layers below it.**

```
Cypher: CALL gds.wcc.stream('myGraph', {...})
   |
   v
WccStreamProc.stream()                        <- 6 lines; just delegates
  proc/community/src/main/java/org/neo4j/gds/wcc/WccStreamProc.java:41-48
   |    return facade.algorithms().community().wccStream(graphName, config);
   v
GraphDataScienceProcedures (interface)        <- injected via @Context
  procedures/procedures-facade-api/.../GraphDataScienceProcedures.java
   |
   v
LocalCommunityProcedureFacade.wccStream()     <- resolves graph from catalog
  procedures/algorithms-facade/.../LocalCommunityProcedureFacade.java:1158
   |
   v
Wcc / WccStub (the actual union-find)         <- runs on the projected
  algo/src/main/java/org/neo4j/gds/wcc/Wcc.java     in-heap graph
```

Two facts follow:

1. **We never need to touch GDS's code.** Neo4j registers procedures from
   any plugin jar in the `plugins/` directory. A 200-line Java shim class
   with `@Procedure(value = "grain.wcc.stream", mode = READ)` sits beside
   GDS (or entirely without GDS installed) and is indistinguishable in call
   shape from the real thing. Procedure names are namespaced, so
   `grain.wcc.stream` coexists with `gds.wcc.stream` — which is exactly
   what we want for the side-by-side demo: same session, both calls, watch
   the RAM.
2. **The expensive step GDS performs before the algorithm — `gds.graph.project`
   — is the step we refuse to perform.** Our procedure takes the graph from
   a GRAIN snapshot on disk, not from the in-heap catalog. The demo *is*
   the absence of the projection.

---

## Timeline A: Swap WCC (`grain.wcc.stream`)

- **Opening move:** write the Java shim (`grain.wcc.stream(snapshotName, config)`),
  bind it to the existing Rust WCC over JNI, and a `grain.snapshot.build`
  procedure that exports edges from the live store and builds the GRAIN
  snapshot once.
- **Week 1:** shim calls a Rust `cdylib` via JNI (`Java_com_grain_Native_wccStream`).
  Rust maps the snapshot, runs label propagation with O(V) labels resident,
  writes `(nodeId, componentId)` pairs into a direct ByteBuffer handed over
  the boundary; Java wraps it as a `Stream<WccStreamResult>`-shaped record.
  Parity harness: run `gds.wcc.stream` and `grain.wcc.stream` on the same
  store, sort, diff. WCC parity is trivial to verify — component *labels*
  may differ, but the partition must be identical (canonicalize by min
  member).
- **Month 1:** receipt integration — `grain.wcc.estimate` returns the exact
  bytes and modeled wall-clock from the manifest, next to GDS's own
  `gds.wcc.stream.estimate` (which on a big graph returns "blocked:
  estimated memory exceeds free memory" — their estimator becomes our demo
  co-star). Demo script: 8 GB heap, LDBC-scale graph, GDS blocks, GRAIN
  finishes.
- **Quarter 1:** incremental snapshot refresh (journal replay), warm-start
  story deferred (WCC re-runs are cheap anyway), publishable benchmark.
- **Long-term shape:** WCC becomes the wedge; the same shim pattern (one
  Java class per proc, one Rust entry point) is stamped out for the other
  six families.
- **Likelihood of technical success:** highest of all candidates. Union-find
  / label propagation is ~200 lines of Rust; the repo already has a working
  WCC (`src/lib.rs` GDS emulation targets it); result shape is the simplest
  possible (two longs per row).
- **Stress points:** node-ID mapping (Neo4j internal ids vs. GRAIN dense
  ids) must be exact — this is the single most likely source of silent
  wrongness. The export step (store → snapshot) is the demo's setup cost;
  it must be one command.
- **Inflection points:** if the JNI result-streaming feels clunky, the
  fallback (Arrow IPC over a Unix socket to a sidecar process) changes the
  transport but not the architecture.

## Timeline B: Swap PageRank (`grain.pageRank.stream`)

- **Opening move:** same shim pattern, Rust PageRank with two rank vectors
  resident, edges streamed per iteration.
- **Week 1:** slower start than WCC — PageRank needs the iteration loop,
  damping, tolerance handling, and *float* parity, which is where this
  timeline diverges.
- **Month 1:** the parity harness fights you: GDS PageRank results depend on
  iteration order, concurrency, and float summation order. "Same to 1e-6"
  requires argumentation, not a diff. Every demo Q&A now contains "why is
  row 47 different in the 7th decimal?" — a credibility tax paid at the
  worst moment.
- **Quarter 1:** works, benchmarks well (delta convergence is a good story),
  but the verification asterisk never goes away.
- **Long-term shape:** great *second* algorithm — the delta/convergence
  machinery it forces us to build is reused by Louvain and FastRP.
- **Likelihood:** high technically; medium as a *first* demo.
- **Stress points:** float parity; iteration-count semantics
  (`maxIterations` vs. tolerance interplay must match GDS's).
- **Inflection points:** if we accept "top-k ranking identical" instead of
  value parity, the pain drops — but the demo claim weakens.

## Timeline C: Swap Louvain (`grain.louvain.stream`)

- **Opening move:** same shim; Rust Louvain with capped tallies.
- **Week 1-Month 1:** hardest algorithm of the three: multi-level collapse,
  modularity accounting, and — fatally for a first POC — **nondeterminism**.
  GDS Louvain itself is not deterministic across runs (vertex order and
  concurrency change the local optima). Parity can only be argued via
  modularity-score equivalence, not per-row diff.
- **Quarter 1:** when it works, it is the most *valuable* swap (biggest
  complaint volume after projection itself: the 5-hour/70 GB Louvain SO
  post), and warm-start is its killer feature. But it is a quarter-1
  deliverable, not a week-1 one.
- **Long-term shape:** the flagship algorithm for the *paying* fraud-team
  segment; wrong place to *start*.
- **Likelihood:** medium; most engineering risk of the three.
- **Stress points:** approximation caveats (capped tallies) land in the
  very first conversation with a user — before trust exists.
- **Inflection points:** shipping Louvain *after* WCC has established the
  pattern converts every caveat conversation from "is this thing real?"
  to "what's the accuracy/RAM dial?"

## Timeline D: Deep swap — intercept `gds.wcc.stream` itself (fork/patch GDS)

- **Opening move:** fork GDS, replace `WccStreamProc`'s delegation with a
  JNI call; users install our patched jar instead of GDS.
- **Week 1:** compiles; feels powerful ("no new procedure names!").
- **Month 1:** the trap closes: we now track every GDS release, our jar
  conflicts with their licensing/registration machinery
  (`procedures/extension/OpenGraphDataScienceExtension.java`), enterprise
  users cannot legally or operationally replace their GDS plugin, and the
  demo now requires users to *uninstall* something.
- **Long-term shape:** a maintenance treadmill that inherits GDS's GPL
  surface and Neo4j's release cadence. Abandoned by quarter 1.
- **Likelihood of regret:** very high. Included as the cautionary timeline.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
|---|---|---|---|---|---|
| **A: WCC shim** | Fastest to demo; trivially verifiable parity; biggest usage share (~20%); simplest result shape | Least "impressive" algorithm to systems people | Total — one jar, delete it and nothing changed | Low | Only the plugin classloader and JNI |
| **B: PageRank shim** | Best-known name; delta-convergence story | Float-parity credibility tax in every demo | Total | Medium | Float semantics vs. GDS |
| **C: Louvain shim** | Highest complaint volume; warm-start killer feature | Nondeterministic parity; approximation caveats on day one; slowest build | Total | Medium-high as first move; low as second/third | Modularity-equivalence argument accepted by audience |
| **D: patch GDS** | Same procedure names | Fork treadmill, GPL surface, uninstall friction | Poor — users installed a patched jar | Very high | Neo4j release cadence, GDS licensing machinery |

**Shared inflection point (all timelines):** the node-ID mapping and the
one-command snapshot build. Whichever algorithm we pick, the POC lives or
dies on `CALL grain.snapshot.build('myGraph')` being boring and reliable.

---

## Decision Filter

- **Strongest if everything goes normally:** Timeline A (WCC). It compounds:
  the shim pattern, ID mapping, snapshot build, receipt procedure, and
  parity harness built for WCC are 80% of every subsequent swap.
- **Safest if things go badly:** also A — total reversibility (one jar),
  exact parity (partition diff), and the smallest Rust surface to debug.
- **Fastest uncertainty-collapsing experiment:** one week — JNI round trip
  only: a `grain.ping` procedure that calls into the Rust cdylib, maps an
  existing GRAIN snapshot, and streams back the first 1,000
  `(nodeId, degree)` pairs. That single spike de-risks the classloader, the
  JNI transport, and the ID mapping — everything else is known Rust work.

**The pick: WCC.** Not because it is the hardest — because it is the most
*checkable*, and simulation01's whole argument is that certainty is the
product. The first algorithm we swap must be the one where a skeptic can
verify us with a `diff`.

---

## The Architecture (gtm-POC-01)

### Components

```
                     NEO4J PROCESS (stock, unmodified)
  +----------------------------------------------------------------+
  |  Cypher: CALL grain.wcc.stream('snap1')                        |
  |     |                                                          |
  |  grain-plugin.jar          gds-plugin.jar (optional, for the   |
  |  +--------------------+     side-by-side demo)                 |
  |  | GrainProcedures    |                                        |
  |  |  @Procedure        |                                        |
  |  |  grain.snapshot.build   grain.wcc.estimate                  |
  |  |  grain.wcc.stream       grain.ping                          |
  |  +---------|----------+                                        |
  |            | JNI (System.loadLibrary("grain_ffi"))             |
  +------------|---------------------------------------------------+
               v
  libgrain_ffi.so  (Rust cdylib, in the plugin dir)
  +----------------------------------------------------------------+
  |  #[no_mangle] extern "system" fns:                             |
  |    grain_snapshot_build(store_export_path) -> manifest         |
  |    grain_estimate(manifest, algo) -> receipt bytes             |
  |    grain_wcc_stream(manifest, out_bytebuffer) -> n_rows        |
  |  Internally: mmap GRAIN snapshot; labels[V] resident;          |
  |  edges streamed; RSS bounded by the receipt.                   |
  +----------------------------------------------------------------+
               ^
               | reads (mmap, page cache does the caching)
  GRAIN snapshot directory on disk (built once, refreshed via journal)
```

### The five design decisions, with rationale

1. **Sidecar procedure names (`grain.*`), never shadowing `gds.*`.**
   Neo4j's procedure registry is namespaced; `grain.wcc.stream` installs
   next to GDS with zero conflict. The side-by-side call is the demo.
   (Timeline D shows why shadowing is a trap.)

2. **JNI, not JNA/Panama, for the POC.** JNI is available on every Neo4j
   JVM (Neo4j 5.x runs on Java 17/21; Panama's FFM is finalized only in
   22+). One `static { System.loadLibrary("grain_ffi"); }` in the shim,
   `.so` shipped in the same plugin directory. Upgrade path to Panama later
   is mechanical.

3. **Results cross the boundary as one direct ByteBuffer, not per-row
   calls.** JNI per-row callbacks are the classic 100x mistake. Rust fills
   a `(u64 nodeId, u64 componentId)` array in a `ByteBuffer.allocateDirect`
   region; Java wraps it in a lazy `Stream`. For results larger than RAM
   (not the case for WCC's 16 B/row), the same interface pages through the
   buffer in chunks.

4. **The graph never crosses the FFI boundary.** The Rust side reads its
   own GRAIN snapshot from disk. `grain.snapshot.build` exports
   `(srcId, dstId)` pairs from the live store *once* (kernel-API scan in the
   shim, streamed to the Rust builder), records the Neo4j-id ↔ dense-id
   mapping in a sidecar, and from then on the JVM heap carries only the
   result buffer. This is the anti-`gds.graph.project`: the heavy structure
   lives in mmap'd files owned by the OS page cache, not the heap.

5. **The receipt is a procedure, and it quotes before it runs.**
   `grain.wcc.estimate('snap1')` reads ~1 KB of manifest and returns
   `{bytesResident, bytesStreamed, estSeconds}` — computed the same way the
   run enforces it (fixed arena). The demo pairs it with GDS's own
   estimator, which on the target graph returns its "blocked: exceeds free
   memory" refusal — their honesty about the wall becomes our stage.

### Parity verification (the part that makes it a GTM asset, not a toy)

```
1. Same store, same node set.
2. CALL gds.wcc.stream    -> (nodeId, componentId_gds)
3. CALL grain.wcc.stream  -> (nodeId, componentId_grain)
4. Canonicalize both: component label := min(nodeId in component).
5. diff — must be empty. Publish the harness with the POC.
```

Labels may legally differ (GDS makes no label-stability promise); the
*partition* may not. This canonicalization is why WCC is the only family
where a hostile skeptic can verify us in one line — and that property is
worth more to go-to-market than any benchmark.

### Explicitly out of scope for POC-01

- Write-back (`grain.wcc.write`) — mutating the store multiplies the
  surface area; stream + the user's own `SET` clause covers the demo.
- Incremental snapshot maintenance beyond a manual rebuild.
- Any second algorithm before the WCC parity harness is public.

### Honest caveats

- The one-time export is a real cost on huge stores; the POC must print its
  progress and its own receipt ("export: ~N minutes, snapshot: ~M GB").
- JNI + a native library in `plugins/` will require
  `dbms.security.procedures.unrestricted=grain.*` in neo4j.conf — one
  documented config line, but it is friction and must be in the README.
- Aura (managed cloud) does not allow custom plugins — this POC targets
  self-hosted Neo4j, which is exactly the segment with the RAM pain
  (simulation01 §2, §7).
