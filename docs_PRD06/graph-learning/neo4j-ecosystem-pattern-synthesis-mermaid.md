# Neo4j Ecosystem Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `neo4j-ecosystem-pattern-synthesis-ascii.md` / `neo4j-ecosystem-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 23-24 into the category's thesis: an ecosystem is held together not by the server but by its CONTRACTS — a byte format, a message protocol, and the harness that enforces them across six independent codebases |

## 1. The category in one map

```mermaid
flowchart TD
    ECO["18 neo4j-ecosystem repos:<br/>no algorithms, no storage —<br/>the CONTRACT LAYER"]
    ECO --> P23["23 packstream-wire-encoding<br/>STORAGE: the byte contract"]
    ECO --> P24["24 stub-script-conformance<br/>EXECUTION: the enforcement machine"]
    P23 & P24 --> TH["thesis: the ecosystem is a fixed<br/>point of its test suite"]
```

## 2. The contract stack

```mermaid
flowchart TD
    L1["layer 1 — bytes:<br/>PackStream markers (23)"]
    L2["layer 2 — messages:<br/>RUN/PULL ordering, chunking (24)"]
    L3["layer 3 — results:<br/>rows, TCK scenarios (21's turf)"]
    L4["layer 4 — semantics:<br/>transactions, causal consistency"]
    L1 --> L2 --> L3 --> L4
    L1 -.->|"oracle"| O1["byte-diff"]
    L2 -.->|"oracle"| O2["stub scripts"]
    L3 -.->|"oracle"| O3["result-diff / TCK"]
    L4 -.->|"oracle"| O4["history checkers<br/>(Jepsen-style)"]
    O1 & O2 & O3 --> PUB["shipped IN THE OPEN: testkit +<br/>boltstub + TCK — no other corpus<br/>system publishes this much of its<br/>own verification machinery"]
```

## 3. The satellites, contract-wise

```mermaid
flowchart LR
    D["6 drivers (java/py/go/js/.net<br/>+ neo4rs): independent<br/>IMPLEMENTATIONS — each an<br/>oracle for the others"]
    E["testkit + boltstub:<br/>the ENFORCER — one suite,<br/>six adapters (24)"]
    C["openCypher + TCK:<br/>contract 3's grammar"]
    A["APOC: 400+ procedures —<br/>de-facto contract users<br/>depend on"]
    U["browser / shell / OGM /<br/>GDS-client: contract CONSUMERS —<br/>why drift can't be silent"]
    D & C & A & U --> E
```

## 4. The fixed-point loop

```mermaid
flowchart TD
    SV["server changes"] -->|"testkit catches<br/>driver breakage"| OK["agreement<br/>re-verified"]
    DR["driver changes"] -->|"stub scripts catch<br/>protocol drift"| OK
    SP["spec changes"] -->|"six implementations must<br/>co-evolve -> changes are VERSIONED<br/>(Bolt 4, 5, 5.7 feature flags)"| OK
    OK --> MOAT["the moat is not the server<br/>(memgraph/falkordb reimplement it) —<br/>it's the continuously re-verified<br/>agreement"]
```

## 5. The rewrite's free test stack

```mermaid
sequenceDiagram
    participant R as Rust server rewrite
    participant BS as boltstub scripts
    participant DK as 6 stock drivers via testkit
    participant TCK as openCypher TCK
    R->>BS: step 1 — answer byte-for-byte<br/>what the scripts demand (layers 1-2)
    R->>DK: step 2 — tests/neo4j suite,<br/>each stock driver as a client oracle<br/>(6 oracles, zero written by us)
    R->>TCK: step 3 — thousands of result<br/>scenarios through any passing driver
    Note over R,TCK: every failure = a convergence-loop error<br/>signal with a precise location —<br/>the docs_PRD06 thesis at its strongest:<br/>the endpoint is ALREADY INSTRUMENTED
```

## 6. Contract evolution done right

```mermaid
flowchart TD
    EV["element_id, Bolt v5.0"]
    EV --> OLD["old Node struct: 0xB3 —<br/>(id, labels, properties)"]
    EV --> NEW["new Node struct: 0xB4 —<br/>(..., element_id)"]
    OLD & NEW --> DIST["field count lives in the marker<br/>nibble (23) -> old and new are<br/>distinguishable ON THE WIRE"]
    DIST --> NEG["drivers negotiate via the version<br/>handshake; python keeps a back-compat<br/>branch (hydration_handler.py:<br/>element_id=None -> str(id))"]
    NEG --> LESSON["versioned, countable, negotiated —<br/>like schema migrations, never slipped in;<br/>a rewrite must reproduce the NEGOTIATION,<br/>not just the newest format"]
```

## 7. Honest gaps

```mermaid
flowchart TD
    G["not covered by 23-24"]
    G --> G1["APOC procedure dispatch — how 400<br/>procedures register/type-check/stream:<br/>the rewrite's biggest single<br/>compat surface"]
    G --> G2["driver pooling + routing tables:<br/>cluster awareness lives CLIENT-side"]
    G --> G3["OGM object mapping,<br/>cypher-dsl query building"]
    G --> G4["browser's Bolt-over-websocket path"]
```

## 8. Position in the corpus

```mermaid
flowchart TD
    IN["internal categories: storage,<br/>analytics, vector, FTS, graph-db —<br/>all their patterns become INVISIBLE<br/>behind contract layer 1"]
    IN --> EDGE["neo4j-ecosystem: the visible edge —<br/>contracts + enforcement"]
    EDGE --> NEXT["remaining: dataflow-compute<br/>(timely/differential — incremental<br/>computation) and bench-testing<br/>(SQLancer/Jepsen/LDBC — generalized<br/>oracles) to close the corpus"]
    EDGE --> CF["carry-forward: the contracts plus<br/>their enforcement machinery ARE the<br/>product; the server is an implementation<br/>detail six drivers can't see"]
```

## 7c. Worked example — counting the free oracles

```mermaid
flowchart TD
    Q["how much verification does a Bolt<br/>server rewrite inherit for free?"]
    Q --> N1["boltstub scripts: hundreds of scripted<br/>conversations under tests/stub/*/scripts —<br/>each pins one protocol behavior"]
    Q --> N2["6 stock drivers x testkit's tests/neo4j<br/>integration suite = 6 independent<br/>client-side oracles"]
    Q --> N3["openCypher TCK: thousands of<br/>result-level scenarios (layer 3)"]
    N1 & N2 & N3 --> SUM["total: a four-digit count of pre-paid,<br/>vendor-maintained checks BEFORE writing<br/>a single test of our own — versus the<br/>storage-engine category, where a rewrite<br/>must build its crash/recovery harness<br/>from nothing"]
```

## 8b. Design walk — building an ecosystem edge from scratch

```mermaid
flowchart TD
    R1["1. self-describing byte format:<br/>smallest-encoding + nibble dispatch (23) —<br/>decoders skip what they don't know;<br/>evolution = new struct tags + field counts"]
    R2["2. ALL versioning at the handshake,<br/>once per connection — code branches on<br/>a negotiated version, never sniffed bytes"]
    R3["3. conformance harness BEFORE the<br/>second client: one suite + thin adapters (24);<br/>scripts, not prose, define the protocol"]
    R4["4. harness gates every repo's CI —<br/>agreement becomes a property the system<br/>MAINTAINS, not one it once had"]
    R1 --> R2 --> R3 --> R4
    R4 --> CX["counter-example in the corpus:<br/>Lucene vs Tantivy share IDEAS but no wire<br/>contract — ecosystems can't share clients,<br/>compat claims unverifiable. The contract<br/>layer turns N implementations into<br/>ONE ecosystem"]
```

## 9. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/packstream/de.rs` | byte contract, Rust witness (23) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/packstream/v1/__init__.py` | byte contract, canonical writer (23) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/hydration/v1/hydration_handler.py` | hydration + back-compat (23) |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/boltstub/README.md` | conversation DSL (24) |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/tests/stub/basic_query/scripts/single_result.script` | scripted conversation (24) |
| go-driver | `reference-repos-neo4j-family/neo4j-go-driver-src/testkit-backend/backend.go` | per-driver adapter (24) |

## 10. Cross-references

- Members: `packstream-wire-encoding` (23),
  `stub-script-conformance` (24).
- Prior syntheses: graph-db (the server whose edge this
  category defines); storage/analytics/vector/FTS (the
  internals the contracts hide).
- The one-sentence takeaway: six independent codebases agreeing
  on bytes, messages, and semantics — continuously re-verified
  by a public harness — is the hardest part of Neo4j to
  compete with, and the easiest part to REUSE for a rewrite.
- Reading order for this category: 23 (bytes) then 24 (the
  machine that checks them) then this synthesis; the ASCII
  twin adds the same design walk in prose plus the APOC /
  routing-table gap list for future passes.
