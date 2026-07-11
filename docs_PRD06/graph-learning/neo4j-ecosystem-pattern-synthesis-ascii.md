# Neo4j Ecosystem Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `neo4j-ecosystem-pattern-synthesis-ascii.md` / `neo4j-ecosystem-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 23-24 into the category's thesis: an ecosystem is held together not by the server but by its CONTRACTS — a byte format, a message protocol, and the harness that enforces them across six independent codebases |

## 1. The category in one sentence

The 18 neo4j-ecosystem repos (5 official drivers + neo4rs,
testkit, APOC, openCypher, browser, cypher-shell/dsl, OGM,
GDS client, Bolt docs) contain almost no graph algorithms and
no storage — they are the ecosystem's CONTRACT LAYER, and the
two patterns extracted from them are both about contracts:

```text
23 packstream-wire-encoding   storage    the byte contract
24 stub-script-conformance    execution  the enforcement machine
```

## 2. The contract stack

```text
layer 4  semantics    transactions, causal consistency
layer 3  results      rows, TCK scenarios          (21's turf)
layer 2  messages     RUN/PULL ordering, chunking  (24)
layer 1  bytes        PackStream markers           (23)
each layer has its own oracle type: history checkers /
result-diff / stub scripts / byte-diff — and the ecosystem
ships oracles for 1-3 IN THE OPEN: testkit + boltstub + TCK.
No other system in the corpus publishes this much of its own
verification machinery.
```

## 3. What the satellites are, contract-wise

```text
drivers (java/py/go/js/.net + neo4rs):  6 independent
    IMPLEMENTATIONS of contracts 1-2 — each one is an oracle
    for the others (and for any server rewrite)
testkit + boltstub:  the ENFORCER — one suite, six adapters
    (24); scripts double as executable protocol docs
openCypher (+ front-end, libcypher-parser in graph-db):
    contract 3's grammar and TCK, reusable as a front-end
APOC:  400+ procedures — a de-facto contract users depend on;
    the rewrite thesis's "surface area" lives largely here
browser / cypher-shell / OGM / GDS-client:  contract CONSUMERS
    — the reason the contracts can't drift silently
docs-bolt (7687.org):  the prose spec the scripts execute
```

## 4. The category's one deep lesson

Neo4j's moat is not the server — competitors (memgraph,
falkordb) reimplement the server surface. The moat is that six
driver codebases, a wire spec, a conformance harness, and a
procedure library all AGREE, and that agreement is continuously
re-verified. An ecosystem is a fixed point of its test suite:

```text
server changes  -> testkit catches driver breakage
driver changes  -> stub scripts catch protocol drift
spec changes    -> six implementations must co-evolve,
                   so changes are versioned (Bolt 4, 5, 5.7
                   feature flags) not silent
```

This is why the docs_PRD06 known-endpoint rewrite thesis is
strongest exactly here: the endpoint is not just observable,
it is ALREADY INSTRUMENTED — the vendor maintains the
differential harness that a rewrite needs.

## 5. Worked example — the rewrite's free test stack

```text
goal: Rust server speaking Bolt.
step 1: pass boltstub-derived scripts (layer 1-2): the scripts
        say byte-for-byte what a compliant server answers
step 2: run testkit's tests/neo4j suite with TEST_DRIVER_NAME
        = each of the six stock drivers against the rewrite:
        6 independent client oracles, zero written by us
step 3: run openCypher TCK through any passing driver
        (layer 3, thousands of scenarios)
cost: standing up docker + adapters config. every failing
      script/scenario is a convergence-loop error signal
      (generate -> diff -> regenerate) with a precise location.
```

## 6. Worked example — contract evolution done right

```text
element_id (Bolt/PackStream v5.0):
    old: Node struct = (id, labels, properties)         3 fields
    new: Node struct = (id, labels, properties,
                        element_id)                     4 fields
    python driver hydrate_node keeps a back-compat branch:
    element_id=None -> str(id)   (hydration_handler.py)
    struct field COUNT is in the marker nibble (0xB3 vs 0xB4,
    pattern 23) — so old and new are distinguishable on the
    wire, and drivers negotiate via the version handshake.
lesson: versioned, countable, negotiated — contract changes in
this ecosystem are engineered like schema migrations, not
slipped in. A rewrite must reproduce the NEGOTIATION, not just
the newest format.
```

## 7. Honest gaps

```text
not covered by 23-24 (later passes if wanted):
    - APOC's procedure-dispatch machinery (how 400 procedures
      register, type-check, and stream) — the biggest single
      compat surface for the rewrite
    - driver connection pooling / routing-table handling
      (cluster awareness lives client-side!)
    - OGM's object mapping and cypher-dsl's query building
    - the browser's Bolt-over-websocket path
```

## 7b. Design walk — building an ecosystem edge from scratch

```text
if you were designing a multi-language client ecosystem today,
the category's two patterns compose into a recipe:
 1. pick a self-describing byte format with smallest-encoding
    rules and nibble dispatch (23) — decoders skip what they
    don't know; format evolution = new struct tags + field
    counts, never re-parsing ambiguity
 2. put ALL versioning at the handshake, once per connection —
    downstream code branches on a negotiated version, not on
    sniffed bytes
 3. write the conformance harness BEFORE the second client:
    one test suite + thin adapters (24); scripts, not prose,
    define the protocol
 4. let the harness gate every repo's CI — agreement is then a
    property the system maintains, not one it once had
counter-example held up by the corpus: FTS engines (Lucene vs
Tantivy) share IDEAS but no wire contract — so their ecosystems
can't share clients, and compatibility claims are unverifiable.
The contract layer is what turns N implementations into ONE
ecosystem.
```

## 8. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/packstream/de.rs` | byte contract, Rust witness (23) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/packstream/v1/__init__.py` | byte contract, canonical writer (23) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/hydration/v1/hydration_handler.py` | entity hydration + back-compat (23) |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/boltstub/README.md` | conversation DSL (24) |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/tests/stub/basic_query/scripts/single_result.script` | scripted conversation (24) |
| go-driver | `reference-repos-neo4j-family/neo4j-go-driver-src/testkit-backend/backend.go` | per-driver adapter (24) |

## 9. Cross-references

- Members: `packstream-wire-encoding` (23),
  `stub-script-conformance` (24).
- Prior syntheses: graph-db (the server whose edge this
  category defines), FTS/vector/storage/analytics (internal
  patterns that all become invisible behind contract layer 1).
- The carry-forward sentence: an ecosystem is a fixed point of
  its test suite — the contracts plus their enforcement
  machinery ARE the product; the server is an implementation
  detail that six drivers can't see.
- Next categories: dataflow-compute (timely/differential —
  incremental computation) and bench-testing (SQLancer,
  Jepsen, LDBC — the generalized oracles), closing the corpus.
