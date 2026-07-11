# Stub Script Conformance — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `stub-script-conformance-ascii.md` / `stub-script-conformance-mermaid.md` |
| One-line job | Testkit's architecture for testing six driver codebases with ONE test suite: tests speak a JSON protocol to a thin per-driver backend, and a scripted fake server (boltstub) plays exact Bolt byte conversations — conformance testing industrialized |

## 1. The four boxes

```mermaid
flowchart LR
    NK["nutkit tests<br/>(one Python suite<br/>for ALL drivers)"]
    BE["testkit backend<br/>(per driver: thin adapter,<br/>~1-3 kLOC)"]
    DR["DRIVER UNDER TEST<br/>java / python / go /<br/>js / .net / ruby"]
    BS["boltstub<br/>(scripted fake server)"]
    NK <-->|"JSON protocol:<br/>NewDriver, NewSession,<br/>SessionRun..."| BE
    BE <-->|"native API calls"| DR
    DR <-->|"Bolt bytes — must match<br/>the script EXACTLY"| BS
```

## 2. The economics

```mermaid
flowchart TD
    P["6 languages x same Bolt contract<br/>(pattern 23) x same session/tx/routing<br/>semantics"]
    P --> BAD["naive: 6 test suites — 6x work,<br/>6 subtly different 'correct's"]
    P --> GOOD["testkit: ONE suite + 6 thin adapters"]
    GOOD --> ADP["python: testkitbackend/<br/>fromtestkit.py + totestkit.py<br/>go: testkit-backend/backend.go —<br/>handleRequest switch (582-863)"]
    ADP --> WIN["cost of a new driver: one adapter;<br/>benefit: hundreds of accumulated<br/>scripts for free"]
```

## 3. The stub script DSL

```mermaid
flowchart TD
    S["single_result.script<br/>(tests/stub/basic_query/scripts/)"]
    S --> H["head: !: BOLT version pin<br/>A: HELLO auto-response<br/>*: RESET allowed anytime"]
    S --> B["body: C: RUN — client MUST send<br/>S: SUCCESS fields — server replies<br/>repeat block: C: PULL -><br/>S: RECORD + SUCCESS"]
    B --> W["wildcards '*' absorb benign<br/>per-driver variation; everything<br/>else is pinned"]
    W --> A["any unexpected client message -><br/>nonzero exit -> test FAILS:<br/>the script IS the assertion"]
```

## 4. One test through six drivers

```mermaid
sequenceDiagram
    participant T as nutkit test
    participant A as adapter (go)
    participant D as go driver
    participant ST as boltstub
    T->>A: {"name": "NewSession", ...}
    A->>D: driver.NewSession(ctx, cfg)<br/>(backend.go:863, 944)
    T->>A: {"name": "SessionRun", "RETURN 1"}
    A->>D: session.Run(...)
    D->>ST: HELLO -> RUN -> PULL (Bolt bytes)
    ST-->>D: scripted SUCCESS / RECORD / SUCCESS
    ST->>ST: every line matched? exit 0
    Note over T,ST: swap the adapter -> the SAME test runs<br/>against python, java, js, .net, ruby
```

## 5. The two oracle modes

```mermaid
flowchart TD
    O["where does truth come from?"]
    O --> STUB["tests/stub: boltstub scripts —<br/>a scripted IDEAL server; provokes<br/>errors/disconnects/routing cases a<br/>real server can't produce on demand"]
    O --> REAL["tests/neo4j: dockerized real server —<br/>end-to-end, catches what scripts<br/>didn't anticipate"]
    STUB & REAL --> SKIP["TEST_DRIVER_NAME adjusts expected<br/>outcomes: drivers may differ in<br/>DECLARED places; everything else<br/>is shared contract"]
```

## 6. What scripts catch that results can't

```mermaid
sequenceDiagram
    participant DA as driver A (correct)
    participant DB as driver B (buggy retry)
    participant H as result-level diff
    participant SS as stub script
    DA->>H: rows [1]
    DB->>H: rows [1]
    H-->>H: identical -> PASS (blind!)
    DB->>SS: opens a SECOND connection<br/>after a spurious RESET
    SS-->>DB: script line unmatched -> FAIL
    Note over H,SS: the script pins the CONVERSATION:<br/>double-sends, missing resets, eager<br/>PULLs — invisible in results, real<br/>cost on real servers
```

## 7. The corpus's verification keystone

```mermaid
flowchart TD
    TH["docs_PRD06 thesis:<br/>the harness is the durable asset"]
    TH --> TK["testkit = the thesis in production:<br/>new Bolt version -> six driver teams<br/>converge by passing ONE suite"]
    TK --> RW["mirror benefit for a server rewrite:<br/>point the six STOCK drivers + testkit<br/>at the rewrite — every driver<br/>becomes an oracle"]
    RW --> DOC["boltstub scripts double as executable<br/>protocol documentation: line by line,<br/>what a compliant server must say"]
```

## 8. Corpus kin

```mermaid
flowchart LR
    TK2["testkit: SCRIPTED conversations<br/>(this pattern)"]
    SQ["SQLancer (bench-testing, next):<br/>GENERATES conversations —<br/>metamorphic query oracles"]
    JE["Jepsen (bench-testing):<br/>scripts FAILURES, not messages —<br/>partitions, clock skew"]
    TK2 --> SQ --> JE
    TK2 -.-> K23["pattern 23: the bytes<br/>the scripts pin"]
    TK2 -.-> K21["pattern 21: RUN/PULL is pull<br/>execution's wire face"]
```

## 9. The layered contract picture

```mermaid
flowchart TD
    L1["layer 1 — bytes: PackStream markers,<br/>integer boundaries (23)"]
    L2["layer 2 — messages: RUN/PULL/SUCCESS<br/>ordering, chunking, resets (this pattern)"]
    L3["layer 3 — results: rows, multisets,<br/>TCK scenarios (21, graph-db synthesis)"]
    L4["layer 4 — semantics: transactions,<br/>causal consistency, recovery<br/>(needs Jepsen-style harnesses)"]
    L1 --> L2 --> L3 --> L4
    L4 --> N["each layer needs its OWN oracle type:<br/>byte-diff, script, result-diff, history<br/>checker — no single harness covers all;<br/>testkit owns layers 1-2 and rents 3"]
```

## 9b. Anatomy of a script feature set

```mermaid
flowchart TD
    F["boltstub script features<br/>(boltstub/README.md, grammar.lark)"]
    F --> F1["!: BOLT 5.7 FF 01 —<br/>pin version AND feature flags<br/>(varint-encoded, client must opt in)"]
    F --> F2["!: ALLOW RESTART / CONCURRENT —<br/>one script, many connections,<br/>each with its own play position"]
    F --> F3["A: auto-respond | *: anytime |<br/>?: optional | repeat blocks —<br/>a tiny regular language over<br/>message sequences"]
    F1 & F2 & F3 --> INS["insight: the DSL is a grammar over<br/>CONVERSATIONS, exactly as pattern 19's<br/>FST is over terms — protocol conformance<br/>= language membership, and the stub<br/>server is the accepting automaton"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/boltstub/README.md` | stub-script DSL spec |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/tests/stub/basic_query/scripts/single_result.script` | example scripted conversation |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/nutkit/protocol/__init__.py` | frontend JSON protocol |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/testkitbackend/fromtestkit.py` | adapter: JSON -> native API |
| go-driver | `reference-repos-neo4j-family/neo4j-go-driver-src/testkit-backend/backend.go` | adapter dispatch (582-863) |

## 11. Cross-references

- Sibling patterns: `packstream-wire-encoding` (23),
  `pull-operator-pipeline` (21).
- The rewrite play: adopt testkit BEFORE writing the server —
  its scripts are a pre-paid, behavior-complete, vendor-
  maintained spec of "speaks Bolt correctly".
- Next: neo4j-ecosystem synthesis, then dataflow-compute and
  bench-testing to close the corpus.
