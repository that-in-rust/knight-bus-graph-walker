# Stub Script Conformance — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `stub-script-conformance-ascii.md` / `stub-script-conformance-mermaid.md` |
| One-line job | Testkit's architecture for testing six driver codebases with ONE test suite: tests speak a JSON protocol to a thin per-driver backend, and a scripted fake server (boltstub) plays exact Bolt byte conversations — conformance testing industrialized |

## 1. The problem

Neo4j ships drivers in 6 languages. Each must implement the
same Bolt/PackStream contract (pattern 23), the same session/
transaction/routing semantics, the same error handling. Writing
6 test suites means 6x the work and 6 subtly different notions
of "correct". Testkit inverts this: ONE Python test suite, and
each driver provides only a thin adapter.

## 2. The four-box architecture

```text
+----------------+   JSON protocol    +------------------+
| nutkit tests   | <----------------> | testkit backend  |
| (Python, one   |  "NewSession",     | (per driver: 300 |
|  suite for all |  "SessionRun"...   |  -line adapter)  |
|  drivers)      |                    +---------+--------+
+----------------+                              | native API
                                       +--------v--------+
+----------------+     Bolt bytes      | DRIVER UNDER    |
| boltstub       | <-----------------> | TEST (java, py, |
| (scripted fake |  exact scripted     |  go, js, .net,  |
|  server)       |  conversations      |  ruby)          |
+----------------+                     +-----------------+
testkit-src: nutkit/{frontend,backend,protocol}, boltstub/,
tests/{stub,neo4j,tls}; drivers each carry the adapter:
python: testkitbackend/{backend,fromtestkit,totestkit}.py
go:     testkit-backend/backend.go — handleRequest switch
        dispatching "NewSession" etc. (backend.go:582-863)
```

## 3. The stub script DSL

Boltstub is a fake server driven by a script of the EXACT
conversation (boltstub/README.md, grammar.lark):

```text
tests/stub/basic_query/scripts/single_result.script:
!: BOLT #BOLT_PROTOCOL#          head: pin the version
A: HELLO {"{}": "*"}             Auto-respond to HELLO
*: RESET                         allow RESET anytime
C: RUN {"U": "*"} {"{}": "*"} {"{}": "*"}   Client MUST send
S: SUCCESS {"fields": ["n"]}     Server replies
{*                               repeat block
    C: PULL {"n": {"Z": "*"}, "[qid]": -1}
    S: RECORD [#RESULT#]
       SUCCESS {"type": "r", "has_more": false}
*}
?: GOODBYE                       optional
any unexpected client message -> nonzero exit -> test FAILS.
"*" wildcards let one script match all drivers' benign
variation while pinning what matters.
```

The script IS the assertion: a driver passes if its byte-level
conversation matches the play. This tests the UNOBSERVABLE
middle (retries, resets, pipelining, version negotiation) that
result-diffing can never see.

## 4. The two oracle modes

```text
tests/stub/*:   against boltstub — tests the DRIVER's protocol
                behavior against a scripted ideal server;
                covers errors/disconnects/routing edge cases
                impossible to provoke in a real server
tests/neo4j/*:  against a real dockerized server — end-to-end,
                catches what scripts didn't anticipate
per-driver skips: TEST_DRIVER_NAME adjusts expected outcomes
                (README) — the suite admits drivers legitimately
                differ in declared places; everything else is
                shared contract.
```

## 5. Worked example — one test, six drivers

```text
test_can_run_simple_query (conceptually):
    frontend: backend.send("NewDriver", uri, auth)
              backend.send("NewSession"); ("SessionRun", "RETURN 1")
    each backend adapter translates to its native API:
        go:     switch "NewSession" -> driver.NewSession(ctx, cfg)
                (backend.go:863, 944)
        python: fromtestkit.py parses -> neo4j.Session.run()
                -> totestkit.py serializes results back
    boltstub verifies: HELLO, RUN, PULL arrive with the right
    shapes, in the right order, on the pinned Bolt version
cost per new driver: one ~1-3 kLOC adapter. benefit: the
entire accumulated suite (hundreds of scripts) for free.
```

## 6. Worked example — what a script catches that results don't

```text
scenario: driver retries a failed RUN on a new connection.
result-level diff: both drivers return the same rows — PASS.
stub script: expects exactly ONE connection, sees driver B
    open a second one after a spurious RESET — script line
    unmatched -> FAIL.
this is the pattern's whole value: it pins the CONVERSATION,
catching protocol-level divergence (double-sends, missing
resets, eager PULLs) that is invisible at the result level
but burns real servers (wasted connections, leaked cursors).
```

## 7. Why this is the corpus's verification keystone

The docs_PRD06 rewrite thesis says: the harness is the durable
asset. Testkit is that thesis, shipped in production by Neo4j
itself: when the server team ships a new Bolt version, six
driver teams converge on it by passing one suite. A Rust server
rewrite gets the mirror benefit for free — point the SAME six
stock drivers plus testkit's dockerized integration tests at
the rewrite, and every driver becomes an oracle. Boltstub even
provides the inverse tool: its scripts document, line by line,
what a compliant SERVER must say — executable protocol
documentation (7687.org grammar included).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/boltstub/README.md` | stub-script DSL spec |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/tests/stub/basic_query/scripts/single_result.script` | example scripted conversation |
| testkit | `reference-repos-neo4j-family/neo4j-testkit-src/nutkit/protocol/__init__.py` | frontend JSON protocol (requests/responses) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/testkitbackend/fromtestkit.py` | adapter: JSON -> native API |
| go-driver | `reference-repos-neo4j-family/neo4j-go-driver-src/testkit-backend/backend.go` | adapter: handleRequest dispatch (582-863) |

## 9. Cross-references

- Sibling patterns: `packstream-wire-encoding` (23 — the bytes
  the scripts pin), `pull-operator-pipeline` (21 — RUN/PULL is
  the wire face of pull execution).
- Corpus kin: SQLancer and Jepsen (bench-testing category,
  next) generalize the idea — SQLancer generates the
  conversations instead of scripting them; Jepsen scripts
  FAILURES instead of messages.
- The rewrite play: adopt testkit before writing the server —
  its stub scripts and integration suite are a pre-paid,
  behavior-complete spec of what "speaks Bolt correctly"
  means, maintained by the vendor.
- Next: neo4j-ecosystem synthesis, then dataflow-compute and
  bench-testing to close the corpus.
