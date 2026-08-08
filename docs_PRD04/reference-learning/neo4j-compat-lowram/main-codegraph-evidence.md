# Main-Agent CodeGraph Evidence

**Tool:** `@sdsrs/code-graph` / `code-graph-mcp` 0.114.1 via `npx`  
**Date:** 2026-08-08  
**Purpose:** Independently verify the architectural spine used to integrate the three evidence lanes. This is not a substitute for their file-level TSVs.

## 1. Index Health

| Repository | Indexed files | Nodes | Edges | Health | Important scope note |
|---|---:|---:|---:|---|---|
| `neo4j-gds-src` | 4,921 | 38,262 | 521,221 | Healthy | Strong Java call/import/reference graph; 15,756 unresolved calls require exact-file confirmation for ambiguous common method names. |
| `neo4j-src` | 8,002 | 113,831 | 1,632,830 | Healthy | Large Java graph; 36,076 unresolved calls. Use for discovery and blast-radius shape, then inspect exact code. |
| `neo4j-testkit-src` | 255 | 5,179 | 21,064 | Healthy | Python/Rust/Go graph exposes frontend/protocol/backend architecture and feature taxonomy. |
| `opencypher-src` | 3 code-bearing files | 3 | 0 | Healthy but source-sparse | The Git denominator has 262 tracked files / 3,670,812 bytes. Gherkin, grammar, AsciiDoc, PDF, JSON, and graph fixtures require non-code classification/direct reading. |

### Commands

```bash
npx --yes @sdsrs/code-graph health-check --json
npx --yes @sdsrs/code-graph search "memory estimation algorithm working set" --json --limit 30
npx --yes @sdsrs/code-graph show PageRankMemoryEstimateDefinition.memoryEstimation --json --refs --impact --context-lines 12
npx --yes @sdsrs/code-graph show Pregel.memoryEstimation --json --refs --impact --context-lines 20
npx --yes @sdsrs/code-graph show DijkstraMemoryEstimateDefinition.memoryEstimation --json --refs --impact --context-lines 12
npx --yes @sdsrs/code-graph search "Bolt protocol state machine auto commit transaction" --json --limit 40
npx --yes @sdsrs/code-graph show CreateAutocommitStatementStateTransition.process --json --refs --impact --context-lines 16
npx --yes @sdsrs/code-graph search "driver backend frontend protocol feature test session result summary" --json --limit 40
npx --yes @sdsrs/code-graph show Feature --json --refs --impact --context-lines 12
```

## 2. GDS Estimation Findings

### CG-MAIN-001: PageRank delegates memory modeling to Pregel

**Path:** `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java`  
**Symbol:** `PageRankMemoryEstimateDefinition.memoryEstimation`

The PageRank estimator delegates to `Pregel.memoryEstimation` with one `DOUBLE` node value and queue/async/sender flags disabled. This is evidence that PageRank's current GDS model is shaped by the generic Pregel execution representation rather than by a universal graph-size multiple.

**Knight Walker consequence:** The PageRank profile should preserve GDS's honest per-execution-state decomposition as an oracle while being free to choose a different edge stream, rank plane, accumulator, partition, spill, or output layout.

### CG-MAIN-002: Pregel charges multiple state classes

**Path:** `neo4j-gds-src/pregel/src/main/java/org/neo4j/gds/beta/pregel/Pregel.java`  
**Symbol:** four-argument `Pregel.memoryEstimation`

The estimator includes:

- Per-node vote bits.
- Per-thread partitioned compute-step state.
- Node-value storage derived from the declared property schema.
- Async or sync message queues for queue-based execution.
- Reducing message arrays, optionally tracking senders, for array-based execution.

**Knight Walker consequence:** `REQ-ADM-002`, `REQ-ADM-006`, and `REQ-ALG-006` are not speculative categories. Upstream already demonstrates that node, worker, message, and algorithm-value planes matter independently. Knight Walker's product distinction is to extend that model to complete conversion/output/OS/spill terms, select a legal plan, enforce it, and publish prediction error.

### CG-MAIN-003: Dijkstra has a different working-set shape

**Path:** `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/paths/dijkstra/DijkstraMemoryEstimateDefinition.java`  
**Symbol:** `DijkstraMemoryEstimateDefinition.memoryEstimation`

The estimator charges:

- A huge-long priority queue.
- Reverse-path mapping.
- Optional relationship-ID mapping.
- Optional per-node target bitset.
- Per-node visited bitset.

**Knight Walker consequence:** Bounded-path admission must model queue/frontier/path/target/output choices; copying PageRank's state model would be structurally wrong. This directly supports algorithm-specific storage contracts rather than a single “graph in RAM” multiplier.

### CG-MAIN-004: Graph resolution is not proof by itself

The `show --refs --impact` result for common `memoryEstimation` symbols sometimes connected many unrelated algorithm facades because Java has numerous identically named methods and the index reports unresolved calls. Exact `code_content`, file path, signature, and direct repository reading remain authoritative.

**Audit rule:** A graph edge can prioritize a read. It cannot by itself establish behavioral identity or licensing provenance.

## 3. Bolt Boundary Findings

### CG-MAIN-005: Neo4j auto-commit is internally an implicit transaction

**Path:** `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/fsm/transition/ready/CreateAutocommitStatementStateTransition.java`  
**Symbol:** `CreateAutocommitStatementStateTransition.process`

The Bolt state transition:

1. Optionally impersonates a user.
2. Begins an implicit transaction with database, access mode, bookmarks, timeout, metadata, and notification configuration.
3. Runs the statement with parameters.
4. Publishes statement metadata and selected database.
5. Moves the protocol state to `AUTO_COMMIT`.

Its impact view shows one production caller and a large hidden test caller set.

**Knight Walker consequence:** A production driver expects auto-commit state-machine behavior, but Knight Walker does not need to copy Neo4j's transactional kernel internally for an immutable read-only analytical profile. It must reproduce only the externally visible state, metadata, streaming, reset, cancellation, and error contracts it advertises. Explicit transactions, bookmarks, writes, routing, impersonation, and administration can remain rejected unless a named workflow earns them.

## 4. TestKit Findings

### CG-MAIN-006: Compatibility is naturally a feature vector

**Path:** `neo4j-testkit-src/nutkit/protocol/feature.py`  
**Symbol:** `Feature`

TestKit enumerates capabilities rather than treating compatibility as one Boolean. Its taxonomy includes:

- Driver APIs and result-consumption behaviors.
- Bolt versions 3.0 through 6.0 and handshake features.
- Authentication and TLS capabilities.
- Retry, bookmark, routing, impersonation, temporal/spatial/vector types.
- Optimizations such as connection reuse and pull pipelining.
- Test-backend details and mock-time behavior.

**Knight Walker consequence:** `REQ-ADAPT-001` should produce a machine-readable feature vector. The first profile can truthfully claim, for example, a specific Bolt 5.x read-only auto-commit subset plus result streaming/summary receipts while marking routing, explicit transactions, writes, bookmark semantics, broad types, and auth modes unsupported. This is more credible than “Neo4j compatible.”

### CG-MAIN-007: TestKit is an adapter oracle, not the product architecture

Search results center on `nutkit/frontend/session.py`, `result.py`, `driver.py`, and protocol request/response objects. These are valuable for black-box behavior and official-driver integration. They do not define graph storage, admission, or low-RAM execution.

**Knight Walker consequence:** TestKit belongs in the verification lane and compatibility boundary. It must not pull driver-management breadth into the algorithm runner.

## 5. openCypher Coverage Limitation

### CG-MAIN-008: Most conformance truth is non-code

The openCypher repository contains 262 tracked files, including:

- `grammar/openCypher.bnf`.
- Gherkin scenarios under `tck/features/`.
- Named graph fixtures and JSON/Cypher graph data.
- Cypher Improvement Proposals and specification documents.
- Build metadata, PDF, and image assets.

The source graph sees only three code-bearing files and no useful edges. Therefore:

1. `graph_indexed` cannot be assigned mechanically to Gherkin/specification files.
2. TCK scenarios relevant to the supported profile must be directly read and classified.
3. Write/admin/general-query scenarios should be `expected_unsupported`, `oracle_only`, or deferred, not silently counted as passing scope.
4. Grammar and TCK license/provenance must be recorded independently of executable source.

## 6. Integration Decisions

| Decision | Evidence | Mega-spec consequence |
|---|---|---|
| Model algorithm state by structural plane | CG-MAIN-001..003 | Keep term-level algorithm profiles and independent storage plans. |
| Add enforcement rather than claim estimates are guarantees | GDS estimators describe state but do not establish OS enforcement | Preserve admission/enforcement/receipt separation. |
| Emulate external auto-commit behavior without a transactional kernel | CG-MAIN-005 | Keep OLTP and explicit transaction breadth outside first profile. |
| Publish a capability matrix | CG-MAIN-006 | Replace binary compatibility language with a versioned feature vector. |
| Use TestKit at the boundary | CG-MAIN-007 | Official-driver verification is an oracle lane, not core architecture. |
| Combine graph search with denominator reconciliation | CG-MAIN-004, CG-MAIN-008 | File coverage is proven only by the union of graph-indexed source and classified/direct-read non-code evidence. |

## 7. Remaining Integration Work

- Reconcile the three agent TSVs against `all-files-denominator.tsv`.
- Attach their file-level evidence IDs to the exact module and profile decisions.
- Resolve license/provenance for grammar, TCK, TestKit, GDS, and Neo4j-derived fixtures.
- Convert the TestKit feature taxonomy into the first machine-readable Knight Walker compatibility manifest.
- Turn the PageRank/Pregel and path estimator findings into calibrated Rust working-set models and adversarial tests, not copied Java architecture.
