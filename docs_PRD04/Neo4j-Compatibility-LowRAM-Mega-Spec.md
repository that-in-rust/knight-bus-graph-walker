# Neo4j Compatibility x Bounded Low-RAM Graph Compute Mega Spec

**Status:** Evidence-reconciled executable specification; implementation is not complete  
**Founder north star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`  
**Evidence spine:** `docs_PRD04/reference-learning/neo4j-compat-lowram/`  
**Existing compatibility input:** `docs_PMF_01/PMF007-Bolt-Cypher-Mega-Spec.md`  
**Implementation language:** Rust  
**Normative terms:** `SHALL`, `SHALL NOT`, `WHEN`, `THEN`, `AND`

> Product thesis: Knight Walker is an artifact-to-answer bounded graph runner. Neo4j compatibility is an adoption adapter. The differentiated product is an enforceable, proof-carrying choice among `fit`, `spill`, `approximate`, and `refuse` for a named analytical workload under a declared resource budget.

This specification deliberately does not say “rewrite Neo4j.” A full database rewrite is neither a user outcome nor a defensible first milestone. The implementation earns compatibility surface area only when the surface advances a named artifact-to-answer workflow, a differential correctness oracle, or a paid learning milestone.

---

# 1. Executable Requirements

## 1.1 Requirement Traceability Rules

Every requirement in this document SHALL satisfy all of the following:

1. It has a stable `REQ-<DOMAIN>-<NNN>.<REV>` identifier.
2. It uses observable `WHEN...THEN...SHALL` language.
3. It maps to one or more tests in Section 2.
4. It names a failure or refusal behavior.
5. Performance and memory claims identify the measurement boundary and environment.
6. Compatibility claims name the exact supported profile and explicit unsupported surface.
7. Repository-derived claims cite a file-level evidence ID after the three evidence lanes are reconciled.

### Reconciled evidence baseline

| Lane | Repositories | Files | Direct reads | Graph indexed | Other classified | Dossier |
|---|---:|---:|---:|---:|---:|---|
| Core compatibility | 4 | 12,847 | 3,495 | 8,315 | 1,037 | `reference-learning/neo4j-compat-lowram/agent-01-core-compatibility.md` |
| GDS and low RAM | 6 | 12,213 | 986 | 6,110 | 5,117 | `reference-learning/neo4j-compat-lowram/agent-02-gds-lowram.md` |
| Verification ecosystem | 10 | 7,202 | 533 | 4,761 | 1,908 | `reference-learning/neo4j-compat-lowram/agent-03-verification-ecosystem.md` |
| **Total** | **20** | **32,262** | **5,014** | **19,186** | **8,062** | `scripts/validate_neo4j_family_evidence.py` passes |

The reconciled union contains 845 binary-classified, 114 generated-classified, and 7,103 non-code-classified files. `direct_read` means the immutable Git blob was consumed and verified; the critical paths cited by the dossiers also received semantic inspection. `graph_indexed` means structurally indexed source, not a claim of complete human semantic understanding.

## 1.2 Product Boundary Requirements

### REQ-PROD-001.0: Preserve the bounded-runner product boundary

**WHEN** a feature, compatibility surface, or architectural component is proposed  
**THEN** its specification SHALL identify the named artifact-to-answer workflow it enables  
**AND** SHALL identify whether it improves correctness, enforced peak RAM, predictability, useful latency, or adoption  
**AND** SHALL classify the work as `must_build`, `adapter_only`, `oracle_only`, `defer`, or `reject`  
**AND** SHALL reject general Neo4j parity as sufficient justification.

### REQ-PROD-002.0: Require a versioned workload profile

**WHEN** Knight Walker accepts a graph computation  
**THEN** the request SHALL name a versioned workload profile  
**AND** the profile SHALL bind accepted artifact schema, query or algorithm semantics, supported configuration, resource model, output schema, and verification oracle  
**AND** an unknown profile or version SHALL be refused before artifact execution.

### REQ-PROD-003.0: Tie compatibility to a target workflow

**WHEN** a Bolt message, Cypher clause, GDS procedure, driver behavior, browser behavior, or OGM behavior is considered  
**THEN** the surface SHALL name at least one target workflow or differential test that requires it  
**AND** a surface with neither SHALL remain deferred or rejected  
**AND** omission SHALL be explicit rather than silently misimplemented.

### REQ-PROD-004.0: Prefer one proof-carrying vertical slice

**WHEN** roadmap work competes between broader compatibility and a complete bounded execution slice  
**THEN** the implementation SHALL prioritize the slice that includes artifact ingestion, admission, plan choice, execution, differential correctness, and receipt emission  
**AND** SHALL not count parser-only, protocol-only, or algorithm-only breadth as an end-to-end milestone.

### REQ-PROD-005.0: Make falsification first-class

**WHEN** a design-partner workflow shows that the dominant pain is ingestion, schema, permissions, UI, or operational workflow rather than bounded graph analysis  
**THEN** the product record SHALL capture that evidence  
**AND** SHALL narrow, redirect, or kill the corresponding workload profile  
**AND** SHALL not reinterpret the interview as validation of low-RAM OLAP.

## 1.3 Portable Artifact Requirements

### REQ-ART-001.0: Validate a versioned artifact manifest

**WHEN** an artifact is opened  
**THEN** the engine SHALL validate the manifest schema version, artifact version, content hashes, endianness, integer widths, graph directionality, identifier domain, and required files  
**AND** SHALL refuse corrupt, incomplete, ambiguous, or unsupported artifacts before algorithm allocation.

### REQ-ART-002.0: Record relevant graph cardinalities

**WHEN** an artifact is admitted  
**THEN** its manifest SHALL expose node count, relationship count, relationship-type counts, label counts, property-column counts, property nullability, identifier width, degree summary, and representation bytes  
**AND** profile-specific estimators SHALL declare which cardinalities they consume  
**AND** a missing required cardinality SHALL refuse the affected profile before estimation.

### REQ-ART-003.0: Separate persistent and ephemeral bytes

**WHEN** representation size is reported  
**THEN** the manifest SHALL distinguish persistent artifact bytes, memory-mapped address ranges, resident-page expectations, retained heap/off-heap bytes, conversion bytes, temporary files, algorithm state, frontier state, and output bytes  
**AND** SHALL not use on-disk file size as a synonym for peak RAM.

### REQ-ART-004.0: Make artifact identity content-addressable

**WHEN** the same logical artifact is reused  
**THEN** its identity SHALL be derived from a canonical manifest plus content hashes  
**AND** receipts SHALL include that identity  
**AND** mutation without a new identity SHALL fail verification.

### REQ-ART-005.0: Support deterministic dense-ID mapping

**WHEN** external identifiers are converted to dense internal identifiers  
**THEN** the mapping SHALL be deterministic for the same canonical input and profile  
**AND** SHALL be versioned and hashed  
**AND** output translation SHALL preserve the external identifier contract without requiring all original strings in the algorithm hot path.

### REQ-ART-006.0: Permit profile-specific sidecars

**WHEN** an algorithm benefits from transpose topology, degree arrays, weight columns, type partitions, destination ordering, hub indexes, landmark tables, or other sidecars  
**THEN** the manifest SHALL identify each sidecar, its derivation, bytes, checksum, and compatible profile versions  
**AND** the estimator SHALL charge any missing sidecar conversion before admitting the run.

## 1.4 Admission and Working-Set Requirements

### REQ-ADM-001.0: Accept a hard resource contract

**WHEN** a user submits a run  
**THEN** the request SHALL accept a hard peak-memory ceiling  
**AND** MAY accept temporary-storage, read-byte, write-byte, wall-time, CPU-time, result-size, and approximation-quality ceilings  
**AND** absent ceilings SHALL resolve through a versioned, visible policy rather than an undocumented default.

### REQ-ADM-002.0: Estimate the complete material working set

**WHEN** admission evaluates an exact run  
**THEN** the estimator SHALL account for fixed engine state, persistent representation residency, per-node state, per-edge state, frontier or queue state, output state, conversion state, concurrency replicas, I/O buffers, spill metadata, allocator slack, and operating-system safety margin  
**AND** SHALL identify terms that are zero because a sidecar can be referenced in place.  
**Evidence:** GDS composite estimates `A02-009746`, ranges `A02-009747`, projection-plus-algorithm orchestration `A02-009428`; Neo4j trackers `A01-002127`, `A01-002119`, `A01-006606`, `A01-009272`.

### REQ-ADM-003.0: Report estimate range and calibration state

**WHEN** an estimate is produced  
**THEN** it SHALL include expected bytes, conservative upper-bound bytes, model version, calibration dataset identifier, confidence or calibration class, and all input terms  
**AND** an uncalibrated estimator SHALL say `uncalibrated` rather than fabricating confidence  
**AND** SHALL NOT emit a calibrated confidence class until its quality gate passes.

### REQ-ADM-004.0: Reject overflow and impossible arithmetic

**WHEN** any count, multiplication, addition, alignment, or unit conversion overflows the estimator's numeric domain  
**THEN** admission SHALL refuse the run  
**AND** SHALL return the failing term and operand classes without allocating algorithm state.

### REQ-ADM-005.0: Charge conversion before execution

**WHEN** the requested profile needs an absent or incompatible representation  
**THEN** admission SHALL model conversion peak memory, temporary bytes, read/write I/O, and retained output  
**AND** SHALL include conversion in the selected plan  
**AND** SHALL not hide conversion behind the algorithm's reported memory.

### REQ-ADM-006.0: Charge concurrency honestly

**WHEN** concurrency is greater than one  
**THEN** the estimator SHALL distinguish shared immutable state, per-worker state, partition imbalance, synchronization state, and output merge state  
**AND** SHALL not multiply or divide the whole estimate by thread count without term-level evidence.

### REQ-ADM-007.0: Admit from upper bound, not optimistic mean

**WHEN** the expected estimate fits but the conservative upper bound exceeds the hard ceiling  
**THEN** an exact in-memory `fit` plan SHALL not be admitted  
**AND** the planner SHALL choose an eligible spill/approximate plan or refuse.

### REQ-ADM-008.0: Produce a refusal before side effects

**WHEN** no eligible plan satisfies all declared ceilings  
**THEN** Knight Walker SHALL refuse before creating partial output or allocating material algorithm state  
**AND** SHALL return the binding constraint, estimate terms, and eligible user-controlled alternatives.

## 1.5 Plan Selection Requirements

### REQ-PLAN-001.0: Select exactly one explicit plan class

**WHEN** admission completes  
**THEN** the result SHALL be exactly one of `fit`, `spill`, `approximate`, or `refuse`  
**AND** SHALL include a canonical plan hash  
**AND** execution SHALL reject a plan whose hash no longer matches the admitted artifact, profile, configuration, or budget.

### REQ-PLAN-002.0: Define `fit` semantics

**WHEN** `fit` is selected  
**THEN** all material retained and transient states SHALL fit under the conservative memory ceiling without algorithmic spill  
**AND** demand paging SHALL not be misreported as zero memory  
**AND** output materialization SHALL be included.

### REQ-PLAN-003.0: Define `spill` semantics

**WHEN** `spill` is selected  
**THEN** the plan SHALL declare buffer sizes, partitioning, run count or bound, merge passes or bound, temporary-storage upper bound, expected read/write bytes, cleanup policy, and crash-recovery behavior  
**AND** every buffer SHALL be charged to the same hard memory contract  
**AND** an unbounded temporary-storage or merge plan SHALL be refused.

### REQ-PLAN-004.0: Define `approximate` semantics

**WHEN** `approximate` is selected  
**THEN** the plan SHALL declare the approximation method, deterministic seed policy, memory bound, quality/error metric, requested threshold, expected threshold, and fallback behavior  
**AND** SHALL never label an approximate answer exact  
**AND** a method unable to satisfy the requested quality contract SHALL be ineligible or refused.

### REQ-PLAN-005.0: Define `refuse` semantics

**WHEN** `refuse` is selected  
**THEN** no algorithm execution SHALL start  
**AND** the receipt SHALL identify the binding resource or unsupported semantic  
**AND** SHALL list only alternatives that the planner can actually construct.

### REQ-PLAN-006.0: Make plan choice deterministic

**WHEN** artifact identity, profile, engine version, estimator version, configuration, budget, and calibration policy are identical  
**THEN** plan class and canonical plan hash SHALL be identical  
**AND** any allowed machine-specific variation SHALL be explicit input to the plan  
**AND** a plan-hash mismatch SHALL refuse execution.

## 1.6 Resource Enforcement Requirements

### REQ-ENF-001.0: Enforce the process memory ceiling

**WHEN** an admitted run starts  
**THEN** the execution environment SHALL enforce a peak-memory boundary using an operating-system-enforced or equivalently supervised mechanism  
**AND** SHALL document whether the boundary covers RSS, cgroup memory, child processes, mapped pages, page cache, shared pages, and allocator arenas  
**AND** an in-process counter alone SHALL not be called a hard ceiling.

### REQ-ENF-002.0: Reserve internal buffers before use

**WHEN** a phase requests an internal buffer, worker-local state, queue, hash table, or output batch  
**THEN** it SHALL reserve against a shared resource ledger before material allocation  
**AND** failed reservation SHALL trigger a legal plan transition, cancellation, or refusal rather than uncontrolled growth.

### REQ-ENF-003.0: Bound output memory

**WHEN** output cardinality is data-dependent  
**THEN** the profile SHALL define streaming, pagination, external materialization, top-k, or hard row/byte limits  
**AND** output memory SHALL be charged to the run  
**AND** exceeding the limit SHALL produce a deterministic termination receipt.

### REQ-ENF-004.0: Bound frontier and skew

**WHEN** a traversal or partitioned algorithm encounters a frontier, hub, or partition larger than its modeled bound  
**THEN** execution SHALL spill, repartition, switch to an admitted alternate representation, or terminate  
**AND** SHALL not allocate beyond the enforced ceiling to preserve latency.

### REQ-ENF-005.0: Support cooperative cancellation

**WHEN** deadline, user cancellation, budget violation, or supervised memory pressure occurs  
**THEN** every long-running phase SHALL observe cancellation within a profile-defined bound  
**AND** SHALL close mappings, files, and temporary artifacts  
**AND** SHALL emit a terminal receipt with partial-work accounting but no answer labeled complete.

### REQ-ENF-006.0: Measure overshoot

**WHEN** the run terminates  
**THEN** the receipt SHALL report admitted ceiling, measured high-water mark, measurement source/scope, and overshoot bytes/percentage  
**AND** quality gates SHALL fail when overshoot exceeds the profile's declared tolerance.

## 1.7 Proof-Carrying Receipt Requirements

### REQ-RCPT-001.0: Emit a machine-verifiable pre-run receipt

**WHEN** admission finishes  
**THEN** Knight Walker SHALL emit a versioned pre-run receipt containing artifact identity, manifest version, graph cardinalities, representation bytes, profile/configuration, estimate terms/range/calibration, selected plan, hard ceilings, expected I/O/temp storage, runtime range or `unknown`, and canonical plan hash.

### REQ-RCPT-002.0: Emit bounded progress events

**WHEN** execution is running  
**THEN** it SHALL expose phase, progress numerator/denominator when knowable, current high-water mark, bytes read/written/mapped/spilled, cancellation state, and cold/warm state  
**AND** observability itself SHALL have bounded memory and event rate.

### REQ-RCPT-003.0: Emit a terminal receipt on every admitted path

**WHEN** an admitted run succeeds, fails, is cancelled, exceeds a deadline, or is killed by a resource boundary  
**THEN** it SHALL produce or durably recover a terminal receipt  
**AND** the receipt SHALL distinguish engine failure from supervised enforcement.

### REQ-RCPT-004.0: Report measured resource decomposition

**WHEN** the terminal receipt is emitted  
**THEN** it SHALL report peak and retained memory with available heap, off-heap, mapped, page-cache, child-process, and temporary-storage components  
**AND** unavailable components SHALL be marked unavailable with the measurement reason.

### REQ-RCPT-005.0: Report estimator error

**WHEN** a measured high-water mark exists  
**THEN** the terminal receipt SHALL report absolute error and percentage error against expected and upper-bound estimates  
**AND** SHALL identify the estimator/calibration version used for admission  
**AND** a missing measurement SHALL report estimator error as unavailable rather than zero.

### REQ-RCPT-006.0: Report deterministic answer identity

**WHEN** a run produces an answer  
**THEN** the terminal receipt SHALL include output schema/version, cardinality, canonical checksum, exact/approximate status, and approximation metric where applicable  
**AND** canonicalization SHALL be independent of worker scheduling where the profile promises deterministic answers  
**AND** canonicalization failure SHALL prevent the answer from being marked proof-complete.

### REQ-RCPT-007.0: Redact secrets

**WHEN** queries, parameters, file paths, properties, or errors contain sensitive values  
**THEN** receipts SHALL include only profile-approved names, hashes, counts, and redacted diagnostics  
**AND** a conformance test SHALL search serialized receipts for supplied secret canaries.

### REQ-RCPT-008.0: Version every proof component

**WHEN** a receipt is serialized  
**THEN** it SHALL identify engine, artifact, manifest, profile, parser, planner, estimator, storage-layout, algorithm, receipt-schema, and oracle versions  
**AND** consumers SHALL reject unknown breaking receipt versions.

## 1.8 Neo4j Adoption Adapter Requirements

### REQ-ADAPT-001.0: Publish an explicit compatibility profile

**WHEN** Knight Walker advertises Neo4j compatibility  
**THEN** it SHALL name a versioned profile listing supported Bolt versions, authentication modes, session modes, transaction modes, Cypher grammar/semantics, value types, notifications, errors, GDS procedures, driver versions, and administrative exclusions  
**AND** SHALL not use the unqualified phrase “Neo4j compatible.”

### REQ-ADAPT-002.0: Use official drivers as black-box clients

**WHEN** a profile claims support for a Neo4j driver  
**THEN** unmodified official-driver code SHALL connect, run profile queries, consume results, and receive receipts  
**AND** driver-specific fixture code SHALL remain outside the engine's semantic core  
**AND** any black-box driver mismatch SHALL fail that driver/profile claim.

### REQ-ADAPT-003.0: Preserve supported query semantics

**WHEN** a query falls inside a supported Cypher profile  
**THEN** parsing, parameter binding, null behavior, path semantics, ordering guarantees, projection, errors, and result value types SHALL match the declared Neo4j/openCypher oracle  
**AND** unsupported semantics SHALL fail before partial execution.

### REQ-ADAPT-004.0: Compile to a bounded canonical plan

**WHEN** a supported Cypher query is accepted  
**THEN** it SHALL compile to a profile-owned canonical intermediate representation  
**AND** that representation SHALL expose resource-relevant operators to admission  
**AND** SHALL not embed Neo4j kernel architecture as a requirement.

### REQ-ADAPT-005.0: Keep OLTP semantics outside the first product slice

**WHEN** an explicit transaction, write clause, schema command, administration command, causal bookmark, cluster-routing behavior, or unsupported procedure is requested  
**THEN** the first bounded-OLAP profile SHALL reject it with a stable Neo4j-shaped error class  
**AND** SHALL not emulate success without the underlying guarantees.

### REQ-ADAPT-006.0: Support profile-gated GDS procedure calls

**WHEN** a supported `gds.*` procedure or function is called through Cypher/Bolt  
**THEN** argument defaults, validation, result columns, execution mode, and error behavior SHALL match the profile oracle  
**AND** memory estimation SHALL feed the Knight Walker admission protocol rather than merely copying a Neo4j estimate string  
**AND** an unsupported mode or configuration SHALL fail with a stable profile error.

### REQ-ADAPT-007.0: Preserve Neo4j as an oracle, not a dependency

**WHEN** differential verification runs  
**THEN** Neo4j/GDS MAY provide expected results, errors, plans, and fixtures  
**AND** production Knight Walker execution SHALL not require a Neo4j server, JVM, or Neo4j data directory unless the profile explicitly describes migration/import.

### REQ-ADAPT-008.0: Separate parser provenance from engine license

**WHEN** code, grammar, tests, fixtures, or behavior from a Neo4j-family repository influences implementation  
**THEN** its license, source commit, transformation, distribution obligation, and clean-room status SHALL be recorded  
**AND** incompatible code SHALL be used only as an oracle unless legal review approves distribution.

### REQ-ADAPT-009.0: Publish compatibility as a feature vector

**WHEN** a compatibility profile is released  
**THEN** it SHALL publish machine-readable feature flags for protocol version, handshake, authentication, connection mode, session mode, transaction mode, values, result consumption, summaries, errors, cancellation, notifications, routing, bookmarks, retries, and optimizations  
**AND** SHALL mark each feature `supported`, `unsupported`, `adapter_only`, or `not_tested` for every claimed driver/version cell  
**AND** an untested cell SHALL NOT inherit support from another language or version.  
**Evidence:** `A03-005854` TestKit `Feature`; `CG-MAIN-006`.

### Initial compatibility profile ladder

| Profile | Purpose | Included | Explicit exclusions |
|---|---|---|---|
| `KB-ACCESS-P0` | First security/dependency/access-path proof | Captured read-only query family, one customer-selected official driver, direct Bolt, bounded results, fit/spill/refuse, receipt | Writes, arbitrary procedures, routing cluster, OGM, Browser parity, universal Cypher |
| `KB-ACCESS-P1` | Earned adoption expansion | Additional founder-required drivers, managed reads/retries, notifications/GQL status, optional single-node routing facade | Write retry semantics, cluster administration, general procedures |
| `KB-ANALYTICS-P2` | Named custom-OLAP profiles | Founder-ordered algorithms through explicit GDS-shaped calls and all four plan decisions | Arbitrary GDS/APOC surface, custom plugins |
| `KB-INTERACTIVE-P3` | Evidence-gated interactive use | Required graph values, plans, counters, cancellation for a named UI workflow | Browser clone |
| `KB-OGM-P4` | Evidence-gated object access | Selected read-only OGM query/load paths | Save/delete/cache/write parity |
| `KB-GENERAL` | Rejected current scope | None | General Neo4j rewrite |

## 1.9 Bolt Profile Requirements

### REQ-BOLT-001.0: Negotiate only a pinned protocol profile

**WHEN** a client sends the Bolt identification and version proposals  
**THEN** the gateway SHALL negotiate only versions explicitly enabled by the compatibility manifest  
**AND** SHALL return the protocol-defined no-match behavior for all others  
**AND** SHALL NOT advertise Manifest, capability, message, or state behavior absent from the tested profile.  
**Evidence:** `A01-000987`, `A01-001097`, `A03-005733`, `A03-005786`, `A03-006741`.

### REQ-BOLT-002.0: Bound framing and PackStream decoding

**WHEN** a client submits chunks, structures, strings, byte arrays, lists, maps, or nested values  
**THEN** the gateway SHALL enforce profile limits for message bytes, chunk count, nesting depth, aggregate values, string/byte bytes, list entries, map entries, and parameter bytes before corresponding allocation  
**AND** malformed or oversized input SHALL fail within the connection decode permit.  
**Evidence:** `A01-001374`, `A01-001389`, `A01-001394`.

### REQ-BOLT-003.0: Preserve the declared finite-state machine

**WHEN** a client sends valid or invalid sequences involving negotiation, authentication, `RUN`, `PULL`, `DISCARD`, failure, interruption, and `RESET`  
**THEN** state transitions, ignored messages, terminal metadata, and recovery SHALL match the selected transcript oracle  
**AND** an invalid sequence SHALL fail without leaking a query, connection, or resource permit.  
**Evidence:** `A01-000989`, `A01-000990`, `A01-001075`, `A03-005797`.

### REQ-BOLT-004.0: Make result production demand-driven

**WHEN** a client sends `PULL` with demand `n`  
**THEN** the runtime SHALL produce no more than the accepted demand before yielding  
**AND** SHALL bound decoded records, encoded records, network buffers, and pending output under the same resource policy  
**AND** a slow or disconnected consumer SHALL backpressure, spill under an admitted plan, or cancel rather than materialize the result.  
**Evidence:** `A01-001236`, `A01-001179`; official result contracts `A03-002332`, `A03-005424`, `A03-003452`, `A03-001905`, `A03-002177`.

### REQ-BOLT-005.0: Propagate discard and reset cancellation

**WHEN** a client discards a stream, disconnects, or sends `RESET`  
**THEN** cancellation SHALL reach artifact reads, algorithm loops, output encoders, and spill operations within the profile bound  
**AND** all associated permits and temporary files SHALL be released or quarantined deterministically  
**AND** a cosmetic protocol reset without execution cancellation SHALL fail conformance.

### REQ-BOLT-006.0: Keep the full receipt out of standard rows

**WHEN** a compatible result is returned  
**THEN** standard record columns SHALL remain profile-compatible  
**AND** the gateway MAY expose a redacted run/receipt identifier through tested extension metadata  
**AND** SHALL persist the complete receipt through a file or sidecar API when official drivers drop unknown summary metadata  
**AND** missing receipt retrieval SHALL make the proof incomplete rather than mutating query rows.  
**Evidence:** Current seeds `KW-CURRENT-001`, `KW-CURRENT-002`, `KW-CURRENT-003`; TestKit summary oracle `A03-006676`.

## 1.10 Cypher Profile Requirements

### REQ-CYPH-001.0: Derive syntax scope from captured queries

**WHEN** `KB-ACCESS-P0` is frozen  
**THEN** its accepted grammar and semantic features SHALL be the union required by named founder query cases plus explicit negative boundaries  
**AND** every accepted feature SHALL map to a bounded canonical operator  
**AND** general grammar coverage without a customer or oracle case SHALL remain unsupported.  
**Evidence:** grammar `A03-006971`; TCK semantics `A03-006975`, `A03-007000`; Neo4j parser scale `A01-005304`, `A01-005396`.

### REQ-CYPH-002.0: Perform typed semantic validation

**WHEN** query text parses  
**THEN** the compiler SHALL validate variable binding, parameter types, null behavior, expression dependencies, path semantics, projections, ordering, and profile-owned function/procedure signatures before admission  
**AND** a syntax-only parse SHALL NOT imply executable compatibility.  
**Evidence:** `A01-004974`, `A01-004992`, `A01-003915`.

### REQ-CYPH-003.0: Refuse unsupported capability before artifact work

**WHEN** a parsed query contains an unsupported clause, write, dynamic procedure, transaction mode, value type, result shape, or unbudgetable operator  
**THEN** the compiler SHALL return a stable compatibility refusal before an expensive artifact scan or material algorithm allocation  
**AND** SHALL distinguish syntax, semantic, capability, and resource refusal phases.

### REQ-CYPH-004.0: Define internal identifier semantics

**WHEN** a query observes Neo4j internal IDs or element IDs  
**THEN** the profile SHALL either provide an explicit artifact correspondence map, compare through a stable domain key, or reject the query  
**AND** SHALL NOT silently drop, renumber, or normalize an identifier that is semantically visible.  
**Evidence:** Differential rules in Agent 03; official graph-value contracts in the driver evidence spine.

## 1.11 GDS Mode Requirements

### REQ-GDS-001.0: Make estimate mode side-effect free

**WHEN** a supported `*.estimate` operation is invoked  
**THEN** it SHALL emit the Knight Walker pre-run plan/estimate receipt without executing the algorithm or publishing output state  
**AND** an unknown required estimate term SHALL cause an honest uncalibrated response or refusal, never a privileged bypass.  
**Evidence:** `A02-009428`, `A02-009746`, `A02-009747`, `A02-007613`.

### REQ-GDS-002.0: Bound stream mode by consumer demand

**WHEN** a supported `*.stream` operation runs  
**THEN** result production SHALL obey Bolt/client backpressure and declared row/byte buffers  
**AND** output state SHALL be included in admission and receipts  
**AND** a stopped consumer SHALL backpressure, spill, or cancel without unbounded accumulation.

### REQ-GDS-003.0: Keep stats mode aggregate-only

**WHEN** a supported `*.stats` operation runs  
**THEN** it SHALL compute only its declared aggregate result and SHALL NOT materialize per-node/per-edge stream rows as an intermediate convenience  
**AND** an implementation that requires row materialization SHALL charge it explicitly or refuse the stats plan.

### REQ-GDS-004.0: Represent mutate as an immutable sidecar

**WHEN** a supported `*.mutate` operation succeeds  
**THEN** it SHALL publish a new content-addressed artifact layer or property sidecar with schema, bytes, checksum, and provenance  
**AND** SHALL NOT create an unaccounted mutable in-memory catalog state.  
**Evidence:** projection/catalog spine `A02-010472`, `A02-008263`, `A02-008363`.

### REQ-GDS-005.0: Bound write as an adapter export

**WHEN** a supported `*.write` operation is selected  
**THEN** it SHALL export through bounded batches with explicit destination, output bytes, I/O, retry/idempotence, and failure cleanup  
**AND** SHALL remain adapter work rather than introducing a Neo4j transactional storage engine  
**AND** unsupported destinations SHALL refuse before execution.

### REQ-GDS-006.0: Forbid privileged admission bypass

**WHEN** a compatibility request includes administrator, `sudo`, or equivalent privileged configuration  
**THEN** the same hard resource admission and enforcement SHALL apply  
**AND** privilege MAY authorize data/capability access but SHALL NOT disable an unknown estimator or memory guard.  
**Evidence:** GDS bypass/skip behavior observed in `A02-007613` is an oracle difference Knight Walker intentionally rejects.

## 1.12 Determinism Mode Requirements

### REQ-DET-001.0: Declare the execution reproducibility mode

**WHEN** a floating-point, randomized, partitioned, or parallel algorithm is admitted  
**THEN** the plan SHALL declare `deterministic_strict`, `deterministic_tolerant`, or `throughput` mode  
**AND** SHALL bind seed, worker count, partition/reduction order, floating-point policy, and checksum/tolerance promise  
**AND** a throughput result SHALL NOT inherit a strict deterministic checksum claim.

## 1.13 Algorithm-Shaped Storage Requirements

### REQ-ALG-001.0: Prioritize founder-ordered algorithm families

**WHEN** selecting implementation order  
**THEN** the default sequence SHALL be bounded paths, components/WCC, PageRank/centrality, NodeSimilarity/kNN, Louvain/Leiden, triangles/clustering, then FastRP/embeddings  
**AND** changing the order SHALL cite customer or benchmark evidence  
**AND** an unsupported priority change SHALL be rejected by roadmap review.

### REQ-ALG-002.0: Give each algorithm a storage contract

**WHEN** an algorithm profile is specified  
**THEN** it SHALL declare traversal direction, required topology ordering, mutable state, read/write access pattern, frontier behavior, convergence state, weight/property needs, output shape, deterministic policy, and eligible representations  
**AND** SHALL not inherit a universal graph representation by default.

### REQ-ALG-003.0: Offer multiple honest trade-off plans

**WHEN** an algorithm has viable RAM/latency trade-offs  
**THEN** the planner SHALL expose at least the eligible exact in-memory, exact bounded-spill, and approximate bounded options  
**AND** each option SHALL have independent estimates and receipts  
**AND** the API SHALL permit a user to forbid approximation or spill  
**AND** SHALL refuse when the remaining allowed options cannot satisfy the contract.

### REQ-ALG-004.0: Specialize bounded path execution

**WHEN** bounded path search is selected  
**THEN** eligible plans SHALL model visited state, predecessor/path materialization, frontier skew, direction sidecars, result caps, and hub behavior  
**AND** SHALL support a bounded external frontier or refuse before an unbounded path explosion.
**Evidence:** BFS kernel/estimate `A02-007002`, `A02-007004`; bounded traversal adapter `A02-000312`, `A02-005290`.

### REQ-ALG-005.0: Specialize WCC execution

**WHEN** WCC/components is selected  
**THEN** eligible plans SHALL model parent/label state, compression or hooking state, edge scans, convergence/supersteps, partition state, and component output  
**AND** deterministic component identifiers SHALL be canonicalized when promised  
**AND** an oversized label/partition state SHALL spill under an admitted plan or terminate without breaching the ceiling.
**Evidence:** WCC kernel/estimate `A02-007155`, `A02-007156`.

### REQ-ALG-006.0: Specialize PageRank execution

**WHEN** PageRank is selected  
**THEN** eligible plans SHALL model rank vectors, next vectors or accumulators, degree/normalization state, dangling mass, edge access, convergence/error threshold, iteration cap, and output  
**AND** the receipt SHALL distinguish preparation time from iterative compute  
**AND** reaching the iteration cap without the declared convergence condition SHALL produce a non-converged result status.
**Evidence:** PageRank kernel/estimate `A02-006976`, `A02-006978`; Pregel decomposition `CG-MAIN-001`, `CG-MAIN-002`.

### REQ-ALG-007.0: Specialize similarity execution

**WHEN** NodeSimilarity or kNN is selected  
**THEN** eligible plans SHALL model feature/neighborhood representation, candidate generation, pair state, top-k heaps, skew, pruning, approximation quality, and output bounds  
**AND** all-pairs materialization SHALL be refused unless explicitly budgeted.
**Evidence:** NodeSimilarity kernel/estimate `A02-007086`, `A02-007087`; kNN kernel/estimate `A02-007053`, `A02-007056`.

### REQ-ALG-008.0: Specialize community execution

**WHEN** Louvain or Leiden is selected  
**THEN** eligible plans SHALL model community labels, weights, move proposals, aggregation levels, induced graphs, convergence, randomness/seed, and hierarchy output  
**AND** level transitions SHALL be visible in progress receipts  
**AND** a level whose induced graph cannot satisfy any legal plan SHALL spill, approximate under contract, or refuse.
**Evidence:** Louvain `A02-006940`, `A02-006942`; Leiden `A02-006928`, `A02-006930`.

### REQ-ALG-009.0: Specialize triangle execution

**WHEN** triangle or clustering metrics are selected  
**THEN** eligible plans SHALL model orientation, degree ordering, sorted-neighbor requirements, intersections, temporary wedges, skew, and output  
**AND** representation conversion SHALL be charged if adjacency is not intersection-ready  
**AND** missing intersection-ready storage whose conversion cannot fit SHALL refuse the run.
**Evidence:** triangle intersection kernel/estimate `A02-007125`, `A02-007127`.

### REQ-ALG-010.0: Specialize FastRP execution

**WHEN** FastRP or embedding generation is selected  
**THEN** eligible plans SHALL model embedding dimensions, random seed/state, feature columns, intermediate vectors, iteration passes, normalization, output persistence, and quantized or streamed alternatives  
**AND** output vectors SHALL be charged even when topology is mapped  
**AND** output beyond the declared memory/storage contract SHALL stream, spill, or refuse rather than accumulate silently.
**Evidence:** FastRP kernel/estimate `A02-006780`, `A02-006782`.

## 1.14 Storage Layout Requirements

### REQ-STOR-001.0: Keep topology immutable during analytical execution

**WHEN** an admitted analytical run begins  
**THEN** its topology and profile sidecars SHALL be immutable and content-addressed for the run  
**AND** mutable algorithm state SHALL live in separate bounded planes  
**AND** a changed source graph SHALL produce a new artifact identity  
**AND** mutation under an admitted identity SHALL terminate verification.

### REQ-STOR-002.0: Permit zero-copy topology references

**WHEN** the selected algorithm can consume the artifact's topology directly  
**THEN** the plan SHALL reference it without duplicate projection  
**AND** SHALL charge mapped residency expectations and page-fault/I/O behavior honestly  
**AND** SHALL report duplicate topology bytes as zero.

### REQ-STOR-003.0: Make property planes columnar and selective

**WHEN** a profile requires node or relationship properties  
**THEN** it SHALL map or decode only declared columns and row domains  
**AND** shall charge decompression/decoding buffers  
**AND** irrelevant property planes SHALL not enter the working set.

### REQ-STOR-004.0: Support profile-specific numeric widths

**WHEN** cardinalities and value domains fit narrower integer or floating-point representations  
**THEN** the storage profile MAY select narrower widths  
**AND** SHALL validate overflow and precision policy  
**AND** SHALL record widths in the manifest and receipt.

### REQ-STOR-005.0: Bound external-memory partitions

**WHEN** a spill plan partitions topology or state  
**THEN** partition size SHALL be derived from the enforced memory ledger  
**AND** skew SHALL be detected before or during partitioning  
**AND** oversized partitions SHALL be recursively partitioned, processed with a legal alternate plan, or refused.

### REQ-STOR-006.0: Make temporary artifacts recoverable and disposable

**WHEN** a spill or conversion plan writes temporary data  
**THEN** each temporary artifact SHALL have run identity, format version, checksum policy, lifetime, and cleanup state  
**AND** cancellation/crash cleanup SHALL be idempotent  
**AND** unrelated runs SHALL not consume or delete it.

## 1.15 Verification Requirements

### REQ-VER-001.0: Maintain a three-level oracle hierarchy

**WHEN** a profile is tested  
**THEN** verification SHALL include a tiny hand-verifiable oracle, a differential Neo4j/openCypher/GDS oracle where licensing and availability permit, and a production-shaped artifact benchmark  
**AND** no performance result SHALL substitute for semantic correctness  
**AND** oracle disagreement SHALL block the corresponding compatibility or performance claim.

### REQ-VER-002.0: Reuse upstream conformance fixtures by role

**WHEN** openCypher TCK, Neo4j TestKit, driver tests, GDS tests, or procedure fixtures are imported or translated  
**THEN** each fixture SHALL be tagged `must_pass`, `expected_unsupported`, `oracle_only`, or `license_blocked`  
**AND** profile coverage SHALL count only `must_pass` fixtures.

### REQ-VER-003.0: Compare canonical outputs

**WHEN** Neo4j/GDS and Knight Walker execute the same supported job  
**THEN** the harness SHALL canonicalize schema, row ordering policy, numeric tolerance, identifiers, paths, nulls, and floating-point values before comparison  
**AND** SHALL retain both raw outputs when a mismatch occurs.

### REQ-VER-004.0: Compare errors and refusal phase

**WHEN** a request is invalid, unsupported, or over budget  
**THEN** tests SHALL verify error class, stable code, redacted diagnostic, and whether rejection occurred at parse, semantic, admission, or execution phase  
**AND** over-budget work SHALL be rejected before material algorithm allocation.

### REQ-VER-005.0: Measure cold and warm runs separately

**WHEN** latency, I/O, or RAM is benchmarked  
**THEN** the harness SHALL label cache condition, artifact preparation state, process lifetime, machine configuration, concurrency, and repetitions  
**AND** SHALL not average cold preparation into warm query latency without separately reporting both.

### REQ-VER-006.0: Compare total time-to-answer

**WHEN** a specialized storage plan requires conversion or sidecar preparation  
**THEN** benchmarks SHALL report preparation, admission, execution, output, and cleanup separately  
**AND** SHALL report amortized time only with an explicit reuse count.

### REQ-VER-007.0: Calibrate memory estimates continuously

**WHEN** a benchmark receipt has a measured high-water mark  
**THEN** the calibration pipeline SHALL retain estimator inputs, prediction, observation, error, environment, and profile version  
**AND** estimator changes SHALL be validated on held-out artifacts to avoid fitting only the demo graph.

### REQ-VER-008.0: Make benchmark claims reproducible

**WHEN** a RAM or latency delta is published  
**THEN** the repository SHALL contain command, artifact identity or generator seed, environment manifest, Neo4j/GDS version/configuration, Knight Walker version/configuration, raw receipts, and summarization code  
**AND** SHALL label estimates separately from measurements  
**AND** a claim missing its reproduction bundle SHALL not be published as measured evidence.

## 1.16 Security and Operational Requirements

### REQ-SEC-001.0: Default to local read-only analytical execution

**WHEN** the first product profile starts  
**THEN** it SHALL expose only read-only analytical operations over an immutable artifact  
**AND** SHALL bind to local-only transport by default  
**AND** remote exposure SHALL require explicit authentication, authorization, and transport configuration.

### REQ-SEC-002.0: Treat artifacts as untrusted input

**WHEN** parsing manifests, topology, sidecars, properties, queries, Bolt values, or spill files  
**THEN** the engine SHALL bounds-check offsets/counts/lengths, reject path traversal, cap recursive or nested structures, and avoid unchecked allocation from input-controlled sizes.

### REQ-SEC-003.0: Make failure cleanup deterministic

**WHEN** an execution fails or is cancelled  
**THEN** output SHALL remain absent or explicitly incomplete  
**AND** locks, mappings, descriptors, reservations, worker tasks, and temporary files SHALL reach a documented terminal state  
**AND** rerunning cleanup SHALL be safe.

### REQ-OPS-001.0: Run as a boring local binary or container

**WHEN** a user installs the first supported profile  
**THEN** it SHALL run as a documented local binary or container without requiring a cluster, JVM, Neo4j server, external catalog, or control plane  
**AND** the receipt SHALL be available as a file and through the compatibility surface.

### REQ-OPS-002.0: Preserve backward-readable receipts

**WHEN** engine versions change  
**THEN** current tooling SHALL read all receipt versions declared supported by policy  
**AND** breaking schema changes SHALL use a new major receipt version and migration or explicit refusal.

## 1.17 Product-Learning Requirements

### REQ-PMF-001.0: Validate the receipt as a paid outcome

**WHEN** design-partner interviews and pilots run  
**THEN** evidence SHALL distinguish interest in lower RAM from willingness to pay for predictable admission, enforced bounds, and audit receipts  
**AND** SHALL record the current workaround, frequency, cost, authority, urgency, and paid next step  
**AND** interest without a committed next step SHALL not unlock roadmap breadth.

### REQ-PMF-002.0: Use a named first ICP

**WHEN** a workload enters the first roadmap  
**THEN** it SHALL name a security, IAM, dependency, SBOM, or access-path persona; artifact; recurring question; current tool; resource failure; and budget owner  
**AND** generic “graph users” SHALL not qualify.

### REQ-PMF-003.0: Gate breadth on a paid design partner

**WHEN** the first proof slice is correct and bounded  
**THEN** expansion to a second algorithm family or broader Neo4j surface SHALL require a paid pilot, signed design-partner commitment, or equivalent strong founder evidence  
**AND** GitHub interest alone SHALL not trigger a compatibility rewrite.

---

# 2. Test Matrix

The matrix below is normative. A requirement is incomplete until every mapped test is implemented or explicitly marked as a future milestone with owner and dependency.

| Test ID | Requirements | Test level | Fixture or environment | Passing observation |
|---|---|---|---|---|
| TEST-PROD-001 | PROD-001..005 | Spec lint + roadmap audit | Requirement registry | Every item has workflow, class, metric, and falsification rule; broad parity items fail lint. |
| TEST-ART-001 | ART-001, ART-004 | Unit + corruption matrix | Valid artifact; bit flips; missing files; version skew | Valid opens; every corrupt/ambiguous variant refuses before algorithm allocation. |
| TEST-ART-002 | ART-002, ART-003, ART-006 | Manifest golden | Tiny, skewed, weighted, typed artifacts | Cardinalities and byte planes match independently computed truth. |
| TEST-ART-003 | ART-005 | Property test | Random external identifiers and shuffled input | Canonical input produces identical dense mapping/hash; round-trip output preserves IDs. |
| TEST-ADM-001 | ADM-001..003 | Estimator golden | Hand-sized graph with analytic byte model | Every term, expected bound, upper bound, model version, and calibration class match golden values. |
| TEST-ADM-002 | ADM-004 | Property/fuzz | Near-`u64` and `usize` cardinalities | Every overflow refuses without allocation panic or wraparound. |
| TEST-ADM-003 | ADM-005, ART-006 | Integration | Missing transpose/property sidecar | Conversion bytes/I/O/temp appear in estimate and selected plan. |
| TEST-ADM-004 | ADM-006 | Parameter sweep | Concurrency 1..machine limit | Shared and worker-local terms scale according to the declared model; no hidden replicas. |
| TEST-ADM-005 | ADM-007, ADM-008 | Boundary test | Ceiling just below/at/above upper bound | `fit` only at legal bound; alternatives or refusal occur before material allocation. |
| TEST-PLAN-001 | PLAN-001, PLAN-006 | Determinism | Repeated identical request across process restarts | Plan class and canonical plan hash are identical. |
| TEST-PLAN-002 | PLAN-002 | Integration | In-memory eligible artifact | No algorithm spill; all measured state stays within ceiling tolerance. |
| TEST-PLAN-003 | PLAN-003, STOR-005, STOR-006 | Integration + failure injection | Forced multi-run spill and skewed partition | Bounded buffers, declared passes/I/O/temp, recursive handling, idempotent cleanup. |
| TEST-PLAN-004 | PLAN-004 | Differential/statistical | Approximate profile with exact small oracle | Quality metric and seed policy satisfy contract; answer is labeled approximate. |
| TEST-PLAN-005 | PLAN-005 | Refusal | Impossible resource and unsupported-semantic cases | No partial output or material allocation; alternatives are executable. |
| TEST-ENF-001 | ENF-001, ENF-006 | Supervised process | Linux cgroup v2; macOS documented supervisor fallback | High-water mark and overshoot are measured; deliberate breach terminates within policy. |
| TEST-ENF-002 | ENF-002 | Concurrency stress | Worker/queue/hash/output allocation pressure | Reservations prevent aggregate unaccounted growth and fail deterministically. |
| TEST-ENF-003 | ENF-003, ENF-004 | Adversarial graph | High-degree hub and exploding path output | Frontier/output spill or termination occurs without ceiling breach. |
| TEST-ENF-004 | ENF-005, SEC-003 | Cancellation/failure injection | Every long phase and spill merge | Cancellation latency meets profile bound; cleanup is complete and idempotent. |
| TEST-RCPT-001 | RCPT-001, RCPT-008 | JSON/schema golden | One case per plan class | Pre-run receipt contains every required version, estimate, ceiling, plan, and identity field. |
| TEST-RCPT-002 | RCPT-002 | Event-rate test | Long multi-phase run | Progress is monotonic/bounded; telemetry memory and event rate remain under contract. |
| TEST-RCPT-003 | RCPT-003, RCPT-004 | Terminal-path matrix | Success, parse fail, admission refuse, cancel, deadline, OOM supervisor | Each admitted path yields/recoverably persists a correctly classified terminal receipt. |
| TEST-RCPT-004 | RCPT-005, VER-007 | Calibration test | Held-out artifacts and repeated runs | Error fields are exact; calibration data retains environment and versions. |
| TEST-RCPT-005 | RCPT-006 | Determinism | Repeated thread counts and schedules | Canonical exact output checksum is stable where profile promises determinism. |
| TEST-RCPT-006 | RCPT-007 | Secret-canary scan | Secrets in query, parameters, paths, properties, errors | No secret canary appears in serialized receipt, log, or error metadata. |
| TEST-ADAPT-001 | ADAPT-001, ADAPT-005 | Profile schema + negative suite | Compatibility manifest | Every supported/unsupported surface is machine-readable; excluded writes/admin fail stably. |
| TEST-ADAPT-002 | ADAPT-002 | Black-box driver matrix | Official Python first; Java/JS/Go/.NET only when profile-gated | Unmodified client connects, executes, consumes, and obtains receipt. |
| TEST-ADAPT-003 | ADAPT-003, ADAPT-004 | TCK-derived differential | Supported Cypher scenarios | Results/errors match canonical oracle and compile to resource-visible plan IR. |
| TEST-ADAPT-004 | ADAPT-006 | GDS differential | Profile-gated procedure fixtures | Defaults, errors, columns, output, and Knight Walker admission agree with contracts. |
| TEST-ADAPT-005 | ADAPT-007 | Packaging test | Clean machine/container | Production binary executes with no Neo4j/JVM runtime dependency. |
| TEST-ADAPT-006 | ADAPT-008 | License/provenance audit | Imported grammar, fixtures, code, and generated assets | Every external item has source commit, license, use role, and distribution decision. |
| TEST-ADAPT-007 | ADAPT-009 | Capability-manifest audit | TestKit feature taxonomy plus selected driver matrix | Every matrix cell is explicit; unsupported/not-tested cells never inherit support. |
| TEST-BOLT-001 | BOLT-001..002 | Transcript + property/fuzz | Selected BoltStub versions, malformed chunks and nested PackStream values | Only pinned versions negotiate; all decode limits fail inside the connection permit. |
| TEST-BOLT-002 | BOLT-003, BOLT-005 | State-machine/failure injection | RUN/PULL/DISCARD/FAILURE/RESET/disconnect scripts | State and cancellation match the profile; no query/connection/temp/permit leak. |
| TEST-BOLT-003 | BOLT-004, BOLT-006 | Black-box slow-consumer integration | Official driver with small fetch/demand and receipt lookup | Production follows demand; buffers stay bounded; rows remain unchanged; full receipt remains retrievable. |
| TEST-CYPH-001 | CYPH-001..003 | Profile compiler + differential | Founder query corpus, tagged TCK subset, rejected corpus | Accepted syntax/semantics map to bounded IR; every other construct refuses at the correct phase. |
| TEST-CYPH-002 | CYPH-004 | Identifier differential | Domain-key, correspondence-map, and internal-ID-dependent cases | Supported IDs compare through declared mapping; unsupported internal-ID semantics refuse. |
| TEST-GDS-001 | GDS-001, GDS-006 | Estimate/privilege negative test | Supported estimate call with normal and privileged metadata | No algorithm side effect; identical hard admission; unknown terms never bypass guard. |
| TEST-GDS-002 | GDS-002..003 | Backpressure + aggregate test | Slow stream consumer and stats call | Stream buffers remain bounded; stats never materializes hidden per-row output. |
| TEST-GDS-003 | GDS-004..005 | Artifact/export integration | Mutate sidecar and bounded write destination with injected failure | New immutable sidecar is addressed/provenanced; export batches and cleanup obey resource contract. |
| TEST-DET-001 | DET-001 | Cross-schedule repeatability | Strict/tolerant/throughput plans over thread and partition sweeps | Receipt binds mode; strict checksums or tolerant deltas pass; throughput never claims strict identity. |
| TEST-ALG-001 | ALG-001, ALG-002 | Registry/spec lint | Algorithm profile registry | Priority and complete access/storage/state contracts exist for every advertised profile. |
| TEST-ALG-002 | ALG-003 | Planner matrix | Multiple budgets per algorithm | Eligible exact/spill/approximate/refuse choices appear with independent estimates. |
| TEST-ALG-003 | ALG-004 | Differential + adversarial | Bounded BFS/shortest path, hub, path explosion | Correct bounded answer; frontier/output behavior honors plan and receipt. |
| TEST-ALG-004 | ALG-005 | Differential | WCC small truth + production artifact | Canonical components match oracle; state and scans match estimate. |
| TEST-ALG-005 | ALG-006 | Differential + convergence | PageRank small truth + production artifact | Scores within declared tolerance; preparation/iterations separated; bound enforced. |
| TEST-ALG-006 | ALG-007 | Differential + quality | Similarity exact tiny + approximate production | Top-k/quality/output contracts hold; no accidental all-pairs state. |
| TEST-ALG-007 | ALG-008 | Differential + seeded repeat | Louvain/Leiden fixtures | Quality and deterministic-seed policy hold; level transitions receipted. |
| TEST-ALG-008 | ALG-009 | Differential + skew | Triangle fixtures and high-degree graph | Counts match; orientation/conversion/skew memory are charged. |
| TEST-ALG-009 | ALG-010 | Differential + output stress | FastRP fixed seed and dimensions | Vector schema/quality policy holds; intermediate and output bytes remain bounded. |
| TEST-STOR-001 | STOR-001, STOR-002 | Mapping/accounting | Reusable immutable artifact | Artifact identity stable; duplicate topology zero; mapped residency measured honestly. |
| TEST-STOR-002 | STOR-003, STOR-004 | Selectivity/width matrix | Wide-property artifact and numeric boundaries | Only requested columns decoded; widths validate overflow/precision and reduce charged bytes. |
| TEST-VER-001 | VER-001..004 | Verification ladder | Tiny truth, upstream oracle, production-shaped artifact | Correctness, errors, and refusal phase are proven at all three levels. |
| TEST-VER-002 | VER-005, VER-006 | Benchmark harness | Cold/warm and prepare/reuse scenarios | Phase timings/cache state reported separately; amortization names reuse count. |
| TEST-VER-003 | VER-008 | Reproduction test | Fresh checkout and documented dependencies | Published claim reproduces from stored artifact identity/raw receipts/summary code. |
| TEST-SEC-001 | SEC-001, SEC-002 | Security/fuzz | Network defaults and malformed inputs | Local read-only default; malformed values never drive unchecked offsets or allocations. |
| TEST-OPS-001 | OPS-001, OPS-002 | Packaging + schema compatibility | Binary/container and historical receipts | Boring local run succeeds; current tools read supported old receipts or refuse by policy. |
| TEST-PMF-001 | PMF-001..003 | Founder evidence audit | Interview/pilot records | Named ICP, pain/cost/authority, receipt value, and paid evidence gate roadmap breadth. |

---

# 3. TDD Plan

## 3.1 Delivery Doctrine

Each milestone SHALL use `STUB -> RED -> GREEN -> REFACTOR`. A milestone is complete only when its end-to-end proof runs from the user boundary to a terminal receipt. Tests SHALL be written against traits and artifact/receipt schemas before implementation details.

## 3.2 Phase 0: Evidence and Contract Lock

**RED**

1. Generate the 32,262-file Neo4j-family denominator.
2. Require exactly one reconciled evidence row per `(repo, path, git_blob)`.
3. Fail while any evidence lane is missing, duplicated, stale, or uses an illegal coverage state.
4. Lint this specification for IDs, WHEN/THEN/SHALL clauses, test mappings, founder classifications, and unresolved evidence placeholders.

**GREEN**

1. Reconcile all three lane TSVs.
2. Promote founder-critical estimator, planner, storage, parser, protocol, TCK/TestKit, and priority-algorithm sources to `direct_read`.
3. Cite file-level evidence IDs in compatibility and algorithm appendices.
4. Freeze the first workload profile and oracle.

**REFACTOR**

1. Collapse duplicate upstream surfaces into explicit build/adapter/oracle/defer/reject decisions.
2. Remove requirements that do not advance a named workflow or verification loop.

## 3.3 Phase 1: Admission and Receipt Core

**RED:** Implement `TEST-ART-001..003`, `TEST-ADM-001..005`, and `TEST-RCPT-001` against Rust traits and versioned schemas.  
**GREEN:** Build artifact manifest validation, checked arithmetic, term-level estimator, plan enum, and pre-run receipt with no algorithm execution.  
**REFACTOR:** Keep artifact, estimator, planner, and receipt crates independent from Bolt/Cypher/GDS adapters.

Recommended Rust boundaries:

```text
artifact_manifest_core
    -> workload_profile_core
    -> working_set_estimator
    -> bounded_plan_selector
    -> execution_resource_ledger
    -> proof_receipt_schema

neo4j_bolt_adapter
    -> cypher_profile_compiler
    -> workload_profile_core

algorithm_profile_runner
    -> storage_layout_provider
    -> execution_resource_ledger
    -> proof_receipt_schema
```

## 3.4 Phase 2: Hard Enforcement and Forced Spill

**RED:** Implement `TEST-PLAN-003`, `TEST-ENF-001..004`, and `TEST-RCPT-002..004` with deliberately tiny ceilings and failure injection.  
**GREEN:** Add shared reservations, supervised execution boundary, bounded buffers, spill files, cancellation, cleanup, progress events, and measured terminal receipts.  
**REFACTOR:** Unify phase accounting without coupling algorithms to OS-specific enforcement.

## 3.5 Phase 3: First Founder Slice

Default candidate: a bounded path/access-path job over a portable security/IAM/dependency artifact, using the existing Bolt/Cypher neighborhood profile as an adoption seed.

**RED:** Implement `TEST-ADAPT-002..003`, `TEST-BOLT-001..003`, `TEST-CYPH-001..002`, `TEST-ALG-003`, `TEST-STOR-001`, `TEST-VER-001..003`, and `TEST-PMF-001`.  
**GREEN:** Wire unmodified official Python driver -> Bolt -> bounded Cypher profile -> admission -> fit/spill/refuse -> exact path answer -> differential Neo4j oracle -> receipt.  
**REFACTOR:** Ensure Neo4j details terminate at the adapter/compiler boundary; the runner consumes only canonical workload IR.

Exit artifact:

```text
production-shaped query
        |
official Neo4j driver
        |
bounded Bolt/Cypher profile
        |
artifact + budget + canonical workload IR
        |
estimate -> fit | spill | refuse
        |
enforced execution
        |
answer + before/during/after receipt
        |
differential correctness + Neo4j/GDS RAM/latency comparison
```

## 3.6 Phase 4: PageRank as the First Iterative OLAP Proof

**RED:** Implement `TEST-GDS-001..003`, `TEST-DET-001`, and `TEST-ALG-005` over tiny truth, Neo4j/GDS differential, memory boundary sweep, cold/warm preparation, and held-out estimator calibration.  
**GREEN:** Add GDS-shaped estimate/stream/stats/mutate/write behavior for the PageRank profile, algorithm-shaped degree/rank planes, exact in-memory plan, exact bounded-spill plan, deterministic output checksum, and preparation-versus-iteration receipts.  
**REFACTOR:** Extract only storage/iteration primitives demonstrated reusable by WCC or another approved profile.

## 3.7 Phase 5: Earn Additional Algorithms

Implement WCC, similarity, community, triangles, and FastRP only in founder-evidence order. Every addition repeats the full loop:

1. Profile and oracle tests RED.
2. Full-working-set model RED.
3. Fit/spill/approximate/refuse matrix RED.
4. Algorithm-shaped storage and executor GREEN.
5. Calibration and adversarial skew GREEN.
6. Receipt and published comparison GREEN.
7. Shared abstractions only after two concrete profiles prove them.

## 3.8 Progress Journal Contract

After every meaningful RED, GREEN, or REFACTOR transition, update:

`journals/neo4j-compat-lowram-mega-spec-progress.md`

Each checkpoint SHALL record exact tests, status, files in motion, next three actions, blockers, design decisions, and measured counts/bytes/errors.

---

# 4. Quality Gates

## 4.1 Specification Gates

- `QG-SPEC-001`: 100% of requirements have stable IDs and observable WHEN/THEN/SHALL clauses.
- `QG-SPEC-002`: 100% of requirements map to at least one test ID.
- `QG-SPEC-003`: 100% of compatibility surfaces have build/adapter/oracle/defer/reject classification.
- `QG-SPEC-004`: No unqualified “Neo4j compatible,” “hard memory limit,” “deterministic,” “faster,” or “lower RAM” claim survives without a profile and measurement contract.
- `QG-SPEC-005`: The first milestone is a complete artifact-to-answer proof, not subsystem breadth.

## 4.2 Evidence Gates

- `QG-EVID-001`: Denominator contains exactly one row per Git-tracked file across exactly 20 assigned repositories.
- `QG-EVID-002`: Evidence union exactly matches all denominator `(repo, path, blob, bytes, extension)` values.
- `QG-EVID-003`: No duplicate evidence IDs or repo/path keys.
- `QG-EVID-004`: Every relevance `>= 80` source is `direct_read`.
- `QG-EVID-005`: Founder-critical estimator, planner, storage, protocol, parser, TCK/TestKit, and priority-algorithm sources are `direct_read`.
- `QG-EVID-006`: Every architectural claim cites local code/test evidence or is labeled hypothesis.
- `QG-EVID-007`: Every borrowed/translated fixture or implementation records license and provenance.

## 4.3 Correctness Gates

- `QG-CORR-001`: Tiny hand truth passes exactly.
- `QG-CORR-002`: Supported Cypher/Bolt/GDS profile passes its differential oracle.
- `QG-CORR-003`: Exact algorithm output meets declared integer exactness or floating tolerance.
- `QG-CORR-004`: Approximate output meets declared quality bound and is visibly labeled approximate.
- `QG-CORR-005`: Unsupported semantics fail before partial answer emission.
- `QG-CORR-006`: Result checksum is stable across permitted concurrency/scheduling variation where promised.

## 4.4 Resource Gates

- `QG-RAM-001`: Peak boundary uses a documented OS/supervisor measurement scope; internal counters alone do not pass.
- `QG-RAM-002`: Observed peak stays at or below declared ceiling plus the profile's explicit enforcement tolerance.
- `QG-RAM-003`: Estimator upper bound underpredicts observed peak in no more than the declared held-out calibration rate.
- `QG-RAM-004`: Spill plan temporary bytes and I/O remain within declared upper bounds.
- `QG-RAM-005`: Output, conversion, page residency, concurrency, and cleanup states are included in accounting.
- `QG-RAM-006`: A budget one unit below the legal bound chooses spill/approximate/refuse, never uncontrolled fit.

Initial calibration targets, to be replaced by measured profile-specific values:

| Stage | Underprediction tolerance | Median expected-estimate error | Enforcement overshoot |
|---|---:|---:|---:|
| Prototype | 0% against conservative upper bound on test corpus | <= 25% | <= max(64 MiB, 5% of ceiling) |
| Design-partner pilot | 0% on supported profile corpus | <= 15% | <= max(32 MiB, 3% of ceiling) |
| Stable profile | 0% on release qualification corpus | <= 10% | <= max(16 MiB, 2% of ceiling) |

These are engineering targets, not current measurements.

## 4.5 Latency and Benchmark Gates

- `QG-PERF-001`: Cold preparation, warm execution, admission, output, and cleanup are reported separately.
- `QG-PERF-002`: Neo4j/GDS and Knight Walker run on the same machine class, artifact semantics, concurrency policy, and output contract.
- `QG-PERF-003`: Published values include median, p95, p99 or maximum as appropriate, repetitions, variance, and raw receipts.
- `QG-PERF-004`: A low-RAM plan may be slower, but its time/resource trade-off SHALL be explicit and useful to the target workflow.
- `QG-PERF-005`: Preparation amortization is reported only with explicit reuse count and unamortized total time.

## 4.6 Operational and Security Gates

- `QG-OPS-001`: Clean local binary/container test requires no Neo4j or JVM runtime.
- `QG-OPS-002`: Cancellation and crash cleanup are idempotent.
- `QG-OPS-003`: Receipt schema compatibility tests cover all supported historical versions.
- `QG-SEC-001`: Secret-canary scan passes across receipts, logs, and driver errors.
- `QG-SEC-002`: Artifact/query/protocol fuzzing has no unchecked allocation, out-of-bounds access, panic, or path traversal.
- `QG-SEC-003`: Network transport is local and read-only by default.

## 4.7 Founder Gates

- `QG-PMF-001`: At least five target-role interviews identify a recurring artifact-to-answer job and current resource failure.
- `QG-PMF-002`: At least two design partners provide production-shaped artifacts or faithful generators and oracle queries.
- `QG-PMF-003`: At least one partner commits money or equivalent procurement-backed effort specifically for bounded execution/predictable receipts.
- `QG-PMF-004`: If dominant pain is outside bounded graph analysis, the affected profile is narrowed or killed.
- `QG-PMF-005`: A second algorithm or broad driver surface is not funded solely by technical elegance.

---

# 5. Open Questions

## 5.1 Founder Questions

1. Which exact security/IAM/dependency artifact and recurring answer has the strongest budget owner and urgency?
2. Does the buyer value a hard refusal and receipt enough to change procurement, scheduling, or cloud sizing behavior?
3. Is Neo4j query compatibility required in production, or is it primarily a migration/demo accelerator?
4. Which is the first paid promise: “run this existing query under 8 GB,” “know before running,” “prove resource use afterward,” or a bundle of all three?
5. What evidence kills the graph wedge and redirects deterministic compute toward a non-graph batch workload?

## 5.2 Compatibility Questions

1. Which production-shaped Cypher queries from target users fit a bounded analytical profile?
2. Which Bolt version and official driver should define the first black-box contract after Python?
3. Which openCypher TCK scenarios are legally and semantically usable as must-pass tests?
4. Which Neo4j TestKit behaviors are necessary for a read-only auto-commit analytical endpoint?
5. Which GDS procedure result columns and estimate modes are essential to unchanged user code?
6. Is parsing implemented clean-room in Rust, delegated to a separately licensed parser bridge, or profile-specific without general grammar claims?

## 5.3 Memory and Enforcement Questions

1. Is Linux cgroup v2 the production enforcement baseline, with macOS limited to development measurement?
2. How should mapped clean pages and page cache be charged to a user-visible hard ceiling?
3. Can allocator arenas be bounded sufficiently in-process, or should every admitted run execute in a supervised worker process?
4. What enforcement overshoot is useful and honest at 5 GB, 10 GB, and 50 GB ceilings?
5. How are shared artifact pages charged when multiple concurrent runs reuse them?
6. Which estimate terms can be proven analytically and which require empirical calibration?

## 5.4 Algorithm and Storage Questions

1. Should bounded paths or PageRank be the first complete external-memory OLAP proof after the existing walk compatibility seed?
2. Which PageRank spill design gives the best first trade-off: edge-streamed pull, partitioned push with external accumulation, or blocked destination tiles?
3. For WCC, is a disk-backed label/parent plane with streaming edges sufficient, or does practical convergence require partition-local compression state?
4. For similarity, which candidate-generation strategy avoids all-pairs state while retaining buyer-relevant quality?
5. Which sidecars deserve persistent artifact status versus per-run conversion?
6. How much layout proliferation can the manifest/versioning system support before operational complexity erases RAM savings?
7. Which approximation bounds are legible enough for security and dependency users to trust?

## 5.5 Verification Questions

1. What is the minimum legal distributable oracle corpus derived from Neo4j/openCypher/GDS repositories?
2. Which results require exact equality, stable ordering, set equality, or floating tolerance?
3. How will stochastic community and embedding algorithms be compared without claiming schedule-independent identity they cannot provide?
4. Which production artifacts can be retained, anonymized, or synthetically regenerated for public reproducibility?
5. What held-out corpus is sufficient before memory calibration can be called reliable?

## 5.6 Post-Reconciliation Decisions

The three evidence lanes pass `scripts/validate_neo4j_family_evidence.py`: 32,262 denominator rows equal 32,262 evidence rows with no identity, blob, byte, extension, status, relevance, critical-read, or evidence-ID violations. Repository coverage no longer blocks specification work. These decisions still require founder, implementation, measurement, or legal evidence:

1. Which exact captured customer query, official driver/version, Bolt profile, and artifact freeze `KB-ACCESS-P0`?
2. Is bounded BFS/access path the first customer slice, and is PageRank the first iterative GDS-shaped proof after it?
3. Which grammar, TCK, TestKit, driver, GDS, and APOC fixtures may be distributed, translated, or used only as private oracles after legal review?
4. Which initial numeric ceilings, enforcement overshoot, estimator error, cancellation latency, temp-space, and I/O thresholds are useful to the first partner?
5. Does a partner require managed read transactions, routing, graph values, GQL statuses, or a named procedure before the compatibility bridge materially reduces ceremony?

The evidence proves what exists, how large it is, and which contracts can serve as oracles. It does not prove that the market values the resulting product or that the current Rust implementation satisfies these requirements.
