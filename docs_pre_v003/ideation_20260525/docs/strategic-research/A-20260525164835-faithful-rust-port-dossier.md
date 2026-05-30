# Faithful Rust Port of Neo4j: Fact Check, Backend Strategy, and LLM-Readiness Dossier

This dossier fact-checks `/Users/amuldotexe/Downloads/Faithful-Rust-Port-Analysis.md` against:

- the local `neo4j/neo4j` checkout in `ref-repo-folder/neo4j`
- the current Knight Bus repo evidence
- official Neo4j documentation
- the backend-only storage ideas in [Knight Bus Algorithm Storage Atlas](../KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md)

The chosen target is:

> the usage interface should feel flawlessly like Neo4j, while the backend is free to diverge if that is what makes the Rust system materially better.

That framing matters. It means "faithful" should primarily mean **user-visible compatibility**, not **internal architectural fidelity**.

## Premise Check

### Short answer

A faithful Rust port can be done **in principle**, but only if "faithful" is defined as:

- Neo4j-shaped external behavior
- Neo4j-shaped Bolt and driver compatibility
- Neo4j-shaped Cypher semantics within an explicit version envelope
- Neo4j-shaped transactional and error behavior
- a clearly declared migration boundary for storage and import

It should **not** be defined as:

- byte-for-byte internal engine similarity
- identical file formats
- "same everything" without a compatibility matrix
- a short schedule inferred from LOC compression alone

### Overall accuracy estimate for the attached note

- `Overall`: `40/100`
- `As product/adoption thesis`: about `70/100`
- `As engineering estimate and delivery plan`: about `20/100`

Why the score is not lower:

- the note correctly identifies the strongest adoption lever: keep the user-facing surface familiar
- the note is directionally right that the current Knight Bus proof wins by aligning storage to a narrow hot path
- the note is directionally right that tests should become executable specifications

Why the score is not higher:

- it conflates product promise, protocol compatibility, and internal engine design
- it makes strong numeric claims without primary evidence
- it understates the complexity of Cypher, Bolt, drivers, transaction semantics, and operational behavior
- it treats backend specialization and full Neo4j compatibility as if they were the same problem

### Important correction

The most useful target is **not**:

> "replace the Neo4j binary, same everything"

The most useful target is:

> "keep the Neo4j user contract stable, but allow the Rust backend to choose better storage and execution strategies behind that contract"

That is a much stronger engineering brief.

## Expert Lenses

- `Product adoption lens`: the winning promise is low switching cost, not language purity.
- `Kernel and database lens`: user-visible compatibility is much broader than storage and query speed.
- `Protocol and driver lens`: Bolt, sessions, transactions, retries, and errors are part of the product.
- `Backend systems lens`: the atlas ideas are valuable, but they belong behind the interface, not in front of it.
- `Skeptical delivery lens`: large compatibility surfaces do not collapse safely into optimistic LOC ratios and short timelines.

## Candidate Approaches

| approach | upside | downside | verdict |
| --- | --- | --- | --- |
| Full internal port of Neo4j Community architecture | closest to the original codebase structure | inherits most of the original system complexity while still rewriting everything | reject as the default framing |
| Knight Bus-style specialized engine with a new user interface | fastest path to performance proofs | gives up the low-friction Neo4j adoption story | reject for this goal |
| Neo4j-compatible facade with a Rust-specialized backend | preserves the adoption story while allowing internal layout and execution divergence | requires a very explicit compatibility contract | choose |
| Dual-plane architecture: general compatibility core plus specialized algorithm artifacts | best long-term blend of OLTP compatibility and backend acceleration | more complex than a narrow benchmark engine | choose as the long-term shape |

## Chosen Thesis

The strongest defensible thesis is:

1. The **frontend contract** should remain Neo4j-shaped.
2. The **backend contract** should be free to become Knight Bus-shaped where that improves the dominant inner loop.
3. The current atlas ideas should be imported **only into backend design**.
4. The single most important missing artifact is a **user-observable compatibility contract**.
5. LLMs should not be asked to generate serious implementation code until that contract and its executable tests exist.

### What "flawlessly like Neo4j" should mean

At minimum, the public contract should cover:

- connection schemes and Bolt handshake behavior
- authentication flow
- session semantics
- explicit and implicit transactions
- query execution and result materialization
- retry and retryable error behavior
- Cypher version behavior
- status codes, error messages, and notifications
- database selection semantics
- import and migration workflow
- supported procedures and functions

### What should remain backend-only

These ideas from the atlas are useful, but they should stay internal:

- `BaseGraphSnapshot`
- `PropertyPlane`
- `AlgorithmArtifact`
- `ComputeScratch`
- `ResultSidecar`
- `FormatSelectionProfile`
- layout-family names such as `AnchorDualCsrLayoutV1` or `InboundPowerLayoutV1`

Those are good engine words.

They are **not** good user-facing compatibility words if the product promise is "this feels like Neo4j."

## Evidence and Verification

### Sourced facts

- The current Neo4j Cypher manual says the current manual covers `Cypher 25`, and that new features are added there while `Cypher 5` is frozen: [Cypher Manual introduction](https://neo4j.com/docs/cypher-manual/current/introduction/), [Select Cypher version](https://neo4j.com/docs/cypher-manual/25/queries/select-version/).
- The Bolt docs explicitly document a compatibility matrix, PackStream, state transitions, and handshake details. That alone is evidence that "same Bolt" is a real compatibility surface, not a trivial serialization swap: [Bolt Protocol docs](https://neo4j.com/docs/bolt/current/), [Bolt handshake](https://neo4j.com/docs/bolt/current/bolt/handshake/).
- The Neo4j Python driver docs show that session and transaction behavior includes retryability semantics, explicit and implicit transaction modes, and driver-mediated behavior: [Python driver transactions](https://neo4j.com/docs/python-manual/current/transactions/), [Advanced query mechanisms](https://neo4j.com/docs/python-manual/current/query-advanced/).
- The Java driver docs distinguish `neo4j` routing behavior from direct connections: [Java driver API](https://neo4j.com/docs/api/java-driver/current/org.neo4j.driver/org/neo4j/driver/Driver.html).
- The operations manual confirms that `neo4j-admin database import full` writes into Neo4j's native format and has edition-specific behavior and operational assumptions: [neo4j-admin import](https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/).
- The local Neo4j checkout README states Community Edition is GPLv3 and that Enterprise includes closed-source components not present in this repo: [local README](../../ref-repo-folder/neo4j/README.asciidoc).
- Neo4j's trademark policy states that open-source licensing does not grant trademark rights and that modified products should not be branded as Neo4j without permission: [Trademark Policy and Guidelines](https://legal.neo4j.com/).
- The current Knight Bus README and prior notes show that the repo's strongest proof today is a narrow one: fixed-hop traversal over immutable snapshots can beat Cypher over Bolt on the same dataset, but that is not yet proof of general Neo4j replacement: [README](../../README.md), [Knight Bus Rust Vs Neo4j ELI5](./A-20260416151416-rust-vs-neo4j-proof-eli5.md), [v001 PRD](../v001-PRD.md).
- The atlas correctly argues that different graph algorithms want different byte shapes, but those ideas are more directly applicable to backend execution strategy than to the public compatibility surface: [Knight Bus Algorithm Storage Atlas](../KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md).

### Local repo verification

These are directly verified from the local `ref-repo-folder/neo4j` checkout:

- top-level `community/` directories in this checkout: `69`
- `community/cypher` Java + Scala line count: about `842,622`
- `community/cypher` Scala line count: about `674,196`
- `community/cypher` Java line count: about `168,426`
- `community/bolt` Java line count: about `72,982`
- `community/kernel` Java line count: about `121,729`
- `.feature` files under the local Cypher spec-suite resource tree: `65`
- counted `Scenario` lines in those local feature files: about `1,407`

These numbers do **not** prove an external total test count, but they do show that the attached note's precision around module sizes and scenario counts is not reliable as written.

### Claim-by-claim fact check

| claim from the note | verdict | assessment |
| --- | --- | --- |
| "Replace the Neo4j binary. Nothing else changes." | `unsupported as stated` | Good product aspiration, but too broad unless the exact compatibility envelope is written down first. |
| "Same Cypher, same Bolt, same drivers, same everything." | `unsupported` | Cypher versioning, Bolt state machine coverage, driver retries, auth, errors, admin behavior, and procedures make this much broader than the note admits. |
| "Same data files (or a one-time migration)." | `internally inconsistent` | "same everything" and "migration acceptable" are two different promises. The project must choose one. |
| "cypher is 701,841 LOC" | `contradicted by local checkout` | The current local checkout is larger than that. |
| "bolt is 42,064 LOC" | `contradicted by local checkout` | The local checkout is materially larger. |
| "Neo4j's Cucumber test suite passes (3,874 scenarios)." | `unsupported from available evidence` | The local tree clearly has feature assets, but the exact quoted scenario count is not established by the evidence gathered here. |
| "Bun proved this exact playbook works: 705K LOC Zig to Rust in ~1 week with AI." | `unsupported` | The official Bun repo describes Bun as written in Zig and a drop-in Node replacement. No primary source found here supports the specific rewrite claim. |
| "Rust will give 3x to 10x gains across the board." | `speculative` | Narrow workload wins are plausible; broad faithful-port multipliers are not established. |
| "This is a 3-6 month project with AI assistance." | `unsupported` | No credible evidence in the gathered sources supports that estimate for a compatibility-grade effort. |
| "Tests should be the executable spec." | `directionally right` | This is one of the strongest parts of the note. |
| "Storage should match the dominant inner loop." | `supported` | This is well aligned with the Knight Bus evidence and the atlas. |
| "Backend specialization can coexist with a Neo4j-like product story." | `supported with constraints` | Yes, but only if specialization stays behind a stable compatibility surface. |

### What the skeptical lens says

The weakest premise in the note is not "Rust can be fast."

The weakest premise is:

> if the backend is better, the product is automatically a drop-in replacement.

That premise fails because the user-facing contract is much wider than storage and hot-loop speed.

### What the other lenses salvage

- The `product lens` salvages the adoption insight: low switching cost is the right commercial north star.
- The `backend systems lens` salvages the atlas insight: specialized internal layouts are a real lever.
- The `specification lens` salvages the test philosophy: executable compatibility suites are mandatory.

## Final Synthesis

### The most important thing

If the goal is maximum adoption, the most important artifact is:

> a compatibility contract that defines exactly what a current Neo4j user can expect to remain unchanged.

That contract matters more than the first Rust storage layout, more than the first benchmark, and more than the first line-count estimate.

Without it, "faithful" means nothing precise.

With it, the backend is finally allowed to get inventive.

### The backend-only ideas that should be imported now

These ideas from the atlas and prior Knight Bus notes are worth carrying forward immediately:

- immutable sealed artifacts
- dense integer IDs
- `mmap` open paths
- exact key lookup kept separate from traversal or compute
- result sidecars instead of mutating the base artifact
- a small reusable set of layout families selected by workload
- storage shaped around the dominant inner loop

### The backend-only ideas that should *not* leak into the product contract

- new public nouns such as `BaseGraphSnapshot` or `FormatSelectionProfile`
- a bespoke CLI that replaces Cypher as the primary interface
- benchmark claims that compare specialized replay against the wrong Neo4j layer and then generalize too far
- per-algorithm storage talk presented as if it were the minimum v1 product

### The key architectural recommendation

Use a **compiler-style split**:

- one Neo4j-shaped frontend contract
- one Rust backend with multiple internal execution and layout families

That lets the system say:

- "to the user, this is Neo4j-shaped"
- "to the backend, each workload gets the byte shape it actually deserves"

This is the cleanest way to combine the adoption thesis with the atlas thesis.

## Roadmap

### Phase 0: define the contract before writing serious Rust

Create a `Neo4j Compatibility Contract Pack` containing:

- target product envelope: Community only, or more
- Cypher envelope: `Cypher 5`, `Cypher 25`, or both
- driver matrix: Python, JavaScript, Java at minimum
- Bolt version matrix and handshake traces
- auth/session/transaction expectations
- error/status/notification expectations
- import and migration story
- explicit non-goals

This is the highest-priority pre-work item.

### Phase 1: capture ground truth from a real Neo4j instance

Build a golden-behavior corpus that records:

- query text
- parameters
- expected result records
- expected ordering rules
- expected errors and status codes
- retryable versus non-retryable failures
- session and transaction traces

The ideal shape is "live Neo4j behavior turned into executable fixtures."

### Phase 2: build the compatibility shell

Before backend cleverness, prove the public contract:

- Bolt handshake and session state
- auth
- explicit and implicit transactions
- result streaming and summaries
- basic Cypher parsing and execution envelope

This shell can initially support a narrow subset, but it must fail in deliberate, well-specified ways.

### Phase 3: build the minimum backend for general property-graph correctness

Do not jump straight from Knight Bus to per-algorithm bespoke layouts for everything.

First build the minimum backend needed for:

- anchor lookup
- labeled node and relationship storage
- basic transactional read/write semantics
- query execution correctness on a defined Cypher subset

### Phase 4: add backend-specialized artifact families behind the facade

This is where the atlas pays off.

Use the atlas internally, not publicly, for:

- `AnchorDualCsrLayoutV1`-style traversal artifacts
- `InboundPowerLayoutV1`-style power-iteration artifacts
- `RelaxationFrontierLayoutV1`-style shortest-path artifacts
- `OrderedWedgeLayoutV1`-style intersection artifacts

These should power:

- narrow workload accelerators
- future GDS-like procedure implementations
- materialized internal projections

They should not redefine the public user interface.

### Phase 5: widen coverage only after compatibility proofs hold

Only after the compatibility shell and golden corpus are stable should the effort widen into:

- more Cypher surface
- more procedures and functions
- broader planner coverage
- more operational tooling
- any GDS-like algorithm surface

## Open Questions

- Is the real compatibility target `Cypher 25`, `Cypher 5`, or a deliberate dual-version envelope?
- Must the first version support both `bolt://` and `neo4j://` behavior, or is direct-driver mode enough for v1?
- Is the initial goal read-mostly compatibility, or full transactional write parity?
- Is any GDS-like surface actually in scope for v1, or should the atlas remain strictly a backend planning document until later?
- Is the branding story "Neo4j-compatible" or "drop-in for Neo4j workloads"? Trademark constraints matter here.

## Missing Context For LLM-Assisted Implementation

These are the main missing inputs an LLM would need before it could safely help implement a serious compatibility-grade Rust effort:

1. `User Promise Matrix`
   - exact statement of what remains unchanged for users

2. `Golden Compatibility Corpus`
   - recorded query, result, error, and driver behavior fixtures

3. `Bolt Coverage Matrix`
   - handshake versions, message coverage, state transitions

4. `Cypher Envelope`
   - language versions, unsupported clauses, expected semantics

5. `Procedure and Function Inventory`
   - what must exist, what can be deferred

6. `Operational Contract`
   - import, startup, config, logs, monitoring, admin behavior

7. `Migration Boundary`
   - whether storage is migrated, proxied, imported, or recompiled

8. `Backend Layout Glossary`
   - internal mapping from workloads to layout families, especially if the atlas is adopted

9. `Benchmark Fairness Matrix`
   - Cypher vs GDS vs custom-runtime comparisons kept clearly separated

10. `Legal and Naming Constraints`
   - GPL, trademark, and compatibility-language boundaries

## What would change the conclusion

The conclusion would become much more optimistic if the scope is reduced to:

- read-mostly workloads
- a narrow Cypher subset
- direct-driver mode
- migrated storage
- no GDS promise in v1

The conclusion would become much more pessimistic if the scope includes:

- broad Cypher parity across `Cypher 5` and `Cypher 25`
- official-driver behavioral parity across languages
- full operational resemblance
- wide procedure coverage
- GDS-like algorithm parity in the same release

## Bottom line

The attached note is strongest when it says:

- users want the familiar Neo4j surface
- backend storage should align with the dominant inner loop

It is weakest when it says:

- "same everything"
- "Bun proved the schedule"
- "the module and test counts are known enough to estimate a short delivery"

The winning move is not a literal porting mindset.

The winning move is:

> preserve the Neo4j-shaped user contract, but treat the Rust backend as a new execution engine that is allowed to use Knight Bus-style and atlas-style specialization behind the curtain.
